//! Direct process runner (spec §29).
//!
//! Captures stdout and stderr concurrently (no pipe deadlock). Whole streams
//! currently buffer in memory; `INLINE_CAP` bounds receipts, not capture memory.
//! Output is redacted through the [`CredentialBroker`] before persistence or
//! truncation, so secrets crossing the inline boundary cannot leak to artifacts.

use crate::artifact::ArtifactSpool;
use crate::credential::CredentialBroker;
use crate::{ToolError, ToolsContext, authorize};
use futures_util::FutureExt;
use std::collections::{BTreeMap, HashMap};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::Duration;
#[cfg(any(unix, windows, test))]
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

#[cfg(all(test, unix))]
tokio::task_local! {
    static PANIC_AFTER_SPAWN_READY: PathBuf;
}

/// Inline receipt cap per stream. Full capture is currently buffered in memory
/// before redaction and artifact storage; this is not a capture memory bound.
pub const INLINE_CAP: usize = 1024 * 1024;

/// What to run.
#[derive(Clone, Debug)]
pub struct ProcessSpec {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: HashMap<String, String>,
    pub timeout: Duration,
}

impl ProcessSpec {
    #[must_use]
    pub fn new(program: &str) -> Self {
        Self {
            program: program.to_owned(),
            args: Vec::new(),
            cwd: None,
            env: HashMap::new(),
            timeout: Duration::from_secs(120),
        }
    }
}

/// What came back.
#[derive(Clone, Debug)]
pub struct ProcessReceipt {
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub stdout_artifact: Option<tachyon_types::ArtifactId>,
    pub stderr_artifact: Option<tachyon_types::ArtifactId>,
}

/// Runs `spec` (policy `process.spawn`, scope = program name).
pub async fn run(context: &ToolsContext, spec: &ProcessSpec) -> Result<ProcessReceipt, ToolError> {
    run_cancellable(context, spec, CancellationToken::new()).await
}

/// Runs an owned process group. Already-cancelled requests never spawn. A
/// timeout includes inherited stdout/stderr pipes, not just the leader's life.
/// Cancellation/timeout sends TERM, allows 100 ms grace, then sends KILL and
/// waits for the owned process group to have no live members, then reaps the
/// immediate child. Panics after spawn are caught long enough to terminate and
/// drain the owned tree before unwinding to the owner. Dropping or
/// aborting this future still sends KILL without awaiting; immediate-child
/// reaping then relies on Tokio's best-effort reaper.
/// Unsupported platforms fail closed until equivalent tree ownership exists.
pub async fn run_cancellable(
    context: &ToolsContext,
    spec: &ProcessSpec,
    cancel: CancellationToken,
) -> Result<ProcessReceipt, ToolError> {
    if cancel.is_cancelled() {
        return Err(ToolError::ProcessCancelledBeforeStart);
    }
    let cwd = tachyon_policy::contain(
        &context.workspace_root,
        spec.cwd.as_deref().unwrap_or(Path::new(".")),
    )?;
    // Bind the inherited environment too, and execute this exact snapshot.
    let mut env: BTreeMap<std::ffi::OsString, std::ffi::OsString> = std::env::vars_os().collect();
    env.extend(
        spec.env
            .iter()
            .map(|(key, value)| (key.into(), value.into())),
    );
    let encoded_env: Vec<_> = env
        .iter()
        .map(|(key, value)| (key.as_encoded_bytes(), value.as_encoded_bytes()))
        .collect();
    let operation = serde_json::json!({
        "op": "process.spawn",
        "program": spec.program,
        "args": spec.args,
        "cwd": cwd.as_os_str().as_encoded_bytes(),
        "env": encoded_env,
        "timeout": { "secs": spec.timeout.as_secs(), "nanos": spec.timeout.subsec_nanos() },
    });
    authorize(
        &context.policy,
        &context.approvals,
        "process.spawn",
        &spec.program,
        &operation,
        &format!("run {} {}", spec.program, spec.args.join(" ")),
    )?;
    let mut command = tokio::process::Command::new(&spec.program);
    command.args(&spec.args);
    command.env_clear().envs(env);
    command.current_dir(cwd);
    command.stdout(std::process::Stdio::piped());
    command.stderr(std::process::Stdio::piped());
    let (status, stdout, stderr) = execute(command, spec.timeout, cancel).await?;
    finish(
        &context.artifacts,
        &context.credentials,
        status.code(),
        false,
        &stdout,
        &stderr,
    )
}

#[cfg(unix)]
async fn execute(
    mut command: tokio::process::Command,
    timeout: Duration,
    cancel: CancellationToken,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), ToolError> {
    // Recheck after synchronous containment/approval/environment preparation.
    if cancel.is_cancelled() {
        return Err(ToolError::ProcessCancelledBeforeStart);
    }
    #[cfg(target_os = "linux")]
    ensure_procfs_available(Path::new("/proc")).map_err(|error| {
        ToolError::ProcessStartFailed(format!(
            "cannot safely monitor the process group because procfs cannot prove group drain: {error}"
        ))
    })?;
    configure_owned_session(&mut command);
    command.kill_on_drop(true);
    let child = command
        .spawn()
        .map_err(|error| ToolError::ProcessStartFailed(error.to_string()))?;
    let pid = child.id().expect("newly spawned child has a PID");
    let mut owned = OwnedChild {
        child,
        group: Some(i32::try_from(pid).expect("Unix PIDs fit pid_t")),
    };
    // These are scoped futures, not detached reader tasks. The deadline spans
    // both leader exit AND inherited pipes; all readers drop on every exit path.
    let result = AssertUnwindSafe(async {
        let stdout = owned.child.stdout.take().ok_or_else(|| {
            stage_io("take-stdout", &std::io::Error::other("missing stdout pipe"))
        })?;
        let stderr = owned.child.stderr.take().ok_or_else(|| {
            stage_io("take-stderr", &std::io::Error::other("missing stderr pipe"))
        })?;
        #[cfg(all(test, unix))]
        if let Ok(ready) = PANIC_AFTER_SPAWN_READY.try_with(Clone::clone) {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("test did not trigger the post-spawn panic");
            panic!("injected verification worker panic after process-tree start");
        }

        let output = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(ToolError::ProcessCancelled),
            () = tokio::time::sleep(timeout) => Err(ToolError::ProcessTimeout(timeout)),
            output = async {
                tokio::try_join!(
                    owned.wait_for_exit(pid),
                    read_stream(stdout),
                    read_stream(stderr)
                )
            } => output.map_err(|error| stage_io("wait-or-read", &error)),
        };
        match output {
            Ok(((), stdout, stderr)) => match owned.kill_and_reap().await {
                Ok(status) => Ok((status, stdout, stderr)),
                Err(error) => Err(stage_io("kill-and-reap", &error)),
            },
            Err(error) => Err(error),
        }
    })
    .catch_unwind()
    .await;
    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            owned.terminate_until_reaped().await;
            Err(error)
        }
        Err(panic) => {
            owned.terminate_until_reaped().await;
            std::panic::resume_unwind(panic)
        }
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn configure_owned_session(command: &mut tokio::process::Command) {
    use std::os::unix::process::CommandExt as _;
    // A fresh session creates a process group whose PGID is this child's PID.
    // It also prevents unrelated processes in the caller's session from
    // joining the group that Tachyon must drain and reap.
    // SAFETY: the pre-exec hook only calls POSIX async-signal-safe `setsid` and
    // reads errno if it fails. The spawned child is not a process-group leader,
    // so creating its own session and group is valid.
    unsafe {
        command.as_std_mut().pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
}

/// Labels an IO failure with the process-lifecycle stage that produced it,
/// keeping the raw OS code in the message for platform diagnosis.
#[cfg(unix)]
fn stage_io(stage: &'static str, error: &std::io::Error) -> ToolError {
    ToolError::Io(std::io::Error::new(
        error.kind(),
        format!("{stage} (os error {:?}): {error}", error.raw_os_error()),
    ))
}

/// Windows process-tree ownership via Job Objects (spec: "Job Object or
/// equivalent tree ownership"). The child is assigned to a fresh job with
/// `KILL_ON_JOB_CLOSE`; every descendant joins the same job unless it holds
/// breakaway rights, so closing or terminating the job ends the whole tree.
/// Reaping the owned leader still reserves its PID until `Child::wait`.
#[cfg(windows)]
#[allow(unsafe_code)]
async fn execute(
    mut command: tokio::process::Command,
    timeout: Duration,
    cancel: CancellationToken,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), ToolError> {
    use std::os::windows::process::CommandExt as _;
    use windows_sys::Win32::System::Threading::{
        CREATE_SUSPENDED, OpenProcess, PROCESS_SET_QUOTA, PROCESS_TERMINATE,
    };
    if cancel.is_cancelled() {
        return Err(ToolError::ProcessCancelledBeforeStart);
    }
    // Create the containment boundary before the child exists. The initial
    // thread stays suspended until assignment succeeds, so setup errors cannot
    // leave a process running outside the Job Object.
    let job = JobObject::create().map_err(|error| win_stage_io("create-job", &error))?;
    command.kill_on_drop(true);
    command.as_std_mut().creation_flags(CREATE_SUSPENDED);
    let child = command
        .spawn()
        .map_err(|error| ToolError::ProcessStartFailed(error.to_string()))?;
    let mut owned = OwnedChild {
        child,
        job,
        job_assigned: false,
    };
    let result = AssertUnwindSafe(async {
        // Open our own handle: the suspended, unreaped child keeps its PID
        // reserved until assignment and `Child::wait`. PROCESS_SET_QUOTA +
        // PROCESS_TERMINATE are required for job assignment.
        let pid = owned.child.id().expect("newly spawned child has a PID");
        // SAFETY: `pid` identifies our live suspended child; the handle is
        // owned and closed immediately after assignment.
        let raw = unsafe { OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, 0, pid) };
        if raw.is_null() {
            return Err(win_stage_io(
                "open-process",
                &std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: `raw` is the live handle of our suspended child; the job
        // outlives this call inside `OwnedChild`.
        let assigned = unsafe { AssignProcessToJobObject(owned.job.handle, raw) };
        // SAFETY: assignment copied what it needs; our open handle is now excess.
        unsafe { CloseHandle(raw) };
        if assigned == 0 {
            return Err(win_stage_io("assign-job", &std::io::Error::last_os_error()));
        }
        owned.job_assigned = true;
        resume_suspended_process(pid).map_err(|error| win_stage_io("resume-child", &error))?;

        let stdout = owned.child.stdout.take().ok_or_else(|| {
            win_stage_io("take-stdout", &std::io::Error::other("missing stdout pipe"))
        })?;
        let stderr = owned.child.stderr.take().ok_or_else(|| {
            win_stage_io("take-stderr", &std::io::Error::other("missing stderr pipe"))
        })?;
        let output = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(ToolError::ProcessCancelled),
            () = tokio::time::sleep(timeout) => Err(ToolError::ProcessTimeout(timeout)),
            output = async {
                tokio::try_join!(
                    owned.child.wait(),
                    read_stream(stdout),
                    read_stream(stderr)
                )
            } => output.map_err(|error| win_stage_io("wait-or-read", &error)),
        };
        match output {
            Ok((status, stdout, stderr)) => match owned.terminate().await {
                Ok(_) => Ok((status, stdout, stderr)),
                Err(error) => Err(win_stage_io("terminate-job", &error)),
            },
            Err(error) => Err(error),
        }
    })
    .catch_unwind()
    .await;
    match result {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            owned.terminate_until_reaped().await;
            Err(error)
        }
        Err(panic) => {
            owned.terminate_until_reaped().await;
            std::panic::resume_unwind(panic)
        }
    }
}

/// Resume the single primary thread created suspended by `CREATE_SUSPENDED`.
/// No user code can create descendants until this thread is resumed.
#[cfg(windows)]
#[allow(unsafe_code)]
fn resume_suspended_process(pid: u32) -> std::io::Result<()> {
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_NO_MORE_FILES, GetLastError, INVALID_HANDLE_VALUE, SetLastError,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    // SAFETY: Toolhelp returns an owned snapshot handle or INVALID_HANDLE_VALUE.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    let result = (|| {
        let mut entry = THREADENTRY32 {
            dwSize: u32::try_from(std::mem::size_of::<THREADENTRY32>())
                .expect("thread entry struct fits u32"),
            ..THREADENTRY32::default()
        };
        // SAFETY: `entry` is initialized to the documented size and writable.
        if unsafe { Thread32First(snapshot, &raw mut entry) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut primary_thread = None;
        loop {
            if entry.th32OwnerProcessID == pid {
                if primary_thread.replace(entry.th32ThreadID).is_some() {
                    return Err(std::io::Error::other(
                        "suspended child unexpectedly has multiple threads",
                    ));
                }
            }
            // A failed next call is the normal end-of-snapshot condition only
            // when Windows reports ERROR_NO_MORE_FILES.
            unsafe { SetLastError(0) };
            // SAFETY: `entry` remains writable and the snapshot stays open.
            if unsafe { Thread32Next(snapshot, &raw mut entry) } == 0 {
                // SAFETY: GetLastError is thread-local and called immediately
                // after the failing Toolhelp API on this same thread.
                if unsafe { GetLastError() } == ERROR_NO_MORE_FILES {
                    break;
                }
                return Err(std::io::Error::last_os_error());
            }
        }
        let thread_id = primary_thread.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "suspended child has no primary thread",
            )
        })?;
        // SAFETY: the thread id came from the snapshot for our suspended child.
        let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, thread_id) };
        if thread.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the owned thread handle has THREAD_SUSPEND_RESUME access.
        let previous_suspend_count = unsafe { ResumeThread(thread) };
        let resume_result = if previous_suspend_count == u32::MAX {
            // Preserve ResumeThread's error before CloseHandle can alter it.
            // SAFETY: GetLastError is thread-local and called immediately
            // after the failed ResumeThread call on this same thread.
            let error = unsafe { GetLastError() };
            Err(std::io::Error::from_raw_os_error(error as i32))
        } else if previous_suspend_count != 1 {
            Err(std::io::Error::other(format!(
                "primary thread had unexpected suspend count {previous_suspend_count}"
            )))
        } else {
            Ok(())
        };
        // SAFETY: `thread` is the owned handle returned by OpenThread.
        if unsafe { CloseHandle(thread) } == 0 {
            return Err(resume_result
                .err()
                .unwrap_or_else(std::io::Error::last_os_error));
        }
        resume_result
    })();
    // SAFETY: `snapshot` is a valid owned Toolhelp handle, closed exactly once.
    unsafe { CloseHandle(snapshot) };
    result
}

/// Labels an IO failure with the process-lifecycle stage that produced it,
/// keeping the raw OS code in the message for platform diagnosis.
#[cfg(windows)]
fn win_stage_io(stage: &'static str, error: &std::io::Error) -> ToolError {
    ToolError::Io(std::io::Error::new(
        error.kind(),
        format!("{stage} (os error {:?}): {error}", error.raw_os_error()),
    ))
}

/// An owned Windows Job Object: closing the last handle kills the tree when
/// `KILL_ON_JOB_CLOSE` is set, which it always is here.
#[cfg(windows)]
struct JobObject {
    handle: HANDLE,
}

/// SAFETY: the handle is owned (created by us, closed in `Drop`); only the
/// owning `OwnedChild` touches it, and all methods take `&self` across awaits
/// without transferring ownership.
#[cfg(windows)]
#[allow(unsafe_code)]
unsafe impl Send for JobObject {}
#[cfg(windows)]
#[allow(unsafe_code)]
unsafe impl Sync for JobObject {}

#[cfg(windows)]
#[allow(unsafe_code)]
impl JobObject {
    fn create() -> std::io::Result<Self> {
        // SAFETY: null security/name creates an unnamed job owned by us.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: `info` is a live, aligned struct of the documented size.
        let set = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&raw const info).cast(),
                u32::try_from(std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                    .expect("job limits struct fits u32"),
            )
        };
        if set == 0 {
            // SAFETY: the handle is valid and owned; closing exactly once here.
            unsafe { CloseHandle(handle) };
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { handle })
    }

    fn terminate(&self) -> std::io::Result<()> {
        // SAFETY: handle is a valid owned job; exit code is arbitrary.
        let ended = unsafe { TerminateJobObject(self.handle, 1) };
        if ended == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    fn active_processes(&self) -> std::io::Result<u32> {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        let size = u32::try_from(std::mem::size_of_val(&info))
            .map_err(|_| std::io::Error::other("job accounting structure exceeds u32"))?;
        // SAFETY: `info` is writable, aligned, and sized for the selected
        // accounting information class; the handle is owned by this object.
        let queried = unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectBasicAccountingInformation,
                (&raw mut info).cast(),
                size,
                std::ptr::null_mut(),
            )
        };
        if queried == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(info.ActiveProcesses)
    }

    async fn wait_until_empty(&self) -> std::io::Result<()> {
        loop {
            let active = self.active_processes()?;
            if active == 0 {
                return Ok(());
            }
            // A member could have created a child while termination was in
            // flight. Reapply the job-wide kill until the count reaches zero.
            self.terminate()?;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
impl Drop for JobObject {
    fn drop(&mut self) {
        // KILL_ON_JOB_CLOSE ends the tree as the last handle closes.
        // SAFETY: valid owned handle, closed exactly once.
        unsafe { CloseHandle(self.handle) };
    }
}

#[cfg(windows)]
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
#[cfg(windows)]
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};

/// Windows tree owner: the job ends every member; the reaped leader's PID is
/// still reserved by `Child` until waited, as on Unix.
#[cfg(windows)]
struct OwnedChild {
    child: tokio::process::Child,
    job: JobObject,
    job_assigned: bool,
}

#[cfg(windows)]
impl OwnedChild {
    /// Terminates active job members, reaps the leader, and waits for the job to
    /// become empty. An already-dead tree and an already-reaped leader both resolve.
    async fn terminate(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if self.job_assigned {
            match self.job.active_processes() {
                Ok(0) => {}
                Ok(_) | Err(_) => self.job.terminate()?,
            }
        } else if self.child.try_wait()?.is_none() {
            // Until assignment and resume, CREATE_SUSPENDED guarantees the
            // child has not executed code that could create descendants.
            self.child.start_kill()?;
        }
        let status = self.child.wait().await?;
        if self.job_assigned {
            self.job.wait_until_empty().await?;
        }
        Ok(status)
    }

    /// Keeps the process-tree owner alive until the leader is reaped. A
    /// transient lifecycle error must not let the verifier release its
    /// workspace lease while the job can still contain a running process.
    async fn terminate_until_reaped(&mut self) {
        loop {
            match self.terminate().await {
                Ok(_) => return,
                Err(error) => {
                    tracing::error!(%error, "owned process tree is not yet reaped; retrying termination");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

#[cfg(any(unix, windows, test))]
async fn read_stream(mut stream: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// Own a fresh Unix process group until it has been signalled. Keep the leader
/// unreaped until then, reserving its PID so a stale PGID cannot target a reused
/// process group after an early leader exit. Descendants must not call setsid /
/// setpgid to escape: process groups are lifecycle ownership, not a sandbox.
#[cfg(unix)]
struct OwnedChild {
    child: tokio::process::Child,
    group: Option<libc::pid_t>,
}

#[cfg(unix)]
impl OwnedChild {
    /// Fast-poll window (M13): detection granularity for children that
    /// live less than this window's budget. The old fixed 10 ms sleep
    /// dominated every short child's measured runtime; short-lived
    /// children (sh, python3, git) now exit within ~1 ms of detection.
    const FAST_EXIT_POLLS: u32 = 50;
    /// Poll delay while the child is inside the fast window.
    const EXIT_POLL_FAST: Duration = Duration::from_millis(1);
    /// Poll delay after the fast window: bounded wakeups for long-lived
    /// children (verify commands, test runners).
    const EXIT_POLL_SLOW: Duration = Duration::from_millis(10);

    fn exit_poll_delay(polls: u32) -> Duration {
        if polls < Self::FAST_EXIT_POLLS {
            Self::EXIT_POLL_FAST
        } else {
            Self::EXIT_POLL_SLOW
        }
    }

    async fn wait_for_exit(&self, pid: u32) -> std::io::Result<()> {
        // Exit detection is poll-based by design: `leader_exited` uses
        // WNOWAIT so the leader stays unreaped and its PGID stays
        // reserved until `kill_and_reap` signals the group (PID-reuse
        // guard). Only the sleep granularity is variable — fast at first,
        // then backed off.
        let mut polls: u32 = 0;
        loop {
            if leader_exited(pid)? {
                return Ok(());
            }
            let delay = Self::exit_poll_delay(polls);
            polls = polls.saturating_add(1);
            tokio::time::sleep(delay).await;
        }
    }

    async fn terminate(&mut self) -> std::io::Result<()> {
        if let Some(group) = self.group {
            // Allow cooperative children to flush and reap descendants, but
            // never let ignored TERM or inherited pipe handles block cleanup.
            if signal_group(group, libc::SIGTERM).is_ok() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        self.kill_and_reap().await?;
        Ok(())
    }

    /// Retries a failed kill/reap before allowing an owned verification
    /// worker to finish and release the workspace lease.
    async fn terminate_until_reaped(&mut self) {
        loop {
            match self.terminate().await {
                Ok(()) => return,
                Err(error) => {
                    tracing::error!(%error, "owned process group is not yet reaped; retrying termination");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn kill_and_reap(&mut self) -> std::io::Result<std::process::ExitStatus> {
        if let Some(group) = self.group {
            signal_group(group, libc::SIGKILL)?;
            wait_for_group_exit(group).await?;
            // Disarm only after the live group is gone, before reaping frees the PID.
            self.group = None;
        }
        self.child.wait().await
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn group_has_live_processes(group: libc::pid_t) -> std::io::Result<bool> {
    assert!(group > 1, "only probe an owned process group");
    // If the kernel reports no group, skip procfs; otherwise procfs
    // distinguishes live members from zombies so cleanup does not wait on an
    // unrelated reaper.
    // SAFETY: signal 0 probes only the group created for the owned child.
    let probe = unsafe { libc::kill(-group, 0) };
    if probe != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        if error.raw_os_error() != Some(libc::EPERM) {
            return Err(error);
        }
    }
    let mut live = false;
    visit_proc_stats(Path::new("/proc"), |pid, stat| {
        let (state, process_group) = parse_proc_stat(stat, pid)?;
        live = process_group == group && state != "Z" && state != "X";
        Ok(live)
    })?;
    Ok(live)
}

#[cfg(target_os = "linux")]
fn visit_proc_stats(
    proc_root: &Path,
    mut visit: impl FnMut(u32, &str) -> std::io::Result<bool>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(proc_root)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if proc_entry_disappeared(&error) => continue,
            Err(error) => return Err(error),
        };
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(|value| value.parse::<u32>().ok()) else {
            continue;
        };
        let stat = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if proc_entry_disappeared(&error) => continue,
            // This child has its own session, so unrelated inaccessible
            // processes cannot join its group. Descendants that change their
            // credentials are outside the process-group containment contract.
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => continue,
            Err(error) => return Err(error),
        };
        if visit(pid, &stat)? {
            break;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn proc_entry_disappeared(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::NotFound || error.raw_os_error() == Some(libc::ESRCH)
}

#[cfg(target_os = "linux")]
fn ensure_procfs_available(proc_root: &Path) -> std::io::Result<()> {
    // Verify the procfs table and our own stat entry before spawning. Once
    // spawned, the child gets a private session; cleanup may ignore other
    // users' permission-denied entries because they cannot join its process
    // group. A same-identity descendant remains readable under supported
    // procfs configurations.
    let _entries = std::fs::read_dir(proc_root)?;
    let own_pid = std::process::id();
    let stat = std::fs::read_to_string(proc_root.join(own_pid.to_string()).join("stat"))?;
    parse_proc_stat(&stat, own_pid).map(|_| ())
}

#[cfg(target_os = "linux")]
fn parse_proc_stat(stat: &str, pid: u32) -> std::io::Result<(&str, i32)> {
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("malformed /proc/{pid}/stat"),
        ));
    };
    let mut fields = fields.split_whitespace();
    let Some(state) = fields.next() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("missing state in /proc/{pid}/stat"),
        ));
    };
    let _parent = fields.next();
    let Some(process_group) = fields.next().and_then(|value| value.parse::<i32>().ok()) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("malformed process group in /proc/{pid}/stat"),
        ));
    };
    Ok((state, process_group))
}

#[cfg(all(unix, not(target_os = "linux")))]
#[allow(unsafe_code)]
fn group_has_live_processes(group: libc::pid_t) -> std::io::Result<bool> {
    // Same-UID members remain signalable; ESRCH and EPERM mean no live member
    // remains (macOS reports EPERM for a group containing only its zombie leader).
    // SAFETY: signal 0 probes only the process group created for this child.
    if unsafe { libc::kill(-group, 0) } == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::EPERM)) {
        Ok(false)
    } else {
        Err(error)
    }
}

#[cfg(unix)]
async fn wait_for_group_exit(group: libc::pid_t) -> std::io::Result<()> {
    loop {
        if !group_has_live_processes(group)? {
            return Ok(());
        }
        // A process can fork between the initial group signal and its delivery.
        // Re-signal while live members remain so late children are covered too.
        signal_group(group, libc::SIGKILL)?;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[cfg(unix)]
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(group) = self.group {
            // Destructors cannot await a grace period. Force-kill the group
            // synchronously; Child's kill_on_drop + Tokio's orphan reaper are
            // the fallback for immediate-child reaping when the future drops.
            let _ = signal_group(group, libc::SIGKILL);
        }
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn signal_group(group: libc::pid_t, signal: libc::c_int) -> std::io::Result<()> {
    assert!(group > 1, "only signal an owned child process group");
    // SAFETY: negative positive-PGID targets only the fresh group we created.
    // The owned leader remains unreaped, preventing PID/PGID reuse until disarm.
    if unsafe { libc::kill(-group, signal) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else if error.raw_os_error() == Some(libc::EPERM) {
        // macOS reports EPERM (not ESRCH) when the group holds no live,
        // signalable process — the exited-but-unreaped leader plus, at most,
        // zombies. Same-UID live members are always signalable, so EPERM
        // means no worker remains; treating it as fatal would fail every
        // reaping of an already-exited tree. (setuid-root descendants are
        // outside the workspace-exclusion threat model: they can escape
        // containment regardless of signaling.)
        Ok(())
    } else {
        Err(error)
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn leader_exited(pid: u32) -> std::io::Result<bool> {
    // SAFETY: zero is a valid initial siginfo_t representation; waitid writes
    // to this live, aligned buffer. P_PID selects only our child, WNOHANG avoids
    // blocking the executor, and WNOWAIT leaves reaping to the owned Child.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid,
            &raw mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == 0 {
        Ok(info.si_signo != 0)
    } else {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

fn finish(
    spool: &ArtifactSpool,
    broker: &CredentialBroker,
    exit_code: Option<i32>,
    timed_out: bool,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<ProcessReceipt, ToolError> {
    let stdout = broker.redact_bytes(stdout);
    let stderr = broker.redact_bytes(stderr);
    let (stdout_inline, stdout_truncated, stdout_artifact) = split_stream(spool, &stdout)?;
    let (stderr_inline, stderr_truncated, stderr_artifact) = split_stream(spool, &stderr)?;
    Ok(ProcessReceipt {
        exit_code,
        timed_out,
        stdout: stdout_inline,
        stderr: stderr_inline,
        stdout_truncated,
        stderr_truncated,
        stdout_artifact,
        stderr_artifact,
    })
}

fn split_stream(
    spool: &ArtifactSpool,
    bytes: &[u8],
) -> Result<(Vec<u8>, bool, Option<tachyon_types::ArtifactId>), ToolError> {
    if bytes.len() <= INLINE_CAP {
        return Ok((bytes.to_vec(), false, None));
    }
    let id = spool.store(bytes)?;
    Ok((bytes[..INLINE_CAP].to_vec(), true, Some(id)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn fast_exit_poll_window_keeps_short_child_detection_below_slow_backoff() {
        assert_eq!(OwnedChild::exit_poll_delay(0), Duration::from_millis(1));
        assert_eq!(OwnedChild::exit_poll_delay(49), Duration::from_millis(1));
        assert_eq!(OwnedChild::exit_poll_delay(50), Duration::from_millis(10));
        assert_eq!(
            OwnedChild::exit_poll_delay(u32::MAX),
            Duration::from_millis(10)
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "M13 perf component: release mode, run with --ignored"]
    async fn comp_wait_for_exit_fast_window_latency() {
        const WARMUP: usize = 5;
        const SAMPLES: usize = 20;
        let mut samples = Vec::with_capacity(SAMPLES);

        for sample in 0..(WARMUP + SAMPLES) {
            let mut command = tokio::process::Command::new("/bin/sh");
            command
                .arg("-c")
                .arg("sleep 0.002")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .process_group(0)
                .kill_on_drop(true);
            let child = command.spawn().expect("spawn short-lived child");
            let pid = child.id().expect("spawned child has a PID");
            let mut owned = OwnedChild {
                child,
                group: Some(i32::try_from(pid).expect("Unix PIDs fit pid_t")),
            };

            let start = std::time::Instant::now();
            owned.wait_for_exit(pid).await.expect("wait for child exit");
            let elapsed = start.elapsed();
            owned.kill_and_reap().await.expect("drain and reap child");

            if sample >= WARMUP {
                samples.push(elapsed);
            }
        }

        samples.sort();
        let p50 = samples[samples.len() / 2];
        println!("perf[process_wait] n={SAMPLES} p50={p50:?} target p50<8ms");
        assert!(
            p50 < Duration::from_millis(8),
            "process wait regression: p50={p50:?} >= 8ms"
        );
        println!("perf[process_wait] PASS");
    }

    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct BrokenPipe;

    impl tokio::io::AsyncRead for BrokenPipe {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("fixture read failure")))
        }
    }

    #[tokio::test]
    async fn stream_read_failure_is_not_successful_empty_output() {
        let result = read_stream(BrokenPipe).await;
        assert!(matches!(result, Err(error) if error.to_string() == "fixture read failure"));
    }

    /// Signaling an exited leader's group must not error: on macOS the group
    /// holds no live member and `kill` reports EPERM rather than ESRCH. Pins
    /// the tolerance so reaping an already-exited tree stays green there.
    #[cfg(unix)]
    #[tokio::test]
    async fn exited_group_kill_is_not_an_error() {
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("true");
        child.process_group(0);
        let mut child = child.spawn().expect("spawn true");
        let pid = child.id();
        while !leader_exited(pid).expect("waitid") {
            tokio::task::yield_now().await;
        }
        #[cfg(target_os = "linux")]
        assert!(
            !group_has_live_processes(pid as libc::pid_t).expect("inspect zombie-only group"),
            "the liveness probe must treat a zombie-only group as drained"
        );
        signal_group(pid as libc::pid_t, libc::SIGKILL).expect("exited group kill");
        child.wait().expect("reap");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn procfs_probe_rejects_a_root_without_process_entries() {
        let root = std::env::temp_dir().join(format!(
            "tachyon-empty-proc-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir(&root).expect("create empty procfs fixture");
        let result = ensure_procfs_available(&root);
        std::fs::remove_dir(&root).expect("remove empty procfs fixture");
        assert!(
            result.is_err(),
            "an empty directory cannot support process-group liveness checks"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn procfs_scan_tolerates_disappeared_entries() {
        assert!(proc_entry_disappeared(&std::io::Error::from_raw_os_error(
            libc::ESRCH
        )));
        assert!(proc_entry_disappeared(&std::io::Error::from(
            std::io::ErrorKind::NotFound
        )));
        assert!(!proc_entry_disappeared(&std::io::Error::from_raw_os_error(
            libc::EACCES
        )));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn owned_process_session_rejects_external_group_members() {
        use std::os::unix::process::CommandExt as _;

        let mut command = tokio::process::Command::new("/bin/sleep");
        command.arg("30");
        configure_owned_session(&mut command);
        let child = command.spawn().expect("spawn isolated child");
        let pid = child.id().expect("child PID");
        let mut owned = OwnedChild {
            child,
            group: Some(i32::try_from(pid).expect("Unix PIDs fit pid_t")),
        };

        let mut unrelated = std::process::Command::new("/bin/true");
        unrelated.process_group(i32::try_from(pid).expect("Unix PIDs fit pid_t"));
        let error = unrelated
            .spawn()
            .expect_err("another session cannot join the owned process group");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::EPERM),
            "joining a group in another session must fail with EPERM"
        );
        owned.kill_and_reap().await.expect("drain isolated process");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn panic_during_process_tree_execution_drains_through_the_production_boundary() {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "tachyon-panic-reap-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).expect("create temporary root");
        let ready = root.join("ready");
        let leader = root.join("leader");
        let descendant = root.join("descendant");
        let panic = root.join("panic");
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("printf '%s' \"$$\" > \"$LEADER_FILE\"; sleep 30 & printf '%s' \"$!\" > \"$DESCENDANT_FILE\"; printf ready > \"$READY_FILE\"; wait")
            .env("LEADER_FILE", &leader)
            .env("DESCENDANT_FILE", &descendant)
            .env("READY_FILE", &ready)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let run = PANIC_AFTER_SPAWN_READY.scope(
            panic.clone(),
            execute(command, Duration::from_secs(30), CancellationToken::new()),
        );
        let mut task = tokio::spawn(run);
        if tokio::time::timeout(Duration::from_secs(1), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err()
        {
            let _ = std::fs::write(&panic, b"panic now");
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
            panic!("child did not start");
        }
        if tokio::time::timeout(Duration::from_secs(1), async {
            while !leader.exists() || !descendant.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err()
        {
            let _ = std::fs::write(&panic, b"panic now");
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
            panic!("process tree did not record its members");
        }
        std::fs::write(&panic, b"panic now").expect("trigger injected panic");
        let joined = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("worker did not finish panic cleanup")
            .expect_err("injected worker panic must unwind after cleanup");
        assert!(
            joined.is_panic(),
            "worker should preserve its panic payload"
        );
        let group = std::fs::read_to_string(&leader)
            .expect("read process group id")
            .parse::<libc::pid_t>()
            .expect("parse process group id");
        assert!(
            !group_has_live_processes(group).expect("inspect process group"),
            "worker panic must drain every live process in the owned group"
        );
        std::fs::remove_dir_all(root).expect("remove temporary root");
    }
}
