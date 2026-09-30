//! M10 prerequisite: the supervisor's real jobs must not monopolize its mailbox.
//!
//! Every test drives the production supervisor path only. Workspace exclusion,
//! control acknowledgement and process reap are observed through the real lease,
//! the real durable row and the real child process.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tachyon_core::{ConstraintStrength, SupervisorHandle, TaskStatus, create_task, recover_task};
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool, workspace::WorkspaceLease};
use tachyon_types::{SessionId, TaskId, WorkspaceId};
use tachyon_verify::{AcceptanceContract, Clause, VerificationRisk};
use tokio_util::sync::CancellationToken;

/// Windows reap proof: the PID cannot be recycled while the leader is
/// unreaped, and opening it fails once the process is gone.
/// Windows reap wait: the cancel acknowledgement is answered after owned
/// effect workers drain, but the real child reap can lag it on a loaded
/// runner. Poll for death bounded (#37) instead of asserting an
/// instantaneous read — a child still live after the window proves the
/// acknowledgement outran the reap; scheduler lag alone cannot fail it.
/// Same delay-robust treatment as #33.
#[cfg(windows)]
async fn wait_pid_dead(pid: u32) -> bool {
    for _ in 0..200 {
        if pid_dead(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    pid_dead(pid)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn pid_dead(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};
    // SAFETY: read-only open of a live child PID; the handle is closed below.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return true;
    }
    // SAFETY: handle came from the successful OpenProcess above.
    unsafe { CloseHandle(handle) };
    false
}

struct Fixture {
    root: PathBuf,
    context: Arc<ToolsContext>,
    store: Arc<StoreWriter>,
    task: SupervisorHandle,
}

impl Fixture {
    async fn new() -> Self {
        let root =
            std::env::temp_dir().join(format!("tachyon-responsive-{}", uuid::Uuid::now_v7()));
        let workspace = root.join("ws");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("source.txt"), b"original").unwrap();
        let context = Arc::new(ToolsContext::new(
            workspace,
            Policy::trusted_workspace(),
            ArtifactSpool::new(root.join("artifacts")),
        ));
        std::fs::create_dir_all(root.join("state")).unwrap();
        let store = Arc::new(StoreWriter::open(&root.join("state")).await.unwrap());
        let session = SessionId::generate();
        store.create_session(&session.to_string()).await.unwrap();
        let task = create_task(
            session,
            WorkspaceId::generate(),
            "responsive".into(),
            store.clone(),
        )
        .await
        .unwrap();
        Self {
            root,
            context,
            store,
            task,
        }
    }

    fn contract() -> AcceptanceContract {
        AcceptanceContract {
            clauses: vec![Clause::FileUnchanged {
                path: "source.txt".into(),
            }],
        }
    }

    async fn close(self) {
        self.task.shutdown().await.unwrap();
        self.store.close().await;
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

async fn poll_pending(future: std::pin::Pin<&mut impl std::future::Future>) {
    let mut future = future;
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending()))
            .await
    );
}

/// One poll: true when the future is still pending, without waiting on it.
/// Unix-only today: the Windows path probes liveness through the child PID.
#[cfg(unix)]
async fn still_pending(future: std::pin::Pin<&mut impl std::future::Future>) -> bool {
    let mut future = future;
    std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

/// Waits, without a fixed sleep, until the durable row records `status`.
async fn durable_status(store: &StoreWriter, id: TaskId, status: &str) -> bool {
    for _ in 0..4_000 {
        let row = store.load_task(&id.to_string()).await.unwrap().unwrap();
        if row.status == status {
            return true;
        }
        tokio::task::yield_now().await;
    }
    false
}

#[tokio::test]
async fn held_lease_stalls_baseline_not_mailbox_or_steering() {
    let f = Fixture::new().await;
    let lease = WorkspaceLease::acquire(&f.context.workspace_root, &CancellationToken::new())
        .await
        .unwrap();
    let mut configure = Box::pin(f.task.configure_verification(
        f.context.clone(),
        Fixture::contract(),
        VerificationRisk::Affected,
    ));
    // Polling enqueues Configure ahead of GetState, without relying on a sleep.
    poll_pending(configure.as_mut()).await;
    let state = tokio::time::timeout(Duration::from_secs(2), f.task.get_state()).await;
    let unbound_while_held = state
        .as_ref()
        .ok()
        .and_then(|s| s.as_ref().ok())
        .is_some_and(|s| s.verification.is_none());
    let steering = tokio::time::timeout(
        Duration::from_secs(2),
        f.task
            .add_constraint("keep policy".into(), ConstraintStrength::Preference),
    )
    .await;
    drop(lease);
    let configured = tokio::time::timeout(Duration::from_secs(2), configure)
        .await
        .unwrap();
    let final_state = f.task.get_state().await.unwrap();
    f.close().await;
    assert!(
        unbound_while_held,
        "baseline bypassed the shared workspace lease or blocked GetState: {state:?}"
    );
    assert!(
        steering.unwrap().is_ok(),
        "steering did not respond while baseline waited"
    );
    assert!(
        configured.is_err(),
        "stale baseline configured acceptance after steering"
    );
    assert!(final_state.verification.is_none());
    assert_eq!(final_state.revision, 1);
}

#[tokio::test]
async fn held_lease_stalls_planner_not_mailbox_or_steering() {
    let f = Fixture::new().await;
    f.task
        .configure_verification(
            f.context.clone(),
            Fixture::contract(),
            VerificationRisk::Affected,
        )
        .await
        .unwrap();
    let lease = WorkspaceLease::acquire(&f.context.workspace_root, &CancellationToken::new())
        .await
        .unwrap();
    let mut verify = Box::pin(f.task.verify_and_complete(f.context.clone()));
    poll_pending(verify.as_mut()).await;
    let observed = tokio::time::timeout(Duration::from_secs(2), f.task.get_state())
        .await
        .expect("GetState blocked while the planner waited for the workspace lease")
        .unwrap();
    let steered = tokio::time::timeout(
        Duration::from_secs(2),
        f.task.add_message("stop this plan".into()),
    )
    .await
    .expect("steering blocked while the planner waited for the workspace lease")
    .unwrap();
    drop(lease);
    let refused = tokio::time::timeout(Duration::from_secs(5), verify)
        .await
        .unwrap();
    let final_state = f.task.get_state().await.unwrap();
    let spawned_process = f.context.workspace_root.join("target").exists();
    f.close().await;
    assert_eq!(
        observed.status,
        TaskStatus::Created,
        "a verification stage ran while a competing workspace lease was held"
    );
    // Acceptance binding bumped the revision once; steering bumps it again.
    assert_eq!(steered.revision, 2);
    assert!(
        refused.is_err(),
        "a plan produced under a held lease was accepted"
    );
    assert_ne!(final_state.status, TaskStatus::Verifying);
    assert!(
        !spawned_process,
        "a verifier process ran under a held lease"
    );
}

#[tokio::test]
async fn cancel_acknowledges_after_real_reap_while_the_mailbox_serves() {
    #[cfg(unix)]
    use tokio::io::AsyncBufReadExt as _;
    #[cfg(unix)]
    use tokio::io::AsyncReadExt as _;
    let mut f = Fixture::new().await;
    let mut policy = Policy::trusted_workspace();
    policy.allow("verify.command", "workspace/**");
    // Unix: a python child with a loopback readiness handshake. Windows: a
    // PowerShell sleeper publishing its PID to a file — python-on-Windows
    // process/socket lifetime proved too flaky to observe a reap through.
    #[cfg(unix)]
    policy.allow("process.spawn", "python3");
    #[cfg(windows)]
    policy.allow("process.spawn", "powershell");
    Arc::get_mut(&mut f.context).unwrap().policy = policy;
    // A loopback readiness handshake proves the real command is alive; no sleep
    // or marker polling. Dropping the stream unblocks it if the test fails.
    #[cfg(unix)]
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    #[cfg(unix)]
    let port = listener.local_addr().unwrap().port();
    #[cfg(unix)]
    let command = tachyon_verify::CommandCheck {
        program: "python3".into(),
        args: vec![
            "-c".into(),
            "import os,signal,socket,sys; signal.signal(signal.SIGTERM, signal.SIG_IGN); \
             s=socket.create_connection(('127.0.0.1',int(sys.argv[1]))); \
             s.sendall((str(os.getpid())+'\\n').encode()); s.recv(1)"
                .into(),
            port.to_string(),
        ],
        cwd: ".".into(),
        env: BTreeMap::new(),
        timeout_ms: 30_000,
    };
    #[cfg(windows)]
    let command = {
        tachyon_verify::CommandCheck {
            program: "powershell".into(),
            args: vec![
                "-NoProfile".into(),
                "-Command".into(),
                "New-Item -ItemType Directory -Force target | Out-Null; [System.IO.File]::WriteAllText('target/pid', \"$PID\"); Start-Sleep 60".into(),
            ],
            cwd: ".".into(),
            env: BTreeMap::new(),
            // The sleeper writes its PID and then sleeps 60 s, so the
            // command outlives any readiness window we are willing to
            // wait for; the old 30 s killed a slow cold start before it
            // could publish anything.
            timeout_ms: 120_000,
        }
    };
    f.task
        .configure_verification(
            f.context.clone(),
            AcceptanceContract {
                clauses: vec![Clause::CommandPasses { command }],
            },
            VerificationRisk::Affected,
        )
        .await
        .unwrap();
    let handle = f.task.clone();
    let context = f.context.clone();
    // `mut` serves the Unix readiness select below; Windows only polls it.
    #[cfg_attr(windows, allow(unused_mut))]
    let mut pending = tokio::spawn(async move { handle.verify_and_complete(context).await });
    #[cfg(unix)]
    let mut eof = {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                result = &mut pending => panic!("verifier exited before readiness: {result:?}"),
                accepted = listener.accept() => accepted.unwrap(),
            }
        })
        .await
        .unwrap();
        let mut stream = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let _: u32 = line.trim().parse().unwrap();
        // Portable child-liveness probe: the child never sends again, so a read
        // pends while it lives and resolves EOF once it is reaped. No /proc.
        let mut eof = Box::pin(async move {
            loop {
                let mut byte = [0u8; 1];
                match stream.read(&mut byte).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => panic!("liveness probe failed: {error}"),
                }
            }
        });
        assert!(still_pending(eof.as_mut()).await);
        eof
    };
    // Windows readiness and liveness via PID file: the sleeper publishes $PID,
    // and the PID cannot be recycled while the leader is unreaped.
    #[cfg(windows)]
    let child_pid: u32 = {
        let pidfile = f.root.join("ws/target/pid");
        // A cold PowerShell on a loaded runner can take tens of seconds
        // to reach `-Command`; this window is bounded by the sleeper's
        // own 60 s sleep (it publishes the PID before sleeping), not by
        // a guess at startup cost.
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if pidfile.exists() {
                    break;
                }
                assert!(!pending.is_finished(), "verifier exited before readiness");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("verifier child never published its PID");
        let text = std::fs::read_to_string(&pidfile).unwrap();
        let pid: u32 = text.trim().parse().expect("child pid");
        assert!(!pid_dead(pid), "the child died before the cancel probe");
        pid
    };

    let mut cancel = Box::pin(f.task.cancel());
    poll_pending(cancel.as_mut()).await;
    // The intent must be durable — a liveness claim about the journal alone.
    // `control()` fires the cancel token before it journals the `Cancelled`
    // transition, so the real reap can finish while that write is still
    // committing on a loaded runner: "already reaped when the row was
    // observed durable" is a legitimate interleaving, not a defect, and is
    // deliberately not asserted here. The orderings below read current
    // state instead of comparing two observation times.
    assert!(
        durable_status(&f.store, f.task.task_id(), "Cancelled").await,
        "the durable Cancelled intent never appeared"
    );
    // Reads stay serviceable while that acknowledgement waits for real cleanup.
    let observed = tokio::time::timeout(Duration::from_secs(2), f.task.get_state())
        .await
        .expect("GetState blocked while cancellation drained a real process")
        .unwrap();
    assert_eq!(observed.status, TaskStatus::Cancelled);
    let acked = tokio::time::timeout(Duration::from_secs(30), cancel)
        .await
        .expect("the cancel acknowledgement never arrived")
        .unwrap();
    // The acknowledgement is answered only after every owned effect worker
    // has drained, so at the instant it is observed the real child must
    // already be gone. This reads current state (socket EOF / process
    // handle), never the gap between two observations, so scheduler delay
    // cannot fail it — yet a child that is still live here proves the
    // acknowledgement outran the reap.
    #[cfg(unix)]
    assert!(
        !still_pending(eof.as_mut()).await,
        "the cancel was acknowledged while the child still lived"
    );
    #[cfg(windows)]
    assert!(
        wait_pid_dead(child_pid).await,
        "the cancel was acknowledged while the child still lived"
    );
    let refused = pending.await.unwrap();
    assert_eq!(acked.status, TaskStatus::Cancelled);
    assert!(refused.is_err(), "pending work survived cancellation");
    f.close().await;
}

#[tokio::test]
async fn owner_shutdown_cancels_parked_work_and_a_dropped_control_waiter() {
    let f = Fixture::new().await;
    f.task
        .configure_verification(
            f.context.clone(),
            Fixture::contract(),
            VerificationRisk::Affected,
        )
        .await
        .unwrap();
    let lease = WorkspaceLease::acquire(&f.context.workspace_root, &CancellationToken::new())
        .await
        .unwrap();
    let mut verify = Box::pin(f.task.verify_and_complete(f.context.clone()));
    poll_pending(verify.as_mut()).await;
    let mut pause = Box::pin(f.task.pause());
    poll_pending(pause.as_mut()).await;
    // The caller walks away before any acknowledgement arrives; cleanup must
    // still finish and durable admission must still be released.
    drop(pause);
    tokio::time::timeout(Duration::from_secs(10), f.task.shutdown())
        .await
        .expect("shutdown deadlocked behind parked work or a dropped waiter")
        .unwrap();
    let refused = verify.await;
    assert!(refused.is_err(), "pending work survived owner shutdown");
    let recovered = tokio::time::timeout(
        Duration::from_secs(10),
        recover_task(f.task.task_id(), f.store.clone()),
    )
    .await
    .expect("owner shutdown did not release durable admission")
    .unwrap();
    let state = recovered.get_state().await.unwrap();
    assert!(!state.verification.is_some_and(|v| v.in_progress));
    assert_ne!(state.status, TaskStatus::Verifying);
    assert!(!f.context.workspace_root.join("target").exists());
    drop(lease);
    recovered.shutdown().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn burst_beyond_mailbox_capacity_loses_no_command() {
    // G5 full-channel barrier on the production path: far more concurrent
    // senders than SUPERVISOR_MAILBOX slots. Sends select on shutdown, the
    // actor drains, and every command is answered — no deadlock, no loss.
    let f = Fixture::new().await;
    let mut senders = Vec::new();
    for _ in 0..600 {
        let handle = f.task.clone();
        senders.push(tokio::spawn(async move { handle.get_state().await }));
    }
    let mut ok = 0;
    for sender in senders {
        let state = tokio::time::timeout(Duration::from_secs(30), sender)
            .await
            .expect("a burst sender never resolved past a full mailbox")
            .unwrap()
            .expect("a burst command was lost past a full mailbox");
        assert_eq!(state.status, TaskStatus::Created);
        ok += 1;
    }
    assert_eq!(ok, 600);
    f.close().await;
}
