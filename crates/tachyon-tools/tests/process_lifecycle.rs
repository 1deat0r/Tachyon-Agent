//! Actual Unix process trees, bounded even when the runner is broken.
#![cfg(unix)]

#[cfg(target_os = "linux")]
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tachyon_policy::{DefaultPosture, Policy};
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_tools::process::{self, ProcessSpec};
use tachyon_tools::{ToolError, ToolsContext};
use tokio_util::sync::CancellationToken;

struct TreeFixture(PathBuf);

impl TreeFixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let root = std::env::temp_dir().join(format!(
                "tachyon-tree-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::create_dir(&root) {
                Ok(()) => return Self(root),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("create fixture: {error}"),
            }
        }
    }

    fn context(&self) -> ToolsContext {
        let mut policy = Policy::new(DefaultPosture::Deny);
        policy.allow("process.spawn", "/bin/sh");
        ToolsContext::new(
            self.0.clone(),
            policy,
            ArtifactSpool::new(self.0.join("artifacts")),
        )
    }

    fn spec(&self) -> ProcessSpec {
        let mut spec = ProcessSpec::new("/bin/sh");
        // All three processes ignore TERM to exercise forced group cleanup.
        // The deepest child exits on its own after 8s if any test fails.
        spec.args = vec![
            "-c".to_owned(),
            r#"
            trap '' TERM
            printf '%s\n' $$ > leader
            /bin/sh -c 'printf "%s\n" $$ > child; sleep 8 & printf "%s\n" $! > grandchild; wait' &
            wait
        "#
            .to_owned(),
        ];
        spec.cwd = Some(self.0.clone());
        spec.timeout = Duration::from_millis(300);
        spec
    }

    fn pids(&self) -> Vec<i32> {
        ["leader", "child", "grandchild"]
            .iter()
            .filter_map(|name| {
                std::fs::read_to_string(self.0.join(name))
                    .ok()?
                    .trim()
                    .parse()
                    .ok()
            })
            .collect()
    }

    async fn wait_started(&self) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.pids().len() != 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(self.pids().iter().all(|pid| alive(*pid)));
        })
        .await
        .expect("fixture did not start");
    }

    async fn assert_stopped(&self) {
        let pids = self.pids();
        assert_eq!(pids.len(), 3, "fixture did not start its full tree");
        let stopped = tokio::time::timeout(Duration::from_secs(1), async {
            while pids.iter().any(|pid| alive(*pid)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(stopped.is_ok(), "owned processes survived: {pids:?}");
    }
}

#[allow(unsafe_code)]
fn alive(pid: i32) -> bool {
    assert!(pid > 1);
    #[cfg(target_os = "linux")]
    {
        // Zombies are terminated, not surviving processes. The immediate child
        // is additionally checked for reaping in tests that can await cleanup.
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        stat.rsplit_once(") ")
            .is_some_and(|(_, tail)| !tail.starts_with('Z'))
    }
    #[cfg(not(target_os = "linux"))]
    // SAFETY: signal 0 only probes the positive PID recorded by our fixture.
    unsafe {
        libc::kill(pid, 0) == 0
    }
}

impl Drop for TreeFixture {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // Emergency cleanup executes even on a failing red assertion. Kill
        // descendants first; every fixture also has an 8s natural lifetime.
        for pid in self.pids().into_iter().rev().filter(|pid| alive(*pid)) {
            #[cfg(target_os = "linux")]
            if std::fs::read_link(format!("/proc/{pid}/cwd"))
                .ok()
                .as_deref()
                != Some(self.0.as_path())
            {
                continue;
            }
            // SAFETY: positive PID from our private fixture directory; Linux
            // also checks cwd to avoid signalling an unrelated reused PID.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn dropping_running_future_terminates_tree() {
    let fixture = TreeFixture::new();
    let context = fixture.context();
    let mut spec = fixture.spec();
    spec.timeout = Duration::from_secs(5);
    let mut running = Box::pin(process::run(&context, &spec));
    tokio::select! {
        result = &mut running => panic!("runner exited before drop: {result:?}"),
        () = fixture.wait_started() => {},
    }
    drop(running);
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn aborting_running_task_terminates_tree() {
    let fixture = TreeFixture::new();
    let context = fixture.context();
    let mut spec = fixture.spec();
    spec.timeout = Duration::from_secs(5);
    let running = tokio::spawn(async move { process::run(&context, &spec).await });
    fixture.wait_started().await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn timeout_covers_inherited_pipes_after_leader_exit() {
    for redirect in [":", "exec 1>closed-stdout", "exec 2>closed-stderr"] {
        let fixture = TreeFixture::new();
        let context = fixture.context();
        let mut spec = fixture.spec();
        // The leader exits, but a grandchild holds stdout, stderr, or both.

        spec.args[1] = format!(
            r#"
            trap '' TERM
            printf '%s\n' $$ > leader
            /bin/sh -c '{redirect}; printf "%s\n" $$ > child; sleep 8 & printf "%s\n" $! > grandchild; wait' &
            exit 0
        "#
        );
        let result = tokio::time::timeout(Duration::from_secs(2), process::run(&context, &spec))
            .await
            .expect("inherited pipe escaped the process timeout");
        assert!(
            matches!(result, Err(ToolError::ProcessTimeout(_))),
            "{result:?}"
        );
        fixture.assert_stopped().await;
        #[cfg(target_os = "linux")]
        assert!(!Path::new(&format!("/proc/{}", fixture.pids()[0])).exists());
    }
}

#[tokio::test]
async fn termination_allows_term_handler_before_force_kill() {
    let fixture = TreeFixture::new();
    let context = fixture.context();
    let mut spec = fixture.spec();
    spec.args[1] = spec.args[1].replace(
        "trap '' TERM",
        "trap 'printf graceful > term-seen; exit 0' TERM",
    );
    let result = tokio::time::timeout(Duration::from_secs(2), process::run(&context, &spec))
        .await
        .unwrap();
    assert!(
        matches!(result, Err(ToolError::ProcessTimeout(_))),
        "{result:?}"
    );
    assert_eq!(
        std::fs::read(fixture.0.join("term-seen")).unwrap(),
        b"graceful"
    );
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn cancellation_terminates_actual_child_and_grandchild() {
    let fixture = TreeFixture::new();
    let context = fixture.context();
    let mut spec = fixture.spec();
    spec.timeout = Duration::from_secs(5);
    let cancel = CancellationToken::new();
    let mut running = Box::pin(process::run_cancellable(&context, &spec, cancel.clone()));
    tokio::select! {
        result = &mut running => panic!("runner exited before cancellation: {result:?}"),
        () = fixture.wait_started() => {},
    }
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap();
    assert!(
        matches!(result, Err(ToolError::ProcessCancelled)),
        "{result:?}"
    );
    fixture.assert_stopped().await;
    #[cfg(target_os = "linux")]
    assert!(!Path::new(&format!("/proc/{}", fixture.pids()[0])).exists());
}

#[tokio::test]
async fn precancellation_never_spawns() {
    let fixture = TreeFixture::new();
    let context = fixture.context();
    let mut spec = fixture.spec();
    spec.args = vec!["-c".to_owned(), "printf started > spawned".to_owned()];
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = process::run_cancellable(&context, &spec, cancel.clone()).await;
    assert!(
        matches!(result, Err(ToolError::ProcessCancelledBeforeStart)),
        "{result:?}"
    );
    assert!(!fixture.0.join("spawned").exists());
    // Even invalid preflight inputs must be bypassed for an already-cancelled
    // request, rather than attempting a spawn and then killing it.
    spec.program = "tachyon-fixture-no-such-program".to_owned();
    spec.cwd = Some(fixture.0.join("missing"));
    let result = process::run_cancellable(&context, &spec, cancel).await;
    assert!(
        matches!(result, Err(ToolError::ProcessCancelledBeforeStart)),
        "{result:?}"
    );
}

#[tokio::test]
async fn timeout_terminates_actual_child_and_grandchild() {
    let fixture = TreeFixture::new();
    let context = fixture.context();
    let spec = fixture.spec();
    let result = tokio::time::timeout(Duration::from_secs(2), process::run(&context, &spec))
        .await
        .expect("process runner exceeded hard test deadline");
    assert!(
        matches!(result, Err(ToolError::ProcessTimeout(_))),
        "{result:?}"
    );
    fixture.assert_stopped().await;
    #[cfg(target_os = "linux")]
    assert!(
        !Path::new(&format!("/proc/{}", fixture.pids()[0])).exists(),
        "leader was not reaped"
    );
}
