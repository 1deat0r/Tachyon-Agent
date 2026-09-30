//! ADR-0006 release gate, crash half: a process killed inside a
//! Supervisor-owned evidence read must recover by journaling a
//! generation-interrupted event — unfinished nodes become `Cancelled`,
//! the active pointer clears, already allocated generation numbers stay
//! allocated, and explicit re-entry reads under a NEW generation rather
//! than resuming the old one.
//!
//! The seam is the M12 `evidence.read` fault point, armed in a child
//! process exactly like the M12 effect-barrier kill tests.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tachyon_core::evidence::EvidenceStageError;
use tachyon_core::runtime::{EvidenceRequest, RuntimeBounds};
use tachyon_core::{TaskStatus, create_task, recover_task};
use tachyon_ir::NodeStatus;
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::ToolsContext;
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_types::{SessionId, TaskId, WorkspaceId};
use tokio_util::sync::CancellationToken;

const ARMED: &str = "TACHYON_EVIDENCE_CRASH_CHILD";
const CHILD_DIR: &str = "TACHYON_EVIDENCE_CRASH_DIR";
const SEAM: &str = "evidence.read";
const RUN_ID: &str = "run-crash";

fn request(path: &str) -> EvidenceRequest {
    EvidenceRequest {
        capability: "fs.read".to_owned(),
        path: path.to_owned(),
    }
}

/// Child body: reach the armed evidence seam and park there forever.
fn crash_child(dir: &Path) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let ws = dir.join("ws");
        std::fs::create_dir_all(ws.join("src")).unwrap();
        std::fs::write(ws.join("src/a.rs"), "fn a() {}\n").unwrap();
        std::fs::create_dir_all(dir.join("state")).unwrap();
        let store = Arc::new(StoreWriter::open(&dir.join("state")).await.unwrap());
        let session = SessionId::generate();
        store.create_session(&session.to_string()).await.unwrap();
        let handle = create_task(
            session,
            WorkspaceId::generate(),
            "crash mid evidence read".to_owned(),
            store,
        )
        .await
        .unwrap();
        std::fs::write(dir.join("task-id"), handle.task_id().to_string()).unwrap();
        handle.start_run(RUN_ID.to_owned(), 0).await.unwrap();
        let context = Arc::new(ToolsContext::new(
            ws,
            Policy::trusted_workspace(),
            ArtifactSpool::new(dir.join("artifacts")),
        ));
        let _ = handle
            .collect_evidence(
                RUN_ID.to_owned(),
                0,
                vec![request("src/a.rs")],
                RuntimeBounds::default(),
                false,
                Instant::now(),
                CancellationToken::new(),
                context,
            )
            .await;
        // Unreachable while the seam is armed; a clean exit if it was not.
        std::process::exit(0);
    });
}

/// Spawns the child, waits until it is parked on the armed seam, then
/// SIGKILLs it — the process-death half of the gate.
fn arm_child_and_kill(dir: &Path) {
    let marker = dir.join("reached");
    let exe = std::env::current_exe().expect("current exe");
    let mut child = Command::new(&exe)
        .args([
            "--exact",
            "crash_mid_evidence_read_recovers_by_interrupting_the_generation",
            "--nocapture",
        ])
        .env(ARMED, "1")
        .env(CHILD_DIR, dir)
        .env("TACHYON_FAULT_POINT", SEAM)
        .env("TACHYON_FAULT_REACHED_FILE", &marker)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn armed child");

    let started = Instant::now();
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("evidence crash child exited before {SEAM}: {status}");
        }
        if started.elapsed() > Duration::from_secs(20) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not reach the {SEAM} seam");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), SEAM);
    assert!(
        child.try_wait().unwrap().is_none(),
        "child must be parked at the seam"
    );
    child.kill().unwrap();
    assert!(
        !child.wait().unwrap().success(),
        "SIGKILL must not look clean"
    );
}

/// Reopens the store, recovers the task and asserts the interrupted
/// generation was closed rather than resumed.
async fn recover_and_reenter(dir: &Path) {
    let task_id: TaskId = std::fs::read_to_string(dir.join("task-id"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let store = Arc::new(StoreWriter::open(&dir.join("state")).await.unwrap());
    let recovered = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered.get_state().await.unwrap();

    assert_eq!(
        state.status,
        TaskStatus::Recovering,
        "an interrupted generation lands the task in Recovering"
    );
    assert_eq!(
        state.execution_generation, None,
        "the unsettled generation's active pointer is cleared"
    );
    assert_eq!(
        state.next_execution_generation, 2,
        "the generation was durably allocated before dispatch, so it is never reused"
    );
    assert!(
        state.evidence_receipts.is_empty(),
        "a crash before the success receipt leaves no usable output"
    );
    assert!(
        !state.node_statuses.is_empty(),
        "the accepted graph's node states were journalled"
    );
    assert!(
        state
            .node_statuses
            .values()
            .all(|status| *status == NodeStatus::Cancelled),
        "every unfinished node of that generation is Cancelled: {:?}",
        state.node_statuses
    );

    // Explicit re-entry: a fresh run, a fresh generation, freshly opened
    // targets — never a reuse of the interrupted one.
    recovered
        .start_run("run-after-crash".to_owned(), 0)
        .await
        .unwrap();
    let context = Arc::new(ToolsContext::new(
        dir.join("ws"),
        Policy::trusted_workspace(),
        ArtifactSpool::new(dir.join("artifacts")),
    ));
    let batch = recovered
        .collect_evidence(
            "run-after-crash".to_owned(),
            0,
            vec![request("src/a.rs")],
            RuntimeBounds::default(),
            false,
            Instant::now(),
            CancellationToken::new(),
            context,
        )
        .await
        .unwrap_or_else(|err: EvidenceStageError| {
            panic!("re-entry must collect under a new generation: {err:?}")
        });
    assert_eq!(batch.receipts.len(), 1);
    assert_eq!(
        batch.receipts[0].generation, 2,
        "re-entry is a new generation"
    );
    let state = recovered.get_state().await.unwrap();
    assert_eq!(state.execution_generation, Some(2));
    assert_eq!(state.evidence_receipts.len(), 1);

    recovered.shutdown().await.unwrap();
    store.close().await;
}

#[test]
fn crash_mid_evidence_read_recovers_by_interrupting_the_generation() {
    if std::env::var(ARMED).is_ok() {
        crash_child(Path::new(&std::env::var(CHILD_DIR).unwrap()));
        return;
    }

    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir: PathBuf = std::env::temp_dir().join(format!(
        "tachyon-evidence-crash-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    arm_child_and_kill(&dir);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(recover_and_reenter(&dir));
    let _ = std::fs::remove_dir_all(&dir);
}

static COUNTER: AtomicU64 = AtomicU64::new(0);
