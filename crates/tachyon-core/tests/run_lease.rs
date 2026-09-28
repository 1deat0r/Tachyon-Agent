//! M11 slice 1 (workspace lease): the run path holds the `WorkspaceLease`
//! on the pinned canonical root from before any workspace-touching
//! stage through `drive()` completion, and drive-reachable inner
//! acquisitions reuse the run-held lease instead of re-acquiring the
//! non-reentrant lock (which would self-deadlock). The guard is owned
//! by the `ToolsContext` the run carries, so exclusion lasts exactly as
//! long as the run's context and no longer.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use tachyon_core::create_task;
use tachyon_core::driver::{DriveHost, EvidenceMode, RunPlan, TaskModelContext, drive};
use tachyon_models::fake::{FakeModelProvider, FakeResponse};
use tachyon_mutation::blake3_hex;
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::workspace::WorkspaceLease;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::{ProviderId, SessionId, WorkspaceId};
use tachyon_verify::{AcceptanceContract, Clause, VerificationRisk};

use tachyon_core::runtime::{EvidenceRequest, RuntimeBounds};

static COUNTER: AtomicU64 = AtomicU64::new(0);

const TARGET: &str = "src/lib.rs";
const BROKEN: &str = "pub fn answer() -> u8 { 7 }\\n";
const FIXED: &str = "pub fn answer() -> u8 { 42 }\\n";

/// A run whose context carries the workspace lease must complete the
/// whole shared path — evidence, mutation and VERIFICATION — without a
/// single self-deadlock, because every drive-reachable inner acquisition
/// (verification.rs capture/plan, verify runner) reuses the attached
/// guard. The lease releases exactly when the last owner of the context
/// drops it, proving the guard travels with the run and is not leaked
/// into the process-wide registry.
#[tokio::test]
async fn run_held_lease_passes_through_verification_without_self_deadlock() {
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("tachyon-run-lease-{}-{id}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let ws = dir.join("ws");
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::create_dir_all(ws.join("notes")).unwrap();
    std::fs::write(ws.join(TARGET), BROKEN).unwrap();
    std::fs::write(ws.join("notes/readme.txt"), "keep me\\n").unwrap();

    let canonical = std::fs::canonicalize(&ws).unwrap();

    // The host takes the lease the gateway's prepare takes: typed,
    // non-blocking, on the canonical root.
    let lease = WorkspaceLease::try_acquire(&canonical)
        .await
        .unwrap()
        .expect("the workspace must be free before the run");

    let mut policy = Policy::trusted_workspace();
    policy.allow("mutation.patch", "workspace/**");
    policy.allow("fs.delete", "workspace/**");
    let context = Arc::new(
        ToolsContext::new(
            canonical.clone(),
            policy,
            ArtifactSpool::new(dir.join("artifacts")),
        )
        .with_workspace_lease(lease),
    );

    std::fs::create_dir_all(dir.join("state")).unwrap();
    std::fs::create_dir_all(dir.join("mutation-state")).unwrap();
    let store = Arc::new(StoreWriter::open(&dir.join("state")).await.unwrap());
    let session = SessionId::generate();
    store.create_session(&session.to_string()).await.unwrap();
    let task = create_task(
        session,
        WorkspaceId::generate(),
        "Fix the wrong answer".to_owned(),
        store.clone(),
    )
    .await
    .unwrap();
    let task_id = task.task_id();

    let provider = Arc::new(FakeModelProvider::new(ProviderId("bench-script".into())));
    let script = serde_json::json!({
        "decision": "propose_execution",
        "operations": [{
            "capability": "mutation.patch",
            "reason": "Correct the return value to satisfy the task objective",
            "args": {
                "path": TARGET,
                "base_hash": blake3_hex(BROKEN.as_bytes()),
                "new_content": FIXED,
            }
        }]
    });
    provider.push_response(FakeResponse {
        text: script.to_string(),
        decision: serde_json::from_value(script.clone()).expect("typed proposal fixture"),
        input_tokens: 0,
        output_tokens: 0,
    });

    let plan = RunPlan {
        origin: Instant::now(),
        evidence_mode: EvidenceMode::Serial,
        evidence: vec![EvidenceRequest {
            capability: "fs.read".to_owned(),
            path: TARGET.to_owned(),
        }],
        contract: AcceptanceContract {
            clauses: vec![Clause::ChangedPathsWithin {
                paths: vec![TARGET.to_owned()],
            }],
        },
        risk: VerificationRisk::Affected,
        mutation_dir: dir.join("mutation-state"),
        batch_id: "run-lease-batch-1".to_owned(),
        model: "scripted-replay-1".to_owned(),
        task_context: TaskModelContext::from_task(&task.get_state().await.unwrap()),
        requested_checks: Vec::new(),
        available_checks: Vec::new(),
        bounds: RuntimeBounds::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };

    let host = DriveHost::Supervisor {
        handle: task,
        store: store.clone(),
    };

    // The non-reentrancy proof: with the lease attached, the run's
    // inner acquisitions must pass through instead of acquiring. A
    // re-acquiring inner path would block forever and this timeout
    // would fire.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(90),
        drive(host, context.clone(), provider, plan),
    )
    .await
    .expect("drive self-deadlocked on the run-held workspace lease")
    .unwrap();
    assert_eq!(outcome.outcome.as_deref(), Some("completed"));
    assert_eq!(
        outcome.task_id.as_deref(),
        Some(task_id.to_string().as_str()),
        "the completed run belongs to the created task"
    );

    // While the host still owns the context, the run's lease is held:
    // no other holder can acquire the canonical root.
    assert!(
        WorkspaceLease::try_acquire(&canonical)
            .await
            .unwrap()
            .is_none(),
        "the context's lease must keep the canonical root excluded"
    );

    // The guard lives with the context, not the registry: dropping the
    // last owner releases it.
    drop(context);
    assert!(
        WorkspaceLease::try_acquire(&canonical)
            .await
            .unwrap()
            .is_some(),
        "the lease must release with the last context owner"
    );

    store.close().await;
    let mut scratch = PathBuf::from(&dir);
    scratch.pop();
    let _ = std::fs::remove_dir_all(&dir);
}
