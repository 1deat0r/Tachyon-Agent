//! ADR-0006 release gate for the Supervisor-owned evidence stage: the
//! typed command's fences, target preparation that fails closed, the one
//! shared linearizable stage byte budget, durable receipts with
//! receipt-scoped retrieval, and generation lifecycle (accept → commit →
//! a later generation) — each asserted against the durable journal, not
//! against a return value alone.
//!
//! The crash half of the gate lives in `evidence_crash.rs`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tachyon_core::evidence::EvidenceStageError;
use tachyon_core::runtime::{
    EvidenceRequest, RuntimeBounds, RuntimeError, hash_bytes, max_overlap,
};
use tachyon_core::{CoreError, create_task};
use tachyon_ir::NodeStatus;
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::ToolsContext;
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_types::{NodeId, SessionId, WorkspaceId};
use tokio_util::sync::CancellationToken;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// One workspace + task + run, ready to accept evidence generations.
struct Harness {
    dir: PathBuf,
    ws: PathBuf,
    handle: tachyon_core::SupervisorHandle,
    context: Arc<ToolsContext>,
    run_id: String,
}

fn request(path: &str) -> EvidenceRequest {
    EvidenceRequest {
        capability: "fs.read".to_owned(),
        path: path.to_owned(),
    }
}

async fn harness(label: &str) -> Harness {
    harness_inner(label, true).await
}

/// A task with no acknowledged run: its evidence fence must refuse.
async fn harness_idle(label: &str) -> Harness {
    harness_inner(label, false).await
}

async fn harness_inner(label: &str, start_run: bool) -> Harness {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "tachyon-evidence-{label}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let ws = dir.join("ws");
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::create_dir_all(ws.join("notes")).unwrap();
    std::fs::write(ws.join("src/a.rs"), "fn a() {}\n").unwrap();
    std::fs::write(ws.join("src/b.rs"), "fn b() {}\n").unwrap();
    std::fs::write(ws.join("src/c.rs"), "fn c() {}\n").unwrap();
    std::fs::write(ws.join("notes/d.txt"), "delta\n").unwrap();
    std::fs::create_dir_all(dir.join("state")).unwrap();

    let store = Arc::new(StoreWriter::open(&dir.join("state")).await.unwrap());
    let session = SessionId::generate();
    store.create_session(&session.to_string()).await.unwrap();
    let handle = create_task(
        session,
        WorkspaceId::generate(),
        "collect evidence".to_owned(),
        store,
    )
    .await
    .unwrap();
    let context = Arc::new(ToolsContext::new(
        ws.clone(),
        Policy::trusted_workspace(),
        ArtifactSpool::new(dir.join("artifacts")),
    ));
    let run_id = "run-evidence-test".to_owned();
    if start_run {
        handle.start_run(run_id.clone(), 0).await.unwrap();
    }
    Harness {
        dir,
        ws,
        handle,
        context,
        run_id,
    }
}

impl Harness {
    async fn collect_with(
        &self,
        run_id: &str,
        revision: u64,
        requests: Vec<EvidenceRequest>,
        bounds: RuntimeBounds,
        concurrent: bool,
        cancel: CancellationToken,
    ) -> Result<tachyon_core::EvidenceBatch, EvidenceStageError> {
        self.handle
            .collect_evidence(
                run_id.to_owned(),
                revision,
                requests,
                bounds,
                concurrent,
                Instant::now(),
                cancel,
                self.context.clone(),
            )
            .await
    }

    async fn collect(
        &self,
        requests: Vec<EvidenceRequest>,
        bounds: RuntimeBounds,
        concurrent: bool,
    ) -> Result<tachyon_core::EvidenceBatch, EvidenceStageError> {
        self.collect_with(
            &self.run_id,
            0,
            requests,
            bounds,
            concurrent,
            CancellationToken::new(),
        )
        .await
    }

    fn artifact_blob(&self, id: &str) -> PathBuf {
        self.dir.join("artifacts").join(&id[..2]).join(id)
    }
}

#[tokio::test]
async fn collect_returns_verified_bytes_durable_receipts_and_real_overlap() {
    let h = harness("happy").await;
    let requests = vec![
        request("src/a.rs"),
        request("src/b.rs"),
        request("src/c.rs"),
        request("notes/d.txt"),
    ];
    let batch = h
        .collect(requests, RuntimeBounds::default(), true)
        .await
        .expect("the supervisor path must settle a valid batch");

    assert_eq!(batch.items.len(), 4, "every request is answered");
    let paths: Vec<&str> = batch.items.iter().map(|item| item.path.as_str()).collect();
    assert!(
        paths.windows(2).all(|w| w[0] <= w[1]),
        "items are sorted by canonical key: {paths:?}"
    );
    for item in &batch.items {
        let expected = std::fs::read(h.ws.join(&item.path)).unwrap();
        assert_eq!(item.bytes, expected, "bytes are what was read");
        assert_eq!(
            item.hash,
            hash_bytes(&expected),
            "the receipt's freshness hash rides on the item"
        );
    }
    assert!(
        max_overlap(&batch.intervals_us) >= 2,
        "concurrent mode must still measure real overlap: {:?}",
        batch.intervals_us
    );

    let state = h.handle.get_state().await.unwrap();
    assert_eq!(state.execution_generation, Some(1), "generation allocated");
    assert_eq!(state.next_execution_generation, 2, "counter advanced");
    assert_eq!(batch.receipts.len(), 4);
    for receipt in &batch.receipts {
        assert_eq!(receipt.generation, 1);
        assert_eq!(receipt.capability_version, 1);
        assert_eq!(
            state
                .evidence_receipts
                .get(&receipt.node_id)
                .map(|r| &r.artifact),
            Some(&receipt.artifact),
            "the receipt is journalled with its node success"
        );
        assert_eq!(
            state.node_statuses.get(&receipt.node_id),
            Some(&NodeStatus::Succeeded)
        );
    }
    assert_eq!(batch.graph_nodes, 4);
}

#[tokio::test]
async fn serial_mode_keeps_one_node_in_flight_and_still_commits() {
    let h = harness("serial").await;
    let batch = h
        .collect(
            vec![
                request("src/a.rs"),
                request("src/b.rs"),
                request("src/c.rs"),
            ],
            RuntimeBounds::default(),
            false,
        )
        .await
        .expect("serial collection settles");
    assert_eq!(batch.items.len(), 3);
    assert_eq!(
        max_overlap(&batch.intervals_us),
        1,
        "serial mode is one node at a time: {:?}",
        batch.intervals_us
    );
}

#[tokio::test]
async fn retrieval_requires_a_durable_receipt_and_verifies_content() {
    let h = harness("fetch").await;
    let batch = h
        .collect(vec![request("src/a.rs")], RuntimeBounds::default(), false)
        .await
        .expect("collect");
    let receipt = &batch.receipts[0];

    // Retrieval is bound to the task's durable workspace pin (ADR-0006
    // §13): a receipt alone, with no pinned workspace, is not enough.
    assert!(
        matches!(
            h.handle.fetch_evidence(receipt.node_id).await,
            Err(CoreError::EvidenceRetrievalUnavailable)
        ),
        "an unpinned task must not hand out evidence"
    );
    let canonical_ws = std::fs::canonicalize(&h.ws).unwrap();
    h.handle
        .pin_workspace_root(canonical_ws.display().to_string())
        .await
        .unwrap();

    let item = h
        .handle
        .fetch_evidence(receipt.node_id)
        .await
        .expect("receipt-scoped retrieval");
    assert_eq!(item.bytes, std::fs::read(h.ws.join("src/a.rs")).unwrap());

    // An artifact id alone grants nothing: an unknown node has no
    // receipt, so there is nothing to authorize the read.
    assert!(
        matches!(
            h.handle.fetch_evidence(NodeId::generate()).await,
            Err(CoreError::UnknownEvidenceReceipt { .. })
        ),
        "a node with no durable receipt must be refused"
    );

    // Corruption is caught before the bytes reach anything.
    std::fs::write(h.artifact_blob(&receipt.artifact.0), b"tampered").unwrap();
    assert!(
        h.handle.fetch_evidence(receipt.node_id).await.is_err(),
        "a blob that no longer matches its receipt must fail closed"
    );
}

#[tokio::test]
async fn preparation_fails_closed_before_any_generation_is_allocated() {
    let h = harness("prep").await;

    // A directory is not a regular file.
    let err = h
        .collect(vec![request("src")], RuntimeBounds::default(), false)
        .await
        .expect_err("a directory target must be refused");
    assert!(
        matches!(
            &err,
            EvidenceStageError::Stage(RuntimeError::StaleEvidence(_))
        ),
        "unexpected error: {err:?}"
    );

    // Path-size bound is checked before any path is opened.
    let tight = RuntimeBounds {
        max_evidence_path_bytes: 4,
        ..RuntimeBounds::default()
    };
    let err = h
        .collect(vec![request("src/a.rs")], tight, false)
        .await
        .expect_err("an over-long path must be refused");
    assert!(
        matches!(
            &err,
            EvidenceStageError::Stage(RuntimeError::InvalidArgs { .. })
        ),
        "unexpected error: {err:?}"
    );

    // Request-count bound is checked before any path is opened.
    let few = RuntimeBounds {
        max_evidence_requests: 1,
        ..RuntimeBounds::default()
    };
    let err = h
        .collect(vec![request("src/a.rs"), request("src/b.rs")], few, false)
        .await
        .expect_err("an over-long request list must be refused");
    assert!(
        matches!(
            &err,
            EvidenceStageError::Stage(RuntimeError::TooManyEvidence(2))
        ),
        "unexpected error: {err:?}"
    );

    let state = h.handle.get_state().await.unwrap();
    assert_eq!(
        state.execution_generation, None,
        "a refused preparation allocates no generation"
    );
    assert!(state.evidence_receipts.is_empty());
    assert!(!h.dir.join("artifacts").exists() || artifact_count(&h) == 0);
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_escapes_fail_closed() {
    let h = harness("symlink").await;
    std::os::unix::fs::symlink("/etc/hosts", h.ws.join("escape")).unwrap();
    let err = h
        .collect(vec![request("escape")], RuntimeBounds::default(), false)
        .await
        .expect_err("a symlink leaving the workspace must be refused");
    assert!(
        matches!(&err, EvidenceStageError::Stage(_)),
        "unexpected error: {err:?}"
    );
    let state = h.handle.get_state().await.unwrap();
    assert_eq!(state.execution_generation, None);
    assert!(state.evidence_receipts.is_empty());
}

#[tokio::test]
async fn stage_byte_budget_fails_the_whole_batch_without_partial_receipts() {
    let h = harness("budget").await;
    std::fs::write(h.ws.join("src/big-a.bin"), vec![0_u8; 1000]).unwrap();
    std::fs::write(h.ws.join("src/big-b.bin"), vec![1_u8; 1000]).unwrap();
    let bounds = RuntimeBounds {
        max_evidence_bytes_per_stage: 1500,
        ..RuntimeBounds::default()
    };

    let err = h
        .collect(
            vec![request("src/big-a.bin"), request("src/big-b.bin")],
            bounds,
            true,
        )
        .await
        .expect_err("a batch whose files exceed the stage budget must fail");
    match &err {
        EvidenceStageError::Stage(RuntimeError::Evidence(message)) => assert!(
            message.contains("byte budget"),
            "expected a budget failure, got {message}"
        ),
        other => panic!("expected a stage byte budget failure, got {other:?}"),
    }

    let state = h.handle.get_state().await.unwrap();
    assert_eq!(
        state.execution_generation, None,
        "exhaustion retires the generation after the workers drain"
    );
    assert!(
        state.evidence_receipts.is_empty(),
        "no partial evidence batch is ever journalled"
    );
    assert!(
        state
            .node_statuses
            .values()
            .all(|status| *status == NodeStatus::Cancelled),
        "every node of the interrupted generation is Cancelled: {:?}",
        state.node_statuses
    );
}

#[tokio::test]
async fn cancelled_stage_opens_nothing_and_journals_no_generation() {
    let h = harness("cancel").await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let err = h
        .collect_with(
            &h.run_id,
            0,
            vec![request("src/a.rs")],
            RuntimeBounds::default(),
            true,
            cancel,
        )
        .await
        .expect_err("a cancelled stage must refuse before dispatch");
    assert!(
        matches!(&err, EvidenceStageError::Stage(RuntimeError::Evidence(_))),
        "unexpected error: {err:?}"
    );

    let state = h.handle.get_state().await.unwrap();
    assert_eq!(state.execution_generation, None);
    assert!(state.evidence_receipts.is_empty());
    assert!(
        state.node_statuses.is_empty(),
        "cancellation before dispatch allocates no nodes"
    );
}

#[tokio::test]
async fn stale_run_and_revision_fences_refuse_before_opening_or_allocating() {
    let h = harness("fence").await;
    let requests = vec![request("src/a.rs")];

    let err = h
        .collect_with(
            "run-someone-else",
            0,
            requests.clone(),
            RuntimeBounds::default(),
            false,
            CancellationToken::new(),
        )
        .await
        .expect_err("a different run than the acknowledged one is refused");
    assert!(
        matches!(
            &err,
            EvidenceStageError::Core(CoreError::RunAlreadyActive { .. })
        ),
        "unexpected error: {err:?}"
    );

    // With no run acknowledged at all, the fence says so.
    let idle = harness_idle("fence-idle").await;
    let err = idle
        .collect_with(
            "run-never-acknowledged",
            0,
            requests.clone(),
            RuntimeBounds::default(),
            false,
            CancellationToken::new(),
        )
        .await
        .expect_err("a run this supervisor never acknowledged is refused");
    assert!(
        matches!(&err, EvidenceStageError::Core(CoreError::UnknownRun { .. })),
        "unexpected error: {err:?}"
    );
    let idle_state = idle.handle.get_state().await.unwrap();
    assert_eq!(idle_state.execution_generation, None);

    let err = h
        .collect_with(
            &h.run_id,
            7,
            requests.clone(),
            RuntimeBounds::default(),
            false,
            CancellationToken::new(),
        )
        .await
        .expect_err("a revision the task has moved past is refused");
    assert!(
        matches!(
            &err,
            EvidenceStageError::Core(CoreError::StaleRunProposal { .. })
        ),
        "unexpected error: {err:?}"
    );

    let state = h.handle.get_state().await.unwrap();
    assert_eq!(state.execution_generation, None, "no fence means no work");
    assert!(state.evidence_receipts.is_empty());

    // The real fence still works.
    let batch = h
        .collect(requests, RuntimeBounds::default(), false)
        .await
        .expect("the acknowledged run and revision collect normally");
    assert_eq!(batch.receipts.len(), 1);
}

#[tokio::test]
async fn a_later_generation_keeps_earlier_receipts_for_audit() {
    let h = harness("generations").await;
    let first = h
        .collect(
            vec![request("src/a.rs"), request("src/b.rs")],
            RuntimeBounds::default(),
            false,
        )
        .await
        .expect("first generation");
    let second = h
        .collect(
            vec![request("notes/d.txt")],
            RuntimeBounds::default(),
            false,
        )
        .await
        .expect("second generation");

    assert_eq!(first.receipts[0].generation, 1);
    assert_eq!(second.receipts[0].generation, 2);
    let state = h.handle.get_state().await.unwrap();
    assert_eq!(state.execution_generation, Some(2));
    assert_eq!(state.next_execution_generation, 3);
    assert_eq!(
        state.evidence_receipts.len(),
        3,
        "earlier receipts stay attached to their generation for audit"
    );
    for receipt in &first.receipts {
        assert_eq!(
            state.node_statuses.get(&receipt.node_id),
            Some(&NodeStatus::Succeeded),
            "an earlier generation's successes are not rewritten"
        );
    }

    // Retrieval still reaches an earlier generation's receipt (which
    // needs the durable workspace pin the provenance is bound to).
    let canonical_ws = std::fs::canonicalize(&h.ws).unwrap();
    h.handle
        .pin_workspace_root(canonical_ws.display().to_string())
        .await
        .unwrap();
    let item = h
        .handle
        .fetch_evidence(first.receipts[0].node_id)
        .await
        .expect("older receipts stay retrievable");
    assert!(!item.bytes.is_empty());
}

fn artifact_count(h: &Harness) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(h.dir.join("artifacts")) {
        for entry in entries.flatten() {
            if let Ok(inner) = std::fs::read_dir(entry.path()) {
                count += inner.count();
            }
        }
    }
    count
}
