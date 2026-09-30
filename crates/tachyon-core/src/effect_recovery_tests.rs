use std::collections::BTreeMap;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{
    ConstraintStrength, EffectState, TaskStatus, ValidatedExecutionGraph, create_task, recover_task,
};
use tachyon_ir::{
    AccessSet, CancellationPolicy, EffectClass, ExecutionGraph, ExecutionNode, ExecutorKind,
    IR_VERSION, Idempotency, Invocation, NodePriority, NodeStatus, ResourceClaim, ResourceKey,
    RetryPolicy, SpeculationPolicy, TimeoutPolicy,
};
use tachyon_store::StoreWriter;
use tachyon_types::{CapabilityId, NodeId, SessionId, TaskId, WorkspaceId};

const CHILD_DIR: &str = "TACHYON_EFFECT_RECOVERY_CHILD_DIR";
const CHILD_MODE: &str = "TACHYON_EFFECT_RECOVERY_CHILD_MODE";

async fn task_with_store(tag: &str) -> (Arc<StoreWriter>, std::path::PathBuf, TaskId) {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-effect-recovery-{tag}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(StoreWriter::open(&dir).await.unwrap());
    let session = SessionId::generate();
    store.create_session(&session.to_string()).await.unwrap();
    let handle = create_task(
        session,
        WorkspaceId::generate(),
        "effect recovery integration".to_owned(),
        store.clone(),
    )
    .await
    .unwrap();
    let task_id = handle.task_id();
    handle.shutdown().await.unwrap();
    (store, dir, task_id)
}

fn effect_graph(task_id: TaskId, idempotency: Idempotency) -> (ExecutionGraph, NodeId) {
    let node_id = NodeId::generate();
    let node = ExecutionNode {
        id: node_id,
        task_id,
        planned_revision: 0,
        executor: ExecutorKind::Tool,
        invocation: Invocation {
            capability: CapabilityId("test.external_write".to_owned()),
            args: serde_json::json!({"target": "record-1"}),
            contract_version: tachyon_ir::CAPABILITY_CONTRACT_NONE,
        },
        inputs: vec![],
        expected_outputs: vec![],
        access: AccessSet::default(),
        resources: ResourceClaim::default(),
        effect_class: EffectClass::DestructiveExternalMutation,
        idempotency,
        speculation: SpeculationPolicy::Forbidden,
        timeout: TimeoutPolicy::default(),
        retry: RetryPolicy::default(),
        cancellation: CancellationPolicy::Immediate,
        verification: vec![],
        priority: NodePriority::Normal,
    };
    let graph = ExecutionGraph {
        version: IR_VERSION,
        nodes: BTreeMap::from([(node_id, node)]),
        dependencies: vec![],
    };
    (graph, node_id)
}

#[tokio::test]
async fn unsafe_prepared_effect_is_journalled_and_node_becomes_unknown_after_crash() {
    let (store, dir, task_id) = task_with_store("unsafe").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (graph, node_id) = effect_graph(task_id, Idempotency::NonIdempotent);

    let installed = handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    assert_eq!(installed.node_statuses[&node_id], NodeStatus::Pending);
    handle.start_node(node_id).await.unwrap();
    let prepared = handle
        .prepare_effect(node_id, "effect-unsafe-1".to_owned())
        .await
        .unwrap();
    assert_eq!(prepared.node_statuses[&node_id], NodeStatus::Prepared);

    let row = store.load_effect("effect-unsafe-1").await.unwrap().unwrap();
    assert_eq!(row.state, "prepared");
    let node_id_text = node_id.to_string();
    assert_eq!(row.node_id.as_deref(), Some(node_id_text.as_str()));
    let events = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap();
    assert!(events.iter().any(|event| event.kind == "effect_prepared"));

    handle.shutdown().await.unwrap();
    let recovered = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered.get_state().await.unwrap();
    assert_eq!(state.status, TaskStatus::Recovering);
    assert_eq!(state.node_statuses[&node_id], NodeStatus::UnknownAfterCrash);
    assert_eq!(
        state.effects["effect-unsafe-1"].state,
        EffectState::UnknownAfterCrash
    );
    let row = store.load_effect("effect-unsafe-1").await.unwrap().unwrap();
    assert_eq!(row.state, "unknown_after_crash");
    let events = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == "effect_unknown_after_crash")
    );

    recovered.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn cancelled_task_reconciles_interrupted_nodes_without_reopening() {
    let (store, dir, task_id) = task_with_store("cancelled-recovery").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (prepared_graph, prepared_node) = effect_graph(task_id, Idempotency::NonIdempotent);
    let (running_graph, running_node) = effect_graph(task_id, Idempotency::Keyed);
    let graph = ExecutionGraph {
        version: IR_VERSION,
        nodes: prepared_graph
            .nodes
            .into_iter()
            .chain(running_graph.nodes)
            .collect(),
        dependencies: vec![],
    };
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(prepared_node).await.unwrap();
    handle
        .prepare_effect(prepared_node, "effect-cancelled-1".to_owned())
        .await
        .unwrap();
    handle.start_node(running_node).await.unwrap();

    let cancelled = handle.cancel().await.unwrap();
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    handle.shutdown().await.unwrap();

    let recovered = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered.get_state().await.unwrap();
    assert_eq!(state.status, TaskStatus::Cancelled);
    assert_eq!(
        state.node_statuses[&prepared_node],
        NodeStatus::UnknownAfterCrash
    );
    assert_eq!(state.node_statuses[&running_node], NodeStatus::Pending);
    assert_eq!(
        state.effects["effect-cancelled-1"].state,
        EffectState::UnknownAfterCrash
    );
    assert_eq!(
        store
            .load_effect("effect-cancelled-1")
            .await
            .unwrap()
            .unwrap()
            .state,
        "unknown_after_crash"
    );
    let events_after_recovery = store
        .load_events_since(&task_id.to_string(), -1)
        .await
        .unwrap();
    assert!(
        events_after_recovery
            .iter()
            .any(|event| event.kind == "effect_unknown_after_crash")
    );

    recovered.shutdown().await.unwrap();
    let recovered_again = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered_again.get_state().await.unwrap();
    assert_eq!(state.status, TaskStatus::Cancelled);
    assert_eq!(
        state.node_statuses[&prepared_node],
        NodeStatus::UnknownAfterCrash
    );
    assert_eq!(state.node_statuses[&running_node], NodeStatus::Pending);
    assert_eq!(
        store
            .load_events_since(&task_id.to_string(), -1)
            .await
            .unwrap()
            .len(),
        events_after_recovery.len(),
        "a second recovery must replay classifications without duplicating them"
    );

    recovered_again.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn keyed_prepared_effect_remains_reconcilable_and_commits_through_supervisor() {
    let (store, dir, task_id) = task_with_store("keyed").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (graph, node_id) = effect_graph(task_id, Idempotency::Keyed);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(node_id).await.unwrap();
    handle
        .prepare_effect(node_id, "effect-keyed-1".to_owned())
        .await
        .unwrap();
    handle.shutdown().await.unwrap();

    let recovered = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered.get_state().await.unwrap();
    assert_eq!(state.status, TaskStatus::Recovering);
    assert_eq!(state.node_statuses[&node_id], NodeStatus::Prepared);
    assert_eq!(state.effects["effect-keyed-1"].state, EffectState::Prepared);
    assert_eq!(
        store
            .load_effect("effect-keyed-1")
            .await
            .unwrap()
            .unwrap()
            .state,
        "prepared"
    );

    recovered
        .commit_effect("effect-keyed-1", "remote-receipt-1".to_owned())
        .await
        .unwrap();
    let completed = recovered.complete_node(node_id).await.unwrap();
    assert_eq!(completed.node_statuses[&node_id], NodeStatus::Succeeded);
    assert_eq!(
        completed.effects["effect-keyed-1"].state,
        EffectState::Committed
    );
    let row = store.load_effect("effect-keyed-1").await.unwrap().unwrap();
    assert_eq!(row.state, "committed");
    assert_eq!(row.receipt.as_deref(), Some("remote-receipt-1"));
    let events = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap();
    assert!(events.iter().any(|event| event.kind == "effect_committed"));

    recovered.shutdown().await.unwrap();
    let after_restart = recover_task(task_id, store.clone()).await.unwrap();
    let state = after_restart.get_state().await.unwrap();
    assert_eq!(state.node_statuses[&node_id], NodeStatus::Succeeded);
    assert_eq!(
        state.effects["effect-keyed-1"].receipt.as_deref(),
        Some("remote-receipt-1")
    );
    after_restart.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn running_node_without_prepared_effect_returns_to_pending_on_recovery() {
    let (store, dir, task_id) = task_with_store("running").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (graph, node_id) = effect_graph(task_id, Idempotency::NonIdempotent);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(node_id).await.unwrap();
    handle.shutdown().await.unwrap();

    let recovered = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered.get_state().await.unwrap();
    assert_eq!(state.status, TaskStatus::Recovering);
    assert_eq!(state.node_statuses[&node_id], NodeStatus::Pending);

    let before = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert!(recovered.start_node(node_id).await.is_err());
    assert_eq!(
        store
            .load_events_since(&task_id.to_string(), 0)
            .await
            .unwrap()
            .len(),
        before,
        "Recovering must not dispatch work"
    );
    assert_eq!(recovered.resume().await.unwrap().status, TaskStatus::Paused);
    assert_eq!(
        recovered.resume().await.unwrap().status,
        TaskStatus::Created
    );
    assert_eq!(
        recovered.start_node(node_id).await.unwrap().node_statuses[&node_id],
        NodeStatus::Running
    );

    recovered.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn supervisor_refuses_to_start_nodes_with_conflicting_access_sets() {
    let (store, dir, task_id) = task_with_store("access-conflict").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (mut graph, first_id) = effect_graph(task_id, Idempotency::Pure);
    let mut second = graph.nodes[&first_id].clone();
    second.id = NodeId::generate();
    let shared = ResourceKey::parse("file:/shared").unwrap();
    graph.nodes.get_mut(&first_id).unwrap().access.writes = vec![shared.clone()];
    second.access.writes = vec![shared];
    let second_id = second.id;
    graph.nodes.insert(second_id, second);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(first_id).await.unwrap();

    let before = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert!(handle.start_node(second_id).await.is_err());
    let state = handle.get_state().await.unwrap();
    assert_eq!(state.node_statuses[&first_id], NodeStatus::Running);
    assert_eq!(state.node_statuses[&second_id], NodeStatus::Pending);
    let after = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert_eq!(before, after, "rejected dispatch must perform no write");

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn stale_plan_is_refused_after_steering_without_journal_writes() {
    let (store, dir, task_id) = task_with_store("stale-plan").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (graph, node_id) = effect_graph(task_id, Idempotency::NonIdempotent);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle
        .add_constraint(
            "keep all writes under workspace".to_owned(),
            ConstraintStrength::Hard,
        )
        .await
        .unwrap();

    let before = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert!(handle.start_node(node_id).await.is_err());
    assert!(
        handle
            .prepare_effect(node_id, "effect-stale-plan".to_owned())
            .await
            .is_err()
    );
    let after = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert_eq!(before, after);
    assert!(
        store
            .load_effect("effect-stale-plan")
            .await
            .unwrap()
            .is_none()
    );

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn paused_task_refuses_dispatch_and_effect_preparation() {
    let (store, dir, task_id) = task_with_store("paused").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (mut graph, first_id) = effect_graph(task_id, Idempotency::NonIdempotent);
    let mut second = graph.nodes[&first_id].clone();
    second.id = NodeId::generate();
    let second_id = second.id;
    graph.nodes.insert(second_id, second);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(first_id).await.unwrap();
    handle.pause().await.unwrap();

    let before = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert!(handle.start_node(second_id).await.is_err());
    assert!(
        handle
            .prepare_effect(first_id, "effect-paused".to_owned())
            .await
            .is_err()
    );
    let after = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert_eq!(before, after);
    assert!(store.load_effect("effect-paused").await.unwrap().is_none());

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn consequential_node_cannot_succeed_without_a_committed_effect() {
    let (store, dir, task_id) = task_with_store("missing-barrier").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (graph, node_id) = effect_graph(task_id, Idempotency::Idempotent);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(node_id).await.unwrap();

    let before = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert!(handle.complete_node(node_id).await.is_err());
    let after = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert_eq!(before, after);
    assert_eq!(
        handle.get_state().await.unwrap().node_statuses[&node_id],
        NodeStatus::Running
    );

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn duplicate_effect_preparation_never_reauthorizes_the_action() {
    let (store, dir, task_id) = task_with_store("duplicate-prepare").await;
    let handle = recover_task(task_id, store.clone()).await.unwrap();
    let (graph, node_id) = effect_graph(task_id, Idempotency::NonIdempotent);
    handle
        .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
        .await
        .unwrap();
    handle.start_node(node_id).await.unwrap();
    handle
        .prepare_effect(node_id, "effect-duplicate".to_owned())
        .await
        .unwrap();

    let before = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert!(
        handle
            .prepare_effect(node_id, "effect-duplicate".to_owned())
            .await
            .is_err()
    );
    let after = store
        .load_events_since(&task_id.to_string(), 0)
        .await
        .unwrap()
        .len();
    assert_eq!(before, after);

    handle.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn process_kill_at_effect_barrier_seams_recovers_consistently() {
    if let Ok(dir) = std::env::var(CHILD_DIR) {
        let dir = std::path::PathBuf::from(dir);
        let mode = std::env::var(CHILD_MODE).unwrap();
        let effect_id = format!("effect-kill-{mode}");
        let store = Arc::new(StoreWriter::open(&dir).await.unwrap());
        let session = SessionId::generate();
        store.create_session(&session.to_string()).await.unwrap();
        let handle = create_task(
            session,
            WorkspaceId::generate(),
            "kill after effect barrier".to_owned(),
            store.clone(),
        )
        .await
        .unwrap();
        let task_id = handle.task_id();
        let (graph, node_id) = effect_graph(task_id, Idempotency::NonIdempotent);
        handle
            .install_execution_graph(ValidatedExecutionGraph::from_unchecked_test_graph(graph))
            .await
            .unwrap();
        handle.start_node(node_id).await.unwrap();
        std::fs::write(dir.join("task-node-id"), format!("{task_id}\n{node_id}")).unwrap();
        handle
            .prepare_effect(node_id, effect_id.clone())
            .await
            .unwrap();
        if mode != "prepared" {
            handle
                .commit_effect(&effect_id, "remote-receipt-kill-test".to_owned())
                .await
                .unwrap();
        }
        std::process::exit(0);
    }

    for (mode, fault_point) in [
        ("prepared", "effect.prepared"),
        ("remote_return", "effect.remote_return"),
        ("committed", "effect.committed"),
    ] {
        run_parent_recovery_case(mode, fault_point).await;
    }
}

async fn run_parent_recovery_case(mode: &str, fault_point: &str) {
    let dir = std::env::temp_dir().join(format!(
        "tachyon-effect-kill-{mode}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let marker = dir.join("fault-reached");
    let store = Arc::new(StoreWriter::open(&dir).await.unwrap());
    store.close().await;

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "effect_recovery_tests::process_kill_at_effect_barrier_seams_recovers_consistently",
            "--nocapture",
        ])
        .env(CHILD_DIR, &dir)
        .env(CHILD_MODE, mode)
        .env("TACHYON_FAULT_POINT", fault_point)
        .env("TACHYON_FAULT_REACHED_FILE", &marker)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    let started = Instant::now();
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("effect fault child exited before {fault_point}: {status}");
        }
        if started.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child did not reach effect fault point {fault_point}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), fault_point);
    assert!(
        child.try_wait().unwrap().is_none(),
        "child should park at seam"
    );
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());

    let identities = std::fs::read_to_string(dir.join("task-node-id")).unwrap();
    let mut identities = identities.lines();
    let task_id: TaskId = identities.next().unwrap().parse().unwrap();
    let node_id: NodeId = identities.next().unwrap().parse().unwrap();
    let effect_id = format!("effect-kill-{mode}");
    let store = Arc::new(StoreWriter::open(&dir).await.unwrap());
    let recovered = recover_task(task_id, store.clone()).await.unwrap();
    let state = recovered.get_state().await.unwrap();
    assert_eq!(state.status, TaskStatus::Recovering);
    if mode == "committed" {
        assert_eq!(state.node_statuses[&node_id], NodeStatus::Prepared);
        assert_eq!(state.effects[&effect_id].state, EffectState::Committed);
        assert_eq!(
            state.effects[&effect_id].receipt.as_deref(),
            Some("remote-receipt-kill-test")
        );
        let completed = recovered.complete_node(node_id).await.unwrap();
        assert_eq!(completed.node_statuses[&node_id], NodeStatus::Succeeded);
    } else {
        assert_eq!(state.node_statuses[&node_id], NodeStatus::UnknownAfterCrash);
        assert_eq!(
            state.effects[&effect_id].state,
            EffectState::UnknownAfterCrash
        );
        assert_eq!(
            store.load_effect(&effect_id).await.unwrap().unwrap().state,
            "unknown_after_crash"
        );
    }

    recovered.shutdown().await.unwrap();
    store.close().await;
    std::fs::remove_dir_all(dir).unwrap();
}
