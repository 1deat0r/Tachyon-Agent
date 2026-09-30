//! M13 T2: scheduler dispatch overhead (spec §43: < 1 ms p95 excluding
//! executor work).
//!
//! A 120-node chain runs through a zero-work executor that stamps entry and
//! exit instants. For each edge the gap `entry(successor) − exit(predecessor)`
//! is exactly the scheduler's own dispatch work: outcome handling, readiness
//! recomputation, conflict/resource grant, task spawn, and wakeup — the
//! executor contributes nothing inside that window. Ignore-gated; the M13
//! ledger runs it with `cargo test --release … -- --ignored --nocapture`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tachyon_ir::{
    AccessSet, CancellationPolicy, Dependency, DependencyCondition, EffectClass, ExecutionGraph,
    ExecutionNode, ExecutorKind, IR_VERSION, Idempotency, Invocation, NodePriority, ResourceClaim,
    RetryPolicy, SpeculationPolicy, TimeoutPolicy,
};
use tachyon_scheduler::{Budgets, Executor, ExecutorRegistry, NodeOutcome, ResolvedInputs, spawn};
use tachyon_types::{CapabilityId, NodeId, TaskId};
use tokio_util::sync::CancellationToken;

const CHAIN: usize = 120;
const TARGET_P95: Duration = Duration::from_millis(1);
const FINISH_DEADLINE: Duration = Duration::from_secs(15);

/// Entry/exit stamps per node, recorded by the zero-work executor.
#[derive(Default)]
struct TimingExecutor {
    stamps: Mutex<HashMap<NodeId, (Instant, Instant)>>,
}

#[async_trait]
impl Executor for TimingExecutor {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Native
    }

    async fn execute(
        &self,
        node: &ExecutionNode,
        _inputs: ResolvedInputs,
        _cancel: CancellationToken,
    ) -> NodeOutcome {
        let entry = Instant::now();
        let exit = Instant::now();
        self.stamps
            .lock()
            .expect("stamps lock")
            .insert(node.id, (entry, exit));
        NodeOutcome::success(serde_json::Map::new(), exit.duration_since(entry))
    }
}

/// One pure native node shaped like the scheduler's own test fixtures.
fn chain_node(task: TaskId) -> ExecutionNode {
    ExecutionNode {
        id: NodeId::generate(),
        task_id: task,
        planned_revision: 0,
        executor: ExecutorKind::Native,
        invocation: Invocation {
            capability: CapabilityId("test.noop".to_owned()),
            args: serde_json::json!({}),
            contract_version: tachyon_ir::CAPABILITY_CONTRACT_NONE,
        },
        inputs: vec![],
        expected_outputs: vec![],
        access: AccessSet {
            reads: vec![],
            writes: vec![],
        },
        resources: ResourceClaim::default(),
        effect_class: EffectClass::Pure,
        idempotency: Idempotency::Pure,
        speculation: SpeculationPolicy::Forbidden,
        timeout: TimeoutPolicy::default(),
        retry: RetryPolicy::default(),
        cancellation: CancellationPolicy::Immediate,
        verification: vec![],
        priority: NodePriority::Normal,
    }
}

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    (p50, p95)
}

#[tokio::test]
#[ignore = "M13 perf target: release mode, run with --ignored"]
async fn t2_dispatch_gap_p95_under_1ms() {
    let task = TaskId::generate();
    let nodes: Vec<ExecutionNode> = (0..CHAIN).map(|_| chain_node(task)).collect();
    let ids: Vec<NodeId> = nodes.iter().map(|node| node.id).collect();
    let dependencies: Vec<Dependency> = (0..CHAIN - 1)
        .map(|index| Dependency {
            from: ids[index],
            to: ids[index + 1],
            condition: DependencyCondition::OnSuccess,
        })
        .collect();
    let graph = ExecutionGraph {
        version: IR_VERSION,
        nodes: nodes.into_iter().map(|node| (node.id, node)).collect(),
        dependencies,
    };

    let timing = Arc::new(TimingExecutor::default());
    let mut registry: ExecutorRegistry = HashMap::new();
    registry.insert(ExecutorKind::Native, timing.clone());
    let (handle, join) = spawn(Budgets::default(), registry);

    handle.submit(task, graph).await.expect("chain submits");

    let deadline = Instant::now() + FINISH_DEADLINE;
    loop {
        let done = timing.stamps.lock().expect("stamps lock").len();
        if done == CHAIN {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "chain stalled: {done}/{CHAIN} nodes executed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    join.abort();

    let stamps = timing.stamps.lock().expect("stamps lock");
    let mut gaps = Vec::with_capacity(CHAIN - 1);
    let mut executor_work = Duration::ZERO;
    for index in 0..CHAIN - 1 {
        let (pre_exit, _) = stamps[&ids[index]];
        let (next_entry, next_exit) = stamps[&ids[index + 1]];
        let gap = next_entry
            .checked_duration_since(pre_exit)
            .expect("successor starts after predecessor exits");
        gaps.push(gap);
        executor_work = executor_work.max(next_exit.duration_since(next_entry));
    }
    drop(stamps);

    let (p50, p95) = percentiles(gaps);
    let measured = CHAIN - 1;
    println!(
        "perf[T2] n={measured} p50={p50:?} p95={p95:?} target p95<1ms {} (max executor stamp width {executor_work:?})",
        if p95 < TARGET_P95 { "PASS" } else { "MISS" }
    );
    assert!(
        p95 < TARGET_P95,
        "T2 miss: dispatch gap p95={p95:?} >= {TARGET_P95:?}"
    );
    println!("perf[T2] PASS");
}
