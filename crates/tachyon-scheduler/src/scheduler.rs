//! Dependency-aware, effect-aware DAG scheduler (spec §11–§14).
//!
//! The loop owns readiness, atomic conflict/resource grants, critical-path
//! priority, retries, timeouts, and structured cancellation. Executors own
//! only single-node execution. Concurrency happens only when declared
//! dependencies, access sets, and budgets all permit it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tachyon_ir::{
    DependencyCondition, ExecutionGraph, ExecutionNode, ExecutorKind, IrError, NodeStatus,
    SpeculationPolicy,
};
use tachyon_types::{NodeId, TaskId};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::executor::{Executor, NodeOutcome, OutcomeStatus, ResolvedInputs};

/// Scheduler mailbox capacity; backpressure beats unbounded growth.
pub const SCHEDULER_MAILBOX: usize = 256;

/// Default per-capability duration estimate in ms before measurements exist.
const DEFAULT_ESTIMATE_MS: f64 = 100.0;

/// EWMA weight for new duration samples.
const ESTIMATE_ALPHA: f64 = 0.3;

/// Speculative nodes wait behind this penalty (Allowed only; Preferred
/// competes freely but still yields to grants).
const SPECULATION_PENALTY: f64 = 1_000.0;

const FAST_FINISH_POLLS: u32 = 50;
const FINISH_POLL_FAST: Duration = Duration::from_millis(1);
const FINISH_POLL_SLOW: Duration = Duration::from_millis(10);

fn finish_poll_delay(polls: u32) -> Duration {
    if polls < FAST_FINISH_POLLS {
        FINISH_POLL_FAST
    } else {
        FINISH_POLL_SLOW
    }
}

/// Age bonus per second of readiness, against starvation.
const AGE_BONUS_PER_SEC: f64 = 0.5;

/// Ready-state recompute tick.
const TICK: Duration = Duration::from_millis(50);

/// Errors produced by the scheduler.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// Graph failed validation on submit.
    #[error("invalid graph: {0}")]
    InvalidGraph(#[from] IrError),
    /// Registered executor did not prove automatic retry safety for this node.
    #[error("registered executor did not authorize retrying node {0}")]
    RetryNotAuthorized(NodeId),
    /// No task run with this id.
    #[error("unknown task: {0}")]
    UnknownTask(TaskId),
    /// A live run already exists for this task.
    #[error("task already submitted: {0}")]
    DuplicateTask(TaskId),
    /// No executor registered for this kind.
    #[error("no executor for {0:?}")]
    UnknownExecutor(ExecutorKind),
    /// An executor ended without settling a node's effect outcome.
    #[error("task {task_id} node {node_id} ended with an unknown outcome")]
    UnknownOutcome {
        /// Task that owns the unresolved node.
        task_id: TaskId,
        /// Node whose effect may need reconciliation.
        node_id: NodeId,
    },
    /// Mailbox full; back off and retry.
    #[error("scheduler mailbox full")]
    MailboxFull,
    /// Scheduler loop ended before answering.
    #[error("scheduler gone")]
    SchedulerGone,
}

/// Executor registry: one implementation per kind.
pub type ExecutorRegistry = HashMap<ExecutorKind, Arc<dyn Executor>>;

/// Capacity budgets (spec §10). Claims beyond budget wait.
#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    /// CPU shares (whole cores = 1000).
    pub cpu_units: u32,
    /// Process slots.
    pub process_slots: u32,
    /// Concurrent network operations.
    pub network_slots: u32,
    /// Memory in MiB.
    pub memory_mb: u64,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            cpu_units: 4_000,
            process_slots: 8,
            network_slots: 16,
            memory_mb: 8_192,
        }
    }
}

/// Point-in-time task run state for clients and tests.
#[derive(Clone, Debug)]
pub struct TaskRunSnapshot {
    /// Task run.
    pub task_id: TaskId,
    /// Per-node status.
    pub statuses: HashMap<NodeId, NodeStatus>,
    /// Attempts made per node.
    pub attempts: HashMap<NodeId, u32>,
    /// True when every node is terminal.
    pub finished: bool,
    /// First node whose effect outcome needs reconciliation.
    pub unresolved_node: Option<NodeId>,
    /// Structured outputs of every succeeded node, keyed by node id.
    /// Completed output is part of the public run snapshot on purpose: a
    /// caller that must journal durable receipts (ADR-0006 §11) cannot
    /// prove which bytes a succeeded node supplied from statuses alone.
    pub outputs: HashMap<NodeId, serde_json::Map<String, serde_json::Value>>,
}

/// Commands the scheduler owns.
pub enum SchedulerCommand {
    /// Validates and starts a graph run.
    Submit {
        /// Task the graph belongs to.
        task_id: TaskId,
        /// Graph to run.
        graph: ExecutionGraph,
        /// Reply when accepted.
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Cancels a run and waits until every running executor has drained.
    CancelTask {
        /// Task to cancel.
        task_id: TaskId,
        /// Reply once all in-flight executors have drained.
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Reads a run snapshot.
    Status {
        /// Task to inspect.
        task_id: TaskId,
        /// Reply with the snapshot.
        reply: oneshot::Sender<Result<TaskRunSnapshot, SchedulerError>>,
    },
}

/// Cloneable handle to a running scheduler.
#[derive(Clone, Debug)]
pub struct SchedulerHandle {
    tx: mpsc::Sender<SchedulerCommand>,
}

impl SchedulerHandle {
    /// Submits a graph run after validation.
    pub async fn submit(
        &self,
        task_id: TaskId,
        graph: ExecutionGraph,
    ) -> Result<(), SchedulerError> {
        graph.validate(task_id)?;
        let (reply, rx) = oneshot::channel();
        self.send(SchedulerCommand::Submit {
            task_id,
            graph,
            reply,
        })
        .await;
        receive(rx).await
    }

    /// Requests cancellation and waits until all in-flight executors have drained.
    pub async fn cancel_task(&self, task_id: TaskId) -> Result<(), SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.send(SchedulerCommand::CancelTask { task_id, reply })
            .await;
        receive(rx).await
    }

    /// Reads a run snapshot.
    pub async fn status(&self, task_id: TaskId) -> Result<TaskRunSnapshot, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.send(SchedulerCommand::Status { task_id, reply }).await;
        receive(rx).await
    }

    /// Waits until a run finishes or `timeout` elapses.
    ///
    /// M13: completion was noticed on a fixed 10 ms tick, which added up
    /// to 10 ms of pure poll latency to every short run (each verification
    /// pays this after the child is already done). Fast checks for the
    /// first `FAST_FINISH_POLLS`, then the original 10 ms cadence — the
    /// steady-state wakeup rate for long runs is unchanged.
    pub async fn wait_finished(
        &self,
        task_id: TaskId,
        timeout: Duration,
    ) -> Result<TaskRunSnapshot, SchedulerError> {
        let deadline = Instant::now() + timeout;
        let mut polls: u32 = 0;
        loop {
            let snapshot = self.status(task_id).await?;
            if let Some(node_id) = snapshot.unresolved_node {
                return Err(SchedulerError::UnknownOutcome { task_id, node_id });
            }
            if snapshot.finished || Instant::now() >= deadline {
                return Ok(snapshot);
            }
            let delay = finish_poll_delay(polls);
            polls = polls.saturating_add(1);
            tokio::time::sleep(delay).await;
        }
    }

    async fn send(&self, command: SchedulerCommand) {
        let _ = self.tx.send(command).await;
    }
}

async fn receive<T>(rx: oneshot::Receiver<Result<T, SchedulerError>>) -> Result<T, SchedulerError> {
    rx.await.map_err(|_| SchedulerError::SchedulerGone)?
}

/// Spawns the scheduler loop.
#[must_use]
pub fn spawn(
    budgets: Budgets,
    registry: ExecutorRegistry,
) -> (SchedulerHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(SCHEDULER_MAILBOX);
    let join = tokio::spawn(run_loop(budgets, registry, rx));
    (SchedulerHandle { tx }, join)
}

/// One granted node's held claims.
struct Grant {
    task_id: TaskId,
    node_id: NodeId,
    access: tachyon_ir::AccessSet,
    cpu: u32,
    process_slots: u32,
    network_slots: u32,
    memory_mb: u64,
}

/// Currently consumed capacity.
#[derive(Default)]
struct Usage {
    cpu: u32,
    process_slots: u32,
    network_slots: u32,
    memory_mb: u64,
}

/// Per-node run bookkeeping.
struct NodeRun {
    status: NodeStatus,
    ready_at: Option<Instant>,
    blocked_until: Option<Instant>,
    outputs: serde_json::Map<String, serde_json::Value>,
}

/// Per-task run bookkeeping.
struct TaskRun {
    task_id: TaskId,
    graph: ExecutionGraph,
    nodes: HashMap<NodeId, NodeRun>,
    attempts: HashMap<NodeId, u32>,
    outputs: HashMap<NodeId, serde_json::Map<String, serde_json::Value>>,
    scope: CancellationToken,
    cancel_waiters: Vec<oneshot::Sender<Result<(), SchedulerError>>>,
    unresolved_node: Option<NodeId>,
    finished: bool,
}

/// Completion delivered by the `JoinSet`.
struct Completion {
    task_id: TaskId,
    node_id: NodeId,
    outcome: NodeOutcome,
}

struct Loop {
    budgets: Budgets,
    registry: ExecutorRegistry,
    tasks: HashMap<TaskId, TaskRun>,
    in_flight: HashMap<tokio::task::Id, (TaskId, NodeId)>,
    grants: Vec<Grant>,
    used: Usage,
    estimates: HashMap<String, f64>,
}

async fn run_loop(
    budgets: Budgets,
    registry: ExecutorRegistry,
    mut rx: mpsc::Receiver<SchedulerCommand>,
) {
    let mut app = Loop {
        budgets,
        registry,
        tasks: HashMap::new(),
        in_flight: HashMap::new(),
        grants: Vec::new(),
        used: Usage::default(),
        estimates: HashMap::new(),
    };
    let mut running = JoinSet::new();
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut accepting = true;
    loop {
        // `JoinSet::join_next` on an empty set resolves immediately, which
        // would busy-spin the loop and starve command handling: only poll
        // it while something runs.
        let joining = !running.is_empty();
        tokio::select! {
            biased;
            command = rx.recv(), if accepting => {
                if let Some(command) = command {
                    app.handle(command);
                } else {
                    accepting = false;
                    app.cancel_all();
                }
            }
            completed = async { running.join_next_with_id().await }, if joining => {
                if let Some(result) = completed {
                    match result {
                        Ok((id, completion)) => {
                            app.in_flight.remove(&id);
                            app.complete(completion);
                        }
                        Err(error) => {
                            let worker_id = error.id();
                            let detail = error.to_string();
                            app.mark_unknown(worker_id, &detail);
                        }
                    }
                }
            }
            _ = tick.tick() => {}
        }
        if !accepting && running.is_empty() {
            break;
        }
        app.pump(&mut running);
    }
}

impl Loop {
    fn handle(&mut self, command: SchedulerCommand) {
        match command {
            SchedulerCommand::Submit {
                task_id,
                graph,
                reply,
            } => {
                let outcome = self.submit(task_id, graph);
                let _ = reply.send(outcome);
            }
            SchedulerCommand::CancelTask { task_id, reply } => {
                self.cancel_task(task_id, reply);
            }
            SchedulerCommand::Status { task_id, reply } => {
                let outcome = self.snapshot(task_id);
                let _ = reply.send(outcome);
            }
        }
    }

    fn submit(&mut self, task_id: TaskId, graph: ExecutionGraph) -> Result<(), SchedulerError> {
        if self.tasks.contains_key(&task_id) {
            return Err(SchedulerError::DuplicateTask(task_id));
        }
        for node in graph.nodes.values() {
            let Some(executor) = self.registry.get(&node.executor) else {
                return Err(SchedulerError::UnknownExecutor(node.executor));
            };
            if node.retry.attempts > 1 && !executor.automatic_retry_safe(node) {
                return Err(SchedulerError::RetryNotAuthorized(node.id));
            }
        }
        let nodes = graph
            .nodes
            .keys()
            .map(|id| {
                (
                    *id,
                    NodeRun {
                        status: NodeStatus::Pending,
                        ready_at: None,
                        blocked_until: None,
                        outputs: serde_json::Map::new(),
                    },
                )
            })
            .collect();
        self.tasks.insert(
            task_id,
            TaskRun {
                task_id,
                graph,
                nodes,
                attempts: HashMap::new(),
                outputs: HashMap::new(),
                scope: CancellationToken::new(),
                cancel_waiters: Vec::new(),
                unresolved_node: None,
                finished: false,
            },
        );
        Ok(())
    }

    fn cancel_task(&mut self, task_id: TaskId, reply: oneshot::Sender<Result<(), SchedulerError>>) {
        let Some(run) = self.tasks.get_mut(&task_id) else {
            let _ = reply.send(Err(SchedulerError::UnknownTask(task_id)));
            return;
        };
        run.cancel_waiters.push(reply);
        if !run.finished {
            run.scope.cancel();
            for node in run.nodes.values_mut() {
                // Keep running nodes and their grants live until their executor
                // reports that its cancellation policy has safely drained.
                if node.status != NodeStatus::Running
                    && node.status != NodeStatus::UnknownAfterCrash
                    && !node.status.is_terminal()
                {
                    node.status = NodeStatus::Cancelled;
                }
            }
            run.finished = run.nodes.values().all(|node| node.status.is_terminal());
        }
        self.settle_cancellations(task_id);
    }

    fn cancel_all(&mut self) {
        for run in self.tasks.values_mut() {
            if run.finished {
                continue;
            }
            run.scope.cancel();
            for node in run.nodes.values_mut() {
                if node.status != NodeStatus::Running
                    && node.status != NodeStatus::UnknownAfterCrash
                    && !node.status.is_terminal()
                {
                    node.status = NodeStatus::Cancelled;
                }
            }
            run.finished = run.nodes.values().all(|node| node.status.is_terminal());
        }
        let finished: Vec<TaskId> = self
            .tasks
            .values()
            .filter_map(|run| run.finished.then_some(run.task_id))
            .collect();
        for task_id in finished {
            self.settle_cancellations(task_id);
        }
    }

    fn settle_cancellations(&mut self, task_id: TaskId) {
        let Some(run) = self.tasks.get_mut(&task_id) else {
            return;
        };
        if let Some(node_id) = run.unresolved_node {
            if run
                .nodes
                .values()
                .any(|node| node.status == NodeStatus::Running)
            {
                return;
            }
            for waiter in std::mem::take(&mut run.cancel_waiters) {
                let _ = waiter.send(Err(SchedulerError::UnknownOutcome { task_id, node_id }));
            }
            return;
        }
        if !run.finished {
            return;
        }
        for waiter in std::mem::take(&mut run.cancel_waiters) {
            let _ = waiter.send(Ok(()));
        }
    }

    /// Keeps an executor panic or abort unresolved instead of freeing its
    /// grant or retrying an effect whose outcome is no longer known.
    fn mark_unknown(&mut self, worker_id: tokio::task::Id, error: &str) {
        let Some((task_id, node_id)) = self.in_flight.remove(&worker_id) else {
            tracing::error!(%error, "scheduler worker ended without a tracked node");
            return;
        };
        self.mark_unknown_node(task_id, node_id, error);
    }

    fn mark_unknown_node(&mut self, task_id: TaskId, node_id: NodeId, error: &str) {
        if let Some(run) = self.tasks.get_mut(&task_id) {
            // An unsettled effect makes every later task action unsafe. Stop
            // dispatch immediately and ask all current siblings to drain.
            run.scope.cancel();
            for node in run.nodes.values_mut() {
                if node.status != NodeStatus::Running
                    && node.status != NodeStatus::UnknownAfterCrash
                    && !node.status.is_terminal()
                {
                    node.status = NodeStatus::Cancelled;
                }
            }
            if let Some(node) = run.nodes.get_mut(&node_id) {
                node.status = NodeStatus::UnknownAfterCrash;
            }
            run.unresolved_node.get_or_insert(node_id);
        }
        tracing::error!(task = %task_id, node = %node_id, %error, "executor ended without a settled effect outcome");
        self.settle_cancellations(task_id);
    }

    fn snapshot(&self, task_id: TaskId) -> Result<TaskRunSnapshot, SchedulerError> {
        let Some(run) = self.tasks.get(&task_id) else {
            return Err(SchedulerError::UnknownTask(task_id));
        };
        Ok(TaskRunSnapshot {
            task_id,
            statuses: run
                .nodes
                .iter()
                .map(|(id, node)| (*id, node.status))
                .collect(),
            attempts: run.attempts.clone(),
            finished: run.finished,
            unresolved_node: run.unresolved_node,
            outputs: run.outputs.clone(),
        })
    }

    /// Releases one node's grant after it has settled, or after a local
    /// pre-dispatch failure that never started executor work.
    fn release_grant(&mut self, task_id: TaskId, node_id: NodeId) {
        if let Some(index) = self
            .grants
            .iter()
            .position(|grant| grant.task_id == task_id && grant.node_id == node_id)
        {
            let grant = self.grants.remove(index);
            self.used.cpu = self.used.cpu.saturating_sub(grant.cpu);
            self.used.process_slots = self.used.process_slots.saturating_sub(grant.process_slots);
            self.used.network_slots = self.used.network_slots.saturating_sub(grant.network_slots);
            self.used.memory_mb = self.used.memory_mb.saturating_sub(grant.memory_mb);
        }
    }

    /// Applies a completion after the executor has returned and drained.
    /// Each step re-borrows so grant release and state updates never alias.
    fn complete(&mut self, completion: Completion) {
        let task_id = completion.task_id;
        let node_id = completion.node_id;
        let live = self
            .tasks
            .get(&task_id)
            .and_then(|run| run.nodes.get(&node_id))
            .is_some_and(|node| node.status == NodeStatus::Running);
        if !live {
            return;
        }
        let duration = completion.outcome.duration;
        let unknown_reason = match &completion.outcome.status {
            OutcomeStatus::Unknown { reason } => Some(reason.clone()),
            _ => None,
        };
        if let Some(reason) = unknown_reason {
            self.mark_unknown_node(task_id, node_id, &reason);
            self.observe_duration(task_id, node_id, duration);
            return;
        }
        self.release_grant(task_id, node_id);
        match completion.outcome.status {
            OutcomeStatus::Success => {
                let outputs = completion.outcome.outputs;
                if let Some(run) = self.tasks.get_mut(&task_id) {
                    if let Some(node) = run.nodes.get_mut(&node_id) {
                        node.status = NodeStatus::Succeeded;
                        node.outputs.clone_from(&outputs);
                    }
                    run.outputs.insert(node_id, outputs);
                }
            }
            OutcomeStatus::Failed { error } => {
                let (made, budget, backoff, cancelled) =
                    self.tasks.get(&task_id).map_or((0, 1, 0, true), |run| {
                        (
                            run.attempts.get(&node_id).copied().unwrap_or(0),
                            run.graph
                                .nodes
                                .get(&node_id)
                                .map_or(1, |node| node.retry.attempts),
                            run.graph
                                .nodes
                                .get(&node_id)
                                .map_or(0, |node| node.retry.backoff_ms),
                            run.scope.is_cancelled(),
                        )
                    });
                if made < budget && !cancelled {
                    if let Some(run) = self.tasks.get_mut(&task_id)
                        && let Some(node) = run.nodes.get_mut(&node_id)
                    {
                        node.status = NodeStatus::Pending;
                        if backoff > 0 {
                            node.blocked_until =
                                Some(Instant::now() + Duration::from_millis(backoff));
                        }
                    }
                    tracing::debug!(node = ?node_id, error, "node failed; retrying");
                } else if let Some(run) = self.tasks.get_mut(&task_id)
                    && let Some(node) = run.nodes.get_mut(&node_id)
                {
                    node.status = NodeStatus::Failed;
                }
            }
            OutcomeStatus::Cancelled => {
                if let Some(run) = self.tasks.get_mut(&task_id)
                    && let Some(node) = run.nodes.get_mut(&node_id)
                {
                    node.status = NodeStatus::Cancelled;
                }
            }
            OutcomeStatus::Unknown { .. } => unreachable!("handled above"),
        }
        self.observe_duration(task_id, node_id, duration);
        self.update_finished(task_id);
        self.settle_cancellations(task_id);
    }

    /// Records a duration sample for the node's capability estimate.
    fn observe_duration(&mut self, task_id: TaskId, node_id: NodeId, duration: Duration) {
        let key = self
            .tasks
            .get(&task_id)
            .and_then(|run| run.graph.nodes.get(&node_id))
            .map(|node| node.invocation.capability.0.clone());
        let Some(key) = key else {
            return;
        };
        let sample = duration.as_secs_f64() * 1_000.0;
        let current = self
            .estimates
            .get(&key)
            .copied()
            .unwrap_or(DEFAULT_ESTIMATE_MS);
        self.estimates
            .insert(key, current + ESTIMATE_ALPHA * (sample - current));
    }

    /// Records a terminal node without executor involvement (input
    /// resolution failure, synthesized cancel).
    fn finish_node(
        &mut self,
        task_id: TaskId,
        node_id: NodeId,
        status: NodeStatus,
        outputs: Option<serde_json::Map<String, serde_json::Value>>,
    ) {
        if let Some(run) = self.tasks.get_mut(&task_id)
            && let Some(node) = run.nodes.get_mut(&node_id)
            && !node.status.is_terminal()
        {
            node.status = status;
            if let Some(outputs) = outputs {
                node.outputs.clone_from(&outputs);
                run.outputs.insert(node_id, outputs);
            }
        }
        self.release_grant(task_id, node_id);
        self.update_finished(task_id);
        self.settle_cancellations(task_id);
    }

    fn update_finished(&mut self, task_id: TaskId) {
        if let Some(run) = self.tasks.get_mut(&task_id) {
            run.finished = run.nodes.values().all(|node| node.status.is_terminal());
        }
    }

    /// Estimated remaining critical-path ms from `node` to graph exit.
    fn critical_path(
        &self,
        run: &TaskRun,
        node_id: NodeId,
        memo: &mut HashMap<NodeId, f64>,
    ) -> f64 {
        if let Some(score) = memo.get(&node_id) {
            return *score;
        }
        let mut best: f64 = 0.0;
        for edge in &run.graph.dependencies {
            if edge.from == node_id {
                let child_estimate = run.graph.nodes.get(&edge.to).map_or(0.0, |child| {
                    self.estimates
                        .get(&child.invocation.capability.0)
                        .copied()
                        .unwrap_or(DEFAULT_ESTIMATE_MS)
                });
                best = best.max(child_estimate + self.critical_path(run, edge.to, memo));
            }
        }
        memo.insert(node_id, best);
        best
    }

    /// One scheduling pass: refresh readiness, skip the unsatisfiable,
    /// score the ready, grant and dispatch in order.
    fn pump(&mut self, running: &mut JoinSet<Completion>) {
        // Refresh readiness and propagate skips to a fixpoint.
        loop {
            let mut changed = false;
            for task_id in self.tasks.keys().copied().collect::<Vec<_>>() {
                changed |= self.refresh_task(task_id);
            }
            if !changed {
                break;
            }
        }
        // Score ready nodes across tasks.
        let mut scored: Vec<(TaskId, NodeId, f64, bool)> = Vec::new();
        for run in self.tasks.values() {
            let mut memo = HashMap::new();
            for (node_id, node) in &run.nodes {
                if node.status != NodeStatus::Ready {
                    continue;
                }
                let Some(def) = run.graph.nodes.get(node_id) else {
                    continue;
                };
                let now = Instant::now();
                let age = node.ready_at.map_or(0.0, |ready| {
                    now.duration_since(ready).as_secs_f64() * AGE_BONUS_PER_SEC
                });
                let penalty = match def.speculation {
                    SpeculationPolicy::Forbidden | SpeculationPolicy::Preferred => 0.0,
                    SpeculationPolicy::Allowed => SPECULATION_PENALTY,
                };
                let score =
                    self.critical_path(run, *node_id, &mut memo) + def.priority.score() + age
                        - penalty;
                let speculative = !matches!(def.speculation, SpeculationPolicy::Forbidden);
                scored.push((run.task_id, *node_id, score, speculative));
            }
        }
        scored.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        let any_forbidden_ready = scored.iter().any(|(_, _, _, speculative)| !speculative);
        for (task_id, node_id, _, speculative) in scored {
            // Allowed speculation waits until no committed work is ready.
            if speculative && any_forbidden_ready {
                let def = self
                    .tasks
                    .get(&task_id)
                    .and_then(|run| run.graph.nodes.get(&node_id));
                if matches!(
                    def.map(|node| node.speculation),
                    Some(SpeculationPolicy::Allowed)
                ) {
                    continue;
                }
            }
            self.try_dispatch(task_id, node_id, running);
        }
    }

    /// Moves Pending nodes to Ready/Skipped. Returns true on any change.
    fn refresh_task(&mut self, task_id: TaskId) -> bool {
        let now = Instant::now();
        let mut changed = false;
        let pending: Vec<NodeId> = self
            .tasks
            .get(&task_id)
            .map(|run| {
                run.nodes
                    .iter()
                    .filter_map(|(id, node)| (node.status == NodeStatus::Pending).then_some(*id))
                    .collect()
            })
            .unwrap_or_default();
        for node_id in pending {
            let decision = self.pending_decision(task_id, node_id, now);
            match decision {
                PendingDecision::Wait => {}
                PendingDecision::Ready => {
                    if let Some(run) = self.tasks.get_mut(&task_id)
                        && let Some(node) = run.nodes.get_mut(&node_id)
                    {
                        node.status = NodeStatus::Ready;
                        if node.ready_at.is_none() {
                            node.ready_at = Some(now);
                        }
                        changed = true;
                    }
                }
                PendingDecision::Skip => {
                    if let Some(run) = self.tasks.get_mut(&task_id)
                        && let Some(node) = run.nodes.get_mut(&node_id)
                    {
                        node.status = NodeStatus::Skipped;
                        changed = true;
                    }
                    self.update_finished(task_id);
                }
            }
        }
        changed
    }

    fn pending_decision(&self, task_id: TaskId, node_id: NodeId, now: Instant) -> PendingDecision {
        let Some(run) = self.tasks.get(&task_id) else {
            return PendingDecision::Wait;
        };
        let Some(node) = run.nodes.get(&node_id) else {
            return PendingDecision::Wait;
        };
        if let Some(until) = node.blocked_until
            && now < until
        {
            return PendingDecision::Wait;
        }
        for edge in run
            .graph
            .dependencies
            .iter()
            .filter(|edge| edge.to == node_id)
        {
            let Some(parent) = run.nodes.get(&edge.from) else {
                continue;
            };
            if !parent.status.is_terminal() {
                return PendingDecision::Wait;
            }
            if !condition_matches(parent.status, edge.condition) {
                return PendingDecision::Skip;
            }
        }
        PendingDecision::Ready
    }

    /// Attempts one dispatch: resolves inputs, checks speculation safety,
    /// grants access/resources atomically, then spawns execution.
    fn try_dispatch(
        &mut self,
        task_id: TaskId,
        node_id: NodeId,
        running: &mut JoinSet<Completion>,
    ) {
        let Some(run) = self.tasks.get(&task_id) else {
            return;
        };
        if run.scope.is_cancelled() {
            return;
        }
        let Some(def) = run.graph.nodes.get(&node_id).cloned() else {
            return;
        };
        if !matches!(def.speculation, SpeculationPolicy::Forbidden)
            && !def.effect_class.speculation_safe()
        {
            return;
        }
        // Resolve inputs before taking any grant.
        let inputs = match self.resolve_inputs(task_id, &def) {
            Ok(inputs) => inputs,
            Err(error) => {
                self.finish_node(task_id, node_id, NodeStatus::Failed, None);
                tracing::warn!(node = ?node_id, error, "input resolution failed");
                return;
            }
        };
        if !self.grantable(&def) {
            return;
        }
        let Some(executor) = self.registry.get(&def.executor).cloned() else {
            self.finish_node(task_id, node_id, NodeStatus::Failed, None);
            return;
        };
        self.grant(task_id, node_id, &def);
        if let Some(run) = self.tasks.get_mut(&task_id) {
            if let Some(node) = run.nodes.get_mut(&node_id) {
                node.status = NodeStatus::Running;
            }
            let made = run.attempts.entry(node_id).or_insert(0);
            *made += 1;
        }
        let child = run_scope_child(self, task_id);
        let timeout = def.timeout.hard_ms;
        let worker = running.spawn(async move {
            let outcome = run_one(executor, def, inputs, child, timeout).await;
            Completion {
                task_id,
                node_id,
                outcome,
            }
        });
        self.in_flight.insert(worker.id(), (task_id, node_id));
    }

    /// Resolves a node's input bindings against ancestors' outputs.
    fn resolve_inputs(
        &self,
        task_id: TaskId,
        def: &ExecutionNode,
    ) -> Result<ResolvedInputs, String> {
        let Some(run) = self.tasks.get(&task_id) else {
            return Err("unknown task".to_owned());
        };
        let mut resolved = serde_json::Map::new();
        for binding in &def.inputs {
            let outputs = run.outputs.get(&binding.from).ok_or_else(|| {
                format!(
                    "input {:?} has no outputs from {:?}",
                    binding.name, binding.from
                )
            })?;
            let value = serde_json::Value::Object(outputs.clone());
            let found = value.pointer(&binding.pointer).cloned().ok_or_else(|| {
                format!(
                    "pointer {:?} missing in outputs of {:?}",
                    binding.pointer, binding.from
                )
            })?;
            resolved.insert(binding.name.clone(), found);
        }
        Ok(resolved)
    }

    /// True when no running grant conflicts and budgets fit.
    fn grantable(&self, def: &ExecutionNode) -> bool {
        for grant in &self.grants {
            if grant.access.conflicts_with(&def.access) {
                return false;
            }
        }
        let claim = &def.resources;
        self.used.cpu + u32::from(claim.cpu_units) <= self.budgets.cpu_units
            && self.used.process_slots + u32::from(claim.process_slots)
                <= self.budgets.process_slots
            && self.used.network_slots + u32::from(claim.network_slots)
                <= self.budgets.network_slots
            && self.used.memory_mb + u64::from(claim.memory_mb.unwrap_or(0))
                <= self.budgets.memory_mb
    }

    /// Records a grant and consumes budget. Caller checked [`Self::grantable`]
    /// in the same synchronous pass, so check-and-insert is atomic.
    fn grant(&mut self, task_id: TaskId, node_id: NodeId, def: &ExecutionNode) {
        let claim = &def.resources;
        self.used.cpu += u32::from(claim.cpu_units);
        self.used.process_slots += u32::from(claim.process_slots);
        self.used.network_slots += u32::from(claim.network_slots);
        self.used.memory_mb += u64::from(claim.memory_mb.unwrap_or(0));
        self.grants.push(Grant {
            task_id,
            node_id,
            access: def.access.clone(),
            cpu: u32::from(claim.cpu_units),
            process_slots: u32::from(claim.process_slots),
            network_slots: u32::from(claim.network_slots),
            memory_mb: u64::from(claim.memory_mb.unwrap_or(0)),
        });
    }
}

enum PendingDecision {
    Wait,
    Ready,
    Skip,
}

fn condition_matches(status: NodeStatus, condition: DependencyCondition) -> bool {
    match condition {
        DependencyCondition::OnSuccess => status == NodeStatus::Succeeded,
        DependencyCondition::OnFailure => status == NodeStatus::Failed,
        DependencyCondition::OnCompletion => status.is_terminal(),
    }
}

fn run_scope_child(app: &Loop, task_id: TaskId) -> CancellationToken {
    app.tasks
        .get(&task_id)
        .map_or_else(CancellationToken::new, |run| run.scope.child_token())
}

/// Runs one attempt. Cancellation and timeout are cooperative: signal the
/// executor, then retain the future (and its scheduler grant) until it returns.
async fn run_one(
    executor: Arc<dyn Executor>,
    node: ExecutionNode,
    inputs: ResolvedInputs,
    cancel: CancellationToken,
    timeout_ms: Option<u64>,
) -> NodeOutcome {
    let mut execution = Box::pin(executor.execute(&node, inputs, cancel.clone()));
    if let Some(ms) = timeout_ms {
        let mut deadline = Box::pin(tokio::time::sleep(Duration::from_millis(ms)));
        tokio::select! {
            biased;
            outcome = &mut execution => outcome,
            () = cancel.cancelled() => execution.await,
            () = &mut deadline => {
                cancel.cancel();
                let outcome = execution.await;
                if outcome.status == OutcomeStatus::Cancelled {
                    NodeOutcome::failed(format!("timeout after {ms}ms"), outcome.duration)
                } else {
                    // A worker that completed successfully after its deadline
                    // must not be reported as failed: a consequential effect
                    // may already have happened. Keep the known outcome.
                    outcome
                }
            }
        }
    } else {
        tokio::select! {
            biased;
            outcome = &mut execution => outcome,
            () = cancel.cancelled() => execution.await,
        }
    }
}

/// Test and gate helpers live here so unit and property tests share them.
#[cfg(test)]
pub mod test_support {
    use super::{Budgets, ExecutorRegistry, SchedulerHandle, spawn};
    use crate::executor::FakeExecutor;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tachyon_ir::{
        AccessSet, CancellationPolicy, EffectClass, ExecutionGraph, ExecutionNode, ExecutorKind,
        Invocation, NodePriority, ResourceClaim, RetryPolicy, SpeculationPolicy, TimeoutPolicy,
    };
    use tachyon_types::{CapabilityId, NodeId, TaskId};

    /// Builds a pure native node with `writes` write keys.
    #[must_use]
    pub fn test_node(task: TaskId, writes: Vec<&str>) -> ExecutionNode {
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
                writes: writes
                    .into_iter()
                    .map(|key| tachyon_ir::ResourceKey(format!("file:{key}")))
                    .collect(),
            },
            resources: ResourceClaim::default(),
            effect_class: EffectClass::Pure,
            idempotency: tachyon_ir::Idempotency::Pure,
            speculation: SpeculationPolicy::Forbidden,
            timeout: TimeoutPolicy::default(),
            retry: RetryPolicy::default(),
            cancellation: CancellationPolicy::Immediate,
            verification: vec![],
            priority: NodePriority::Normal,
        }
    }

    /// Spawns a scheduler with one fake executor of each requested kind.
    #[must_use]
    pub fn spawn_with_fake(
        kinds: &[ExecutorKind],
        latency: Duration,
    ) -> (
        SchedulerHandle,
        Vec<Arc<FakeExecutor>>,
        tokio::task::JoinHandle<()>,
    ) {
        let mut registry: ExecutorRegistry = HashMap::new();
        let mut fakes = Vec::new();
        for kind in kinds {
            let fake = Arc::new(FakeExecutor::new(*kind, latency));
            registry.insert(*kind, fake.clone());
            fakes.push(fake);
        }
        let (handle, join) = spawn(Budgets::default(), registry);
        (handle, fakes, join)
    }

    /// Runs `body` on a current-thread runtime (for proptest sync tests).
    pub fn block_on<F, T>(body: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
            .block_on(body)
    }

    /// Builds a graph from nodes and `(from_index, to_index)` edges.
    #[must_use]
    pub fn assemble(
        _task: TaskId,
        nodes: Vec<ExecutionNode>,
        edges: &[(usize, usize)],
        condition: tachyon_ir::DependencyCondition,
    ) -> ExecutionGraph {
        let ids: Vec<NodeId> = nodes.iter().map(|node| node.id).collect();
        ExecutionGraph {
            version: tachyon_ir::IR_VERSION,
            nodes: nodes.into_iter().map(|node| (node.id, node)).collect(),
            dependencies: edges
                .iter()
                .map(|(from, to)| tachyon_ir::Dependency {
                    from: ids[*from],
                    to: ids[*to],
                    condition,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{assemble, block_on, spawn_with_fake, test_node};
    use super::{Budgets, ExecutorRegistry, SchedulerError, finish_poll_delay, spawn};
    use crate::executor::{Executor, NodeOutcome, ResolvedInputs};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tachyon_ir::{
        DependencyCondition, EffectClass, ExecutionNode, ExecutorKind, Idempotency, NodeStatus,
    };
    use tachyon_types::{NodeId, TaskId};
    use tokio::sync::{Notify, mpsc};

    #[test]
    fn fast_finish_poll_window_is_pinned_before_slow_backoff() {
        assert_eq!(finish_poll_delay(0), Duration::from_millis(1));
        assert_eq!(finish_poll_delay(49), Duration::from_millis(1));
        assert_eq!(finish_poll_delay(50), Duration::from_millis(10));
        assert_eq!(finish_poll_delay(u32::MAX), Duration::from_millis(10));
    }

    enum DrainEvent {
        Started(NodeId),
        CancellationObserved(NodeId),
    }

    struct DrainExecutor {
        blocked_node: NodeId,
        events: mpsc::UnboundedSender<DrainEvent>,
        release: Arc<Notify>,
        succeed_after_cancel: bool,
    }

    struct PanicOnCancelExecutor {
        events: mpsc::UnboundedSender<DrainEvent>,
    }

    struct UnknownAfterCancelExecutor {
        events: mpsc::UnboundedSender<DrainEvent>,
        release: Arc<Notify>,
    }

    struct UnknownWithSlowSiblingExecutor {
        events: mpsc::UnboundedSender<DrainEvent>,
        unknown_node: NodeId,
        release_unknown: Arc<Notify>,
        release_sibling: Arc<Notify>,
    }

    struct UnknownCancelsTaskExecutor {
        events: mpsc::UnboundedSender<DrainEvent>,
        unknown_node: NodeId,
        sibling_node: NodeId,
        release_sibling: Arc<Notify>,
    }

    #[async_trait]
    impl Executor for DrainExecutor {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Native
        }

        async fn execute(
            &self,
            node: &ExecutionNode,
            _inputs: ResolvedInputs,
            cancel: tokio_util::sync::CancellationToken,
        ) -> NodeOutcome {
            let started = Instant::now();
            self.events.send(DrainEvent::Started(node.id)).unwrap();
            if node.id == self.blocked_node {
                cancel.cancelled().await;
                self.events
                    .send(DrainEvent::CancellationObserved(node.id))
                    .unwrap();
                self.release.notified().await;
                if self.succeed_after_cancel
                    || node.cancellation
                        == tachyon_ir::CancellationPolicy::NonCancellableAfterCommit
                {
                    NodeOutcome::success(ResolvedInputs::new(), started.elapsed())
                } else {
                    NodeOutcome::cancelled(started.elapsed())
                }
            } else {
                NodeOutcome::success(ResolvedInputs::new(), started.elapsed())
            }
        }
    }

    #[async_trait]
    impl Executor for PanicOnCancelExecutor {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Native
        }

        async fn execute(
            &self,
            node: &ExecutionNode,
            _inputs: ResolvedInputs,
            cancel: tokio_util::sync::CancellationToken,
        ) -> NodeOutcome {
            self.events.send(DrainEvent::Started(node.id)).unwrap();
            cancel.cancelled().await;
            panic!("simulated executor panic with an unresolved effect outcome");
        }
    }

    #[async_trait]
    impl Executor for UnknownAfterCancelExecutor {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Native
        }

        async fn execute(
            &self,
            node: &ExecutionNode,
            _inputs: ResolvedInputs,
            cancel: tokio_util::sync::CancellationToken,
        ) -> NodeOutcome {
            let started = Instant::now();
            self.events.send(DrainEvent::Started(node.id)).unwrap();
            cancel.cancelled().await;
            self.events
                .send(DrainEvent::CancellationObserved(node.id))
                .unwrap();
            self.release.notified().await;
            NodeOutcome::unknown(
                "simulated ambiguous effect outcome".into(),
                started.elapsed(),
            )
        }
    }

    #[async_trait]
    impl Executor for UnknownWithSlowSiblingExecutor {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Native
        }

        async fn execute(
            &self,
            node: &ExecutionNode,
            _inputs: ResolvedInputs,
            cancel: tokio_util::sync::CancellationToken,
        ) -> NodeOutcome {
            let started = Instant::now();
            self.events.send(DrainEvent::Started(node.id)).unwrap();
            cancel.cancelled().await;
            self.events
                .send(DrainEvent::CancellationObserved(node.id))
                .unwrap();
            if node.id == self.unknown_node {
                self.release_unknown.notified().await;
                NodeOutcome::unknown(
                    "simulated ambiguous effect outcome".into(),
                    started.elapsed(),
                )
            } else {
                self.release_sibling.notified().await;
                NodeOutcome::cancelled(started.elapsed())
            }
        }
    }

    #[async_trait]
    impl Executor for UnknownCancelsTaskExecutor {
        fn kind(&self) -> ExecutorKind {
            ExecutorKind::Native
        }

        async fn execute(
            &self,
            node: &ExecutionNode,
            _inputs: ResolvedInputs,
            cancel: tokio_util::sync::CancellationToken,
        ) -> NodeOutcome {
            let started = Instant::now();
            self.events.send(DrainEvent::Started(node.id)).unwrap();
            if node.id == self.unknown_node {
                return NodeOutcome::unknown(
                    "simulated ambiguous effect outcome".into(),
                    started.elapsed(),
                );
            }
            if node.id == self.sibling_node {
                cancel.cancelled().await;
                self.events
                    .send(DrainEvent::CancellationObserved(node.id))
                    .unwrap();
                self.release_sibling.notified().await;
            }
            NodeOutcome::success(ResolvedInputs::new(), started.elapsed())
        }
    }

    #[tokio::test]
    async fn retries_require_executor_trust_and_consistent_effect_metadata() {
        let task = TaskId::generate();
        let mut forged = test_node(task, vec!["shared"]);
        forged.effect_class = EffectClass::DestructiveExternalMutation;
        forged.idempotency = Idempotency::Pure;
        forged.retry.attempts = 2;
        let node_id = forged.id;
        let graph = assemble(task, vec![forged], &[], DependencyCondition::OnSuccess);
        let (handle, _, _join) = spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(1));

        assert!(matches!(
            handle.submit(task, graph).await,
            Err(SchedulerError::RetryNotAuthorized(id)) if id == node_id
        ));
    }

    #[tokio::test]
    async fn panicking_executor_preserves_unknown_outcome_and_grant() {
        let first_task = TaskId::generate();
        let first_node = test_node(first_task, vec!["shared"]);
        let first_id = first_node.id;
        let first_graph = assemble(
            first_task,
            vec![first_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let second_task = TaskId::generate();
        let second_node = test_node(second_task, vec!["shared"]);
        let second_id = second_node.id;
        let second_graph = assemble(
            second_task,
            vec![second_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(
            ExecutorKind::Native,
            Arc::new(PanicOnCancelExecutor { events: events_tx }),
        );
        let (handle, join) = spawn(Budgets::default(), registry);
        handle.submit(first_task, first_graph).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == first_id
        ));

        let cancel_handle = handle.clone();
        let cancellation = tokio::spawn(async move { cancel_handle.cancel_task(first_task).await });
        assert!(matches!(
            cancellation.await.unwrap(),
            Err(SchedulerError::UnknownOutcome { task_id, node_id })
                if task_id == first_task && node_id == first_id
        ));
        let first = handle.status(first_task).await.unwrap();
        assert!(!first.finished);
        assert_eq!(first.statuses[&first_id], NodeStatus::UnknownAfterCrash);

        handle.submit(second_task, second_graph).await.unwrap();
        let second = handle.status(second_task).await.unwrap();
        assert_eq!(second.statuses[&second_id], NodeStatus::Ready);

        handle.cancel_task(second_task).await.unwrap();
        drop(handle);
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .expect("scheduler shutdown did not drain")
            .unwrap();
    }

    #[tokio::test]
    async fn explicit_unknown_outcome_keeps_conflicting_grant_and_surfaces_to_waiter() {
        let first_task = TaskId::generate();
        let first_node = test_node(first_task, vec!["shared"]);
        let first_id = first_node.id;
        let first_graph = assemble(
            first_task,
            vec![first_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let second_task = TaskId::generate();
        let second_node = test_node(second_task, vec!["shared"]);
        let second_id = second_node.id;
        let second_graph = assemble(
            second_task,
            vec![second_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(
            ExecutorKind::Native,
            Arc::new(UnknownAfterCancelExecutor {
                events: events_tx,
                release: release.clone(),
            }),
        );
        let (handle, join) = spawn(Budgets::default(), registry);

        handle.submit(first_task, first_graph).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == first_id
        ));
        let cancel_handle = handle.clone();
        let cancellation = tokio::spawn(async move { cancel_handle.cancel_task(first_task).await });
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::CancellationObserved(id)) if id == first_id
        ));

        handle.submit(second_task, second_graph).await.unwrap();
        assert_eq!(
            handle.status(second_task).await.unwrap().statuses[&second_id],
            NodeStatus::Ready,
            "conflicting work ran while the first outcome was unsettled"
        );
        assert!(
            !cancellation.is_finished(),
            "unknown outcome returned before drain"
        );

        release.notify_one();
        assert!(matches!(
            cancellation.await.unwrap(),
            Err(SchedulerError::UnknownOutcome { task_id, node_id })
                if task_id == first_task && node_id == first_id
        ));
        let first = handle.status(first_task).await.unwrap();
        assert!(!first.finished);
        assert_eq!(first.statuses[&first_id], NodeStatus::UnknownAfterCrash);
        assert_eq!(first.unresolved_node, Some(first_id));

        drop(handle);
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .expect("scheduler shutdown did not drain")
            .unwrap();
    }

    #[tokio::test]
    async fn cancel_waiter_drains_siblings_before_returning_unknown_outcome() {
        let task = TaskId::generate();
        let nodes: Vec<_> = (0..2).map(|_| test_node(task, vec![])).collect();
        let unknown_node = nodes[0].id;
        let sibling_node = nodes[1].id;
        let graph = assemble(task, nodes, &[], DependencyCondition::OnSuccess);

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let release_unknown = Arc::new(Notify::new());
        let release_sibling = Arc::new(Notify::new());
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(
            ExecutorKind::Native,
            Arc::new(UnknownWithSlowSiblingExecutor {
                events: events_tx,
                unknown_node,
                release_unknown: release_unknown.clone(),
                release_sibling: release_sibling.clone(),
            }),
        );
        let (handle, join) = spawn(Budgets::default(), registry);
        handle.submit(task, graph).await.unwrap();

        let mut started = Vec::new();
        for _ in 0..2 {
            match tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap()
            {
                Some(DrainEvent::Started(id)) => started.push(id),
                _ => panic!("expected both independent executors to start"),
            }
        }
        started.sort_unstable();
        let mut expected = vec![unknown_node, sibling_node];
        expected.sort_unstable();
        assert_eq!(started, expected);

        let cancel_handle = handle.clone();
        let cancellation = tokio::spawn(async move { cancel_handle.cancel_task(task).await });
        let mut cancellation_observed = Vec::new();
        for _ in 0..2 {
            match tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap()
            {
                Some(DrainEvent::CancellationObserved(id)) => cancellation_observed.push(id),
                _ => panic!("expected both executors to observe cancellation"),
            }
        }
        cancellation_observed.sort_unstable();
        assert_eq!(cancellation_observed, expected);

        release_unknown.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = handle.status(task).await.unwrap();
                if snapshot.statuses[&unknown_node] == NodeStatus::UnknownAfterCrash {
                    assert_eq!(snapshot.statuses[&sibling_node], NodeStatus::Running);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first executor did not report its unknown outcome");
        assert!(
            !cancellation.is_finished(),
            "cancel_task returned while a sibling executor was still running"
        );

        release_sibling.notify_one();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), cancellation)
                .await
                .unwrap()
                .unwrap(),
            Err(SchedulerError::UnknownOutcome { task_id, node_id })
                if task_id == task && node_id == unknown_node
        ));
        let snapshot = handle.status(task).await.unwrap();
        assert_eq!(snapshot.statuses[&sibling_node], NodeStatus::Cancelled);
        assert_eq!(snapshot.unresolved_node, Some(unknown_node));

        drop(handle);
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .expect("scheduler shutdown did not drain")
            .unwrap();
    }

    #[tokio::test]
    async fn unknown_effect_cancels_pending_nodes_and_drains_running_siblings() {
        let task = TaskId::generate();
        let nodes: Vec<_> = (0..3).map(|_| test_node(task, vec![])).collect();
        let unknown_node = nodes[0].id;
        let sibling_node = nodes[1].id;
        let dependent_node = nodes[2].id;
        let graph = assemble(task, nodes, &[(1, 2)], DependencyCondition::OnSuccess);

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let release_sibling = Arc::new(Notify::new());
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(
            ExecutorKind::Native,
            Arc::new(UnknownCancelsTaskExecutor {
                events: events_tx,
                unknown_node,
                sibling_node,
                release_sibling: release_sibling.clone(),
            }),
        );
        let (handle, join) = spawn(Budgets::default(), registry);
        handle.submit(task, graph).await.unwrap();

        let mut started = Vec::new();
        for _ in 0..2 {
            match tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap()
            {
                Some(DrainEvent::Started(id)) => started.push(id),
                _ => panic!("expected unknown node and sibling to start"),
            }
        }
        started.sort_unstable();
        let mut expected = vec![unknown_node, sibling_node];
        expected.sort_unstable();
        assert_eq!(started, expected);

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::CancellationObserved(id)) if id == sibling_node
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = handle.status(task).await.unwrap();
                if snapshot.unresolved_node == Some(unknown_node) {
                    assert_eq!(snapshot.statuses[&sibling_node], NodeStatus::Running);
                    assert_eq!(snapshot.statuses[&dependent_node], NodeStatus::Cancelled);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unknown effect did not stop later task work");

        release_sibling.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = handle.status(task).await.unwrap();
                if snapshot.statuses[&sibling_node] == NodeStatus::Succeeded {
                    assert_eq!(snapshot.statuses[&dependent_node], NodeStatus::Cancelled);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sibling did not drain after release");

        drop(handle);
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .expect("scheduler shutdown did not drain")
            .unwrap();
    }

    #[tokio::test]
    async fn independent_nodes_run_concurrently() {
        let task = TaskId::generate();
        let nodes: Vec<_> = (0..6).map(|_| test_node(task, vec![])).collect();
        let graph = assemble(task, nodes, &[], DependencyCondition::OnSuccess);
        let (handle, fakes, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(50));
        let tracker = fakes[0].tracker();
        let started = Instant::now();
        handle.submit(task, graph).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        let elapsed = started.elapsed();
        assert!(snapshot.finished);
        assert!(elapsed < Duration::from_millis(250), "took {elapsed:?}");
        assert!(tracker.max_concurrent() > 1, "ran serially");
        assert!(tracker.violations().is_empty());
    }

    #[tokio::test]
    async fn dependencies_are_honored() {
        let task = TaskId::generate();
        let nodes: Vec<_> = (0..3).map(|_| test_node(task, vec![])).collect();
        let ids: Vec<_> = nodes.iter().map(|node| node.id).collect();
        let graph = assemble(
            task,
            nodes,
            &[(0, 1), (1, 2)],
            DependencyCondition::OnSuccess,
        );
        let (handle, fakes, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(10));
        let tracker = fakes[0].tracker();
        handle.submit(task, graph).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(snapshot.finished);
        assert_eq!(tracker.start_order(), ids);
    }

    #[tokio::test]
    async fn conflicting_writes_never_overlap() {
        let task = TaskId::generate();
        let first = test_node(task, vec!["/shared"]);
        let second = test_node(task, vec!["/shared"]);
        let graph = assemble(
            task,
            vec![first, second],
            &[],
            DependencyCondition::OnSuccess,
        );
        let (handle, fakes, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(30));
        let tracker = fakes[0].tracker();
        handle.submit(task, graph).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(snapshot.finished);
        assert_eq!(tracker.max_concurrent(), 1);
        assert!(tracker.violations().is_empty());
        assert!(
            snapshot
                .statuses
                .values()
                .all(|status| *status == NodeStatus::Succeeded)
        );
    }

    #[tokio::test]
    async fn failure_paths_skip_and_recover() {
        let task = TaskId::generate();
        let mut failing = test_node(task, vec![]);
        failing.retry.attempts = 1;
        let on_success = test_node(task, vec![]);
        let on_failure = test_node(task, vec![]);
        let ids: Vec<_> = [&failing, &on_success, &on_failure]
            .iter()
            .map(|node| node.id)
            .collect();
        let mut graph = assemble(
            task,
            vec![failing.clone(), on_success.clone(), on_failure.clone()],
            &[(0, 1), (0, 2)],
            DependencyCondition::OnSuccess,
        );
        graph.dependencies[1].condition = DependencyCondition::OnFailure;
        let (handle, fakes, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(5));
        fakes[0].fail_times(ids[0], 1);
        handle.submit(task, graph).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(snapshot.finished);
        assert_eq!(snapshot.statuses[&ids[0]], NodeStatus::Failed);
        assert_eq!(snapshot.statuses[&ids[1]], NodeStatus::Skipped);
        assert_eq!(snapshot.statuses[&ids[2]], NodeStatus::Succeeded);
    }

    #[tokio::test]
    async fn retry_succeeds_within_budget() {
        let task = TaskId::generate();
        let mut flaky = test_node(task, vec![]);
        flaky.retry.attempts = 3;
        let id = flaky.id;
        let graph = assemble(task, vec![flaky], &[], DependencyCondition::OnSuccess);
        let (handle, fakes, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(5));
        fakes[0].fail_times(id, 2);
        handle.submit(task, graph).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(snapshot.statuses[&id], NodeStatus::Succeeded);
        assert_eq!(snapshot.attempts[&id], 3);
    }

    #[tokio::test]
    async fn timeout_fails_the_attempt() {
        let task = TaskId::generate();
        let mut slow = test_node(task, vec![]);
        slow.timeout.hard_ms = Some(30);
        let id = slow.id;
        let graph = assemble(task, vec![slow], &[], DependencyCondition::OnSuccess);
        let (handle, _, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(500));
        handle.submit(task, graph).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(snapshot.statuses[&id], NodeStatus::Failed);
    }

    #[tokio::test]
    async fn cancellation_leaves_nothing_running() {
        let task = TaskId::generate();
        let nodes: Vec<_> = (0..8).map(|_| test_node(task, vec![])).collect();
        let graph = assemble(task, nodes, &[], DependencyCondition::OnSuccess);
        let (handle, fakes, _join) =
            spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(500));
        let tracker = fakes[0].tracker();
        handle.submit(task, graph).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.cancel_task(task).await.unwrap();
        let snapshot = handle
            .wait_finished(task, Duration::from_secs(10))
            .await
            .unwrap();
        assert!(snapshot.finished);
        assert!(
            snapshot
                .statuses
                .values()
                .all(|status| status.is_terminal())
        );
        assert_eq!(tracker.current(), 0);
        let running = snapshot
            .statuses
            .values()
            .filter(|status| **status == NodeStatus::Running)
            .count();
        assert_eq!(running, 0);
    }

    #[tokio::test]
    async fn cancellation_keeps_conflicting_grant_until_worker_drains() {
        let first_task = TaskId::generate();
        let first_node = test_node(first_task, vec!["shared"]);
        let first_id = first_node.id;
        let first_graph = assemble(
            first_task,
            vec![first_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let second_task = TaskId::generate();
        let second_node = test_node(second_task, vec!["shared"]);
        let second_id = second_node.id;
        let second_graph = assemble(
            second_task,
            vec![second_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let independent_task = TaskId::generate();
        let independent_node = test_node(independent_task, vec!["independent"]);
        let independent_id = independent_node.id;
        let independent_graph = assemble(
            independent_task,
            vec![independent_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let executor = Arc::new(DrainExecutor {
            blocked_node: first_id,
            events: events_tx,
            release: release.clone(),
            succeed_after_cancel: false,
        });
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(ExecutorKind::Native, executor);
        let (handle, _join) = spawn(Budgets::default(), registry);

        handle.submit(first_task, first_graph).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == first_id
        ));

        let cancel_handle = handle.clone();
        let cancellation = tokio::spawn(async move { cancel_handle.cancel_task(first_task).await });
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::CancellationObserved(id)) if id == first_id
        ));

        handle.submit(second_task, second_graph).await.unwrap();
        handle
            .submit(independent_task, independent_graph)
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == independent_id
        ));
        let waiting = handle.status(second_task).await.unwrap();
        assert_eq!(waiting.statuses[&second_id], NodeStatus::Ready);
        assert!(
            !cancellation.is_finished(),
            "cancel acknowledged before the worker drained"
        );

        release.notify_one();
        cancellation.await.unwrap().unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == second_id
        ));

        let second = handle
            .wait_finished(second_task, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(second.statuses[&second_id], NodeStatus::Succeeded);
        let first = handle
            .wait_finished(first_task, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(first.statuses[&first_id], NodeStatus::Cancelled);
    }

    #[tokio::test]
    async fn timeout_keeps_conflicting_grant_until_worker_drains() {
        let first_task = TaskId::generate();
        let mut first_node = test_node(first_task, vec!["shared"]);
        first_node.timeout.hard_ms = Some(20);
        first_node.cancellation = tachyon_ir::CancellationPolicy::NonCancellableAfterCommit;
        let first_id = first_node.id;
        let first_graph = assemble(
            first_task,
            vec![first_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let second_task = TaskId::generate();
        let second_node = test_node(second_task, vec!["shared"]);
        let second_id = second_node.id;
        let second_graph = assemble(
            second_task,
            vec![second_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let independent_task = TaskId::generate();
        let independent_node = test_node(independent_task, vec!["independent"]);
        let independent_id = independent_node.id;
        let independent_graph = assemble(
            independent_task,
            vec![independent_node],
            &[],
            DependencyCondition::OnSuccess,
        );

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let executor = Arc::new(DrainExecutor {
            blocked_node: first_id,
            events: events_tx,
            release: release.clone(),
            succeed_after_cancel: false,
        });
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(ExecutorKind::Native, executor);
        let (handle, _join) = spawn(Budgets::default(), registry);

        handle.submit(first_task, first_graph).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == first_id
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::CancellationObserved(id)) if id == first_id
        ));

        handle.submit(second_task, second_graph).await.unwrap();
        handle
            .submit(independent_task, independent_graph)
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == independent_id
        ));
        let first = handle.status(first_task).await.unwrap();
        assert!(
            !first.finished,
            "timeout finished before the worker drained"
        );
        assert_eq!(first.statuses[&first_id], NodeStatus::Running);
        let waiting = handle.status(second_task).await.unwrap();
        assert_eq!(waiting.statuses[&second_id], NodeStatus::Ready);

        release.notify_one();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == second_id
        ));
        let first = handle
            .wait_finished(first_task, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(first.statuses[&first_id], NodeStatus::Succeeded);
        let second = handle
            .wait_finished(second_task, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(second.statuses[&second_id], NodeStatus::Succeeded);
    }

    #[tokio::test]
    async fn scheduler_shutdown_waits_for_running_worker_drain() {
        let task = TaskId::generate();
        let node = test_node(task, vec!["shared"]);
        let node_id = node.id;
        let graph = assemble(task, vec![node], &[], DependencyCondition::OnSuccess);

        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let executor = Arc::new(DrainExecutor {
            blocked_node: node_id,
            events: events_tx,
            release: release.clone(),
            succeed_after_cancel: false,
        });
        let mut registry: ExecutorRegistry = HashMap::new();
        registry.insert(ExecutorKind::Native, executor);
        let (handle, join) = spawn(Budgets::default(), registry);

        handle.submit(task, graph).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::Started(id)) if id == node_id
        ));
        drop(handle);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), events_rx.recv())
                .await
                .unwrap(),
            Some(DrainEvent::CancellationObserved(id)) if id == node_id
        ));
        assert!(
            !join.is_finished(),
            "scheduler shut down before its worker drained"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .expect("scheduler did not exit after its worker drained")
            .unwrap();
    }

    #[test]
    fn random_graphs_never_overlap_conflicts() {
        use proptest::prelude::*;
        use std::collections::HashSet;

        let case = (
            2..=6_usize,
            prop::collection::vec((0..6_usize, 0..6_usize), 0..8),
            prop::collection::vec((0..3_usize, 0..2_usize), 0..6),
        );
        let mut runner =
            proptest::test_runner::TestRunner::new(proptest::test_runner::Config::with_cases(24));
        runner
            .run(&case, |(count, raw_edges, access)| {
                let task = TaskId::generate();
                let mut nodes: Vec<_> = (0..count).map(|_| test_node(task, vec![])).collect();
                let pool = ["/a", "/b", "/c"];
                for (index, (read, writes)) in access.iter().enumerate() {
                    if index >= nodes.len() {
                        break;
                    }
                    nodes[index].access.reads = vec![tachyon_ir::ResourceKey(format!(
                        "file:{}",
                        pool[read % pool.len()]
                    ))];
                    nodes[index].access.writes = (0..*writes)
                        .map(|k| {
                            tachyon_ir::ResourceKey(format!(
                                "file:{}",
                                pool[(read + k + 1) % pool.len()]
                            ))
                        })
                        .collect();
                }
                let mut seen = HashSet::new();
                let mut edges = Vec::new();
                for (from, to) in raw_edges {
                    let (from, to) = (from % count, to % count);
                    if from < to && seen.insert((from, to)) {
                        edges.push((from, to));
                    }
                }
                let graph = assemble(task, nodes.clone(), &edges, DependencyCondition::OnSuccess);
                prop_assert_eq!(graph.validate(task), Ok(()));
                // Spawn inside the runtime: `tokio::spawn` needs a reactor.
                let (snapshot, violations, start_order, finish_order) = block_on(async {
                    let (handle, fakes, _join) =
                        spawn_with_fake(&[ExecutorKind::Native], Duration::from_millis(5));
                    let tracker = fakes[0].tracker();
                    handle.submit(task, graph).await.unwrap();
                    let snapshot = handle
                        .wait_finished(task, Duration::from_secs(20))
                        .await
                        .unwrap();
                    (
                        snapshot,
                        tracker.violations(),
                        tracker.start_order(),
                        tracker.finish_order(),
                    )
                });
                prop_assert!(snapshot.finished);
                prop_assert!(violations.is_empty());
                // Dependency order: every edge's parent finished before the child started.
                let starts: HashMap<_, _> = start_order
                    .into_iter()
                    .enumerate()
                    .map(|(index, id)| (id, index))
                    .collect();
                let finishes: HashMap<_, _> = finish_order
                    .into_iter()
                    .enumerate()
                    .map(|(index, id)| (id, index))
                    .collect();
                let ids: Vec<_> = nodes.iter().map(|node| node.id).collect();
                for (from, to) in edges {
                    let (parent, child) = (ids[from], ids[to]);
                    // Skipped subtrees never run; only check executed pairs.
                    if let (Some(started), Some(finished)) =
                        (starts.get(&child), finishes.get(&parent))
                    {
                        prop_assert!(finished < started);
                    }
                }
                Ok(())
            })
            .unwrap();
    }
}
