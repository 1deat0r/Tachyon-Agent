//! Executors: the work behind [`ExecutorKind`](tachyon_ir::ExecutorKind).
//!
//! The scheduler owns readiness, grants, and bookkeeping; executors own
//! only the execution of one granted node. [`FakeExecutor`] is the
//! deterministic stand-in used by every Milestone 2 gate.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value};
use tachyon_ir::{AccessSet, EffectClass, ExecutionNode, ExecutorKind, Idempotency};
use tachyon_types::NodeId;
use tokio_util::sync::CancellationToken;

/// Inputs resolved from ancestors' outputs, by binding name.
pub type ResolvedInputs = Map<String, Value>;

/// What one node execution produced.
#[derive(Clone, Debug)]
pub struct NodeOutcome {
    /// Terminal result of the attempt.
    pub status: OutcomeStatus,
    /// Structured outputs, by [`OutputBinding`](tachyon_ir::OutputBinding) name.
    pub outputs: Map<String, Value>,
    /// Wall-clock time of the attempt.
    pub duration: Duration,
}

/// Attempt result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeStatus {
    /// Node succeeded.
    Success,
    /// Node failed; carries a short reason.
    Failed {
        /// Failure reason.
        error: String,
    },
    /// Node was cancelled after the executor confirmed it stopped before any
    /// consequential effect occurred.
    Cancelled,
    /// The executor cannot prove whether the consequential effect occurred.
    /// The scheduler must retain this node's grants for reconciliation.
    Unknown {
        /// Short reason the outcome could not be settled.
        reason: String,
    },
}

impl NodeOutcome {
    /// Successful attempt with `outputs`.
    #[must_use]
    pub fn success(outputs: Map<String, Value>, duration: Duration) -> Self {
        Self {
            status: OutcomeStatus::Success,
            outputs,
            duration,
        }
    }

    /// Failed attempt.
    #[must_use]
    pub fn failed(error: String, duration: Duration) -> Self {
        Self {
            status: OutcomeStatus::Failed { error },
            outputs: Map::new(),
            duration,
        }
    }

    /// Cancelled attempt with a confirmed no-effect outcome.
    #[must_use]
    pub fn cancelled(duration: Duration) -> Self {
        Self {
            status: OutcomeStatus::Cancelled,
            outputs: Map::new(),
            duration,
        }
    }

    /// Attempt whose effect outcome could not be determined.
    #[must_use]
    pub fn unknown(reason: String, duration: Duration) -> Self {
        Self {
            status: OutcomeStatus::Unknown { reason },
            outputs: Map::new(),
            duration,
        }
    }
}

/// Executes one granted node. Implementations must observe `cancel` and
/// settle their work safely. The future must not return until all work it
/// started has stopped or its effect outcome has been reconciled. The
/// scheduler retains the node's access and resource grants until then.
#[async_trait]
pub trait Executor: Send + Sync {
    /// Executor kind served.
    fn kind(&self) -> ExecutorKind;

    /// Whether the registered executor's trusted capability contract proves
    /// that repeating this exact node is safe. The default rejects retries;
    /// IR metadata alone is a proposal, not authority.
    fn automatic_retry_safe(&self, _node: &ExecutionNode) -> bool {
        false
    }

    /// Runs `node` with resolved `inputs`, returning its settled outcome.
    async fn execute(
        &self,
        node: &ExecutionNode,
        inputs: ResolvedInputs,
        cancel: CancellationToken,
    ) -> NodeOutcome;
}

/// One entry in the tracker's running set.
#[derive(Clone, Debug)]
struct RunningEntry {
    node: NodeId,
    access: AccessSet,
}

#[derive(Debug, Default)]
struct TrackerState {
    current: usize,
    max: usize,
    running: Vec<RunningEntry>,
    violations: Vec<String>,
    start_order: Vec<NodeId>,
    finish_order: Vec<NodeId>,
}

/// Observes a [`FakeExecutor`]: concurrency levels, start/finish order,
/// and access-conflict violations. Cloneable; shared with the executor.
#[derive(Clone, Debug, Default)]
pub struct Tracker {
    inner: Arc<Mutex<TrackerState>>,
}

impl Tracker {
    /// Highest concurrent execution count observed.
    #[must_use]
    pub fn max_concurrent(&self) -> usize {
        self.inner.lock().map_or(0, |state| state.max)
    }

    /// Executions still registered (zero when everything drained).
    #[must_use]
    pub fn current(&self) -> usize {
        self.inner.lock().map_or(0, |state| state.current)
    }

    /// Conflict violations recorded (`empty` means the scheduler never
    /// overlapped conflicting access sets).
    #[must_use]
    pub fn violations(&self) -> Vec<String> {
        self.inner
            .lock()
            .map_or_default(|state| state.violations.clone())
    }

    /// Node start order.
    #[must_use]
    pub fn start_order(&self) -> Vec<NodeId> {
        self.inner
            .lock()
            .map_or_default(|state| state.start_order.clone())
    }

    /// Node finish order.
    #[must_use]
    pub fn finish_order(&self) -> Vec<NodeId> {
        self.inner
            .lock()
            .map_or_default(|state| state.finish_order.clone())
    }
}

/// Unregisters from the tracker on drop, so aborted futures cannot leak
/// a phantom "running" entry.
struct RunningGuard {
    tracker: Tracker,
    node: NodeId,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.tracker.inner.lock() {
            state.running.retain(|entry| entry.node != self.node);
            state.current = state.current.saturating_sub(1);
            state.finish_order.push(self.node);
        }
    }
}

/// Deterministic executor for gates and tests: fixed latency, scripted
/// failures, full concurrency auditing.
pub struct FakeExecutor {
    kind: ExecutorKind,
    latency: Duration,
    fail_remaining: Mutex<HashMap<NodeId, u32>>,
    tracker: Tracker,
}

impl FakeExecutor {
    /// Creates an executor of `kind` that takes `latency` per node.
    #[must_use]
    pub fn new(kind: ExecutorKind, latency: Duration) -> Self {
        Self {
            kind,
            latency,
            fail_remaining: Mutex::new(HashMap::new()),
            tracker: Tracker::default(),
        }
    }

    /// Shared tracker.
    #[must_use]
    pub fn tracker(&self) -> Tracker {
        self.tracker.clone()
    }

    /// Fails the next `times` attempts of `node`, then succeeds.
    pub fn fail_times(&self, node: NodeId, times: u32) {
        if let Ok(mut fails) = self.fail_remaining.lock() {
            fails.insert(node, times);
        }
    }

    /// Fails every attempt of the given nodes.
    pub fn fail_nodes(&self, nodes: &HashSet<NodeId>) {
        if let Ok(mut fails) = self.fail_remaining.lock() {
            for node in nodes {
                fails.insert(*node, u32::MAX);
            }
        }
    }
}

#[async_trait]
impl Executor for FakeExecutor {
    fn kind(&self) -> ExecutorKind {
        self.kind
    }

    fn automatic_retry_safe(&self, node: &ExecutionNode) -> bool {
        node.effect_class == EffectClass::Pure
            && matches!(
                node.idempotency,
                Idempotency::Pure | Idempotency::Idempotent
            )
    }

    async fn execute(
        &self,
        node: &ExecutionNode,
        inputs: ResolvedInputs,
        cancel: CancellationToken,
    ) -> NodeOutcome {
        let started = std::time::Instant::now();
        let guard = RunningGuard {
            tracker: self.tracker.clone(),
            node: node.id,
        };
        if let Ok(mut state) = self.tracker.inner.lock() {
            let conflicts: Vec<NodeId> = state
                .running
                .iter()
                .filter(|running| running.access.conflicts_with(&node.access))
                .map(|running| running.node)
                .collect();
            for id in conflicts {
                state
                    .violations
                    .push(format!("{0:?} overlaps running {id:?}", node.id));
            }
            state.running.push(RunningEntry {
                node: node.id,
                access: node.access.clone(),
            });
            state.current += 1;
            state.max = state.max.max(state.current);
            state.start_order.push(node.id);
        }
        let _ = inputs;
        tokio::select! {
            biased;
            () = cancel.cancelled() => NodeOutcome::cancelled(started.elapsed()),
            () = tokio::time::sleep(self.latency) => {
                let fail = self.fail_remaining.lock().is_ok_and(|mut fails| {
                    let remaining = fails.get(&node.id).copied().unwrap_or(0);
                    if remaining > 0 {
                        fails.insert(node.id, remaining.saturating_sub(1));
                        true
                    } else {
                        false
                    }
                });
                drop(guard);
                if fail {
                    NodeOutcome::failed("scripted fake failure".to_owned(), started.elapsed())
                } else {
                    let mut outputs = Map::new();
                    outputs.insert(
                        "echo".to_owned(),
                        Value::String(format!("{:?}", node.invocation.capability)),
                    );
                    NodeOutcome::success(outputs, started.elapsed())
                }
            }
        }
    }
}
