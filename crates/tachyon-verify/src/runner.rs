//! Scheduler-backed execution. Only executor-private evidence can pass a check.
#[cfg(all(test, unix))]
#[path = "runner_tests.rs"]
mod tests;
use crate::{Clause, VerificationPlan, VerifyError, WorkspaceSnapshot, plan::leaf};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tachyon_ir::{ExecutionNode, ExecutorKind, NodeStatus};
use tachyon_scheduler::{
    Budgets, Executor, ExecutorRegistry, NodeOutcome, ResolvedInputs, SchedulerError,
    SchedulerHandle, TaskRunSnapshot,
};
use tachyon_tools::{
    ToolError, ToolsContext,
    process::{ProcessSpec, run_cancellable},
    workspace::WorkspaceLease,
};
use tachyon_types::{ArtifactId, NodeId, TaskId};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationReport {
    task_id: TaskId,
    revision: u64,
    binding: String,
    snapshot: WorkspaceSnapshot,
    failures: Vec<String>,
    checks: Vec<CheckEvidence>,
    // Deserialized evidence is historical, not fresh completion authority.
    #[serde(skip)]
    fresh: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckEvidence {
    node_id: NodeId,
    command_hash: Option<String>,
    status: NodeStatus,
    scheduler_status: Option<NodeStatus>,
    exit_code: Option<i32>,
    stdout_artifact: Option<ArtifactId>,
    stderr_artifact: Option<ArtifactId>,
    diagnostic: String,
}

impl VerificationReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.fresh
            && self.failures.is_empty()
            && !self.checks.is_empty()
            && self.checks.iter().all(|check| {
                check.status == NodeStatus::Succeeded
                    && check.scheduler_status == Some(NodeStatus::Succeeded)
            })
    }
    #[must_use]
    pub fn failures(&self) -> &[String] {
        &self.failures
    }
    #[must_use]
    pub fn snapshot(&self) -> &WorkspaceSnapshot {
        &self.snapshot
    }
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }
    #[must_use]
    pub fn checks(&self) -> &[CheckEvidence] {
        &self.checks
    }
}

impl CheckEvidence {
    fn empty(node_id: NodeId) -> Self {
        Self {
            node_id,
            command_hash: None,
            status: NodeStatus::Failed,
            scheduler_status: None,
            exit_code: None,
            stdout_artifact: None,
            stderr_artifact: None,
            diagnostic: String::new(),
        }
    }
    #[must_use]
    pub fn status(&self) -> NodeStatus {
        self.status
    }
    #[must_use]
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

struct CheckRunner {
    plan: Arc<VerificationPlan>,
    context: Arc<ToolsContext>,
    evidence: Mutex<BTreeMap<NodeId, CheckEvidence>>,
    lease: WorkspaceLease,
    lifetime: Arc<dyn Send + Sync>,
    /// M11 typed parking: the first policy ask observed by a check node,
    /// kept typed (the node itself can only fail) so `run_with_lifetime`
    /// aborts the whole run with the request instead of recording a
    /// failed check. Set exactly once per parked ask.
    asked: Mutex<Option<tachyon_policy::ApprovalRequest>>,
}

#[derive(Default)]
struct Workers {
    closed: bool,
    tasks: tokio::task::JoinSet<()>,
}

struct VerificationExecutor {
    worker: Arc<CheckRunner>,
    workers: Arc<Mutex<Workers>>,
    scope: CancellationToken,
}

#[async_trait]
impl Executor for VerificationExecutor {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Verification
    }
    async fn execute(
        &self,
        node: &ExecutionNode,
        inputs: ResolvedInputs,
        cancel: CancellationToken,
    ) -> NodeOutcome {
        let started = Instant::now();
        if cancel.is_cancelled() || self.scope.is_cancelled() {
            return NodeOutcome::cancelled(started.elapsed());
        }
        let token = self.scope.child_token();
        // The owned worker outlives a dropped caller future, so forward both
        // caller cancellation and this executor's scope into its token.
        let _cancel_on_drop = token.clone().drop_guard();
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut workers = self.workers.lock().expect("verification workers poisoned");
            if workers.closed {
                return NodeOutcome::cancelled(started.elapsed());
            }
            let worker = self.worker.clone();
            let node = node.clone();
            workers.tasks.spawn(async move {
                let mut execution = Box::pin(worker.execute_owned(&node, inputs, token.clone()));
                let outcome = tokio::select! {
                    biased;
                    outcome = &mut execution => outcome,
                    () = cancel.cancelled() => {
                        token.cancel();
                        execution.await
                    }
                };
                let _ = tx.send(outcome);
            });
        }
        rx.await.unwrap_or_else(|_| {
            NodeOutcome::unknown(
                "verification worker lost before settling its effect".into(),
                started.elapsed(),
            )
        })
    }
}

impl CheckRunner {
    async fn execute_owned(
        &self,
        node: &ExecutionNode,
        inputs: ResolvedInputs,
        cancel: CancellationToken,
    ) -> NodeOutcome {
        let started = Instant::now();
        if !inputs.is_empty() || self.plan.graph.nodes.get(&node.id) != Some(node) {
            return NodeOutcome::failed("unbound verification node".into(), started.elapsed());
        }
        if let Err(error) = self.plan.validate_node(node) {
            return NodeOutcome::failed(error.to_string(), started.elapsed());
        }
        let result = self.check(node, cancel).await;
        let mut evidence = CheckEvidence::empty(node.id);
        let outcome = match result {
            Ok(record) => {
                evidence = record;
                if evidence.status == NodeStatus::Succeeded {
                    NodeOutcome::success(serde_json::Map::new(), started.elapsed())
                } else {
                    NodeOutcome::failed(evidence.diagnostic.clone(), started.elapsed())
                }
            }
            Err(error) => {
                if let VerifyError::UnknownOutcome { reason, .. } = &error {
                    evidence.diagnostic = bounded(reason);
                    NodeOutcome::unknown(evidence.diagnostic.clone(), started.elapsed())
                } else {
                    // M11 typed parking: stash the ask typed so
                    // `run_with_lifetime` aborts with the request; the node
                    // outcome itself can only be a failure. Every other
                    // error stays the failed check it was.
                    if let VerifyError::ApprovalRequired(request) = &error {
                        *self.asked.lock().expect("verification ask slot poisoned") =
                            Some(request.clone());
                    }
                    evidence.diagnostic = bounded(&error.to_string());
                    NodeOutcome::failed(evidence.diagnostic.clone(), started.elapsed())
                }
            }
        };
        self.evidence
            .lock()
            .expect("verification evidence poisoned")
            .insert(node.id, evidence);
        outcome
    }
}

impl CheckRunner {
    async fn capture(&self) -> Result<WorkspaceSnapshot, VerifyError> {
        let context = self.context.clone();
        let guards = (self.lease.clone(), self.lifetime.clone());
        tokio::task::spawn_blocking(move || {
            let _guards = guards;
            WorkspaceSnapshot::capture_authorized(&context)
        })
        .await
        .map_err(blocked)?
    }

    async fn check(
        &self,
        node: &ExecutionNode,
        cancel: CancellationToken,
    ) -> Result<CheckEvidence, VerifyError> {
        let clause = self
            .plan
            .checks
            .get(&node.id)
            .ok_or_else(|| VerifyError::Blocked("missing required check".into()))?;
        let mut record = CheckEvidence::empty(node.id);
        let before = self.capture().await?;
        if !self.plan.planned.same_sources(&before) {
            return Err(VerifyError::Blocked(
                "source drift before verification check".into(),
            ));
        }
        let Clause::CommandPasses { command } = leaf(clause) else {
            crate::plan::evaluate_clause(clause, &self.plan.baseline, &self.plan.planned)
                .map_err(VerifyError::Blocked)?;
            record.status = NodeStatus::Succeeded;
            return Ok(record);
        };
        if !self.plan.preflight_failures.is_empty() {
            return Err(VerifyError::Blocked(
                "required acceptance clauses already blocked".into(),
            ));
        }
        command.validate()?;
        let (resolved, scope) = resolve_command_target(&self.context, command)?;
        verify_command_authorized(&self.context, &scope, &node.invocation.args)?;
        let spec = ProcessSpec {
            program: command.program.clone(),
            args: command.args.clone(),
            cwd: Some(resolved),
            env: command
                .env
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            timeout: Duration::from_millis(command.timeout_ms),
        };
        // M12 fault point: arm before the child spawns so a kill lands
        // with the verify command not yet started (no false Completed).
        tachyon_tools::fault::reach("verify.command").await;
        let receipt = run_cancellable(&self.context, &spec, cancel)
            .await
            .map_err(|error| match error {
                // The process might have started and performed effects before
                // cancellation, timeout, or a lifecycle IO failure. Reaping
                // settles the worker lifetime, but not the operation outcome.
                unknown @ (ToolError::ProcessCancelled
                | ToolError::ProcessTimeout(_)
                | ToolError::Io(_)) => VerifyError::UnknownOutcome {
                    node_id: Some(node.id),
                    reason: unknown.to_string(),
                },
                known @ (ToolError::ProcessCancelledBeforeStart
                | ToolError::ProcessStartFailed(_)) => VerifyError::Blocked(known.to_string()),
                other => VerifyError::Blocked(other.to_string()),
            })?;
        record.command_hash = Some(
            blake3::hash(
                &serde_json::to_vec(command)
                    .map_err(|error| VerifyError::InvalidContract(error.to_string()))?,
            )
            .to_hex()
            .to_string(),
        );
        record.exit_code = receipt.exit_code;
        record.status = if receipt.exit_code == Some(0) && !receipt.timed_out {
            NodeStatus::Succeeded
        } else {
            NodeStatus::Failed
        };
        record.diagnostic = if record.status == NodeStatus::Succeeded {
            String::new()
        } else {
            format!(
                "command exited {:?}: {}",
                receipt.exit_code,
                bounded(&String::from_utf8_lossy(&receipt.stderr))
            )
        };
        let spool = self.context.artifacts.clone();
        let guards = (self.lease.clone(), self.lifetime.clone());
        let (out, err) = tokio::task::spawn_blocking(move || {
            let _guards = guards;
            Ok::<_, tachyon_tools::ToolError>((
                receipt
                    .stdout_artifact
                    .map_or_else(|| spool.store(&receipt.stdout), Ok)?,
                receipt
                    .stderr_artifact
                    .map_or_else(|| spool.store(&receipt.stderr), Ok)?,
            ))
        })
        .await
        .map_err(|error| VerifyError::Blocked(error.to_string()))?
        .map_err(|error| VerifyError::Blocked(error.to_string()))?;
        let after = self.capture().await?;
        if !self.plan.planned.same_sources(&after) {
            record.status = NodeStatus::Failed;
            record.diagnostic = "source drift during verification command".into();
        }
        record.stdout_artifact = Some(out);
        record.stderr_artifact = Some(err);
        Ok(record)
    }
}

/// Executes fresh work, never cached model-supplied success claims.
pub async fn run(
    plan: VerificationPlan,
    context: Arc<ToolsContext>,
    cancel: CancellationToken,
) -> Result<VerificationReport, VerifyError> {
    run_with_lifetime(plan, context, cancel, Arc::new(())).await
}

/// As [`run`], retaining a caller-owned lifetime guard in actual effect workers.
///
/// Core supplies its task-ownership guard here. This opaque anchor conveys no
/// authorization or completion authority; it only prevents premature owner
/// The workspace lease for one verification run (M11 slice 1). A run
/// path that took the lease on its pinned root at `StartRun` prepare
/// carries it on the context and must get it back: the per-root registry
/// lock is NOT reentrant, so re-acquiring the run-held root here would
/// self-deadlock the run. Contexts without an attached lease keep the
/// stage-local acquisition. The root must equal the caller's canonical
/// root (prepare root-checked it against the durable pin; the caller
/// checked it against the baseline) — anything else fails closed.
async fn run_workspace_lease(
    context: &ToolsContext,
    actual_root: &Path,
    cancel: &CancellationToken,
) -> Result<WorkspaceLease, VerifyError> {
    let lease = match context.workspace_lease().cloned() {
        Some(run_lease) => run_lease,
        None => WorkspaceLease::acquire(actual_root, cancel)
            .await
            .map_err(blocked)?,
    };
    if lease.root() != actual_root {
        return Err(VerifyError::Blocked(
            "workspace lease root differs from the verification root".into(),
        ));
    }
    Ok(lease)
}

struct VerificationSession {
    plan: Arc<VerificationPlan>,
    worker: Arc<CheckRunner>,
    owner: SchedulerOwner,
    lease: WorkspaceLease,
}

async fn prepare_session(
    plan: VerificationPlan,
    context: Arc<ToolsContext>,
    cancel: CancellationToken,
    lifetime: Arc<dyn Send + Sync>,
) -> Result<VerificationSession, VerifyError> {
    plan.contract.validate()?;
    plan.validate_graph()?;
    if plan.graph.nodes.is_empty() {
        return Err(VerifyError::Blocked("missing verification nodes".into()));
    }
    let actual_root = canonical_root(context.workspace_root.clone()).await?;
    if actual_root != plan.baseline.root() {
        return Err(VerifyError::Blocked(
            "verification context root differs from baseline".into(),
        ));
    }
    // One canonical workspace runs one verification at a time process-wide.
    // Each run owns an independent scheduler whose grants cannot conflict
    // with another run's grants, so without this lease two supervisors could
    // execute conflicting `dir:/workspace/**` write claims concurrently.
    // The guard is held through scheduler shutdown and worker drain, so a
    // timed-out scheduler cannot release its grant while its process is
    // still handling TERM and the next run starts early.
    let lease = run_workspace_lease(&context, &actual_root, &cancel).await?;
    let plan = Arc::new(plan);
    let worker = Arc::new(CheckRunner {
        plan: plan.clone(),
        context,
        evidence: Mutex::new(BTreeMap::new()),
        lease: lease.clone(),
        lifetime,
        asked: Mutex::new(None),
    });
    let workers = Arc::new(Mutex::new(Workers::default()));
    let scope = cancel.child_token();
    let executor = Arc::new(VerificationExecutor {
        worker: worker.clone(),
        workers: workers.clone(),
        scope: scope.clone(),
    });
    let registry: ExecutorRegistry = HashMap::from([(
        ExecutorKind::Verification,
        executor.clone() as Arc<dyn Executor>,
    )]);
    let (handle, join) = tachyon_scheduler::spawn(Budgets::default(), registry);
    let owner = SchedulerOwner {
        handle: Some(handle),
        join: Some(join),
        workers,
        scope,
    };
    Ok(VerificationSession {
        plan,
        worker,
        owner,
        lease,
    })
}

/// Runs verification while retaining a caller-owned lifetime guard in workers.
/// Core uses this to preserve task ownership during cancellation/abort cleanup
/// or a blocking snapshot and artifact-spool write.
pub async fn run_with_lifetime(
    plan: VerificationPlan,
    context: Arc<ToolsContext>,
    cancel: CancellationToken,
    lifetime: Arc<dyn Send + Sync>,
) -> Result<VerificationReport, VerifyError> {
    let session = prepare_session(plan, context, cancel.clone(), lifetime).await?;
    execute_session(session, cancel).await
}

async fn execute_session(
    session: VerificationSession,
    cancel: CancellationToken,
) -> Result<VerificationReport, VerifyError> {
    let VerificationSession {
        plan,
        worker,
        owner,
        lease,
    } = session;
    let scheduler = owner.handle.as_ref().expect("owned scheduler");
    scheduler
        .submit(plan.task_id, plan.graph.clone())
        .await
        .map_err(blocked)?;
    let budget = plan
        .graph
        .nodes
        .values()
        .map(|node| node.timeout.hard_ms.unwrap_or(30_000))
        .sum::<u64>()
        .saturating_add(5_000);
    let statuses = match wait_for_scheduler(
        scheduler,
        plan.task_id,
        &cancel,
        Duration::from_millis(budget),
    )
    .await
    {
        Ok(statuses) => statuses,
        Err(VerifyError::UnknownOutcome { node_id, reason }) => {
            return Err(close_unknown(owner, node_id, reason).await);
        }
        Err(error) => return Err(error),
    };
    if let Some(node_id) = running_node(&statuses) {
        return Err(close_unknown(
            owner,
            Some(node_id),
            "scheduler deadline elapsed while the check worker was running".into(),
        )
        .await);
    }
    if !statuses.finished {
        owner.close().await?;
        return Err(VerifyError::Blocked(
            "verification deadline elapsed before all checks became runnable".into(),
        ));
    }
    owner.close().await?;
    if let Some(request) = take_asked(&worker) {
        return Err(VerifyError::ApprovalRequired(request));
    }
    let snapshot = worker.capture().await?;
    let records = worker
        .evidence
        .lock()
        .expect("verification evidence poisoned");
    drop(lease);
    let mut failures = plan.preflight_failures.clone();
    if !plan.planned.same_sources(&snapshot) {
        failures.push("source drift after verification".into());
    }
    let mut checks = Vec::new();
    for id in plan.graph.nodes.keys() {
        let mut record = records
            .get(id)
            .cloned()
            .unwrap_or_else(|| CheckEvidence::empty(*id));
        record.scheduler_status = statuses.statuses.get(id).copied();
        if record.status != NodeStatus::Succeeded
            || record.scheduler_status != Some(NodeStatus::Succeeded)
            || statuses.attempts.get(id) != Some(&1)
        {
            failures.push(format!(
                "required check {id}: {:?}: {}",
                record.scheduler_status, record.diagnostic
            ));
        }
        checks.push(record);
    }
    Ok(VerificationReport {
        task_id: plan.task_id,
        revision: plan.revision,
        binding: plan.binding.clone(),
        snapshot,
        failures,
        checks,
        fresh: true,
    })
}

/// Resolves a command cwd to its canonical target and policy scope.
/// Authorization and execution must both use this resolved target: the
/// lexical alias may point through a symlink at a denied directory.
fn resolve_command_target(
    context: &ToolsContext,
    command: &crate::CommandCheck,
) -> Result<(PathBuf, String), VerifyError> {
    let (resolved, _) =
        tachyon_tools::resolve_scope(&context.workspace_root, Path::new(&command.cwd))
            .map_err(blocked)?;
    let canonical_root = context.workspace_root.canonicalize().map_err(blocked)?;
    let relative = resolved
        .strip_prefix(&canonical_root)
        .map_err(|_| blocked("verification cwd escapes workspace"))?;
    let relative = relative
        .to_str()
        .ok_or_else(|| blocked("non-UTF8 verification cwd"))?
        .replace(std::path::MAIN_SEPARATOR, "/");
    let scope = if relative.is_empty() {
        "workspace/".to_owned()
    } else {
        format!("workspace/{relative}")
    };
    Ok((resolved, scope))
}

fn bounded(value: &str) -> String {
    value.chars().take(2_048).collect()
}
fn blocked(error: impl std::fmt::Display) -> VerifyError {
    VerifyError::Blocked(error.to_string())
}

fn scheduler_error(task_id: TaskId, error: &SchedulerError) -> VerifyError {
    let node_id = match error {
        SchedulerError::UnknownOutcome { node_id, .. } => Some(*node_id),
        _ => None,
    };
    VerifyError::UnknownOutcome {
        node_id,
        reason: format!("task {task_id} scheduler could not settle the effect outcome: {error}"),
    }
}

async fn wait_for_scheduler(
    scheduler: &SchedulerHandle,
    task_id: TaskId,
    cancel: &CancellationToken,
    budget: Duration,
) -> Result<TaskRunSnapshot, VerifyError> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => {
            scheduler.cancel_task(task_id).await
                .map_err(|error| scheduler_error(task_id, &error))?;
            scheduler.status(task_id).await
                .map_err(|error| scheduler_error(task_id, &error))
        }
        status = scheduler.wait_finished(task_id, budget) => {
            status.map_err(|error| scheduler_error(task_id, &error))
        },
    }
}

fn running_node(snapshot: &TaskRunSnapshot) -> Option<NodeId> {
    snapshot
        .statuses
        .iter()
        .find_map(|(id, status)| (*status == NodeStatus::Running).then_some(*id))
}

async fn close_unknown(
    owner: SchedulerOwner,
    node_id: Option<NodeId>,
    reason: String,
) -> VerifyError {
    let reason = match owner.close().await {
        Ok(()) => reason,
        Err(drain_error) => format!("{reason}; worker drain failed: {drain_error}"),
    };
    VerifyError::UnknownOutcome { node_id, reason }
}

/// Enforces `verify.command` policy for one acceptance command. M11
/// typed parking: an ask keeps its exact pending request typed all the
/// way to the driver (the run parks instead of failing a check); only
/// non-approval failures collapse to the stringy guard.
fn verify_command_authorized(
    context: &ToolsContext,
    scope: &str,
    invocation: &serde_json::Value,
) -> Result<(), VerifyError> {
    tachyon_tools::authorize(
        &context.policy,
        &context.approvals,
        "verify.command",
        scope,
        &serde_json::json!({
            "invocation": invocation,
            "resolved_scope": scope,
        }),
        "execute required verification command",
    )
    .map_err(|error| match error {
        tachyon_tools::ToolError::ApprovalRequired { request, .. } => {
            VerifyError::ApprovalRequired(*request)
        }
        other => blocked(other),
    })
}

/// Canonicalizes the verification root off the async executor; a failed
/// canonicalize is a blocked verification.
async fn canonical_root(root: PathBuf) -> Result<PathBuf, VerifyError> {
    Ok(tokio::task::spawn_blocking(move || root.canonicalize())
        .await
        .map_err(blocked)??)
}

/// M11 typed parking: takes the first policy ask a check node observed
/// (see [`CheckRunner::asked`]). `run_with_lifetime` calls this after
/// the scheduler is closed so the typed request aborts the run with the
/// same cleanup as the success path — scope/workers drop with the
/// return, and a stage-local (non-run) lease guard drops here. On a run
/// path the driver's `ParkedJob.context` keeps the run-scoped lease for
/// the whole wait, so exclusion persists until decision or cancel
/// (held-whole-run is the M11 contract — R2 seat3 observation).
fn take_asked(worker: &CheckRunner) -> Option<tachyon_policy::ApprovalRequest> {
    worker
        .asked
        .lock()
        .expect("verification ask slot poisoned")
        .take()
}

struct SchedulerOwner {
    handle: Option<SchedulerHandle>,
    join: Option<tokio::task::JoinHandle<()>>,
    workers: Arc<Mutex<Workers>>,
    scope: CancellationToken,
}
impl SchedulerOwner {
    async fn close(mut self) -> Result<(), VerifyError> {
        match self.begin_close() {
            Some(drain) => drain.await.map_err(blocked)?,
            None => Ok(()),
        }
    }

    fn begin_close(&mut self) -> Option<tokio::task::JoinHandle<Result<(), VerifyError>>> {
        let scheduler = self.join.take()?;
        self.handle.take();
        self.scope.cancel();
        let mut tasks = {
            let mut workers = self.workers.lock().expect("verification workers poisoned");
            workers.closed = true;
            std::mem::take(&mut workers.tasks)
        };
        scheduler.abort();
        // Actual workers retain the lease and opaque owner guard. Aborting the
        // caller or its close-waiter drops only this JoinHandle, not the drain
        // job. Never abort the worker JoinSet: processes must finish TERM/KILL
        // and reap before the next conflicting stage can acquire the lease.
        Some(tokio::spawn(async move {
            let mut failure = match scheduler.await {
                Err(error) if !error.is_cancelled() => Some(blocked(error)),
                _ => None,
            };
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    failure.get_or_insert_with(|| blocked(error));
                }
            }
            failure.map_or(Ok(()), Err)
        }))
    }
}
impl Drop for SchedulerOwner {
    fn drop(&mut self) {
        // A dropped JoinHandle leaves its owned drain job running. Its real
        // workers, not this wrapper, hold exclusion until cleanup has finished.
        drop(self.begin_close());
    }
}

#[cfg(test)]
mod scheduler_failure_tests {
    use super::*;

    #[test]
    fn scheduler_loss_after_dispatch_is_an_unknown_effect_outcome() {
        let task_id = TaskId::generate();
        assert!(matches!(
            scheduler_error(task_id, &SchedulerError::SchedulerGone),
            VerifyError::UnknownOutcome { node_id: None, .. }
        ));

        let node_id = NodeId::generate();
        assert!(matches!(
            scheduler_error(
                task_id,
                &SchedulerError::UnknownOutcome { task_id, node_id }
            ),
            VerifyError::UnknownOutcome { node_id: Some(unknown), .. } if unknown == node_id
        ));
    }
}
