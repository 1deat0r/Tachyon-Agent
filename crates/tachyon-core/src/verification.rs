//! Supervisor-owned verification. Only this module can commit completion.
//!
//! Every workspace-touching step runs as an owned actor job: the baseline
//! capture, the plan/source scan and the final authorized rehash. The actor keeps
//! serving `GetState`, steering and control acknowledgements while a job waits for
//! the shared workspace lease, scans, runs or reaps a process. No mailbox handler
//! awaits a worker inline, and every blocking closure carries both the
//! workspace-lease and the task-ownership anchors.
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tachyon_ir::ExecutorKind;
use tachyon_tools::{ToolsContext, workspace::WorkspaceLease};
use tachyon_types::TaskId;
use tachyon_verify::{
    AcceptanceContract, HardRequirement, VerificationPlan, VerificationReport, VerificationRisk,
    VerifyError, WorkspaceSnapshot,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ConstraintStrength, CoreError, Loop, StateEvent, SupervisorCommand, SupervisorHandle,
    TaskState, TaskStatus, receive,
};

/// Durable baseline and latest run evidence. Never accepted from a model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationState {
    pub baseline: WorkspaceSnapshot,
    pub risk: VerificationRisk,
    pub in_progress: bool,
    /// Unknown effects after an interrupted verifier must be reconciled first.
    pub interrupted: bool,
    pub report: Option<VerificationReport>,
    pub error: Option<String>,
}

impl VerificationState {
    pub(super) fn new(baseline: WorkspaceSnapshot, risk: VerificationRisk) -> Self {
        Self {
            baseline,
            risk,
            in_progress: false,
            interrupted: false,
            report: None,
            error: None,
        }
    }
}

/// Private identity of one owned operation: a fresh run id plus the task and the
/// revision it was admitted at. Jobs carry it and only a matching live operation
/// may apply their results. It is never serialized, journalled or exposed to a
/// caller, so no serialized or caller-supplied field can be forged into authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OperationKey {
    id: Uuid,
    task: TaskId,
    revision: u64,
}

impl OperationKey {
    /// The task this operation belongs to. Used to reject a foreign result.
    fn belongs_to(self, task: TaskId) -> bool {
        self.task == task
    }
}

/// Which owned job produced a result.
pub(super) enum JobValue {
    Baseline(Result<WorkspaceSnapshot, CoreError>),
    Planned(Result<VerificationPlan, CoreError>),
    Verified(Result<VerificationReport, VerifyError>),
    Rehashed(Result<WorkspaceSnapshot, CoreError>),
}

/// One finished owned job. `lease` is a guard, not a proof: the actor retains it
/// until its own durable transaction has committed, then drops it. A result whose
/// operation was cancelled or superseded is dropped together with its guard, so no
/// detached write can outlive a grant.
pub(super) struct JobResult {
    key: OperationKey,
    value: JobValue,
    lease: Option<WorkspaceLease>,
}

impl JobResult {
    /// Which step this result belongs to; an out-of-phase result is refused.
    fn phase(&self) -> Phase {
        match self.value {
            JobValue::Baseline(_) => Phase::CapturingBaseline,
            JobValue::Planned(_) => Phase::Planning,
            JobValue::Verified(_) => Phase::Verifying,
            JobValue::Rehashed(_) => Phase::Rehashing,
        }
    }
}

/// Step of a live operation currently occupying an owned worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    CapturingBaseline,
    Planning,
    Verifying,
    Rehashing,
}

/// Acceptance retained until the baseline becomes durable.
struct Binding {
    contract: AcceptanceContract,
    risk: VerificationRisk,
}

pub(super) struct ActiveVerification {
    key: OperationKey,
    cancel: CancellationToken,
    phase: Phase,
    context: Arc<ToolsContext>,
    reply: Option<oneshot::Sender<Result<TaskState, CoreError>>>,
    binding: Option<Binding>,
    /// Report retained between the verifier job and the final rehash job.
    report: Option<VerificationReport>,
}

/// Control acknowledgements that may only be answered once the actual effect
/// workers have drained. Further commands keep being served meanwhile, and a
/// caller that drops its receiver neither aborts cleanup nor releases admission.
#[derive(Default)]
pub(super) struct DrainAck {
    waiting: Vec<oneshot::Sender<Result<TaskState, CoreError>>>,
}

impl SupervisorHandle {
    /// Trusted runtime API: bind acceptance and a source baseline BEFORE mutation.
    /// This is intentionally not a model capability or gateway command. Once bound,
    /// neither model output nor a second configuration can weaken the contract.
    pub async fn configure_verification(
        &self,
        context: Arc<ToolsContext>,
        contract: AcceptanceContract,
        risk: VerificationRisk,
    ) -> Result<TaskState, CoreError> {
        let (reply, rx) = oneshot::channel();
        self.send(SupervisorCommand::ConfigureVerification {
            context,
            contract,
            risk,
            reply,
        })
        .await;
        receive(rx).await?
    }

    /// Run fresh executable checks and let the supervisor decide completion.
    /// There is no public "mark completed" or "accept this report" API.
    pub async fn verify_and_complete(
        &self,
        context: Arc<ToolsContext>,
    ) -> Result<TaskState, CoreError> {
        let (reply, rx) = oneshot::channel();
        self.send(SupervisorCommand::VerifyAndComplete { context, reply })
            .await;
        receive(rx).await?
    }
}

impl Loop {
    /// No new owned verification work starts while an operation is live or while
    /// superseded workers are still draining their effects.
    fn busy(&self) -> bool {
        self.active.is_some() || !self.jobs.is_empty() || self.drain.is_some()
    }

    fn verification_status_allowed(&self) -> Result<(), CoreError> {
        if !matches!(
            self.state.status,
            TaskStatus::Created | TaskStatus::Planning | TaskStatus::Executing
        ) {
            return Err(CoreError::IllegalTransition {
                from: self.state.status,
                to: TaskStatus::Verifying,
            });
        }
        Ok(())
    }

    /// Admits an operation and returns its private key plus its cancel token. The
    /// token is a child of the owner lifecycle, so shutdown always cancels it
    /// instead of orphaning a worker behind a released owner.
    fn begin_operation(
        &mut self,
        context: Arc<ToolsContext>,
        reply: oneshot::Sender<Result<TaskState, CoreError>>,
        phase: Phase,
        binding: Option<Binding>,
    ) -> (OperationKey, CancellationToken) {
        let key = OperationKey {
            id: Uuid::now_v7(),
            task: self.state.id,
            revision: self.state.revision,
        };
        let cancel = self.ownership.lifecycle().shutdown.child_token();
        self.active = Some(ActiveVerification {
            key,
            cancel: cancel.clone(),
            phase,
            context,
            reply: Some(reply),
            binding,
            report: None,
        });
        (key, cancel)
    }

    /// After the actor journalled its own transition, re-bind the live key to the
    /// committed revision so later job results are matched against current truth.
    fn resync_operation(&mut self) {
        if let Some(active) = &mut self.active {
            active.key.revision = self.state.revision;
        }
    }

    /// Hands an outcome to the operation's caller and ends the operation. A caller
    /// that dropped its receiver is ignored; nothing here carries authority.
    fn complete_operation(&mut self, outcome: Result<TaskState, CoreError>) {
        if let Some(active) = self.active.take()
            && let Some(reply) = active.reply
        {
            let _ = reply.send(outcome);
        }
    }

    /// Answers pending control acknowledgements once every owned worker is gone.
    fn settle_if_drained(&mut self) {
        if !self.jobs.is_empty() {
            return;
        }
        let Some(drain) = self.drain.take() else {
            return;
        };
        for waiting in drain.waiting {
            let _ = waiting.send(Ok(self.state.clone()));
        }
    }

    /// Queues a control acknowledgement: answered now when nothing drains,
    /// otherwise as soon as the actual effect workers have finished.
    fn acknowledge_after_drain(&mut self, reply: oneshot::Sender<Result<TaskState, CoreError>>) {
        if self.jobs.is_empty() {
            let _ = reply.send(Ok(self.state.clone()));
            return;
        }
        self.drain
            .get_or_insert_with(DrainAck::default)
            .waiting
            .push(reply);
    }

    /// Cancels owned verification work. The durable interruption record is written
    /// before the cancel, so a crash can never replay verifier effects, and the
    /// cancellation releases nothing until the actual workers have drained.
    async fn supersede_operation(&mut self) -> Result<(), CoreError> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        if self
            .state
            .verification
            .as_ref()
            .is_some_and(|v| v.in_progress)
        {
            self.transition_journalled(StateEvent::VerificationInterrupted)
                .await?;
        }
        active.cancel.cancel();
        if let Some(reply) = active.reply {
            let _ = reply.send(Err(blocked(
                "verification interrupted by steering/cancellation",
            )));
        }
        Ok(())
    }

    /// Pause/cancel: the durable intent is written promptly and the
    /// acknowledgement only after the actual drain. A dropped acknowledgement
    /// receiver does not abort that cleanup.
    pub(super) async fn control(
        &mut self,
        target: TaskStatus,
        reply: oneshot::Sender<Result<TaskState, CoreError>>,
    ) {
        if self.state.status.is_terminal() {
            let _ = reply.send(Err(CoreError::IllegalTransition {
                from: self.state.status,
                to: target,
            }));
            return;
        }
        if let Err(error) = self.supersede_operation().await {
            let _ = reply.send(Err(error));
            return;
        }
        let from = self.state.status;
        if from != target
            && let Err(error) = self
                .transition_journalled(StateEvent::Status { from, to: target })
                .await
        {
            let _ = reply.send(Err(error));
            return;
        }
        self.acknowledge_after_drain(reply);
    }

    /// Steering: the intent is durable immediately and the operation and revision
    /// move on, so a queued or late job result can no longer be committed. Only
    /// pause/cancel acknowledgements wait for the drain.
    pub(super) async fn steer(&mut self, event: StateEvent) -> Result<TaskState, CoreError> {
        if self.state.status.is_terminal() {
            return Err(CoreError::IllegalTransition {
                from: self.state.status,
                to: self.state.status,
            });
        }
        self.supersede_operation().await?;
        self.transition_journalled(event).await
    }

    /// Baseline capture as an owned job: it acquires the shared workspace lease and
    /// hands it back so the durable `VerificationConfigured` record commits while
    /// the workspace is still excluded.
    pub(super) fn configure_verification(
        &mut self,
        context: Arc<ToolsContext>,
        contract: AcceptanceContract,
        risk: VerificationRisk,
        reply: oneshot::Sender<Result<TaskState, CoreError>>,
    ) {
        let allowed = self.verification_status_allowed().and_then(|()| {
            if self.state.verification.is_some() || self.busy() {
                return Err(blocked("acceptance is already bound or a job is draining"));
            }
            if !self.state.graph.nodes.is_empty() {
                return Err(blocked("bind acceptance before executing work"));
            }
            contract.validate().map_err(CoreError::from)
        });
        if let Err(error) = allowed {
            let _ = reply.send(Err(error));
            return;
        }
        let (key, cancel) = self.begin_operation(
            context.clone(),
            reply,
            Phase::CapturingBaseline,
            Some(Binding { contract, risk }),
        );
        let lifetime = self.ownership.lifetime();
        self.jobs.spawn(async move {
            let (value, lease) = leased_capture(context, lifetime, cancel).await;
            JobResult {
                key,
                value: JobValue::Baseline(value),
                lease,
            }
        });
    }

    /// Everything the planner needs, validated without blocking the mailbox. The
    /// non-verifier graph guard stays until the debugging integration adds private
    /// terminal/effect proof for general work nodes.
    fn verification_request_allowed(&self) -> Result<(), CoreError> {
        self.verification_status_allowed()?;
        if self.busy() {
            return Err(blocked("verification is already running or draining"));
        }
        let Some(verification) = &self.state.verification else {
            return Err(blocked("no authoritative acceptance contract and baseline"));
        };
        if verification.interrupted || verification.in_progress {
            return Err(blocked(
                "interrupted verifier effects require reconciliation; no blind replay",
            ));
        }
        if self
            .state
            .graph
            .nodes
            .values()
            .any(|n| n.executor != ExecutorKind::Verification)
        {
            return Err(blocked("non-verifier work has no terminal/effect evidence"));
        }
        Ok(())
    }

    pub(super) fn start_verification(
        &mut self,
        context: Arc<ToolsContext>,
        reply: oneshot::Sender<Result<TaskState, CoreError>>,
    ) {
        if let Err(error) = self.verification_request_allowed() {
            let _ = reply.send(Err(error));
            return;
        }
        let (key, cancel) = self.begin_operation(context, reply, Phase::Planning, None);
        self.spawn_planner(key, cancel);
    }

    fn spawn_planner(&mut self, key: OperationKey, cancel: CancellationToken) {
        let Some((baseline, risk)) = self
            .state
            .verification
            .as_ref()
            .map(|verification| (verification.baseline.clone(), verification.risk))
        else {
            self.complete_operation(Err(blocked(
                "no authoritative acceptance contract and baseline",
            )));
            return;
        };
        let Some(context) = self.active.as_ref().map(|active| active.context.clone()) else {
            return;
        };
        let contract = self.state.acceptance.clone();
        let task_id = self.state.id;
        let revision = key.revision;
        let hard = self
            .state
            .constraints
            .iter()
            .filter(|c| c.strength == ConstraintStrength::Hard)
            .map(|c| HardRequirement {
                id: c.id,
                text: c.text.clone(),
            })
            .collect::<Vec<_>>();
        let lifetime = self.ownership.lifetime();
        self.jobs.spawn(async move {
            let (value, lease) = leased_plan(
                task_id, revision, context, contract, baseline, &hard, risk, cancel, lifetime,
            )
            .await;
            JobResult {
                key,
                value: JobValue::Planned(value),
                lease,
            }
        });
    }

    /// The verifier acquires its own lease (core released the planner's), so the
    /// two stages never lock recursively.
    fn spawn_verifier(&mut self, key: OperationKey, plan: VerificationPlan) {
        let Some(active) = &mut self.active else {
            return;
        };
        if active.key != key {
            return;
        }
        active.phase = Phase::Verifying;
        let context = active.context.clone();
        let cancel = active.cancel.clone();
        let lifetime = self.ownership.lifetime();
        self.jobs.spawn(async move {
            let value = tachyon_verify::run_with_lifetime(plan, context, cancel, lifetime).await;
            JobResult {
                key,
                value: JobValue::Verified(value),
                lease: None,
            }
        });
    }

    /// The final authorized rehash. Its lease travels back with the result so the
    /// supervisor's durable completion transaction commits while the workspace is
    /// still excluded: a competing same-root stage can only enter after that
    /// commit, or it invalidates the rehash instead.
    fn spawn_rehash(&mut self, key: OperationKey, report: VerificationReport) {
        let Some(active) = &mut self.active else {
            return;
        };
        if active.key != key {
            return;
        }
        active.phase = Phase::Rehashing;
        active.report = Some(report);
        let context = active.context.clone();
        let cancel = active.cancel.clone();
        let lifetime = self.ownership.lifetime();
        self.jobs.spawn(async move {
            let (value, lease) = leased_rehash(context, lifetime, cancel).await;
            JobResult {
                key,
                value: JobValue::Rehashed(value),
                lease,
            }
        });
    }

    /// Routes one finished owned job. A result that does not belong to the live
    /// operation (superseded, cancelled, foreign run id, stale revision, wrong
    /// phase) is discarded with its lease guard and can replace no state.
    pub(super) async fn finish_job(&mut self, joined: Result<JobResult, tokio::task::JoinError>) {
        let result = match joined {
            Ok(result) => result,
            Err(error) => {
                let message = format!("verification worker failed: {error}");
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.phase == Phase::Verifying)
                {
                    self.interrupt_live_verification(message).await;
                } else {
                    self.fail_live_operation(message).await;
                }
                self.settle_if_drained();
                return;
            }
        };
        let live = self.active.as_ref().is_some_and(|active| {
            active.key == result.key
                && active.phase == result.phase()
                && result.key.belongs_to(active.key.task)
        });
        if !live || self.state.revision != result.key.revision {
            self.settle_if_drained();
            return;
        }
        let JobResult { key, value, lease } = result;
        match value {
            JobValue::Baseline(result) => self.finish_baseline(key, result, lease).await,
            JobValue::Planned(result) => self.finish_plan(key, result, lease).await,
            JobValue::Verified(result) => self.finish_report(key, result).await,
            JobValue::Rehashed(result) => self.finish_rehash(key, result, lease).await,
        }
        self.settle_if_drained();
    }

    async fn fail_live_operation(&mut self, message: String) {
        let Some(key) = self.active.as_ref().map(|active| active.key) else {
            return;
        };
        if self.state.verification.is_none() {
            self.complete_operation(Err(blocked(&message)));
            return;
        }
        let report = self.active.as_mut().and_then(|active| active.report.take());
        self.journal_finished(key, report, Some(message)).await;
    }

    /// A verifier worker can panic after an acceptance command has started.
    /// Keep its durable in-progress marker unresolved so recovery cannot replay
    /// the command under the ordinary failed-check path.
    async fn interrupt_live_verification(&mut self, message: String) {
        if self.active.is_none() {
            return;
        }
        match self
            .transition_journalled(StateEvent::VerificationInterrupted)
            .await
        {
            Ok(_) => self.complete_operation(Err(CoreError::VerificationBlocked(message))),
            Err(error) => self.complete_operation(Err(error)),
        }
    }

    async fn finish_baseline(
        &mut self,
        key: OperationKey,
        result: Result<WorkspaceSnapshot, CoreError>,
        lease: Option<WorkspaceLease>,
    ) {
        let binding = self
            .active
            .as_mut()
            .and_then(|active| active.binding.take());
        let baseline = match result {
            Ok(baseline) => baseline,
            Err(error) => {
                self.complete_operation(Err(error));
                return;
            }
        };
        let Some(Binding { contract, risk }) = binding else {
            self.complete_operation(Err(blocked("baseline job lost its acceptance binding")));
            return;
        };
        if self.active.as_ref().is_none_or(|active| active.key != key) {
            return;
        }
        let committed = self
            .transition_journalled(StateEvent::VerificationConfigured {
                contract,
                baseline,
                risk,
            })
            .await;
        // Release only after the durable record exists: nothing can mutate the
        // workspace between the authorised scan and the persisted baseline.
        drop(lease);
        match committed {
            Ok(state) => {
                self.resync_operation();
                self.complete_operation(Ok(state));
            }
            Err(error) => self.complete_operation(Err(error)),
        }
    }

    async fn finish_plan(
        &mut self,
        key: OperationKey,
        result: Result<VerificationPlan, CoreError>,
        lease: Option<WorkspaceLease>,
    ) {
        let plan = match result {
            Ok(plan) => plan,
            Err(error) => {
                self.complete_operation(Err(error));
                return;
            }
        };
        let graph = plan.graph().clone();
        let started = self
            .transition_journalled(StateEvent::VerificationStarted { graph })
            .await;
        // The durable Started record precedes any process effect, and the plan's
        // lease ends here: the verifier acquires its own.
        drop(lease);
        match started {
            Ok(_) => {
                self.resync_operation();
                self.spawn_verifier(key, plan);
            }
            Err(error) => self.complete_operation(Err(error)),
        }
    }

    /// A passing report is never completion authority on its own: it must still be
    /// rehashed under a fresh lease before this actor commits anything.
    async fn finish_report(
        &mut self,
        key: OperationKey,
        result: Result<VerificationReport, VerifyError>,
    ) {
        let report = match result {
            Ok(report) => report,
            Err(VerifyError::ApprovalRequired(request)) => {
                // M11 typed parking: an ask is not a verification failure.
                // The durable Finished arm clears the Started flag and
                // lands the status back on `Executing` (admissible for
                // both the park and its grant-rerun), and the TYPED
                // request — never a string — replies to the driver so it
                // parks exactly like the evidence stage.
                if self.active.as_ref().is_none_or(|active| active.key != key) {
                    return;
                }
                let message = format!(
                    "approval required: {} on {}",
                    request.capability.0, request.scope
                );
                let finished = self
                    .transition_journalled(StateEvent::VerificationFinished {
                        report: None,
                        error: Some(message),
                        completed: false,
                    })
                    .await;
                match finished {
                    Ok(_) => self.complete_operation(Err(CoreError::Verification(
                        VerifyError::ApprovalRequired(request),
                    ))),
                    Err(error) => self.complete_operation(Err(error)),
                }
                return;
            }
            Err(error @ VerifyError::UnknownOutcome { .. }) => {
                if self.active.as_ref().is_none_or(|active| active.key != key) {
                    return;
                }
                self.interrupt_live_verification(error.to_string()).await;
                return;
            }
            Err(error) => {
                self.journal_finished(key, None, Some(error.to_string()))
                    .await;
                return;
            }
        };
        if let Some(error) = self.report_error(&report, key) {
            self.journal_finished(key, Some(report), Some(error)).await;
            return;
        }
        self.spawn_rehash(key, report);
    }

    fn report_error(&self, report: &VerificationReport, key: OperationKey) -> Option<String> {
        if self.state.status != TaskStatus::Verifying
            || self.state.revision != key.revision
            || report.revision() != key.revision
            || report.task_id() != self.state.id
        {
            return Some("stale/foreign verification result".into());
        }
        if !report.passed() {
            return Some(format!(
                "required acceptance failed: {}",
                report.failures().join("; ")
            ));
        }
        None
    }

    /// Commits completion only under the still-held rehash lease and only while
    /// this operation is the current one at the current revision.
    async fn finish_rehash(
        &mut self,
        key: OperationKey,
        result: Result<WorkspaceSnapshot, CoreError>,
        lease: Option<WorkspaceLease>,
    ) {
        let retained = self
            .active
            .as_mut()
            .filter(|active| active.key == key && active.phase == Phase::Rehashing)
            .and_then(|active| active.report.take());
        let Some(report) = retained else {
            return;
        };
        let fresh =
            self.state.revision == key.revision && self.state.status == TaskStatus::Verifying;
        let error = if fresh {
            match result {
                Ok(now) if report.snapshot().same_sources(&now) => None,
                Ok(_) => Some("sources changed after verification; fresh checks required".into()),
                Err(error) => Some(error.to_string()),
            }
        } else {
            Some("stale/foreign verification result".into())
        };
        self.journal_finished(key, Some(report), error).await;
        drop(lease);
    }

    /// The one durable completion transaction. `completed` is decided here, never
    /// by a caller or a provider, and the terminal status is the last write.
    async fn journal_finished(
        &mut self,
        key: OperationKey,
        report: Option<VerificationReport>,
        error: Option<String>,
    ) {
        if self.active.as_ref().is_none_or(|active| active.key != key) {
            return;
        }
        let completed = error.is_none();
        let outcome = self
            .transition_journalled(StateEvent::VerificationFinished {
                report,
                error: error.clone(),
                completed,
            })
            .await
            .and_then(|state| {
                error.map_or(Ok(state), |error| {
                    Err(CoreError::VerificationBlocked(error))
                })
            });
        self.complete_operation(outcome);
    }

    /// Shutdown / closed-mailbox path: cancel owned work and wait for the actual
    /// drain. No status is written here — durable truth already records the
    /// interruption — so a terminal task is never resurrected, and a control
    /// acknowledgement whose receiver walked away cannot block release.
    pub(super) async fn stop_owned_work(&mut self) {
        let active = self.active.take();
        if let Some(active) = &active {
            active.cancel.cancel();
        }
        let pending = self
            .drain
            .take()
            .map_or_else(Vec::new, |drain| drain.waiting);
        while self.jobs.join_next().await.is_some() {}
        if let Some(active) = active
            && let Some(reply) = active.reply
        {
            let _ = reply.send(Err(blocked("supervisor shut down")));
        }
        for waiting in pending {
            let _ = waiting.send(Ok(self.state.clone()));
        }
    }
}

fn blocked(message: &str) -> CoreError {
    CoreError::VerificationBlocked(message.to_owned())
}

/// Acquire the shared workspace lease, then scan under it with both the lease and
/// the task-ownership anchors alive in the actual blocking worker. The lease is
/// returned so the caller can hold exclusion across its own durable write.
async fn leased_capture(
    context: Arc<ToolsContext>,
    lifetime: Arc<dyn Send + Sync>,
    cancel: CancellationToken,
) -> (Result<WorkspaceSnapshot, CoreError>, Option<WorkspaceLease>) {
    // M11 slice 1: when the run path attached its lease (StartRun prepare)
    // reuse that guard — the per-root registry lock is NOT reentrant, so
    // re-acquiring the root the run already holds would self-deadlock.
    // A context without an attached lease keeps the stage-local acquire.
    let lease = match context.workspace_lease().cloned() {
        Some(run_lease) => run_lease,
        None => match WorkspaceLease::acquire(&context.workspace_root, &cancel).await {
            Ok(lease) => lease,
            Err(error) => return (Err(blocked(&error.to_string())), None),
        },
    };
    // Cancellation is rechecked after acquisition and immediately before the
    // scan: a cancelled stage performs no workspace read.
    if cancel.is_cancelled() {
        return (
            Err(blocked("workspace stage cancelled before its scan")),
            None,
        );
    }
    let worker = lease.clone();
    let scanned = tokio::task::spawn_blocking(move || {
        let _guards = (worker, lifetime);
        WorkspaceSnapshot::capture_authorized(&context)
    })
    .await
    .map_err(|error| blocked(&format!("source snapshot worker failed: {error}")))
    .map(|result| result.map_err(CoreError::from));
    match scanned {
        Ok(snapshot) => (snapshot, Some(lease)),
        Err(error) => (Err(error), None),
    }
}

/// The same shared-lease scan, with the test-only hold point that proves exclusion
/// across the rehash → durable-completion window.
async fn leased_rehash(
    context: Arc<ToolsContext>,
    lifetime: Arc<dyn Send + Sync>,
    cancel: CancellationToken,
) -> (Result<WorkspaceSnapshot, CoreError>, Option<WorkspaceLease>) {
    let captured = leased_capture(context, lifetime, cancel).await;
    #[cfg(test)]
    hold::maybe_hold().await;
    captured
}

#[allow(clippy::too_many_arguments)]
async fn leased_plan(
    task_id: TaskId,
    revision: u64,
    context: Arc<ToolsContext>,
    contract: AcceptanceContract,
    baseline: WorkspaceSnapshot,
    hard: &[HardRequirement],
    risk: VerificationRisk,
    cancel: CancellationToken,
    lifetime: Arc<dyn Send + Sync>,
) -> (Result<VerificationPlan, CoreError>, Option<WorkspaceLease>) {
    // M11 slice 1: reuse the run-held lease when present (non-reentrant
    // lock); stage-local acquire only for contexts without one.
    let lease = match context.workspace_lease().cloned() {
        Some(run_lease) => run_lease,
        None => match WorkspaceLease::acquire(&context.workspace_root, &cancel).await {
            Ok(lease) => lease,
            Err(error) => return (Err(blocked(&error.to_string())), None),
        },
    };
    if cancel.is_cancelled() {
        return (
            Err(blocked("workspace stage cancelled before its plan scan")),
            None,
        );
    }
    let hard = hard.to_vec();
    let worker = lease.clone();
    let planned = tokio::task::spawn_blocking(move || {
        let _guards = (worker, lifetime);
        VerificationPlan::build_authorized(
            task_id, revision, &contract, &baseline, &hard, risk, &context,
        )
    })
    .await
    .map_err(|error| blocked(&format!("verification planner failed: {error}")))
    .map(|result| result.map_err(CoreError::from));
    match planned {
        Ok(plan) => (plan, Some(lease)),
        Err(error) => (Err(error), None),
    }
}

/// Test-only helpers for private barriers inside this crate.
#[cfg(test)]
pub(super) mod test_support {
    use super::{JobResult, JobValue, OperationKey};
    use tachyon_types::TaskId;

    /// A synthetic finished job for the ownership drain barrier in `lib.rs`
    /// tests. No live operation matches its key, so the actor discards it.
    pub(crate) fn unowned_job(task: TaskId) -> JobResult {
        JobResult {
            key: OperationKey {
                id: uuid::Uuid::now_v7(),
                task,
                revision: 0,
            },
            value: JobValue::Baseline(Err(crate::CoreError::VerificationBlocked(
                "private test barrier".into(),
            ))),
            lease: None,
        }
    }
}

/// Test-only hold point inside the real final-rehash job: it pauses that job
/// between its authorized scan and the actor's durable completion transaction,
/// with the workspace lease still held. Compiled out of every non-test build.
#[cfg(test)]
pub(super) mod hold {
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    static POINT: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>> = Mutex::new(None);

    /// Serializes the private-barrier tests, because there is one process-wide
    /// hold point. Held for the whole test, never as a timing delay.
    pub(super) static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Install the hold. The receiver fires when the next real rehash reaches it;
    /// sending on the returned sender releases the held job.
    pub(super) fn install() -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let mut point = POINT.lock().expect("hold point");
        assert!(point.is_none(), "a hold point is already installed");
        *point = Some((entered, release_rx));
        (entered_rx, release)
    }

    pub(super) async fn maybe_hold() {
        let taken = POINT.lock().expect("hold point").take();
        if let Some((entered, release)) = taken {
            let _ = entered.send(());
            let _ = release.await;
        }
    }
}

#[cfg(test)]
mod actor_tests {
    //! Private barrier tests through the production supervisor. The hold point
    //! pauses the real final-rehash job with its workspace lease held, so the
    //! rehash → durable-completion window can be observed deterministically.
    use super::hold;
    use crate::{TaskStatus, create_task, recover_task};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tachyon_policy::Policy;
    use tachyon_store::StoreWriter;
    use tachyon_tools::{ToolsContext, artifact::ArtifactSpool, workspace::WorkspaceLease};
    use tachyon_types::{SessionId, WorkspaceId};
    use tachyon_verify::{AcceptanceContract, Clause, CommandCheck, VerificationRisk};
    use tokio_util::sync::CancellationToken;

    struct Fixture {
        root: PathBuf,
        context: Arc<ToolsContext>,
        store: Arc<StoreWriter>,
        task: crate::SupervisorHandle,
    }

    impl Fixture {
        async fn new() -> Self {
            Self::with_contract(AcceptanceContract {
                clauses: vec![Clause::FileUnchanged {
                    path: "source.txt".into(),
                }],
            })
            .await
        }

        async fn with_contract(contract: AcceptanceContract) -> Self {
            let root =
                std::env::temp_dir().join(format!("tachyon-rehash-{}", uuid::Uuid::now_v7()));
            let workspace = root.join("ws");
            std::fs::create_dir_all(&workspace).unwrap();
            std::fs::write(workspace.join("source.txt"), b"original").unwrap();
            let mut policy = Policy::trusted_workspace();
            policy.allow("verify.command", "workspace/**");
            policy.allow("process.spawn", "python3");
            let context = Arc::new(ToolsContext::new(
                workspace,
                policy,
                ArtifactSpool::new(root.join("artifacts")),
            ));
            std::fs::create_dir_all(root.join("state")).unwrap();
            let store = Arc::new(StoreWriter::open(&root.join("state")).await.unwrap());
            let session = SessionId::generate();
            store.create_session(&session.to_string()).await.unwrap();
            let task = create_task(
                session,
                WorkspaceId::generate(),
                "rehash".into(),
                store.clone(),
            )
            .await
            .unwrap();
            task.configure_verification(context.clone(), contract, VerificationRisk::Affected)
                .await
                .unwrap();
            Self {
                root,
                context,
                store,
                task,
            }
        }

        fn canonical_root(&self) -> PathBuf {
            self.context.workspace_root.canonicalize().unwrap()
        }

        async fn row_status(&self) -> String {
            self.store
                .load_task(&self.task.task_id().to_string())
                .await
                .unwrap()
                .unwrap()
                .status
        }

        async fn close(self) {
            self.task.shutdown().await.unwrap();
            self.store.close().await;
            std::fs::remove_dir_all(self.root).unwrap();
        }
    }

    /// Waits, without a fixed sleep, until the durable row records `status`.
    async fn durable_status(f: &Fixture, status: &str) -> bool {
        for _ in 0..4_000 {
            if f.row_status().await == status {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    }

    /// One poll: true when the future is still pending, without waiting on it.
    async fn pending(future: std::pin::Pin<&mut impl std::future::Future>) -> bool {
        let mut future = future;
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx).is_pending()))
            .await
    }

    #[tokio::test]
    async fn stalled_rehash_keeps_serving_and_refuses_a_stale_completion() {
        let _serial = hold::SERIAL.lock().await;
        let f = Fixture::new().await;
        let (entered, release) = hold::install();
        let handle = f.task.clone();
        let context = f.context.clone();
        let pending_verify = tokio::spawn(async move { handle.verify_and_complete(context).await });
        // The real rehash job now holds the workspace lease inside its window.
        tokio::time::timeout(Duration::from_secs(10), entered)
            .await
            .expect("the final rehash job never reached its authorized-scan hold point")
            .unwrap();
        assert_ne!(f.row_status().await, "Completed");
        // Steering is serviceable while that job is stalled, and it supersedes it.
        let steered = tokio::time::timeout(
            Duration::from_secs(2),
            f.task.add_message("stop this approach".into()),
        )
        .await
        .expect("steering blocked behind the stalled final rehash")
        .unwrap();
        assert_eq!(steered.revision, 2);
        assert!(steered.verification.unwrap().interrupted);
        release.send(()).unwrap();
        let refused = tokio::time::timeout(Duration::from_secs(5), pending_verify)
            .await
            .unwrap()
            .unwrap();
        assert!(
            refused.is_err(),
            "stale completion was accepted after the operation moved on"
        );
        let state = f.task.get_state().await.unwrap();
        assert_ne!(state.status, TaskStatus::Completed);
        assert!(state.verification.unwrap().report.is_none());
        assert_ne!(f.row_status().await, "Completed");
        // The discarded result released its lease: the workspace is free again.
        let lease = tokio::time::timeout(
            Duration::from_secs(5),
            WorkspaceLease::acquire(&f.root.join("ws/."), &CancellationToken::new()),
        )
        .await
        .expect("a discarded result leaked the workspace lease")
        .unwrap();
        assert_eq!(lease.root(), f.canonical_root());
        drop(lease);
        f.close().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unknown_verification_effect_is_journaled_and_cannot_be_replayed() {
        let _serial = hold::SERIAL.lock().await;
        let f = Fixture::with_contract(AcceptanceContract {
            clauses: vec![Clause::CommandPasses {
                command: CommandCheck {
                    program: "python3".into(),
                    args: vec![
                        "-c".into(),
                        "from pathlib import Path\nimport time\nwith open('marker', 'a') as f: f.write('x')\ntime.sleep(30)".into(),
                    ],
                    cwd: ".".into(),
                    env: BTreeMap::default(),
                    timeout_ms: 100,
                },
            }],
        })
        .await;

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            f.task.verify_and_complete(f.context.clone()),
        )
        .await
        .expect("timed verification did not settle");
        assert!(
            result.is_err(),
            "unknown check effect cannot complete a task"
        );
        assert_eq!(
            std::fs::read_to_string(f.context.workspace_root.join("marker")).unwrap(),
            "x"
        );

        let state = f.task.get_state().await.unwrap();
        assert_eq!(state.status, TaskStatus::Recovering);
        let verification = state.verification.unwrap();
        assert!(verification.interrupted);
        assert!(!verification.in_progress);
        assert!(verification.report.is_none());

        assert!(
            f.task.verify_and_complete(f.context.clone()).await.is_err(),
            "an interrupted check with an unknown effect must not be replayed"
        );
        assert_eq!(
            std::fs::read_to_string(f.context.workspace_root.join("marker")).unwrap(),
            "x"
        );
        f.close().await;
    }

    #[tokio::test]
    async fn competing_alias_stage_cannot_enter_between_rehash_and_durable_completion() {
        let _serial = hold::SERIAL.lock().await;
        let f = Fixture::new().await;
        let canonical = f.canonical_root();
        #[allow(unused_mut)]
        let mut aliases = vec![f.root.join("ws/./../ws")];
        #[cfg(unix)]
        {
            let link = f.root.join("ws-alias");
            std::os::unix::fs::symlink(&f.context.workspace_root, &link).unwrap();
            aliases.push(link);
        }
        let (entered, release) = hold::install();
        let handle = f.task.clone();
        let context = f.context.clone();
        let completion = tokio::spawn(async move { handle.verify_and_complete(context).await });
        tokio::time::timeout(Duration::from_secs(10), entered)
            .await
            .expect("the final rehash job never reached its authorized-scan hold point")
            .unwrap();
        // Competing same-root stages, spelled as canonical aliases, try to enter
        // while the rehash window is open. Each records what the durable row said
        // when it was actually admitted.
        let mut contenders = Vec::new();
        for alias in aliases {
            let store = f.store.clone();
            let id = f.task.task_id();
            contenders.push(tokio::spawn(async move {
                let lease = WorkspaceLease::acquire(&alias, &CancellationToken::new())
                    .await
                    .unwrap();
                let status = store
                    .load_task(&id.to_string())
                    .await
                    .unwrap()
                    .unwrap()
                    .status;
                (lease.root().to_path_buf(), status)
            }));
        }
        for contender in &mut contenders {
            assert!(
                pending(std::pin::Pin::new(contender)).await,
                "a competing workspace stage was admitted inside the rehash window"
            );
        }
        assert_ne!(
            f.row_status().await,
            "Completed",
            "completion became durable before the rehash window closed"
        );
        release.send(()).unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(10), completion)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, TaskStatus::Completed);
        for contender in contenders {
            let (root, admitted_status) = tokio::time::timeout(Duration::from_secs(10), contender)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                root, canonical,
                "alias spelling bypassed workspace exclusion"
            );
            assert_eq!(
                admitted_status, "Completed",
                "a competing same-root stage entered between the final rehash and the durable completion"
            );
        }
        f.close().await;
    }

    #[tokio::test]
    async fn pause_acknowledges_only_after_the_drain_and_survives_a_dropped_waiter() {
        let _serial = hold::SERIAL.lock().await;
        let f = Fixture::new().await;
        let (entered, release) = hold::install();
        let handle = f.task.clone();
        let context = f.context.clone();
        let completion = tokio::spawn(async move { handle.verify_and_complete(context).await });
        tokio::time::timeout(Duration::from_secs(10), entered)
            .await
            .expect("the final rehash job never reached its authorized-scan hold point")
            .unwrap();
        let mut pause = Box::pin(f.task.pause());
        assert!(pending(pause.as_mut()).await);
        // The durable intent is recorded before the drain can finish: the held
        // rehash job cannot be released until this test releases it.
        assert!(
            durable_status(&f, "Paused").await,
            "the pause intent was not durable before the drain: {}",
            f.row_status().await
        );
        // ... and the acknowledgement waits for the real worker to finish.
        assert!(
            pending(pause.as_mut()).await,
            "pause was acknowledged while an effect worker still ran"
        );
        // Further reads and steering stay serviceable while that ack waits.
        let observed = tokio::time::timeout(Duration::from_secs(2), f.task.get_state())
            .await
            .expect("GetState blocked behind a pending pause acknowledgement")
            .unwrap();
        assert_eq!(observed.status, TaskStatus::Paused);
        let steered = tokio::time::timeout(
            Duration::from_secs(2),
            f.task.add_message("still serviceable".into()),
        )
        .await
        .expect("steering blocked behind a pending pause acknowledgement")
        .unwrap();
        assert_eq!(steered.revision, 2);
        // The caller walks away: cleanup must still complete and release.
        drop(pause);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), f.task.shutdown())
            .await
            .expect("a dropped control waiter deadlocked owner shutdown")
            .unwrap();
        let refused = completion.await.unwrap();
        assert!(refused.is_err());
        let recovered = tokio::time::timeout(
            Duration::from_secs(10),
            recover_task(f.task.task_id(), f.store.clone()),
        )
        .await
        .expect("owner shutdown did not release durable admission")
        .unwrap();
        let state = recovered.get_state().await.unwrap();
        assert_eq!(state.status, TaskStatus::Paused);
        assert!(state.verification.unwrap().interrupted);
        recovered.shutdown().await.unwrap();
        f.close().await;
    }
}
