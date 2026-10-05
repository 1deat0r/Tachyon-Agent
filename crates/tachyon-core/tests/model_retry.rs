//! Bounded model retry safety on the production Supervisor path.
use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tachyon_core::driver::{DriveError, DriveHost, EvidenceMode, RunPlan, TaskModelContext, drive};
use tachyon_core::runtime::{EvidenceRequest, RuntimeBounds};
use tachyon_core::{CoreError, SupervisorHandle, TaskStatus, create_task};
use tachyon_models::{
    AgentDecision, ModelCapabilities, ModelError, ModelEventSink, ModelFeature, ModelInvocation,
    ModelProvider, ModelRequest, ModelResult, ModelUsage, ProviderEstimate, UsageProvenance,
};
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::{ProviderId, SessionId, WorkspaceId};
use tachyon_verify::{AcceptanceContract, Clause, VerificationRisk};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const BROKEN: &str = "pub fn answer() -> u8 { 7 }\n";
const FIXED: &str = "pub fn answer() -> u8 { 42 }\n";

struct Provider {
    script: Mutex<VecDeque<Result<AgentDecision, ModelError>>>,
    requests: Mutex<Vec<ModelRequest>>,
    entered: Notify,
    release: Notify,
    block_at: Option<usize>,
    steer: Option<SupervisorHandle>,
    rewrite: Option<PathBuf>,
    first_delay: Option<Duration>,
    first_call_at: Mutex<Option<tokio::time::Instant>>,
    second_finish_at: Mutex<Option<tokio::time::Instant>>,
}

impl Provider {
    fn new(script: Vec<Result<AgentDecision, ModelError>>) -> Self {
        Self {
            script: Mutex::new(script.into()),
            requests: Mutex::new(Vec::new()),
            entered: Notify::new(),
            release: Notify::new(),
            block_at: None,
            steer: None,
            rewrite: None,
            first_delay: None,
            first_call_at: Mutex::new(None),
            second_finish_at: Mutex::new(None),
        }
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

struct CallClock<'a> {
    finish_at: &'a Mutex<Option<tokio::time::Instant>>,
    resume: bool,
}
impl Drop for CallClock<'_> {
    fn drop(&mut self) {
        *self.finish_at.lock().unwrap() = Some(tokio::time::Instant::now());
        if self.resume {
            // Journal acknowledgments use real SQLite workers. Do not let
            // paused-time auto-advance expire their pool-acquisition timers.
            tokio::time::resume();
        }
    }
}

#[async_trait::async_trait]
impl ModelProvider for Provider {
    fn id(&self) -> ProviderId {
        ProviderId("retry-test".into())
    }
    fn capabilities(&self) -> ModelCapabilities {
        ModelCapabilities {
            features: BTreeSet::from([ModelFeature::StructuredOutput]),
            context_window_tokens: 128_000,
            ..ModelCapabilities::default()
        }
    }
    fn estimate(&self, _: &ModelRequest) -> ProviderEstimate {
        ProviderEstimate {
            latency_ms: 1.0,
            input_tokens: 1,
        }
    }
    async fn invoke(
        &self,
        request: ModelRequest,
        sink: ModelEventSink,
    ) -> Result<ModelResult, ModelError> {
        self.invoke_observed(request, sink).await.result
    }
    async fn invoke_observed(&self, request: ModelRequest, _: ModelEventSink) -> ModelInvocation {
        self.requests.lock().unwrap().push(request.clone());
        let count = self.count();
        if count == 1
            && let Some(delay) = self.first_delay
        {
            tokio::time::pause();
            *self.first_call_at.lock().unwrap() = Some(tokio::time::Instant::now());
            tokio::time::sleep(delay).await;
        }
        self.entered.notify_one();
        let _clock = (count == 2).then(|| CallClock {
            finish_at: &self.second_finish_at,
            resume: self.first_delay.is_some(),
        });
        if self.block_at == Some(count) {
            self.release.notified().await;
        }
        if count == 1 {
            if let Some(task) = &self.steer {
                task.add_message("new constraint".into()).await.unwrap();
            }
            if let Some(path) = &self.rewrite {
                std::fs::write(path, "external edit\n").unwrap();
            }
        }
        let usage = ModelUsage {
            input_tokens: Some(11),
            output_tokens: Some(7),
            provenance: UsageProvenance::ProviderReported,
        };
        let result = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra call")
            .map(|decision| ModelResult {
                decision,
                usage,
                input_tokens: 11,
                output_tokens: 7,
                latency_ms: 1.0,
                provider: self.id(),
                model: request.model,
            });
        ModelInvocation { result, usage }
    }
}

struct Harness {
    dir: PathBuf,
    ws: PathBuf,
    store: Arc<StoreWriter>,
    task: SupervisorHandle,
    context: Arc<ToolsContext>,
    plan: RunPlan,
}

impl Harness {
    async fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("tachyon-retry-{}", uuid::Uuid::now_v7()));
        let ws = dir.join("ws");
        std::fs::create_dir_all(ws.join("src")).unwrap();
        std::fs::write(ws.join("src/lib.rs"), BROKEN).unwrap();
        std::fs::create_dir_all(dir.join("state")).unwrap();
        let store = Arc::new(StoreWriter::open(&dir.join("state")).await.unwrap());
        let session = SessionId::generate();
        store.create_session(&session.to_string()).await.unwrap();
        let task = create_task(
            session,
            WorkspaceId::generate(),
            "Fix answer".into(),
            store.clone(),
        )
        .await
        .unwrap();
        task.pin_workspace_root(ws.canonicalize().unwrap().display().to_string())
            .await
            .unwrap();
        let state = task.get_state().await.unwrap();
        let mut policy = Policy::trusted_workspace();
        policy.allow("mutation.patch", "workspace/**");
        policy.allow("fs.delete", "workspace/**");
        policy.allow("verify.command", "workspace/**");
        let context = Arc::new(ToolsContext::new(
            ws.clone(),
            policy,
            ArtifactSpool::new(dir.join("artifacts")),
        ));
        let plan = RunPlan {
            origin: Instant::now(),
            evidence_mode: EvidenceMode::Serial,
            evidence: vec![EvidenceRequest {
                capability: "fs.read".into(),
                path: "src/lib.rs".into(),
            }],
            contract: AcceptanceContract {
                clauses: vec![Clause::ChangedPathsWithin {
                    paths: vec!["src/lib.rs".into()],
                }],
            },
            risk: VerificationRisk::Affected,
            mutation_dir: dir.join("mutation"),
            batch_id: "retry-batch".into(),
            model: "retry-test".into(),
            task_context: TaskModelContext::from_task(&state),
            requested_checks: vec![],
            available_checks: vec![],
            bounds: RuntimeBounds::default(),
            cancel: CancellationToken::new(),
        };
        Self {
            dir,
            ws,
            store,
            task,
            context,
            plan,
        }
    }
    async fn run(
        &self,
        provider: Arc<Provider>,
    ) -> Result<tachyon_core::driver::RunOutcome, DriveError> {
        drive(
            DriveHost::Supervisor {
                handle: self.task.clone(),
                store: self.store.clone(),
            },
            self.context.clone(),
            provider,
            self.plan.clone(),
        )
        .await
    }
    fn source(&self) -> String {
        std::fs::read_to_string(self.ws.join("src/lib.rs")).unwrap()
    }
    async fn close(self) {
        let _ = self.task.shutdown().await;
        self.store.close().await;
        std::fs::remove_dir_all(self.dir).unwrap();
    }
}

fn malformed() -> Result<AgentDecision, ModelError> {
    Err(ModelError::MalformedOutput(
        "secret response must not be journaled".into(),
    ))
}
#[allow(clippy::unnecessary_wraps)] // script entries intentionally share Result
fn patch(base: &str, replacement: &str) -> Result<AgentDecision, ModelError> {
    Ok(serde_json::from_value(
        serde_json::json!({"decision":"propose_execution", "operations":[{
        "capability":"mutation.patch", "reason":"repair", "args":{"path":"src/lib.rs",
        "base_hash":tachyon_mutation::blake3_hex(base.as_bytes()),"new_content":replacement}}]}),
    )
    .unwrap())
}

#[tokio::test]
async fn malformed_then_valid_completes_with_identical_context_and_all_usage() {
    let h = Harness::new().await;
    let p = Arc::new(Provider::new(vec![malformed(), patch(BROKEN, FIXED)]));
    let outcome = h.run(p.clone()).await.unwrap();
    assert_eq!(outcome.outcome.as_deref(), Some("completed"));
    assert_eq!(outcome.recovery.as_deref(), Some("recovered_completed"));
    assert_eq!(h.source(), FIXED);
    assert_eq!(p.count(), 2);
    {
        let requests = p.requests.lock().unwrap();
        assert_eq!(requests[0], requests[1]);
    }
    assert_eq!(outcome.usage.input_tokens, Some(22));
    assert_eq!(outcome.usage.output_tokens, Some(14));
    assert_eq!(
        outcome.model_attempts[0].error.as_deref(),
        Some("malformed_output")
    );
    assert!(outcome.model_attempts[1].error.is_none());
    let state = outcome.state.unwrap();
    assert_eq!(
        state
            .stages
            .iter()
            .filter(|s| s.stage == "model_attempt")
            .count(),
        2
    );
    assert!(
        !serde_json::to_string(&state)
            .unwrap()
            .contains("secret response")
    );
    h.close().await;
}

#[tokio::test]
async fn exhausted_or_nonretryable_errors_never_mutate_and_keep_their_type() {
    for (error, calls) in [
        (ModelError::MalformedOutput("bad".into()), 2),
        (ModelError::Unauthorized, 1),
        (ModelError::Transport("offline".into()), 1),
    ] {
        let h = Harness::new().await;
        let p = Arc::new(Provider::new(vec![Err(error.clone()), Err(error.clone())]));
        assert!(matches!(h.run(p.clone()).await, Err(DriveError::Provider(e)) if e == error));
        assert_eq!(p.count(), calls);
        assert_eq!(h.source(), BROKEN);
        assert!(!h.plan.mutation_dir.exists());
        let state = h
            .task
            .mark_failed("exhausted model attempts".into())
            .await
            .unwrap();
        assert_eq!(state.status, TaskStatus::Failed);
        h.task.shutdown().await.unwrap();
        let recovered = tachyon_core::recover_task(h.task.task_id(), h.store.clone())
            .await
            .unwrap();
        assert_eq!(
            recovered.get_state().await.unwrap().status,
            TaskStatus::Failed
        );
        recovered.shutdown().await.unwrap();
        h.close().await;
    }
}

#[tokio::test]
async fn valid_first_call_and_nonexecution_decisions_are_not_retried() {
    let h = Harness::new().await;
    let p = Arc::new(Provider::new(vec![patch(BROKEN, FIXED)]));
    assert!(h.run(p.clone()).await.is_ok());
    assert_eq!(p.count(), 1);
    h.close().await;
    let h = Harness::new().await;
    let p = Arc::new(Provider::new(vec![Ok(AgentDecision::Complete {
        summary: "done".into(),
    })]));
    assert!(matches!(
        h.run(p.clone()).await,
        Err(DriveError::NonExecutionDecision(_))
    ));
    assert_eq!(p.count(), 1);
    assert_eq!(h.source(), BROKEN);
    h.close().await;
}

#[tokio::test]
async fn steering_after_failure_prevents_retry_and_writes() {
    let h = Harness::new().await;
    let mut p = Provider::new(vec![malformed(), patch(BROKEN, FIXED)]);
    p.steer = Some(h.task.clone());
    let p = Arc::new(p);
    assert!(matches!(
        h.run(p.clone()).await,
        Err(DriveError::Core(CoreError::StaleRunProposal { .. }))
    ));
    assert_eq!(p.count(), 1);
    assert_eq!(h.source(), BROKEN);
    h.close().await;
}

#[tokio::test]
async fn cancellation_during_either_attempt_drains_without_writes() {
    for attempt in [1, 2] {
        let h = Harness::new().await;
        let mut p = Provider::new(vec![malformed(), patch(BROKEN, FIXED)]);
        p.block_at = Some(attempt);
        let p = Arc::new(p);
        let run = h.run(p.clone());
        let cancel = async {
            while p.count() < attempt {
                p.entered.notified().await;
            }
            h.plan.cancel.cancel();
        };
        let (result, ()) = tokio::join!(run, cancel);
        assert!(matches!(result, Err(DriveError::RunCancelled)));
        assert_eq!(p.count(), attempt);
        assert_eq!(h.source(), BROKEN);
        assert!(!h.plan.mutation_dir.exists());
        h.close().await;
    }
}

#[tokio::test]
async fn retry_uses_one_total_deadline() {
    let mut h = Harness::new().await;
    h.plan.bounds.model_deadline_ms = 200;
    let mut p = Provider::new(vec![malformed(), patch(BROKEN, FIXED)]);
    p.block_at = Some(2);
    p.first_delay = Some(Duration::from_millis(100));
    let p = Arc::new(p);
    let clock = async {
        // Keep a runnable controller while SQLite acknowledgments settle.
        // This prevents unrelated timers from auto-advancing virtual time.
        while p.first_call_at.lock().unwrap().is_none() {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(101)).await;
        while p.count() < 2 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(109)).await;
    };
    let (result, ()) = tokio::join!(h.run(p.clone()), clock);
    assert!(
        matches!(
            &result,
            Err(DriveError::Provider(ModelError::Timeout {
                timeout_ms: 200
            }))
        ),
        "unexpected result: {:?}",
        result.as_ref().err()
    );
    // Virtual time: resetting the retry budget would take about 300 ms.
    let elapsed =
        p.second_finish_at.lock().unwrap().unwrap() - p.first_call_at.lock().unwrap().unwrap();
    assert!(elapsed >= Duration::from_millis(100));
    assert!(elapsed < Duration::from_millis(250));
    assert_eq!(p.count(), 2);
    assert_eq!(h.source(), BROKEN);
    h.close().await;
}

#[tokio::test]
async fn stale_preimages_after_retry_refuse_mutation() {
    let h = Harness::new().await;
    let mut p = Provider::new(vec![malformed(), patch(BROKEN, FIXED)]);
    p.rewrite = Some(h.ws.join("src/lib.rs"));
    let p = Arc::new(p);
    assert!(matches!(
        h.run(p.clone()).await,
        Err(DriveError::Mutation(_))
    ));
    assert_eq!(p.count(), 2);
    assert_eq!(h.source(), "external edit\n");
    h.close().await;
}

#[tokio::test]
async fn steering_during_retry_rejects_late_valid_proposal() {
    let h = Harness::new().await;
    let mut p = Provider::new(vec![malformed(), patch(BROKEN, FIXED)]);
    p.block_at = Some(2);
    let p = Arc::new(p);
    let steer = async {
        while p.count() < 2 {
            p.entered.notified().await;
        }
        h.task.add_message("new constraint".into()).await.unwrap();
        p.release.notify_one();
    };
    let (result, ()) = tokio::join!(h.run(p.clone()), steer);
    assert!(matches!(
        result,
        Err(DriveError::Core(CoreError::StaleRunProposal { .. }))
    ));
    assert_eq!(p.count(), 2);
    assert_eq!(h.source(), BROKEN);
    assert!(!h.plan.mutation_dir.exists());
    h.close().await;
}

#[tokio::test]
async fn valid_but_invalid_patch_is_never_retried_or_applied() {
    let h = Harness::new().await;
    let decision = serde_json::from_value(serde_json::json!({
        "decision":"propose_execution", "operations":[{
            "capability":"mutation.patch", "reason":"invalid escape", "args":{
                "path":"../escape.rs", "base_hash":tachyon_mutation::blake3_hex(BROKEN.as_bytes()),
                "new_content":FIXED
            }
        }]
    }))
    .unwrap();
    let p = Arc::new(Provider::new(vec![Ok(decision)]));
    assert!(h.run(p.clone()).await.is_err());
    assert_eq!(p.count(), 1);
    assert_eq!(h.source(), BROKEN);
    assert!(!h.plan.mutation_dir.exists());
    assert!(!h.dir.join("escape.rs").exists());
    h.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn retry_cannot_turn_a_failed_acceptance_check_into_completion() {
    let mut h = Harness::new().await;
    Arc::get_mut(&mut h.context)
        .unwrap()
        .policy
        .allow("process.spawn", "sh");
    h.plan.contract.clauses.push(Clause::CommandPasses {
        command: tachyon_verify::CommandCheck {
            program: "sh".into(),
            args: vec!["-c".into(), "exit 1".into()],
            cwd: ".".into(),
            env: std::collections::BTreeMap::new(),
            timeout_ms: 1_000,
        },
    });
    let p = Arc::new(Provider::new(vec![malformed(), patch(BROKEN, FIXED)]));
    assert!(matches!(h.run(p.clone()).await,
        Err(DriveError::Core(CoreError::VerificationBlocked(reason)))
        if reason.contains("Some(Failed)") && !reason.contains("approval required")));
    assert_ne!(
        h.task.get_state().await.unwrap().status,
        TaskStatus::Completed
    );
    assert_eq!(h.source(), FIXED);
    assert_eq!(p.count(), 2);
    h.close().await;
}
