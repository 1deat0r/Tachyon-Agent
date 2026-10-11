//! Milestone 21: deterministic production-driver replay of the four
//! retained M20 failure classes (`docs/milestones/M21_PLAN.md`).
//!
//! Every regression runs the ONE production driver
//! (`tachyon_core::driver::drive`) against a fresh scratch copy of the
//! checked-in `reservation-ledger` fixture. No live model calls happen:
//! a scripted provider replays the exact retained behavior. Production
//! behavior, prompt, acceptance, model policy, and retry policy are
//! unchanged; these regressions turn the M20 failures into development
//! data before any prompt, retry, or deadline candidate is selected.
//!
//! Replayed cells (frozen inputs hashed in `M21_MANIFEST.json`; the
//! retained bytes are also BLAKE3-pinned below so a drift fails loudly):
//!
//! * `reservation-ledger/full/3` — partial repair: the exact retained
//!   `reserve.rs` patch applies but the acceptance contract rejects it
//!   (the unchanged broken `release.rs` still fails visible and hidden
//!   tests), the run fails with `VerificationBlocked`, and the production
//!   host failure path replays `recovered_failed`.
//! * `reservation-ledger/serial/3` — transport error: one attempt, no
//!   blind retry, unknown usage, zero writes, durable `recovered_failed`.
//! * `reservation-ledger/full/4` and `reservation-ledger/serial/4` —
//!   model-stage deadline: the provider parks past the configured
//!   deadline, the production `timeout_at` path fires the typed
//!   `ModelError::Timeout`, one attempt, unknown usage, zero writes,
//!   durable `recovered_failed`. The deadline value is shortened for
//!   test wall-clock; the mechanism is the production path with the same
//!   typed error, usage accounting, and retry policy as the frozen
//!   120000 ms protocol.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use tachyon_core::driver::{DriveError, DriveHost, EvidenceMode, RunPlan, TaskModelContext, drive};
use tachyon_core::runtime::{EvidenceRequest, RuntimeBounds};
use tachyon_core::{CoreError, SupervisorHandle, TaskStatus, create_task, recover_task};
use tachyon_models::{
    AgentDecision, ModelCallRecord, ModelCapabilities, ModelError, ModelEventSink, ModelFeature,
    ModelInvocation, ModelProvider, ModelRequest, ModelResult, ModelUsage, ProposedOperation,
    ProviderEstimate, UsageProvenance,
};
use tachyon_mutation::blake3_hex;
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::{CapabilityId, ProviderId, SessionId, WorkspaceId};
use tachyon_verify::{AcceptanceContract, VerificationRisk};
use tokio_util::sync::CancellationToken;

const FIXTURE_ID: &str = "reservation-ledger";

/// BLAKE3 of the retained partial patch
/// (`docs/milestones/STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.reserve.rs.txt`).
const RETAINED_PATCH_BLAKE3: &str =
    "a2d0cccbb0a5df5f6f36aac2f30bceeadb84b7dc52b2ac8eb4904e414bff651a";
/// BLAKE3 of the retained unchanged release counterpart, byte-identical
/// to the checked-in broken `fixtures/reservation-ledger/subject/src/release.rs`.
const RETAINED_RELEASE_BLAKE3: &str =
    "c0143346246999905e7eadce7ff6409317009a27dc6bf5be50c329d036ba0cf4";
/// BLAKE3 of the retained replay log
/// (`docs/milestones/STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.log.txt`).
const RETAINED_LOG_BLAKE3: &str =
    "69853fa71d02a50de2334d552a3bd205ad2bb649f31f95be6b17660ff7b2319d";
/// BLAKE3 of the checked-in broken
/// `fixtures/reservation-ledger/subject/src/reserve.rs` the retained patch bound to.
const BROKEN_RESERVE_BLAKE3: &str =
    "99210fd406a540aa0cd9387590d4600dfff7427dea6e769dda59aa2647986170";

/// Model-stage deadline for the deadline regressions. The frozen M20
/// protocol used 120000 ms; the production `timeout_at` path, typed
/// error, usage accounting, and retry policy are value-independent, so
/// the regression exercises the same mechanism with a short deadline.
const REPLAY_DEADLINE_MS: u64 = 250;
/// How long the scripted provider parks before the deadline must fire.
const HANG: Duration = Duration::from_secs(30);

/// One fixture's benchmark descriptor (`fixtures/reservation-ledger/bench.json`).
#[derive(Debug, Deserialize)]
struct Descriptor {
    objective: String,
    evidence_paths: Vec<String>,
    protected_paths: Vec<String>,
    change_paths: Vec<String>,
    contract: AcceptanceContract,
    requested_checks: Vec<String>,
    available_checks: Vec<String>,
}

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn descriptor() -> Descriptor {
    let path = fixtures_root().join(FIXTURE_ID).join("bench.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("descriptor {}: {error}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|error| panic!("descriptor parse: {error}"))
}

/// The retained M20 failure artifact, byte-exact.
fn retained(name: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/milestones")
        .join(name);
    std::fs::read(&path)
        .unwrap_or_else(|error| panic!("retained artifact {}: {error}", path.display()))
}

/// One scripted provider behavior per regression.
enum Behavior {
    /// The retained partial repair: one `mutation.patch` operation.
    Patch {
        path: String,
        base_hash: String,
        new_content: String,
    },
    /// A provider-side failure with unknown usage (the M20 transport
    /// and timeout records carry no usage metadata).
    Error(ModelError),
    /// Parks past the model-stage deadline.
    Hang,
}

struct ReplayProvider {
    behavior: Behavior,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ReplayProvider {
    fn patch(path: &str, base_hash: String, new_content: String) -> Self {
        Self {
            behavior: Behavior::Patch {
                path: path.to_owned(),
                base_hash,
                new_content,
            },
            requests: Mutex::new(Vec::new()),
        }
    }

    fn error(error: ModelError) -> Self {
        Self {
            behavior: Behavior::Error(error),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn hang() -> Self {
        Self {
            behavior: Behavior::Hang,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn request_count(&self) -> usize {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

#[async_trait]
impl ModelProvider for ReplayProvider {
    fn id(&self) -> ProviderId {
        ProviderId("m21-replay".into())
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
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request.clone());
        match &self.behavior {
            Behavior::Patch {
                path,
                base_hash,
                new_content,
            } => {
                let decision = AgentDecision::ProposeExecution {
                    operations: vec![ProposedOperation {
                        capability: CapabilityId("mutation.patch".into()),
                        args: serde_json::json!({
                            "path": path,
                            "base_hash": base_hash,
                            "new_content": new_content,
                        }),
                        reason: "replay the retained M20 partial repair".to_owned(),
                    }],
                };
                let usage = ModelUsage {
                    input_tokens: Some(1200),
                    output_tokens: Some(340),
                    provenance: UsageProvenance::ProviderReported,
                };
                ModelInvocation {
                    result: Ok(ModelResult {
                        decision,
                        input_tokens: 1200,
                        output_tokens: 340,
                        usage,
                        latency_ms: 1.0,
                        provider: self.id(),
                        model: request.model,
                    }),
                    usage,
                }
            }
            Behavior::Error(error) => ModelInvocation {
                result: Err(error.clone()),
                usage: ModelUsage::default(),
            },
            Behavior::Hang => {
                tokio::time::sleep(HANG).await;
                // Reached only if the deadline failed to fire: a loud
                // failure, never a silent pass.
                ModelInvocation {
                    result: Err(ModelError::Internal(
                        "hang outlived the model-stage deadline".into(),
                    )),
                    usage: ModelUsage::default(),
                }
            }
        }
    }
}

/// Fresh scratch workspace plus the checked-in fixture copy every
/// regression replays against. The checked-in tree is never patched.
struct Harness {
    dir: PathBuf,
    ws: PathBuf,
    fixture: PathBuf,
    store: Arc<StoreWriter>,
    task: SupervisorHandle,
    context: Arc<ToolsContext>,
    plan: RunPlan,
    descriptor: Descriptor,
}

impl Harness {
    async fn new(evidence_mode: EvidenceMode, deadline_ms: u64) -> Self {
        let descriptor = descriptor();
        let fixture = fixtures_root().join(FIXTURE_ID);
        let dir = std::env::temp_dir().join(format!("tachyon-m21-{}", uuid::Uuid::now_v7()));
        let ws = dir.join("ws");
        copy_dir(&fixture, &ws).expect("fixture copy");
        std::fs::create_dir_all(dir.join("state")).expect("state dir");
        let store = Arc::new(
            StoreWriter::open(&dir.join("state"))
                .await
                .expect("store open"),
        );
        let session = SessionId::generate();
        store
            .create_session(&session.to_string())
            .await
            .expect("session create");
        let task = create_task(
            session,
            WorkspaceId::generate(),
            descriptor.objective.clone(),
            store.clone(),
        )
        .await
        .expect("create_task");
        task.pin_workspace_root(
            ws.canonicalize()
                .expect("canonicalize")
                .display()
                .to_string(),
        )
        .await
        .expect("pin workspace root");
        let state = task.get_state().await.expect("task state");
        let mut policy = Policy::trusted_workspace();
        policy.allow("fs.read", "workspace/**");
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
            evidence_mode,
            evidence: descriptor
                .evidence_paths
                .iter()
                .map(|path| EvidenceRequest {
                    capability: "fs.read".into(),
                    path: path.clone(),
                })
                .collect(),
            contract: descriptor.contract.clone(),
            risk: VerificationRisk::Affected,
            mutation_dir: dir.join("mutation"),
            batch_id: "m21-replay-1".into(),
            model: "m21-replay-provider".into(),
            task_context: TaskModelContext::from_task(&state),
            requested_checks: descriptor.requested_checks.clone(),
            available_checks: descriptor.available_checks.clone(),
            bounds: RuntimeBounds {
                model_deadline_ms: deadline_ms,
                ..RuntimeBounds::default()
            },
            cancel: CancellationToken::new(),
        };
        Self {
            dir,
            ws,
            fixture,
            store,
            task,
            context,
            plan,
            descriptor,
        }
    }

    /// Byte snapshot of every protected path in the scratch workspace.
    fn protected_snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.descriptor
            .protected_paths
            .iter()
            .map(|rel| {
                (
                    rel.clone(),
                    std::fs::read(self.ws.join(rel)).unwrap_or_default(),
                )
            })
            .collect()
    }

    /// Every file in the scratch workspace (target/ excluded) whose
    /// bytes differ from the checked-in fixture: the observed change set.
    fn observed_changes(&self) -> Vec<String> {
        observed_changes(&self.fixture, &self.ws)
    }

    /// One `cargo test --offline --locked` invocation in the scratch
    /// workspace. `scratch_target` isolates the build into the harness
    /// directory; otherwise the build reuses the workspace target the
    /// production verification run already populated.
    async fn cargo_test(&self, extra: &[&str], scratch_target: bool) -> (bool, String) {
        let mut command = tokio::process::Command::new("cargo");
        command
            .args(["test", "--offline", "--locked"])
            .args(extra)
            .current_dir(&self.ws)
            .env("CARGO_NET_OFFLINE", "true");
        if scratch_target {
            command.env("CARGO_TARGET_DIR", self.dir.join("target"));
        }
        let out = command.output().await.expect("cargo test could not start");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
        )
    }

    /// The durable `model_attempt` records from the task journal.
    async fn model_attempts(&self) -> Vec<ModelCallRecord> {
        let state = self.task.get_state().await.expect("task state");
        state
            .stages
            .iter()
            .filter(|record| record.stage == "model_attempt")
            .map(|record| {
                serde_json::from_str(&record.detail).expect("model_attempt record parses")
            })
            .collect()
    }

    /// The production host failure path (`bench_matrix` error arm):
    /// mark the run failed, shut down, and prove recovery replays the
    /// durable `Failed` status.
    async fn mark_failed_and_recover(&self, reason: String) {
        let state = self.task.mark_failed(reason).await.expect("mark_failed");
        assert_eq!(state.status, TaskStatus::Failed);
        self.task.shutdown().await.expect("shutdown");
        let recovered = recover_task(self.task.task_id(), self.store.clone())
            .await
            .expect("recover_task");
        let state = recovered.get_state().await.expect("recovered state");
        assert_eq!(state.status, TaskStatus::Failed);
        recovered.shutdown().await.expect("recovered shutdown");
    }

    async fn close(self) {
        self.store.close().await;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

fn observed_changes(fixture: &Path, ws: &Path) -> Vec<String> {
    let mut changed = Vec::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(ws.join(&dir)) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let rel = dir.join(&name);
            let rel_str = rel.to_string_lossy().replace('\\', "/");
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                if rel_str == "target" {
                    continue;
                }
                stack.push(rel);
                continue;
            }
            let now = std::fs::read(ws.join(&rel)).unwrap_or_default();
            let before = std::fs::read(fixture.join(&rel)).unwrap_or_default();
            if now != before {
                changed.push(rel_str);
            }
        }
    }
    changed.sort();
    changed
}

/// `reservation-ledger/full/3`: the retained partial repair applies
/// exactly one authorized file, the acceptance gate rejects it with the
/// retained failing tests, and the failure becomes durable.
#[tokio::test]
async fn retained_partial_repair_is_rejected_by_acceptance_and_recovers_failed() {
    let h = Harness::new(EvidenceMode::Concurrent, 120_000).await;

    // Frozen-input pins: the replay binds the exact retained bytes.
    let retained_patch = retained("STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.reserve.rs.txt");
    let retained_release = retained("STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.release.rs.txt");
    let retained_log = retained("STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.log.txt");
    assert_eq!(blake3_hex(&retained_patch), RETAINED_PATCH_BLAKE3);
    assert_eq!(blake3_hex(&retained_release), RETAINED_RELEASE_BLAKE3);
    assert_eq!(blake3_hex(&retained_log), RETAINED_LOG_BLAKE3);

    // The checked-in fixture still holds the exact broken bytes the
    // retained failure bound to: reserve broken, release unchanged.
    let broken_reserve =
        std::fs::read(h.ws.join("subject/src/reserve.rs")).expect("broken reserve read");
    let broken_release =
        std::fs::read(h.ws.join("subject/src/release.rs")).expect("broken release read");
    assert_eq!(blake3_hex(&broken_reserve), BROKEN_RESERVE_BLAKE3);
    assert_eq!(blake3_hex(&broken_release), RETAINED_RELEASE_BLAKE3);

    // Broken-first: the fixture fails its own acceptance before any patch.
    let (passed, _) = h.cargo_test(&[], true).await;
    assert!(!passed, "broken-first regression must fail");
    let before = h.protected_snapshot();

    let provider = Arc::new(ReplayProvider::patch(
        "subject/src/reserve.rs",
        blake3_hex(&broken_reserve),
        String::from_utf8(retained_patch).expect("retained patch is UTF-8"),
    ));
    let error = drive(
        DriveHost::Supervisor {
            handle: h.task.clone(),
            store: h.store.clone(),
        },
        h.context.clone(),
        provider.clone(),
        h.plan.clone(),
    )
    .await
    .expect_err("the retained partial repair must not complete");

    // Completion is gated by the acceptance contract, never by model
    // self-report: the partial repair is refused with a typed error.
    let reason = match &error {
        DriveError::Core(CoreError::VerificationBlocked(reason)) => reason.clone(),
        other => panic!("unexpected drive error: {other:?}"),
    };
    assert!(
        reason.contains("required acceptance failed"),
        "unexpected refusal reason: {reason}"
    );

    // One model call, no retry: a rejected patch is never retried.
    assert_eq!(provider.request_count(), 1);
    let attempts = h.model_attempts().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt, 1);
    assert!(attempts[0].error.is_none());
    assert_eq!(
        attempts[0].usage.provenance,
        UsageProvenance::ProviderReported
    );

    // Exactly the authorized file changed; every protected path is
    // byte-identical. The release file the patch never touched is the
    // retained failure's cause. The change stays within the
    // descriptor's authorized change paths.
    let observed = h.observed_changes();
    assert_eq!(observed, vec!["subject/src/reserve.rs".to_string()]);
    for changed in &observed {
        assert!(
            h.descriptor.change_paths.contains(changed),
            "unauthorized change: {changed}"
        );
    }
    assert_eq!(h.protected_snapshot(), before);

    // The rejection is the retained failure class: the same visible and
    // hidden tests fail as in the retained replay log.
    let (visible_passed, visible_out) = h
        .cargo_test(&["-p", "reservation-ledger", "--test", "visible"], false)
        .await;
    assert!(!visible_passed, "visible tests must fail in the replay");
    assert!(
        visible_out.contains("visible_reserve_is_atomic ... ok"),
        "reserve behavior must pass: {visible_out}"
    );
    assert!(
        visible_out.contains("visible_release_is_atomic ... FAILED"),
        "release behavior must fail: {visible_out}"
    );
    let (holdout_passed, holdout_out) = h
        .cargo_test(&["-p", "reservation-ledger", "--test", "holdout"], false)
        .await;
    assert!(!holdout_passed, "hidden tests must fail in the replay");
    for test in [
        "integer_limits_and_error_precedence_preserve_state",
        "smallest_sku_precedence_does_not_follow_batch_order",
        "sequences_match_independent_state_oracle",
    ] {
        assert!(
            holdout_out.contains(&format!("{test} ... FAILED")),
            "{test} must fail in the replay: {holdout_out}"
        );
    }

    // The failure is not durable until the production host marks it;
    // the host failure path then replays `recovered_failed`.
    let state = h.task.get_state().await.expect("task state");
    assert_ne!(state.status, TaskStatus::Completed);
    assert_ne!(state.status, TaskStatus::Failed);
    h.mark_failed_and_recover("drive failed: verification_failed".into())
        .await;
    h.close().await;
}

/// `reservation-ledger/serial/3`: a transport failure makes exactly one
/// attempt (only malformed output retries), records unknown usage, and
/// writes nothing before the durable `recovered_failed` replay.
#[tokio::test]
async fn transport_error_makes_one_attempt_writes_nothing_and_recovers_failed() {
    let h = Harness::new(EvidenceMode::Serial, 120_000).await;
    let before = h.protected_snapshot();
    let provider = Arc::new(ReplayProvider::error(ModelError::Transport(
        "connection reset by peer".into(),
    )));
    let error = drive(
        DriveHost::Supervisor {
            handle: h.task.clone(),
            store: h.store.clone(),
        },
        h.context.clone(),
        provider.clone(),
        h.plan.clone(),
    )
    .await
    .expect_err("a transport failure must fail the run");
    assert!(
        matches!(&error, DriveError::Provider(ModelError::Transport(_))),
        "unexpected drive error: {error:?}"
    );

    // No blind retry: a transport failure is not a malformed output.
    assert_eq!(provider.request_count(), 1);
    let attempts = h.model_attempts().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt, 1);
    assert_eq!(attempts[0].error.as_deref(), Some("transport"));
    // The M20 record carries no usage: unknown provenance, no counters.
    assert_eq!(attempts[0].usage.provenance, UsageProvenance::Unknown);
    assert_eq!(attempts[0].usage.input_tokens, None);
    assert_eq!(attempts[0].usage.output_tokens, None);

    // Zero writes: no mutation directory, no workspace change, no
    // protected-path drift.
    assert!(!h.plan.mutation_dir.exists());
    assert_eq!(h.observed_changes(), Vec::<String>::new());
    assert_eq!(h.protected_snapshot(), before);

    h.mark_failed_and_recover("drive failed: transport".into())
        .await;
    h.close().await;
}

/// `reservation-ledger/full/4`: the model-stage deadline fires the
/// production `timeout_at` path in concurrent evidence mode.
#[tokio::test]
async fn model_stage_deadline_times_out_in_full_mode_without_writes() {
    deadline_replay(EvidenceMode::Concurrent).await;
}

/// `reservation-ledger/serial/4`: the same deadline mechanism in
/// serial evidence mode.
#[tokio::test]
async fn model_stage_deadline_times_out_in_serial_mode_without_writes() {
    deadline_replay(EvidenceMode::Serial).await;
}

async fn deadline_replay(evidence_mode: EvidenceMode) {
    let h = Harness::new(evidence_mode, REPLAY_DEADLINE_MS).await;
    let before = h.protected_snapshot();
    let provider = Arc::new(ReplayProvider::hang());
    let error = drive(
        DriveHost::Supervisor {
            handle: h.task.clone(),
            store: h.store.clone(),
        },
        h.context.clone(),
        provider.clone(),
        h.plan.clone(),
    )
    .await
    .expect_err("a parked provider must time out");
    assert!(
        matches!(&error, DriveError::Provider(ModelError::Timeout { timeout_ms })
            if *timeout_ms == REPLAY_DEADLINE_MS),
        "unexpected drive error: {error:?}"
    );

    // One attempt; the deadline is not a retryable malformed output.
    assert_eq!(provider.request_count(), 1);
    let attempts = h.model_attempts().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt, 1);
    assert_eq!(attempts[0].error.as_deref(), Some("timeout"));
    // The driver synthesizes the timed-out invocation with unknown
    // usage; it never invents counters (the M20 timeout records).
    assert_eq!(attempts[0].usage.provenance, UsageProvenance::Unknown);
    assert_eq!(attempts[0].usage.input_tokens, None);
    assert_eq!(attempts[0].usage.output_tokens, None);

    // Zero writes, same as the retained timeout cells.
    assert!(!h.plan.mutation_dir.exists());
    assert_eq!(h.observed_changes(), Vec::<String>::new());
    assert_eq!(h.protected_snapshot(), before);

    h.mark_failed_and_recover("drive failed: timeout".into())
        .await;
    h.close().await;
}
