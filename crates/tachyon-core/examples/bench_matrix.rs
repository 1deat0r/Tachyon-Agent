//! M14 benchmark matrix host: descriptor-driven thin runtime host.
//!
//! This example contains no agent decision logic. It loads
//! `fixtures/<id>/bench.json`, copies the fixture to a fresh scratch
//! workspace, proves the broken-first regression fails, then runs the ONE
//! shared production driver (`tachyon_core::driver::drive`): evidence ->
//! model -> patch -> verification. Scripted responses prove runtime
//! integration and verification, never model reasoning quality.
//!
//! Modes (spec §44): `full` (concurrent evidence, supervisor path),
//! `no-speculation` and `no-judgment` (identical to `full` — the MVP
//! driver has no speculation or judgment stage, so both are measured
//! aliases and report `coincides_with`), `serial` (sequential evidence,
//! supervisor path), `reference` (same shared steps through the driver
//! with no task and no journal — the in-tree serial control group; this
//! host runs the verification tail itself). `fixture-check` proves the
//! fixture properties without the driver: broken-first fails, applying
//! `fixtures/solutions/<id>/` passes, protected paths stay byte-identical
//! and exactly `change_paths` changed.
//!
//! JSON report goes to stdout (one line per run); diagnostics to stderr.

#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tachyon_core::create_task;
use tachyon_core::driver::{DriveHost, EvidenceMode, RunPlan, TaskModelContext, drive};
use tachyon_core::runtime::{EvidenceRequest, RuntimeBounds, evidence_concurrency, max_overlap};
use tachyon_models::fake::{FakeModelProvider, FakeResponse};
use tachyon_models::{
    ModelCapabilities, ModelError, ModelEventSink, ModelProvider, ModelRequest, ModelResult,
    ProviderEstimate, UsageProvenance,
};
use tachyon_mutation::blake3_hex;
use tachyon_policy::Policy;
use tachyon_store::StoreWriter;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::{ProviderId, SessionId, WorkspaceId};
use tachyon_verify::{AcceptanceContract, VerificationRisk};

/// The five spec §44 modes this host implements.
const MODES: &[&str] = &[
    "full",
    "no-speculation",
    "no-judgment",
    "serial",
    "reference",
];

/// One fixture's benchmark descriptor (`fixtures/<id>/bench.json`).
#[derive(Debug, Deserialize)]
struct Descriptor {
    id: String,
    class: String,
    objective: String,
    evidence_paths: Vec<String>,
    protected_paths: Vec<String>,
    change_paths: Vec<String>,
    contract: AcceptanceContract,
    requested_checks: Vec<String>,
    available_checks: Vec<String>,
    check_note: String,
}

/// Provider decorator that times every invoke so the report carries real
/// model-call durations instead of an assumption. Call counts come from
/// the inner fake (`request_count`); this wrapper only accumulates time.
struct TimedProvider {
    inner: Arc<FakeModelProvider>,
    total: Mutex<Duration>,
}

impl TimedProvider {
    fn new(inner: Arc<FakeModelProvider>) -> Self {
        Self {
            inner,
            total: Mutex::new(Duration::ZERO),
        }
    }

    fn stats(&self) -> Duration {
        *self
            .total
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl ModelProvider for TimedProvider {
    fn id(&self) -> ProviderId {
        self.inner.id()
    }

    fn capabilities(&self) -> ModelCapabilities {
        self.inner.capabilities()
    }

    fn estimate(&self, request: &ModelRequest) -> ProviderEstimate {
        self.inner.estimate(request)
    }

    async fn invoke(
        &self,
        request: ModelRequest,
        sink: ModelEventSink,
    ) -> Result<ModelResult, ModelError> {
        let started = Instant::now();
        let result = self.inner.invoke(request, sink).await;
        *self
            .total
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += started.elapsed();
        result
    }
}

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn load_descriptor(id: &str) -> Result<Descriptor, String> {
    check_in_bounds(&PathBuf::from("fixtures"), id)?;
    let path = fixtures_root().join(id).join("bench.json");
    let raw = std::fs::read_to_string(&path)
        .map_err(|error| format!("descriptor {}: {error}", path.display()))?;
    let descriptor: Descriptor =
        serde_json::from_str(&raw).map_err(|error| format!("descriptor parse: {error}"))?;
    if descriptor.id != id {
        return Err(format!(
            "descriptor id {} does not match fixture {id}",
            descriptor.id
        ));
    }
    Ok(descriptor)
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

/// A fixture or descriptor path is trusted only when it stays inside its
/// root: this refuses absolute paths and `..` escapes before any fs op.
/// (Bench descriptors are checked-in reviewed files and the operator runs
/// the host locally, but self-attack by typo'd paths is still a bug.)
fn check_in_bounds(root: &Path, rel: &str) -> Result<(), String> {
    let rel_path = Path::new(rel);
    if rel.is_empty()
        || rel_path.is_absolute()
        || rel_path
            .components()
            .any(|component| component == std::path::Component::ParentDir)
    {
        return Err(format!("path escapes its root: {rel}"));
    }
    let _ = root;
    Ok(())
}

fn snapshot_paths(ws: &Path, paths: &[String]) -> BTreeMap<String, Vec<u8>> {
    paths
        .iter()
        .map(|rel| (rel.clone(), std::fs::read(ws.join(rel)).unwrap_or_default()))
        .collect()
}

/// Every file in the scratch workspace (target/ excluded) whose bytes
/// differ from the checked-in fixture: the observed change set.
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

/// Outcome of one `cargo test` invocation: pass, test-failure, or a spawn
/// failure. Spawn failures (missing toolchain, unstartable child) must
/// never masquerade as a broken fixture.
#[derive(Debug, PartialEq, Eq)]
enum CargoOutcome {
    Passed,
    TestsFailed,
    CouldNotStart(String),
}

async fn cargo_test(ws: &Path, target_dir: &Path) -> CargoOutcome {
    let out = tokio::process::Command::new("cargo")
        .args(["test", "--offline", "--locked"])
        .current_dir(ws)
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_NET_OFFLINE", "true")
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => CargoOutcome::Passed,
        Ok(_) => CargoOutcome::TestsFailed,
        Err(error) => {
            eprintln!("cargo test could not start: {error}");
            CargoOutcome::CouldNotStart(error.to_string())
        }
    }
}

/// `fixture-check`: broken-first fails, the shipped solution repairs it,
/// protected paths stay byte-identical, exactly `change_paths` changed.
async fn fixture_check(descriptor: &Descriptor) -> Result<String, String> {
    let fixture = fixtures_root().join(&descriptor.id);
    let scratch = std::env::temp_dir().join(format!(
        "tachyon-m14-check-{}-{}",
        descriptor.id,
        uuid::Uuid::now_v7()
    ));
    let ws = scratch.join("ws");
    copy_dir(&fixture, &ws).map_err(|error| format!("fixture copy: {error}"))?;
    let before = snapshot_paths(&ws, &descriptor.protected_paths);

    match cargo_test(&ws, &scratch.join("target")).await {
        CargoOutcome::TestsFailed => {}
        CargoOutcome::Passed => {
            return Err(format!(
                "{}: checked-in fixture unexpectedly passes; broken-first is vacuous",
                descriptor.id
            ));
        }
        CargoOutcome::CouldNotStart(error) => {
            return Err(format!(
                "{0}: broken-first cargo could not start: {error}",
                descriptor.id
            ));
        }
    }

    for rel in &descriptor.change_paths {
        check_in_bounds(&ws, rel)?;
        let solution = fixtures_root()
            .join("solutions")
            .join(&descriptor.id)
            .join(rel);
        let fixed = std::fs::read(&solution)
            .map_err(|error| format!("solution {}: {error}", solution.display()))?;
        let broken_bytes =
            std::fs::read(ws.join(rel)).map_err(|error| format!("current {rel}: {error}"))?;
        if fixed == broken_bytes {
            return Err(format!(
                "{}: solution for {rel} is byte-identical to the broken fixture",
                descriptor.id
            ));
        }
        std::fs::write(ws.join(rel), fixed).map_err(|error| format!("apply: {error}"))?;
    }

    match cargo_test(&ws, &scratch.join("target")).await {
        CargoOutcome::Passed => {}
        CargoOutcome::TestsFailed => {
            return Err(format!(
                "{}: fixture does not pass after applying its shipped solution",
                descriptor.id
            ));
        }
        CargoOutcome::CouldNotStart(error) => {
            return Err(format!(
                "{0}: post-fix cargo could not start: {error}",
                descriptor.id
            ));
        }
    }

    let after = snapshot_paths(&ws, &descriptor.protected_paths);
    if before != after {
        return Err(format!(
            "{}: protected paths changed during self-check",
            descriptor.id
        ));
    }
    let observed = observed_changes(&fixture, &ws);
    if observed != descriptor.change_paths {
        return Err(format!(
            "{}: observed changes {observed:?} != expected {:?}",
            descriptor.id, descriptor.change_paths
        ));
    }
    let _ignored = std::fs::remove_dir_all(&scratch);
    Ok(format!("fixture self-check ok: {}", descriptor.id))
}

/// Fresh scratch workspace plus the broken-first proof and a snapshot of
/// every protected path. Returned paths live under one fresh `scratch`
/// dir so the caller can drain it after the run.
struct PreparedScratch {
    scratch: PathBuf,
    ws: PathBuf,
    target: PathBuf,
    before: BTreeMap<String, Vec<u8>>,
}

fn prepare_scratch(descriptor: &Descriptor) -> Result<PreparedScratch, String> {
    let fixture = fixtures_root().join(&descriptor.id);
    let scratch = std::env::temp_dir().join(format!(
        "tachyon-m14-{}-{}",
        descriptor.id,
        uuid::Uuid::now_v7()
    ));
    let ws = scratch.join("ws");
    copy_dir(&fixture, &ws).map_err(|error| format!("fixture copy: {error}"))?;
    let before = snapshot_paths(&ws, &descriptor.protected_paths);
    let target = scratch.join("target");
    Ok(PreparedScratch {
        scratch,
        ws,
        target,
        before,
    })
}

/// Scripted proposal: one `mutation.patch` operation per `change_path` with
/// full-file solution content bound to the broken bytes' hash.
fn build_script(descriptor: &Descriptor, ws: &Path) -> Result<serde_json::Value, String> {
    let mut operations = Vec::new();
    for rel in &descriptor.change_paths {
        check_in_bounds(ws, rel)?;
        let current =
            std::fs::read(ws.join(rel)).map_err(|error| format!("target read {rel}: {error}"))?;
        check_in_bounds(&fixtures_root().join("solutions").join(&descriptor.id), rel)?;
        let solution = fixtures_root()
            .join("solutions")
            .join(&descriptor.id)
            .join(rel);
        let fixed =
            std::fs::read(&solution).map_err(|error| format!("solution read {rel}: {error}"))?;
        if fixed == current {
            return Err(format!("solution for {rel} matches broken fixture"));
        }
        operations.push(serde_json::json!({
            "capability": "mutation.patch",
            "reason": "Apply the tested fixture correction",
            "args": {
                "path": rel,
                "base_hash": blake3_hex(&current),
                "new_content": String::from_utf8_lossy(&fixed),
            }
        }));
    }
    Ok(serde_json::json!({
        "decision": "propose_execution",
        "operations": operations,
    }))
}

async fn run_sample(
    descriptor: &Descriptor,
    mode: &str,
    sample: Option<u64>,
) -> Result<serde_json::Value, String> {
    let harness_start = Instant::now();
    let fixture = fixtures_root().join(&descriptor.id);
    let prepared = prepare_scratch(descriptor)?;
    let scratch = prepared.scratch;
    let ws = prepared.ws;
    let target = prepared.target;
    let before = prepared.before;
    eprintln!(
        "run {}/{} sample {:?} scratch {}",
        descriptor.id,
        mode,
        sample,
        ws.display()
    );

    // Broken regression must fail first (fixture state, not setup): a
    // spawn failure is an error, never a pass.
    match cargo_test(&ws, &target).await {
        CargoOutcome::TestsFailed => {}
        CargoOutcome::Passed => {
            return Err(format!(
                "{}: broken-first cargo test did not fail",
                descriptor.id
            ));
        }
        CargoOutcome::CouldNotStart(error) => {
            return Err(format!(
                "{0}: broken-first cargo could not start: {error}",
                descriptor.id
            ));
        }
    }

    let script = build_script(descriptor, &ws)?;
    let fake = Arc::new(FakeModelProvider::new(ProviderId(format!(
        "bench-script-{}",
        descriptor.id
    ))));
    fake.push_response(FakeResponse {
        text: script.to_string(),
        decision: serde_json::from_value(script.clone()).expect("typed proposal fixture"),
        input_tokens: 0,
        output_tokens: 0,
    });
    let provider = Arc::new(TimedProvider::new(fake.clone()));

    let requests: Vec<EvidenceRequest> = descriptor
        .evidence_paths
        .iter()
        .map(|path| EvidenceRequest {
            capability: "fs.read".into(),
            path: path.clone(),
        })
        .collect();

    // Supervisor path creates its task BEFORE the run so every stage is
    // journaled; reference keeps no task and no journal.
    let store = if mode == "reference" {
        None
    } else {
        let state_dir = scratch.join("state");
        std::fs::create_dir_all(&state_dir).map_err(|error| format!("state dir: {error}"))?;
        let store = Arc::new(
            StoreWriter::open(&state_dir)
                .await
                .map_err(|error| format!("store: {error}"))?,
        );
        let session = SessionId::generate();
        store
            .create_session(&session.to_string())
            .await
            .map_err(|error| format!("session: {error}"))?;
        let task = create_task(
            session,
            WorkspaceId::generate(),
            descriptor.objective.clone(),
            store.clone(),
        )
        .await
        .map_err(|error| format!("create_task: {error}"))?;
        Some((task, store))
    };

    let mut policy = Policy::trusted_workspace();
    policy.allow("fs.read", "workspace/**");
    policy.allow("mutation.patch", "workspace/**");
    policy.allow("fs.delete", "workspace/**");
    policy.allow("verify.command", "workspace/**");
    let context = Arc::new(ToolsContext::new(
        ws.clone(),
        policy,
        ArtifactSpool::new(scratch.join("artifacts")),
    ));
    let mutation_dir = scratch.join("mutation-state");
    std::fs::create_dir_all(&mutation_dir).map_err(|error| format!("mutation dir: {error}"))?;

    let mut store_holder: Option<Arc<StoreWriter>> = None;
    let host = match store {
        Some((task, store)) => {
            store_holder = Some(store.clone());
            DriveHost::Supervisor {
                handle: task,
                store,
            }
        }
        None => DriveHost::Reference,
    };

    // Reference is the serial control group (M10 semantics); the two
    // alias modes measure the full path because no speculation or
    // judgment stage exists to disable. WARNING to future stages: the
    // judgment_calls/speculation_* report fields below are measured as
    // zero because there is no such stage to call — adding a speculation
    // or judgment stage MUST update these modes and fields, which will
    // not catch the change on their own.
    let evidence_mode = if matches!(mode, "full" | "no-speculation" | "no-judgment") {
        EvidenceMode::Concurrent
    } else {
        EvidenceMode::Serial
    };

    let origin = Instant::now();
    let plan = RunPlan {
        origin,
        evidence_mode,
        evidence: requests.clone(),
        contract: descriptor.contract.clone(),
        risk: VerificationRisk::Affected,
        mutation_dir,
        batch_id: "bench-batch-1".into(),
        model: "scripted-replay-1".into(),
        task_context: TaskModelContext {
            revision: None,
            objective: descriptor.objective.clone(),
            history: Vec::new(),
            constraints: Vec::new(),
        },
        requested_checks: descriptor.requested_checks.clone(),
        available_checks: descriptor.available_checks.clone(),
        bounds: RuntimeBounds::default(),
        cancel: tokio_util::sync::CancellationToken::new(),
    };
    let drive_start = Instant::now();
    let drive_result = drive(host, context, provider.clone(), plan).await;
    // Always close the store, including on drive failure, before
    // propagating the error: a leaked open writer is a lifecycle break.
    // (`close` only shuts the pool down; errors are logged, not fatal.)
    if let Some(store) = store_holder {
        store.close().await;
    }
    let outcome = drive_result.map_err(|error| format!("drive: {error}"))?;
    let drive_ms = as_millis_u64(drive_start.elapsed());

    // Reference keeps its declared control-loop verification tail.
    // A spawn failure is an error here too, never a pass.
    let (label, task_id, revision, recovery, final_verification_ms, verify_subprocesses) =
        if mode == "reference" {
            let tail_start = Instant::now();
            let passed = match cargo_test(&ws, &target).await {
                CargoOutcome::Passed => true,
                CargoOutcome::TestsFailed => false,
                CargoOutcome::CouldNotStart(error) => {
                    return Err(format!("reference tail cargo could not start: {error}"));
                }
            };
            let tail_ms = as_millis_u64(tail_start.elapsed());
            (
                if passed {
                    "completed_reference"
                } else {
                    "verification_failed"
                }
                .to_string(),
                None,
                None,
                None,
                Some(as_millis_u64(origin.elapsed())),
                Some(tail_ms),
            )
        } else {
            (
                outcome
                    .outcome
                    .clone()
                    .ok_or_else(|| "supervisor run returned no outcome".to_string())?,
                outcome.task_id.clone(),
                outcome.revision,
                outcome.recovery.clone(),
                outcome.final_verification_ms,
                None,
            )
        };

    let after = snapshot_paths(&ws, &descriptor.protected_paths);
    let observed = observed_changes(&fixture, &ws);
    let model_ms = provider.stats();
    let model_calls = fake.request_count() as u64;
    let usage_provenance = match outcome.usage.provenance {
        UsageProvenance::ProviderReported => "provider_reported",
        UsageProvenance::Scripted => "scripted",
        UsageProvenance::Unknown => "unknown",
    };

    let timings = outcome.node_timings.clone();
    let max_concurrency = if evidence_mode == EvidenceMode::Serial {
        usize::from(!timings.is_empty())
    } else {
        max_overlap(&outcome.intervals_us)
    };
    let helper_check = evidence_concurrency(&outcome.node_timings);
    eprintln!("max_overlap={max_concurrency} evidence_concurrency={helper_check}");

    let verified = matches!(label.as_str(), "completed" | "completed_reference");
    let completion_ms = final_verification_ms.unwrap_or(drive_ms);
    let (coincides_with, mode_note) = match mode {
        "no-speculation" | "no-judgment" => (
            Some("full"),
            "MVP driver has no speculation or judgment stage; measured as an alias of full",
        ),
        _ => (None, ""),
    };

    let report = serde_json::json!({
        "fixture": descriptor.id,
        "class": descriptor.class,
        "mode": mode,
        "sample": sample,
        "provider": format!("bench-script-{}", descriptor.id),
        "model": "scripted-replay-1",
        "provider_note": "pinned scripted FakeModelProvider (docs/11 #11): identical model across every mode by construction",
        "coincides_with": coincides_with,
        "mode_note": mode_note,
        "outcome": label,
        "verified": verified,
        "broken_first_failed": true,
        "completion_ms": completion_ms,
        "task_wall_ms": drive_ms,
        "harness_ms": as_millis_u64(harness_start.elapsed()),
        "first_evidence_ms": outcome.first_evidence_ms,
        "first_edit_ms": outcome.first_edit_ms,
        "final_verification_ms": final_verification_ms,
        "verify_host_tail_ms": verify_subprocesses,
        "model_calls": model_calls,
        "model_ms": as_millis_f64(model_ms),
        "judgment_calls": 0u64,
        "jev_calls": 0u64,
        "judgment_note": "no judgment stage exists in the MVP driver; judgment would surface as a provider call here",
        "tool_calls": (descriptor.evidence_paths.len() + 2) as u64,
        "tool_calls_note": "evidence reads + mutation prepare/commit; verification subprocess counted separately",
        "verify_subprocesses": 1u64,
        "input_tokens": outcome.usage.input_tokens,
        "output_tokens": outcome.usage.output_tokens,
        "usage_provenance": usage_provenance,
        "retries": 0u64,
        "provider_failures": 0u64,
        "verification_failures": u64::from(!verified),
        "user_interventions": 0u64,
        "user_interventions_note": "trusted-workspace policy auto-allows every fixture operation; a parked approval would surface as a driver error",
        "speculation_started": 0u64,
        "speculation_used": 0u64,
        "speculation_discarded": 0u64,
        "speculation_note": "speculation policy is Forbidden in the MVP runtime (spec §14: no speculative mutation)",
        "evidence_concurrency": max_concurrency,
        "evidence_nodes": outcome.evidence_graph_nodes,
        "changed_paths": outcome.changed_paths,
        "observed_changes": observed,
        "expected_changes": descriptor.change_paths,
        "observed_matches_expected": observed == descriptor.change_paths,
        "selected_checks": outcome.selected_checks,
        "check_broadening": outcome.check_broadening,
        "check_note": descriptor.check_note,
        "protected_unchanged": before == after,
        "task_id": task_id,
        "revision": revision,
        "recovery": recovery,
        "corpus": fixture.display().to_string(),
    });

    // Drain this sample's scratch (fixture copies plus fixture-local cargo
    // artifacts) so the 150-sample matrix cannot exhaust the disk; keep it
    // on failure for debugging.
    if verified {
        let _ignored = std::fs::remove_dir_all(&scratch);
    }

    Ok(report)
}

fn as_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn as_millis_f64(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let id = args.next().unwrap_or_default();
    let mode = args.next().unwrap_or_default();
    let sample = args.next().and_then(|raw| raw.parse::<u64>().ok());
    if id.is_empty() || mode.is_empty() {
        eprintln!("usage: bench_matrix <fixture-id> <mode|fixture-check> [sample]");
        std::process::exit(2);
    }
    let descriptor = match load_descriptor(&id) {
        Ok(descriptor) => descriptor,
        Err(error) => {
            println!("{}", serde_json::json!({"fixture": id, "error": error}));
            std::process::exit(1);
        }
    };
    if mode == "fixture-check" {
        match fixture_check(&descriptor).await {
            Ok(line) => println!("{line}"),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
        return;
    }
    if !MODES.contains(&mode.as_str()) {
        println!(
            "{}",
            serde_json::json!({"fixture": id, "mode": mode, "outcome": "unimplemented",
                "note": format!("modes: {MODES:?} + fixture-check")})
        );
        std::process::exit(2);
    }
    match run_sample(&descriptor, &mode, sample).await {
        Ok(report) => println!("{report}"),
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({"fixture": id, "mode": mode, "outcome": "error", "error": error})
            );
            std::process::exit(1);
        }
    }
}
