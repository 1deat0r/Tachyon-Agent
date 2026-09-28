//! M13 `comp[verification]`: verification harness cost (report-only).
//!
//! No §43 number names verification; this baseline feeds the M13 report's
//! critical-path breakdown. Two halves over a trivial always-passing
//! command in a scratch workspace:
//!   - `comp[verification.plan]`: baseline snapshot + affected-first plan
//!     build (deterministic work, no process spawned);
//!   - `comp[verification.run]`: a full run — contract/graph validation,
//!     fresh private evidence, policy-bound command spawn, report build.
//!     Dominated by the child process itself by design; the report states
//!     both numbers so the harness share is visible.
//!
//! Ignore-gated; the M13 ledger runs it with
//! `cargo test --release … -- --ignored --nocapture`.
#![cfg(unix)]

mod common;
use common::Workspace;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tachyon_policy::{DefaultPosture, Policy};
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::TaskId;
use tachyon_verify::{
    AcceptanceContract, Clause, CommandCheck, VerificationPlan, VerificationRisk,
    WorkspaceSnapshot, run,
};
use tokio_util::sync::CancellationToken;

const PLAN_SAMPLES: usize = 20;
const RUN_SAMPLES: usize = 20;

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    (p50, p95)
}

#[cfg(target_os = "linux")]
fn linux_process_drain_budget(base: Duration) -> Duration {
    // Match the process runner's Linux budget: cleanup scans visible numeric
    // `/proc` entries while the zombie leader reserves its process-group ID.
    let process_count = std::fs::read_dir("/proc")
        .expect("read Linux process table for perf budget")
        .map(|entry| {
            entry
                .expect("read Linux process-table entry")
                .file_name()
                .to_string_lossy()
                .parse::<u32>()
                .is_ok()
        })
        .filter(|is_process| *is_process)
        .count();
    let scan_allowance =
        Duration::from_micros(u64::try_from(process_count).expect("process count fits u64") * 12);
    base + scan_allowance + Duration::from_millis(6)
}

fn trivial_command() -> CommandCheck {
    CommandCheck {
        program: "python3".into(),
        args: vec!["-c".into(), "pass".into()],
        cwd: ".".into(),
        env: BTreeMap::new(),
        timeout_ms: 2_000,
    }
}

fn granted() -> Policy {
    let mut policy = Policy::new(DefaultPosture::Deny);
    policy.allow("verify.command", "workspace/**");
    policy.allow("process.spawn", "python3");
    for capability in ["fs.read", "fs.metadata", "fs.list"] {
        policy.allow(capability, "workspace/**");
    }
    policy
}

fn context(ws: &Workspace, artifacts: &Workspace) -> Arc<ToolsContext> {
    Arc::new(ToolsContext::new(
        ws.path().to_path_buf(),
        granted(),
        ArtifactSpool::new(artifacts.path().to_path_buf()),
    ))
}

fn plan_for(ws: &Workspace) -> VerificationPlan {
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    VerificationPlan::build(
        TaskId::generate(),
        7,
        &AcceptanceContract {
            clauses: vec![Clause::CommandPasses {
                command: trivial_command(),
            }],
        },
        &baseline,
        &[],
        VerificationRisk::Affected,
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "M13 perf component: release mode, run with --ignored"]
async fn comp_verification_plan_and_run_latency() {
    let ws = Workspace::new();
    ws.write("src/lib.rs", "pub fn trivial() {}\n");
    let artifacts = Workspace::new();

    // Warmup: first capture/plan/run touches cold page cache.
    let warm = plan_for(&ws);
    let warm_report = run(warm, context(&ws, &artifacts), CancellationToken::new())
        .await
        .expect("warmup run");
    assert!(warm_report.passed(), "trivial command passes");

    let mut plan_latencies = Vec::with_capacity(PLAN_SAMPLES);
    for _ in 0..PLAN_SAMPLES {
        let start = Instant::now();
        let plan = plan_for(&ws);
        plan_latencies.push(start.elapsed());
        assert!(!plan.graph().nodes.is_empty(), "plan lowers to IR nodes");
    }

    let plan = plan_for(&ws);
    let mut run_latencies = Vec::with_capacity(RUN_SAMPLES);
    for _ in 0..RUN_SAMPLES {
        let start = Instant::now();
        let report = run(
            plan.clone(),
            context(&ws, &artifacts),
            CancellationToken::new(),
        )
        .await
        .expect("verification run");
        run_latencies.push(start.elapsed());
        assert!(report.passed(), "trivial command passes every run");
    }

    let (plan_p50, plan_p95) = percentiles(plan_latencies);
    let (run_p50, run_p95) = percentiles(run_latencies);
    println!(
        "comp[verification.plan] n={PLAN_SAMPLES} p50={plan_p50:?} p95={plan_p95:?} (snapshot + affected-first plan)"
    );
    println!(
        "comp[verification.run] n={RUN_SAMPLES} p50={run_p50:?} p95={run_p95:?} (full run incl. child process)"
    );

    // Linux includes the process runner's awaited process-group drain. Its
    // `/proc` scan allowance scales with visible process count; exact unit
    // assertions pin both the process and scheduler fast-poll strategies.
    #[cfg(target_os = "linux")]
    let budget = linux_process_drain_budget(Duration::from_millis(18));
    #[cfg(not(target_os = "linux"))]
    let budget = Duration::from_millis(18);
    assert!(
        run_p50 < budget,
        "verification regression: run p50={run_p50:?} >= {budget:?}"
    );
}
