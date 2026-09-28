//! M13 `comp[process_output]`: process runner output-handling cost
//! (report-only, unix child).
//!
//! No §43 number names process output; this baseline feeds the M13
//! report's critical-path breakdown. Two arms over `/bin/sh`:
//!   - baseline: an empty child (`:`) — spawn, group ownership, wait,
//!     reap, redaction pass over zero bytes;
//!   - payload: a child emitting a fixed ~100 KB stdout stream — the same
//!     path plus capture, redaction and inline/spool splitting.
//!
//! The difference is the handling cost the report attributes to output
//! processing. Ignore-gated; the M13 ledger runs it with
//! `cargo test --release … -- --ignored --nocapture`.
#![cfg(unix)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tachyon_policy::{DefaultPosture, Policy};
use tachyon_tools::ToolsContext;
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_tools::process::{self, ProcessSpec};

const WARMUP: usize = 5;
const SAMPLES: usize = 20;
const PAYLOAD_SCRIPT: &str = "seq 1 20000";

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    (p50, p95)
}

#[cfg(target_os = "linux")]
fn linux_process_drain_budget(base: Duration) -> Duration {
    // The Linux group-drain proof scans visible numeric `/proc` entries while
    // the zombie leader reserves its process-group ID. Scale the end-to-end
    // allowance with that table size; 12 µs per entry covers the measured
    // ~16 ms scan at ~1.4k processes, with 6 ms for fixed work and host noise.
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

fn scratch() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "tachyon-m13-proc-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).expect("scratch dir");
    root
}

fn context(root: &std::path::Path) -> ToolsContext {
    let mut policy = Policy::new(DefaultPosture::Ask);
    policy.allow("process.spawn", "/bin/sh");
    ToolsContext::new(
        root.to_path_buf(),
        policy,
        ArtifactSpool::new(root.join("artifacts")),
    )
}

fn shell(script: &str) -> ProcessSpec {
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".to_owned(), script.to_owned()];
    spec.timeout = Duration::from_secs(3);
    spec
}

#[tokio::test]
#[ignore = "M13 perf component: release mode, run with --ignored"]
async fn comp_process_output_handling_latency() {
    let root = scratch();
    let ctx = context(&root);

    let baseline = shell(":");
    let payload = shell(PAYLOAD_SCRIPT);

    for _ in 0..WARMUP {
        let receipt = process::run(&ctx, &baseline).await.expect("baseline");
        assert_eq!(receipt.exit_code, Some(0));
        let receipt = process::run(&ctx, &payload).await.expect("payload");
        assert_eq!(receipt.exit_code, Some(0));
        assert!(!receipt.stdout.is_empty());
    }

    let mut baseline_latencies = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let receipt = process::run(&ctx, &baseline).await.expect("baseline");
        baseline_latencies.push(start.elapsed());
        assert_eq!(receipt.exit_code, Some(0));
        assert!(receipt.stdout.is_empty());
    }

    let mut payload_latencies = Vec::with_capacity(SAMPLES);
    let mut payload_bytes = 0_usize;
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let receipt = process::run(&ctx, &payload).await.expect("payload");
        payload_latencies.push(start.elapsed());
        assert_eq!(receipt.exit_code, Some(0));
        assert!(!receipt.stdout_truncated, "100KB fits the inline cap");
        payload_bytes = receipt.stdout.len();
    }
    assert!(payload_bytes > 50_000, "payload arm really emits bytes");

    let (base_p50, base_p95) = percentiles(baseline_latencies);
    let (payload_p50, payload_p95) = percentiles(payload_latencies);
    println!(
        "comp[process_output.baseline] n={SAMPLES} p50={base_p50:?} p95={base_p95:?} (empty child, fixed runner overhead)"
    );
    println!(
        "comp[process_output.payload] n={SAMPLES} p50={payload_p50:?} p95={payload_p95:?} (stdout_bytes={payload_bytes})"
    );

    // Linux includes process-group drain; its `/proc` scan allowance scales
    // with the visible process table. The poll schedule has a separate exact
    // unit assertion, so host process counts cannot hide a polling regression.
    #[cfg(target_os = "linux")]
    let budget = linux_process_drain_budget(Duration::from_millis(6));
    #[cfg(not(target_os = "linux"))]
    let budget = Duration::from_millis(6);
    assert!(
        base_p50 < budget,
        "process runner regression: empty-child p50={base_p50:?} >= {budget:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}
