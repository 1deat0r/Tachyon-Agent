//! Issue #57 slice (b), ticket 02: `StartRun` retry-safety contract
//! (ADR-0005 "do not blindly retry `StartRun`" — the gateway half).
//!
//! A client that sends `StartRun` and loses the response must always get
//! a typed answer and must never cause a second driver spawn for the
//! same task:
//!
//!   * (a) retry while the run is active → typed `run_already_active`,
//!     exactly one provider invocation (one driver);
//!   * (b) restart mid-run → retry after restart lands the existing
//!     recovery/re-entry path (or a typed refusal) with a single active
//!     driver — never two concurrent drivers across the restart;
//!   * (c) lost-response before spawn (prep refused, admission rolled
//!     back) → plain retry admits normally; no stale admission leak.
//!
//! Sequence note for (b): `RunningGateway::shutdown` drains connections
//! and closes the store but does not abort the detached driver task — an
//! in-process zombie driver would hold task ownership (and the workspace
//! lease) forever, which a real process kill never does. The test models
//! process death by shutting down while the driver is parked mid-run,
//! then letting that run die across the shutdown boundary (provider
//! release) before restarting on the same data directory — the durable
//! mid-run shape (stage records + workspace pin) survives, the old
//! driver does not, exactly as after a kill.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tachyon_gateway::GatewayRuntime;
use tachyon_gateway::start_with;
use tachyon_models::{
    ModelCapabilities, ModelError, ModelEventSink, ModelProvider, ModelRequest, ModelResult,
    ProviderEstimate,
};
use tachyon_tools::workspace::WorkspaceLease;
use tachyon_types::ProviderId;
use tokio::sync::Notify;

mod common;
use common::{armed_runtime, code_of, err, new_task, ok, send, test_dir};

/// Provider that counts concurrent and total invocations and PARKS
/// inside `invoke` until released — one driver in flight stays
/// observable (`active` gauge), a duplicate spawn would show up as a
/// second concurrent invocation, and releasing with an error lets the
/// test end a run's driver deterministically (run-path prior art:
/// `BlockingProvider` in `run_path.rs`).
struct ParkedProvider {
    active: Arc<AtomicU64>,
    max_active: Arc<AtomicU64>,
    entered: Arc<AtomicU64>,
    release: Arc<Notify>,
}

static PARKED_CAPABILITIES: OnceLock<ModelCapabilities> = OnceLock::new();

#[async_trait::async_trait]
impl ModelProvider for ParkedProvider {
    fn id(&self) -> ProviderId {
        ProviderId("bench-parked".into())
    }

    fn capabilities(&self) -> ModelCapabilities {
        PARKED_CAPABILITIES
            .get_or_init(|| ModelCapabilities {
                context_window_tokens: 128_000,
                ..ModelCapabilities::default()
            })
            .clone()
    }

    fn estimate(&self, request: &ModelRequest) -> ProviderEstimate {
        ProviderEstimate {
            latency_ms: 1.0,
            input_tokens: request.estimated_input_tokens(),
        }
    }

    async fn invoke(
        &self,
        _request: ModelRequest,
        _sink: ModelEventSink,
    ) -> Result<ModelResult, ModelError> {
        let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(now, Ordering::SeqCst);
        self.entered.fetch_add(1, Ordering::SeqCst);
        tokio::select! {
            biased;
            () = self.release.notified() => {}
            () = std::future::pending::<()>() => {}
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        Err(ModelError::ProviderUnavailable(
            "released by startrun_retry test".to_owned(),
        ))
    }
}

struct Harness {
    active: Arc<AtomicU64>,
    max_active: Arc<AtomicU64>,
    entered: Arc<AtomicU64>,
    release: Arc<Notify>,
    runtime: GatewayRuntime,
}

fn harness() -> Harness {
    let active = Arc::new(AtomicU64::new(0));
    let max_active = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(AtomicU64::new(0));
    let release = Arc::new(Notify::new());
    let provider = Arc::new(ParkedProvider {
        active: active.clone(),
        max_active: max_active.clone(),
        entered: entered.clone(),
        release: release.clone(),
    });
    Harness {
        active,
        max_active,
        entered,
        release,
        runtime: armed_runtime(provider),
    }
}

async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// A minimal Cargo workspace so default acceptance resolves and the run
/// actually spawns into the parked provider.
fn cargo_ws(dir: &Path) -> PathBuf {
    let ws = dir.join("cargo-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("Cargo.toml"), "[package]\nname = \"w\"\n").unwrap();
    std::fs::canonicalize(&ws).unwrap()
}

fn start_run(task_id: &str, ws: &Path) -> tachyon_protocol::Command {
    tachyon_protocol::Command::StartRun {
        task_id: task_id.parse().unwrap(),
        workspace_root: ws.display().to_string(),
        acceptance: None,
    }
}

/// Ticket AC 1 — lost-response retry while the run is active: the
/// duplicate `StartRun` (the retry a client sends after losing the
/// first response) is the typed `run_already_active` refusal, and the
/// provider invocation count proves exactly ONE driver ever spawned —
/// no second driver, no second provider call.
#[tokio::test]
async fn lost_response_retry_while_active_is_typed_and_spawns_one_driver() {
    let dir = test_dir();
    let h = harness();
    let gateway = start_with(&dir, h.runtime.clone()).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = cargo_ws(&dir);

    // First StartRun (its response is "lost" by the client).
    let first = ok(&socket, start_run(&task, &ws)).await;
    assert_eq!(
        first["workspace_root"],
        ws.display().to_string(),
        "ack carries the canonical root: {first}"
    );
    wait_until("the driver reaches the model stage", || {
        h.active.load(Ordering::SeqCst) == 1
    })
    .await;

    // The retry: typed refusal, not a second spawn.
    let got = err(&socket, start_run(&task, &ws)).await;
    assert_eq!(code_of(&got), "run_already_active", "{got}");

    // A second retry (client double-send) stays typed as well.
    let again = err(&socket, start_run(&task, &ws)).await;
    assert_eq!(code_of(&again), "run_already_active", "{again}");

    // Single-driver invariant: exactly one provider invocation total,
    // never two concurrent, regardless of the retries.
    assert_eq!(
        h.entered.load(Ordering::SeqCst),
        1,
        "retries must not spawn a second driver"
    );
    assert_eq!(h.max_active.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.active.load(Ordering::SeqCst),
        1,
        "the single run is in flight"
    );

    gateway.shutdown().await;
}

/// Ticket AC 1 (b) — restart mid-run: `StartRun`, shut the gateway down
/// while the run is parked mid-run, restart on the same data directory,
/// retry `StartRun` on the same task. The retry must take the existing
/// recovery/re-entry path (or a typed refusal) and the invariant that
/// holds across the restart is single-active-run: at no point do two
/// drivers run concurrently, and while the post-restart run is in
/// flight a further duplicate is still the typed `run_already_active`.
#[tokio::test]
async fn restart_mid_run_retry_startrun_never_double_spawns() {
    let dir = test_dir();
    let h = harness();
    let gateway = start_with(&dir, h.runtime.clone()).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = cargo_ws(&dir);

    // Phase 1: a real StartRun; wait until the driver is mid-run
    // (parked inside the provider) so shutdown lands DURING the run.
    ok(&socket, start_run(&task, &ws)).await;
    wait_until("the phase-1 driver reaches the model stage", || {
        h.active.load(Ordering::SeqCst) == 1
    })
    .await;

    // Shut down mid-run (recovery.rs restart pattern).
    gateway.shutdown().await;

    // Model process death: the parked run dies across the shutdown
    // boundary (no journal writes on this path — the drive fails before
    // any further proposal). Waiting for the lease to come back proves
    // the old driver's future (with it its supervisor handle and task
    // ownership) has been dropped, which is what lets the restarted
    // gateway recover the task instead of hitting `TaskAlreadyOwned`.
    h.release.notify_one();
    wait_until("the old driver fully exits", || {
        h.active.load(Ordering::SeqCst) == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if WorkspaceLease::try_acquire(&ws).await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the old run never released the workspace lease");

    // Restart on the same data directory; the mid-run durable shape
    // (pin + stage records) must be recovered, task non-terminal.
    let gateway = start_with(&dir, h.runtime.clone()).await.unwrap();
    let socket = gateway.address().to_owned();
    let recovered = ok(
        &socket,
        tachyon_protocol::Command::GetTask {
            task_id: task.parse().unwrap(),
        },
    )
    .await;
    assert!(
        !["Completed", "Failed", "Cancelled"]
            .contains(&recovered["task"]["status"].as_str().unwrap()),
        "restart must not terminate the task: {recovered}"
    );
    assert_eq!(
        recovered["task"]["workspace_root"].as_str(),
        Some(ws.to_str().unwrap()),
        "the durable pin survived the restart"
    );

    // The client retries StartRun (its original response was lost).
    // Contract: typed refusal OR the existing recovery/re-entry path —
    // never a second concurrent driver.
    let retry = send(&socket, start_run(&task, &ws)).await;
    let expected_spawns = match retry.0 {
        200 => {
            // Recovery path admitted exactly one new driver.
            wait_until("the re-entered driver reaches the model stage", || {
                h.active.load(Ordering::SeqCst) == 1
            })
            .await;
            // While it is in flight, a duplicate stays typed.
            let dup = err(&socket, start_run(&task, &ws)).await;
            assert_eq!(code_of(&dup), "run_already_active", "{dup}");
            2
        }
        400 => {
            // A typed refusal is also an allowed answer; pin the code.
            let code = code_of(&retry.2);
            assert_eq!(
                code, "run_already_active",
                "typed refusal after restart must be the in-flight answer, got {retry:?}"
            );
            1
        }
        other => panic!(
            "StartRun retry after restart must be typed or recovery, got {retry:?} ({other})"
        ),
    };

    // Single-active-run invariant across the whole timeline: one
    // invocation per gateway era, never two concurrent drivers.
    assert_eq!(
        h.max_active.load(Ordering::SeqCst),
        1,
        "a second driver ran concurrently with another at some point"
    );
    assert_eq!(
        h.entered.load(Ordering::SeqCst),
        expected_spawns,
        "one spawn per admitted run, never a second concurrent driver"
    );

    gateway.shutdown().await;
}

/// Ticket AC 1 (c) — lost-response before spawn: a prep refusal (bad
/// workspace root) rolls the admission entry back, so the very next
/// retry is judged on its own merits — first the same refusal again
/// (no stale `run_already_active`), then a corrected `StartRun` that
/// admits normally, with exactly one driver spawned.
#[tokio::test]
async fn prep_refusal_rolls_back_admission_so_corrected_retry_admits() {
    let dir = test_dir();
    let h = harness();
    let gateway = start_with(&dir, h.runtime.clone()).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = cargo_ws(&dir);
    let missing = dir.join("no-such-ws");

    // Prep refuses BEFORE spawn (workspace does not exist).
    let refused = err(
        &socket,
        tachyon_protocol::Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: missing.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&refused), "workspace_not_found", "{refused}");

    // Lost-response retry of the SAME refused request: must refuse on
    // its own merits again — a leaked admission slot would surface as
    // a spurious `run_already_active` here.
    let retried = err(
        &socket,
        tachyon_protocol::Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: missing.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&retried), "workspace_not_found", "{retried}");

    // Corrected StartRun (the client fixed the root) admits normally.
    ok(&socket, start_run(&task, &ws)).await;
    wait_until("the driver reaches the model stage", || {
        h.active.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(h.entered.load(Ordering::SeqCst), 1);

    // And the admitted run holds admission as usual.
    let dup = err(&socket, start_run(&task, &ws)).await;
    assert_eq!(code_of(&dup), "run_already_active", "{dup}");
    assert_eq!(h.entered.load(Ordering::SeqCst), 1);

    gateway.shutdown().await;
}
