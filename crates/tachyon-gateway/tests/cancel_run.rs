//! M11 blocker closure (cancellation drain): `drive()` takes a
//! cooperative cancellation input, the gateway wires `Command::Cancel`
//! to the ACTIVE run's token, stages halt at their boundaries, the
//! receipts of already-committed stages stay journalled, no effect ever
//! lands without its receipt, and a cancelled run is an operator
//! outcome — never a recorded run failure.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use std::path::PathBuf;

use tachyon_gateway::{RunningGateway, start_with};
use tachyon_models::{
    ModelCapabilities, ModelError, ModelEventSink, ModelProvider, ModelRequest, ModelResult,
    ProviderEstimate,
};
use tachyon_protocol::Command;
use tachyon_tools::workspace::WorkspaceLease;
use tachyon_types::ProviderId;

mod common;
use common::{armed_runtime, new_task, ok, test_dir};

/// Provider that parks forever inside the model stage: the run is
/// active and non-parked when the operator cancels.
struct BlockingProvider;

/// Panics after the run reaches the model stage. A panic is an unknown run
/// outcome, so cancellation must not turn it into a normal terminal cancel.
struct PanickingProvider;

static BLOCKING_CAPABILITIES: OnceLock<ModelCapabilities> = OnceLock::new();

#[async_trait::async_trait]
impl ModelProvider for BlockingProvider {
    fn id(&self) -> ProviderId {
        ProviderId("bench-block".into())
    }

    fn capabilities(&self) -> ModelCapabilities {
        BLOCKING_CAPABILITIES
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
        std::future::pending::<()>().await;
        unreachable!("pending forever");
    }
}

#[async_trait::async_trait]
impl ModelProvider for PanickingProvider {
    fn id(&self) -> ProviderId {
        ProviderId("bench-panic".into())
    }

    fn capabilities(&self) -> ModelCapabilities {
        BLOCKING_CAPABILITIES
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
        panic!("scripted provider panic");
    }
}

/// After `CancelTask` the gateway evicts its supervisor map entry, so a read
/// may need one journal recovery before the actor is available. Poll through
/// that window; any other refusal is a real failure.
async fn state_when_readable(socket: &std::path::Path, task: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let (status, payload, got) = common::send(
                socket,
                Command::GetTask {
                    task_id: task.parse().unwrap(),
                },
            )
            .await;
            if status == 200 {
                break payload;
            }
            assert!(
                got.starts_with("task_already_owned|") || got.starts_with("supervisor_gone|"),
                "unexpected refusal while reading the cancelled task: {got}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the cancelled task never became readable")
}

/// Starts a gateway with the blocking provider, creates a Cargo
/// workspace, spawns the run, and waits until it is ACTIVE and
/// mid-stage: evidence journalled (its receipt must survive the
/// cancel), model stage started (provider parked inside it — nothing
/// about this run waits on an approval), workspace lease held.
async fn active_run_fixture() -> (RunningGateway, PathBuf, String, PathBuf, PathBuf, String) {
    let dir = test_dir();
    let runtime = armed_runtime(Arc::new(BlockingProvider));
    let gateway = start_with(&dir, runtime).await.unwrap();
    let socket = gateway.address().to_owned();

    // A Cargo workspace so default acceptance resolves and the run
    // actually spawns into the blocking provider.
    let ws = test_dir().join("cancel-ws");
    std::fs::create_dir_all(&ws).unwrap();
    let cargo_body = "[package]\nname = \"w\"\n";
    std::fs::write(ws.join("Cargo.toml"), cargo_body).unwrap();
    let canonical = std::fs::canonicalize(&ws).unwrap();

    let task = new_task(&socket).await;
    ok(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = ok(
                &socket,
                Command::GetTask {
                    task_id: task.parse().unwrap(),
                },
            )
            .await;
            let stages = state["task"]["stages"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if stages.iter().any(|s| s["stage"] == "model") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the run never reached the model stage");
    let state = state_when_readable(&socket, &task).await;
    assert!(
        !state["task"]["evidence_summary"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "the active run must have journalled its evidence receipt: {}",
        state["task"]["evidence_summary"]
    );
    assert!(
        WorkspaceLease::try_acquire(&canonical)
            .await
            .unwrap()
            .is_none(),
        "the active run must hold the workspace lease"
    );
    (gateway, socket, task, ws, canonical, cargo_body.to_owned())
}

/// Cancel during an ACTIVE (non-parked) run: the driver halts the
/// remaining stages, the workspace lease is released once the driver
/// leaves, the already-journalled receipts (evidence) stay, no new
/// effect ever lands, and no failure text is recorded for the operator's
/// cancel.
#[tokio::test]
async fn cancel_during_an_active_run_halts_remaining_stages_with_receipts_reconciled() {
    let (gateway, socket, task, ws, canonical, cargo_body) = active_run_fixture().await;

    // 2. The operator cancels the active run. The command waits for the
    //    driver to leave before the supervisor makes cancellation terminal.
    let ack = ok(
        &socket,
        Command::CancelTask {
            task_id: task.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(ack["task"]["status"], "Cancelled", "{ack}");

    // The acknowledgement is also the drain barrier: the driver has left,
    // released the workspace lease, and cannot enter another stage.
    let lease = WorkspaceLease::try_acquire(&canonical)
        .await
        .unwrap()
        .expect("CancelTask acknowledged before the run released its workspace lease");
    drop(lease);

    let state = state_when_readable(&socket, &task).await;
    let stages: Vec<&str> = state["task"]["stages"]
        .as_array()
        .map(|stages| stages.iter().filter_map(|s| s["stage"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        stages.contains(&"model"),
        "the model stage started before the cancel: {stages:?}"
    );
    assert!(
        !stages.contains(&"mutation") && !stages.contains(&"verify"),
        "remaining stages must halt after Cancel: {stages:?}"
    );
    assert!(
        state["task"]["changed_files"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "no effect may land without its receipt: {}",
        state["task"]["changed_files"]
    );
    assert_eq!(
        std::fs::read_to_string(ws.join("Cargo.toml")).unwrap(),
        cargo_body,
        "the halted run must not have touched the workspace"
    );

    // 5. A cancelled run is an operator outcome, not a run failure.
    assert!(
        gateway.task_failure(task.parse().unwrap()).await.is_none(),
        "a cancelled run must not be recorded as a run failure"
    );

    gateway.shutdown().await;
}

#[tokio::test]
async fn cancellation_does_not_hide_a_panicking_driver_outcome() {
    let dir = test_dir();
    let runtime = armed_runtime(Arc::new(PanickingProvider));
    let gateway = start_with(&dir, runtime).await.unwrap();
    let socket = gateway.address().to_owned();
    let ws = test_dir().join("panic-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("Cargo.toml"), "[package]\nname = \"w\"\n").unwrap();
    let canonical = std::fs::canonicalize(&ws).unwrap();
    let task = new_task(&socket).await;

    ok(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = ok(
                &socket,
                Command::GetTask {
                    task_id: task.parse().unwrap(),
                },
            )
            .await;
            if state["task"]["stages"]
                .as_array()
                .is_some_and(|stages| stages.iter().any(|stage| stage["stage"] == "model"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the run never reached the panicking provider");

    let got = common::err(
        &socket,
        Command::CancelTask {
            task_id: task.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(common::code_of(&got), "run_outcome_unknown", "{got}");
    let state = ok(
        &socket,
        Command::GetTask {
            task_id: task.parse().unwrap(),
        },
    )
    .await;
    assert_ne!(state["task"]["status"], "Cancelled", "{state}");
    assert!(
        WorkspaceLease::try_acquire(&canonical)
            .await
            .unwrap()
            .is_some(),
        "the panicking driver's dropped context must release its workspace lease"
    );
    assert!(gateway.task_failure(task.parse().unwrap()).await.is_some());
    gateway.shutdown().await;
}

/// R1 board Seat5-B2: a command after `CancelTask` recovers an actor for
/// the terminal task (reads must keep answering), so every mutation path
/// must still refuse it. The recovered actor is write-proof by the
/// central terminal guard (`transition_journalled` refuses any event
/// from a terminal state; `start_run`/`prepare_run` re-check
/// explicitly) — resurrection can never revive a dead task.
#[tokio::test]
async fn commands_after_cancel_never_revive_the_terminal_task() {
    use common::{code_of, err};

    let dir = test_dir();
    let runtime = armed_runtime(common::fake());
    let gateway = start_with(&dir, runtime).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let task_id = || task.parse().unwrap();

    ok(&socket, Command::CancelTask { task_id: task_id() }).await;

    // First command after cancel: the map entry was evicted, so this
    // read RECOVERS an actor for the terminal task — the exact shape
    // seat5 flagged.
    let state = state_when_readable(&socket, &task).await;
    assert_eq!(state["task"]["status"], "Cancelled", "{state}");
    let revision_before = state["task"]["revision"].clone();

    let duplicate_cancel = err(&socket, Command::CancelTask { task_id: task_id() }).await;
    assert_eq!(
        code_of(&duplicate_cancel),
        "illegal_transition",
        "repeat cancellation of a terminal task: {duplicate_cancel}"
    );

    // Every mutator is typed-refused by the recovered actor...
    let resume = err(&socket, Command::ResumeTask { task_id: task_id() }).await;
    assert_eq!(code_of(&resume), "illegal_transition", "resume: {resume}");
    let pause = err(&socket, Command::PauseTask { task_id: task_id() }).await;
    assert_eq!(code_of(&pause), "illegal_transition", "pause: {pause}");
    let start = err(
        &socket,
        Command::StartRun {
            task_id: task_id(),
            workspace_root: dir.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&start), "illegal_transition", "start: {start}");
    let steer = err(
        &socket,
        Command::SendMessage {
            task_id: task_id(),
            message: "try to steer a corpse".to_owned(),
        },
    )
    .await;
    assert_eq!(code_of(&steer), "illegal_transition", "steer: {steer}");

    // ...and nothing moved: still Cancelled, same revision.
    let after = ok(&socket, Command::GetTask { task_id: task_id() }).await;
    assert_eq!(after["task"]["status"], "Cancelled", "{after}");
    assert_eq!(
        after["task"]["revision"], revision_before,
        "no mutation may slip through the recovered actor"
    );

    gateway.shutdown().await;
}
