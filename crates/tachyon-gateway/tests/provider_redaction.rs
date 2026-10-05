//! G7 redaction clause (plan item 5, spec §35): a provider error body
//! that embeds a REGISTERED key value must be scrubbed by the
//! [`CredentialBroker`] registry BEFORE it can reach anything a client
//! can read — the run-failure record the gateway exposes, every
//! journalled payload, and every response frame. The raw key must never
//! appear anywhere.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tachyon_gateway::start_with;
use tachyon_models::{
    ModelCapabilities, ModelError, ModelEventSink, ModelProvider, ModelRequest, ModelResult,
    ProviderEstimate,
};
use tachyon_protocol::Command;
use tachyon_tools::credential::CredentialBroker;

mod common;
use common::test_dir;

/// Registered key under test: the config loader registers exactly this
/// way (`register(key_bytes, "provider-api-key")`).
const KEY: &str = "sk-g7-registered-secret-value";

/// A provider whose error body embeds the registered key — the leak
/// vector G7 exists to close.
struct LeakyProvider;

#[async_trait]
impl ModelProvider for LeakyProvider {
    fn id(&self) -> tachyon_types::ProviderId {
        tachyon_types::ProviderId("bench-leaky".into())
    }

    fn capabilities(&self) -> ModelCapabilities {
        ModelCapabilities {
            context_window_tokens: 128_000,
            ..ModelCapabilities::default()
        }
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
        Err(ModelError::ProviderUnavailable(format!(
            "upstream rejected bearer token {KEY}: check the credential"
        )))
    }
}

/// A minimal cargo workspace: `StartRun`'s detection only needs the file
/// to exist; the run fails at the model stage before any verification.
fn cargo_workspace() -> std::path::PathBuf {
    let ws = test_dir().join("cargo-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("Cargo.toml"),
        "[package]\nname=\"g7\"\nversion=\"0.0.0\"\n",
    )
    .unwrap();
    ws
}

#[tokio::test]
async fn registered_key_is_scrubbed_before_any_client_can_read_it() {
    let mut redactor = CredentialBroker::default();
    redactor.register(KEY.as_bytes(), "provider-api-key");

    let runtime = tachyon_gateway::GatewayRuntime {
        provider: Some(Arc::new(LeakyProvider)),
        label: tachyon_gateway::FAKE_PROVIDER_LABEL.to_owned(),
        model: "scripted-replay-1".to_owned(),
        redactor,
    };
    let dir = test_dir();
    let gateway = start_with(&dir, runtime).await.unwrap();
    let socket = gateway.address().to_owned();

    let ws = cargo_workspace();
    let session = common::ok(
        &socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = common::ok(
        &socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "redaction probe".to_owned(),
            idempotency_key: None,
        },
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();

    common::ok(
        &socket,
        Command::StartRun {
            task_id: task_id.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;

    // Wait for the shared driver to record the provider failure.
    let task_key: tachyon_types::TaskId = task_id.parse().unwrap();
    let mut failure = None;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        failure = gateway.task_failure(task_key).await;
        if failure.is_some() {
            break;
        }
    }
    let failure = failure.expect("the run fails and records a scrubbed message");

    // 1. The exposed failure record: scrubbed, handle-token present.
    assert!(
        !failure.contains(KEY),
        "key leaked into the run failure record: {failure}"
    );
    assert!(
        failure.contains("[redacted:provider-api-key"),
        "registered secret not scrubbed: {failure}"
    );

    // 2. Every journalled payload (what the TUI streams back).
    let events = gateway
        .store()
        .load_events_since(&task_id, -1)
        .await
        .unwrap();
    for event in &events {
        let blob = format!("{} {}", event.kind, event.payload);
        assert!(
            !blob.contains(KEY),
            "key leaked into journal {} seq {}: {blob}",
            event.kind,
            event.seq
        );
    }

    // 3. Every response frame this test can read: state + failure view.
    let state = common::ok(&socket, Command::GetTask { task_id: task_key }).await;
    let rendered = serde_json::to_string(&state).unwrap();
    assert!(
        !rendered.contains(KEY),
        "key leaked into a GetTask frame: {rendered}"
    );

    // 4. Issue #72: the failed run leaves canonical Failed status plus
    // the scrubbed text on the GetTask payload (poll: the supervisor
    // journal lands after the failure record).
    let mut observed = None;
    for _ in 0..100 {
        let view = common::ok(&socket, Command::GetTask { task_id: task_key }).await;
        let status = view["task"]["status"].as_str().unwrap_or("").to_owned();
        if status == "Failed" {
            observed = Some(view);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let view = observed.expect("a failed run must leave task status Failed");
    let exposed = view["failure"].as_str().unwrap_or("");
    assert!(
        exposed.contains("[redacted:provider-api-key"),
        "GetTask must expose the scrubbed failure: {view}"
    );

    gateway.shutdown().await;
}

/// Path-shape guard: the test's own imports keep `Path` used if the
/// helper signature changes; harmless no-op.
#[allow(dead_code)]
fn _path_type_check(p: &Path) -> &Path {
    p
}
