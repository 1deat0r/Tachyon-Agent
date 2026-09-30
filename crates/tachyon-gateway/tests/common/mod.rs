//! Shared helpers for tachyon-gateway integration tests (M11 writer C).
#![allow(dead_code)] // each test target imports only what it needs

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tachyon_gateway::transport::connect;
use tachyon_gateway::{FAKE_PROVIDER_LABEL, GatewayRuntime};
use tachyon_models::ModelProvider;
use tachyon_models::fake::FakeModelProvider;
use tachyon_protocol::{Command, CommandResult, RequestEnvelope, ResponseEnvelope};
use tachyon_tools::credential::CredentialBroker;
use tachyon_types::{EventId, ProviderId};

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// One unique directory per call: never call twice inside one test.
pub fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("tachyon-m11-{}-{id}", std::process::id()))
}

/// Gateway runtime with the scripted fake provider armed (label included).
pub fn armed_runtime(provider: Arc<dyn ModelProvider>) -> GatewayRuntime {
    GatewayRuntime {
        provider: Some(provider),
        label: FAKE_PROVIDER_LABEL.to_owned(),
        model: "scripted-replay-1".to_owned(),
        redactor: CredentialBroker::default(),
    }
}

/// An empty-script fake: every run reaches the model stage and fails
/// there honestly (the script is what drives a full run in G5).
pub fn fake() -> Arc<dyn ModelProvider> {
    Arc::new(FakeModelProvider::new(ProviderId("bench-script".into())))
}

/// Sends one command; returns (200, payload, "") or (400, Null, "code|message").
pub async fn send(socket: &Path, command: Command) -> (u16, Value, String) {
    let mut stream = connect(socket).await.expect("connect to gateway");
    let request = RequestEnvelope {
        protocol_version: tachyon_protocol::PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command,
    };
    let bytes = tachyon_protocol::encode_frame(&request).expect("encode request");
    stream.write_all(&bytes).await.expect("send request");
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.expect("read prefix");
    let len = u32::from_le_bytes(prefix) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.expect("read payload");
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    let (response, _): (ResponseEnvelope, usize) =
        tachyon_protocol::decode_frame(&framed).expect("decode response");
    match response.result {
        CommandResult::Ok { payload } => (200, payload, String::new()),
        CommandResult::Err { code, message } => (400, Value::Null, format!("{code}|{message}")),
    }
}

/// Sends a command and panics unless it succeeded.
pub async fn ok(socket: &Path, command: Command) -> Value {
    let (status, payload, err) = send(socket, command).await;
    assert_eq!(status, 200, "expected success, got {err}");
    payload
}

/// Sends a command and panics unless it was refused; returns `code|message`.
pub async fn err(socket: &Path, command: Command) -> String {
    let (status, _, err) = send(socket, command).await;
    assert_eq!(status, 400, "expected a typed refusal");
    err
}

/// Stable `code` of an `err(...)` result.
pub fn code_of(err: &str) -> &str {
    err.split('|').next().unwrap_or(err)
}

/// Creates a session and a task through the protocol; returns task id.
pub async fn new_task(socket: &Path) -> String {
    let session = ok(
        socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = ok(
        socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "gateway test task".to_owned(),
        },
    )
    .await;
    task["task_id"].as_str().unwrap().to_owned()
}

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
