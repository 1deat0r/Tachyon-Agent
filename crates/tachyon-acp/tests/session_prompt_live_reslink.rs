//! M25: first live-model ACP prompt turns (frozen measurement).
//!
//! `#[ignore]` + env-gated: never runs in `cargo verify` or the default
//! suite. Explicit run only:
//!
//! ```sh
//! TACHYON_ACP_LIVE=1 TACHYON_M27_OUTPUT=target/m27/raw.jsonl \
//!   cargo test --release -p tachyon-acp --test session_prompt_live_reslink \
//!   -- --ignored --test-threads=1
//! ```
//!
//! Three sequential turns (fresh gateway, adapter, and fixture copy
//! each), driving the full ACP boundary against the live model pinned
//! in `docs/milestones/M25_PLAN.md`. Evaluation only: rows are
//! recorded, never graded — the test fails solely on harness faults
//! (unparseable frames, missing task, unwritable output).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::json;
use tachyon_models::{
    ModelCapabilities, ModelError, ModelEventSink, ModelInvocation, ModelProvider, ModelRequest,
    ModelResult, OpenAiCompatConfig, OpenAiCompatProvider, ProviderEstimate, UsageProvenance,
};

mod common;
use common::{Adapter, classify, parse_frame, test_dir};

/// Frozen verbatim in `M25_PLAN.md`.
const PROMPT: &str = "Fix equal_range so it returns the complete half-open range of entries equal to the query in a sorted slice. For absent values return the empty range at the insertion point. Preserve the public API and other functions. The defect is in subject/src/implementation.rs.";

/// Generous bound: a live-model turn plus a real `cargo test` tail.
const TURN_WAIT: Duration = Duration::from_secs(300);

fn require_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("live test requires {name}"))
}

/// One recorded model call for the evidence row.
#[derive(Clone, Debug)]
struct CallRow {
    attempt: u32,
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
    provenance: UsageProvenance,
    error: Option<String>,
}

/// Live provider with call recording. Delegates everything; counts and
/// preserves per-attempt usage like the bench harness.
struct RecordingProvider {
    inner: OpenAiCompatProvider,
    calls: Mutex<Vec<CallRow>>,
}

impl RecordingProvider {
    fn live() -> (Arc<Self>, String) {
        let base_url = std::env::var("TACHYON_LIVE_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:14000".to_owned());
        let model = require_env("TACHYON_LIVE_MODEL");
        assert!(!model.trim().is_empty(), "TACHYON_LIVE_MODEL must be set");
        let api_key_env = std::env::var("TACHYON_LIVE_API_KEY_ENV")
            .unwrap_or_else(|_| "TACHYON_LIVE_API_KEY_UNSET".to_owned());
        OpenAiCompatConfig::validate_base_url(&base_url, false)
            .expect("live base_url must be valid");
        let inner = OpenAiCompatProvider::local(
            tachyon_types::ProviderId("bench-live".into()),
            OpenAiCompatConfig {
                base_url,
                model: model.clone(),
                api_key_env: Some(api_key_env),
                ..OpenAiCompatConfig::default()
            },
        );
        (
            Arc::new(Self {
                inner,
                calls: Mutex::new(Vec::new()),
            }),
            model,
        )
    }

    fn rows(&self) -> Vec<CallRow> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl ModelProvider for RecordingProvider {
    fn id(&self) -> tachyon_types::ProviderId {
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
        self.invoke_observed(request, sink).await.result
    }
    async fn invoke_observed(
        &self,
        request: ModelRequest,
        sink: ModelEventSink,
    ) -> ModelInvocation {
        let index = {
            let mut calls = self
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let index = calls.len();
            calls.push(CallRow {
                attempt: u32::try_from(index + 1).unwrap_or(u32::MAX),
                input_tokens: None,
                output_tokens: None,
                provenance: UsageProvenance::Unknown,
                error: Some("interrupted".into()),
            });
            index
        };
        let invocation = self.inner.invoke_observed(request, sink).await;
        {
            let mut calls = self
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            calls[index].input_tokens = invocation.usage.input_tokens;
            calls[index].output_tokens = invocation.usage.output_tokens;
            calls[index].provenance = invocation.usage.provenance;
            calls[index].error = invocation
                .result
                .as_ref()
                .err()
                .map(|e| e.code().to_owned());
        }
        invocation
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

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "Completed" | "Failed" | "Cancelled")
}

fn provenance_name(provenance: UsageProvenance) -> &'static str {
    match provenance {
        UsageProvenance::ProviderReported => "provider_reported",
        UsageProvenance::Scripted => "scripted",
        UsageProvenance::Unknown => "unknown",
    }
}

fn millis_since(started: &Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[tokio::test]
#[ignore = "live measurement only: requires TACHYON_ACP_LIVE=1, proxy model, and M25 freeze"]
async fn live_prompt_turns_with_resourcelink() {
    assert_eq!(
        std::env::var("TACHYON_ACP_LIVE").as_deref(),
        Ok("1"),
        "live test requires TACHYON_ACP_LIVE=1"
    );
    let output = PathBuf::from(require_env("TACHYON_M27_OUTPUT"));
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).expect("output dir");
    }

    for sample in 1..=3u64 {
        let row = live_turn(sample).await;
        let line = serde_json::to_string(&row).expect("row serializes");
        {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&output)
                .expect("row file appends");
            writeln!(file, "{line}").expect("row writes");
        }
        println!("{line}");
    }
}

async fn live_turn(sample: u64) -> serde_json::Value {
    let started = Instant::now();
    let (provider, model_name, gateway, socket, dir) = start_live_gateway().await;
    let ws = dir.join(format!("ws-{sample}"));
    copy_dir(&fixtures_root().join("duplicate-range"), &ws).expect("fixture copies");

    let mut adapter = Adapter::spawn(&dir);
    let (end_turn, task_id, response_line) = adapter_prompt(&mut adapter, &socket, &ws).await;
    let status = poll_terminal(&socket, &task_id).await;
    let verified = status == "Completed";
    let _ = classify(&response_line);

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    gateway.shutdown().await;

    build_row(
        sample,
        &model_name,
        verified,
        &status,
        end_turn,
        exit_ok,
        &provider.rows(),
        millis_since(&started),
    )
}

/// Fresh in-process gateway armed with the recording live provider.
async fn start_live_gateway() -> (
    Arc<RecordingProvider>,
    String,
    tachyon_gateway::RunningGateway,
    PathBuf,
    PathBuf,
) {
    let (provider, model_name) = RecordingProvider::live();
    let runtime = tachyon_gateway::GatewayRuntime {
        provider: Some(provider.clone()),
        label: "bench-live".to_owned(),
        model: model_name.clone(),
        redactor: tachyon_tools::credential::CredentialBroker::default(),
    };
    let dir = test_dir();
    let gateway = tachyon_gateway::start_with(&dir, runtime)
        .await
        .expect("gateway starts");
    let socket = gateway.address().to_owned();
    (provider, model_name, gateway, socket, dir)
}

/// One `session/new` + `session/prompt` exchange over adapter stdio.
/// Returns the `end_turn` flag, the turn's task id (resolved through the
/// gateway's own session history), and the raw response line.
async fn adapter_prompt(adapter: &mut Adapter, socket: &Path, ws: &Path) -> (bool, String, String) {
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"session/new","params":{{"cwd":{}}}}}"#,
            json!(ws.display().to_string())
        ))
        .await;
    let (_, created_line) = adapter.read_until_response(json!(1), TURN_WAIT).await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();
    // M27: the prompt carries a resource_link to the fixture's own
    // visible test file (143 bytes, relevant evidence) inside the
    // pinned workspace copy.
    let link_uri = format!("file://{}/subject/tests/visible.rs", ws.display());
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{{"sessionId":"{session_id}","prompt":[{{"type":"text","text":{}}},{{"type":"resource_link","uri":{}}}]}}}}"#,
            serde_json::to_string(PROMPT).expect("prompt serializes"),
            serde_json::to_string(&link_uri).expect("link serializes"),
        ))
        .await;
    let (_, response_line) = adapter.read_until_response(json!(2), TURN_WAIT).await;
    let end_turn = parse_frame(&response_line)["result"]["stopReason"].as_str() == Some("end_turn");
    let session = common::gw_ok(
        socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().expect("session id parses"),
        },
    )
    .await;
    let task_id = session["turns"][0]["task_id"]
        .as_str()
        .expect("turn task")
        .to_owned();
    (end_turn, task_id, response_line)
}

/// Poll gateway task state to terminal (evaluation: record, don't grade).
async fn poll_terminal(socket: &Path, task_id: &str) -> String {
    let mut status = String::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        let state = common::gw_ok(
            socket,
            tachyon_protocol::Command::GetTask {
                task_id: task_id.parse().expect("task id parses"),
            },
        )
        .await;
        status = state["task"]["status"].as_str().unwrap_or("").to_owned();
        if is_terminal(&status) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    status
}

#[allow(clippy::too_many_arguments)]
fn build_row(
    sample: u64,
    model_name: &str,
    verified: bool,
    status: &str,
    end_turn: bool,
    exit_ok: bool,
    calls: &[CallRow],
    completion_ms: u64,
) -> serde_json::Value {
    let input_tokens: Option<u32> = Some(calls.iter().map(|c| c.input_tokens.unwrap_or(0)).sum());
    let output_tokens: Option<u32> = Some(calls.iter().map(|c| c.output_tokens.unwrap_or(0)).sum());
    let provenance = calls
        .first()
        .map_or(UsageProvenance::Unknown, |c| c.provenance);
    json!({
        "fixture": "duplicate-range",
        "mode": "acp-prompt-reslink",
        "links": 1,
        "sample": sample,
        "model": model_name,
        "proposal_variant": "m23-framing",
        "outcome": if verified { "completed" } else { "error" },
        "verified": verified,
        "task_status": status,
        "adapter_end_turn": end_turn,
        "adapter_clean_exit": exit_ok,
        "model_calls": calls.len(),
        "model_attempts": calls.iter().map(|c| json!({
            "attempt": c.attempt,
            "error": c.error,
            "usage": {"input_tokens": c.input_tokens, "output_tokens": c.output_tokens,
                      "provenance": provenance_name(c.provenance)},
        })).collect::<Vec<_>>(),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "usage_provenance": provenance_name(provenance),
        "completion_ms": completion_ms,
    })
}
