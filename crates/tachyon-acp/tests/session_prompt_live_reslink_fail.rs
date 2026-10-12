//! M25: first live-model ACP prompt turns (frozen measurement).
//!
//! `#[ignore]` + env-gated: never runs in `cargo verify` or the default
//! suite. Explicit run only:
//!
//! ```sh
//! TACHYON_ACP_LIVE=1 TACHYON_M28_OUTPUT=target/m28/raw.jsonl \
//!   cargo test --release -p tachyon-acp --test session_prompt_live_reslink_fail \
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
    ModelResult, OpenAiCompatConfig, OpenAiCompatProvider, ProviderEstimate,
};

mod common;
use common::{Adapter, parse_frame, test_dir};

/// Frozen verbatim in `M25_PLAN.md`.
const PROMPT: &str = "Fix equal_range so it returns the complete half-open range of entries equal to the query in a sorted slice. For absent values return the empty range at the insertion point. Preserve the public API and other functions. The defect is in subject/src/implementation.rs.";

/// Generous bound: a live-model turn plus a real `cargo test` tail.
const TURN_WAIT: Duration = Duration::from_secs(300);

fn require_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("live test requires {name}"))
}

/// Live provider with call counting. Delegates everything; records
/// one marker per invocation so failure rows prove zero model calls.
struct RecordingProvider {
    inner: OpenAiCompatProvider,
    calls: Mutex<Vec<()>>,
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

    fn calls(&self) -> usize {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
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
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(());
        self.inner.invoke_observed(request, sink).await
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

fn millis_since(started: &Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[tokio::test]
#[ignore = "live measurement only: requires TACHYON_ACP_LIVE=1, proxy model, and M28 freeze"]
async fn live_prompt_turns_with_bad_links() {
    assert_eq!(
        std::env::var("TACHYON_ACP_LIVE").as_deref(),
        Ok("1"),
        "live test requires TACHYON_ACP_LIVE=1"
    );
    let output = PathBuf::from(require_env("TACHYON_M28_OUTPUT"));
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).expect("output dir");
    }

    // s1 sibling-escape -> out_of_root; s2 oversized-in-ws -> oversized.
    for sample in 1..=2u64 {
        let row = live_fail_turn(sample).await;
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

/// One failure turn: prepares the bad link target, prompts with it,
/// and records the refusal. Any task creation or model call on these
/// rows fails the test — the confinement claim under measurement.
async fn live_fail_turn(sample: u64) -> serde_json::Value {
    let started = Instant::now();
    let (provider, model_name, gateway, socket, dir) = start_live_gateway().await;
    let ws = dir.join(format!("ws-fail-{sample}"));
    copy_dir(&fixtures_root().join("duplicate-range"), &ws).expect("fixture copies");
    let (link_uri, expected_data) = if sample == 1 {
        let outside = dir.join("outside");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::fs::write(outside.join("secret.txt"), b"nope").expect("outside file");
        (
            format!("file://{}/secret.txt", outside.display()),
            "out_of_root",
        )
    } else {
        let big = ws.join("big.bin");
        std::fs::write(&big, vec![b'x'; 65537]).expect("oversized file");
        (format!("file://{}/big.bin", ws.display()), "oversized")
    };

    let mut adapter = Adapter::spawn(&dir);
    let session_id = adapter_new_session(&mut adapter, &ws).await;
    let response_line = adapter_prompt_blocks(&mut adapter, &session_id, &link_uri).await;
    let response = parse_frame(&response_line);
    let error_data = response["error"]["data"].as_str().unwrap_or("").to_owned();
    let error_code = response["error"]["code"].as_i64().unwrap_or(0);

    // No turn may exist: the refusal precedes CreateTask.
    let session = common::gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().expect("session id parses"),
        },
    )
    .await;
    let turns = session["turns"].as_array().cloned().unwrap_or_default();
    let calls = provider.calls();
    // Hard assertions: the confinement claim under measurement.
    assert!(turns.is_empty(), "refused prompts create no turns");
    assert_eq!(calls, 0, "refused prompts make no model calls");
    assert_eq!(error_code, -32602, "refusal code");
    assert_eq!(error_data, expected_data, "exact taxonomy marker");

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    gateway.shutdown().await;

    let verified = error_data == expected_data && turns.is_empty() && calls == 0;
    json!({
        "fixture": "duplicate-range",
        "mode": "acp-prompt-reslink-fail",
        "sample": sample,
        "model": model_name,
        "proposal_variant": "m23-framing",
        "outcome": if verified { "completed" } else { "error" },
        "verified": verified,
        "task_status": "none",
        "adapter_end_turn": false,
        "adapter_clean_exit": exit_ok,
        "adapter_error_code": error_code,
        "adapter_error_data": error_data,
        "expected_data": expected_data,
        "model_calls": calls,
        "model_attempts": [],
        "input_tokens": null,
        "output_tokens": null,
        "usage_provenance": "unknown",
        "completion_ms": millis_since(&started),
        "links": 1,
    })
}

/// Minimal session/new (no servers) returning the session id.
async fn adapter_new_session(adapter: &mut Adapter, ws: &Path) -> String {
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"session/new","params":{{"cwd":{}}}}}"#,
            json!(ws.display().to_string())
        ))
        .await;
    let (_, created_line) = adapter.read_until_response(json!(1), TURN_WAIT).await;
    parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned()
}

/// One prompt carrying text plus a single `resource_link`; returns the
/// raw response line (expected: an error frame).
/// One prompt carrying text plus a single `resource_link`; returns the
/// raw response line (expected: an error frame).
async fn adapter_prompt_blocks(adapter: &mut Adapter, session_id: &str, link_uri: &str) -> String {
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{{"sessionId":"{session_id}","prompt":[{{"type":"text","text":{}}},{{"type":"resource_link","uri":{}}}]}}}}"#,
            serde_json::to_string(PROMPT).expect("prompt serializes"),
            serde_json::to_string(link_uri).expect("link serializes"),
        ))
        .await;
    let (_, response_line) = adapter.read_until_response(json!(2), TURN_WAIT).await;
    response_line
}

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
