//! Ticket 03 (ACP MCP-stdio slice): mediated tool calls, secret
//! redaction, and crash semantics.
//!
//! External contract under test —
//!
//!   * a known-tool `CallMCPTool` parks (`awaiting_approval` + a
//!     session-scoped `approval_id`); `ApproveMCPTool` executes it
//!     exactly once through `authorize()` and returns the
//!     broker-redacted receipt; a second grant of the same id fails as
//!     `approval_missing` (duplicate grants never re-execute);
//!   * an unknown tool fails as `unknown_capability` with provably no
//!     child I/O (the fake logs every `tools/call` it receives; the log
//!     stays empty), an unpinned server as `unknown_mcp_server`, a
//!     parked-but-not-live server as `mcp_not_live`, and non-object
//!     arguments as `invalid_mcp_call`;
//!   * deny consumes the park and a late grant afterwards is ignored
//!     (`approval_missing`); a grant from another session fails as
//!     `approval_session_mismatch` without consuming;
//!   * a `secret: true` env value appears as its broker handle (never
//!     raw) in `ListMCPServers` output, in the call receipt, and nowhere
//!     in the durable `state.db*` files — the provider-key redaction
//!     proof, mirrored for MCP secrets;
//!   * a child that dies mid-call never reports success: the grant fails
//!     as typed `mcp_call_failed`, the row falls back to `stopped`, and
//!     the next call fails as `mcp_not_live` (retry needs a fresh
//!     approved launch + call, never a blind replay).
//!
//! Crash-model note (§19 matrix): an MCP tool runs in an untrusted child
//! with ambient authority and declares no recovery path, so its compiled
//! node is `DestructiveExternalMutation` + `Unknown` idempotency — an
//! interrupted call classifies `UnknownAfterCrash`. At the gateway seam
//! there is no task journal to mark, so the classification IS the typed
//! `mcp_call_failed` refusal plus the `stopped` fallback: never success,
//! never an automatic retry.
//!
//! In-process limitation (same shape as `startrun_retry.rs` documents):
//! the test cannot deterministically SIGKILL the child at the exact
//! in-flight instant without a pid race, so the fake takes a poison tool
//! name (`die`) and exits itself inside the call window. The gateway
//! observes EOF on the call pipes exactly as after a `kill -9` of the
//! child — the kill-mid-run shape, minus the signal.

use std::path::{Path, PathBuf};

use serde_json::Value;
use tachyon_gateway::{RunningGateway, start};
use tachyon_protocol::{Command, McpEnvEntry, McpServerDescriptor};
use tachyon_types::{ApprovalId, SessionId};

mod common;
use common::{
    code_of, create_rooted_session, err, ok, public_env, script_server, secret_env, send,
    server_by_id, test_dir, write_script,
};

/// Registered secret under test: registered in the broker at
/// `RegisterMCPServers` time, echoed back by the fake as a call
/// argument, and required to appear as its handle everywhere after.
const SECRET: &str = "mcp-ticket-03-live-secret-value";

/// Fake MCP child: `initialize` (echoes the gateway version),
/// `tools/list` (`echo` + poison `die` + `keyecho` + `borked`), then one
/// `tools/call` round trip per line — `echo` answers `{"content":
/// [{"type": "text", <arguments-json>}]}` and logs the call, `keyecho`
/// answers with the caller's `input` argument as a JSON object KEY (the
/// secret-as-key redaction case), `borked` answers a JSON-RPC error
/// embedding the secret env (the `mcp_tool_error` redaction case), and
/// `die` exits immediately without answering (the fault-kill harness).
const FAKE_CALL: &str = r#"
import json, os, sys
calls = os.environ.get("CALLS_FILE", "")
secret = os.environ.get("API_TOKEN", "")
version = os.environ.get("REPORT_VERSION", "2024-11-05")
def readline():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)
req = readline()
assert req.get("method") == "initialize", req
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": req.get("id"),
    "result": {"protocolVersion": version,
               "serverInfo": {"name": "fake-mcp", "version": "0.1"}},
}) + "\n")
sys.stdout.flush()
msg = readline()
if "id" not in msg:
    msg = readline()
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": msg.get("id"),
    "result": {"tools": [{"name": "echo", "description": "echo input"},
                         {"name": "die", "description": "exit mid-call"},
                         {"name": "keyecho", "description": "echo input as key"},
                         {"name": "borked", "description": "always errors"}]},
}) + "\n")
sys.stdout.flush()
while True:
    call = readline()
    if call.get("method") != "tools/call":
        continue
    params = call.get("params") or {}
    if params.get("name") == "die":
        sys.exit(1)
    if params.get("name") == "borked":
        sys.stdout.write(json.dumps({
            "jsonrpc": "2.0", "id": call.get("id"),
            "error": {"code": -32000,
                      "message": "borked failed while holding " + secret},
        }) + "\n")
        sys.stdout.flush()
        continue
    if calls:
        with open(calls, "a") as f:
            f.write(str(params.get("name", "")) + "\n")
    if params.get("name") == "keyecho":
        key = (params.get("arguments") or {}).get("input", "")
        result = {key: "saw-the-key"}
    else:
        result = {"content": [{"type": "text",
                              "text": json.dumps(params.get("arguments", {}))}]}
    sys.stdout.write(json.dumps({
        "jsonrpc": "2.0", "id": call.get("id"),
        "result": result,
    }) + "\n")
    sys.stdout.flush()
"#;

async fn register(socket: &Path, session_id: &str, servers: Vec<McpServerDescriptor>) -> Value {
    ok(
        socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers,
        },
    )
    .await
}

async fn approve_launch(
    socket: &Path,
    session_id: &str,
    approval_id: &str,
) -> (u16, Value, String) {
    send(
        socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
        },
    )
    .await
}

async fn list(socket: &Path, session_id: &str) -> Value {
    ok(
        socket,
        Command::ListMCPServers {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await
}

async fn call(
    socket: &Path,
    session_id: &str,
    server_id: &str,
    tool: &str,
    arguments: Value,
) -> (u16, Value, String) {
    let session: SessionId = session_id.parse().unwrap();
    send(
        socket,
        Command::CallMCPTool {
            session_id: session,
            server_id: server_id.to_owned(),
            tool: tool.to_owned(),
            arguments_json: arguments,
        },
    )
    .await
}

async fn approve_call(socket: &Path, session_id: &str, approval_id: &str) -> (u16, Value, String) {
    let session: SessionId = session_id.parse().unwrap();
    let approval: ApprovalId = approval_id.parse().unwrap();
    send(
        socket,
        Command::ApproveMCPTool {
            session_id: session,
            approval_id: approval,
        },
    )
    .await
}

async fn deny_call(
    socket: &Path,
    session_id: &str,
    approval_id: &str,
    reason: &str,
) -> (u16, Value, String) {
    let session: SessionId = session_id.parse().unwrap();
    let approval: ApprovalId = approval_id.parse().unwrap();
    send(
        socket,
        Command::DenyMCPTool {
            session_id: session,
            approval_id: approval,
            reason: reason.to_owned(),
        },
    )
    .await
}

/// Started gateway with one `live` server ("alpha") whose calls append
/// to `calls_file`. The gateway is kept alive by the harness (dropping
/// it would kill the child through `kill_on_drop`).
struct LiveHarness {
    gateway: RunningGateway,
    dir: PathBuf,
    socket: PathBuf,
    session_id: String,
    calls_file: PathBuf,
}

impl LiveHarness {
    async fn start(env: Vec<McpEnvEntry>) -> Self {
        let dir = test_dir();
        let root = dir.join("session-root");
        std::fs::create_dir_all(&root).unwrap();
        let calls_file = dir.join("calls.log");
        let script = write_script(&dir, "fake_call.py", FAKE_CALL);
        let gateway = start(&dir).await.unwrap();
        let socket = gateway.address().to_owned();
        let session_id = create_rooted_session(&socket, &root).await;
        let mut full_env = vec![public_env("CALLS_FILE", calls_file.to_str().unwrap())];
        full_env.extend(env);
        let registered = register(
            &socket,
            &session_id,
            vec![script_server("alpha", &script, full_env)],
        )
        .await;
        let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
        let (status, _, message) = approve_launch(&socket, &session_id, &approval_id).await;
        assert_eq!(status, 200, "launch grants: {message}");
        let listed = list(&socket, &session_id).await;
        assert_eq!(server_by_id(&listed, "alpha")["status"], "live");
        Self {
            gateway,
            dir,
            socket,
            session_id,
            calls_file,
        }
    }

    async fn park_echo(&self, arguments: Value) -> String {
        let (status, payload, message) =
            call(&self.socket, &self.session_id, "alpha", "echo", arguments).await;
        assert_eq!(status, 200, "call parks: {message}");
        assert_eq!(payload["status"], "awaiting_approval");
        assert_eq!(payload["server_id"], "alpha");
        assert_eq!(payload["tool"], "echo");
        payload["approval_id"].as_str().unwrap().to_owned()
    }

    async fn shutdown(self) -> PathBuf {
        self.gateway.shutdown().await;
        self.dir
    }
}

fn secret_handle(listed: &Value) -> String {
    server_by_id(listed, "alpha")["env"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["name"] == "API_TOKEN")
        .map(|entry| entry["value"].as_str().unwrap().to_owned())
        .expect("API_TOKEN env is listed")
}

fn call_log(harness: &LiveHarness) -> Vec<String> {
    std::fs::read_to_string(&harness.calls_file)
        .map(|text| text.lines().map(str::to_owned).collect())
        .unwrap_or_default()
}

#[tokio::test]
async fn mediated_call_parks_then_grants_once_with_redacted_receipt() {
    let harness = LiveHarness::start(vec![secret_env("API_TOKEN", SECRET)]).await;

    let approval_id = harness
        .park_echo(serde_json::json!({"input": SECRET}))
        .await;

    let (status, receipt, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 200, "grant executes: {message}");
    assert_eq!(receipt["status"], "ok");
    assert_eq!(receipt["server_id"], "alpha");
    assert_eq!(receipt["tool"], "echo");
    let listed = list(&harness.socket, &harness.session_id).await;
    let handle = secret_handle(&listed);
    assert!(
        !handle.contains(SECRET),
        "list shows the broker handle, never the raw secret"
    );
    let echoed = receipt["result"]["content"][0]["text"]
        .as_str()
        .expect("echo result carries the arguments text");
    let echoed_args: Value = serde_json::from_str(echoed).unwrap();
    assert_eq!(
        echoed_args["input"],
        format!("[redacted:{handle}]"),
        "the receipt redacts the registered secret to its handle"
    );
    assert!(
        !serde_json::to_string(&receipt).unwrap().contains(SECRET),
        "raw secret appears nowhere in the receipt"
    );
    assert_eq!(
        call_log(&harness),
        vec!["echo"],
        "the child ran exactly once"
    );

    let (status, _, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 400, "duplicate grant fails closed");
    assert_eq!(code_of(&message), "approval_missing");
    assert_eq!(call_log(&harness), vec!["echo"], "no second execution");

    let dir = harness.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn unknown_tool_fails_before_child_io_with_typed_refusals() {
    let harness = LiveHarness::start(vec![]).await;

    let (status, _, message) = call(
        &harness.socket,
        &harness.session_id,
        "alpha",
        "nosuchtool",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "unknown_capability");
    assert!(
        !harness.calls_file.exists(),
        "an unknown tool must never reach the child"
    );

    let (status, _, message) = call(
        &harness.socket,
        &harness.session_id,
        "ghost",
        "echo",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "unknown_mcp_server");

    let (status, _, message) = call(
        &harness.socket,
        &harness.session_id,
        "alpha",
        "echo",
        serde_json::json!([1, 2]),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "invalid_mcp_call");

    let ghost_session: String = tachyon_types::SessionId::generate().to_string();
    let (status, _, message) = call(
        &harness.socket,
        &ghost_session,
        "alpha",
        "echo",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "unknown_session");

    let dir = harness.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn parked_server_is_not_live_until_its_launch_grants() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_call.py", FAKE_CALL);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;
    register(
        &socket,
        &session_id,
        vec![script_server("alpha", &script, vec![])],
    )
    .await;

    let (status, _, message) =
        call(&socket, &session_id, "alpha", "echo", serde_json::json!({})).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "mcp_not_live");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn deny_consumes_park_and_late_grant_is_ignored() {
    let harness = LiveHarness::start(vec![]).await;

    let approval_id = harness.park_echo(serde_json::json!({"input": "hi"})).await;
    let (status, denied, message) = deny_call(
        &harness.socket,
        &harness.session_id,
        &approval_id,
        "not now",
    )
    .await;
    assert_eq!(status, 200, "deny consumes: {message}");
    assert_eq!(denied["status"], "denied");

    let (status, _, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 400, "late grant after deny is ignored");
    assert_eq!(code_of(&message), "approval_missing");
    assert!(call_log(&harness).is_empty(), "nothing ever executed");

    let other_root = harness.dir.join("other-root");
    std::fs::create_dir_all(&other_root).unwrap();
    let other_session = create_rooted_session(&harness.socket, &other_root).await;
    let approval_id = harness
        .park_echo(serde_json::json!({"input": "again"}))
        .await;
    let (status, _, message) = approve_call(&harness.socket, &other_session, &approval_id).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "approval_session_mismatch");
    let (status, receipt, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 200, "the owning session still grants: {message}");
    assert_eq!(receipt["status"], "ok");
    assert_eq!(call_log(&harness), vec!["echo"]);

    let refused = err(
        &harness.socket,
        Command::DenyMCPTool {
            session_id: other_session.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
            reason: "consumed already".to_owned(),
        },
    )
    .await;
    assert_eq!(code_of(&refused), "approval_missing");

    let dir = harness.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn kill_mid_call_never_reports_success_and_marks_stopped() {
    let harness = LiveHarness::start(vec![]).await;

    let (status, payload, message) = call(
        &harness.socket,
        &harness.session_id,
        "alpha",
        "die",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "even the fatal call parks first: {message}");
    let approval_id = payload["approval_id"].as_str().unwrap().to_owned();

    let (status, _, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 400, "a dead child is never silent success");
    assert_eq!(code_of(&message), "mcp_call_failed");

    let listed = list(&harness.socket, &harness.session_id).await;
    assert_eq!(
        server_by_id(&listed, "alpha")["status"],
        "stopped",
        "the interrupted server falls back to stopped"
    );
    assert!(
        server_by_id(&listed, "alpha")["tools"]
            .as_array()
            .unwrap()
            .is_empty(),
        "stopping clears the stale inventory"
    );

    let (status, _, message) = call(
        &harness.socket,
        &harness.session_id,
        "alpha",
        "echo",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(
        status, 400,
        "retry needs a fresh approved launch, never a replay"
    );
    assert_eq!(code_of(&message), "mcp_not_live");

    let dir = harness.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn secret_env_stays_handles_across_list_receipt_and_durable_files() {
    let harness = LiveHarness::start(vec![secret_env("API_TOKEN", SECRET)]).await;

    let listed = list(&harness.socket, &harness.session_id).await;
    let listed_string = serde_json::to_string(&listed).unwrap();
    assert!(
        !listed_string.contains(SECRET),
        "list output carries handles only"
    );
    let handle = secret_handle(&listed);
    assert!(listed_string.contains(&handle));

    let approval_id = harness
        .park_echo(serde_json::json!({"token": SECRET}))
        .await;
    let (status, receipt, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 200, "grant executes: {message}");
    let receipt_string = serde_json::to_string(&receipt).unwrap();
    assert!(!receipt_string.contains(SECRET), "receipt is redacted");
    assert!(receipt_string.contains(&handle), "receipt shows the handle");

    let dir = harness.dir.clone();
    harness.shutdown().await;
    let mut durable = Vec::new();
    for name in ["state.db", "state.db-wal", "state.db-shm"] {
        let path = dir.join(name);
        if path.exists() {
            durable.extend(std::fs::read(&path).unwrap());
        }
    }
    assert!(!durable.is_empty(), "the store files exist to scan");
    let raw = SECRET.as_bytes();
    assert!(
        durable.windows(raw.len()).all(|window| window != raw),
        "the raw secret persists nowhere: store, journal, or receipts"
    );

    std::fs::remove_dir_all(&dir).unwrap();
}

/// Fake MCP child that emits one id-less server notification before every
/// `tools/call` response, then answers `echo` exactly like [`FAKE_CALL`]:
/// the granted call must skip the notification within its bound and
/// return the real result with the server still live — never fail a
/// compliant server for notifying mid-call.
const FAKE_NOTIFY_CALL: &str = r#"
import json, os, sys
version = os.environ.get("REPORT_VERSION", "2024-11-05")
def readline():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)
req = readline()
assert req.get("method") == "initialize", req
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": req.get("id"),
    "result": {"protocolVersion": version,
               "serverInfo": {"name": "fake-mcp", "version": "0.1"}},
}) + "\n")
sys.stdout.flush()
msg = readline()
if "id" not in msg:
    msg = readline()
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": msg.get("id"),
    "result": {"tools": [{"name": "echo", "description": "echo input"}]},
}) + "\n")
sys.stdout.flush()
while True:
    call = readline()
    if call.get("method") != "tools/call":
        continue
    params = call.get("params") or {}
    sys.stdout.write(json.dumps({
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
        "params": {},
    }) + "\n")
    sys.stdout.flush()
    sys.stdout.write(json.dumps({
        "jsonrpc": "2.0", "id": call.get("id"),
        "result": {"content": [{"type": "text",
                               "text": json.dumps(params.get("arguments", {}))}]},
    }) + "\n")
    sys.stdout.flush()
"#;

async fn deny_launch(socket: &Path, session_id: &str, approval_id: &str) -> (u16, Value, String) {
    send(
        socket,
        Command::DenyMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
            reason: "not trusted".to_owned(),
        },
    )
    .await
}

#[tokio::test]
async fn secret_echoed_as_a_json_key_returns_handle_only() {
    // The untrusted child received the secret via env and echoes it as
    // a JSON object KEY: the receipt must still carry the broker handle
    // only — `redact_value` scrubs keys through the broker exactly
    // like values.
    let harness = LiveHarness::start(vec![secret_env("API_TOKEN", SECRET)]).await;
    let listed = list(&harness.socket, &harness.session_id).await;
    let handle = secret_handle(&listed);

    let (status, payload, message) = call(
        &harness.socket,
        &harness.session_id,
        "alpha",
        "keyecho",
        serde_json::json!({"input": SECRET}),
    )
    .await;
    assert_eq!(status, 200, "known-tool call parks: {message}");
    let approval_id = payload["approval_id"].as_str().unwrap().to_owned();
    let (status, receipt, message) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(status, 200, "grant executes: {message}");
    let mut expected = serde_json::Map::new();
    expected.insert(
        format!("[redacted:{handle}]"),
        serde_json::Value::String("saw-the-key".to_owned()),
    );
    assert_eq!(
        receipt["result"],
        Value::Object(expected),
        "the echoed key redacts to the broker handle"
    );
    assert!(
        !serde_json::to_string(&receipt).unwrap().contains(SECRET),
        "raw secret appears nowhere in the receipt, key position included"
    );

    let dir = harness.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn tool_error_answers_fail_typed_with_redacted_detail() {
    // A child that answers `tools/call` with a JSON-RPC error fails as
    // typed `mcp_tool_error`: the detail is length-capped and broker-
    // redacted, so a hostile child cannot launder an env secret into an
    // error string.
    let harness = LiveHarness::start(vec![secret_env("API_TOKEN", SECRET)]).await;
    let listed = list(&harness.socket, &harness.session_id).await;
    let handle = secret_handle(&listed);

    let (status, payload, message) = call(
        &harness.socket,
        &harness.session_id,
        "alpha",
        "borked",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 200, "known-tool call parks: {message}");
    let approval_id = payload["approval_id"].as_str().unwrap().to_owned();
    let (status, _, failure) =
        approve_call(&harness.socket, &harness.session_id, &approval_id).await;
    assert_eq!(
        status, 400,
        "the error answer is a typed refusal, never success"
    );
    assert_eq!(code_of(&failure), "mcp_tool_error");
    assert!(!failure.contains(SECRET), "the error detail is redacted");
    assert!(
        failure.contains(&handle),
        "the error detail shows the handle"
    );
    let listed = list(&harness.socket, &harness.session_id).await;
    assert_eq!(
        server_by_id(&listed, "alpha")["status"],
        "live",
        "a tool error is not a transport death: the server stays live"
    );

    let dir = harness.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn call_against_a_refused_server_fails_closed() {
    // Refusing the launch parks nothing and spawns nothing — and a later
    // `CallMCPTool` against the refused server fails as `mcp_not_live`
    // without touching any child: denial is the cancellation, and there
    // is no live child to route to.
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_call.py", FAKE_CALL);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server("alpha", &script, vec![])],
    )
    .await;
    let launch_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, message) = deny_launch(&socket, &session_id, &launch_id).await;
    assert_eq!(status, 200, "deny refuses the launch: {message}");

    let (status, _, failure) =
        call(&socket, &session_id, "alpha", "echo", serde_json::json!({})).await;
    assert_eq!(status, 400, "calls against a refused server fail closed");
    assert_eq!(code_of(&failure), "mcp_not_live");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn notifying_child_stays_live_through_call_with_result_intact() {
    // A notification arriving between the granted `tools/call` request
    // and its response is skipped within the call bound: the grant
    // returns the real redacted result and the server stays live with
    // its inventory intact.
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_notify_call.py", FAKE_NOTIFY_CALL);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server("chatty", &script, vec![])],
    )
    .await;
    let launch_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, message) = send(
        &socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: launch_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(status, 200, "launch grants: {message}");

    let (status, payload, message) = call(
        &socket,
        &session_id,
        "chatty",
        "echo",
        serde_json::json!({"hello": "world"}),
    )
    .await;
    assert_eq!(status, 200, "known-tool call parks: {message}");
    let call_id = payload["approval_id"].as_str().unwrap().to_owned();
    let (status, receipt, message) = approve_call(&socket, &session_id, &call_id).await;
    assert_eq!(status, 200, "notified call still executes: {message}");
    assert_eq!(receipt["status"], "ok");
    let text = receipt["result"]["content"][0]["text"]
        .as_str()
        .expect("echo result carries the arguments text");
    assert_eq!(
        serde_json::from_str::<Value>(text).unwrap(),
        serde_json::json!({"hello": "world"}),
        "the notification was skipped, not mistaken for the reply"
    );
    let listed = list(&socket, &session_id).await;
    let chatty = server_by_id(&listed, "chatty");
    assert_eq!(chatty["status"], "live", "the notifying server stays live");
    assert_eq!(chatty["tools"].as_array().unwrap().len(), 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
