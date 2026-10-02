//! Ticket 01 (ACP cancellation-drain slice, issue #57 blocker 4):
//! cancelling a task expires its session's parked MCP approvals.
//!
//! External contract under test —
//!
//!   * `CancelTask` with a parked MCP launch approval expires the park:
//!     the durable `mcp_approvals` row records the cancel outcome, a
//!     late `ApproveMCPServers`/`DenyMCPServers` for the dead id fails
//!     as `approval_missing`, and the server never launches (no spawn
//!     marker);
//!   * `CancelTask` with a parked MCP call approval expires the park:
//!     late `ApproveMCPTool`/`DenyMCPTool` fail as `approval_missing`
//!     and the call never executes (empty calls log);
//!   * both orderings resolve deterministically: deny-then-cancel keeps
//!     the denial (no rewrite), cancel-then-deny keeps the cancel
//!     outcome (the late deny consumes nothing).
//!
//! The MCP child is always a fake test script speaking
//! newline-delimited JSON-RPC over stdin/stdout — never a real server
//! binary. Spawn evidence is a marker file the script appends to on
//! startup; call evidence is a calls log.

use std::path::{Path, PathBuf};

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_protocol::Command;
use tachyon_store::StoreWriter;

mod common;
use common::{
    code_of, create_rooted_session, err, ok, public_env, script_server, send, test_dir,
    write_script,
};

/// Fake MCP launch child: records startup evidence, then answers the
/// `initialize` + `tools/list` handshake like the gated-launch fake.
const FAKE_LAUNCH: &str = r#"
import json, os, sys, time
marker = os.environ.get("MARKER_FILE", "")
if marker:
    with open(marker, "a") as f:
        f.write("spawn\n")
def readline():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)
req = readline()
assert req.get("method") == "initialize", req
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": req.get("id"),
    "result": {"protocolVersion": "2024-11-05",
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
    time.sleep(60)
"#;

/// Fake MCP call child: handshake as above, then one `tools/call`
/// round trip per line, logging every call to `CALLS_FILE`.
const FAKE_CALL: &str = r#"
import json, os, sys
calls = os.environ.get("CALLS_FILE", "")
def readline():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)
req = readline()
assert req.get("method") == "initialize", req
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": req.get("id"),
    "result": {"protocolVersion": "2024-11-05",
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
    if calls:
        with open(calls, "a") as f:
            f.write(str(params.get("name", "")) + "\n")
    result = {"content": [{"type": "text",
                           "text": json.dumps(params.get("arguments", {}))}]}
    sys.stdout.write(json.dumps({
        "jsonrpc": "2.0", "id": call.get("id"), "result": result,
    }) + "\n")
    sys.stdout.flush()
"#;

/// One rooted session plus one task in it; returns `(dir, socket,
/// session_id, task_id, gateway)`. The returned gateway must stay alive
/// for the test (dropping it kills live children through
/// `kill_on_drop`, which is what the never-spawns assertions need to be
/// meaningful).
async fn session_with_task() -> (
    PathBuf,
    PathBuf,
    String,
    String,
    tachyon_gateway::RunningGateway,
) {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;
    let task = ok(
        &socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "cancel-drain test task".to_owned(),
            idempotency_key: None,
        },
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    (dir, socket, session_id, task_id, gateway)
}

async fn cancel(socket: &Path, task_id: &str) -> Value {
    let ack = ok(
        socket,
        Command::CancelTask {
            task_id: task_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(ack["task"]["status"], "Cancelled", "{ack}");
    ack
}

async fn durable_mcp_outcome(dir: &Path, approval_id: &str) -> String {
    let store = StoreWriter::open(dir).await.unwrap();
    let row = store
        .get_mcp_approval(approval_id)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("no durable mcp_approvals row for {approval_id}"));
    store.close().await;
    row.outcome
}

/// Cancel with a parked MCP launch approval expires the park: the
/// durable row records the cancel outcome, late approve/deny fail as
/// `approval_missing`, and the server never launches.
#[tokio::test]
async fn cancel_with_parked_launch_expires_park_and_never_spawns() {
    let (dir, socket, session_id, task_id, _guard) = session_with_task().await;
    let marker = dir.join("spawn.marker");
    let script = write_script(&dir, "fake_launch.py", FAKE_LAUNCH);
    let server = script_server(
        "alpha",
        &script,
        vec![
            public_env("MARKER_FILE", &marker.display().to_string()),
            public_env("REPORT_VERSION", "2024-11-05"),
        ],
    );
    let parked = ok(
        &socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![server],
        },
    )
    .await;
    let approval_id = parked["approval_id"].as_str().unwrap().to_owned();

    cancel(&socket, &task_id).await;

    assert_eq!(
        code_of(
            &err(
                &socket,
                Command::ApproveMCPServers {
                    session_id: session_id.parse().unwrap(),
                    approval_id: approval_id.parse().unwrap(),
                },
            )
            .await
        ),
        "approval_missing"
    );
    assert_eq!(
        code_of(
            &err(
                &socket,
                Command::DenyMCPServers {
                    session_id: session_id.parse().unwrap(),
                    approval_id: approval_id.parse().unwrap(),
                    reason: "too late".to_owned(),
                },
            )
            .await
        ),
        "approval_missing"
    );
    assert!(
        !marker.exists(),
        "the cancelled launch must never spawn its child"
    );
    assert_eq!(
        durable_mcp_outcome(&dir, &approval_id).await,
        "cancelled",
        "the expired park records its durable outcome"
    );
}

/// Parks one call on a live server and returns its approval id.
async fn park_call_on_live_server(
    socket: &Path,
    session_id: &str,
    dir: &Path,
) -> (String, PathBuf) {
    let calls_file = dir.join("calls.log");
    let script = write_script(dir, "fake_call.py", FAKE_CALL);
    let server = script_server(
        "alpha",
        &script,
        vec![
            public_env("CALLS_FILE", &calls_file.display().to_string()),
            public_env("REPORT_VERSION", "2024-11-05"),
        ],
    );
    let parked = ok(
        socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![server],
        },
    )
    .await;
    let launch_id = parked["approval_id"].as_str().unwrap().to_owned();
    ok(
        socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: launch_id.parse().unwrap(),
        },
    )
    .await;
    let (status, payload, _) = send(
        socket,
        Command::CallMCPTool {
            session_id: session_id.parse().unwrap(),
            server_id: "alpha".to_owned(),
            tool: "echo".to_owned(),
            arguments_json: serde_json::json!({"input": "hi"}),
        },
    )
    .await;
    assert_eq!(status, 200, "{payload}");
    assert_eq!(payload["status"], "awaiting_approval", "{payload}");
    let approval_id = payload["approval_id"].as_str().unwrap().to_owned();
    (approval_id, calls_file)
}

/// Cancel with a parked MCP call approval expires the park: late
/// grant/refusal fail as `approval_missing` and the call never
/// executes.
#[tokio::test]
async fn cancel_with_parked_call_expires_park_and_never_executes() {
    let (dir, socket, session_id, task_id, _guard) = session_with_task().await;
    let (approval_id, calls_file) = park_call_on_live_server(&socket, &session_id, &dir).await;

    cancel(&socket, &task_id).await;

    assert_eq!(
        code_of(
            &err(
                &socket,
                Command::ApproveMCPTool {
                    session_id: session_id.parse().unwrap(),
                    approval_id: approval_id.parse().unwrap(),
                },
            )
            .await
        ),
        "approval_missing"
    );
    assert_eq!(
        code_of(
            &err(
                &socket,
                Command::DenyMCPTool {
                    session_id: session_id.parse().unwrap(),
                    approval_id: approval_id.parse().unwrap(),
                    reason: "too late".to_owned(),
                },
            )
            .await
        ),
        "approval_missing"
    );
    assert!(
        !calls_file.exists(),
        "the cancelled call must never execute: {}",
        calls_file.display()
    );
    assert_eq!(
        durable_mcp_outcome(&dir, &approval_id).await,
        "cancelled",
        "the expired park records its durable outcome"
    );
}

/// Deny-then-cancel: the denial stands — cancel never rewrites a
/// decided row, so there is exactly one outcome.
#[tokio::test]
async fn deny_then_cancel_keeps_denial_without_rewrite() {
    let (dir, socket, session_id, task_id, _guard) = session_with_task().await;
    let (approval_id, calls_file) = park_call_on_live_server(&socket, &session_id, &dir).await;

    ok(
        &socket,
        Command::DenyMCPTool {
            session_id: session_id.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
            reason: "nope".to_owned(),
        },
    )
    .await;

    cancel(&socket, &task_id).await;

    assert_eq!(
        durable_mcp_outcome(&dir, &approval_id).await,
        "denied",
        "cancel must not rewrite the earlier denial"
    );
    assert!(
        !calls_file.exists(),
        "the denied call must never execute: {}",
        calls_file.display()
    );
}

/// Cancel-then-deny: the cancel wins — the late deny fails typed and
/// changes no state.
#[tokio::test]
async fn cancel_then_deny_is_a_typed_noop() {
    let (dir, socket, session_id, task_id, _guard) = session_with_task().await;
    let (approval_id, calls_file) = park_call_on_live_server(&socket, &session_id, &dir).await;

    cancel(&socket, &task_id).await;

    assert_eq!(
        code_of(
            &err(
                &socket,
                Command::DenyMCPTool {
                    session_id: session_id.parse().unwrap(),
                    approval_id: approval_id.parse().unwrap(),
                    reason: "too late".to_owned(),
                },
            )
            .await
        ),
        "approval_missing"
    );
    assert_eq!(
        durable_mcp_outcome(&dir, &approval_id).await,
        "cancelled",
        "the late deny must not rewrite the cancel outcome"
    );
    assert!(
        !calls_file.exists(),
        "the cancelled call must never execute: {}",
        calls_file.display()
    );
}
