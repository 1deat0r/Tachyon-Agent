//! Ticket 03 (ACP cancellation-drain slice, issue #57 blocker 4):
//! crash matrix + no-resurrection pinning at the gateway seam.
//!
//! External contract under test —
//!
//!   * restart expires MCP launch + call parks: no pre-restart approval
//!     id (parked launch, parked call, or already-consumed grant) works
//!     after restart — every late approve/deny fails as the typed
//!     `approval_missing`, nothing launches (no spawn marker) and nothing
//!     executes (empty calls log). The durable `mcp_approvals` rows are
//!     audit only: the live parks are in-memory and die with the gateway;
//!   * restart never downgrades a task to `Cancelled`: a cancelled task
//!     stays `Cancelled`, while tasks in cancel-adjacent states (parked
//!     MCP launch/call, fresh sibling of a cancelled task) recover to a
//!     non-cancelled status — the `kill_restart.rs:198` precedent extended
//!     to the cancel-adjacent paths;
//!   * a fresh register + approve after restart still connects, proving
//!     the refusal above is per-id expiry, not a blanket breakage.
//!
//! The MCP child is always a fake test script speaking
//! newline-delimited JSON-RPC over stdin/stdout — never a real server
//! binary. Restart is `shutdown` + `start` on the same store dir: the
use std::path::Path;

use tachyon_gateway::start;
use tachyon_protocol::Command;

mod common;
use common::{
    code_of, create_rooted_session, err, ok, public_env, script_server, send, server_by_id,
    test_dir, write_script,
};

/// Fake MCP call child: handshake, then one `tools/call` round trip per
/// line, logging every call to `CALLS_FILE`.
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

/// Fake MCP launch child: records startup evidence, then answers the
/// handshake and idles (its park is never granted).
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

async fn session_with_task(socket: &Path, root: &Path, objective: &str) -> (String, String) {
    let session_id = create_rooted_session(socket, root).await;
    let task = ok(
        socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: objective.to_owned(),
            idempotency_key: None,
        },
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    (session_id, task_id)
}

async fn status_of(socket: &Path, task_id: &str) -> String {
    ok(
        socket,
        Command::GetTask {
            task_id: task_id.parse().unwrap(),
        },
    )
    .await["task"]["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// No pre-restart approval id works after restart: parked launch ids,
/// parked call ids, and already-consumed grant ids all fail typed, and
/// nothing launches or executes. Cancel-adjacent tasks are never
/// downgraded to `Cancelled` by the restart.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn restart_expires_mcp_parks_without_resurrection() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let (session_id, task_id) = session_with_task(&socket, &root, "restart matrix task").await;

    // One live server to park a call on.
    let calls_file = dir.join("calls.log");
    let call_script = write_script(&dir, "fake_call.py", FAKE_CALL);
    let parked = ok(
        &socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![script_server(
                "alpha",
                &call_script,
                vec![
                    public_env("CALLS_FILE", &calls_file.display().to_string()),
                    public_env("REPORT_VERSION", "2024-11-05"),
                ],
            )],
        },
    )
    .await;
    let consumed_launch: String = parked["approval_id"].as_str().unwrap().to_owned();
    ok(
        &socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: consumed_launch.parse().unwrap(),
        },
    )
    .await;

    // Parked call (never granted): the absence-of-resurrection target.
    let (status, payload, _) = send(
        &socket,
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
    let parked_call: String = payload["approval_id"].as_str().unwrap().to_owned();

    // Parked launch (never granted): the second resurrection target.
    let marker = dir.join("spawn.marker");
    let launch_script = write_script(&dir, "fake_launch.py", FAKE_LAUNCH);
    let parked = ok(
        &socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![script_server(
                "beta",
                &launch_script,
                vec![
                    public_env("MARKER_FILE", &marker.display().to_string()),
                    public_env("REPORT_VERSION", "2024-11-05"),
                ],
            )],
        },
    )
    .await;
    let parked_launch: String = parked["approval_id"].as_str().unwrap().to_owned();

    // A cancelled sibling: terminal stability across the same restart.
    let (_dead_session, dead_task) =
        session_with_task(&socket, &root, "cancelled before restart").await;
    let ack = ok(
        &socket,
        Command::CancelTask {
            task_id: dead_task.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(ack["task"]["status"], "Cancelled", "{ack}");

    gateway.shutdown().await;

    // --- restart on the SAME store dir ---
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    // Restart never downgrades: the parked task is not Cancelled, the
    // cancelled task is still Cancelled.
    assert_ne!(
        status_of(&socket, &task_id).await,
        "Cancelled",
        "restart must not cancel a task parked on MCP approvals"
    );
    assert_eq!(
        status_of(&socket, &dead_task).await,
        "Cancelled",
        "terminal Cancelled survives restart"
    );

    // Parked launch id: typed refusal on both decisions, nothing spawns.
    for got in [
        err(
            &socket,
            Command::ApproveMCPServers {
                session_id: session_id.parse().unwrap(),
                approval_id: parked_launch.parse().unwrap(),
            },
        )
        .await,
        err(
            &socket,
            Command::DenyMCPServers {
                session_id: session_id.parse().unwrap(),
                approval_id: parked_launch.parse().unwrap(),
                reason: "too late".to_owned(),
            },
        )
        .await,
    ] {
        assert_eq!(code_of(&got), "approval_missing", "{got}");
    }
    // Parked call id: typed refusal on both decisions, nothing executes.
    for got in [
        err(
            &socket,
            Command::ApproveMCPTool {
                session_id: session_id.parse().unwrap(),
                approval_id: parked_call.parse().unwrap(),
            },
        )
        .await,
        err(
            &socket,
            Command::DenyMCPTool {
                session_id: session_id.parse().unwrap(),
                approval_id: parked_call.parse().unwrap(),
                reason: "too late".to_owned(),
            },
        )
        .await,
    ] {
        assert_eq!(code_of(&got), "approval_missing", "{got}");
    }
    // Consumed grant id: replaying the pre-restart grant fails typed.
    let got = err(
        &socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: consumed_launch.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(code_of(&got), "approval_missing", "{got}");

    assert!(
        !marker.exists(),
        "the parked launch must never spawn after restart"
    );
    assert!(
        !calls_file.exists(),
        "the parked call must never execute after restart"
    );
    let listed = ok(
        &socket,
        Command::ListMCPServers {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    for server in listed["servers"].as_array().unwrap() {
        assert_ne!(
            server["status"], "live",
            "no server relaunches at boot: {server}"
        );
    }
    assert_eq!(
        server_by_id(&listed, "alpha")["status"],
        "stopped",
        "the pre-restart live server falls back at boot"
    );
    // Fresh ids still connect: the refusal above is per-id expiry.
    let script = write_script(&dir, "fake_reload.py", FAKE_CALL);
    let parked = ok(
        &socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![script_server(
                "alpha",
                &script,
                vec![
                    public_env("CALLS_FILE", &calls_file.display().to_string()),
                    public_env("REPORT_VERSION", "2024-11-05"),
                ],
            )],
        },
    )
    .await;
    let fresh: String = parked["approval_id"].as_str().unwrap().to_owned();
    assert_ne!(fresh, consumed_launch, "reload parks under a fresh id");
    let launched = ok(
        &socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: fresh.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(launched["servers"][0]["status"], "live", "{launched}");

    gateway.shutdown().await;
}

/// Restart never downgrades cancel-adjacent tasks to `Cancelled`:
/// cancelled stays terminal, fresh and parked-launch siblings stay
/// non-cancelled. Extends the `kill_restart.rs:198` precedent beyond the
/// plain fresh-task path.
#[tokio::test]
async fn restart_never_downgrades_cancel_adjacent_tasks() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let (session_id, cancelled) =
        session_with_task(&socket, &root, "cancelled stays cancelled").await;
    let ack = ok(
        &socket,
        Command::CancelTask {
            task_id: cancelled.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(ack["task"]["status"], "Cancelled", "{ack}");

    let (_fresh_session, fresh) = session_with_task(&socket, &root, "fresh sibling").await;

    // Sibling parked on an MCP launch approval (cancel-adjacent park).
    let (park_session, parked_task) = session_with_task(&socket, &root, "parked sibling").await;
    let script = write_script(&dir, "fake_park.py", FAKE_LAUNCH);
    let parked = ok(
        &socket,
        Command::RegisterMCPServers {
            session_id: park_session.parse().unwrap(),
            servers: vec![script_server(
                "gamma",
                &script,
                vec![public_env(
                    "MARKER_FILE",
                    &dir.join("park.marker").display().to_string(),
                )],
            )],
        },
    )
    .await;
    let parked_launch: String = parked["approval_id"].as_str().unwrap().to_owned();
    let _ = session_id;

    gateway.shutdown().await;
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    assert_eq!(
        status_of(&socket, &cancelled).await,
        "Cancelled",
        "terminal Cancelled is stable across restart"
    );
    assert_ne!(
        status_of(&socket, &fresh).await,
        "Cancelled",
        "restart must not cancel a fresh task (kill_restart.rs:198 precedent)"
    );
    assert_ne!(
        status_of(&socket, &parked_task).await,
        "Cancelled",
        "restart must not cancel a task parked on an MCP launch"
    );
    let got = err(
        &socket,
        Command::ApproveMCPServers {
            session_id: park_session.parse().unwrap(),
            approval_id: parked_launch.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(code_of(&got), "approval_missing", "{got}");

    gateway.shutdown().await;
}
