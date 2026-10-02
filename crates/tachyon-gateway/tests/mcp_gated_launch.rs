//! Ticket 02 (ACP MCP-stdio slice): approval-gated stdio launch,
//! handshake, and tool inventory.
//!
//! External contract under test —
//!
//!   * `RegisterMCPServers` parks the launch: rows stay
//!     `awaiting_approval`, the response carries a session-scoped
//!     `approval_id`, and no child process spawns;
//!   * `DenyMCPServers` marks the set `refused`; nothing ever spawns and
//!     the consumed approval fails closed afterwards;
//!   * `ApproveMCPServers` spawns each pinned server as a supervised stdio
//!     child (cwd = pinned session root, secrets injected from the broker
//!     vault), performs the `initialize` handshake, records the
//!     `tools/list` inventory, and `ListMCPServers` reports `live` plus
//!     the real post-handshake version and tool inventory;
//!   * a version mismatch reaps the child and marks the server `stopped`
//!     with typed `mcp_version_mismatch`; an abrupt child exit is typed
//!     `mcp_handshake_failed`;
//!   * a gateway restart leaves servers `stopped` and never auto-relaunches
//!     (pre-restart approvals are gone); a fresh approved reload reconnects
//!     only the pinned set, and unknown approvals fail closed;
//!   * a client disconnect (new connection, gateway alive) leaves live
//!     servers running.
//!
//! The MCP child is always a fake test script speaking newline-delimited
//! JSON-RPC over stdin/stdout — never a real server binary. Spawn evidence
//! is a marker file the script appends to on startup, so "nothing spawns"
//! and "never auto-relaunches" are file assertions, not guesses.

/// MCP wire version the gateway negotiates (mirrors the gateway's
/// `MCP_PROTOCOL_VERSION`; the fake child must echo it back).
const MCP_VERSION: &str = "2024-11-05";

/// default [`MCP_VERSION`]) and `tools/list` (two fixed tools), records
/// startup evidence, then sleeps until killed. A baked-in
/// `__EXPECTED_SECRET__` mismatch on `API_TOKEN` exits before the
/// handshake, proving the broker-injected secret reached the child only
/// when launch succeeds — without the expectation itself ever crossing
/// the gateway (it lives only in the harness script file).
const FAKE_OK_TEMPLATE: &str = r#"
import json, os, sys, time
marker = os.environ.get("MARKER_FILE", "")
if marker:
    with open(marker, "a") as f:
        f.write("spawn\n")
pidfile = os.environ.get("PID_FILE", "")
if pidfile:
    with open(pidfile, "w") as f:
        f.write(str(os.getpid()))
cwdf = os.environ.get("CWD_FILE", "")
if cwdf:
    with open(cwdf, "w") as f:
        f.write(os.getcwd())
expected = __EXPECTED_SECRET__
if expected and os.environ.get("API_TOKEN", "") != expected:
    sys.exit(1)
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
                         {"name": "add", "description": "add numbers"}]},
}) + "\n")
sys.stdout.flush()
while True:
    time.sleep(60)
"#;

/// The fake child with no secret expectation (empty literal is falsy).
fn fake_ok() -> String {
    FAKE_OK_TEMPLATE.replace("__EXPECTED_SECRET__", "\"\"")
}

/// The fake child expecting `secret` on `API_TOKEN`: the literal is
/// JSON-quoted into the script, so the value never crosses the gateway
/// as a public env entry.
fn fake_ok_with_secret(secret: &str) -> String {
    FAKE_OK_TEMPLATE.replace(
        "__EXPECTED_SECRET__",
        &serde_json::to_string(secret).unwrap(),
    )
}

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_protocol::{Command, McpServerDescriptor};
use tachyon_types::{ApprovalId, SessionId};

mod common;
use common::{
    code_of, create_rooted_session, err, ok, public_env, script_server, secret_env, send,
    server_by_id, test_dir, write_script,
};

/// Fake MCP child that exits before any handshake I/O (EOF path).
const FAKE_EOF: &str = "import sys; sys.exit(1)\n";

/// Fake MCP child that emits id-less server notifications (MCP 2024-11-05
/// permits e.g. `notifications/tools/list_changed` at any time) before
/// each handshake reply, then behaves exactly like `fake_ok`: the
/// gateway must skip the notifications within its bound and still admit
/// the launch with the real inventory — never kill a compliant server
/// for notifying.
const FAKE_NOTIFY: &str = r#"
import json, sys, time
def readline():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)
def notify():
    sys.stdout.write(json.dumps({
        "jsonrpc": "2.0",
        "method": "notifications/tools/list_changed",
        "params": {},
    }) + "\n")
    sys.stdout.flush()
req = readline()
assert req.get("method") == "initialize", req
notify()
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": req.get("id"),
    "result": {"protocolVersion": "2024-11-05",
               "serverInfo": {"name": "fake-mcp", "version": "0.1"}},
}) + "\n")
sys.stdout.flush()
msg = readline()
if "id" not in msg:
    msg = readline()
notify()
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": msg.get("id"),
    "result": {"tools": [{"name": "echo", "description": "echo input"},
                         {"name": "add", "description": "add numbers"}]},
}) + "\n")
sys.stdout.flush()
while True:
    time.sleep(60)
"#;

/// Fake MCP child that shouts its secret env to stderr at startup, then
/// behaves exactly like `fake_ok`: stderr belongs to logs through the
/// broker redactor, so the launch must succeed while the raw secret
/// stays out of every durable and listed output.
const FAKE_NOISY_TEMPLATE: &str = r#"
import json, os, sys, time
sys.stderr.write(os.environ.get("API_TOKEN", "") + "\n")
sys.stderr.flush()
marker = os.environ.get("MARKER_FILE", "")
if marker:
    with open(marker, "a") as f:
        f.write("spawn\n")
expected = __EXPECTED_SECRET__
if expected and os.environ.get("API_TOKEN", "") != expected:
    sys.exit(1)
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
                         {"name": "add", "description": "add numbers"}]},
}) + "\n")
sys.stdout.flush()
while True:
    time.sleep(60)
"#;

/// The noisy fake expecting `secret` on `API_TOKEN` (and shouting it to
/// stderr): the literal is JSON-quoted into the script, so the value
/// never crosses the gateway as a public env entry.
fn fake_noisy_with_secret(secret: &str) -> String {
    FAKE_NOISY_TEMPLATE.replace(
        "__EXPECTED_SECRET__",
        &serde_json::to_string(secret).unwrap(),
    )
}

fn marker_count(marker: &Path) -> usize {
    std::fs::read_to_string(marker).map_or(0, |text| text.lines().count())
}

async fn create_rootless_session(socket: &Path) -> String {
    ok(
        socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

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

async fn approve(socket: &Path, session_id: &str, approval_id: &str) -> (u16, Value, String) {
    let approval: ApprovalId = approval_id.parse().unwrap();
    let session: SessionId = session_id.parse().unwrap();
    send(
        socket,
        Command::ApproveMCPServers {
            session_id: session,
            approval_id: approval,
        },
    )
    .await
}

async fn deny(
    socket: &Path,
    session_id: &str,
    approval_id: &str,
    reason: &str,
) -> (u16, Value, String) {
    let approval: ApprovalId = approval_id.parse().unwrap();
    let session: SessionId = session_id.parse().unwrap();
    send(
        socket,
        Command::DenyMCPServers {
            session_id: session,
            approval_id: approval,
            reason: reason.to_owned(),
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

fn child_pid(pidfile: &Path) -> Option<i32> {
    std::fs::read_to_string(pidfile)
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

#[cfg(target_os = "linux")]
fn process_alive(pid: i32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
async fn register_parks_launch_with_awaiting_approval_and_spawns_nothing() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "parked",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    assert_eq!(
        registered["servers"],
        serde_json::json!([{"server_id": "parked", "status": "awaiting_approval"}]),
        "register parks the launch and reports the wait"
    );
    let approval_id = registered["approval_id"]
        .as_str()
        .expect("register returns a session-scoped approval id");
    assert!(!approval_id.is_empty());

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !marker.exists(),
        "a parked launch must never spawn its child"
    );

    let listed = list(&socket, &session_id).await;
    assert_eq!(
        server_by_id(&listed, "parked")["status"],
        "awaiting_approval"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn deny_marks_refused_and_never_spawns() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "denied",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let (status, payload, _) = deny(&socket, &session_id, &approval_id, "not trusted").await;
    assert_eq!(status, 200, "deny is answered, not parked");
    assert_eq!(
        payload["servers"],
        serde_json::json!([{"server_id": "denied", "status": "refused"}])
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!marker.exists(), "a denied set must never spawn");

    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "denied")["status"], "refused");

    let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 400, "a consumed (denied) approval fails closed");
    assert_eq!(code_of(&failure), "approval_missing");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn approve_launches_handshake_and_records_inventory() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let raw_secret = "launch-secret-abc-789";
    let script = write_script(&dir, "fake_ok.py", &fake_ok_with_secret(raw_secret));
    let marker = dir.join("spawns.log");
    let cwds = dir.join("cwd.txt");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "worker",
            &script,
            vec![
                public_env("MARKER_FILE", marker.to_str().unwrap()),
                public_env("CWD_FILE", cwds.to_str().unwrap()),
                secret_env("API_TOKEN", raw_secret),
            ],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let (status, payload, _) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "approve launches the pinned set");
    assert_eq!(
        payload["servers"],
        serde_json::json!([{
            "server_id": "worker",
            "status": "live",
            "version": MCP_VERSION,
            "tools": [
                {"name": "echo", "description": "echo input"},
                {"name": "add", "description": "add numbers"},
            ],
        }]),
        "approve reports the real post-handshake inventory"
    );
    assert_eq!(marker_count(&marker), 1, "exactly one child spawned");

    let listed = list(&socket, &session_id).await;
    let worker = server_by_id(&listed, "worker");
    assert_eq!(worker["status"], "live");
    assert_eq!(worker["version"], MCP_VERSION);
    assert_eq!(worker["tools"], payload["servers"][0]["tools"]);
    let listed_raw = serde_json::to_string(&listed).unwrap();
    assert!(
        !listed_raw.contains(raw_secret),
        "raw secret appears nowhere in list output"
    );

    let cwd = std::fs::read_to_string(&cwds).unwrap();
    let canonical = std::fs::canonicalize(&root).unwrap();
    assert_eq!(
        PathBuf::from(cwd),
        canonical,
        "the child working directory is the pinned session root"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn unknown_approvals_fail_closed_for_approve_and_deny() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;
    let other: String = ok(
        &socket,
        Command::CreateSession {
            workspace_root: Some(root.display().to_string()),
        },
    )
    .await["session_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let unknown = ApprovalId::generate().to_string();
    let (status, _, failure) = approve(&socket, &session_id, &unknown).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&failure), "approval_missing");

    let (status, _, failure) = deny(&socket, &session_id, &unknown, "nope").await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&failure), "approval_missing");

    let missing: SessionId = SessionId::generate();
    let gone = err(
        &socket,
        Command::ApproveMCPServers {
            session_id: missing,
            approval_id: unknown.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(code_of(&gone), "unknown_session");

    let gone = err(
        &socket,
        Command::DenyMCPServers {
            session_id: missing,
            approval_id: unknown.parse().unwrap(),
            reason: "nope".to_owned(),
        },
    )
    .await;
    assert_eq!(code_of(&gone), "unknown_session");

    // An approval is scoped to the session it was registered under.
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let registered = register(
        &socket,
        &session_id,
        vec![script_server("scoped", &script, vec![])],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, failure) = approve(&socket, &other, &approval_id).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&failure), "approval_session_mismatch");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn reregister_invalidates_the_superseded_approval() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let first = register(
        &socket,
        &session_id,
        vec![script_server(
            "flapping",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    let first_id = first["approval_id"].as_str().unwrap().to_owned();
    let second = register(
        &socket,
        &session_id,
        vec![script_server(
            "flapping",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    let second_id = second["approval_id"].as_str().unwrap().to_owned();
    assert_ne!(first_id, second_id, "one approval covers one register call");

    let (status, _, failure) = approve(&socket, &session_id, &first_id).await;
    assert_eq!(status, 400, "a superseded approval fails closed");
    assert_eq!(code_of(&failure), "approval_missing");

    let (status, payload, _) = approve(&socket, &session_id, &second_id).await;
    assert_eq!(status, 200);
    assert_eq!(payload["servers"][0]["status"], "live");
    assert_eq!(marker_count(&marker), 1, "only the granted launch spawned");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn version_mismatch_reaps_the_child_and_marks_stopped() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let pidfile = dir.join("child.pid");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "stale",
            &script,
            vec![
                public_env("PID_FILE", pidfile.to_str().unwrap()),
                public_env("REPORT_VERSION", "1999-01-01"),
            ],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 400, "a version mismatch fails the approve");
    assert_eq!(code_of(&failure), "mcp_version_mismatch");

    let listed = list(&socket, &session_id).await;
    let stale = server_by_id(&listed, "stale");
    assert_eq!(stale["status"], "stopped");
    assert_eq!(stale["version"], Value::Null);
    assert_eq!(stale["tools"], serde_json::json!([]));

    #[cfg(target_os = "linux")]
    {
        let pid = child_pid(&pidfile).expect("fake child recorded its pid");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while process_alive(pid) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            !process_alive(pid),
            "the mismatched child must be reaped, not orphaned"
        );
    }

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn abrupt_child_exit_is_a_typed_handshake_failure() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_eof.py", FAKE_EOF);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server("crashy", &script, vec![])],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&failure), "mcp_handshake_failed");

    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "crashy")["status"], "stopped");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn approve_without_a_session_root_fails_closed() {
    let dir = test_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rootless_session(&socket).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "rootless",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 400, "no pinned root means no child scope");
    assert_eq!(code_of(&failure), "mcp_no_session_root");
    assert!(!marker.exists(), "the fail-closed launch spawns nothing");

    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "rootless")["status"], "stopped");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn restart_leaves_stopped_and_approved_reload_reconnects() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let marker_env = || public_env("MARKER_FILE", marker.to_str().unwrap());

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server("worker", &script, vec![marker_env()])],
    )
    .await;
    let stale_approval = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, _) = approve(&socket, &session_id, &stale_approval).await;
    assert_eq!(status, 200);
    assert_eq!(marker_count(&marker), 1);
    gateway.shutdown().await;

    // A restart never auto-relaunches: rows fall back to stopped and the
    // pre-restart grant is gone with the approving gateway.
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    assert_eq!(marker_count(&marker), 1, "no spawn happened at boot");
    let listed = list(&socket, &session_id).await;
    let worker = server_by_id(&listed, "worker");
    assert_eq!(worker["status"], "stopped");
    assert_eq!(worker["version"], Value::Null);

    let (status, _, failure) = approve(&socket, &session_id, &stale_approval).await;
    assert_eq!(status, 400, "grants never survive a restart");
    assert_eq!(code_of(&failure), "approval_missing");

    // An explicit reload under a fresh approval reconnects the pin.
    let registered = register(
        &socket,
        &session_id,
        vec![script_server("worker", &script, vec![marker_env()])],
    )
    .await;
    let fresh = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, payload, _) = approve(&socket, &session_id, &fresh).await;
    assert_eq!(status, 200);
    assert_eq!(payload["servers"][0]["status"], "live");
    assert_eq!(marker_count(&marker), 2, "only the approved reload spawned");
    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "worker")["status"], "live");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn approved_reload_reconnects_only_the_pinned_set() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let marker_env = || public_env("MARKER_FILE", marker.to_str().unwrap());

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![
            script_server("a", &script, vec![marker_env()]),
            script_server("b", &script, vec![marker_env()]),
        ],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, _) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200);
    assert_eq!(marker_count(&marker), 2);
    gateway.shutdown().await;

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    // Reload names only `a`: `b` stays stopped, never relaunched.
    let registered = register(
        &socket,
        &session_id,
        vec![script_server("a", &script, vec![marker_env()])],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, payload, _) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200);
    assert_eq!(payload["servers"].as_array().unwrap().len(), 1);
    assert_eq!(marker_count(&marker), 3, "only the pinned server respawned");

    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "a")["status"], "live");
    assert_eq!(
        server_by_id(&listed, "b")["status"],
        "stopped",
        "the unpinned server stays stopped after restart"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn disconnect_leaves_live_servers_running() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let pidfile = dir.join("child.pid");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "durable",
            &script,
            vec![
                public_env("MARKER_FILE", marker.to_str().unwrap()),
                public_env("PID_FILE", pidfile.to_str().unwrap()),
            ],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, _) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200);

    // Every command arrives on a fresh connection; the live child is owned
    // by the gateway, not by any client socket.
    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "durable")["status"], "live");
    let listed = list(&socket, &session_id).await;
    assert_eq!(
        server_by_id(&listed, "durable")["tools"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "inventory survives client churn"
    );

    #[cfg(target_os = "linux")]
    {
        let pid = child_pid(&pidfile).expect("fake child recorded its pid");
        assert!(
            process_alive(pid),
            "the live child outlives every client connection"
        );
    }

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn nonexistent_command_is_a_typed_spawn_failure() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let mut missing = script_server("missing", "/nonexistent/mcp-server", vec![]);
    missing.command = "/nonexistent/mcp-server".to_owned();
    let registered = register(&socket, &session_id, vec![missing]).await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    // Registration checks shape only; executability is proven at launch.
    let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&failure), "mcp_spawn_failed");

    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "missing")["status"], "stopped");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn notifying_child_stays_live_through_handshake_with_inventory_intact() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_notify.py", FAKE_NOTIFY);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server("chatty", &script, vec![])],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    // A compliant server that notifies is not a protocol violation: the
    // launch admits it live with the real inventory, and nothing is
    // reaped or marked stopped.
    let (status, payload, message) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "notified handshake still launches: {message}");
    assert_eq!(
        payload["servers"],
        serde_json::json!([{
            "server_id": "chatty",
            "status": "live",
            "version": MCP_VERSION,
            "tools": [
                {"name": "echo", "description": "echo input"},
                {"name": "add", "description": "add numbers"},
            ],
        }]),
        "the inventory after notifications is the real one"
    );
    let listed = list(&socket, &session_id).await;
    assert_eq!(server_by_id(&listed, "chatty")["status"], "live");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn reregister_of_a_live_server_reaps_the_old_child() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok());
    let marker = dir.join("spawns.log");
    let pidfile = dir.join("child.pid");
    let env = || {
        vec![
            public_env("MARKER_FILE", marker.to_str().unwrap()),
            public_env("PID_FILE", pidfile.to_str().unwrap()),
        ]
    };
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server("solo", &script, env())],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, message) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "first launch grants: {message}");
    let old_pid = child_pid(&pidfile).expect("live child recorded its pid");

    // Re-registering the live id re-parks the row: the old approved
    // child is reaped (a running process the durable state calls parked
    // must never linger), and nothing new spawns without a fresh grant.
    let registered = register(
        &socket,
        &session_id,
        vec![script_server("solo", &script, env())],
    )
    .await;
    let listed = list(&socket, &session_id).await;
    assert_eq!(
        server_by_id(&listed, "solo")["status"],
        "awaiting_approval",
        "the re-pinned row parks again"
    );
    assert_eq!(marker_count(&marker), 1, "re-register spawns nothing");
    let _fresh = registered["approval_id"].as_str().unwrap().to_owned();

    #[cfg(target_os = "linux")]
    {
        let mut reaped = false;
        for _ in 0..100 {
            if !process_alive(old_pid) {
                reaped = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(reaped, "the superseded child is reaped on re-register");
    }

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn stderr_echoing_child_keeps_secrets_out_of_durable_files() {
    // Stderr belongs to logs, never to silent pipes — and every line
    // passes through the broker redactor before it reaches `tracing`.
    // Tracing output itself is unobservable from this seam, so the test
    // pins the observable halves: stderr chatter neither breaks the
    // launch nor lands the raw secret in any durable or listed output
    // (the receipt redaction in the mediated suite proves the same
    // broker mapping on the receipt path).
    let raw_secret = "stderr-shouted-secret-456";
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_noisy.py", &fake_noisy_with_secret(raw_secret));
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "noisy",
            &script,
            vec![secret_env("API_TOKEN", raw_secret)],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, payload, message) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "stderr chatter breaks nothing: {message}");
    assert_eq!(payload["servers"][0]["status"], "live");

    let listed = list(&socket, &session_id).await;
    let listed_string = serde_json::to_string(&listed).unwrap();
    assert!(
        !listed_string.contains(raw_secret),
        "list shows handles only"
    );

    gateway.shutdown().await;
    let mut durable = Vec::new();
    for name in ["state.db", "state.db-wal", "state.db-shm"] {
        let path = dir.join(name);
        if path.exists() {
            durable.extend(std::fs::read(&path).unwrap());
        }
    }
    assert!(!durable.is_empty(), "the store files exist to scan");
    let raw = raw_secret.as_bytes();
    assert!(
        durable.windows(raw.len()).all(|window| window != raw),
        "the raw secret persists nowhere durable"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn secret_reregister_rearms_the_vault_after_restart() {
    // Grants never survive a restart — and neither does the in-memory
    // vault, while the rows keep naming their handles. The supported
    // recovery is a fresh register (which re-registers the secret
    // material) plus a fresh approval: the launch then succeeds with
    // the real inventory. A restart without that re-arm can never
    // launch — the pre-restart id answers `approval_missing` and rows
    // stay `stopped`. The unarmed-vault refusal itself
    // (`mcp_spawn_failed` before any spawn) is pinned by the
    // `unarmed_vault` unit test at the transport seam.
    let raw_secret = "restart-rearmed-secret-789";
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_ok.py", &fake_ok_with_secret(raw_secret));
    let marker = dir.join("spawns.log");
    let marker_env = || public_env("MARKER_FILE", marker.to_str().unwrap());

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;
    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "worker",
            &script,
            vec![marker_env(), secret_env("API_TOKEN", raw_secret)],
        )],
    )
    .await;
    let first = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, message) = approve(&socket, &session_id, &first).await;
    assert_eq!(status, 200, "first launch grants: {message}");
    gateway.shutdown().await;

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let (status, _, failure) = approve(&socket, &session_id, &first).await;
    assert_eq!(status, 400, "the pre-restart grant is gone with its vault");
    assert_eq!(code_of(&failure), "approval_missing");

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "worker",
            &script,
            vec![marker_env(), secret_env("API_TOKEN", raw_secret)],
        )],
    )
    .await;
    let fresh = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, payload, message) = approve(&socket, &session_id, &fresh).await;
    assert_eq!(status, 200, "the re-armed reload reconnects: {message}");
    assert_eq!(payload["servers"][0]["status"], "live");
    assert_eq!(marker_count(&marker), 2, "only approved launches spawned");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
