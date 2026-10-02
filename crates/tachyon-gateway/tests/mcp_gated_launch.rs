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
    code_of, create_rooted_session, err, ok, public_env, script_server, secret_arg, secret_env,
    send, server_by_id, test_dir, write_script,
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

/// Fake MCP child that demands `__EXPECTED_ARG__` in its own argv and
/// exits before the handshake otherwise: a live launch IS the proof that
/// the broker-resolved raw secret reached `execve` as an argument.
const FAKE_ARG_TEMPLATE: &str = r#"
import json, os, sys, time
marker = os.environ.get("MARKER_FILE", "")
if marker:
    with open(marker, "a") as f:
        f.write("spawn\n")
expected = __EXPECTED_ARG__
if expected and expected not in sys.argv:
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

/// The argv fake expecting `arg` as one argv element: the literal is
/// JSON-quoted into the script, so the value never crosses the gateway
/// as a public entry.
fn fake_ok_with_arg(arg: &str) -> String {
    FAKE_ARG_TEMPLATE.replace("__EXPECTED_ARG__", &serde_json::to_string(arg).unwrap())
}

fn marker_count(marker: &Path) -> usize {
    std::fs::read_to_string(marker).map_or(0, |text| text.lines().count())
}

/// Rewrites one pinned row's JSON column directly in `state.db` — the
/// hostile-mutation / legacy-row seam register-time validation can never
/// see (`env_json` for env entries, `args_json` for argv entries). The
/// trailing `changes()` proves exactly one row was rewritten (its line
/// is the last in the output, after the `busy_timeout` echo).
fn mutate_row_json(db: &Path, server_id: &str, column: &str, json: &str) {
    let sql = format!(
        "PRAGMA busy_timeout = 5000;\n\
         UPDATE mcp_servers SET {column} = '{json}' WHERE server_id = '{server_id}';\n\
         SELECT changes();"
    );
    let output = std::process::Command::new("sqlite3")
        .arg(db)
        .arg(sql)
        .output()
        .expect("sqlite3 CLI rewrites the pinned row");
    assert!(
        output.status.success(),
        "sqlite3 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        stdout.lines().last(),
        Some("1"),
        "exactly one pinned row mutated: {stdout:?}"
    );
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

/// Ticket 01 (ACP env-secrets slice): the stored row is untrusted at
/// the use site, not just at register. Each case pins a valid row,
/// then rewrites its `env_json` straight in `state.db` (bypassing
/// register-time validation) with a hostile entry: the full descriptor
/// core re-runs immediately before spawn and refuses with typed
/// `invalid_mcp_descriptor` — name-only message, zero processes
/// started, and the existing launch-failure lifecycle marks the row
/// `stopped`.
#[tokio::test]
async fn mutated_row_launch_refuses_typed_with_zero_process() {
    // One case per rule group the launch re-validation must cover:
    // loader denylist, interpreter denylist, inherited allowlist
    // override, env name shape, and env value bounds.
    let cases: Vec<(&str, String)> = vec![
        ("LD_PRELOAD", "/tmp/evil.so".to_owned()),
        ("BASH_ENV", "/tmp/hostile-bashrc".to_owned()),
        ("PATH", "/hostile/bin".to_owned()),
        ("BAD-NAME", "hostile-value".to_owned()),
        ("BLOB", "v".repeat(17_000)),
    ];
    for (bad_name, bad_value) in &cases {
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
                "mutated",
                &script,
                vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
            )],
        )
        .await;
        let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

        let env_json = serde_json::json!([
            {"name": bad_name, "value": bad_value, "secret": false},
            {"name": "MARKER_FILE", "value": marker.to_str().unwrap(), "secret": false},
        ])
        .to_string();
        mutate_row_json(&dir.join("state.db"), "mutated", "env_json", &env_json);

        let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
        assert_eq!(
            status, 400,
            "{bad_name}: a mutated row must refuse launch: {failure}"
        );
        assert_eq!(
            code_of(&failure),
            "invalid_mcp_descriptor",
            "{bad_name}: the refusal is typed: {failure}"
        );
        assert!(
            failure.contains(bad_name),
            "{bad_name}: the refusal names the offending entry: {failure}"
        );
        assert!(
            !failure.contains(bad_value),
            "{bad_name}: the refusal never echoes the value: {failure}"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!marker.exists(), "{bad_name}: zero processes started");

        let listed = list(&socket, &session_id).await;
        assert_eq!(
            server_by_id(&listed, "mutated")["status"],
            "stopped",
            "{bad_name}: the launch-failure lifecycle marks the row stopped"
        );

        gateway.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
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

/// Ticket 02 (ACP env-secrets slice): a `secret: true` argv entry
/// registers with the vault at pin time (the durable row keeps only the
/// handle), resolves into the child's argv at spawn — the fake exits
/// before the handshake unless its argv carries the raw value, so a live
/// launch IS the delivery proof — while list output and the durable
/// bytes stay handle-only for the secret and raw for the neighbour.
#[tokio::test]
async fn secret_arg_resolves_into_argv_and_lists_handle_only() {
    let raw_secret = "argv-delivered-secret-555";
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_arg.py", &fake_ok_with_arg(raw_secret));
    let marker = dir.join("spawns.log");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![McpServerDescriptor {
            server_id: "guarded".to_owned(),
            command: "/usr/bin/python3".to_owned(),
            args: vec![script.as_str().into(), secret_arg(raw_secret)],
            env: vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        }],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let listed = list(&socket, &session_id).await;
    let args: Vec<Value> = server_by_id(&listed, "guarded")["args"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(
        args[0],
        serde_json::json!({"value": script, "secret": false}),
        "the non-secret script arg echoes raw"
    );
    assert_eq!(args[1]["secret"], true);
    let handle = args[1]["value"].as_str().unwrap().to_owned();
    assert_ne!(
        handle, raw_secret,
        "secret arg lists as a handle, never raw"
    );
    assert!(
        handle.contains("mcp-secret"),
        "handle names the broker vault: {handle}"
    );
    assert!(
        !serde_json::to_string(&listed).unwrap().contains(raw_secret),
        "raw secret arg appears nowhere in list output"
    );

    // The fake exits before the handshake unless its argv carries the
    // raw secret: a 200 launch proves the vault resolved into argv.
    let (status, payload, message) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "the raw secret reached argv: {message}");
    assert_eq!(payload["servers"][0]["status"], "live");
    assert_eq!(marker_count(&marker), 1, "exactly one child spawned");

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
        "the raw secret arg persists nowhere durable"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Ticket 02: launch resolves secret handles at the use site, so a row
/// naming a handle this gateway's vault never issued (the post-restart
/// shape, written straight to `state.db`) fails closed as typed
/// `mcp_spawn_failed` BEFORE any process starts. The same rewrite also
/// proves the dual-format parse: one legacy plain-string element and one
/// secret entry in the same array.
#[tokio::test]
async fn missing_secret_arg_handle_refuses_spawn_with_zero_process() {
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_arg.py", &fake_ok_with_arg("anything"));
    let marker = dir.join("spawns.log");
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "guarded",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    let args_json = serde_json::json!([
        script,
        {"value": "mcp-secret-404", "secret": true},
    ])
    .to_string();
    mutate_row_json(&dir.join("state.db"), "guarded", "args_json", &args_json);

    let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(
        status, 400,
        "a handle outside this gateway's vault must refuse launch: {failure}"
    );
    assert_eq!(
        code_of(&failure),
        "mcp_spawn_failed",
        "the refusal is typed: {failure}"
    );
    assert!(
        failure.contains("not in this gateway's vault"),
        "the refusal names the vault loss: {failure}"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!marker.exists(), "zero processes started");

    let listed = list(&socket, &session_id).await;
    assert_eq!(
        server_by_id(&listed, "guarded")["status"],
        "stopped",
        "the launch-failure lifecycle marks the row stopped"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Ticket 02: launch re-validates argv entries at the use site exactly
/// like env — a row mutated past the register-time bounds (oversized or
/// NUL-carrying value, written straight to `state.db`) refuses with
/// typed `invalid_mcp_descriptor` — value never echoed — and zero
/// processes start.
#[tokio::test]
async fn mutated_arg_row_refuses_launch_typed_with_zero_process() {
    let cases: Vec<(&str, String)> = vec![
        ("oversized arg", "x".repeat(4097)),
        ("NUL arg", "a\u{0}b".to_owned()),
    ];
    for (what, bad_value) in &cases {
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
                "mutated",
                &script,
                vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
            )],
        )
        .await;
        let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

        // One legacy plain-string element plus the hostile value: the
        // dual-format parse accepts the row, then the bounds re-run.
        let args_json = serde_json::json!([script, bad_value]).to_string();
        mutate_row_json(&dir.join("state.db"), "mutated", "args_json", &args_json);

        let (status, _, failure) = approve(&socket, &session_id, &approval_id).await;
        assert_eq!(
            status, 400,
            "{what}: a mutated arg row must refuse launch: {failure}"
        );
        assert_eq!(
            code_of(&failure),
            "invalid_mcp_descriptor",
            "{what}: the refusal is typed: {failure}"
        );
        assert!(
            failure.contains("arg"),
            "{what}: the refusal names the offending arg rule: {failure}"
        );
        assert!(
            !failure.contains(bad_value),
            "{what}: the refusal never echoes the value: {failure}"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!marker.exists(), "{what}: zero processes started");

        let listed = list(&socket, &session_id).await;
        assert_eq!(
            server_by_id(&listed, "mutated")["status"],
            "stopped",
            "{what}: the launch-failure lifecycle marks the row stopped"
        );

        gateway.shutdown().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

/// Ticket 02: rows written before secret args existed are a plain JSON
/// string array; the use site parses them as non-secret entries and the
/// launch proceeds unchanged (no migration, dual-format parse).
#[tokio::test]
async fn legacy_plain_string_args_row_still_launches() {
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
            "legacy",
            &script,
            vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();

    // The pre-secret-args writer's exact shape: a plain string array.
    let args_json = serde_json::json!([script]).to_string();
    mutate_row_json(&dir.join("state.db"), "legacy", "args_json", &args_json);

    let (status, payload, message) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "a legacy row still launches: {message}");
    assert_eq!(payload["servers"][0]["status"], "live");
    assert_eq!(marker_count(&marker), 1, "exactly one child spawned");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Ticket 02: mirror of `secret_reregister_rearms_the_vault_after_restart`
/// for argv — grants and the in-memory vault never survive a restart
/// while the row keeps naming its handle: the pre-restart approval fails
/// closed as `approval_missing`, the pinned handle still lists raw-free
/// after the reopen, and a fresh register (re-supplying the raw value)
/// re-arms the vault so the relaunch succeeds. The armed-vault refusal
/// itself is the `unarmed_vault_refuses_spawn_for_secret_args` unit test
/// at the transport seam plus `missing_secret_arg_handle_refuses_spawn...`
/// above at the gateway seam.
#[tokio::test]
async fn secret_arg_reregister_rearms_the_vault_after_restart() {
    let raw_secret = "restart-rearmed-arg-secret-321";
    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script = write_script(&dir, "fake_arg.py", &fake_ok_with_arg(raw_secret));
    let marker = dir.join("spawns.log");
    let descriptor = |script: &str| McpServerDescriptor {
        server_id: "worker".to_owned(),
        command: "/usr/bin/python3".to_owned(),
        args: vec![script.into(), secret_arg(raw_secret)],
        env: vec![public_env("MARKER_FILE", marker.to_str().unwrap())],
    };

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;
    let registered = register(&socket, &session_id, vec![descriptor(&script)]).await;
    let first = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, _, message) = approve(&socket, &session_id, &first).await;
    assert_eq!(status, 200, "first launch grants: {message}");
    gateway.shutdown().await;

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let (status, _, failure) = approve(&socket, &session_id, &first).await;
    assert_eq!(status, 400, "the pre-restart grant is gone with its vault");
    assert_eq!(code_of(&failure), "approval_missing");

    // The row kept its handle — and only its handle — across the reopen.
    let listed = list(&socket, &session_id).await;
    let args = server_by_id(&listed, "worker")["args"].as_array().unwrap();
    assert_eq!(args[1]["secret"], true);
    assert_ne!(
        args[1]["value"].as_str().unwrap(),
        raw_secret,
        "the durable row still names the handle, never the raw secret"
    );
    assert!(
        !serde_json::to_string(&listed).unwrap().contains(raw_secret),
        "list stays handle-only after the restart"
    );

    let registered = register(&socket, &session_id, vec![descriptor(&script)]).await;
    let fresh = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, payload, message) = approve(&socket, &session_id, &fresh).await;
    assert_eq!(status, 200, "the re-armed reload reconnects: {message}");
    assert_eq!(payload["servers"][0]["status"], "live");
    assert_eq!(marker_count(&marker), 2, "only approved launches spawned");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
