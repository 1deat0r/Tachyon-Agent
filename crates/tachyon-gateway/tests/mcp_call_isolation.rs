//! Ticket 02 (ACP cancellation-drain slice, issue #57 blocker 4):
//! refusals never block behind a hung call + prompt call abort.
//!
//! External contract under test —
//!
//!   * deny for session A proceeds while session B's MCP call hangs,
//!     and a second call on another server completes in the meantime
//!     (per-server isolation: no global lock across the call bound);
//!   * `CancelTask` for the hanging session's task aborts the in-flight
//!     call promptly (far under the 60 s call bound) with `stopped` +
//!     `mcp_call_failed` — never success;
//!   * a late completion of an aborted call changes no state and reports
//!     nothing as success (the row stays `stopped`, the next call fails
//!     `mcp_not_live`, no success receipt exists).
//!
//! The MCP child is always a fake test script speaking
//! newline-delimited JSON-RPC over stdin/stdout — never a real server
//! binary. The `hang` tool logs its arrival then sleeps past any test
//! bound (the gateway must abort it, never wait it out); the `slow`
//! tool sleeps 10 s then answers success (an abort must beat the
//! answer and still fail closed).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tachyon_protocol::Command;

mod common;
use common::{
    code_of, create_rooted_session, ok, public_env, script_server, send, server_by_id, test_dir,
    write_script,
};

/// Bound far under the 60 s `tools/call` bound: any refusal path that
/// waits this out is wedged behind the hung call.
const PROMPT: Duration = Duration::from_secs(15);

/// Fake MCP child: handshake as usual, then `echo` (answers at once),
/// `hang` (logs, then sleeps 300 s — only an abort ends the call), and
/// `slow` (logs, sleeps 10 s, then answers success like `echo` — an
/// abort must beat the answer and still fail closed).
const FAKE_HANG: &str = r#"
import json, os, sys, time
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
    "result": {"tools": [{"name": "echo", "description": "answer at once"},
                         {"name": "hang", "description": "never answer"},
                         {"name": "slow", "description": "answer after 10s"}]},
}) + "\n")
sys.stdout.flush()
while True:
    call = readline()
    if call.get("method") != "tools/call":
        continue
    params = call.get("params") or {}
    name = params.get("name", "")
    if calls:
        with open(calls, "a") as f:
            f.write(name + "\n")
    if name == "hang":
        time.sleep(300)
    if name == "slow":
        time.sleep(10)
    result = {"content": [{"type": "text",
                           "text": json.dumps(params.get("arguments", {}))}]}
    sys.stdout.write(json.dumps({
        "jsonrpc": "2.0", "id": call.get("id"),
        "result": result,
    }) + "\n")
    sys.stdout.flush()
"#;

/// Two rooted sessions (A and B, B owning the task the abort targets)
/// on one gateway, each with one live server (`alpha` in A, `beta` in
/// B) backed by the hanging fake. The gateway must stay alive for the
/// test (dropping it kills live children through `kill_on_drop`).
struct TwoSession {
    gateway: tachyon_gateway::RunningGateway,
    socket: PathBuf,
    session_a: String,
    session_b: String,
    task_b: String,
    calls_file: PathBuf,
}

impl TwoSession {
    async fn start() -> Self {
        let dir = test_dir();
        let root_a = dir.join("root-a");
        let root_b = dir.join("root-b");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        let calls_file = dir.join("calls.log");
        let script = write_script(&dir, "fake_hang.py", FAKE_HANG);
        let gateway = tachyon_gateway::start(&dir).await.unwrap();
        let socket = gateway.address().to_owned();
        let session_a = create_rooted_session(&socket, &root_a).await;
        let session_b = create_rooted_session(&socket, &root_b).await;
        let task_b = ok(
            &socket,
            Command::CreateTask {
                session_id: session_b.parse().unwrap(),
                objective: "ticket-02 session B task".to_owned(),
                idempotency_key: None,
            },
        )
        .await["task_id"]
            .as_str()
            .unwrap()
            .to_owned();
        for (session, server_id) in [(&session_a, "alpha"), (&session_b, "beta")] {
            let env = vec![public_env("CALLS_FILE", calls_file.to_str().unwrap())];
            let registered = ok(
                &socket,
                Command::RegisterMCPServers {
                    session_id: session.parse().unwrap(),
                    servers: vec![script_server(server_id, &script, env)],
                },
            )
            .await;
            let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
            let (status, _, message) = send(
                &socket,
                Command::ApproveMCPServers {
                    session_id: session.parse().unwrap(),
                    approval_id: approval_id.parse().unwrap(),
                },
            )
            .await;
            assert_eq!(status, 200, "launch grants: {message}");
        }
        Self {
            gateway,
            socket,
            session_a,
            session_b,
            task_b,
            calls_file,
        }
    }

    /// Parks one call; returns its approval id.
    async fn park(&self, session: &str, server: &str, tool: &str) -> String {
        let (status, payload, message) = send(
            &self.socket,
            Command::CallMCPTool {
                session_id: session.parse().unwrap(),
                server_id: server.to_owned(),
                tool: tool.to_owned(),
                arguments_json: json!({}),
            },
        )
        .await;
        assert_eq!(status, 200, "call parks: {message}");
        assert_eq!(payload["status"], "awaiting_approval");
        payload["approval_id"].as_str().unwrap().to_owned()
    }

    fn calls(&self) -> Vec<String> {
        std::fs::read_to_string(&self.calls_file)
            .map(|text| text.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }

    /// Spins until the child's call log holds `tool` (the call reached
    /// the child and is now in flight), or panics past the bound.
    async fn await_in_flight(&self, tool: &str) {
        let start = Instant::now();
        while !self.calls().iter().any(|line| line == tool) {
            assert!(
                start.elapsed() < PROMPT,
                "call '{tool}' never reached the child"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn listed_status(&self, session: &str, server: &str) -> String {
        let listed = ok(
            &self.socket,
            Command::ListMCPServers {
                session_id: session.parse().unwrap(),
            },
        )
        .await;
        server_by_id(&listed, server)["status"]
            .as_str()
            .unwrap()
            .to_owned()
    }
}

async fn approve_call(socket: &Path, session_id: &str, approval_id: &str) -> (u16, Value, String) {
    send(
        socket,
        Command::ApproveMCPTool {
            session_id: session_id.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
        },
    )
    .await
}

async fn deny_call(socket: &Path, session_id: &str, approval_id: &str) -> (u16, Value, String) {
    send(
        socket,
        Command::DenyMCPTool {
            session_id: session_id.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
            reason: "ticket-02 refusal probe".to_owned(),
        },
    )
    .await
}

/// Deny for session A — and a second granted call on another server —
/// both complete while session B's call hangs: no global lock across
/// the call bound.
#[tokio::test]
async fn deny_and_other_calls_proceed_while_one_call_hangs() {
    let h = TwoSession::start().await;

    // Session B's `hang` call goes in flight in the background.
    let hang_id = h.park(&h.session_b, "beta", "hang").await;
    let socket = h.socket.clone();
    let session_b = h.session_b.clone();
    let mut hung = tokio::spawn(async move { approve_call(&socket, &session_b, &hang_id).await });
    h.await_in_flight("hang").await;

    // Session A parks a call and denies it: the refusal must not wait
    // out the hung call.
    let deny_id = h.park(&h.session_a, "alpha", "echo").await;
    let (status, payload, message) =
        tokio::time::timeout(PROMPT, deny_call(&h.socket, &h.session_a, &deny_id))
            .await
            .expect("deny waited out the hung call");
    assert_eq!(status, 200, "deny grants: {message}");
    assert_eq!(payload["status"], "denied");

    // Another session's granted call completes while B still hangs.
    let echo_id = h.park(&h.session_a, "alpha", "echo").await;
    let (status, payload, message) =
        tokio::time::timeout(PROMPT, approve_call(&h.socket, &h.session_a, &echo_id))
            .await
            .expect("session A's call waited out session B's hung call");
    assert_eq!(status, 200, "second call grants: {message}");
    assert_eq!(payload["status"], "ok");

    // The hung call is still pending — nothing above resolved it.
    match tokio::time::timeout(Duration::from_secs(1), &mut hung).await {
        Err(_) => {}
        Ok(join) => {
            let (status, _, message) = join.unwrap();
            panic!("hung call resolved before its abort: {status} {message}");
        }
    }

    // Cleanup aborts the hung call (the next test owns the strict
    // assertions); here the abort must at least resolve it promptly.
    ok(
        &h.socket,
        Command::CancelTask {
            task_id: h.task_b.parse().unwrap(),
        },
    )
    .await;
    let (status, _, _) = tokio::time::timeout(PROMPT, async { hung.await.unwrap() })
        .await
        .expect("aborted hung call never resolved");
    assert_eq!(status, 400, "aborted call never reports success");

    h.gateway.shutdown().await;
}

/// Cancel for the hanging session's task aborts the in-flight call
/// promptly — far under the 60 s bound — with `stopped` +
/// `mcp_call_failed`, never success.
#[tokio::test]
async fn cancel_aborts_hung_call_promptly_as_failed_and_stopped() {
    let h = TwoSession::start().await;

    let hang_id = h.park(&h.session_b, "beta", "hang").await;
    let socket = h.socket.clone();
    let session_b = h.session_b.clone();
    let hung = tokio::spawn(async move { approve_call(&socket, &session_b, &hang_id).await });
    h.await_in_flight("hang").await;

    let start = Instant::now();
    let ack = ok(
        &h.socket,
        Command::CancelTask {
            task_id: h.task_b.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(ack["task"]["status"], "Cancelled");
    let (status, _, message) = tokio::time::timeout(PROMPT, async { hung.await.unwrap() })
        .await
        .expect("cancel did not abort the hung call promptly");
    let elapsed = start.elapsed();
    assert_eq!(status, 400, "aborted call reports failure: {message}");
    assert_eq!(code_of(&message), "mcp_call_failed", "{message}");
    assert!(
        elapsed < PROMPT,
        "abort took {elapsed:?}, not far under the 60 s bound"
    );

    // §19 at the gateway seam: the row falls back to `stopped` and the
    // dead child is reaped — retry needs a fresh approved launch, never
    // a blind replay into dead pipes. Even parking fails closed here:
    // the liveness re-check fires before any child I/O.
    assert_eq!(h.listed_status(&h.session_b, "beta").await, "stopped");
    let (status, _, message) = send(
        &h.socket,
        Command::CallMCPTool {
            session_id: h.session_b.parse().unwrap(),
            server_id: "beta".to_owned(),
            tool: "echo".to_owned(),
            arguments_json: json!({}),
        },
    )
    .await;
    assert_eq!(status, 400, "call into the reaped child must fail");
    assert_eq!(code_of(&message), "mcp_not_live", "{message}");

    h.gateway.shutdown().await;
}

/// A call that WOULD have succeeded (`slow` answers ok after 10 s)
/// still fails closed when its abort wins the race: the late answer
/// changes no state and reports nothing as success.
#[tokio::test]
async fn late_completion_of_aborted_call_reports_no_success() {
    let h = TwoSession::start().await;

    let slow_id = h.park(&h.session_b, "beta", "slow").await;
    let socket = h.socket.clone();
    let session_b = h.session_b.clone();
    let slow = tokio::spawn(async move { approve_call(&socket, &session_b, &slow_id).await });
    h.await_in_flight("slow").await;

    let start = Instant::now();
    ok(
        &h.socket,
        Command::CancelTask {
            task_id: h.task_b.parse().unwrap(),
        },
    )
    .await;
    let (status, _, message) = tokio::time::timeout(PROMPT, async { slow.await.unwrap() })
        .await
        .expect("aborted slow call never resolved");
    let elapsed = start.elapsed();
    assert_eq!(status, 400, "aborted call never reports success: {message}");
    assert_eq!(code_of(&message), "mcp_call_failed", "{message}");
    // The 10 s answer must not have arrived first: the abort won.
    assert!(
        elapsed < Duration::from_secs(10),
        "abort lost to the late answer ({elapsed:?})"
    );

    // The late answer lands nowhere: row `stopped`, child reaped, no
    // success receipt anywhere in the flow. Even parking fails closed.
    assert_eq!(h.listed_status(&h.session_b, "beta").await, "stopped");
    let (status, _, message) = send(
        &h.socket,
        Command::CallMCPTool {
            session_id: h.session_b.parse().unwrap(),
            server_id: "beta".to_owned(),
            tool: "echo".to_owned(),
            arguments_json: json!({}),
        },
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(code_of(&message), "mcp_not_live", "{message}");

    h.gateway.shutdown().await;
}
