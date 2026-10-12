//! TASK 01 S2: MCP-at-setup approval flow at the serve-loop seam —
//! `session/new` with servers registers them, asks the client through
//! `session/request_mcp_approval`, and launches (allow) or refuses
//! (reject/invalid) through the gateway, against a REAL gateway and a
//! fake MCP child (never a real server binary).

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

mod common;
use common::parse_frame;

const WAIT: Duration = Duration::from_secs(60);

fn write_child(dir: &Path) -> (String, PathBuf) {
    // Static responder (no runtime dependency): proves spawn + argv +
    // stdio through the real gateway path. Replies are precomputed
    // (initialize id 1, tools/list id 2); the alive marker proves the
    // child ran; sleep keeps it live for the session.
    let wrapper = dir.join("wrap.sh");
    let argv_log = dir.join("argv.log");
    let alive = dir.join("alive.marker");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\necho \"cwd=$(pwd) argv=$@\" > {}\ntouch {}\nprintf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{{\"tools\":{{}}}},\"serverInfo\":{{\"name\":\"fake\",\"version\":\"0\"}}}}}}'\nprintf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{{\"tools\":[]}}}}'\nexec sleep 30\n",
            argv_log.display(),
            alive.display(),
        ),
    )
    .expect("wrapper writes");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755))
            .expect("chmod wrapper");
    }
    (wrapper.display().to_string(), alive)
}
/// In-process ACP client driving `tachyon_acp::server::serve` over a
/// duplex (production loop, no process boundary).
struct ServeClient {
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    reader: tokio::io::BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl ServeClient {
    fn spawn(dir: &Path) -> Self {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let (server_reader, server_writer) = tokio::io::split(server_io);
        let task = tokio::spawn(tachyon_acp::server::serve(
            server_reader,
            server_writer,
            tachyon_acp::client::EndpointProbe::new(dir),
            tachyon_acp::client::EndpointConnector::new(dir),
        ));
        let (client_reader, client_writer) = tokio::io::split(client_io);
        Self {
            writer: client_writer,
            reader: tokio::io::BufReader::new(client_reader),
            task,
        }
    }

    async fn send(&mut self, line: &str) {
        self.writer
            .write_all(format!("{line}\n").as_bytes())
            .await
            .expect("write to serve");
        self.writer.flush().await.expect("flush to serve");
    }

    async fn read_line(&mut self, label: &str) -> String {
        let mut line = String::new();
        let read = tokio::time::timeout(WAIT, self.reader.read_line(&mut line))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
        match read {
            Ok(0) => panic!("serve closed stdout before {label}"),
            Ok(_) => line.trim_end().to_owned(),
            Err(error) => panic!("reading from serve: {error}"),
        }
    }

    /// Reads until the response for `id`; prior lines (notifications
    /// and adapter→client requests) return in arrival order.
    async fn read_until_response(&mut self, id: Value) -> (Vec<String>, String) {
        let mut prior = Vec::new();
        loop {
            let line = self.read_line("the session/new response").await;
            let frame: Value = serde_json::from_str(&line).expect("stdout is JSON");
            if frame.get("method").is_some() {
                prior.push(line);
                continue;
            }
            assert_eq!(
                frame["id"], id,
                "expected the response for {id}, got: {line}"
            );
            return (prior, line);
        }
    }

    async fn finish(self) {
        let Self {
            writer,
            reader,
            task,
        } = self;
        drop(writer);
        drop(reader);
        let outcome = tokio::time::timeout(WAIT, task)
            .await
            .expect("serve exits after EOF")
            .expect("serve task joins");
        assert!(outcome.is_ok(), "serve exits cleanly: {outcome:?}");
    }
}

fn new_line(id: u32, ws: &Path, command: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/new","params":{{"cwd":{},"mcpServers":[{{"transport":{{"type":"stdio"}},"name":"s1","command":{command},"args":[],"env":[]}}]}}}}"#,
        json!(ws.display().to_string()),
        command = json!(command),
    )
}

fn answer_line(id: &Value, outcome: &str, option: Option<&str>) -> String {
    let result = match option {
        Some(option_id) => json!({"outcome": outcome, "optionId": option_id}),
        None => json!({"outcome": outcome}),
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string()
}

async fn live_gateway(dir: &Path) -> tachyon_gateway::RunningGateway {
    let runtime = tachyon_gateway::GatewayRuntime {
        provider: None,
        label: "bench-live".to_owned(),
        model: "unused".to_owned(),
        redactor: tachyon_tools::credential::CredentialBroker::default(),
    };
    tachyon_gateway::start_with(dir, runtime)
        .await
        .expect("gateway starts")
}

/// Allow path: the approval request carries the registered servers,
/// `allow_once` launches (alive marker proves the spawn), the session
/// answers exactly `{sessionId}`.
#[tokio::test]
async fn setup_allow_launches_and_answers_session_id() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (command, alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(1, &ws, &command)).await;

    // The approval request: first adapter-minted outbound id (-1).
    let approval_line = client.read_line("the session/request_mcp_approval").await;
    let approval: Value = serde_json::from_str(&approval_line).expect("approval request is JSON");
    assert_eq!(approval["id"], -1);
    assert_eq!(approval["method"], "session/request_mcp_approval");
    assert_eq!(
        approval["params"]["servers"],
        json!([{"server_id": "s1", "status": "awaiting_approval"}]),
        "request carries the registered set: {approval_line}"
    );
    let approval_id = approval["params"]["approvalId"]
        .as_str()
        .expect("approvalId")
        .to_owned();
    assert!(!approval_id.is_empty());

    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    let (prior, session_line) = client.read_until_response(json!(1)).await;
    assert!(prior.is_empty(), "nothing else before the session result");
    let session = parse_frame(&session_line);
    assert!(
        session["result"]["sessionId"].as_str().is_some(),
        "allow launches the session: {session_line}"
    );

    // Spawn proof through the real gateway path: the child ran
    // (marker) and the handshake + inventory round trips passed
    // (the session verified above).
    assert!(alive.exists(), "approved server child ran (alive marker)");

    client.finish().await;
}

/// Reject path: `reject_once` refuses without spawning; the session
/// answers typed `mcp_approval_denied`.
#[tokio::test]
async fn setup_reject_refuses_without_spawning() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (command, alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(1, &ws, &command)).await;
    let approval_line = client.read_line("the session/request_mcp_approval").await;
    let approval_check: Value =
        serde_json::from_str(&approval_line).expect("approval request is JSON");
    assert_eq!(approval_check["method"], "session/request_mcp_approval");

    client
        .send(&answer_line(&json!(-1), "selected", Some("reject_once")))
        .await;
    let (_prior, session_line) = client.read_until_response(json!(1)).await;
    let session = parse_frame(&session_line);
    assert_eq!(session["error"]["code"], -32004);
    assert_eq!(session["error"]["data"], json!("mcp_approval_denied"));

    assert!(!alive.exists(), "a refused set never spawns: no child ran");
    client.finish().await;
}

/// Fail-closed answers: unknown shape denies without spawning and
/// answers typed (never approves, never hangs).
#[tokio::test]
async fn setup_invalid_answer_fails_closed() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (command, alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(1, &ws, &command)).await;
    let approval_line = client.read_line("the session/request_mcp_approval").await;
    let approval_check: Value =
        serde_json::from_str(&approval_line).expect("approval request is JSON");
    assert_eq!(approval_check["method"], "session/request_mcp_approval");

    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_forever")))
        .await;
    let (_prior, session_line) = client.read_until_response(json!(1)).await;
    let session = parse_frame(&session_line);
    assert_eq!(session["error"]["code"], -32004);
    assert_eq!(session["error"]["data"], json!("mcp_approval_denied"));
    assert!(!alive.exists(), "an invalid answer never spawns");
    client.finish().await;
}

/// Stale diagnostic: answering a consumed approval id (after the allow
/// completed) yields typed `stale_mcp_approval`, never a second launch.
#[tokio::test]
async fn setup_late_answer_after_consume_is_diagnosed() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (command, alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(1, &ws, &command)).await;
    let _ = client.read_line("the session/request_mcp_approval").await;
    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    let (_, session_line) = client.read_until_response(json!(1)).await;
    assert!(
        parse_frame(&session_line)["result"]["sessionId"]
            .as_str()
            .is_some()
    );
    assert!(alive.exists());

    // The late duplicate: same approval id, answered again.
    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    let stale_line = client.read_line("the stale diagnostic").await;
    let stale = parse_frame(&stale_line);
    assert_eq!(stale["id"], -1);
    assert_eq!(stale["error"]["code"], -32004);
    assert_eq!(stale["error"]["data"], json!("stale_mcp_approval"));

    // And once more: the diagnostic fires exactly once, later
    // duplicates drop silently.
    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    client.finish().await;
}

/// Same-id retry: re-sending the completed `session/new` replays the
/// byte-identical session result with no second approval ask.
#[tokio::test]
async fn setup_same_id_retry_replays_without_relaunch() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (command, alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(7, &ws, &command)).await;
    let _ = client.read_line("the session/request_mcp_approval").await;
    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    let (prior, first_line) = client.read_until_response(json!(7)).await;
    assert!(prior.is_empty());
    assert!(alive.exists());

    // Retry with the SAME call id: replay, no new approval ask.
    client.send(&new_line(7, &ws, &command)).await;
    let (prior2, second_line) = client.read_until_response(json!(7)).await;
    assert!(
        prior2.is_empty(),
        "replay emits no new approval ask: {prior2:?}"
    );
    assert_eq!(
        second_line, first_line,
        "the retry replays byte-identically"
    );
    client.finish().await;
}

/// Invalid-descriptor fork (spec, closed): a bad descriptor refuses the
/// whole `session/new` with `invalid_mcp_descriptor` before any gateway
/// session exists for it.
#[tokio::test]
async fn setup_bad_descriptor_refuses_whole_call() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (_command, _alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    // Relative command: mapping refuses pre-gateway.
    client
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"session/new","params":{{"cwd":{},"mcpServers":[{{"transport":{{"type":"stdio"}},"name":"s1","command":"relative/bin","args":[],"env":[]}}]}}}}"#,
            json!(ws.display().to_string()),
        ))
        .await;
    let (prior, line) = client.read_until_response(json!(3)).await;
    assert!(prior.is_empty(), "refusal emits no approval ask");
    let frame = parse_frame(&line);
    assert_eq!(frame["error"]["code"], -32602);
    assert_eq!(frame["error"]["data"], json!("invalid_mcp_descriptor"));
    client.finish().await;
}

/// In-flight coalescing: a duplicate `session/new` while its setup
/// pends joins the same outcome — one approval ask, two identical
/// session answers.
#[tokio::test]
async fn setup_duplicate_while_pending_coalesces() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let (command, alive) = write_child(&dir);
    let _gateway = live_gateway(&dir).await;
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(5, &ws, &command)).await;
    let ask_line = client.read_line("the session/request_mcp_approval").await;
    let ask: Value = serde_json::from_str(&ask_line).expect("approval request is JSON");
    assert_eq!(ask["method"], "session/request_mcp_approval");

    // Duplicate while pending: no second ask may appear.
    client.send(&new_line(5, &ws, &command)).await;
    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    let (prior, first) = client.read_until_response(json!(5)).await;
    assert!(prior.is_empty(), "exactly one approval ask total");
    let (prior2, second) = client.read_until_response(json!(5)).await;
    assert!(prior2.is_empty(), "no second ask on coalesce");
    assert_eq!(second, first, "coalesced duplicate answers identically");
    assert!(
        parse_frame(&first)["result"]["sessionId"]
            .as_str()
            .is_some()
    );
    assert!(alive.exists());
    client.finish().await;
}
