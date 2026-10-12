//! TASK 01 S2 (gateway-command observation): the MCP-at-setup exchange
//! against the scripted gateway — `RegisterMCPServers` carries the
//! secret-pinned set, `ApproveMCPServers`/`DenyMCPServers` carry the
//! exact parked approval identity, and secret values cross as
//! broker-bound entries.

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

mod common;
use common::parse_frame;
use common::scripted::{SCRIPT_MCP_APPROVAL_ID, SCRIPT_SESSION_ID, Script, ScriptedGateway};

const WAIT: Duration = Duration::from_secs(30);

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

fn new_line(id: u32, ws: &Path) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/new","params":{{"cwd":{},"mcpServers":[{{"transport":{{"type":"stdio"}},"name":"s1","command":"/usr/bin/srv","args":["--flag"],"env":[{{"name":"MCP_TOKEN","value":"raw"}}]}}]}}}}"#,
        json!(ws.display().to_string()),
    )
}

fn answer_line(id: &Value, outcome: &str, option: Option<&str>) -> String {
    let result = match option {
        Some(option_id) => json!({"outcome": outcome, "optionId": option_id}),
        None => json!({"outcome": outcome}),
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string()
}

fn empty_script() -> Script {
    Script {
        get_tasks: vec![],
        subscribes: vec![],
        approves: vec![],
    }
}

/// Reject path observed: the register carries the secret-pinned set,
/// the deny carries the exact parked identity plus a client-naming
/// reason, and the session answers typed.
#[tokio::test]
async fn setup_reject_observed_with_exact_identity() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let fixture = ScriptedGateway::start(&dir, empty_script());
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(1, &ws)).await;
    let ask_line = client.read_line("the session/request_mcp_approval").await;
    let ask: Value = serde_json::from_str(&ask_line).expect("ask is JSON");
    assert_eq!(ask["params"]["approvalId"], json!(SCRIPT_MCP_APPROVAL_ID));

    client
        .send(&answer_line(&json!(-1), "selected", Some("reject_once")))
        .await;
    let (_, session_line) = client.read_until_response(json!(1)).await;
    let session = parse_frame(&session_line);
    assert_eq!(session["error"]["data"], json!("mcp_approval_denied"));

    let registers = fixture.mcp_registers_seen();
    assert_eq!(registers.len(), 1, "exactly one register");
    assert_eq!(registers[0].0, SCRIPT_SESSION_ID);
    let pinned: Value = serde_json::from_str(&registers[0].1).expect("pins are JSON");
    assert_eq!(pinned[0]["server_id"], json!("s1"));
    assert_eq!(pinned[0]["args"][0]["secret"], json!(true));
    assert_eq!(pinned[0]["env"][0]["secret"], json!(true));

    let denies = fixture.mcp_denies_seen();
    assert_eq!(denies.len(), 1, "exactly one deny");
    assert_eq!(denies[0].0, SCRIPT_SESSION_ID);
    assert_eq!(denies[0].1, SCRIPT_MCP_APPROVAL_ID);
    assert!(
        denies[0].2.contains("setup client"),
        "deny reason names the client: {:?}",
        denies[0]
    );
    assert!(fixture.mcp_approves_seen().is_empty());
    client.finish().await;
}

/// Allow path observed: the approve carries the exact parked identity
/// and the session answers its id.
#[tokio::test]
async fn setup_allow_observed_with_exact_identity() {
    let dir = common::test_dir();
    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    let fixture = ScriptedGateway::start(&dir, empty_script());
    let mut client = ServeClient::spawn(&dir);

    client.send(&new_line(1, &ws)).await;
    let _ = client.read_line("the session/request_mcp_approval").await;
    client
        .send(&answer_line(&json!(-1), "selected", Some("allow_once")))
        .await;
    let (_, session_line) = client.read_until_response(json!(1)).await;
    let session = parse_frame(&session_line);
    assert_eq!(
        session["result"]["sessionId"],
        json!(SCRIPT_SESSION_ID),
        "identity mapping holds: {session_line}"
    );

    let approves = fixture.mcp_approves_seen();
    assert_eq!(approves.len(), 1, "exactly one approve");
    assert_eq!(approves[0].0, SCRIPT_SESSION_ID);
    assert_eq!(approves[0].1, SCRIPT_MCP_APPROVAL_ID);
    assert!(fixture.mcp_denies_seen().is_empty());
    client.finish().await;
}
