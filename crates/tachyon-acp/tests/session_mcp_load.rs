//! TASK 01 load slice: `session/load` with `mcpServers` asserts the
//! pinned set read-only — match replays, mismatch fails typed with
//! zero frames, empty asserts nothing (not even a List call).

use std::time::Duration;

use serde_json::json;
use tachyon_protocol::CommandResult;

mod common;
use common::scripted::{Script, ScriptedGateway};
use common::{Adapter, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";
const PINNED_ROOT: &str = "/tmp";

fn load_line(id: u64, servers: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/load","params":{{"sessionId":"{SESSION_ID}","cwd":"{PINNED_ROOT}","mcpServers":{servers}}}}}"#
    )
}

fn get_session_ok() -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": SESSION_ID,
            "workspace_root": PINNED_ROOT,
            "turns": [],
        }),
    }
}

fn empty_script() -> Script {
    Script {
        get_tasks: vec![],
        subscribes: vec![],
        approves: vec![],
    }
}

fn server_entry(name: &str) -> String {
    format!(
        r#"{{"transport":{{"type":"stdio"}},"name":"{name}","command":"/usr/bin/srv","args":[],"env":[]}}"#
    )
}

/// Match: asserted set equals pins — replay proceeds to `{}`.
#[tokio::test]
async fn load_matching_set_replays() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(&dir, empty_script(), get_session_ok())
        .with_mcp_list(vec!["s1".to_owned()]);
    let mut adapter = Adapter::spawn(&dir);

    adapter
        .send(&load_line(1, &format!("[{}]", server_entry("s1"))))
        .await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(notifications.is_empty(), "empty history replays nothing");
    assert_eq!(parse_frame(&response_line)["result"], json!({}));
    assert_eq!(fixture.list_mcp_calls(), 1, "one read-only list");

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Mismatch (superset asserted): typed `mcp_set_mismatch` with zero
/// replay frames.
#[tokio::test]
async fn load_mismatched_set_fails_closed() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(&dir, empty_script(), get_session_ok())
        .with_mcp_list(vec!["s1".to_owned()]);
    let mut adapter = Adapter::spawn(&dir);

    adapter
        .send(&load_line(
            1,
            &format!("[{},{}]", server_entry("s1"), server_entry("s2")),
        ))
        .await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(notifications.is_empty(), "refusal replays zero frames");
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["data"], json!("mcp_set_mismatch"));

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Empty assert with pins present: replays without listing (no-regression
/// pin for old clients).
#[tokio::test]
async fn load_empty_assertion_skips_the_list() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(&dir, empty_script(), get_session_ok())
        .with_mcp_list(vec!["s1".to_owned()]);
    let mut adapter = Adapter::spawn(&dir);

    adapter.send(&load_line(1, "[]")).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(notifications.is_empty());
    assert_eq!(parse_frame(&response_line)["result"], json!({}));
    assert_eq!(
        fixture.list_mcp_calls(),
        0,
        "empty mcpServers never lists: load stays a pure replay"
    );

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Gateway failure mid-assertion: a refused `ListMCPServers` surfaces
/// typed (never proceeds, never hangs).
#[tokio::test]
async fn load_list_failure_is_typed() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(&dir, empty_script(), get_session_ok())
        .with_mcp_list_refused();
    let mut adapter = Adapter::spawn(&dir);

    adapter
        .send(&load_line(1, &format!("[{}]", server_entry("s1"))))
        .await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(notifications.is_empty(), "failure replays zero frames");
    assert!(parse_frame(&response_line)["error"].is_object());

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}
