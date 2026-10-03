//! session/load ticket 01 (S2) against the scripted gateway fixture:
//! the replay contract (ADR-0005:29 + protocol "Loading Sessions") —
//! every recorded conversation entry streams as `session/update`
//! notifications in `turn_seq` order BEFORE the `{}` result; empty
//! history answers immediately with zero notifications; the typed
//! refusals (unknown session, workspace mismatch) answer inline with
//! ZERO frames replayed.

use std::time::Duration;

use serde_json::{Value, json};
use tachyon_protocol::CommandResult;

mod common;
use common::scripted::{Script, ScriptedGateway};
use common::{Adapter, AdapterFrame, classify, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);

/// Any valid UUID string parses as a session id.
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";

/// The pinned root the load request must match (fixture payload).
const PINNED_ROOT: &str = "/tmp";

fn load_line(id: u64, cwd: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/load","params":{{"sessionId":"{SESSION_ID}","cwd":"{cwd}"}}}}"#
    )
}

/// A scripted `GetSession` success carrying `turns` verbatim — the
/// history the replay must stream in order.
fn get_session_ok(turns: Value) -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": SESSION_ID,
            "workspace_root": PINNED_ROOT,
            "turns": turns,
        }),
    }
}

/// The three-turn history: user/agent interleaved across turns, plus a
/// turn with an empty conversation (no frames for it).
fn three_turn_history() -> Value {
    json!([
        {
            "turn_seq": 1,
            "task_id": "01990f9e-7000-7000-8000-000000000001",
            "status": "Completed",
            "conversation": [
                {"speaker": "user", "content": "first question"},
                {"speaker": "agent", "content": "first answer"},
            ],
        },
        {
            "turn_seq": 2,
            "task_id": "01990f9e-7000-7000-8000-000000000002",
            "status": "Completed",
            "conversation": [
                {"speaker": "user", "content": "second question"},
                {"speaker": "agent", "content": "second answer"},
            ],
        },
        {
            "turn_seq": 3,
            "task_id": "01990f9e-7000-7000-8000-000000000003",
            "status": "Failed",
            "conversation": [],
        },
    ])
}

/// Extracts `(sessionUpdate kind, text)` from raw notification lines.
fn replayed(lines: &[String]) -> Vec<(String, String)> {
    lines
        .iter()
        .map(|line| {
            let AdapterFrame::Notification(frame) = classify(line) else {
                panic!("expected a replay notification: {line}");
            };
            assert_eq!(frame["method"], "session/update", "line: {line}");
            let update = &frame["params"]["update"];
            (
                update["sessionUpdate"].as_str().expect("kind").to_owned(),
                update["content"]["text"].as_str().expect("text").to_owned(),
            )
        })
        .collect()
}

/// THE replay-before-response pin: all 5 conversation entries stream in
/// `turn_seq` / in-turn order as the right chunk kinds, and every
/// notification lands BEFORE the `{}` result frame (`read_until_response`
/// only stops at the response, so ordering is asserted by construction).
#[tokio::test]
async fn session_load_replays_history_in_turn_order_then_responds() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_ok(three_turn_history()),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1, PINNED_ROOT)).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert_eq!(
        replayed(&notifications),
        vec![
            ("user_message_chunk".to_owned(), "first question".to_owned()),
            ("agent_message_chunk".to_owned(), "first answer".to_owned()),
            (
                "user_message_chunk".to_owned(),
                "second question".to_owned()
            ),
            ("agent_message_chunk".to_owned(), "second answer".to_owned()),
        ],
        "the replay is complete and in turn order"
    );
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"],
        json!({}),
        "LoadSessionResponse is an empty result: {response_line}"
    );
    // No messageId: optional in schema, nothing durable maps to one.
    for line in &notifications {
        let AdapterFrame::Notification(frame) = classify(line) else {
            panic!("expected a replay notification: {line}");
        };
        assert!(
            frame["params"]["update"].get("messageId").is_none(),
            "messageId stays absent: {line}"
        );
    }

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Empty history ⇒ zero notifications, straight `{}` — load is
/// repeatable and creates nothing (ADR-0005:40).
#[tokio::test]
async fn session_load_with_empty_history_answers_immediately() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_ok(json!([])),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1, PINNED_ROOT)).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert!(
        notifications.is_empty(),
        "an empty history replays nothing: {notifications:?}"
    );
    let response = parse_frame(&response_line);
    assert_eq!(response["result"], json!({}));

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Unknown session ⇒ the gateway's typed `-32002 unknown_session`
/// (identity check first, ADR-0005:40: loading never creates), and
/// ZERO replay frames precede the error.
#[tokio::test]
async fn session_load_unknown_session_is_typed() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        CommandResult::Err {
            code: "unknown_session".to_owned(),
            message: format!("no session {SESSION_ID}"),
        },
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1, PINNED_ROOT)).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert!(
        notifications.is_empty(),
        "a refused load replays nothing: {notifications:?}"
    );
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32002);
    assert_eq!(response["error"]["data"], json!("unknown_session"));

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// cwd ≠ pinned workspace root ⇒ typed `-32602 workspace_mismatch`
/// (load never rebinds a pinned root, ADR-0005:40), zero replay
/// frames before the error.
#[tokio::test]
async fn session_load_workspace_mismatch_is_typed() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_ok(three_turn_history()),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1, "/tmp/some/other/root")).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert!(
        notifications.is_empty(),
        "a mismatched load replays nothing: {notifications:?}"
    );
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32602);
    assert_eq!(response["error"]["data"], json!("workspace_mismatch"));

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Frame hygiene: every line the load arm writes is a valid ACP frame
/// (notifications are id-less; the response carries the id) — asserted
/// implicitly by `classify` inside the helpers above, kept as an
/// explicit smoke on the notification shape.
#[tokio::test]
async fn load_notifications_are_idless_session_updates() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_ok(three_turn_history()),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(7, PINNED_ROOT)).await;
    let (notifications, response_line) = adapter.read_until_response(json!(7), WAIT).await;
    for line in &notifications {
        match classify(line) {
            AdapterFrame::Notification(frame) => {
                assert_eq!(frame["method"], "session/update");
                assert!(
                    frame.get("id").is_none(),
                    "notifications carry no id: {line}"
                );
            }
            other => panic!("expected a notification, got {other:?}: {line}"),
        }
    }
    let response = parse_frame(&response_line);
    assert_eq!(response["id"], json!(7));

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}
