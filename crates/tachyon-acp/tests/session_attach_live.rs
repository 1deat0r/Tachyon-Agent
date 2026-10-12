//! Attach ticket 02 against the scripted gateway fixture: the read-only
//! live tail — replay-then-tail chunk continuity, a parked approval
//! announced but never answered (no `session/request_permission`, no slot,
//! no decision), the terminal `{status}` result, one bounded re-subscribe
//! before typed `resync_required`, and terminal refusal before any
//! subscription.

use std::time::Duration;

use serde_json::{Value, json};
use tachyon_protocol::CommandResult;

mod common;
use common::scripted::{
    ReplayRow, Script, ScriptedGateway, Step, Subscription, agent_payload, status_payload,
    task_completed,
};
use common::{Adapter, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);

const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";
const LIVE_TASK: &str = "01990f9e-7000-7000-8000-000000000002";
const DONE_TASK: &str = "01990f9e-7000-7000-8000-000000000001";
const APPROVAL_ID: &str = "01990f9e-5555-7000-8000-000000000000";

fn attach_line(id: u64, extra: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/attach","params":{{"sessionId":"{SESSION_ID}"{extra}}}}}"#
    )
}

fn approval_request_payload() -> Value {
    json!({"t": "ApprovalRequest", "v": {"request": {"id": APPROVAL_ID, "summary": "Delete everything"}}})
}

/// A scripted `GetSession` with one still-running turn: the only attachable
/// target (default selection needs no `taskId`).
fn get_session_live() -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": SESSION_ID,
            "workspace_root": "/tmp",
            "turns": [{
                "turn_seq": 1,
                "task_id": LIVE_TASK,
                "status": "Executing",
                "conversation": [{"speaker": "user", "content": "earlier work"}],
            }],
        }),
    }
}

/// A scripted `GetSession` whose latest turn finished: attach refuses typed
/// (replay is `session/load`'s job).
fn get_session_terminal() -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": SESSION_ID,
            "workspace_root": "/tmp",
            "turns": [{
                "turn_seq": 1,
                "task_id": DONE_TASK,
                "status": "Cancelled",
                "conversation": [{"speaker": "user", "content": "earlier work"}],
            }],
        }),
    }
}

fn update_of(line: &str) -> Value {
    let frame: Value = serde_json::from_str(line).expect("stdout is JSON");
    assert_eq!(
        frame["method"], "session/update",
        "attach streams only session/update: {line}"
    );
    frame["params"]["update"].clone()
}

/// The happy path end to end: replay chunk → parked approval announced
/// read-only → live chunk → settlement read → final status notification +
/// `{status}` result. The approval is NEVER answered through the attach:
/// no `session/request_permission` frame, no decision, the stream just
/// keeps tailing past the park.
#[tokio::test]
async fn attach_tails_replay_then_live_and_reports_the_terminal_status() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![task_completed("final answer")],
        subscribes: vec![Subscription {
            replay: vec![ReplayRow {
                seq: 1,
                kind: "agent_message",
                payload: agent_payload("hello"),
            }],
            post: vec![
                Step::Journal {
                    seq: 2,
                    kind: "approval_request",
                    payload: approval_request_payload(),
                },
                Step::Journal {
                    seq: 3,
                    kind: "agent_message",
                    payload: agent_payload("world"),
                },
                Step::Journal {
                    seq: 4,
                    kind: "status",
                    payload: status_payload("Completed"),
                },
            ],
        }],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start_with_get_session(&dir, script, get_session_live());

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&attach_line(1, r#","from_seq":5"#)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert_eq!(
        updates.len(),
        4,
        "replay chunk + park + live chunk + final: {updates:?}"
    );
    let parsed: Vec<Value> = updates
        .iter()
        .map(|line| serde_json::from_str(line).expect("stdout is JSON"))
        .collect();
    assert_eq!(
        parsed[0]["params"]["update"]["sessionUpdate"],
        "agent_message_chunk"
    );
    assert_eq!(parsed[0]["params"]["update"]["content"]["text"], "hello");
    let park = update_of(&updates[1]);
    assert_eq!(park["sessionUpdate"], "tool_call");
    assert_eq!(park["toolCallId"], APPROVAL_ID);
    assert_eq!(parsed[2]["params"]["update"]["content"]["text"], "world");
    let final_update = update_of(&updates[3]);
    assert_eq!(final_update["sessionUpdate"], "task_status");
    assert_eq!(final_update["status"], "Completed");

    let response = parse_frame(&response_line);
    assert_eq!(response["result"], json!({"status": "Completed"}));

    assert_eq!(
        fixture.subscribe_cursors(),
        vec![5],
        "from_seq rides the Subscribe cursor verbatim"
    );
    assert_eq!(
        fixture.get_task_calls(),
        1,
        "one settlement read serves the signal"
    );
    adapter.close_stdin();
    let (_, _, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "adapter exits cleanly after EOF");
}

/// The park is observation-only: covered by the happy path above (the
/// `tool_call` announcement with no `session/request_permission` frame
/// anywhere in the stream). This test pins the negative half directly at
/// the frame level.
#[tokio::test]
async fn attach_never_solicits_an_approval_answer() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![task_completed("final answer")],
        subscribes: vec![Subscription {
            replay: vec![ReplayRow {
                seq: 1,
                kind: "approval_request",
                payload: approval_request_payload(),
            }],
            post: vec![Step::Journal {
                seq: 2,
                kind: "status",
                payload: status_payload("Completed"),
            }],
        }],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start_with_get_session(&dir, script, get_session_live());

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&attach_line(1, "")).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    for line in &updates {
        let frame: Value = serde_json::from_str(line).expect("stdout is JSON");
        assert_ne!(
            frame.get("method"),
            Some(&json!("session/request_permission")),
            "attach never asks: {line}"
        );
    }
    let parked: Vec<&String> = updates
        .iter()
        .filter(|line| update_of(line)["sessionUpdate"] == "tool_call")
        .collect();
    assert_eq!(
        parked.len(),
        1,
        "the park surfaces exactly once: {updates:?}"
    );
    assert_eq!(
        parse_frame(&response_line)["result"],
        json!({"status": "Completed"})
    );
    assert!(
        fixture.approvals_seen().is_empty(),
        "no Approve ever issued through the attach"
    );
    adapter.close_stdin();
    let (_, _, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "adapter exits cleanly after EOF");
}

/// Persistent overflow fails typed `-32004 resync_required` after exactly
/// one bounded re-subscribe at the gateway cursor — never a silent gap,
/// never an unbounded loop.
#[tokio::test]
async fn persistent_resync_fails_the_attach_typed() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![],
        subscribes: vec![
            Subscription {
                replay: vec![],
                post: vec![Step::Resync { after_seq: 1 }],
            },
            Subscription {
                replay: vec![],
                post: vec![Step::Resync { after_seq: 1 }],
            },
        ],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start_with_get_session(&dir, script, get_session_live());

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&attach_line(1, "")).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert!(
        updates.is_empty(),
        "an overflowed attach streams nothing usable"
    );
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32004, "reply: {response_line}");
    assert_eq!(response["error"]["data"], json!("resync_required"));
    assert_eq!(
        fixture.subscribe_cursors(),
        vec![0, 1],
        "one bounded re-subscribe at the gateway cursor"
    );
    adapter.close_stdin();
    let (_, _, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "adapter exits cleanly after EOF");
}

/// A terminal target refuses typed BEFORE any subscription: history is
/// `session/load`'s job, attach is live-tail only.
#[tokio::test]
async fn attach_to_a_terminal_turn_is_refused_before_any_subscription() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_terminal(),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&attach_line(1, "")).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    assert!(updates.is_empty(), "a refused attach streams nothing");
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32602, "reply: {response_line}");
    assert_eq!(response["error"]["data"], json!("attach_target_terminal"));
    assert!(
        fixture.subscribe_cursors().is_empty(),
        "refusal happens before any Subscribe"
    );
    assert_eq!(fixture.get_task_calls(), 0, "no settlement read either");
    adapter.close_stdin();
    let (_, _, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "adapter exits cleanly after EOF");
}
