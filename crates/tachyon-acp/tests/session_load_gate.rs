//! session/load ticket 02 (S1) against the scripted gateway fixture:
//! the ADR-0005:39 recorded-turn prompt gate — a load that recorded a
//! non-terminal turn refuses a later `session/prompt` typed `-32003`
//! BEFORE any `CreateTask`; a recorded turn that has since finished
//! releases the gate (fresh `GetTask`) and the prompt proceeds; a
//! gateway failure at the gate fails typed with zero creates (the gate
//! never guesses a release).

use std::time::Duration;

use serde_json::json;
use tachyon_protocol::CommandResult;

mod common;
use common::scripted::{
    Script, ScriptedGateway, Step, Subscription, status_payload, task_completed, task_status,
};
use common::{Adapter, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);

/// Any valid UUID string parses as a session id.
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";

/// The pinned root the load request must match (fixture payload).
const PINNED_ROOT: &str = "/tmp";

/// The recorded (still running) turn the gate must consult.
const RECORDED_TASK: &str = "01990f9e-7000-7000-8000-000000000007";

fn load_line(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/load","params":{{"sessionId":"{SESSION_ID}","cwd":"{PINNED_ROOT}"}}}}"#
    )
}

fn prompt_line(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{SESSION_ID}","prompt":[{{"type":"text","text":"Gate me."}}]}}}}"#
    )
}

/// A scripted `GetSession` whose LAST turn is still running — the
/// history `session/load` records for the gate.
fn get_session_recording() -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": SESSION_ID,
            "workspace_root": PINNED_ROOT,
            "turns": [{
                "turn_seq": 1,
                "task_id": RECORDED_TASK,
                "status": "Executing",
                "conversation": [{"speaker": "user", "content": "earlier work"}],
            }],
        }),
    }
}

/// THE block pin: load records the running turn, the prompt's gate
/// `GetTask` confirms it is still non-terminal, so the prompt answers
/// typed `-32003 turn_in_progress` and the fixture NEVER sees a
/// `CreateTask` (the overlap would be a second live turn — ADR-0005:39).
#[tokio::test]
async fn prompt_after_load_is_refused_while_the_recorded_turn_runs() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            // The gate's fresh GetTask: still executing.
            get_tasks: vec![task_status("Executing")],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_recording(),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1)).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert_eq!(notifications.len(), 1, "one replayed message");
    assert_eq!(parse_frame(&response_line)["result"], json!({}));

    adapter.send(&prompt_line(2)).await;
    let (updates, response_line) = adapter.read_until_response(json!(2), WAIT).await;
    assert!(updates.is_empty(), "a refused prompt streams nothing");
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32003, "reply: {response_line}");
    assert_eq!(response["error"]["data"], json!("turn_in_progress"));

    // THE zero-create pin: the refusal happened before any task work.
    assert_eq!(fixture.create_task_calls(), 0, "no CreateTask ever issued");
    assert_eq!(fixture.get_task_calls(), 1, "exactly the gate's check");

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// THE release pin: the recorded turn has since finished — the gate's
/// fresh `GetTask` shows terminal, the record clears, and the prompt
/// proceeds through the full pipeline to `end_turn` (the gate never
/// wedges a session whose recorded work is done).
#[tokio::test]
async fn prompt_after_load_proceeds_once_the_recorded_turn_is_terminal() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            // 1: the gate's check (recorded turn → terminal → release).
            // 2: the prompt's fresh-state read (non-terminal → StartRun).
            // 3: the settlement read after the journal below.
            get_tasks: vec![
                task_completed("recorded turn finished"),
                task_status("Executing"),
                task_completed("final answer"),
            ],
            subscribes: vec![Subscription {
                replay: vec![],
                post: vec![Step::Journal {
                    seq: 1,
                    kind: "status",
                    payload: status_payload("Completed"),
                }],
            }],
            approves: vec![],
        },
        get_session_recording(),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1)).await;
    let (_n, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert_eq!(parse_frame(&response_line)["result"], json!({}));

    adapter.send(&prompt_line(2)).await;
    let (_updates, response_line) = adapter.read_until_response(json!(2), WAIT).await;
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "released gate ⇒ the prompt runs: {response_line}"
    );

    // The gate consumed exactly one GetTask; the prompt then created
    // exactly one task (gate released, not bypassed).
    assert_eq!(fixture.get_task_calls(), 3, "gate + fresh + settlement");
    assert_eq!(fixture.create_task_calls(), 1, "one task after release");

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// THE fail-closed pin: a gateway error AT the gate (the fixture's
/// script-exhausted refusal stands in for any command failure) answers
/// typed and creates NOTHING — the gate never guesses a release.
#[tokio::test]
async fn gate_gettask_failure_fails_closed() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        // No GetTask answers: the gate's check hits the fixture's
        // script_exhausted refusal (a gateway-command failure).
        Script {
            get_tasks: vec![],
            subscribes: vec![],
            approves: vec![],
        },
        get_session_recording(),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1)).await;
    let (_n, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert_eq!(parse_frame(&response_line)["result"], json!({}));

    adapter.send(&prompt_line(2)).await;
    let (updates, response_line) = adapter.read_until_response(json!(2), WAIT).await;
    assert!(updates.is_empty(), "a failed gate streams nothing");
    let response = parse_frame(&response_line);
    assert!(
        response.get("result").is_none(),
        "a failed gate never answers success: {response_line}"
    );
    assert_eq!(response["error"]["data"], json!("script_exhausted"));
    assert_eq!(
        fixture.create_task_calls(),
        0,
        "a failed gate never creates a task"
    );

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}
