//! session/load ticket 02 (Phase 7 rework) against the scripted gateway
//! fixture: the STATELESS recorded-turn gate (ADR-0005:39) — a prompt
//! whose session's LAST recorded turn is still non-terminal is refused
//! typed `-32003` BEFORE any `CreateTask`, read from the `GetSession`
//! the prompt pipeline already issues (zero extra gateway calls). The
//! gate holds no memory: it works after `session/load`, WITHOUT any
//! load (post-restart re-prompt — the Phase 7 finding), and releases
//! the moment gateway truth turns terminal (e.g. after
//! `session/cancel` stops the turn).

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

fn cancel_line(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/cancel","params":{{"sessionId":"{SESSION_ID}"}}}}"#
    )
}

/// A scripted `GetSession` whose LAST turn is still running — the
/// gateway truth the stateless gate refuses on.
fn get_session_running() -> CommandResult {
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

/// A scripted `GetSession` whose LAST turn finished — the gateway
/// truth the gate releases on.
fn get_session_terminal() -> CommandResult {
    CommandResult::Ok {
        payload: json!({
            "session_id": SESSION_ID,
            "workspace_root": PINNED_ROOT,
            "turns": [{
                "turn_seq": 1,
                "task_id": RECORDED_TASK,
                "status": "Cancelled",
                "conversation": [{"speaker": "user", "content": "earlier work"}],
            }],
        }),
    }
}

/// THE stateless block pin (and the Phase 7 regression): a prompt with
/// NO preceding `session/load` in this process — the post-restart
/// re-prompt — is still refused typed `-32003 turn_in_progress` and
/// the fixture NEVER sees a `CreateTask` (a second live turn for one
/// session is exactly what ADR-0005:39 forbids; the gateway has no
/// overlap guard).
#[tokio::test]
async fn prompt_without_any_load_is_refused_while_the_recorded_turn_runs() {
    let dir = test_dir();
    let fixture =
        ScriptedGateway::start_with_get_session(&dir, empty_script(), get_session_running());

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&prompt_line(1)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(updates.is_empty(), "a refused prompt streams nothing");
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["code"], -32003, "reply: {response_line}");
    assert_eq!(response["error"]["data"], json!("turn_in_progress"));

    assert_eq!(fixture.create_task_calls(), 0, "no CreateTask ever issued");
    assert_eq!(
        fixture.get_task_calls(),
        0,
        "the gate reads the pipeline's own GetSession — no GetTask"
    );

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Load then prompt (the ticket's original shape): the replay runs,
/// the prompt is still refused on the same running gateway truth.
#[tokio::test]
async fn prompt_after_load_is_refused_while_the_recorded_turn_runs() {
    let dir = test_dir();
    let fixture =
        ScriptedGateway::start_with_get_session(&dir, empty_script(), get_session_running());

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

    assert_eq!(fixture.create_task_calls(), 0, "no CreateTask ever issued");
    assert_eq!(
        fixture.get_task_calls(),
        0,
        "no GetTask — the gate is the GetSession"
    );

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// THE release pin: gateway truth says the recorded turn FINISHED —
/// the gate releases and the prompt runs the full pipeline to
/// `end_turn` (the gate never wedges a session whose recorded work is
/// done).
#[tokio::test]
async fn prompt_after_load_proceeds_once_the_recorded_turn_is_terminal() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session(
        &dir,
        Script {
            // The prompt's own reads: fresh-state, then settlement
            // after the journal below. (No gate GetTask exists.)
            get_tasks: vec![task_status("Executing"), task_completed("final answer")],
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
        get_session_terminal(),
    );

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&load_line(1)).await;
    let (notifications, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert_eq!(
        notifications.len(),
        1,
        "the terminal turn's history still replays"
    );
    assert_eq!(parse_frame(&response_line)["result"], json!({}));

    adapter.send(&prompt_line(2)).await;
    let (_updates, response_line) = adapter.read_until_response(json!(2), WAIT).await;
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "terminal recorded turn ⇒ the prompt runs: {response_line}"
    );
    assert_eq!(
        fixture.get_task_calls(),
        2,
        "fresh + settlement only — no gate GetTask exists"
    );
    assert_eq!(fixture.create_task_calls(), 1, "one task after release");

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// THE fail-closed pin: the reconciliation itself fails (the gateway
/// refuses the `GetSession` the gate needs — scripted as the second
/// call, right after a successful load) ⇒ the prompt answers typed and
/// creates NOTHING — the stateless gate never guesses a release.
#[tokio::test]
async fn prompt_gate_fails_closed_when_reconciliation_fails() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session_sequence(
        &dir,
        empty_script(),
        vec![
            get_session_running(), // load: replay succeeds
            CommandResult::Err {
                code: "script_exhausted".to_owned(),
                message: "gateway refused the reconciliation read".to_owned(),
            }, // prompt: reconciliation fails ⇒ fail closed
        ],
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
        "a failed reconciliation never answers success: {response_line}"
    );
    assert_eq!(response["error"]["data"], json!("script_exhausted"));
    assert_eq!(
        fixture.create_task_calls(),
        0,
        "a failed reconciliation never creates a task"
    );

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// THE cancel-release pin (ADR-0005:41): with no local turn, the
/// cancel derives its target from ITS OWN `GetSession` — the fixture
/// observes exactly ONE `CancelTask` carrying the recorded id — and
/// once gateway truth flips terminal (the next `GetSession` in the
/// script), the stateless gate is released and the prompt runs to
/// `end_turn`. Nothing is remembered, nothing needs clearing.
#[tokio::test]
async fn prompt_gate_releases_after_cancel() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start_with_get_session_sequence(
        &dir,
        Script {
            // The prompt's own reads after the release.
            get_tasks: vec![task_status("Executing"), task_completed("after cancel")],
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
        vec![
            get_session_running(),  // cancel's GetSession: derive the target
            get_session_terminal(), // prompt's GetSession: released
        ],
    );

    let mut adapter = Adapter::spawn(&dir);

    // Stop the recorded turn: no local turn, so the cancel falls back
    // to the turn derived from its own GetSession.
    adapter.send(&cancel_line(1)).await;
    let (_n, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert_eq!(parse_frame(&response_line)["result"], json!({}));
    assert_eq!(
        fixture.cancels_seen(),
        vec![RECORDED_TASK.to_owned()],
        "the cancel stopped exactly the recorded turn"
    );

    // Gateway truth is now terminal ⇒ the stateless gate releases.
    adapter.send(&prompt_line(2)).await;
    let (_updates, response_line) = adapter.read_until_response(json!(2), WAIT).await;
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "released gate ⇒ the prompt runs: {response_line}"
    );
    assert_eq!(
        fixture.get_task_calls(),
        2,
        "fresh + settlement — no gate GetTask"
    );
    assert_eq!(fixture.create_task_calls(), 1);

    fixture.shutdown();
    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

fn empty_script() -> Script {
    Script {
        get_tasks: vec![],
        subscribes: vec![],
        approves: vec![],
    }
}
