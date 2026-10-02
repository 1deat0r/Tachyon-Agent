//! Permission bridge ticket 03 at the scripted-gateway seam: the three
//! stress contracts — cancel during an outstanding permission request,
//! the suspended turn deadline, and the orphan fallback bound — one
//! test per small task, appended in order.
//!
//! What is pinned here (one test per small task):
//! - `cancel_resolves_the_pending_request_locally_then_reports_cancelled`
//!   (S1): `session/cancel` with an armed request resolves the local
//!   reply slot as `cancelled`, sends ZERO Approve/Deny frames, and the
//!   drain-ack order re-pins: request → cancel reply → prompt
//!   `stopReason: cancelled`;
//! - `late_permission_answer_after_cancel_is_ignored` (S2),
//!   `turn_timeout_is_suspended_during_an_outstanding_request` (S3),
//!   and `orphaned_park_falls_back_to_approval_required` (S4) follow.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

mod common;
use common::scripted::{
    SCRIPT_TASK_ID, Script, ScriptedGateway, Step, Subscription, status_payload, task_status,
};
use common::{Adapter, AdapterFrame, classify, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);

/// Any valid UUID string parses as a session id; the fixture answers
/// `GetSession` for whatever the adapter asks.
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";

/// The approval the scripted park journalled (the exact
/// `StateEvent::ApprovalRequest` payload shape).
const APPROVAL_ID: &str = "01990f9e-5555-7000-8000-000000000000";
const APPROVAL_SUMMARY: &str = "Apply patch to src/lib.rs";

fn approval_payload() -> Value {
    json!({
        "t": "ApprovalRequest",
        "v": {"request": {
            "id": APPROVAL_ID,
            "capability": "mutation.patch",
            "scope": "workspace",
            "operation_hash": "abc123",
            "summary": APPROVAL_SUMMARY,
        }},
    })
}

fn prompt_line(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{SESSION_ID}","prompt":[{{"type":"text","text":"Cancel during request."}}]}}}}"#
    )
}

fn cancel_line(id: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/cancel","params":{{"sessionId":"{SESSION_ID}"}}}}"#
    )
}

/// The golden `tool_call` announcement, byte-pinned (identical on every
/// bridge path: the announcement precedes the request, always).
fn golden_tool_call() -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{SESSION_ID}","update":{{"sessionUpdate":"tool_call","title":"{APPROVAL_SUMMARY}","toolCallId":"{APPROVAL_ID}"}}}}}}"#
    )
}

/// The golden `session/request_permission` frame, byte-pinned (first
/// adapter-minted outbound id, −1, exactly the two one-shot options).
fn golden_request() -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":-1,"method":"session/request_permission","params":{{"options":[{{"kind":"allow_once","name":"Allow once","optionId":"allow_once"}},{{"kind":"reject_once","name":"Reject once","optionId":"reject_once"}}],"sessionId":"{SESSION_ID}","toolCall":{{"title":"{APPROVAL_SUMMARY}","toolCallId":"{APPROVAL_ID}"}}}}}}"#
    )
}

/// The shared cancel-during-request park script: fresh read
/// `Executing`, the real supervisor journal order (status FIRST, then
/// the ask), one post-park settlement read — then a terminal
/// `Cancelled` read after the fixture's `CancelTask` (its fixed
/// reaction pushes the terminal journal, mirroring the supervisor's
/// cancel). `approves` stays empty: nothing here may ever be granted.
fn cancel_during_request_script() -> Script {
    Script {
        get_tasks: vec![
            task_status("Executing"),
            task_status("WaitingApproval"),
            task_status("Cancelled"),
        ],
        subscribes: vec![Subscription {
            replay: vec![],
            post: vec![
                Step::Journal {
                    seq: 1,
                    kind: "status",
                    payload: status_payload("WaitingApproval"),
                },
                Step::Journal {
                    seq: 2,
                    kind: "approval_request",
                    payload: approval_payload(),
                },
            ],
        }],
        approves: vec![],
    }
}

/// Drives the prompt to the outstanding request: sends the prompt,
/// reads the byte-pinned golden pair (announcement → request), and
/// returns the parsed request frame (its id is what the client would
/// answer — or, on this ticket, never answer).
async fn park_until_request(adapter: &mut Adapter) -> Value {
    adapter.send(&prompt_line(1)).await;
    let tool_call_line = adapter
        .read_line(WAIT, "the tool_call announcement")
        .await
        .expect("the tool_call frame arrives");
    assert_eq!(tool_call_line, golden_tool_call());
    let request_line = adapter
        .read_line(WAIT, "the session/request_permission frame")
        .await
        .expect("the request frame arrives");
    assert_eq!(request_line, golden_request());
    match classify(&request_line) {
        AdapterFrame::Request(frame) => frame,
        other => panic!("expected an id-bearing request, got {other:?}"),
    }
}

/// S1 (M1a/M1b/M1c): cancel during an outstanding request. The local
/// reply slot resolves as `cancelled` (the client never answers),
/// zero decision frames reach the gateway, and the drain-ack order
/// re-pins: request → cancel reply (after `CancelTask`) → prompt
/// `stopReason: cancelled`. The inline cancel cannot deadlock: it
/// never awaits the answer it just invalidated.
#[tokio::test]
async fn cancel_resolves_the_pending_request_locally_then_reports_cancelled() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start(&dir, cancel_during_request_script());
    let mut adapter = Adapter::spawn(&dir);

    park_until_request(&mut adapter).await;

    // The client cancels WITHOUT answering the request.
    adapter.send(&cancel_line(2)).await;

    // Drain-ack order, first pin: the cancel reply precedes the prompt
    // verdict — reading for id 2 panics if id 1 ever arrived first.
    let (pre_cancel, cancel_reply) = adapter.read_until_response(json!(2), WAIT).await;
    assert!(
        pre_cancel.is_empty(),
        "nothing may sit between the request and the cancel reply: {pre_cancel:?}"
    );
    assert_eq!(
        cancel_reply, r#"{"jsonrpc":"2.0","id":2,"result":{}}"#,
        "the cancel reply is byte-pinned: {cancel_reply}"
    );
    // Second pin: zero decision frames (Approve/Deny paths emit
    // `tool_call_update`, request re-emissions, or anything else)
    // appear between the cancel reply and the prompt verdict.
    let (post_cancel, prompt_reply) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(
        post_cancel.is_empty(),
        "zero frames between the cancel reply and the prompt verdict: {post_cancel:?}"
    );
    let prompt = parse_frame(&prompt_reply);
    assert!(
        prompt.get("error").is_none(),
        "a cancel-owned exchange answers with stopReason, not an error: {prompt_reply}"
    );
    assert_eq!(
        prompt["result"]["stopReason"], "cancelled",
        "reply: {prompt_reply}"
    );

    // The decision pin: exactly one drain ack reached the gateway,
    // zero Approve/Deny did.
    assert!(
        fixture.approvals_seen().is_empty(),
        "cancel must never grant: {:?}",
        fixture.approvals_seen()
    );
    assert!(
        fixture.denies_seen().is_empty(),
        "cancel must never deny either: {:?}",
        fixture.denies_seen()
    );
    assert_eq!(
        fixture.cancels_seen(),
        [SCRIPT_TASK_ID.to_owned()],
        "exactly one CancelTask with the active turn's task"
    );
    assert_eq!(
        fixture.get_task_calls(),
        3,
        "fresh read + park settlement read + terminal read"
    );
    assert_eq!(fixture.subscribe_cursors(), [0], "no resync involved");
    fixture.shutdown();

    adapter.close_stdin();
    let (_rest, stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "clean exit after a cancel-owned exchange");
    assert!(
        stderr.contains("drain ack received"),
        "the adapter logs the awaited drain ack before replying: {stderr}"
    );
    assert!(
        stderr.contains("resolved locally as cancelled"),
        "the local resolution is logged: {stderr}"
    );
    assert!(
        stderr.contains("no decision issued"),
        "the zero-decision outcome is logged: {stderr}"
    );
    // S3 wiring (M3a) proven in the production pipeline: the request
    // armed ⇒ the deadline suspended; the local resolution ⇒ it
    // resumed (with a full budget) before the prompt settled.
    assert!(
        stderr.contains("the turn deadline is suspended"),
        "an outstanding request suspends the turn deadline: {stderr}"
    );
    assert!(
        stderr.contains("the turn deadline resumes"),
        "the resolution resumes the turn deadline: {stderr}"
    );
}

/// S2 (M2a): a `session/request_permission` answer that arrives AFTER
/// the cancel resolved its slot finds no armed slot — it drops with a
/// log line, never panics, never reaches the gateway (zero Approve),
/// and the prompt still settles `cancelled`.
#[tokio::test]
async fn late_permission_answer_after_cancel_is_ignored() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start(&dir, cancel_during_request_script());
    let mut adapter = Adapter::spawn(&dir);

    let request = park_until_request(&mut adapter).await;
    adapter.send(&cancel_line(2)).await;
    let (_pre_cancel, cancel_reply) = adapter.read_until_response(json!(2), WAIT).await;
    assert_eq!(cancel_reply, r#"{"jsonrpc":"2.0","id":2,"result":{}}"#);

    // The LATE answer: `allow_once` for the slot the cancel already
    // resolved. It must find nothing to route to.
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":{},"result":{{"outcome":"selected","optionId":"allow_once"}}}}"#,
            request["id"]
        ))
        .await;

    let (post_cancel, prompt_reply) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(
        post_cancel.is_empty(),
        "the late answer must emit nothing: {post_cancel:?}"
    );
    let prompt = parse_frame(&prompt_reply);
    assert_eq!(
        prompt["result"]["stopReason"], "cancelled",
        "a late allow can never change the verdict: {prompt_reply}"
    );

    // No gateway call: the dropped answer never became a decision.
    assert!(
        fixture.approvals_seen().is_empty(),
        "a late answer must never grant: {:?}",
        fixture.approvals_seen()
    );
    assert!(fixture.denies_seen().is_empty());
    assert_eq!(fixture.cancels_seen(), [SCRIPT_TASK_ID.to_owned()]);
    fixture.shutdown();

    adapter.close_stdin();
    let (_rest, stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "a dropped late answer never panics: {exit_ok}");
    assert!(
        stderr.contains("response frame has no armed slot"),
        "the drop is logged at info level: {stderr}"
    );
}

/// S4 (M4a + M4b): an ORPHANED park — the ask frame arrives but
/// carries no approval id, so the adapter refuses to invent one —
/// emits ZERO frames (never a false request) and fails typed
/// `-32004 approval_required` inside the tightened 2 s orphan bound:
/// the bound actually waits (>= 1.5 s) and is tight (< 4.5 s, below
/// the old 5 s interim grace); never a silent hang, never a guess.
#[tokio::test]
async fn orphaned_park_falls_back_to_approval_required() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![task_status("Executing"), task_status("WaitingApproval")],
        subscribes: vec![Subscription {
            replay: vec![],
            post: vec![
                Step::Journal {
                    seq: 1,
                    kind: "status",
                    payload: status_payload("WaitingApproval"),
                },
                // The ask arrives but carries NO approval id: the
                // adapter must never invent an approval from it.
                Step::Journal {
                    seq: 2,
                    kind: "approval_request",
                    payload: json!({"t": "ApprovalRequest", "v": {"request": {}}}),
                },
            ],
        }],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start(&dir, script);
    let mut adapter = Adapter::spawn(&dir);
    let started = Instant::now();
    adapter.send(&prompt_line(1)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let elapsed = started.elapsed();

    assert!(
        updates.is_empty(),
        "an id-less ask must never emit a false request: {updates:?}"
    );
    let response = parse_frame(&response_line);
    assert!(
        response.get("result").is_none(),
        "an orphaned park fails typed, never guesses a verdict: {response_line}"
    );
    assert_eq!(response["error"]["code"], -32004, "reply: {response_line}");
    assert_eq!(
        response["error"]["data"],
        json!("approval_required"),
        "the parked-on-approval marker: {response_line}"
    );
    assert!(
        elapsed >= Duration::from_millis(1500),
        "the orphan bound actually waits: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(4500),
        "the tightened 2 s orphan bound fires, not the old 5 s interim grace: {elapsed:?}"
    );
    assert!(
        fixture.approvals_seen().is_empty(),
        "an orphaned park decides nothing: {:?}",
        fixture.approvals_seen()
    );
    assert!(fixture.denies_seen().is_empty());
    assert_eq!(fixture.subscribe_cursors(), [0], "no resync involved");
    fixture.shutdown();

    adapter.close_stdin();
    let (_rest, stderr, exit_ok) = adapter.finish().await;
    assert!(
        exit_ok,
        "an orphaned park fails typed, not fatally: {exit_ok}"
    );
    assert!(
        stderr.contains("carries no approval id"),
        "the refused invention is logged: {stderr}"
    );
}
