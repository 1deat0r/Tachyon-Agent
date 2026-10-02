//! Permission bridge ticket 02 at the scripted-gateway seam: every
//! non-allow outcome of the permission exchange fails closed as a
//! gateway `Deny`, and the prompt settles `stopReason: refusal` —
//! never `turn_timed_out`, never `end_turn`, never a hang.
//!
//! What is pinned here (one test per small task, appended in order):
//! - `deny_settles_the_prompt_as_refusal` (S1): `selected` +
//!   `reject_once` ⇒ exactly one `Command::Deny` carrying the parked
//!   identity and a reason naming the ACP client ⇒
//!   `tool_call_update {status: failed}` ⇒ bounded grace with NO
//!   terminal status after the deny (the fixture reproduces the known
//!   gateway gap) ⇒ `stopReason: refusal`;
//! - `standalone_cancelled_outcome_denies` (S3) and
//!   `late_deny_journal_with_no_request_is_handled` (S4) follow.
//!
//! Ticket 01's allow-path files stay unmodified and green; the golden
//! `tool_call` / `session/request_permission` pair is byte-identical
//! here, so the announce→request order is re-pinned on the deny path.

use std::time::{Duration, Instant};

use serde_json::json;

mod common;
use common::scripted::{
    SCRIPT_TASK_ID, Script, ScriptedGateway, Step, Subscription, status_payload, task_completed,
    task_status,
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

fn approval_payload() -> serde_json::Value {
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
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{SESSION_ID}","prompt":[{{"type":"text","text":"Deny settlement."}}]}}}}"#
    )
}

/// The golden `tool_call` announcement, byte-pinned (identical on the
/// deny path: the announcement precedes the request, always).
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

/// The golden `tool_call_update failed` frame (M1b), byte-pinned.
fn golden_tool_call_failed() -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{SESSION_ID}","update":{{"sessionUpdate":"tool_call_update","status":"failed","toolCallId":"{APPROVAL_ID}"}}}}}}"#
    )
}

/// The park script both deny-path tests share: fresh read `Executing`,
/// then the real supervisor journal order (status FIRST, then the ask),
/// then one post-deny settlement read. The fixture's fixed `Deny`
/// reaction supplies the `approval {granted:false}` + `Executing` rows
/// and NEVER a terminal status (the known gateway gap).
fn deny_script() -> Script {
    Script {
        get_tasks: vec![
            task_status("Executing"),
            task_status("WaitingApproval"),
            task_status("Executing"),
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

/// Drives the prompt through the park and reads the golden pair,
/// returning the request id the client must answer.
async fn park_until_request(adapter: &mut Adapter) -> serde_json::Value {
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
        AdapterFrame::Request(frame) => frame["id"].clone(),
        other => panic!("expected an id-bearing request, got {other:?}"),
    }
}

/// S1 — the deny settlement path: `reject_once` drives exactly one
/// gateway `Deny` (reason names the ACP client), the tool call closes
/// `failed`, the bounded grace waits for a terminal status that never
/// journals, and the prompt settles `stopReason: refusal` — never
/// `turn_timed_out`, never `end_turn`, and no `Approve` is ever issued.
#[tokio::test]
async fn deny_settles_the_prompt_as_refusal() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start(&dir, deny_script());
    let mut adapter = Adapter::spawn(&dir);
    let started = Instant::now();

    let request_id = park_until_request(&mut adapter).await;

    // The client REJECTS: `selected` + the offered `reject_once`.
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":{request_id},"result":{{"outcome":"selected","optionId":"reject_once"}}}}"#
        ))
        .await;

    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let elapsed = started.elapsed();

    // M1b: the tool call closes `failed`, byte-pinned — and nothing
    // else streams (the denied run produces no chunks).
    assert_eq!(
        updates.len(),
        1,
        "only the tool_call_update failed precedes the response: {updates:?}"
    );
    assert_eq!(updates[0], golden_tool_call_failed());

    // The prompt settles as a schema-legal success frame carrying
    // `refusal` — not an error frame, not `turn_timed_out`, not
    // `end_turn`.
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "refusal",
        "a deny settles refusal: {response_line}"
    );
    assert!(
        response["result"]["content"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "the refusal carries no invented content: {response_line}"
    );

    // M1c: the settle WAITED the bounded grace for a terminal status
    // (>= ~4 s, the deny grace default) and still landed far below the
    // 300 s turn deadline (no hang, no `turn_timed_out`).
    assert!(
        elapsed >= Duration::from_secs(4),
        "the refusal comes from the grace default, not an instant guess: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "well below the turn deadline: {elapsed:?}"
    );

    // THE decision pin: exactly one `Deny` with the parked identity and
    // a reason naming the ACP client; zero `Approve` ever.
    let denies = fixture.denies_seen();
    assert_eq!(denies.len(), 1, "exactly one Deny: {denies:?}");
    assert_eq!(denies[0].0, SCRIPT_TASK_ID, "the denied task");
    assert_eq!(denies[0].1, APPROVAL_ID, "the journalled approval id");
    assert!(
        denies[0].2.contains("ACP client"),
        "the reason names the ACP client: {}",
        denies[0].2
    );
    assert!(
        fixture.approvals_seen().is_empty(),
        "a reject must never Approve"
    );
    assert_eq!(fixture.subscribe_cursors(), [0], "no resync involved");
    assert_eq!(
        fixture.get_task_calls(),
        3,
        "fresh read + park settlement read + post-deny settlement read"
    );
    fixture.shutdown();

    adapter.close_stdin();
    let (rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "the refused turn ends cleanly: {rest:?}");
}

/// S3 — a STANDALONE `outcome: cancelled` (no `session/cancel` in
/// flight) is a no-approval answer: it fails closed as exactly one
/// gateway `Deny` (reason names the ACP client and the standalone
/// cancelled outcome), the tool call closes `failed`, and the prompt
/// settles `refusal` through the same bounded grace. Zero `Approve`.
#[tokio::test]
async fn standalone_cancelled_outcome_denies() {
    let dir = test_dir();
    let fixture = ScriptedGateway::start(&dir, deny_script());
    let mut adapter = Adapter::spawn(&dir);
    let started = Instant::now();

    let request_id = park_until_request(&mut adapter).await;

    // The client answers `cancelled` — but sent NO session/cancel.
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":{request_id},"result":{{"outcome":"cancelled"}}}}"#
        ))
        .await;

    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let elapsed = started.elapsed();

    assert_eq!(
        updates.len(),
        1,
        "only the tool_call_update failed precedes the response: {updates:?}"
    );
    assert_eq!(updates[0], golden_tool_call_failed());

    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "refusal",
        "a standalone cancelled answer settles refusal: {response_line}"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "the refusal comes from the grace default: {elapsed:?}"
    );

    // THE decision pin: exactly one `Deny`, parked identity, reason
    // names the ACP client AND the standalone cancelled outcome;
    // zero `Approve`.
    let denies = fixture.denies_seen();
    assert_eq!(denies.len(), 1, "exactly one Deny: {denies:?}");
    assert_eq!(denies[0].0, SCRIPT_TASK_ID);
    assert_eq!(denies[0].1, APPROVAL_ID);
    assert!(
        denies[0].2.contains("ACP client") && denies[0].2.contains("cancelled"),
        "the reason names the client and the outcome: {}",
        denies[0].2
    );
    assert!(
        fixture.approvals_seen().is_empty(),
        "a cancelled answer must never Approve"
    );
    assert_eq!(
        fixture.get_task_calls(),
        3,
        "fresh + park + post-deny reads"
    );
    fixture.shutdown();

    adapter.close_stdin();
    let (rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "the refused turn ends cleanly: {rest:?}");
}

/// S4 — the race pin: a journalled `approval {granted:false}` arrives
/// when NO permission request is (or ever was) outstanding. The
/// adapter observes it (explicit log line), decides NOTHING — zero
/// `Deny`, zero `Approve`, no panic — and the turn still settles from
/// the journal's own status row: `end_turn` with the conversation tail,
/// exactly one settlement read. No double-settle by construction:
/// decisions come only from a validated client answer.
#[tokio::test]
async fn late_deny_journal_with_no_request_is_handled() {
    let dir = test_dir();
    let script = Script {
        // Fresh-state read, then ONE settlement read after the status
        // row the deny journal never affects.
        get_tasks: vec![task_status("Executing"), task_completed("final answer")],
        subscribes: vec![Subscription {
            replay: vec![],
            post: vec![
                // The race: a deny decision journal with no request
                // armed and no exchange in flight at all.
                Step::Journal {
                    seq: 1,
                    kind: "approval",
                    payload: json!({
                        "t": "Approval",
                        "v": {
                            "approval": APPROVAL_ID,
                            "granted": false,
                            "reason": "the ACP client rejected the permission request (reject_once)",
                        },
                    }),
                },
                Step::Journal {
                    seq: 2,
                    kind: "status",
                    payload: status_payload("Completed"),
                },
            ],
        }],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start(&dir, script);
    let mut adapter = Adapter::spawn(&dir);

    adapter.send(&prompt_line(1)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    // The turn settles from the status row, exactly as if the deny
    // journal never existed — the journal drove nothing.
    assert!(
        updates.is_empty(),
        "a deny journal emits no ACP frames: {updates:?}"
    );
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "the late deny journal must not redirect the settlement: {response_line}"
    );
    assert_eq!(
        response["result"]["content"][0]["text"], "final answer",
        "reply: {response_line}"
    );

    // ZERO decisions: no double-settle, no panic, nothing issued.
    assert!(
        fixture.denies_seen().is_empty(),
        "an observed deny journal must never mint a second Deny"
    );
    assert!(fixture.approvals_seen().is_empty(), "no Approve either");
    assert_eq!(
        fixture.get_task_calls(),
        2,
        "fresh read + one settlement read; the deny journal arms no read"
    );
    fixture.shutdown();

    adapter.close_stdin();
    let (rest, stderr, exit_ok) = adapter.finish().await;
    assert!(
        stderr.contains("journalled deny observed"),
        "the deny journal is observed and logged, not acted on: {stderr}"
    );
    assert!(exit_ok, "the turn ends cleanly: {rest:?}");
}
