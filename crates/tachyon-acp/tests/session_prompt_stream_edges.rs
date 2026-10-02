//! Ticket 03 stream edges against the scripted gateway fixture: a
//! `ResyncRequired` mid-subscription triggers exactly one bounded
//! re-subscribe at the gateway-provided cursor and the turn still
//! completes with every chunk in order (no silent gap, no duplicates);
//! a second `ResyncRequired` fails the prompt typed instead of
//! truncating; an approval park WITH its `approval_request` frame emits
//! the `tool_call` + `session/request_permission` pair and an
//! `allow_once` answer drives the turn to `end_turn` (permission bridge
//! ticket 01); a park whose ask NEVER arrives falls back to the typed
//! `-32004 approval_required` within the orphan grace instead of
//! hanging.

use std::time::{Duration, Instant};

use serde_json::json;

mod common;
use common::scripted::{
    ApproveAnswer, ReplayRow, SCRIPT_TASK_ID, Script, ScriptedGateway, Step, Subscription,
    agent_payload, status_payload, task_completed, task_status,
};
use common::{Adapter, AdapterFrame, classify, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);

/// Any valid UUID string parses as a session id; the fixture answers
/// `GetSession` for whatever the adapter asks.
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";

/// The approval the scripted park journalled: id, operation identity,
/// and the human title the ACP client is shown (the exact
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
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{SESSION_ID}","prompt":[{{"type":"text","text":"Stream edge."}}]}}}}"#
    )
}

/// Extracts every `agent_message_chunk` text from raw notification
/// lines, in arrival order.
fn chunk_texts(lines: &[String]) -> Vec<String> {
    let mut texts = Vec::new();
    for line in lines {
        let AdapterFrame::Notification(frame) = classify(line) else {
            panic!("expected only notifications before the response: {line}");
        };
        assert_eq!(frame["method"], "session/update", "line: {line}");
        let update = &frame["params"]["update"];
        assert_eq!(
            update["sessionUpdate"], "agent_message_chunk",
            "line: {line}"
        );
        texts.push(update["content"]["text"].as_str().expect("text").to_owned());
    }
    texts
}

/// `ResyncRequired` once ⇒ re-subscribe at the cursor (`after_seq` echoed
/// back by the fixture proves WHICH cursor), the replayed rows restore
/// continuity (chunks: alpha live, beta replayed — exactly once each,
/// in order), and the turn still completes `end_turn`.
#[tokio::test]
async fn resync_required_re_subscribes_at_the_cursor_and_chunks_stay_continuous() {
    let dir = test_dir();
    let script = Script {
        // Call 1: the fresh-state read before StartRun. Call 2: the
        // settlement read after the replayed status row.
        get_tasks: vec![task_status("Executing"), task_completed("final answer")],
        subscribes: vec![
            Subscription {
                replay: vec![],
                post: vec![
                    Step::Journal {
                        seq: 1,
                        kind: "agent_message",
                        payload: agent_payload("alpha"),
                    },
                    // The overflow notice: everything after cursor 1 is
                    // dropped on the floor — the fixture never delivered
                    // beta's row, exactly like a real queue overflow.
                    Step::Resync { after_seq: 1 },
                ],
            },
            Subscription {
                replay: vec![
                    ReplayRow {
                        seq: 2,
                        kind: "status",
                        payload: status_payload("Executing"),
                    },
                    ReplayRow {
                        seq: 3,
                        kind: "agent_message",
                        payload: agent_payload("beta"),
                    },
                ],
                post: vec![Step::Journal {
                    seq: 4,
                    kind: "status",
                    payload: status_payload("Completed"),
                }],
            },
        ],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start(&dir, script);

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&prompt_line(1)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;

    // Chunk-sequence continuity across the resync: nothing lost (beta
    // arrived via the replay), nothing duplicated (alpha never
    // re-arrived), order preserved.
    assert_eq!(
        chunk_texts(&updates),
        ["alpha", "beta"],
        "the re-subscribe must repair the gap, not repeat or drop chunks"
    );
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "reply: {response_line}"
    );
    assert_eq!(
        response["result"]["content"][0]["text"], "final answer",
        "the turn still completed with the full conversation tail"
    );

    // THE cursor pin: initial subscribe from 0, the re-subscribe from
    // the ResyncRequired's after_seq — exactly one re-subscribe.
    assert_eq!(
        fixture.subscribe_cursors(),
        [0, 1],
        "the adapter must re-subscribe at the resync cursor, exactly once"
    );
    assert_eq!(fixture.get_task_calls(), 2, "fresh read + settlement read");
    fixture.shutdown();

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Persistent `ResyncRequired` (the subscription keeps dropping) ⇒ the
/// bounded re-subscribe budget runs out and the prompt fails typed
/// `-32004 resync_required` — never a success frame with a silently
/// truncated stream, never an unbounded re-subscribe loop.
#[tokio::test]
async fn persistent_resync_required_fails_the_prompt_typed() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![task_status("Executing")],
        subscribes: vec![
            Subscription {
                replay: vec![],
                post: vec![Step::Resync { after_seq: 0 }],
            },
            Subscription {
                replay: vec![],
                post: vec![Step::Resync { after_seq: 0 }],
            },
        ],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start(&dir, script);

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&prompt_line(1)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    assert!(
        updates.is_empty(),
        "no chunks can precede a typed failure here: {updates:?}"
    );
    let response = parse_frame(&response_line);
    assert!(
        response.get("result").is_none(),
        "persistent resync must never answer success (silent gap): {response_line}"
    );
    assert_eq!(response["error"]["code"], -32004, "reply: {response_line}");
    assert_eq!(response["error"]["data"], json!("resync_required"));

    // Bounded: exactly one re-subscribe, then the typed refusal.
    assert_eq!(
        fixture.subscribe_cursors(),
        [0, 0],
        "one bounded re-subscribe, then give up typed"
    );
    fixture.shutdown();

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}

/// Approval-parked turn WITH its ask (permission bridge ticket 01) —
/// the REWRITE of the old "park fails typed immediately" test: the park
/// journals `status → WaitingApproval` BEFORE the `approval_request`
/// (the real supervisor order), the adapter announces `tool_call` then
/// sends `session/request_permission` (both byte-pinned against the
/// pinned schema-v1.23.0 shapes), an `allow_once` answer issues `Approve`
/// carrying the parked approval's exact identity, the tool call closes
/// `completed`, the `WaitingApproval → Executing` bounce settles
/// benignly, chunks resume, and the prompt ends `end_turn`.
#[tokio::test]
#[allow(clippy::too_many_lines)] // one narrative: park → request → grant → resume
async fn approval_parked_prompt_emits_the_permission_request_and_allow_resumes() {
    let dir = test_dir();
    let script = Script {
        // Call 1: fresh-state read before StartRun. Call 2: the
        // settlement read after the park journals. Call 3: the
        // post-grant read.
        get_tasks: vec![
            task_status("Executing"),
            task_status("WaitingApproval"),
            task_completed("final answer"),
        ],
        subscribes: vec![Subscription {
            replay: vec![],
            post: vec![
                // The park's real order: status FIRST, then the ask —
                // detection is journal-driven, never snapshot-driven.
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
        approves: vec![ApproveAnswer {
            // The gateway answers the grant with the post-decision
            // snapshot (real `decide` shape: {"task": …}), then journals
            // its reaction: approval granted → Executing → new chunk.
            task: task_status("Executing"),
            post: vec![
                Step::Journal {
                    seq: 3,
                    kind: "approval",
                    payload: json!({
                        "t": "Approval",
                        "v": {"approval": APPROVAL_ID, "granted": true, "reason": ""},
                    }),
                },
                Step::Journal {
                    seq: 4,
                    kind: "status",
                    payload: status_payload("Executing"),
                },
                Step::Journal {
                    seq: 5,
                    kind: "agent_message",
                    payload: agent_payload("resumed after approval"),
                },
            ],
        }],
    };
    let fixture = ScriptedGateway::start(&dir, script);

    let mut adapter = Adapter::spawn(&dir);
    let started = Instant::now();
    adapter.send(&prompt_line(1)).await;

    // THE golden pair: the tool_call announcement PRECEDES the request,
    // byte-exact against the pinned schema shapes (ToolCall requires
    // toolCallId+title; RequestPermissionRequest requires sessionId +
    // toolCall + options, exactly the two one-shot entries).
    let tool_call_line = adapter
        .read_line(WAIT, "the tool_call announcement")
        .await
        .expect("the tool_call frame arrives");
    assert_eq!(
        tool_call_line,
        format!(
            r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{SESSION_ID}","update":{{"sessionUpdate":"tool_call","title":"{APPROVAL_SUMMARY}","toolCallId":"{APPROVAL_ID}"}}}}}}"#
        ),
        "the tool_call announcement is pinned byte-for-byte"
    );
    let request_line = adapter
        .read_line(WAIT, "the session/request_permission frame")
        .await
        .expect("the request frame arrives");
    assert_eq!(
        request_line,
        format!(
            r#"{{"jsonrpc":"2.0","id":-1,"method":"session/request_permission","params":{{"options":[{{"kind":"allow_once","name":"Allow once","optionId":"allow_once"}},{{"kind":"reject_once","name":"Reject once","optionId":"reject_once"}}],"sessionId":"{SESSION_ID}","toolCall":{{"title":"{APPROVAL_SUMMARY}","toolCallId":"{APPROVAL_ID}"}}}}}}"#
        ),
        "the permission request is pinned byte-for-byte"
    );
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the request is emitted promptly, not at the turn deadline: {:?}",
        started.elapsed()
    );

    // The client GRANTS: echo the request's own id with `selected` +
    // the offered `allow_once`.
    let request = match classify(&request_line) {
        AdapterFrame::Request(frame) => frame,
        other => panic!("expected an id-bearing request, got {other:?}"),
    };
    let request_id = request["id"].clone();
    assert_eq!(
        request["params"]["options"].as_array().map(Vec::len),
        Some(2),
        "exactly the two one-shot options: {request_line}"
    );
    adapter
        .send(&format!(
            r#"{{"jsonrpc":"2.0","id":{request_id},"result":{{"outcome":"selected","optionId":"allow_once"}}}}"#
        ))
        .await;

    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let elapsed = started.elapsed();

    // Post-decision frames, in order: the tool call closes `completed`,
    // then the chunk that proves streaming resumed.
    assert_eq!(
        updates.len(),
        2,
        "tool_call_update then the resumed chunk: {updates:?}"
    );
    assert_eq!(
        updates[0],
        format!(
            r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"{SESSION_ID}","update":{{"sessionUpdate":"tool_call_update","status":"completed","toolCallId":"{APPROVAL_ID}"}}}}}}"#
        ),
        "the tool call closes completed, byte-pinned: {}",
        updates[0]
    );
    let chunk = match classify(&updates[1]) {
        AdapterFrame::Notification(frame) => frame,
        other => panic!("expected the resumed chunk, got {other:?}"),
    };
    assert_eq!(
        chunk["params"]["update"]["sessionUpdate"], "agent_message_chunk",
        "line: {}",
        updates[1]
    );
    assert_eq!(
        chunk["params"]["update"]["content"]["text"], "resumed after approval",
        "chunks resume after the grant: {}",
        updates[1]
    );

    // The prompt settles honestly: end_turn with the conversation tail.
    let response = parse_frame(&response_line);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "reply: {response_line}"
    );
    assert_eq!(
        response["result"]["content"][0]["text"], "final answer",
        "reply: {response_line}"
    );
    assert!(
        elapsed < Duration::from_secs(300),
        "well below the turn deadline: {elapsed:?}"
    );

    // THE decision pin: Approve reached the gateway with exactly the
    // parked task + approval identity, once, and nothing else ran.
    assert_eq!(
        fixture.approvals_seen(),
        [(SCRIPT_TASK_ID.to_owned(), APPROVAL_ID.to_owned())],
        "exactly one Approve with the journalled approval id"
    );
    assert_eq!(fixture.subscribe_cursors(), [0], "no resync involved");
    assert_eq!(
        fixture.get_task_calls(),
        3,
        "fresh read + park settlement read + post-grant read"
    );
    fixture.shutdown();

    adapter.close_stdin();
    let (rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok, "the allowed turn ends cleanly: {rest:?}");
}

/// Orphan park (the request-less variant, kept from the pre-bridge
/// behavior): the settlement read observes `WaitingApproval` but NO
/// `approval_request` frame ever arrives, so after the orphan grace the
/// prompt answers typed `-32004 approval_required` — bounded, never the
/// 300 s turn deadline, never a hang, never an invented request.
#[tokio::test]
async fn approval_parked_without_an_ask_frame_falls_back_typed() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![task_status("Executing"), task_status("WaitingApproval")],
        subscribes: vec![Subscription {
            replay: vec![],
            post: vec![Step::Journal {
                seq: 1,
                kind: "status",
                payload: status_payload("WaitingApproval"),
            }],
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
        "an ask-less park emits no frames at all: {updates:?}"
    );
    let response = parse_frame(&response_line);
    assert!(
        response.get("result").is_none(),
        "a parked turn must fail typed, never guess a verdict: {response_line}"
    );
    assert_eq!(response["error"]["code"], -32004, "reply: {response_line}");
    assert_eq!(
        response["error"]["data"],
        json!("approval_required"),
        "the parked-on-approval marker, not the turn timeout: {response_line}"
    );
    assert!(
        elapsed < Duration::from_secs(30),
        "the typed error arrives within the orphan grace, far below the 300 s \
         deadline: {elapsed:?}"
    );
    assert_eq!(fixture.subscribe_cursors(), [0], "no resync involved");
    fixture.shutdown();

    adapter.close_stdin();
    let (rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(
        exit_ok,
        "the parked prompt fails typed, not fatally: {rest:?}"
    );
}

/// The fixture's scripted `GetTask` exhaustion is loud: a mis-sequenced
/// script surfaces as a typed gateway refusal instead of a hang (guards
/// the fixture itself, so a script bug cannot fake a green run).
#[tokio::test]
async fn fixture_script_exhaustion_is_loud_not_silent() {
    let dir = test_dir();
    let script = Script {
        get_tasks: vec![],
        subscribes: vec![],
        approves: vec![],
    };
    let fixture = ScriptedGateway::start(&dir, script);

    let mut adapter = Adapter::spawn(&dir);
    adapter.send(&prompt_line(1)).await;
    let (_updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let response = parse_frame(&response_line);
    assert_eq!(response["error"]["data"], json!("script_exhausted"));
    assert_eq!(
        response["error"]["code"], -32002,
        "the fixture refuses typed: {response_line}"
    );
    assert!(SCRIPT_TASK_ID.parse::<tachyon_types::TaskId>().is_ok());
    fixture.shutdown();

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
}
