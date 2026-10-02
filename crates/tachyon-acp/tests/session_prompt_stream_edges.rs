//! Ticket 03 stream edges against the scripted gateway fixture: a
//! `ResyncRequired` mid-subscription triggers exactly one bounded
//! re-subscribe at the gateway-provided cursor and the turn still
//! completes with every chunk in order (no silent gap, no duplicates);
//! a second `ResyncRequired` fails the prompt typed instead of
//! truncating; an approval-parked prompt fails typed
//! `-32004 approval_required` inside a bounded window instead of
//! hanging.

use std::time::{Duration, Instant};

use serde_json::json;

mod common;
use common::scripted::{
    ReplayRow, SCRIPT_TASK_ID, Script, ScriptedGateway, Step, Subscription, agent_payload,
    status_payload, task_completed, task_status,
};
use common::{Adapter, AdapterFrame, classify, parse_frame, test_dir};

const WAIT: Duration = Duration::from_secs(30);

/// Any valid UUID string parses as a session id; the fixture answers
/// `GetSession` for whatever the adapter asks.
const SESSION_ID: &str = "01990f9e-1111-7000-8000-000000000000";

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

/// Approval-parked prompt: the settlement read observes
/// `WaitingApproval` and the turn fails typed `-32004
/// approval_required` right away — inside the 30 s read bound (the data
/// marker proves it is not the 300 s `turn_timed_out`) — never a hang,
/// never a guessed verdict, never a chunk.
#[tokio::test]
async fn approval_parked_prompt_fails_typed_within_a_bounded_window() {
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
    };
    let fixture = ScriptedGateway::start(&dir, script);

    let mut adapter = Adapter::spawn(&dir);
    let started = Instant::now();
    adapter.send(&prompt_line(1)).await;
    let (updates, response_line) = adapter.read_until_response(json!(1), WAIT).await;
    let elapsed = started.elapsed();
    assert!(
        updates.is_empty(),
        "an approval-parked prompt streams no chunks: {updates:?}"
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
        elapsed < WAIT,
        "the typed error arrives inside the 30 s read bound, far below the 300 s \
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
