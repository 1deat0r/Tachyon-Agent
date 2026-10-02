//! Ticket 03 acceptance: `session/cancel` against a live test gateway —
//! the mid-turn cancel awaits the Supervisor's drain acknowledgement
//! before answering, the frame order pins it, the resolved prompt
//! reports `stopReason: "cancelled"` (never `end_turn`), and the
//! no-active-turn / unknown-session / concurrent / notification-form
//! shapes are all deterministic.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tachyon_models::fake::FakeModelProvider;
use tachyon_types::ProviderId;

mod common;
use common::{
    Adapter, AdapterFrame, GatedProvider, armed_runtime, cargo_package, classify, gw_ok,
    parse_frame, test_dir, wait_until,
};

/// Generous bound: nothing here runs verification (the turn is parked
/// in the model stage), but gateway startup has its own latency.
const WAIT: Duration = Duration::from_secs(60);

fn new_line(id: u64, cwd: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/new","params":{{"cwd":{}}}}}"#,
        json!(cwd)
    )
}

fn prompt_line(id: u64, session_id: &str, text: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/prompt","params":{{"sessionId":"{session_id}","prompt":[{{"type":"text","text":{text}}}]}}}}"#,
        text = serde_json::to_string(text).expect("text serializes")
    )
}

fn cancel_line(id: u64, session_id: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"session/cancel","params":{{"sessionId":"{session_id}"}}}}"#
    )
}

fn cancel_notification(session_id: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/cancel","params":{{"sessionId":"{session_id}"}}}}"#
    )
}

/// The pinned success shape of every id-bearing `session/cancel` reply.
fn assert_cancel_ok(line: &str, id: u64) {
    assert_eq!(
        line,
        format!(r#"{{"jsonrpc":"2.0","id":{id},"result":{{}}}}"#),
        "the cancel reply is byte-pinned: an empty result, one frame per request"
    );
}

/// Spawns a gateway whose fake provider parks inside `invoke`, creates a
/// session through a fresh adapter, and returns the parked-fixture
/// pieces the cancel tests drive. The turn is parked as soon as
/// `entered >= 1`.
struct ParkedTurn {
    gateway: tachyon_gateway::RunningGateway,
    adapter: Adapter,
    session_id: String,
    entered: Arc<AtomicU64>,
    release: Arc<tokio::sync::Notify>,
}

async fn parked_turn(name: &str) -> ParkedTurn {
    let dir = test_dir();
    let workspace = cargo_package(&dir.join("ws"));
    let original = std::fs::read(workspace.join("src/lib.rs")).expect("fixture bytes");
    let fake = FakeModelProvider::new(ProviderId(format!("bench-acp-{name}")));
    fake.push_response(common::patch_response(
        "src/lib.rs",
        &original,
        &common::patched_content(&String::from_utf8_lossy(&original)),
    ));
    let entered = Arc::new(AtomicU64::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let provider = GatedProvider {
        inner: Arc::new(fake),
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    };
    let gateway = tachyon_gateway::start_with(&dir, armed_runtime(Arc::new(provider)))
        .await
        .expect("gateway starts");

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&new_line(1, &workspace.display().to_string()))
        .await;
    let (_, created_line) = adapter.read_until_response(json!(1), WAIT).await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();
    ParkedTurn {
        gateway,
        adapter,
        session_id,
        entered,
        release,
    }
}

impl ParkedTurn {
    /// Sends the first prompt and waits until the run is parked inside
    /// the model stage — the turn is unambiguously active (and its
    /// `CreateTask` long since published) from here on.
    async fn park_at_model(&mut self) {
        self.adapter
            .send(&prompt_line(2, &self.session_id, "Do the thing."))
            .await;
        wait_until("the turn reaches the model stage", 60, || {
            self.entered.load(Ordering::SeqCst) >= 1
        })
        .await;
    }

    /// The gateway's durable view of this session's single turn.
    async fn turn_task_id(&self) -> String {
        let session = gw_ok(
            self.gateway.address(),
            tachyon_protocol::Command::GetSession {
                session_id: self.session_id.parse().unwrap(),
            },
        )
        .await;
        assert_eq!(session["turns"].as_array().map(Vec::len), Some(1));
        session["turns"][0]["task_id"]
            .as_str()
            .expect("task_id")
            .to_owned()
    }

    async fn task_status(&self) -> String {
        let task_id: tachyon_types::TaskId = self.turn_task_id().await.parse().unwrap();
        let task = gw_ok(
            self.gateway.address(),
            tachyon_protocol::Command::GetTask { task_id },
        )
        .await;
        task["task"]["status"].as_str().expect("status").to_owned()
    }
}

/// Cancel mid-turn, with the ordering assertion:
///
/// 1. the drain ack (the awaited `CancelTask` response) precedes the
///    cancel reply — proven because the reply's arrival coincides with
///    the gateway already durably reporting `Cancelled` (the terminal
///    journal is written inside `cancel_run` before its response, and
///    the driver drain precedes that write), and because the adapter
///    logs the ack only after `CancelTask` answered;
/// 2. the cancel reply precedes the prompt's `cancelled` verdict —
///    reading for id 3 panics if the id-2 response arrives first;
/// 3. a `Cancelled` status read alone never yields `end_turn` — the
///    live prompt answers `stopReason: "cancelled"` (regression pin,
///    ticket 03 box 2; the mapping unit covers the table itself).
#[tokio::test]
async fn cancel_mid_turn_awaits_the_drain_ack_then_the_prompt_reports_cancelled() {
    let mut turn = parked_turn("cancel-midturn").await;
    turn.park_at_model().await;

    turn.adapter.send(&cancel_line(3, &turn.session_id)).await;
    // If the prompt's response (id 2) ever preceded the cancel reply,
    // this read panics on the unexpected id — that panic IS the frame
    // ordering assertion.
    let (pre_cancel, cancel_line) = turn.adapter.read_until_response(json!(3), WAIT).await;
    assert_cancel_ok(&cancel_line, 3);
    for line in &pre_cancel {
        let AdapterFrame::Notification(_) = classify(line) else {
            panic!("only notifications may precede the cancel reply: {line}");
        };
    }

    // The drain completed before the client ever saw the reply: the
    // terminal `Cancelled` journal is written inside `cancel_run`
    // after the driver drain and before `CancelTask`'s response.
    assert_eq!(
        turn.task_status().await,
        "Cancelled",
        "the cancel reply must not reach the client before the drain landed"
    );

    let (_updates, prompt_line) = turn.adapter.read_until_response(json!(2), WAIT).await;
    let prompt = parse_frame(&prompt_line);
    assert_eq!(prompt["id"], 2, "reply: {prompt_line}");
    assert!(
        prompt.get("error").is_none(),
        "a cancelled turn answers with stopReason, not an error: {prompt_line}"
    );
    assert_eq!(
        prompt["result"]["stopReason"], "cancelled",
        "a Cancelled status read must never produce end_turn: {prompt_line}"
    );
    assert!(
        prompt["result"]["stopReason"] != "end_turn",
        "regression: Cancelled alone never yields end_turn"
    );

    turn.adapter.close_stdin();
    let (_rest, stderr, exit_ok) = turn.adapter.finish().await;
    assert!(exit_ok, "clean exit after a cancelled turn");
    assert!(
        stderr.contains("drain ack received"),
        "the adapter logs the awaited drain ack before replying: {stderr}"
    );
    turn.release.notify_one();
    turn.gateway.shutdown().await;
}

/// Cancel with no active turn: idempotent ok, byte-pinned, repeatable,
/// and provably side-effect free (the session's turn history stays
/// empty — no `CancelTask` was ever issued).
#[tokio::test]
async fn cancel_with_no_active_turn_is_idempotent_ok_and_repeatable() {
    let dir = test_dir();
    let workspace = dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let gateway = tachyon_gateway::start(&dir).await.expect("gateway starts");
    let socket = gateway.address().to_owned();

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&new_line(1, &workspace.display().to_string()))
        .await;
    let (_, created_line) = adapter.read_until_response(json!(1), WAIT).await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    // Two cancels, no turn in between: same deterministic answer.
    adapter.send(&cancel_line(2, &session_id)).await;
    adapter.send(&cancel_line(3, &session_id)).await;
    adapter.close_stdin();
    let (replies, stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
    assert_eq!(replies.len(), 2, "one frame per cancel: {replies:?}");
    assert_cancel_ok(&replies[0], 2);
    assert_cancel_ok(&replies[1], 3);
    assert!(
        stderr.contains("no active turn; idempotent ok"),
        "the no-op choice is logged: {stderr}"
    );

    // No side effects: the session never gained a turn, so no
    // CancelTask could have touched anything.
    let session = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(
        session["turns"].as_array().map(Vec::len),
        Some(0),
        "a no-op cancel never creates or touches turns: {session}"
    );
    gateway.shutdown().await;
}

/// Cancel for an unknown session is a typed gateway refusal, and a
/// malformed `sessionId` is refused as invalid params — both before any
/// cancel could be attempted.
#[tokio::test]
async fn cancel_for_unknown_session_is_typed() {
    let dir = test_dir();
    let gateway = tachyon_gateway::start(&dir).await.expect("gateway starts");

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(
            r#"{"jsonrpc":"2.0","id":1,"method":"session/cancel","params":{"sessionId":"01990f9e-ffff-7000-8000-000000000000"}}"#,
        )
        .await;
    adapter
        .send(
            r#"{"jsonrpc":"2.0","id":2,"method":"session/cancel","params":{"sessionId":"not-a-uuid"}}"#,
        )
        .await;
    adapter
        .send(r#"{"jsonrpc":"2.0","id":3,"method":"session/cancel"}"#)
        .await;
    adapter.close_stdin();
    let (replies, _stderr, exit_ok) = adapter.finish().await;
    gateway.shutdown().await;

    assert!(exit_ok);
    assert_eq!(replies.len(), 3, "one frame per cancel: {replies:?}");

    let unknown = parse_frame(&replies[0]);
    assert_eq!(unknown["id"], 1);
    assert_eq!(unknown["error"]["code"], -32002, "reply: {}", replies[0]);
    assert_eq!(unknown["error"]["data"], json!("unknown_session"));

    let bad_id = parse_frame(&replies[1]);
    assert_eq!(bad_id["id"], 2);
    assert_eq!(bad_id["error"]["code"], -32602);
    assert_eq!(bad_id["error"]["data"], json!("invalid_session_id"));

    let missing = parse_frame(&replies[2]);
    assert_eq!(missing["id"], 3);
    assert_eq!(missing["error"]["code"], -32602);
    assert_eq!(missing["error"]["data"], json!("invalid_params"));
}

/// Concurrent cancels while a turn is parked: deterministic single
/// response each (both byte-pinned idempotent ok — the first really
/// cancels, the second lands on the already-terminal task and is
/// tolerated), no panic, and the prompt still reports `cancelled`.
#[tokio::test]
async fn concurrent_cancels_each_get_exactly_one_response() {
    let mut turn = parked_turn("cancel-double").await;
    turn.park_at_model().await;

    turn.adapter.send(&cancel_line(3, &turn.session_id)).await;
    turn.adapter.send(&cancel_line(4, &turn.session_id)).await;
    // id 2 (the prompt) may legitimately land before or after id 4 —
    // only the ids asked for here are pinned to arrive exactly once.
    let lines = turn
        .adapter
        .read_until_all(&[json!(3), json!(4), json!(2)], WAIT)
        .await;

    let index = |id: u64| {
        lines
            .iter()
            .position(|line| {
                serde_json::from_str::<Value>(line).is_ok_and(|frame| frame["id"] == json!(id))
            })
            .unwrap_or_else(|| panic!("no response for id {id} in {lines:?}"))
    };
    let (first_cancel, second_cancel, prompt) = (index(3), index(4), index(2));
    assert!(
        first_cancel < second_cancel,
        "the serve loop reads requests sequentially: {lines:?}"
    );
    assert!(
        first_cancel < prompt,
        "the first cancel's reply precedes the prompt verdict: {lines:?}"
    );
    assert_cancel_ok(&lines[first_cancel], 3);
    assert_cancel_ok(&lines[second_cancel], 4);
    let prompt_frame = parse_frame(&lines[prompt]);
    assert_eq!(
        prompt_frame["result"]["stopReason"], "cancelled",
        "reply: {}",
        lines[prompt]
    );

    turn.adapter.close_stdin();
    let (rest, _stderr, exit_ok) = turn.adapter.finish().await;
    assert!(exit_ok, "no panics under concurrent cancels: {rest:?}");
    turn.release.notify_one();
    turn.gateway.shutdown().await;
}

/// The ACP-wire form of `session/cancel` is a NOTIFICATION (schema
/// `CancelNotification`): the adapter runs the same drain-awaiting
/// pipeline but never writes a reply frame — the prompt's `cancelled`
/// verdict is the observable outcome.
#[tokio::test]
async fn session_cancel_notification_form_cancels_without_a_reply_frame() {
    let mut turn = parked_turn("cancel-notification").await;
    turn.park_at_model().await;

    turn.adapter
        .send(&cancel_notification(&turn.session_id))
        .await;
    let (_updates, prompt_line) = turn.adapter.read_until_response(json!(2), WAIT).await;
    let prompt = parse_frame(&prompt_line);
    assert_eq!(prompt["result"]["stopReason"], "cancelled");

    turn.adapter.close_stdin();
    let (rest, stderr, exit_ok) = turn.adapter.finish().await;
    assert!(exit_ok);
    // id 1 (session/new) and id 2 (the prompt) were both consumed
    // interactively; the cancel was a NOTIFICATION, so exactly zero
    // frames may remain — a reply frame for it would have arrived
    // between those reads and panicked `read_until_response(id 2)`.
    assert!(
        rest.is_empty(),
        "a notification gets no reply frame: {rest:?}"
    );
    assert!(
        stderr.contains("drain ack received"),
        "the notification path still awaits the drain: {stderr}"
    );
    turn.release.notify_one();
    turn.gateway.shutdown().await;
}
