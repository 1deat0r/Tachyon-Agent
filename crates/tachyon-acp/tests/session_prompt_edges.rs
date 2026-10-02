//! Ticket 02 acceptance edges: the sequential-turn overlap refusal
//! (first turn unaffected and completed afterwards), idempotency
//! (same-call retry replays the original response with no duplicate
//! task; a fresh call mints a fresh key → a second turn), and session
//! identity durability across an adapter restart.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::json;
use tachyon_models::fake::FakeModelProvider;
use tachyon_types::ProviderId;
use tokio::sync::Notify;

mod common;
use common::{
    Adapter, AdapterFrame, GatedProvider, armed_runtime, cargo_package, gw_ok, parse_frame,
    patch_response, patched_content, test_dir, wait_until,
};

const TURN_WAIT: Duration = Duration::from_secs(180);
const SHORT_WAIT: Duration = Duration::from_secs(30);

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

/// Overlap refusal: while the first turn is parked inside the model
/// stage, a second prompt for the SAME session is answered with the
/// typed conflict — never queued — and the first turn then runs to
/// completion with `stopReason: end_turn`.
#[tokio::test]
async fn overlapping_prompt_is_refused_and_the_first_turn_completes() {
    let dir = test_dir();
    let workspace = cargo_package(&dir.join("ws"));
    let original = std::fs::read(workspace.join("src/lib.rs")).expect("fixture bytes");

    let fake = FakeModelProvider::new(ProviderId("bench-acp-overlap".into()));
    fake.push_response(patch_response(
        "src/lib.rs",
        &original,
        &patched_content(&String::from_utf8_lossy(&original)),
    ));
    let entered = Arc::new(AtomicU64::new(0));
    let release = Arc::new(Notify::new());
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
    let (_, created_line) = adapter.read_until_response(json!(1), SHORT_WAIT).await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    // First turn: send it, then wait until the run is parked inside
    // the provider — the turn is unambiguously active.
    adapter
        .send(&prompt_line(2, &session_id, "First objective."))
        .await;
    wait_until("the first turn reaches the model stage", 60, || {
        entered.load(Ordering::SeqCst) >= 1
    })
    .await;

    // Second prompt, same session: typed conflict, no queueing.
    adapter
        .send(&prompt_line(3, &session_id, "Second objective."))
        .await;
    let (early_updates, conflict_line) = adapter.read_until_response(json!(3), SHORT_WAIT).await;
    assert!(
        early_updates.is_empty(),
        "nothing can stream before the parked model answers: {early_updates:?}"
    );
    let conflict = parse_frame(&conflict_line);
    assert_eq!(conflict["id"], 3);
    assert_eq!(conflict["error"]["code"], -32003, "reply: {conflict_line}");
    assert_eq!(conflict["error"]["data"], json!("turn_in_progress"));

    // Release the gate: the FIRST turn completes, unaffected.
    release.notify_one();
    let (updates, response_line) = adapter.read_until_response(json!(2), TURN_WAIT).await;
    assert!(!updates.is_empty(), "the first turn still streamed");
    for line in &updates {
        let AdapterFrame::Notification(frame) = common::classify(line) else {
            panic!("not a notification: {line}");
        };
        assert_eq!(frame["method"], "session/update", "line: {line}");
        assert_eq!(
            frame["params"]["update"]["sessionUpdate"], "agent_message_chunk",
            "line: {line}"
        );
    }
    let response = parse_frame(&response_line);
    assert_eq!(response["id"], 2);
    assert_eq!(
        response["result"]["stopReason"], "end_turn",
        "the refused overlap never disturbed the first turn: {response_line}"
    );
    assert!(
        !response["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .is_empty()
    );

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
    gateway.shutdown().await;
}

/// Idempotency: the SAME call re-sent with the same request id reuses
/// its key — the gateway replays the original task (one turn only) and
/// the adapter rebuilds the byte-identical original response with no
/// re-streamed chunks; a NEW request id mints a fresh key, producing a
/// second task (`turn_seq` 1 then 2).
#[tokio::test]
async fn same_call_retry_replays_and_fresh_calls_mint_new_keys() {
    let dir = test_dir();
    let workspace = cargo_package(&dir.join("ws"));
    let original = std::fs::read(workspace.join("src/lib.rs")).expect("fixture bytes");

    // Two turns, two DIFFERENT files: turn 1 mutates lib.rs, so turn 2
    // must patch a file turn 1 never touched (its base hash stays the
    // original bytes — a stale base_hash would fail the mutation gate
    // and leave the run non-terminal, which this test must not do).
    let greeting = std::fs::read(workspace.join("src/greeting.rs")).expect("fixture bytes");
    let fake = FakeModelProvider::new(ProviderId("bench-acp-idem".into()));
    fake.push_response(patch_response(
        "src/lib.rs",
        &original,
        &patched_content(&String::from_utf8_lossy(&original)),
    ));
    fake.push_response(patch_response(
        "src/greeting.rs",
        &greeting,
        &patched_content(&String::from_utf8_lossy(&greeting)),
    ));
    let gateway = tachyon_gateway::start_with(&dir, armed_runtime(Arc::new(fake)))
        .await
        .expect("gateway starts");
    let socket = gateway.address().to_owned();

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&new_line(1, &workspace.display().to_string()))
        .await;
    let (_, created_line) = adapter.read_until_response(json!(1), SHORT_WAIT).await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    // First delivery of call id 7.
    let first_request = prompt_line(7, &session_id, "First prompt text.");
    adapter.send(&first_request).await;
    let (first_updates, first_line) = adapter.read_until_response(json!(7), TURN_WAIT).await;
    assert!(!first_updates.is_empty(), "the first turn streamed");
    let first = parse_frame(&first_line);
    assert_eq!(first["result"]["stopReason"], "end_turn");

    // The SAME call re-sent (identical id, identical params).
    adapter.send(&first_request).await;
    let (retry_updates, retry_line) = adapter.read_until_response(json!(7), SHORT_WAIT).await;
    assert!(
        retry_updates.is_empty(),
        "a replayed call never re-streams chunks: {retry_updates:?}"
    );
    assert_eq!(
        retry_line, first_line,
        "same-call retry answers byte-identically with the original response"
    );
    let turns = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(
        turns["turns"].as_array().map(Vec::len),
        Some(1),
        "the replayed key must never create a duplicate task: {turns}"
    );

    // A NEW call id: fresh key, second task, second turn_seq.
    adapter
        .send(&prompt_line(8, &session_id, "Second prompt text."))
        .await;
    let (second_updates, second_line) = adapter.read_until_response(json!(8), TURN_WAIT).await;
    assert!(!second_updates.is_empty(), "the second turn streamed");
    let second = parse_frame(&second_line);
    assert_eq!(second["result"]["stopReason"], "end_turn");

    let turns = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    let turns = turns["turns"].as_array().expect("turns array");
    assert_eq!(turns.len(), 2, "two fresh calls, two tasks: {turns:?}");
    let seqs: Vec<i64> = turns
        .iter()
        .map(|turn| turn["turn_seq"].as_i64().expect("turn_seq"))
        .collect();
    assert_eq!(seqs, [1, 2], "each fresh call takes the next turn_seq");
    let tasks: Vec<&str> = turns
        .iter()
        .map(|turn| turn["task_id"].as_str().expect("task_id"))
        .collect();
    assert_ne!(tasks[0], tasks[1], "distinct tasks for distinct calls");

    adapter.close_stdin();
    let (_rest, _stderr, exit_ok) = adapter.finish().await;
    assert!(exit_ok);
    gateway.shutdown().await;
}

/// Identity durability: a session created through one adapter process
/// still resolves through a FRESH adapter process on the same data
/// dir — the prompt reaches `GetSession` + `CreateTask` with the old id
/// (proven by the gateway's `provider_not_configured` refusal, which
/// fires only AFTER both succeed), and the gateway's own row still
/// echoes the id. No adapter-side store exists to lose it.
#[tokio::test]
async fn session_id_resolves_after_adapter_restart() {
    let dir = test_dir();
    let workspace = dir.join("plain-workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::write(workspace.join("notes.txt"), "no cargo here\n").unwrap();
    // No provider armed: StartRun refuses with provider_not_configured,
    // which is exactly what proves the prompt resolved the session all
    // the way into the run pipeline without inventing anything.
    let gateway = tachyon_gateway::start(&dir).await.expect("gateway starts");
    let socket = gateway.address().to_owned();

    // Adapter instance #1 creates the session, then exits.
    let mut first = Adapter::spawn(&dir);
    first
        .send(&new_line(1, &workspace.display().to_string()))
        .await;
    let (_, created_line) = first.read_until_response(json!(1), SHORT_WAIT).await;
    let session_id = parse_frame(&created_line)["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();
    first.close_stdin();
    let (_replies, _stderr, exit_ok) = first.finish().await;
    assert!(exit_ok, "first adapter exits cleanly");

    // Adapter instance #2: fresh process, same data dir, old id.
    let mut second = Adapter::spawn(&dir);
    second
        .send(&prompt_line(2, &session_id, "Resume-worthy prompt."))
        .await;
    let (updates, response_line) = second.read_until_response(json!(2), SHORT_WAIT).await;
    assert!(updates.is_empty(), "no run ever started: {updates:?}");
    let response = parse_frame(&response_line);
    assert_eq!(response["id"], 2);
    assert_eq!(response["error"]["code"], -32002, "reply: {response_line}");
    assert_eq!(
        response["error"]["data"],
        json!("provider_not_configured"),
        "the id resolved past GetSession and CreateTask — only the \
         missing provider stops the run: {response_line}"
    );
    second.close_stdin();
    let (_replies, _stderr, exit_ok) = second.finish().await;
    assert!(exit_ok);

    // GetSession-equivalent durability, asserted from the gateway too.
    let session = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(session["session_id"].as_str(), Some(session_id.as_str()));

    gateway.shutdown().await;
}
