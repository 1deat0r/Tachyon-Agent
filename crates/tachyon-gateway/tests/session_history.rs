//! Ticket 03 (ACP slice a): ordered Session history replay.
//!
//! External contract under test —
//!
//!   * `GetSession` returns each turn's canonical conversation, derived
//!     read-only from task snapshots + the append-only journal (the same
//!     canonical state `GetTask` serves) in stable turn order;
//!   * the full history payload is byte-identical across a gateway
//!     restart over the same store dir;
//!   * the fetch path performs no writes: session row, task rows,
//!     journal rows, snapshots, effects, and the active-task count are
//!     unchanged before and after `GetSession`;
//!   * works for a legacy session (no root) and a session with a root.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use std::sync::Arc;
use tachyon_gateway::RunningGateway;
use tachyon_gateway::start;
use tachyon_protocol::Command;
use tachyon_store::{JournalEvent, StoreWriter};

mod common;
use common::{ok, test_dir};

/// Creates the workspace directory a test binds as Session root and
/// returns it canonicalized (the exact value the gateway must persist).
fn make_workspace(dir: &Path) -> PathBuf {
    let ws = dir.join("history-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::canonicalize(&ws).unwrap()
}

async fn fetch_session(socket: &Path, session_id: &str) -> Value {
    ok(
        socket,
        Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await
}

/// The canonical user conversation for a scripted steering sequence:
/// exactly what `GetTask`'s `task.conversation` must also report.
fn user_conversation(messages: &[&str]) -> Value {
    Value::Array(
        messages
            .iter()
            .map(|content| json!({"speaker": "user", "content": content}))
            .collect(),
    )
}

/// Opens a rooted session with three tasks; steers the first two so the
/// history carries conversation content and the third stays empty.
async fn rooted_session_with_history(socket: &Path, ws: &Path) -> (String, Vec<String>) {
    let created = ok(
        socket,
        Command::CreateSession {
            workspace_root: Some(ws.display().to_string()),
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();

    let mut task_ids = Vec::new();
    for i in 0..3 {
        let task = ok(
            socket,
            Command::CreateTask {
                session_id: session_id.parse().unwrap(),
                objective: format!("history turn {i}"),
                idempotency_key: None,
            },
        )
        .await;
        task_ids.push(task["task_id"].as_str().unwrap().to_owned());
    }

    // Steering builds conversation content on the first two turns; the
    // third stays empty so a turn with nothing to replay is covered too.
    for (task_id, messages) in [
        (&task_ids[0], vec!["first steering", "second steering"]),
        (&task_ids[1], vec!["only message"]),
    ] {
        for message in messages {
            ok(
                socket,
                Command::SendMessage {
                    task_id: task_id.parse().unwrap(),
                    message: message.to_owned(),
                },
            )
            .await;
        }
    }
    (session_id, task_ids)
}

/// Asserts ticket 01/02 fields are intact and every turn's conversation
/// is the canonical conversation `GetTask` serves.
async fn assert_history_payload(
    before: &Value,
    session_id: &str,
    ws: &Path,
    task_ids: &[String],
    socket: &Path,
) {
    assert_eq!(before["session_id"], session_id);
    assert!(before["created_at"].is_i64(), "{before}");
    assert_eq!(
        before["workspace_root"],
        ws.display().to_string().as_str(),
        "the bound Session root is still reported"
    );

    let turns = before["turns"]
        .as_array()
        .expect("GetSession lists the ordered Session history");
    assert_eq!(turns.len(), 3, "{before}");
    let seqs: Vec<i64> = turns
        .iter()
        .map(|turn| turn["turn_seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3], "stable turn order: {before}");
    for (i, turn) in turns.iter().enumerate() {
        assert_eq!(turn["task_id"], task_ids[i].as_str());
        assert_eq!(turn["status"], "Created", "{turn}");
    }

    // Each turn's conversation is the canonical task conversation — the
    // same state `GetTask` serves, not a second store.
    assert_eq!(
        turns[0]["conversation"],
        user_conversation(&["first steering", "second steering"]),
        "turn 1 replays its steering history: {before}"
    );
    assert_eq!(
        turns[1]["conversation"],
        user_conversation(&["only message"]),
        "turn 2 replays its steering history: {before}"
    );
    assert_eq!(
        turns[2]["conversation"],
        json!([]),
        "a turn with no messages lists an empty conversation, not a null: {before}"
    );
    for (i, task_id) in task_ids.iter().enumerate() {
        let task = ok(
            socket,
            Command::GetTask {
                task_id: task_id.parse().unwrap(),
            },
        )
        .await;
        assert_eq!(
            turns[i]["conversation"],
            task["task"]["conversation"],
            "turn {} conversation equals the canonical GetTask conversation",
            i + 1
        );
    }
}

#[tokio::test]
async fn session_history_replays_ordered_conversation_and_is_byte_identical_across_restart() {
    let dir = test_dir();
    let ws = make_workspace(&dir);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let (session_id, task_ids) = rooted_session_with_history(&socket, &ws).await;
    let before = fetch_session(&socket, &session_id).await;
    assert_history_payload(&before, &session_id, &ws, &task_ids, &socket).await;

    // Real stop over the same store dir — the same shape a process kill
    // leaves on disk (recovery.rs / session_root.rs pattern).
    gateway.shutdown().await;
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let after = fetch_session(&socket, &session_id).await;
    assert_eq!(
        after.to_string(),
        before.to_string(),
        "the full Session history is byte-identical across a gateway restart"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// One durable-state fingerprint: every store fact `GetSession` could
/// conceivably mutate, captured for before/after equality.
struct DurableState {
    session: String,
    task: String,
    journal: Vec<Value>,
    effects: usize,
    active_tasks: Value,
}

fn serialize_events(events: &[JournalEvent]) -> Vec<Value> {
    events
        .iter()
        .map(serde_json::to_value)
        .map(Result::unwrap)
        .collect()
}

async fn durable_state(
    store: &Arc<StoreWriter>,
    socket: &Path,
    session_id: &str,
    task_id: &str,
) -> DurableState {
    let session = store
        .load_session(session_id)
        .await
        .unwrap()
        .expect("the session exists");
    let task = store
        .load_task(task_id)
        .await
        .unwrap()
        .expect("the task exists");
    let journal = store.load_events_since(task_id, -1).await.unwrap();
    let effects = store.load_effects_for_task(task_id).await.unwrap();
    let status = ok(socket, Command::GetStatus).await;
    DurableState {
        session: format!("{session:?}"),
        task: format!("{task:?}"),
        journal: serialize_events(&journal),
        effects: effects.len(),
        active_tasks: status["active_tasks"].clone(),
    }
}

#[tokio::test]
async fn session_history_fetch_is_read_only_and_mints_no_state() {
    let dir = test_dir();
    let gateway: RunningGateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    // Legacy session: created without a root, history must still work.
    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();
    let task = ok(
        &socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "read-only probe".to_owned(),
            idempotency_key: None,
        },
    )
    .await;
    let task_id: String = task["task_id"].as_str().unwrap().to_owned();
    ok(
        &socket,
        Command::SendMessage {
            task_id: task_id.parse().unwrap(),
            message: "legacy steering".to_owned(),
        },
    )
    .await;

    let store = gateway.store();
    let before = durable_state(&store, &socket, &session_id, &task_id).await;

    let first = fetch_session(&socket, &session_id).await;
    let second = fetch_session(&socket, &session_id).await;

    // Legacy (rootless) sessions replay history too.
    assert!(
        first["workspace_root"].is_null(),
        "legacy sessions still report an absent root: {first}"
    );
    assert_eq!(
        first["turns"][0]["conversation"],
        user_conversation(&["legacy steering"]),
        "the legacy session replays its conversation: {first}"
    );
    assert_eq!(first, second, "repeated fetches return identical payloads");

    let after = durable_state(&store, &socket, &session_id, &task_id).await;
    assert_eq!(
        before.session, after.session,
        "fetching a session must not mutate its durable row"
    );
    assert_eq!(
        before.task, after.task,
        "fetching history must not mutate the task row (status, revision, snapshot)"
    );
    assert_eq!(
        before.journal, after.journal,
        "the read path appends no journal rows and rewrites none"
    );
    assert_eq!(
        before.effects, after.effects,
        "the read path creates no effect rows"
    );
    assert_eq!(
        before.active_tasks, after.active_tasks,
        "the read path spawns no supervisors"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
