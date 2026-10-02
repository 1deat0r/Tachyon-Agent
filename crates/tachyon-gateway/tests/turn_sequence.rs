//! Ticket 02 (ACP slice a): monotonic per-session turn sequence.
//!
//! External contract under test —
//!
//!   * `CreateTask` stamps each task with the next per-session turn
//!     sequence atomically; `GetSession` lists the session's turns in
//!     strict ascending order with task id and canonical status;
//!   * ordering is deterministic under same-instant rapid and racing
//!     creates (no wall-clock tie-breaking);
//!   * the ordered skeleton is byte-identical across a gateway restart;
//!   * the backward-compatible `GetSession` fields from ticket 01
//!     (`session_id`, `created_at`, `workspace_root`) remain unchanged.

use std::path::{Path, PathBuf};

use tachyon_gateway::start;
use tachyon_protocol::Command;

mod common;
use common::{ok, test_dir};

/// Creates the workspace directory a test binds as Session root and
/// returns it canonicalized (the exact value the gateway must persist).
fn make_workspace(dir: &Path) -> PathBuf {
    let ws = dir.join("turn-seq-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::canonicalize(&ws).unwrap()
}

async fn fetch_session(socket: &Path, session_id: &str) -> serde_json::Value {
    ok(
        socket,
        Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await
}

#[tokio::test]
async fn get_session_lists_turns_in_strict_sequence_order() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();

    // A fresh session reports an empty turn list, not a missing field.
    let empty = fetch_session(&socket, &session_id).await;
    assert_eq!(
        empty["turns"],
        serde_json::json!([]),
        "an empty session lists zero turns: {empty}"
    );

    // Same-instant rapid creates: three back-to-back tasks, no sleeps —
    // ordering must not come from wall-clock tie-breaking.
    let mut task_ids = Vec::new();
    let mut statuses = Vec::new();
    for i in 0..3 {
        let task = ok(
            &socket,
            Command::CreateTask {
                session_id: session_id.parse().unwrap(),
                objective: format!("turn {i}"),
                idempotency_key: None,
            },
        )
        .await;
        task_ids.push(task["task_id"].as_str().unwrap().to_owned());
        statuses.push(task["status"].as_str().unwrap().to_owned());
    }

    // A racing burst over independent connections: completion order is
    // arbitrary, but each task must still land on its own sequence slot.
    let mut racing = Vec::new();
    for i in 0..3 {
        let socket = socket.clone();
        let sid = session_id.clone();
        racing.push(tokio::spawn(async move {
            let task = ok(
                &socket,
                Command::CreateTask {
                    session_id: sid.parse().unwrap(),
                    objective: format!("racing turn {i}"),
                    idempotency_key: None,
                },
            )
            .await;
            task["task_id"].as_str().unwrap().to_owned()
        }));
    }
    let mut racing_ids = Vec::new();
    for handle in racing {
        racing_ids.push(handle.await.unwrap());
    }

    let fetched = fetch_session(&socket, &session_id).await;
    // Backward-compatible fields from ticket 01 stay exactly as they were.
    assert_eq!(fetched["session_id"], session_id.as_str());
    assert!(fetched["created_at"].is_i64(), "{fetched}");
    assert!(fetched["workspace_root"].is_null(), "{fetched}");

    let turns = fetched["turns"]
        .as_array()
        .expect("GetSession lists an ordered turn skeleton");
    assert_eq!(turns.len(), 6, "every created task is a turn: {fetched}");

    // Strictly ascending, dense sequence starting at 1 — double assigns
    // or wall-clock ties would break this.
    let seqs: Vec<i64> = turns
        .iter()
        .map(|turn| turn["turn_seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6], "turn sequence: {fetched}");

    let listed: Vec<&str> = turns
        .iter()
        .map(|turn| turn["task_id"].as_str().unwrap())
        .collect();

    // The sequential rapid creates keep their creation order and report
    // each task's canonical status.
    for (i, task_id) in task_ids.iter().enumerate() {
        assert_eq!(listed[i], task_id, "turn {} task id", i + 1);
        assert_eq!(
            turns[i]["status"].as_str().unwrap(),
            statuses[i].as_str(),
            "turn {} status",
            i + 1
        );
        assert_eq!(statuses[i], "Created", "canonical fresh-task status");
    }

    // The racing burst accounts for exactly the remaining slots, once each.
    let mut tail: Vec<String> = listed[3..].iter().map(|id| (*id).to_owned()).collect();
    let mut expected = racing_ids.clone();
    tail.sort();
    expected.sort();
    assert_eq!(tail, expected, "racing creates each own one turn slot");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn session_turn_order_survives_gateway_restart() {
    let dir = test_dir();
    let ws = make_workspace(&dir);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: Some(ws.display().to_string()),
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();

    let mut task_ids = Vec::new();
    for i in 0..3 {
        let task = ok(
            &socket,
            Command::CreateTask {
                session_id: session_id.parse().unwrap(),
                objective: format!("durable turn {i}"),
                idempotency_key: None,
            },
        )
        .await;
        task_ids.push(task["task_id"].as_str().unwrap().to_owned());
    }

    let before = fetch_session(&socket, &session_id).await;
    let listed: Vec<&str> = before["turns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|turn| turn["task_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        listed,
        task_ids.iter().map(String::as_str).collect::<Vec<_>>()
    );

    // Real stop over the same store dir — the same shape a process kill
    // leaves on disk (recovery.rs / session_root.rs pattern).
    gateway.shutdown().await;
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let after = fetch_session(&socket, &session_id).await;
    assert_eq!(
        after, before,
        "the ordered turn skeleton is byte-identical across a gateway restart"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
