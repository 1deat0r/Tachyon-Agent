//! G4(ii) — the live attach gate: an in-process gateway (test harness
//! only; `tachyon-gateway` is the harness dep of this crate's tests,
//! never display logic) drives the **real** tachyon-tui
//! reader → decoder → state stack: receive events, drop the client,
//! prove the task is untouched (no cancel), respawn from the client's
//! own last-parsed seq, and assert the exact gapless suffix replay.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tachyon_protocol::{Command, CommandResult, PROTOCOL_VERSION, ResponseEnvelope};
use tachyon_tui::{AppState, AttachConfig, ClientEvent, CommandClient, Subscription};
use tachyon_types::{EventId, SessionId, TaskId};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("tachyon-m11-tui-g4-{}-{id}", std::process::id()))
}

/// One timeout window for anything that should already be on the wire.
const WIRE: Duration = Duration::from_secs(5);

/// Creates the session and task with two steering messages; returns the
/// parsed task id and its wire string form.
async fn seed(client: &CommandClient) -> (TaskId, String) {
    let session = client
        .call(Command::CreateSession {
            workspace_root: None,
        })
        .await
        .expect("CreateSession");
    let session_id: SessionId = session["session_id"]
        .as_str()
        .expect("session_id")
        .parse()
        .expect("session id parses");
    let created = client
        .call(Command::CreateTask {
            session_id,
            objective: "gate objective".to_owned(),
            idempotency_key: None,
        })
        .await
        .expect("CreateTask");
    let task_str = created["task_id"].as_str().expect("task_id").to_owned();
    let task: TaskId = task_str.parse().expect("task id parses");
    for message in ["steering one", "steering two"] {
        let task_id: TaskId = task_str.parse().expect("task id parses");
        client
            .call(Command::SendMessage {
                task_id,
                message: message.to_owned(),
            })
            .await
            .expect("SendMessage");
    }
    (task, task_str)
}

/// Drains accepted envelopes until `state` applies `target`; informational
/// frames are skipped (the consumer contract). Returns the accepted seqs.
async fn drain(sub: &mut Subscription, state: &mut AppState, target: i64, tag: &str) -> Vec<i64> {
    let mut seqs: Vec<i64> = Vec::new();
    while state.last_applied_seq < target {
        match tokio::time::timeout(WIRE, sub.recv()).await {
            Ok(Some(ClientEvent::Envelope(envelope))) => {
                seqs.push(envelope.seq);
                state.apply_envelope(envelope);
            }
            Ok(Some(_)) => {}
            Ok(None) => panic!("reader ended during {tag}: received {seqs:?}"),
            Err(error) => panic!("timeout during {tag}: received {seqs:?} ({error})"),
        }
    }
    seqs
}

/// (3) A fresh connection still sees the same task — status and revision
/// unchanged, cancellation nowhere — and its rows parse into TUI state.
/// Returns the still-open probe connection.
async fn assert_survives_drop(address: &Path, revision_before: serde_json::Value) -> CommandClient {
    let probe = CommandClient::connect(address)
        .await
        .expect("probe connection");
    let listed = probe
        .call(Command::ListTasks { session_id: None })
        .await
        .expect("ListTasks after drop");

    // The REAL ListTasks rows must also parse into TUI state (server key
    // names: id/objective/status/revision), not just into the test.
    let mut state_after = AppState::new(None);
    state_after.apply_response(ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: EventId::generate(),
        result: CommandResult::Ok {
            payload: listed.clone(),
        },
    });
    assert_eq!(
        state_after.tasks.len(),
        1,
        "the real TaskSummary rows parse into TUI state"
    );
    assert_eq!(
        state_after.tasks[0].objective, "gate objective",
        "real row keys parse (id/objective/status/revision)"
    );

    let tasks = listed["tasks"].as_array().expect("task list");
    assert_eq!(
        tasks.len(),
        1,
        "the task survives both client connections dropping"
    );
    assert_eq!(
        tasks[0]["status"], "Created",
        "dropping the TUI client cancels nothing"
    );
    assert_eq!(
        tasks[0]["revision"], revision_before,
        "dropping changes no state"
    );
    probe
}

#[tokio::test]
async fn g4ii_dropping_the_client_cancels_nothing_and_respawn_replays_the_gapless_suffix() {
    let dir = test_dir();
    let gateway = tachyon_gateway::start(&dir)
        .await
        .expect("in-process gateway starts");
    let address = gateway.address().to_owned();
    let store = gateway.store();

    // Seed through a command connection — the same call path the TUI sends on.
    let client = CommandClient::connect(&address)
        .await
        .expect("command connection");
    let (task, task_str) = seed(&client).await;

    // (1) The REAL tachyon-tui stack: reader task → cursor → decoder → state.
    let mut sub = Subscription::attach(&address, Some(task), -1, AttachConfig::production());
    let mut state = AppState::new(Some(task));
    let received = drain(&mut sub, &mut state, 2, "the initial replay").await;
    assert_eq!(
        received,
        vec![0, 1, 2],
        "the initial replay arrives in order, exactly once"
    );
    assert_eq!(
        state.objective.as_deref(),
        Some("gate objective"),
        "the real `created` StateEvent decodes into state"
    );
    assert!(
        state
            .conversation
            .iter()
            .any(|line| line.contains("steering one")),
        "message 1 decoded: {:?}",
        state.conversation
    );
    assert!(
        state
            .conversation
            .iter()
            .any(|line| line.contains("steering two")),
        "message 2 decoded: {:?}",
        state.conversation
    );
    let cursor = sub.last_parsed_seq();
    assert_eq!(cursor, 2, "the reader's own last-parsed seq");

    // (2) Read the pre-drop snapshot, then drop BOTH client connections.
    let before = client
        .call(Command::GetTask { task_id: task })
        .await
        .expect("GetTask before drop");
    assert_eq!(before["task"]["status"], "Created");
    let revision_before = before["task"]["revision"].clone();
    drop(sub);
    drop(client);

    // (3) The gate sentence, from a fresh connection: probe returned.
    let probe = assert_survives_drop(&address, revision_before).await;

    // (4) Events keep committing while detached.
    for _ in 0..4 {
        store
            .append_event(&task_str, "synthetic", "{}")
            .await
            .expect("append while detached");
    }

    // (5) Respawn from the client's OWN last-parsed seq: the ack must carry
    // exactly the four missed events — gapless, and nothing at or below the cursor.
    let mut sub2 = Subscription::attach(&address, Some(task), cursor, AttachConfig::production());
    let mut state2 = AppState::new(Some(task));
    let suffix = drain(&mut sub2, &mut state2, 6, "the respawn suffix").await;
    assert_eq!(
        suffix,
        vec![3, 4, 5, 6],
        "exact gapless suffix replay — no seq <= cursor, no gap"
    );
    assert_eq!(
        sub2.last_parsed_seq(),
        6,
        "the respawn cursor advanced on the suffix"
    );

    // (6) The live tail still flows after the respawned replay.
    store
        .append_event(&task_str, "synthetic", "{}")
        .await
        .expect("live append");
    // Informational frames (the trailing replay Ack) may sit ahead of the
    // live tail — the consumer contract skips them, so this gate does too.
    let next = loop {
        match tokio::time::timeout(WIRE, sub2.recv()).await {
            Ok(Some(ClientEvent::Envelope(envelope))) => break envelope,
            Ok(Some(_)) => {}
            Ok(None) => panic!("respawn reader ended before the live tail"),
            Err(error) => panic!("timeout waiting for the live tail ({error})"),
        }
    };
    assert_eq!(next.seq, 7, "the live tail follows the replayed suffix");

    drop(sub2);
    drop(probe);
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
