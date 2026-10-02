//! Idempotent `CreateTask` (issue #57 ticket 01): a keyed retry after a
//! lost response replays the byte-identical stored success — one task,
//! one turn — including across a gateway restart; a changed objective
//! under the same key is a typed conflict; no key keeps legacy
//! duplicate semantics; every response carries its minted `turn_seq`.

use std::path::Path;

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_gateway::transport::connect;
use tachyon_protocol::{Command, CommandResult, RequestEnvelope, ResponseEnvelope};
use tachyon_types::EventId;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

mod common;
use common::{code_of, err, ok, test_dir};

/// One raw round trip that keeps the FULL response frame bytes, so a
/// replay can be asserted byte-identically. `request_id` is caller-owned
/// because an envelope echo of the same retry must be comparable.
async fn send_raw(
    socket: &Path,
    request_id: EventId,
    command: Command,
) -> (Vec<u8>, u16, Value, String) {
    let mut stream = connect(socket).await.expect("connect to gateway");
    let request = RequestEnvelope {
        protocol_version: tachyon_protocol::PROTOCOL_VERSION,
        request_id,
        command,
    };
    let bytes = tachyon_protocol::encode_frame(&request).expect("encode request");
    stream.write_all(&bytes).await.expect("send request");
    let mut prefix = [0_u8; tachyon_protocol::FRAME_PREFIX_LEN];
    stream.read_exact(&mut prefix).await.expect("read prefix");
    let len = u32::from_le_bytes(prefix) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.expect("read payload");
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    let (response, _): (ResponseEnvelope, usize) =
        tachyon_protocol::decode_frame(&framed).expect("decode response");
    match response.result {
        CommandResult::Ok { payload } => (framed, 200, payload, String::new()),
        CommandResult::Err { code, message } => {
            (framed, 400, Value::Null, format!("{code}|{message}"))
        }
    }
}

async fn new_session(socket: &Path) -> String {
    let session = ok(
        socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    session["session_id"].as_str().unwrap().to_owned()
}

fn keyed(session: &str, objective: &str, key: Option<&str>) -> Command {
    Command::CreateTask {
        session_id: session.parse().unwrap(),
        objective: objective.to_owned(),
        idempotency_key: key.map(str::to_owned),
    }
}

async fn task_count(socket: &Path, session: &str) -> usize {
    let listed = ok(
        socket,
        Command::ListTasks {
            session_id: Some(session.parse().unwrap()),
        },
    )
    .await;
    listed["tasks"].as_array().unwrap().len()
}

/// Duplicate keyed send → the stored response replays VERBATIM: the full
/// response frames are byte-identical and exactly one task exists.
#[tokio::test]
async fn keyed_retry_replays_byte_identical_response_with_one_task() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;
    let command = keyed(&session, "keyed objective", Some("key-1"));
    let request_id = EventId::generate();

    let (first, first_status, first_payload, _) =
        send_raw(&socket, request_id, command.clone()).await;
    let (second, second_status, second_payload, _) = send_raw(&socket, request_id, command).await;

    assert_eq!(first_status, 200, "original keyed create succeeds");
    assert_eq!(second_status, 200, "keyed retry replays, not errors");
    assert_eq!(
        first, second,
        "the replayed response frame is byte-identical"
    );
    assert_eq!(first_payload, second_payload);
    assert_eq!(first_payload["status"], "Created");
    assert_eq!(first_payload["turn_seq"], 1);
    let task_id = first_payload["task_id"].as_str().unwrap();
    assert_eq!(second_payload["task_id"].as_str().unwrap(), task_id);
    assert_eq!(task_count(&socket, &session).await, 1, "no duplicate task");

    let session_view = ok(
        &socket,
        Command::GetSession {
            session_id: session.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(session_view["turns"].as_array().unwrap().len(), 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Same key, different objective → typed `idempotency_key_conflict` and
/// no state change: the first task/turn stay untouched.
#[tokio::test]
async fn same_key_with_changed_objective_is_a_typed_conflict() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;

    let created = ok(&socket, keyed(&session, "first objective", Some("k"))).await;
    let refused = err(&socket, keyed(&session, "different objective", Some("k"))).await;
    assert_eq!(code_of(&refused), "idempotency_key_conflict");

    assert_eq!(task_count(&socket, &session).await, 1);
    let listed = ok(
        &socket,
        Command::ListTasks {
            session_id: Some(session.parse().unwrap()),
        },
    )
    .await;
    assert_eq!(
        listed["tasks"][0]["id"], created["task_id"],
        "the conflict changed nothing"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Crash window: the key row commits with the task, so a retry after a
/// gateway restart still replays the original response — one task.
#[tokio::test]
async fn keyed_retry_after_restart_still_replays_one_task() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;
    let command = keyed(&session, "survives restart", Some("restart-key"));
    let request_id = EventId::generate();

    let (before, before_status, before_payload, _) =
        send_raw(&socket, request_id, command.clone()).await;
    assert_eq!(before_status, 200);
    gateway.shutdown().await;

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let (after, after_status, after_payload, _) = send_raw(&socket, request_id, command).await;
    assert_eq!(after_status, 200, "retry after restart replays");
    assert_eq!(
        before, after,
        "post-restart replay is byte-identical to the pre-crash response"
    );
    assert_eq!(before_payload, after_payload);
    assert_eq!(task_count(&socket, &session).await, 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Two racing keyed sends resolve through store uniqueness to exactly
/// one task; both clients observe the same success payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_sends_create_exactly_one_task() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;
    let command = keyed(&session, "raced objective", Some("race-key"));

    let (first, second) = tokio::join!(
        common::send(&socket, command.clone()),
        common::send(&socket, command.clone()),
    );
    let (status_a, payload_a, error_a) = first;
    let (status_b, payload_b, error_b) = second;
    assert_eq!(status_a, 200, "winner succeeds: {error_a}");
    assert_eq!(status_b, 200, "loser replays instead of failing: {error_b}");
    assert_eq!(
        payload_a, payload_b,
        "both racers converge on one task's response"
    );
    assert_eq!(task_count(&socket, &session).await, 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Legacy semantics, documented: without a key every send creates a new
/// task and a new turn — the additive `turn_seq` makes both visible.
#[tokio::test]
async fn keyless_duplicate_sends_still_create_two_tasks() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;

    let first = ok(&socket, keyed(&session, "no key here", None)).await;
    let second = ok(&socket, keyed(&session, "no key here", None)).await;
    assert_ne!(first["task_id"], second["task_id"], "legacy duplicates");
    assert_eq!(first["turn_seq"], 1);
    assert_eq!(second["turn_seq"], 2);
    assert_eq!(task_count(&socket, &session).await, 2);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The minted `turn_seq` in the success payload is the stamp `GetSession`
/// reports for that turn — keyless and keyed alike.
#[tokio::test]
async fn create_task_turn_seq_matches_get_session() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;

    let keyed_create = ok(&socket, keyed(&session, "turn one", Some("t"))).await;
    let keyless_create = ok(&socket, keyed(&session, "turn two", None)).await;
    let view = ok(
        &socket,
        Command::GetSession {
            session_id: session.parse().unwrap(),
        },
    )
    .await;
    let turns = view["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    for (payload, turn) in [&keyed_create, &keyless_create].iter().zip(turns) {
        assert_eq!(payload["turn_seq"], turn["turn_seq"]);
        assert_eq!(payload["task_id"], turn["task_id"]);
    }
    assert_eq!(keyed_create["turn_seq"], 1);
    assert_eq!(keyless_create["turn_seq"], 2);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Key validation precedes any state touch: empty and oversized keys are
/// refused with `invalid_idempotency_key`; the 128-byte bound itself is
/// accepted.
#[tokio::test]
async fn empty_and_oversized_idempotency_keys_are_refused() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session = new_session(&socket).await;

    let empty = err(&socket, keyed(&session, "obj", Some(""))).await;
    assert_eq!(code_of(&empty), "invalid_idempotency_key");
    let oversized = "k".repeat(129);
    let refused = err(&socket, keyed(&session, "obj", Some(&oversized))).await;
    assert_eq!(code_of(&refused), "invalid_idempotency_key");
    assert_eq!(
        task_count(&socket, &session).await,
        0,
        "refused keys never create state"
    );

    let boundary = "k".repeat(128);
    let accepted = ok(&socket, keyed(&session, "obj", Some(&boundary))).await;
    assert_eq!(accepted["turn_seq"], 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
