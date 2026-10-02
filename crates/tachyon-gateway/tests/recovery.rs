//! Milestone 1 gate: create a task, restart the gateway process, recover
//! the same task/session/state, and continue.
//!
//! The restart here is a real stop (socket released, tasks dropped) and a
//! fresh `start` over the same data directory — the same code path a
//! process kill exercises, minus the signal.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_gateway::transport::connect;
use tachyon_protocol::{Command, CommandResult, RequestEnvelope, ResponseEnvelope};
use tachyon_types::EventId;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("tachyon-gw-{}-{id}", std::process::id()))
}

async fn call(socket: &Path, command: Command) -> Value {
    let mut stream = connect(socket).await.unwrap();
    let request = RequestEnvelope {
        protocol_version: tachyon_protocol::PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command,
    };
    let bytes = tachyon_protocol::encode_frame(&request).unwrap();
    stream.write_all(&bytes).await.unwrap();
    let mut prefix = [0_u8; tachyon_protocol::FRAME_PREFIX_LEN];
    stream.read_exact(&mut prefix).await.unwrap();
    let len = u32::from_le_bytes(prefix) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.unwrap();
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    let (response, _): (ResponseEnvelope, usize) = tachyon_protocol::decode_frame(&framed).unwrap();
    match response.result {
        CommandResult::Ok { payload } => payload,
        CommandResult::Err { code, message } => panic!("command failed {code}: {message}"),
    }
}

#[tokio::test]
async fn restart_recovers_task_and_continues() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let session = call(
        &socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = call(
        &socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "gate probe".to_owned(),
            idempotency_key: None,
        },
    )
    .await;
    let task_id: String = task["task_id"].as_str().unwrap().to_owned();

    call(
        &socket,
        Command::SendMessage {
            task_id: task_id.parse().unwrap(),
            message: "before restart".to_owned(),
        },
    )
    .await;
    gateway.shutdown().await;

    // Fresh process-equivalent over the same data directory.
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let fetched = call(
        &socket,
        Command::GetTask {
            task_id: task_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(fetched["task"]["revision"], 1);
    assert_eq!(fetched["task"]["objective"], "gate probe");
    assert_eq!(fetched["task"]["status"], "Created");

    let continued = call(
        &socket,
        Command::SendMessage {
            task_id: task_id.parse().unwrap(),
            message: "after restart".to_owned(),
        },
    )
    .await;
    assert_eq!(continued["task"]["revision"], 2);

    let listed = call(&socket, Command::ListTasks { session_id: None }).await;
    assert_eq!(listed["tasks"].as_array().unwrap().len(), 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
