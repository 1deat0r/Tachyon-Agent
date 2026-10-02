//! M12 gate: real SIGKILL of the `tachyon` gateway binary, restart,
//! recover. Automates the M1 manual kill -9 proof as a checked-in test.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tachyon_protocol::{
    Command as ProtoCommand, CommandResult, RequestEnvelope, ResponseEnvelope, decode_frame,
    encode_frame,
};
use tachyon_types::{EventId, SessionId, TaskId};

fn tachyon() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tachyon"))
}

fn scratch(tag: &str) -> PathBuf {
    let unique = uuid::Uuid::now_v7().simple().to_string();
    let dir = std::env::temp_dir().join(format!("m12-{tag}-{}", &unique[16..24]));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path) -> PathBuf {
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let root = serde_json::json!({
        "data_dir": data.display().to_string(),
        "log_level": "warn",
        "provider": { "kind": "fake", "model": "scripted-replay-1" },
    });
    let path = dir.join("config.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
    path
}

fn spawn_gateway(config: &Path) -> Child {
    tachyon()
        .args(["--config", &config.display().to_string(), "gateway"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn tachyon gateway")
}

fn wait_endpoint(data: &Path, child: &mut Child) -> serde_json::Value {
    let endpoint = data.join("gateway.json");
    for _ in 0..200 {
        if endpoint.exists()
            && let Ok(text) = std::fs::read_to_string(&endpoint)
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
            && let Some(sock) = v["socket_path"].as_str()
            && socket_accepts(Path::new(sock))
        {
            return v;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("gateway exited early: {status}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    panic!("gateway endpoint never appeared or socket never accepted");
}

/// Connect with bounded retries: Windows named pipes report
/// `ERROR_PIPE_BUSY` ("All pipe instances are busy") between the readiness
/// probe dropping its client and the accept loop staging the next instance.
async fn connect_retry(socket: &Path) -> std::io::Result<tachyon_gateway::transport::Stream> {
    let mut last = None;
    for _ in 0..40 {
        match tachyon_gateway::transport::connect(socket).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
    Err(last.expect("at least one connect attempt"))
}

/// True when a client can complete a connect (accept loop is live).
/// Uses the gateway transport so Windows named pipes work too (spec §36).
fn socket_accepts(path: &Path) -> bool {
    let target = path.to_owned();
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return false;
    };
    rt.block_on(async {
        matches!(
            tokio::time::timeout(Duration::from_millis(2_000), connect_retry(&target)).await,
            Ok(Ok(_))
        )
    })
}

fn socket_of(endpoint: &serde_json::Value) -> PathBuf {
    PathBuf::from(
        endpoint["socket_path"]
            .as_str()
            .expect("socket_path in endpoint"),
    )
}

fn send_command(socket: &Path, command: ProtoCommand) -> (u16, serde_json::Value) {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let socket = socket.to_owned();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let mut stream = connect_retry(&socket).await.expect("connect");
        let request = RequestEnvelope {
            protocol_version: tachyon_protocol::PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command,
        };
        let frame = encode_frame(&request).expect("encode");
        stream.write_all(&frame).await.expect("write");
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).await.expect("prefix");
        let len = u32::from_le_bytes(prefix) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await.expect("payload");
        let mut framed = prefix.to_vec();
        framed.extend_from_slice(&payload);
        let (response, _): (ResponseEnvelope, usize) = decode_frame(&framed).expect("decode");
        match response.result {
            CommandResult::Ok { payload } => (200, payload),
            CommandResult::Err { code, message } => {
                (400, serde_json::json!({"code": code, "message": message}))
            }
        }
    })
}

#[test]
fn kill_restart_gateway_bin_gates_recovers_task_and_continues() {
    let dir = scratch("kill");
    let config = write_config(&dir);
    let data = dir.join("data");

    let mut child = spawn_gateway(&config);
    let endpoint = wait_endpoint(&data, &mut child);
    let socket = socket_of(&endpoint);

    let (s_status, s_payload) = send_command(
        &socket,
        ProtoCommand::CreateSession {
            workspace_root: None,
        },
    );
    assert_eq!(s_status, 200, "CreateSession: {s_payload}");
    let session_id: SessionId = s_payload["session_id"]
        .as_str()
        .expect("session_id")
        .parse()
        .expect("parse session");

    let (t_status, t_payload) = send_command(
        &socket,
        ProtoCommand::CreateTask {
            session_id,
            objective: "kill_restart gate".to_owned(),
            idempotency_key: None,
        },
    );
    assert_eq!(t_status, 200, "CreateTask: {t_payload}");
    let task_id: TaskId = t_payload["task_id"]
        .as_str()
        .expect("task_id")
        .parse()
        .expect("parse task");

    // SIGKILL the real binary (Unix: kill(2); Windows: TerminateProcess).
    child.kill().expect("SIGKILL gateway");
    let status = child.wait().expect("wait dead gateway");
    assert!(!status.success(), "killed process exit: {status:?}");

    // Restart on the SAME config/data dir: stale endpoint must be evicted.
    let mut child2 = spawn_gateway(&config);
    let endpoint2 = wait_endpoint(&data, &mut child2);
    let socket2 = socket_of(&endpoint2);

    let (g_status, g_payload) = send_command(&socket2, ProtoCommand::GetTask { task_id });
    assert_eq!(g_status, 200, "task recovered after SIGKILL: {g_payload}");
    assert_eq!(
        g_payload["task"]["objective"], "kill_restart gate",
        "identity survived kill"
    );
    let recovered = g_payload["task"]["status"].as_str().unwrap();
    assert_ne!(recovered, "Cancelled", "restart must not cancel");
    let rev_before = g_payload["task"]["revision"].as_u64().unwrap_or(0);

    let (m_status, m_payload) = send_command(
        &socket2,
        ProtoCommand::SendMessage {
            task_id,
            message: "after kill".to_owned(),
        },
    );
    assert_eq!(m_status, 200, "post-restart steering: {m_payload}");

    let (after_status, after_payload) = send_command(&socket2, ProtoCommand::GetTask { task_id });
    assert_eq!(after_status, 200);
    let rev_after = after_payload["task"]["revision"].as_u64().unwrap_or(0);
    assert!(
        rev_after >= rev_before,
        "task continued after restart (rev {rev_before} → {rev_after})"
    );

    let _ = child2.kill();
    let _ = child2.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
