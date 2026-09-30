//! M13 T3/T4 and `comp[ipc_frame]` (spec §43).
//!
//! - T3: local gateway command round trip — `Ping` from framed write to
//!   decoded response on the same socket — must stay under 5 ms p95.
//! - T4: first visible task event — `SendMessage` request write to the
//!   `Journal` frame observed by a subscribed client on another socket —
//!   must stay under 50 ms p95. This is the operator-visible path: a user
//!   command turns into a journalled event another connection can render.
//! - `comp[ipc_frame]`: pure frame encode + decode cost, no socket, no
//!   gateway (the serialization half of the IPC budget).
//!
//! Ignore-gated; the M13 ledger runs it with
//! `cargo test --release … -- --ignored --nocapture`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_gateway::transport::{Stream, connect};
use tachyon_protocol::{
    Command, CommandResult, FRAME_PREFIX_LEN, PROTOCOL_VERSION, RequestEnvelope, ResponseEnvelope,
    ServerFrame, decode_frame, decode_server_frame, encode_frame,
};
use tachyon_types::EventId;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("tachyon-m13-{}-{id}", std::process::id()))
}

/// One gateway connection over the real transport.
struct Client {
    stream: Stream,
}

impl Client {
    async fn open(address: &Path) -> Self {
        let stream = connect(address).await.expect("connect to gateway");
        Self { stream }
    }

    /// Sends `command` and returns the response, skipping event frames
    /// (they belong to a subscription on another socket).
    async fn request(&mut self, command: Command) -> ResponseEnvelope {
        let request = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command,
        };
        let bytes = encode_frame(&request).expect("encode request");
        self.request_bytes(&bytes).await
    }

    /// Sends an already encoded request and returns its response, skipping
    /// event frames (they belong to a subscription on another socket).
    async fn request_bytes(&mut self, bytes: &[u8]) -> ResponseEnvelope {
        self.stream.write_all(bytes).await.expect("send request");
        loop {
            match self.frame().await {
                ServerFrame::Response(response) => return response,
                ServerFrame::Event(_) => {}
            }
        }
    }

    async fn call(&mut self, command: Command) -> Value {
        match self.request(command).await.result {
            CommandResult::Ok { payload } => payload,
            CommandResult::Err { code, message } => panic!("command failed {code}: {message}"),
        }
    }

    async fn frame(&mut self) -> ServerFrame {
        let bytes = self.frame_bytes().await;
        let (frame, _used) =
            decode_server_frame(&bytes).expect("tagged ServerFrame from the gateway");
        frame
    }

    async fn frame_within(&mut self, timeout: Duration) -> Option<ServerFrame> {
        match tokio::time::timeout(timeout, self.frame_bytes()).await {
            Ok(bytes) => Some(
                decode_server_frame(&bytes)
                    .expect("tagged ServerFrame from the gateway")
                    .0,
            ),
            Err(_) => None,
        }
    }

    async fn frame_bytes(&mut self) -> Vec<u8> {
        let mut prefix = [0_u8; FRAME_PREFIX_LEN];
        self.stream
            .read_exact(&mut prefix)
            .await
            .expect("frame prefix");
        let len = u32::from_le_bytes(prefix) as usize;
        let mut payload = vec![0_u8; len];
        self.stream
            .read_exact(&mut payload)
            .await
            .expect("frame body");
        let mut framed = prefix.to_vec();
        framed.extend_from_slice(&payload);
        framed
    }
}

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    (p50, p95)
}

/// Creates a session and a task through the protocol; returns task id.
async fn seeded_task(client: &mut Client) -> String {
    let session = client
        .call(Command::CreateSession {
            workspace_root: None,
        })
        .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = client
        .call(Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "m13 perf task".to_owned(),
        })
        .await;
    task["task_id"].as_str().unwrap().to_owned()
}

async fn replayed_creation_ack(address: &Path, task_id: tachyon_types::TaskId) -> ResponseEnvelope {
    let mut subscription = Client::open(address).await;
    subscription
        .request(Command::Subscribe {
            task_id,
            after_seq: -1,
        })
        .await
}

fn assert_creation_replay(response: ResponseEnvelope, task_id: tachyon_types::TaskId) {
    let CommandResult::Ok { payload } = response.result else {
        panic!("subscription succeeds");
    };
    assert_eq!(payload["task_id"], task_id.to_string());
    let events = payload["events"].as_array().expect("replay events array");
    let first = events.first().expect("created event is replayed");
    assert_eq!(first["seq"], 0);
    assert_eq!(first["kind"], "created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "M13 perf target: release mode, run with --ignored"]
async fn t3_local_command_p95_under_5ms() {
    let dir = test_dir();
    let gateway = start(&dir).await.expect("gateway starts");
    let address = gateway.address().to_owned();

    let mut client = Client::open(&address).await;
    // Warmup: connection setup, first dispatch, tokio worker parking.
    for _ in 0..50 {
        let response = client.request(Command::Ping).await;
        assert!(matches!(response.result, CommandResult::Ok { .. }));
    }

    let samples = 200_usize;
    let mut latencies = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let response = client.request(Command::Ping).await;
        latencies.push(start.elapsed());
        assert!(matches!(response.result, CommandResult::Ok { .. }));
    }

    let (p50, p95) = percentiles(latencies);
    println!(
        "perf[T3] n={samples} p50={p50:?} p95={p95:?} target p95<5ms {}",
        if p95 < Duration::from_millis(5) {
            "PASS"
        } else {
            "MISS"
        }
    );
    assert!(
        p95 < Duration::from_millis(5),
        "T3 miss: p95={p95:?} >= 5ms"
    );
    println!("perf[T3] PASS");

    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "M13 perf target: release mode, run with --ignored"]
async fn t4_first_task_event_p95_under_50ms() {
    let dir = test_dir();
    let gateway = start(&dir).await.expect("gateway starts");
    let address = gateway.address().to_owned();

    let mut command = Client::open(&address).await;
    let task_id: tachyon_types::TaskId = seeded_task(&mut command).await.parse().unwrap();

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id,
            after_seq: 0,
        })
        .await;

    let samples = 100_usize;
    let mut latencies = Vec::with_capacity(samples);
    for index in 0..samples {
        let start = Instant::now();
        let (response, frame) = tokio::join!(
            command.request(Command::SendMessage {
                task_id,
                message: format!("steer {index}"),
            }),
            subscription.frame_within(Duration::from_secs(10)),
        );
        latencies.push(start.elapsed());
        assert!(
            matches!(response.result, CommandResult::Ok { .. }),
            "steering message accepted"
        );
        let Some(ServerFrame::Event(envelope)) = frame else {
            panic!("sample {index}: expected a task event frame on the subscription");
        };
        assert_eq!(envelope.task_id, task_id);
    }

    let (p50, p95) = percentiles(latencies);
    println!(
        "perf[T4] n={samples} p50={p50:?} p95={p95:?} target p95<50ms {}",
        if p95 < Duration::from_millis(50) {
            "PASS"
        } else {
            "MISS"
        }
    );
    assert!(
        p95 < Duration::from_millis(50),
        "T4 miss: p95={p95:?} >= 50ms"
    );
    println!("perf[T4] PASS");

    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Post-MVP TTFR measurement: run in release mode with --ignored"]
async fn e2e_create_task_to_first_replayed_entry() {
    let dir = test_dir();
    let gateway = start(&dir).await.expect("gateway starts");
    let address = gateway.address().to_owned();

    let mut command = Client::open(&address).await;
    let session = command
        .call(Command::CreateSession {
            workspace_root: None,
        })
        .await;
    let session_id: tachyon_types::SessionId =
        session["session_id"].as_str().unwrap().parse().unwrap();

    // Prime the gateway, storage connection, and subscription path before
    // measuring the steady-state user path. Every warmup uses a fresh task
    // and subscriber, just like a measured sample.
    for _ in 0..10 {
        let task = command
            .call(Command::CreateTask {
                session_id,
                objective: "m15 ttfr warmup".to_owned(),
            })
            .await;
        let task_id: tachyon_types::TaskId = task["task_id"].as_str().unwrap().parse().unwrap();
        let response = replayed_creation_ack(&address, task_id).await;
        assert_creation_replay(response, task_id);
    }

    let samples = 100_usize;
    let mut latencies = Vec::with_capacity(samples);
    for _ in 0..samples {
        let create_request = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command: Command::CreateTask {
                session_id,
                objective: "m15 ttfr measured task".to_owned(),
            },
        };
        let create_bytes = encode_frame(&create_request).expect("encode CreateTask request");

        // t0: immediately before writing CreateTask to the already-running
        // gateway. Session and gateway setup, plus request encoding, are out
        // of the timed window.
        let start = Instant::now();
        let created = command.request_bytes(&create_bytes).await;
        let CommandResult::Ok { payload: created } = created.result else {
            panic!("CreateTask succeeds");
        };
        let task_id: tachyon_types::TaskId = created["task_id"].as_str().unwrap().parse().unwrap();

        let response = replayed_creation_ack(&address, task_id).await;
        // t1: the Subscribe response is fully decoded, including its replay
        // array. Replay entries are carried in this response payload, not as
        // separate ServerFrame::Event frames.
        latencies.push(start.elapsed());
        assert_creation_replay(response, task_id);
    }

    let (p50, p95) = percentiles(latencies);
    println!(
        "perf[T6] n={samples} p50={p50:?} p95={p95:?} (CreateTask write → Subscribe replay response decode; no target)"
    );

    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "M13 perf component: release mode, run with --ignored"]
fn comp_ipc_frame_encode_decode() {
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command: Command::ListTasks { session_id: None },
    };

    let samples = 5_000_usize;
    let mut latencies = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let bytes = encode_frame(&request).expect("encode request");
        let (decoded, used): (RequestEnvelope, usize) =
            decode_frame(&bytes).expect("decode request");
        latencies.push(start.elapsed());
        assert_eq!(decoded.request_id, request.request_id);
        assert_eq!(used, bytes.len());
    }

    let (p50, p95) = percentiles(latencies);
    println!("comp[ipc_frame] n={samples} p50={p50:?} p95={p95:?} (encode+decode, no socket)");
}
