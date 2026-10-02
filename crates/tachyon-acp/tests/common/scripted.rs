//! Scripted gateway fixture (acp-adapter-lifecycle ticket 03): speaks
//! the real length-prefixed frame protocol on a Unix socket and writes
//! a real `gateway.json` endpoint file, but every command answer and
//! every pushed event comes from an explicit per-test script.
//!
//! Why a fixture exists at all: the live gateway's default run policy
//! never `Ask`s (see `tachyon-gateway/tests/restart_approval.rs`), and
//! the adapter drains its subscription eagerly, so neither a
//! `WaitingApproval` bounce nor a `ResyncRequired` overflow can be
//! provoked deterministically against a live run. The scripted peer
//! makes both reproducible on demand — the adapter cannot tell the
//! difference (same wire protocol, same endpoint file).
#![allow(dead_code)] // each test target uses a subset

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tachyon_gateway::EndpointInfo;
use tachyon_gateway::transport::{Listener, Stream};
use tachyon_protocol::{
    Command, CommandResult, EventEnvelope, FRAME_PREFIX_LEN, GatewayEvent, MAX_FRAME_BYTES,
    PROTOCOL_VERSION, RequestEnvelope, ResponseEnvelope, ServerFrame, decode_frame,
    encode_server_frame,
};
use tachyon_types::{EventId, TaskId, Timestamp};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// The one task every scripted `CreateTask` hands back (any valid UUID).
pub const SCRIPT_TASK_ID: &str = "01990f9e-4444-7000-8000-000000000000";

/// One replay row as the subscribe ack carries it (`payload` is the
/// journalled JSON document, encoded as a string — the real store row
/// shape the adapter's replay parser expects).
pub struct ReplayRow {
    /// Per-task sequence of the row.
    pub seq: i64,
    /// Journal kind (`agent_message`, `status`, …).
    pub kind: &'static str,
    /// Raw journalled payload.
    pub payload: Value,
}

/// One frame the fixture pushes on the subscription right after it has
/// acked a `Subscribe`.
pub enum Step {
    /// A journalled event frame (`GatewayEvent::Journal`).
    Journal {
        /// Envelope sequence.
        seq: i64,
        /// Journal kind.
        kind: &'static str,
        /// Raw journalled payload.
        payload: Value,
    },
    /// The overflow notice (`GatewayEvent::ResyncRequired`), shaped
    /// exactly like the gateway's: envelope `seq` equals `after_seq`.
    Resync {
        /// Cursor the client is told to resume from.
        after_seq: i64,
    },
}

/// The script for ONE `Subscribe` call: the replay rows returned in the
/// ack, then the frames pushed immediately after it.
pub struct Subscription {
    /// Rows carried in the ack's `events` array.
    pub replay: Vec<ReplayRow>,
    /// Frames pushed right after the ack.
    pub post: Vec<Step>,
}

/// The whole fixture script: one answer per `GetTask` call and one
/// entry per `Subscribe` call, each consumed in order. Exhausting any
/// script makes the fixture refuse with `script_exhausted` so a
/// mis-sequenced test fails loudly instead of hanging.
pub struct Script {
    /// `GetTask` answers in call order; each entry is the `task` object.
    pub get_tasks: Vec<Value>,
    /// `Subscribe` scripts in call order.
    pub subscribes: Vec<Subscription>,
}

/// A `task` snapshot with a non-terminal status (the fresh-state read
/// before `StartRun`, and any pre-terminal bounce).
pub fn task_status(status: &str) -> Value {
    json!({ "status": status, "conversation": [] })
}

/// A terminal `Completed` snapshot carrying an agent conversation tail.
pub fn task_completed(tail: &str) -> Value {
    json!({
        "status": "Completed",
        "conversation": [{"speaker": "agent", "content": tail}],
    })
}

/// A journalled `agent_message` payload (the shape that maps to an ACP
/// `agent_message_chunk`).
pub fn agent_payload(message: &str) -> Value {
    json!({ "t": "agent_message", "v": { "message": message } })
}

/// A journalled `status` payload (settlement signal only — the adapter
/// never parses it; `GetTask` decides the status).
pub fn status_payload(status: &str) -> Value {
    json!({ "t": "status", "v": { "status": status } })
}

/// The replay-row JSON shape the store's `load_events_since` ack rows
/// carry (`payload` as a JSON string).
fn replay_row_json(row: &ReplayRow) -> Value {
    json!({
        "seq": row.seq,
        "kind": row.kind,
        "payload": row.payload.to_string(),
    })
}

struct ScriptState {
    /// Unconsumed `GetTask` answers.
    get_tasks: Mutex<VecDeque<Value>>,
    /// Unconsumed `Subscribe` scripts.
    subscribes: Mutex<VecDeque<Subscription>>,
    /// `after_seq` of every `Subscribe` seen, in order.
    cursors: Mutex<Vec<i64>>,
    /// Total `GetTask` calls observed.
    get_task_calls: AtomicUsize,
}

impl ScriptState {
    fn new(script: Script) -> Self {
        Self {
            get_tasks: Mutex::new(script.get_tasks.into()),
            subscribes: Mutex::new(script.subscribes.into()),
            cursors: Mutex::new(Vec::new()),
            get_task_calls: AtomicUsize::new(0),
        }
    }
}

/// A running scripted gateway: endpoint file + listening socket, driven
/// entirely by the [`Script`] it started with.
pub struct ScriptedGateway {
    dir: PathBuf,
    state: Arc<ScriptState>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl ScriptedGateway {
    /// Binds `<dir>/gateway.sock`, writes `<dir>/gateway.json`, and
    /// serves the script until [`ScriptedGateway::shutdown`].
    #[must_use]
    pub fn start(dir: &Path, script: Script) -> Self {
        std::fs::create_dir_all(dir).expect("fixture data dir");
        let socket = dir.join("gateway.sock");
        let listener = Listener::bind(&socket).expect("bind fixture socket");
        let info = EndpointInfo {
            socket_path: socket,
            pid: std::process::id(),
            started_at_micros: Timestamp::now().as_micros(),
            protocol_version: PROTOCOL_VERSION,
        };
        std::fs::write(
            dir.join("gateway.json"),
            serde_json::to_vec_pretty(&info).expect("endpoint serializes"),
        )
        .expect("write endpoint file");
        let state = Arc::new(ScriptState::new(script));
        let task_state = Arc::clone(&state);
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok(stream) = listener.accept().await else {
                    break;
                };
                let conn_state = Arc::clone(&task_state);
                tokio::spawn(async move {
                    let _ignored = serve_connection(stream, conn_state).await;
                });
            }
        });
        Self {
            dir: dir.to_owned(),
            state,
            accept_task,
        }
    }

    /// Every `Subscribe`'s `after_seq`, in call order — the cursor
    /// continuity proof.
    #[must_use]
    pub fn subscribe_cursors(&self) -> Vec<i64> {
        self.state.cursors.lock().expect("cursor lock").clone()
    }

    /// How many `GetTask` round trips the adapter performed.
    #[must_use]
    pub fn get_task_calls(&self) -> usize {
        self.state.get_task_calls.load(Ordering::SeqCst)
    }

    /// Stops accepting; connection handlers end on their own when the
    /// adapter's sockets close.
    pub fn shutdown(&self) {
        self.accept_task.abort();
        let _ignored = std::fs::remove_file(self.dir.join("gateway.sock"));
        let _ignored = std::fs::remove_file(self.dir.join("gateway.json"));
    }
}

fn ok(payload: Value) -> CommandResult {
    CommandResult::Ok { payload }
}

fn refused(code: &str, message: &str) -> CommandResult {
    CommandResult::Err {
        code: code.to_owned(),
        message: message.to_owned(),
    }
}

/// Serves one adapter connection: request/response over the real frame
/// protocol, with scripted pushes injected after each `Subscribe` ack.
async fn serve_connection(mut stream: Stream, state: Arc<ScriptState>) -> std::io::Result<()> {
    loop {
        let Some(framed) = read_frame(&mut stream).await? else {
            return Ok(()); // adapter closed the connection
        };
        let (request, _): (RequestEnvelope, usize) =
            decode_frame(&framed).map_err(|error| std::io::Error::other(error.to_string()))?;
        let request_id = request.request_id;
        let mut post: Vec<Step> = Vec::new();
        let result = match &request.command {
            Command::Ping => ok(json!({
                "pong": true,
                "protocol_version": PROTOCOL_VERSION,
            })),
            Command::GetSession { session_id } => ok(json!({
                "session_id": session_id.to_string(),
                "workspace_root": "/tmp",
            })),
            Command::CreateTask { .. } => ok(json!({ "task_id": SCRIPT_TASK_ID })),
            Command::GetTask { .. } => {
                state.get_task_calls.fetch_add(1, Ordering::SeqCst);
                match state.get_tasks.lock().expect("get-task lock").pop_front() {
                    Some(task) => ok(json!({ "task": task })),
                    None => refused(
                        "script_exhausted",
                        "no scripted GetTask answer left for this call",
                    ),
                }
            }
            Command::StartRun { .. } => ok(json!({ "started": true })),
            Command::Subscribe { after_seq, .. } => {
                state.cursors.lock().expect("cursor lock").push(*after_seq);
                let subscription = state.subscribes.lock().expect("subscribe lock").pop_front();
                match subscription {
                    Some(subscription) => {
                        post = subscription.post;
                        let events: Vec<Value> =
                            subscription.replay.iter().map(replay_row_json).collect();
                        ok(json!({
                            "subscribed": true,
                            "task_id": SCRIPT_TASK_ID,
                            "after_seq": after_seq,
                            "last_seq": events
                                .iter()
                                .filter_map(|row| row.get("seq").and_then(Value::as_i64))
                                .max()
                                .unwrap_or(*after_seq),
                            "events": events,
                        }))
                    }
                    None => refused(
                        "script_exhausted",
                        "no scripted Subscribe left for this call",
                    ),
                }
            }
            _ => refused(
                "unexpected_command",
                "fixture script did not cover this command",
            ),
        };
        write_frame(
            &mut stream,
            &ServerFrame::Response(ResponseEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id,
                result,
            }),
        )
        .await?;
        for step in post {
            let frame = match step {
                Step::Journal { seq, kind, payload } => ServerFrame::Event(EventEnvelope {
                    seq,
                    event_id: EventId::generate(),
                    schema_version: PROTOCOL_VERSION,
                    task_id: task_id(),
                    timestamp: Timestamp::now(),
                    event: GatewayEvent::Journal {
                        kind: kind.to_owned(),
                        payload,
                    },
                }),
                Step::Resync { after_seq } => ServerFrame::Event(EventEnvelope {
                    seq: after_seq,
                    event_id: EventId::generate(),
                    schema_version: PROTOCOL_VERSION,
                    task_id: task_id(),
                    timestamp: Timestamp::now(),
                    event: GatewayEvent::ResyncRequired {
                        task_id: task_id(),
                        after_seq,
                    },
                }),
            };
            write_frame(&mut stream, &frame).await?;
        }
    }
}

fn task_id() -> TaskId {
    SCRIPT_TASK_ID.parse().expect("script task id parses")
}

/// Reads one length-prefixed frame; `None` at a clean EOF.
async fn read_frame(stream: &mut Stream) -> std::io::Result<Option<Vec<u8>>> {
    let mut prefix = [0_u8; FRAME_PREFIX_LEN];
    match stream.read_exact(&mut prefix).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let len = u32::from_le_bytes(prefix) as usize;
    if len > MAX_FRAME_BYTES - FRAME_PREFIX_LEN {
        return Err(std::io::Error::other(
            "fixture frame exceeds the frame limit",
        ));
    }
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await?;
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    Ok(Some(framed))
}

async fn write_frame(stream: &mut Stream, frame: &ServerFrame) -> std::io::Result<()> {
    let bytes =
        encode_server_frame(frame).map_err(|error| std::io::Error::other(error.to_string()))?;
    stream.write_all(&bytes).await?;
    stream.flush().await
}
