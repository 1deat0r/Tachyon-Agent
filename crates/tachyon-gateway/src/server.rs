//! Local gateway server: socket lifecycle, frame loop, command dispatch.
//!
//! One `RunningGateway` owns the listener, the [`StoreWriter`], and the
//! supervisor registry. Each connection is served by its own task; slow
//! clients delay only themselves. Shutdown cancels the accept loop and
//! waits for in-flight connections before releasing the runtime dir.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde_json::{Value, json};
use tachyon_core::driver::{DriveError, DriveHost, EvidenceMode, RunPlan, drive};
use tachyon_core::runtime::{EvidenceRequest, RuntimeBounds};
use tachyon_core::{CoreError, SupervisorHandle, TaskStatus, create_task, recover_task};
use tachyon_models::ModelProvider;
use tachyon_policy::Policy;
use tachyon_protocol::{
    Command, CommandResult, EventEnvelope, GatewayEvent, PROTOCOL_VERSION, RequestEnvelope,
    ResponseEnvelope, ServerFrame, check_version, decode_frame, encode_server_frame,
};
use tachyon_store::{CommitNotice, StoreWriter};
use tachyon_tools::workspace::WorkspaceLease;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool, credential::CredentialBroker};
use tachyon_types::{ApprovalId, EventId, SessionId, TaskId, Timestamp, WorkspaceId};
use tachyon_verify::{AcceptanceContract, Clause, CommandCheck, VerificationRisk};
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::{Mutex, Notify, broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::endpoint::{
    ClaimPaths, EndpointError, claim_runtime_dir, release_runtime_dir, write_endpoint,
};
use crate::transport::{Listener, Stream};

/// Errors produced by the gateway.
#[derive(Debug, Error)]
pub enum GatewayError {
    /// Endpoint claim/setup failure.
    #[error("endpoint error: {0}")]
    Endpoint(#[from] EndpointError),
    /// Durability failure.
    #[error("store error: {0}")]
    Store(#[from] tachyon_store::StoreError),
    /// Task kernel failure.
    #[error("core error: {0}")]
    Core(#[from] CoreError),
    /// Wire protocol failure.
    #[error("protocol error: {0}")]
    Protocol(#[from] tachyon_protocol::ProtocolError),
    /// Socket I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON failure.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Another gateway owns the runtime.
    #[error("gateway already running (pid {pid})")]
    AlreadyRunning {
        /// Owning pid, when known.
        pid: u32,
    },
}

/// Shared mutable gateway state behind one async mutex.
struct GatewayState {
    store: Arc<StoreWriter>,
    supervisors: Mutex<HashMap<TaskId, SupervisorHandle>>,
    /// Runtime data directory: per-task mutation state and artifact
    /// spools live here, never inside a source tree.
    data_dir: PathBuf,
    /// Configured model provider plus its redaction registry (plan item 5).
    runtime: GatewayRuntime,
    /// Tasks with a `StartRun` admitted but not yet finished: one run in
    /// flight per task, checked and inserted under one lock.
    /// In-flight runs by task id, each with its cooperative cancel
    /// token (M11 cancellation drain): `Command::Cancel` fires the token
    /// so the driver halts at its next stage boundary; the entry itself
    /// is removed only by prepare failure or the driver's own exit, so a
    /// fresh `StartRun` stays refused until the driver leaves.
    running: Mutex<HashMap<TaskId, CancellationToken>>,
    /// Tasks with a journal recovery currently running (single-flight
    /// recovery): concurrent `supervisor_for` misses on one task elect a
    /// single recoverer; losers wait for its handle instead of racing
    /// `TaskOwnership::acquire` and surfacing `task_already_owned`.
    recovering: Mutex<HashSet<TaskId>>,
    /// Scrubbed failure text of runs this gateway spawned, keyed by task.
    failures: Mutex<HashMap<TaskId, String>>,
}

/// Label the CLI/TUI prints for the scripted test/replay provider
/// (plan G5: a run through `kind = "fake"` is labeled honestly).
pub const FAKE_PROVIDER_LABEL: &str = "scripted test/replay provider";

/// Plan G5 (M11): the honest provider label as the optional top-level
/// `provider_label` JSON key — present ONLY when this gateway runs
/// `kind = "fake"` (the config maps that kind to
/// [`FAKE_PROVIDER_LABEL`]; every other kind labels itself, and a
/// missing provider carries no label). Key name and top-level position
/// are a wire contract with the TUI reader.
fn provider_label(state: &GatewayState) -> Option<Value> {
    (state.runtime.label == FAKE_PROVIDER_LABEL)
        .then(|| Value::String(FAKE_PROVIDER_LABEL.to_owned()))
}

/// `GetTask` payload: the task object plus the optional top-level
/// `provider_label` key (plan G5 / M11 — key name and position are a
/// wire contract with the TUI reader).
fn get_task_payload(state: &GatewayState, task: &tachyon_core::TaskState) -> Value {
    let mut payload = json!({ "task": task });
    if let Some(label) = provider_label(state) {
        payload["provider_label"] = label;
    }
    payload
}

/// Everything a gateway needs to admit `StartRun`: the provider itself
/// (already built from the operator's `FileConfig` by the process that
/// loaded it), its display label, the neutral model name for requests,
/// and the credential redaction registry holding any resolved API key
/// (spec §35 — registered at config load, never stored in config).
#[derive(Clone, Default)]
pub struct GatewayRuntime {
    /// Provider for spawned runs; `None` refuses `StartRun` honestly.
    pub provider: Option<Arc<dyn ModelProvider>>,
    /// Human label echoed in the `StartRun` acknowledgement.
    pub label: String,
    /// Model name carried in every `ModelRequest`.
    pub model: String,
    /// Redaction registry: provider error text passes this filter before
    /// it is logged, recorded, or reachable by any client.
    pub redactor: CredentialBroker,
}

/// A running gateway. Shut down explicitly; the runtime dir is released
/// after in-flight connections drain.
pub struct RunningGateway {
    socket_path: PathBuf,
    address: PathBuf,
    paths: ClaimPaths,
    state: Arc<GatewayState>,
    shutdown: CancellationToken,
    accept_loop: tokio::task::JoinHandle<()>,
}

impl RunningGateway {
    /// Socket path (Unix) or runtime dir anchor (Windows) for display.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Address clients connect to: socket path on Unix, pipe name on Windows.
    #[must_use]
    pub fn address(&self) -> &Path {
        &self.address
    }

    /// The gateway's own [`StoreWriter`] handle.
    ///
    /// This is the *same* writer the gateway uses, not a second one: callers
    /// share its mutex and its commit notifications, so the single-logical-
    /// writer rule and the subscription fan-out both still hold. It exists so
    /// measurements (M11 G7) can timestamp `t0` at the exact commit return
    /// instead of guessing from a command response.
    #[must_use]
    pub fn store(&self) -> Arc<StoreWriter> {
        self.state.store.clone()
    }

    /// Scrubbed failure text of a run this gateway spawned, if it failed
    /// (spec §35: registered secret values were redacted before the text
    /// was recorded, so this string is the only representation of the
    /// error that exists anywhere a client or log reader can reach).
    pub async fn task_failure(&self, task_id: TaskId) -> Option<String> {
        self.state.failures.lock().await.get(&task_id).cloned()
    }

    /// Signals shutdown, drains connections, drops supervisors, closes
    /// the pool, then releases the runtime dir. After this returns, the
    /// data directory may be moved or deleted on any platform.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.state.supervisors.lock().await.clear();
        self.state.store.close().await;
        let _ = self.accept_loop.await;
        release_runtime_dir(&self.paths);
    }
}

/// Starts a gateway with no model provider configured: every command
/// still works, and `StartRun` refuses honestly with
/// `provider_not_configured` until the operator configures one.
pub async fn start(data_dir: &Path) -> Result<RunningGateway, GatewayError> {
    start_with(data_dir, GatewayRuntime::default()).await
}

/// Starts a gateway on `data_dir` with an explicit [`GatewayRuntime`]:
/// claims the runtime dir, binds the socket, opens the store, and
/// recovers every incomplete task (spec §41).
pub async fn start_with(
    data_dir: &Path,
    runtime: GatewayRuntime,
) -> Result<RunningGateway, GatewayError> {
    let paths = claim_runtime_dir(data_dir).await.map_err(|err| match err {
        EndpointError::AlreadyRunning { pid } => GatewayError::AlreadyRunning { pid },
        other => GatewayError::Endpoint(other),
    })?;
    let listener = Listener::bind(&paths.socket)?;
    let address = listener.local_address();
    let store = Arc::new(StoreWriter::open(data_dir).await?);
    let state = Arc::new(GatewayState {
        store,
        supervisors: Mutex::new(HashMap::new()),
        data_dir: data_dir.to_owned(),
        runtime,
        running: Mutex::new(HashMap::new()),
        recovering: Mutex::new(HashSet::new()),
        failures: Mutex::new(HashMap::new()),
    });
    recover_incomplete(&state).await?;
    let shutdown = CancellationToken::new();
    let accept_loop = tokio::spawn(accept_loop(listener, state.clone(), shutdown.clone()));
    // Publish the endpoint only once the gateway can serve (#35): before
    // this point the Windows pipe has a single instance and no accept loop,
    // so any client discovering the pipe gets ERROR_PIPE_BUSY. On failure
    // shut the just-spawned accept loop down instead of leaking it.
    if let Err(err) = write_endpoint(&paths, &address) {
        shutdown.cancel();
        let _ = accept_loop.await;
        return Err(GatewayError::Endpoint(err));
    }
    Ok(RunningGateway {
        socket_path: paths.socket.clone(),
        address,
        paths,
        state,
        shutdown,
        accept_loop,
    })
}

/// Spawns supervisors for every task that never reached a terminal state.
async fn recover_incomplete(state: &Arc<GatewayState>) -> Result<(), GatewayError> {
    for raw in state.store.incomplete_tasks().await? {
        let task_id = TaskId(Uuid::parse_str(&raw).map_err(|_| {
            GatewayError::Core(CoreError::Corrupt {
                detail: format!("invalid task id {raw:?}"),
            })
        })?);
        let handle = recover_task(task_id, state.store.clone()).await?;
        state.supervisors.lock().await.insert(task_id, handle);
    }
    Ok(())
}

async fn accept_loop(listener: Listener, state: Arc<GatewayState>, shutdown: CancellationToken) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok(stream) => {
                        connections.spawn(serve_connection(stream, state.clone(), shutdown.clone()));
                    }
                    Err(_) => {
                        if shutdown.is_cancelled() {
                            break;
                        }
                    }
                }
            }
        }
    }
    // Connections are stateless request handlers; aborting between requests
    // is safe and lets shutdown complete without waiting on idle clients.
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

/// Serves one connection: a reader loop, one writer task owning the write
/// half, and one forwarder task pushing subscription events (plan D2).
///
/// All three share a connection-scoped token, so a writer or forwarder
/// failure tears down **both** split halves — a connection can never sit
/// event-dead while still answering commands. Reader exit tears them down
/// too, which is what lets a client's half-close surface as EOF.
async fn serve_connection(stream: Stream, state: Arc<GatewayState>, shutdown: CancellationToken) {
    let (read_half, write_half) = tokio::io::split(stream);
    let mut read_half = read_half;
    let outbound = Arc::new(Outbound::new());
    let connection = CancellationToken::new();
    let (control_tx, control_rx) = mpsc::channel::<SubscriptionControl>(4);
    let mut tasks = ConnectionTasks {
        writer: Some(tokio::spawn(writer_task(
            write_half,
            outbound.clone(),
            connection.clone(),
        ))),
        forwarder: Some(tokio::spawn(forwarder_task(
            state.clone(),
            outbound.clone(),
            control_rx,
            connection.clone(),
        ))),
        connection,
    };

    'read: loop {
        if shutdown.is_cancelled() {
            break 'read;
        }
        let request = tokio::select! {
            () = tasks.connection.cancelled() => break 'read,
            request = read_request(&mut read_half) => match request {
                Ok(request) => request,
                Err(ReadError::Eof | ReadError::Fatal) => break 'read,
            },
        };
        match match request {
            Ok(envelope) => dispatch(&state, &envelope).await,
            Err(response) => Handled::Respond(response),
        } {
            Handled::Respond(response) => {
                outbound.push_response(response, &tasks.connection).await;
            }
            Handled::Subscribe { ack, setup } => {
                let control = SubscriptionControl::Switch {
                    task_id: setup.task_id,
                    cursor: setup.cursor,
                    ack,
                    commits: setup.commits,
                };
                // The forwarder enqueues the ack itself, so no previous-task
                // event frame can be queued after it (plan D2 re-Subscribe).
                if control_tx.send(control).await.is_err() {
                    break 'read;
                }
            }
        }
    }

    // Reader exit, writer failure and forwarder failure all land here.
    tasks.shutdown().await;
}

/// Both split halves of a connection plus the token that binds their fate.
///
/// Dropping this without running [`ConnectionTasks::shutdown`] (for example
/// when the accept loop aborts the connection during gateway shutdown) still
/// cancels and aborts both tasks, so no orphan can hold a socket open.
struct ConnectionTasks {
    writer: Option<tokio::task::JoinHandle<()>>,
    forwarder: Option<tokio::task::JoinHandle<()>>,
    connection: CancellationToken,
}

impl ConnectionTasks {
    /// Cancels both halves' work and reaps the tasks.
    async fn shutdown(&mut self) {
        self.connection.cancel();
        if let Some(writer) = self.writer.take() {
            writer.abort();
            let _ = writer.await;
        }
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
            let _ = forwarder.await;
        }
    }
}

impl Drop for ConnectionTasks {
    fn drop(&mut self) {
        self.connection.cancel();
        if let Some(writer) = self.writer.take() {
            writer.abort();
        }
        if let Some(forwarder) = self.forwarder.take() {
            forwarder.abort();
        }
    }
}

enum ReadError {
    Eof,
    Fatal,
}

async fn read_request<R>(
    stream: &mut R,
) -> Result<Result<RequestEnvelope, ResponseEnvelope>, ReadError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut prefix = [0_u8; tachyon_protocol::FRAME_PREFIX_LEN];
    if let Err(err) = stream.read_exact(&mut prefix).await {
        return if err.kind() == std::io::ErrorKind::UnexpectedEof {
            Err(ReadError::Eof)
        } else {
            Err(ReadError::Fatal)
        };
    }
    let len = u32::from_le_bytes(prefix) as usize;
    if len > tachyon_protocol::MAX_FRAME_BYTES - tachyon_protocol::FRAME_PREFIX_LEN {
        return Err(ReadError::Fatal);
    }
    let mut payload = vec![0_u8; len];
    if stream.read_exact(&mut payload).await.is_err() {
        return Err(ReadError::Fatal);
    }
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    match decode_frame::<RequestEnvelope>(&framed) {
        Ok((envelope, _)) => {
            if let Err(err) = check_version(envelope.protocol_version) {
                let response = ResponseEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    request_id: envelope.request_id,
                    result: CommandResult::Err {
                        code: "unsupported_version".to_owned(),
                        message: err.to_string(),
                    },
                };
                Ok(Err(response))
            } else {
                Ok(Ok(envelope))
            }
        }
        Err(_) => Err(ReadError::Fatal),
    }
}

/// One frame the writer must emit.
enum OutboundItem {
    /// A queued frame; `generation` is set for subscription events so a task
    /// switch that happened mid-write is not recorded as delivered.
    Frame {
        /// Frame to write.
        frame: ServerFrame,
        /// Queue generation the frame was queued under, if a subscription
        /// event.
        generation: Option<u64>,
    },
    /// Overflow marker: tell the client to resubscribe from `after_seq`.
    Resync {
        /// Affected task.
        task_id: TaskId,
        /// Last sequence whose frame write **completed** on the socket.
        after_seq: i64,
    },
}

/// A frame waiting to leave one connection.
#[derive(Debug)]
enum Queued {
    /// Command response; never dropped by subscription overflow, so a
    /// subscribed connection keeps serving requests while its stream is
    /// behind.
    Response(ServerFrame),
    /// Subscription event tagged with its queue generation.
    Event {
        /// Frame to write.
        frame: ServerFrame,
        /// Queue generation at push time.
        generation: u64,
    },
}

/// Shared outbound state of one connection: queue, overflow marker, and the
/// delivery cursor the writer maintains at write time.
#[derive(Debug)]
struct QueueState {
    /// Frames waiting for the writer, oldest first.
    frames: VecDeque<Queued>,
    /// Task of the active subscription, when one exists.
    task: Option<TaskId>,
    /// Cursor the active subscription started from (resync fallback).
    cursor: i64,
    /// Incremented on every task switch.
    generation: u64,
    /// Last event sequence whose write completed on the socket.
    last_written: Option<i64>,
    /// Overflow happened; the writer owes the client a `ResyncRequired`.
    resync: bool,
}

/// The one writer task's input: a bounded event queue plus responses.
struct Outbound {
    queue: Mutex<QueueState>,
    /// Wakes the writer (one stored permit, so no wakeup is lost).
    ready: Notify,
    /// Wakes the reader when a full queue has room again.
    drained: Notify,
}

/// Bound on queued subscription event frames (plan D2).
const EVENT_QUEUE_CAPACITY: usize = 256;
/// Bound on queued command responses before the reader applies backpressure.
const RESPONSE_QUEUE_CAPACITY: usize = 1024;

impl Outbound {
    fn new() -> Self {
        Self {
            queue: Mutex::new(QueueState {
                frames: VecDeque::new(),
                task: None,
                cursor: 0,
                generation: 0,
                last_written: None,
                resync: false,
            }),
            ready: Notify::new(),
            drained: Notify::new(),
        }
    }

    /// Queues a command response, waiting when the queue is full so a slow
    /// client still back-pressures its own reader loop.
    async fn push_response(&self, response: ResponseEnvelope, connection: &CancellationToken) {
        let frame = ServerFrame::Response(response);
        loop {
            {
                let mut queue = self.queue.lock().await;
                if queue.frames.len() < RESPONSE_QUEUE_CAPACITY {
                    queue.frames.push_back(Queued::Response(frame));
                    drop(queue);
                    self.ready.notify_one();
                    return;
                }
            }
            tokio::select! {
                () = connection.cancelled() => return,
                () = self.drained.notified() => {}
            }
        }
    }

    /// Queues one subscription event within the bounded queue.
    ///
    /// Returns `false` on overflow: queued events are cleared, the resync
    /// marker is raised, and the forwarder must stop advancing its cursor.
    async fn push_event(&self, frame: ServerFrame) -> bool {
        let mut queue = self.queue.lock().await;
        let queued = queue
            .frames
            .iter()
            .filter(|entry| matches!(entry, Queued::Event { .. }))
            .count();
        if queued >= EVENT_QUEUE_CAPACITY {
            queue
                .frames
                .retain(|entry| !matches!(entry, Queued::Event { .. }));
            queue.resync = true;
            drop(queue);
            self.ready.notify_one();
            return false;
        }
        let generation = queue.generation;
        queue.frames.push_back(Queued::Event { frame, generation });
        drop(queue);
        self.ready.notify_one();
        true
    }

    /// Switches the subscription: previous queued events stay ahead of `ack`
    /// (FIFO flush), delivery bookkeeping resets to the new task, and any
    /// pending resync is superseded by the ack.
    async fn switch_subscription(&self, task_id: TaskId, cursor: i64, ack: ResponseEnvelope) {
        let mut queue = self.queue.lock().await;
        queue.generation += 1;
        queue.task = Some(task_id);
        queue.cursor = cursor;
        queue.last_written = None;
        queue.resync = false;
        queue
            .frames
            .push_back(Queued::Response(ServerFrame::Response(ack)));
        drop(queue);
        self.ready.notify_one();
    }

    /// Next frame to write, or `None` once the connection is torn down.
    async fn take_item(&self, connection: &CancellationToken) -> Option<OutboundItem> {
        loop {
            {
                let mut queue = self.queue.lock().await;
                if queue.resync {
                    queue.resync = false;
                    if let Some(task_id) = queue.task {
                        let after_seq = queue.last_written.unwrap_or(queue.cursor);
                        return Some(OutboundItem::Resync { task_id, after_seq });
                    }
                }
                if let Some(queued) = queue.frames.pop_front() {
                    drop(queue);
                    self.drained.notify_one();
                    return Some(match queued {
                        Queued::Response(frame) => OutboundItem::Frame {
                            frame,
                            generation: None,
                        },
                        Queued::Event { frame, generation } => OutboundItem::Frame {
                            frame,
                            generation: Some(generation),
                        },
                    });
                }
            }
            tokio::select! {
                () = connection.cancelled() => return None,
                () = self.ready.notified() => {}
            }
        }
    }

    /// Records that `seq`'s frame write completed on the socket. Ignored when
    /// the subscription switched after the frame was queued — a stale task
    /// must never be reported as delivered.
    async fn record_delivery(&self, generation: u64, seq: i64) {
        let mut queue = self.queue.lock().await;
        if queue.generation == generation {
            queue.last_written = Some(seq);
        }
    }
}

/// Owns the write half of one connection: the only writer on this socket.
async fn writer_task<W>(mut write_half: W, outbound: Arc<Outbound>, connection: CancellationToken)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    while let Some(item) = outbound.take_item(&connection).await {
        let (frame, generation) = match item {
            OutboundItem::Frame { frame, generation } => (frame, generation),
            OutboundItem::Resync { task_id, after_seq } => (resync_frame(task_id, after_seq), None),
        };
        let Ok(bytes) = encode_server_frame(&frame) else {
            connection.cancel();
            return;
        };
        let written = tokio::select! {
            () = connection.cancelled() => return,
            result = write_half.write_all(&bytes) => result,
        };
        if written.is_err() {
            connection.cancel();
            return;
        }
        if let (Some(generation), ServerFrame::Event(envelope)) = (generation, &frame)
            && matches!(envelope.event, GatewayEvent::Journal { .. })
        {
            outbound.record_delivery(generation, envelope.seq).await;
        }
    }
}

/// Control message from the reader to a connection's forwarder.
enum SubscriptionControl {
    /// Switch (or establish) the subscription: flush previous-task frames,
    /// enqueue `ack`, then stream `task_id` from `cursor`.
    Switch {
        /// Task to stream.
        task_id: TaskId,
        /// Cursor: last sequence the ack's replay array already covers.
        cursor: i64,
        /// Ack response, enqueued by the forwarder before any new event.
        ack: ResponseEnvelope,
        /// Commit notifications, subscribed before the replay snapshot.
        commits: broadcast::Receiver<CommitNotice>,
    },
}

/// The forwarder's live subscription.
struct Subscription {
    /// Task being streamed.
    task_id: TaskId,
    /// Last sequence pushed to the queue.
    cursor: i64,
    /// Overflow: stop advancing until the client re-subscribes.
    stalled: bool,
    /// Commit notifications for this connection.
    commits: broadcast::Receiver<CommitNotice>,
}

/// What woke the forwarder.
enum ForwarderEvent {
    /// Reader asked for a task switch (or the reader is gone).
    Control(Option<SubscriptionControl>),
    /// Store commit notification (or lag/close).
    Commit(Result<CommitNotice, broadcast::error::RecvError>),
}

/// Pushes committed journal events of the subscribed task as
/// [`ServerFrame::event`] frames, pulling by cursor so a lagged broadcast
/// loses nothing (plan D2). No commits means no wakeups: never a busy loop.
async fn forwarder_task(
    state: Arc<GatewayState>,
    outbound: Arc<Outbound>,
    mut control: mpsc::Receiver<SubscriptionControl>,
    connection: CancellationToken,
) {
    let mut current: Option<Subscription> = None;
    loop {
        let event = match &mut current {
            None => tokio::select! {
                () = connection.cancelled() => return,
                message = control.recv() => ForwarderEvent::Control(message),
            },
            Some(subscription) => tokio::select! {
                () = connection.cancelled() => return,
                message = control.recv() => ForwarderEvent::Control(message),
                result = subscription.commits.recv() => ForwarderEvent::Commit(result),
            },
        };
        match event {
            ForwarderEvent::Control(None) => return,
            ForwarderEvent::Control(Some(SubscriptionControl::Switch {
                task_id,
                cursor,
                ack,
                commits,
            })) => {
                outbound.switch_subscription(task_id, cursor, ack).await;
                current = Some(Subscription {
                    task_id,
                    cursor,
                    stalled: false,
                    commits,
                });
            }
            ForwarderEvent::Commit(result) => {
                let subscription = current
                    .as_mut()
                    .expect("a commit notification implies a subscription");
                if subscription.stalled {
                    continue;
                }
                // Another task's commit means nothing was missed here; only
                // lag hides which commits were skipped, so only lag forces a
                // catch-up pull.
                let relevant = match result {
                    Ok((task, _seq)) => task == subscription.task_id.to_string(),
                    Err(
                        broadcast::error::RecvError::Lagged(_)
                        | broadcast::error::RecvError::Closed,
                    ) => true,
                };
                if !relevant {
                    continue;
                }
                if forward_events(&state, &outbound, subscription)
                    .await
                    .is_err()
                {
                    // Store or journal unreadable: fail the whole connection
                    // rather than leave it event-dead-but-answering.
                    connection.cancel();
                    return;
                }
            }
        }
    }
}

/// Pulls every event after the cursor and queues it. `Err` means the
/// connection must die; `Ok(())` may leave the subscription stalled after an
/// overflow (cursor deliberately not advanced past the cleared frames).
async fn forward_events(
    state: &Arc<GatewayState>,
    outbound: &Arc<Outbound>,
    subscription: &mut Subscription,
) -> Result<(), GatewayError> {
    let key = subscription.task_id.to_string();
    let rows = state
        .store
        .load_events_since(&key, subscription.cursor)
        .await?;
    for row in rows {
        let frame = journal_frame(&row, subscription.task_id)?;
        if !outbound.push_event(frame).await {
            subscription.stalled = true;
            return Ok(());
        }
        subscription.cursor = row.seq;
    }
    Ok(())
}

/// Wraps one journal row as the pushed event frame that carries it.
fn journal_frame(
    row: &tachyon_store::JournalEvent,
    task_id: TaskId,
) -> Result<ServerFrame, GatewayError> {
    let payload: Value = serde_json::from_str(&row.payload).map_err(GatewayError::Json)?;
    let schema_version = u16::try_from(row.schema_version).map_err(|_| {
        GatewayError::Core(CoreError::Corrupt {
            detail: format!("journal schema_version {} out of range", row.schema_version),
        })
    })?;
    let event_id = row.event_id.parse().map_err(|err| {
        GatewayError::Core(CoreError::Corrupt {
            detail: format!("journal event id {:?}: {err}", row.event_id),
        })
    })?;
    Ok(ServerFrame::Event(EventEnvelope {
        seq: row.seq,
        event_id,
        schema_version,
        task_id,
        timestamp: Timestamp::from_micros(row.created_at),
        event: GatewayEvent::Journal {
            kind: row.kind.clone(),
            payload,
        },
    }))
}

/// The overflow notice: `after_seq` is the last sequence whose frame write
/// completed on this socket, so a client resuming from it can never skip a
/// frame it never received.
///
/// Its envelope `seq` equals `after_seq`: clients must dispatch this variant
/// **before** applying their `seq ≤ last_seen` replay filter.
fn resync_frame(task_id: TaskId, after_seq: i64) -> ServerFrame {
    ServerFrame::Event(EventEnvelope {
        seq: after_seq,
        event_id: EventId::generate(),
        schema_version: PROTOCOL_VERSION,
        task_id,
        timestamp: Timestamp::now(),
        event: GatewayEvent::ResyncRequired { task_id, after_seq },
    })
}

/// What dispatch wants done with a request's answer.
enum Handled {
    /// Enqueue this response on the connection.
    Respond(ResponseEnvelope),
    /// Hand the connection to its forwarder; the forwarder enqueues `ack`.
    Subscribe {
        /// Ack response for the `Subscribe` request.
        ack: ResponseEnvelope,
        /// Live-stream setup the forwarder needs.
        setup: SubscriptionSetup,
    },
}

/// Everything a forwarder needs to start streaming a task.
struct SubscriptionSetup {
    /// Task to stream.
    task_id: TaskId,
    /// Cursor after the ack's replay array.
    cursor: i64,
    /// Commit notifications subscribed before the replay snapshot.
    commits: broadcast::Receiver<CommitNotice>,
}

async fn dispatch(state: &Arc<GatewayState>, envelope: &RequestEnvelope) -> Handled {
    let wrap = |result: CommandResult| ResponseEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: envelope.request_id,
        result,
    };
    if let Command::Subscribe { task_id, after_seq } = &envelope.command {
        return match subscribe(state, *task_id, *after_seq).await {
            Ok((payload, setup)) => Handled::Subscribe {
                ack: wrap(ok(payload)),
                setup,
            },
            Err(result) => Handled::Respond(wrap(result)),
        };
    }
    Handled::Respond(wrap(handle_command(state, &envelope.command).await))
}

/// Builds the `Subscribe` acknowledgement and its live-stream setup.
///
/// The commit receiver is opened **before** the replay snapshot so a commit
/// landing in between is buffered as a notification rather than lost between
/// snapshot and stream. The ack payload keeps the replayed `events` array the
/// v1 answer carried, alongside the v2 cursor fields.
async fn subscribe(
    state: &Arc<GatewayState>,
    task_id: TaskId,
    after_seq: i64,
) -> Result<(Value, SubscriptionSetup), CommandResult> {
    let commits = state.store.subscribe_commits();
    let key = task_id.to_string();
    // Cursor pushdown: the store filters, so a large journal is never
    // loaded whole per attach; `last_seq` comes from the MAX aggregate.
    let replayed = state
        .store
        .load_events_since(&key, after_seq)
        .await
        .map_err(|err| fail("internal", err.to_string()))?;
    let last_seq = state
        .store
        .latest_seq(&key)
        .await
        .map_err(|err| fail("internal", err.to_string()))?;
    let cursor = replayed.last().map_or(after_seq, |event| event.seq);
    let mut ack = json!({
        "subscribed": true,
        "task_id": key,
        "after_seq": after_seq,
        "last_seq": last_seq,
        "events": replayed,
    });
    // Plan G5 (M11): same optional top-level key as `GetTask`.
    if let Some(label) = provider_label(state) {
        ack["provider_label"] = label;
    }
    Ok((
        ack,
        SubscriptionSetup {
            task_id,
            cursor,
            commits,
        },
    ))
}

fn ok(payload: Value) -> CommandResult {
    CommandResult::Ok { payload }
}

fn fail(code: &str, message: String) -> CommandResult {
    CommandResult::Err {
        code: code.to_owned(),
        message,
    }
}

fn core_err(error: &CoreError) -> CommandResult {
    let code = match error {
        CoreError::UnknownTask(_) => "unknown_task",
        CoreError::TaskAlreadyOwned(_) => "task_already_owned",
        CoreError::IllegalTransition { .. } => "illegal_transition",
        CoreError::MailboxFull => "mailbox_full",
        CoreError::SupervisorGone => "supervisor_gone",
        CoreError::Corrupt { .. } => "corrupt_state",
        CoreError::InvalidExecutionGraph { .. } => "invalid_execution_graph",
        CoreError::NodeTransitionRejected { .. } => "node_transition_rejected",
        CoreError::VerificationBlocked(_) => "verification_blocked",
        CoreError::Verification(_) => "verification_failed",
        CoreError::StaleRunProposal { .. } => "stale_run_proposal",
        CoreError::ForeignProposal { .. } => "foreign_proposal",
        CoreError::UnknownRun { .. } => "unknown_run",
        CoreError::RunAlreadyActive { .. } => "run_already_active",
        CoreError::ApprovalMissing { .. } => "approval_missing",
        CoreError::ApprovalNotPending { .. } => "approval_not_pending",
        CoreError::WorkspaceAlreadyPinned { .. } => "workspace_already_pinned",
        CoreError::Store(_) | CoreError::Json(_) => "internal",
    };
    fail(code, error.to_string())
}

async fn handle_command(state: &Arc<GatewayState>, command: &Command) -> CommandResult {
    match command {
        Command::Ping => ok(json!({"pong": true, "protocol_version": PROTOCOL_VERSION})),
        Command::GetStatus => {
            let active = state.supervisors.lock().await.len();
            ok(json!({"protocol_version": PROTOCOL_VERSION, "active_tasks": active}))
        }
        Command::CreateSession => {
            let id = tachyon_types::SessionId::generate();
            match state.store.create_session(&id.to_string()).await {
                Ok(()) => ok(json!({"session_id": id.to_string()})),
                Err(err) => fail("internal", err.to_string()),
            }
        }
        Command::CreateTask {
            session_id,
            objective,
        } => create_supervised(state, *session_id, objective.clone()).await,
        Command::ListTasks { session_id } => {
            let filter = session_id.map(|id| id.to_string());
            match state.store.list_tasks(filter.as_deref()).await {
                Ok(tasks) => ok(json!({"tasks": tasks})),
                Err(err) => fail("internal", err.to_string()),
            }
        }
        Command::GetTask { task_id } => match with_live_supervisor(
            state,
            *task_id,
            |handle| async move { handle.get_state().await },
        )
        .await
        {
            Ok(task) => ok(get_task_payload(state, &task)),
            Err(result) => result,
        },
        Command::SendMessage { task_id, message } => {
            mutate(state, *task_id, |handle| async move {
                handle.add_message(message.clone()).await
            })
            .await
        }
        Command::PauseTask { task_id } => {
            mutate(
                state,
                *task_id,
                |handle| async move { handle.pause().await },
            )
            .await
        }
        Command::ResumeTask { task_id } => resume_task(state, *task_id).await,
        Command::CancelTask { task_id } => {
            // Cancellation drain first: fire the ACTIVE run's token so
            // the driver halts at its next stage boundary while the
            // supervisor records the terminal `Cancelled` transition.
            // The running entry is left for the driver's own completion
            // cleanup (a fresh StartRun stays refused until it leaves).
            if let Some(cancel) = state.running.lock().await.get(task_id) {
                cancel.cancel();
            }
            let outcome = mutate(
                state,
                *task_id,
                |handle| async move { handle.cancel().await },
            )
            .await;
            state.supervisors.lock().await.remove(task_id);
            outcome
        }
        Command::Approve {
            task_id,
            approval_id,
        } => decide(state, *task_id, *approval_id, true, String::new()).await,
        Command::Deny {
            task_id,
            approval_id,
            reason,
        } => decide(state, *task_id, *approval_id, false, reason.clone()).await,
        Command::StartRun {
            task_id,
            workspace_root,
            acceptance,
        } => start_run(state, *task_id, workspace_root, acceptance.as_deref()).await,
        Command::Subscribe { task_id, after_seq } => {
            // The reader intercepts `Subscribe` in `dispatch` to wire the
            // live stream; this arm answers the identical ack payload when
            // the command is handled on its own.
            match subscribe(state, *task_id, *after_seq).await {
                Ok((payload, _setup)) => ok(payload),
                Err(result) => result,
            }
        }
        Command::GetArtifact { .. } => fail(
            "not_implemented",
            "artifact retrieval has no gateway API yet; use the local artifact spool path"
                .to_owned(),
        ),
    }
}

async fn create_supervised(
    state: &Arc<GatewayState>,
    session_id: SessionId,
    objective: String,
) -> CommandResult {
    if !state
        .store
        .session_exists(&session_id.to_string())
        .await
        .unwrap_or(false)
    {
        return fail("unknown_session", format!("no session {session_id}"));
    }
    match create_task(
        session_id,
        WorkspaceId::generate(),
        objective,
        state.store.clone(),
    )
    .await
    {
        Ok(handle) => {
            let task_id = handle.task_id();
            let status = TaskStatus::Created;
            state.supervisors.lock().await.insert(task_id, handle);
            ok(json!({"task_id": task_id.to_string(), "status": status.name()}))
        }
        Err(err) => core_err(&err),
    }
}

async fn supervisor_for(
    state: &Arc<GatewayState>,
    task_id: TaskId,
) -> Result<SupervisorHandle, CommandResult> {
    if let Some(handle) = state.supervisors.lock().await.get(&task_id) {
        return Ok(handle.clone());
    }
    // Single-flight recovery (R1 seat5-B3, R2 seat3-B1): concurrent misses
    // on one task elect a single recoverer and wait for its outcome
    // instead of racing `TaskOwnership::acquire`. Two ordering rules make
    // the election leak-free: the winner PUBLISHES the handle BEFORE it
    // clears the election flag (no arrival ever sees "no flag, no
    // handle"), and ONLY the electing caller ever clears the flag (a
    // waiter never removes an entry it does not own).
    for _ in 0..500 {
        if let Some(handle) = state.supervisors.lock().await.get(&task_id) {
            return Ok(handle.clone());
        }
        if state.recovering.lock().await.insert(task_id) {
            return finish_recovery(state, task_id).await;
        }
        // Another recovery owns the election; its publish or failure wakes
        // this loop on the next tick (local replay, ms-scale).
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // Bound exhausted (a stuck or panicked recoverer left the flag held):
    // attempt directly WITHOUT touching the flag — a typed
    // `task_already_owned` here is honest after a 5 s wait.
    // `TaskAlreadyOwned` from a successful recover is often the previous
    // lease still draining after run completion (map entry already gone);
    // retry with a short backoff so GetTask/StartRun do not surface a
    // transient race as a hard client error.
    let mut attempts = 0_u32;
    loop {
        match recover_task(task_id, state.store.clone()).await {
            Ok(handle) => {
                state
                    .supervisors
                    .lock()
                    .await
                    .insert(task_id, handle.clone());
                return Ok(handle);
            }
            Err(CoreError::UnknownTask(_)) => {
                return Err(fail("unknown_task", format!("no task {task_id}")));
            }
            Err(CoreError::TaskAlreadyOwned(_)) if attempts < 50 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(other) => return Err(core_err(&other)),
        }
    }
}

/// Completes one ELECTED recovery: on success the handle is published
/// first and the election flag cleared second; on failure the flag is
/// cleared before the typed error is propagated, so waiters retry. Only
/// the elected caller runs this — the flag is never cleared by a waiter.
async fn finish_recovery(
    state: &Arc<GatewayState>,
    task_id: TaskId,
) -> Result<SupervisorHandle, CommandResult> {
    let mut attempts = 0_u32;
    loop {
        match recover_task(task_id, state.store.clone()).await {
            Ok(handle) => {
                state
                    .supervisors
                    .lock()
                    .await
                    .insert(task_id, handle.clone());
                state.recovering.lock().await.remove(&task_id);
                return Ok(handle);
            }
            Err(CoreError::UnknownTask(_)) => {
                state.recovering.lock().await.remove(&task_id);
                return Err(fail("unknown_task", format!("no task {task_id}")));
            }
            // Previous lease draining after run completion: back off and
            // retry inside the election so waiters still see one recoverer.
            Err(CoreError::TaskAlreadyOwned(_)) if attempts < 50 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => {
                state.recovering.lock().await.remove(&task_id);
                return Err(match error {
                    CoreError::UnknownTask(_) => fail("unknown_task", format!("no task {task_id}")),
                    other => core_err(&other),
                });
            }
        }
    }
}

async fn mutate<F, Fut>(state: &Arc<GatewayState>, task_id: TaskId, op: F) -> CommandResult
where
    F: Fn(SupervisorHandle) -> Fut,
    Fut: std::future::Future<Output = Result<tachyon_core::TaskState, CoreError>>,
{
    match with_live_supervisor(state, task_id, op).await {
        Ok(task) => ok(json!({"task": task})),
        Err(result) => result,
    }
}

/// Runs one supervisor operation, recovering once past a stale map entry.
/// A supervisor can shut down between lookup and use — run completion
/// drops the dead handle asynchronously — and the cached handle then
/// fails with `SupervisorGone`. Evicting it and recovering once from the
/// durable journal keeps a client polling live state from seeing a
/// transient `supervisor_gone` for a task the journal still answers.
/// Exactly one recovery attempt; a second failure propagates unchanged.
/// (Evict-then-recover routes through `supervisor_for`, whose
/// single-flight election keeps concurrent recoverers from racing
/// `TaskOwnership::acquire`.)
async fn with_live_supervisor<F, Fut>(
    state: &Arc<GatewayState>,
    task_id: TaskId,
    op: F,
) -> Result<tachyon_core::TaskState, CommandResult>
where
    F: Fn(SupervisorHandle) -> Fut,
    Fut: std::future::Future<Output = Result<tachyon_core::TaskState, CoreError>>,
{
    let handle = match supervisor_for(state, task_id).await {
        Ok(handle) => handle,
        Err(result) => return Err(result),
    };
    match op(handle).await {
        Ok(task) => Ok(task),
        Err(CoreError::SupervisorGone) => {
            state.supervisors.lock().await.remove(&task_id);
            let handle = supervisor_for(state, task_id).await?;
            op(handle).await.map_err(|error| core_err(&error))
        }
        Err(other) => Err(core_err(&other)),
    }
}

/// Routes `Approve`/`Deny` (plan item 8 / D4): the approval id is
/// resolved by a store READ (`load_by_id`), the row's `task_id` must
/// match the command's task scope (mismatch is a typed error), and only
/// then does the decision reach that task's supervisor. Every approval
/// WRITE stays supervisor-owned; missing/late/double decisions flow
/// through the core's typed errors (`approval_missing`,
/// `approval_not_pending`) via [`core_err`].
async fn decide(
    state: &Arc<GatewayState>,
    task_id: TaskId,
    approval: ApprovalId,
    granted: bool,
    reason: String,
) -> CommandResult {
    let row = match state.store.load_by_id(&approval.to_string()).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return fail(
                "approval_missing",
                format!("no approval row for {approval}"),
            );
        }
        Err(err) => return fail("internal", err.to_string()),
    };
    if row.task_id != task_id.to_string() {
        return fail(
            "approval_task_mismatch",
            format!(
                "approval {approval} belongs to task {}, not {task_id}",
                row.task_id
            ),
        );
    }
    let task = match with_live_supervisor(state, task_id, |handle| {
        let reason = reason.clone();
        async move { handle.decide_approval(approval, granted, reason).await }
    })
    .await
    {
        Ok(task) => task,
        Err(result) => return result,
    };
    ok(json!({"task": task}))
}

/// `Resume` dispatch (ADR 0002): `Recovering` with a durable workspace pin
/// means an in-flight run to re-enter — respawn the shared driver through
/// the normal [`start_run`] path (pin is set-once/same-root idempotent).
/// `Recovering` without a pin has no run: the supervisor moves it to
/// `Paused`. Every other status uses the existing resume path.
async fn resume_task(state: &Arc<GatewayState>, task_id: TaskId) -> CommandResult {
    let handle = match supervisor_for(state, task_id).await {
        Ok(handle) => handle,
        Err(result) => return result,
    };
    let current = match handle.get_state().await {
        Ok(task) => task,
        Err(error) => return core_err(&error),
    };
    if current.status == TaskStatus::Recovering
        && let Some(root) = current.workspace_root.clone()
    {
        return start_run(state, task_id, &root, None).await;
    }
    mutate(
        state,
        task_id,
        |handle| async move { handle.resume().await },
    )
    .await
}

/// Admits `Command::StartRun` (plan items 5, 6, 9).
///
/// Order is the safety property, not a convenience: one in-flight run
/// per task (checked and inserted under a single lock), honest provider
/// refusal before any work, supervisor/terminal check, workspace root
/// existence + canonicalization rejection, acceptance resolution — all
/// BEFORE any lease exists. Then the workspace lease is drawn on the
/// canonical root (typed `workspace_busy` exclusion) and the SAME
/// canonical value is pinned into durable task state — R1 board B1
/// amended the plan's original pin-then-lease order: every refusal
/// (busy workspace, pin conflict) must leave NO pin from a run that
/// never started. Pin, lease, policy and evidence roots are one value
/// (see [`prepare_run`]); the ONE shared driver is spawned carrying
/// the lease.
async fn start_run(
    state: &Arc<GatewayState>,
    task_id: TaskId,
    workspace_root: &str,
    acceptance: Option<&str>,
) -> CommandResult {
    let cancel = CancellationToken::new();
    {
        let mut running = state.running.lock().await;
        if running.contains_key(&task_id) {
            return fail(
                "run_already_active",
                format!("a run is already in flight for task {task_id}"),
            );
        }
        running.insert(task_id, cancel.clone());
    }
    let admitted = prepare_run(state, task_id, workspace_root, acceptance, cancel).await;
    if admitted.is_err() {
        state.running.lock().await.remove(&task_id);
    }
    match admitted {
        Ok(payload) => ok(payload),
        Err(result) => result,
    }
}

/// Every pre-spawn step of [`start_run`]. The workspace lease is drawn
/// BEFORE the durable pin, so every refusal here — provider, terminal,
/// canonicalization, acceptance, `workspace_busy`, pin conflict — leaves
/// no pin from an attempted-but-refused run; no refusal spawns a driver.
async fn prepare_run(
    state: &Arc<GatewayState>,
    task_id: TaskId,
    workspace_root: &str,
    acceptance: Option<&str>,
    cancel: CancellationToken,
) -> Result<Value, CommandResult> {
    // 1. Honest provider refusal before any work starts (plan item 5).
    let Some(provider) = state.runtime.provider.clone() else {
        return Err(fail(
            "provider_not_configured",
            "no model provider is configured for this gateway; set the \
             provider section (kind/base_url/model/api_key_env) in the \
             config file"
                .to_owned(),
        ));
    };
    let model = state.runtime.model.clone();
    let label = state.runtime.label.clone();

    // 2. Supervisor + terminal check.
    let handle = supervisor_for(state, task_id).await?;
    let current = handle.get_state().await.map_err(|error| core_err(&error))?;
    if current.status.is_terminal() {
        return Err(fail(
            "illegal_transition",
            format!("task {task_id} is terminal ({})", current.status),
        ));
    }

    // 3. Workspace validation BEFORE policy or lease init: reject what
    //    cannot canonicalize, so ToolsContext's best-effort canonicalize
    //    (tachyon-tools/src/lib.rs) never sees a raw or broken root.
    let canonical = canonical_workspace_root(workspace_root)?;

    // 4. Acceptance resolution (item 9): explicit file wins, Cargo
    //    default detects, everything else fails closed.
    let contract = resolve_acceptance(acceptance, &canonical)?;

    // 5. Workspace lease BEFORE the durable pin (R1 board B1): a busy
    //    workspace must refuse without pinning anything — a pin left by
    //    a refused run would wedge every later retry on a root that
    //    never ran. On a pin refusal below, this lease drops here.
    let lease = acquire_run_lease(&canonical).await?;

    // 6. Durable pin BEFORE drawing policy boundaries (plan item 5,
    //    docs/06 containment): set once through the supervisor's
    //    single-writer journal path; a second, different root is a
    //    typed refusal.
    handle
        .pin_workspace_root(canonical.display().to_string())
        .await
        .map_err(|error| core_err(&error))?;

    // 7. ToolsContext from the PINNED canonical root, with the trusted
    //    workspace policy the bench_matrix example uses, carrying the
    //    run-held lease down into every stage. M11 slice 5: the SAME
    //    `canonical` value that was lease-checked (step 5) and pinned
    //    (step 6) builds the context — `new_from_canonical` performs no
    //    second resolution, so the policy, evidence and mutation roots
    //    can never diverge from the durable pin across that await.
    let context = Arc::new(
        ToolsContext::new_from_canonical(
            canonical.clone(),
            run_policy(),
            ArtifactSpool::new(state.data_dir.join("artifacts").join(task_id.to_string())),
        )
        .with_workspace_lease(lease),
    );
    debug_assert_eq!(
        context.workspace_root, canonical,
        "pin/policy/evidence/mutation root must be one value"
    );

    // 8. Bounded deterministic evidence: sorted walk of the workspace
    //    (never model-influenced), capped by the runtime bounds.
    let bounds = RuntimeBounds::default();
    let evidence = collect_evidence_requests(&canonical, &bounds);
    let plan = RunPlan {
        origin: Instant::now(),
        evidence_mode: EvidenceMode::Concurrent,
        evidence,
        contract,
        risk: VerificationRisk::Affected,
        mutation_dir: state
            .data_dir
            .join("runs")
            .join(task_id.to_string())
            .join("mutation-state"),
        batch_id: format!("run-{}", uuid::Uuid::now_v7()),
        model,
        requested_checks: Vec::new(),
        available_checks: Vec::new(),
        bounds,
        cancel,
    };

    // 9. Spawn the ONE shared driver (plan item 6): journalled events
    //    reach live subscribers through the writer's commit broadcast —
    //    no second notification path. The context (and with it the
    //    workspace lease) moves into this task: exclusion lasts exactly
    //    as long as the run and releases with its last holder.
    let host = DriveHost::Supervisor {
        handle: handle.clone(),
        store: state.store.clone(),
    };
    let spawn_state = state.clone();
    let redactor = state.runtime.redactor.clone();
    tokio::spawn(async move {
        match drive(host, context, provider, plan).await {
            Ok(outcome) => {
                // The driver shut the supervisor down and recovered it
                // once for its round-trip; the map's handle is now dead.
                // Drop it so the next command recovers a fresh one.
                spawn_state.supervisors.lock().await.remove(&task_id);
                spawn_state.running.lock().await.remove(&task_id);
                tracing::info!(
                    task = %task_id,
                    outcome = outcome.outcome.as_deref().unwrap_or("unknown"),
                    "shared-driver run finished"
                );
            }
            Err(DriveError::RunCancelled) => {
                // A cancelled run is an operator outcome, not a run
                // failure: the supervisor already journalled the terminal
                // `Cancelled` state, so no failure text is recorded (and
                // there is no error body to redact or leak).
                tracing::info!(task = %task_id, "shared-driver run halted: task cancelled");
                spawn_state.running.lock().await.remove(&task_id);
            }
            Err(error) => {
                // spec §35: the provider error body passes the redaction
                // registry BEFORE it is logged or recorded anywhere.
                // G7: the registered-key registry scrubs the body
                // BEFORE it is logged or recorded anywhere a client
                // can read (spec §35).
                let scrubbed = redactor.redact(&error.to_string());
                tracing::error!(task = %task_id, error = %scrubbed, "shared-driver run failed");
                spawn_state.failures.lock().await.insert(task_id, scrubbed);
                spawn_state.running.lock().await.remove(&task_id);
            }
        }
    });

    Ok(json!({
        "task_id": task_id.to_string(),
        "status": current.status.name(),
        "workspace_root": canonical.display().to_string(),
        "provider": label,
    }))
}

/// Step 6 of [`prepare_run`]: take the workspace lease on the already
/// pinned canonical root (plan checklist `StartRun` row: resource claim =
/// "workspace lease (existing M9/M10 lease) after canonicalization";
/// plan item 5 pins before any lease boundary is drawn).
///
/// Non-blocking by design: a competitor gets the typed `workspace_busy`
/// refusal instead of queueing behind another task's run — no waiter can
/// starve past a cancellation or lose its refusal. The guard then moves
/// into the spawned run via the `ToolsContext`, so every
/// workspace-touching stage (mutation included) is covered until
/// `drive()` returns; drive-reachable inner acquisitions reuse it
/// because the per-root lock is NOT reentrant.
///
/// The single-source invariant lives here too: the lease key, the
/// durable pin and the policy root must name the same canonical path —
/// a mismatch means the workspace was swapped under us between pin and
/// lease, and anything that fails to resolve now fails closed.
async fn acquire_run_lease(canonical: &Path) -> Result<WorkspaceLease, CommandResult> {
    let lease = match WorkspaceLease::try_acquire(canonical).await {
        Ok(Some(lease)) => lease,
        Ok(None) => {
            return Err(fail(
                "workspace_busy",
                format!(
                    "workspace {} is held by another in-flight run",
                    canonical.display()
                ),
            ));
        }
        Err(error) => {
            return Err(fail(
                "workspace_not_canonical",
                format!(
                    "workspace {} no longer resolves while taking its lease: {error}",
                    canonical.display()
                ),
            ));
        }
    };
    if lease.root() != canonical {
        return Err(fail(
            "workspace_not_canonical",
            format!(
                "workspace root changed between pin ({}) and lease ({})",
                canonical.display(),
                lease.root().display()
            ),
        ));
    }
    Ok(lease)
}

/// Exists + canonicalizes + is a directory, in that order. A missing
/// root is `workspace_not_found`; anything that cannot canonicalize
/// (symlink loops, unreadable parents) is `workspace_not_canonical`; a
/// file root is `workspace_not_a_dir`. Symlinks to real directories are
/// accepted — the PIN is always the canonical target, so no
/// create-through-symlink window survives into policy/containment.
fn canonical_workspace_root(raw: &str) -> Result<PathBuf, CommandResult> {
    let path = Path::new(raw);
    let canonical = std::fs::canonicalize(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            fail(
                "workspace_not_found",
                format!("workspace root {} does not exist", path.display()),
            )
        } else {
            fail(
                "workspace_not_canonical",
                format!(
                    "workspace root {} does not canonicalize: {err}",
                    path.display()
                ),
            )
        }
    })?;
    let metadata = std::fs::metadata(&canonical)
        .map_err(|err| fail("internal", format!("stat {}: {err}", canonical.display())))?;
    if !metadata.is_dir() {
        return Err(fail(
            "workspace_not_a_dir",
            format!("workspace root {} is not a directory", path.display()),
        ));
    }
    Ok(canonical)
}

/// Acceptance resolution (plan item 9): an explicit `--acceptance` JSON
/// file wins over detection; a detected Cargo workspace gets the
/// default contract; anything else refuses (fail closed, never
/// model-influenced).
fn resolve_acceptance(
    explicit: Option<&str>,
    root: &Path,
) -> Result<AcceptanceContract, CommandResult> {
    if let Some(path) = explicit {
        let bytes = std::fs::read(path).map_err(|err| {
            fail(
                "acceptance_unreadable",
                format!("cannot read acceptance file {path}: {err}"),
            )
        })?;
        let contract: AcceptanceContract = serde_json::from_slice(&bytes).map_err(|err| {
            fail(
                "acceptance_unreadable",
                format!("acceptance file {path} is not a contract: {err}"),
            )
        })?;
        contract
            .validate()
            .map_err(|err| fail("acceptance_invalid", err.to_string()))?;
        return Ok(contract);
    }
    let detected = detected_cargo_contract(root).ok_or_else(|| {
        fail(
            "acceptance_required",
            format!(
                "workspace {} is not a Cargo project and no --acceptance \
                 file was given",
                root.display()
            ),
        )
    })?;
    detected
        .validate()
        .map_err(|err| fail("acceptance_invalid", err.to_string()))?;
    Ok(detected)
}

/// Default contract for a detected Cargo workspace (plan item 9):
/// `CommandPasses(cargo test --offline --locked)` plus containment of
/// every change inside the workspace and unchanged manifest/lockfile/
/// migrations files. `ChangedPathsWithin` carries every clean top-level
/// entry of the workspace — the write gate normalizes clause paths and
/// accepts no "." wildcard, so enumerating the top level is the exact
/// representation of "any path inside this workspace" (gate + snapshot
/// evaluation both treat a top-level entry as itself or a descendant).
fn detected_cargo_contract(root: &Path) -> Option<AcceptanceContract> {
    if !root.join("Cargo.toml").is_file() {
        return None;
    }
    Some(AcceptanceContract {
        clauses: vec![
            Clause::CommandPasses {
                command: CommandCheck {
                    program: "cargo".to_owned(),
                    args: vec![
                        "test".to_owned(),
                        "--offline".to_owned(),
                        "--locked".to_owned(),
                    ],
                    cwd: ".".to_owned(),
                    env: BTreeMap::new(),
                    timeout_ms: 180_000,
                },
            },
            Clause::ChangedPathsWithin {
                paths: workspace_scope_paths(root),
            },
            Clause::FileUnchanged {
                path: "Cargo.toml".to_owned(),
            },
            Clause::FileUnchanged {
                path: "Cargo.lock".to_owned(),
            },
            Clause::FileUnchanged {
                path: "migrations/**".to_owned(),
            },
        ],
    })
}

/// Clean top-level entry names of `root`, sorted: names the contract
/// path validator would reject (`.git`/`target`, hidden, or containing
/// path-separator characters) are skipped, so a generated contract can
/// never be invalid — writes to skipped entries stay fail-closed at the
/// gate (outside every scope).
fn workspace_scope_paths(root: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| is_contract_scope_name(name))
        .collect();
    names.sort();
    names
}

fn is_contract_scope_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name != "target"
        && name != ".git"
        && !name.starts_with('.')
        && !name.contains(['\\', ':', '\0'])
}

/// Trusted-workspace policy plus exactly the three grants the
/// `bench_matrix` example adds: normal reads/writes/test inside the
/// workspace run unattended; everything else still Asks (spec §33).
fn run_policy() -> Policy {
    let mut policy = Policy::trusted_workspace();
    policy.allow("mutation.patch", "workspace/**");
    policy.allow("fs.delete", "workspace/**");
    policy.allow("verify.command", "workspace/**");
    policy
}

/// Deterministic, bounded evidence for a run: sorted walk of the
/// workspace (skipping `.git`, `target` and hidden entries, never
/// following symlinked directories), capped at the runtime bounds'
/// request count and byte budget so a huge tree can never blow the
/// stage. No model input touches this selection.
fn collect_evidence_requests(root: &Path, bounds: &RuntimeBounds) -> Vec<EvidenceRequest> {
    const WALK_VISIT_BUDGET: usize = 4096;
    let mut files: Vec<(String, u64)> = Vec::new();
    let mut visits = 0_usize;
    walk_sorted(root, root, &mut files, &mut visits, WALK_VISIT_BUDGET);
    files.sort();
    let mut requests = Vec::new();
    let mut bytes = 0_u64;
    for (rel, size) in files {
        if requests.len() >= bounds.max_evidence_requests {
            break;
        }
        if bytes.saturating_add(size) > bounds.max_evidence_bytes_per_stage {
            continue;
        }
        bytes = bytes.saturating_add(size);
        requests.push(EvidenceRequest {
            capability: "fs.read".to_owned(),
            path: rel,
        });
    }
    requests
}

fn walk_sorted(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(String, u64)>,
    visits: &mut usize,
    budget: usize,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut names: Vec<_> = entries.flatten().collect();
    names.sort_by_key(std::fs::DirEntry::file_name);
    for entry in names {
        if *visits >= budget {
            return;
        }
        *visits += 1;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') || name_str == "target" {
            continue;
        }
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            walk_sorted(root, &path, out, visits, budget);
        } else if let Ok(metadata) = std::fs::metadata(&path)
            && let Ok(rel) = path.strip_prefix(root)
        {
            out.push((rel.to_string_lossy().replace('\\', "/"), metadata.len()));
        }
    }
}

#[cfg(test)]
mod stale_supervisor_tests {
    //! A supervisor that shuts down between map lookup and use (run
    //! completion drops the dead handle asynchronously) must not surface
    //! `supervisor_gone` to a client reading live state — the durable
    //! journal still answers after one recovery.

    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    async fn started_gateway() -> (RunningGateway, PathBuf) {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("tachyon-stale-{}-{id}", std::process::id()));
        let gateway = start_with(&dir, GatewayRuntime::default())
            .await
            .expect("gateway starts");
        (gateway, dir)
    }

    fn ok_payload(result: CommandResult) -> Value {
        match result {
            CommandResult::Ok { payload } => payload,
            CommandResult::Err { code, message } => {
                panic!("expected Ok, got {code}|{message}")
            }
        }
    }

    #[tokio::test]
    async fn get_task_recovers_past_a_dead_mapped_handle() {
        let (gateway, dir) = started_gateway().await;
        let state = gateway.state.clone();

        let session = ok_payload(handle_command(&state, &Command::CreateSession).await);
        let session_id: SessionId = session["session_id"]
            .as_str()
            .expect("session id")
            .parse()
            .expect("session id parses");
        let task = ok_payload(
            handle_command(
                &state,
                &Command::CreateTask {
                    session_id,
                    objective: "stale handle probe".to_owned(),
                },
            )
            .await,
        );
        let task_id: TaskId = task["task_id"]
            .as_str()
            .expect("task id")
            .parse()
            .expect("task id parses");

        // Manufacture the stale entry: the supervisor is shut down but the
        // map still points at the dead handle, exactly the window between
        // driver shutdown and the completion cleanup removing it.
        let dead = state
            .supervisors
            .lock()
            .await
            .get(&task_id)
            .cloned()
            .expect("supervisor mapped");
        dead.shutdown().await.expect("shutdown completes");

        let read = handle_command(&state, &Command::GetTask { task_id }).await;
        let payload = ok_payload(read);
        assert_eq!(
            payload["task"]["status"].as_str().unwrap_or(""),
            "Created",
            "durable journal answers after one recovery, no supervisor_gone"
        );

        gateway.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Seat5-B3 (R1): concurrent recoveries of one task elect a single
    /// recoverer; every racer gets the same live handle, none sees
    /// `task_already_owned`.
    #[tokio::test]
    async fn concurrent_get_task_recovers_once_with_no_already_owned() {
        let (gateway, dir) = started_gateway().await;
        let state = gateway.state.clone();

        let session = ok_payload(handle_command(&state, &Command::CreateSession).await);
        let session_id: SessionId = session["session_id"]
            .as_str()
            .expect("session id")
            .parse()
            .expect("session id parses");
        let task = ok_payload(
            handle_command(
                &state,
                &Command::CreateTask {
                    session_id,
                    objective: "recovery race probe".to_owned(),
                },
            )
            .await,
        );
        let task_id: TaskId = task["task_id"]
            .as_str()
            .expect("task id")
            .parse()
            .expect("task id parses");

        // Evict the mapped handle so every racer below misses the map and
        // must recover from the journal at the same instant.
        state.supervisors.lock().await.remove(&task_id);

        let mut racers = Vec::new();
        for _ in 0..16 {
            let state = state.clone();
            racers.push(tokio::spawn(async move {
                handle_command(&state, &Command::GetTask { task_id }).await
            }));
        }
        let mut already_owned = 0;
        for racer in racers {
            match racer.await.expect("racer completes") {
                CommandResult::Ok { .. } => {}
                CommandResult::Err { code, message } => {
                    if code == "task_already_owned" {
                        already_owned += 1;
                    } else {
                        panic!("unexpected refusal {code}|{message}");
                    }
                }
            }
        }
        assert_eq!(
            already_owned, 0,
            "single-flight recovery: no racer sees task_already_owned"
        );

        gateway.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R1 board B1: `workspace_busy` must refuse BEFORE the durable pin —
    /// a refused run pins nothing, so the retry (same root, once free)
    /// admits and pins normally. Under pin-before-lease the first
    /// refusal wedged the task on a root that never ran.
    #[tokio::test]
    async fn busy_workspace_leaves_no_pin_and_the_retry_admits() {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("tachyon-pinwedge-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ws = dir.join("ws");
        std::fs::create_dir_all(&ws).unwrap();
        std::fs::write(
            ws.join("Cargo.toml"),
            "[package]\nname=\"pinwedge\"\nversion=\"0.0.0\"\n",
        )
        .unwrap();
        let canonical = std::fs::canonicalize(&ws).unwrap();
        let runtime = GatewayRuntime {
            provider: Some(Arc::new(tachyon_models::fake::FakeModelProvider::new(
                tachyon_types::ProviderId("bench-script".into()),
            ))),
            label: FAKE_PROVIDER_LABEL.to_owned(),
            model: "scripted-replay-1".to_owned(),
            redactor: CredentialBroker::default(),
        };
        let gateway = start_with(&dir, runtime).await.expect("gateway starts");
        let state = gateway.state.clone();

        let session = ok_payload(handle_command(&state, &Command::CreateSession).await);
        let session_id: SessionId = session["session_id"]
            .as_str()
            .expect("session id")
            .parse()
            .expect("session id parses");
        let task = ok_payload(
            handle_command(
                &state,
                &Command::CreateTask {
                    session_id,
                    objective: "pin wedge probe".to_owned(),
                },
            )
            .await,
        );
        let task_id: TaskId = task["task_id"]
            .as_str()
            .expect("task id")
            .parse()
            .expect("task id parses");

        // Hold the workspace exactly as a competitor run would.
        let held = WorkspaceLease::try_acquire(&canonical)
            .await
            .expect("lease probe ok")
            .expect("workspace starts free");

        let refused = handle_command(
            &state,
            &Command::StartRun {
                task_id,
                workspace_root: canonical.display().to_string(),
                acceptance: None,
            },
        )
        .await;
        match refused {
            CommandResult::Err { code, .. } => {
                assert_eq!(code.as_str(), "workspace_busy", "competitor holds it");
            }
            CommandResult::Ok { .. } => panic!("expected workspace_busy"),
        }

        // THE blocker assertion: the refusal left no pin behind.
        let read = ok_payload(handle_command(&state, &Command::GetTask { task_id }).await);
        assert!(
            read["task"]["workspace_root"].is_null(),
            "busy refusal must leave no pin: {read}"
        );

        // Free the workspace: the same root now admits and pins.
        drop(held);
        let admitted = handle_command(
            &state,
            &Command::StartRun {
                task_id,
                workspace_root: canonical.display().to_string(),
                acceptance: None,
            },
        )
        .await;
        match admitted {
            CommandResult::Ok { payload } => {
                assert_eq!(
                    payload["workspace_root"].as_str(),
                    Some(canonical.to_str().expect("utf8 root")),
                    "retry pins the canonical root"
                );
            }
            CommandResult::Err { code, message } => {
                panic!("retry must admit, got {code}|{message}")
            }
        }

        gateway.shutdown().await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
