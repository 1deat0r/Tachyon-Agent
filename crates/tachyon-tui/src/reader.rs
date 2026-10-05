//! Gateway-event reader task: the subscription half of an attach.
//!
//! Two connections per attach (plan D2): this module owns the subscription
//! connection and the client cursor rules — resume from the client's own
//! last-parsed seq, drop `seq <= last_seen` on replay, drop frames whose
//! `task_id` is not the attached task, and re-`Subscribe` from the own
//! cursor on `ResyncRequired`, with a bounded reconnect backoff.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use serde_json::Value;
use tachyon_protocol::{
    Command, CommandResult, EventEnvelope, GatewayEvent, PROTOCOL_VERSION, ResponseEnvelope,
    ServerFrame, decode_server_frame, encode_frame, frame_body_len,
};
use tachyon_types::{EventId, TaskId, Timestamp};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use tachyon_gateway::transport::{self, Stream};

/// Bounded channel between the reader task and the state owner (plan
/// item 2): backpressure, never an unbounded queue.
const EVENT_BUFFER: usize = 256;

/// What the client cursor decided about one event frame.
#[derive(Debug, PartialEq)]
pub enum FrameVerdict {
    /// Frame passes the D2 rules: apply it and advance the cursor.
    Accept(EventEnvelope),
    /// Frame fails the D2 rules: drop it silently.
    Drop,
    /// Server signalled overflow: re-`Subscribe` from the client's **own**
    /// last-parsed seq (the server's `after_seq` is a hint, never trusted
    /// over the client cursor).
    Resync,
}

/// The D2 client cursor: the attached task plus the last parsed seq.
#[derive(Debug)]
pub struct Cursor {
    /// Task this cursor belongs to.
    task_id: TaskId,
    /// Last sequence this client parsed and accepted; `after_seq` on open.
    last_seen: i64,
}

impl Cursor {
    /// Cursor resuming from `after_seq` for `task_id`.
    #[must_use]
    pub fn new(task_id: TaskId, after_seq: i64) -> Self {
        Self {
            task_id,
            last_seen: after_seq,
        }
    }

    /// The client's own last-parsed seq — the only cursor reconnects and
    /// resyncs are ever allowed to resume from.
    #[must_use]
    pub fn last_seen(&self) -> i64 {
        self.last_seen
    }

    /// Applies the D2 client rules to `envelope`, advancing the cursor
    /// only on acceptance. Order: the foreign-task guard outranks
    /// everything (plan D2: frames whose `task_id` is not the attached
    /// task are dropped before any dispatch — a stale task's
    /// `ResyncRequired` must never re-`Subscribe` this connection), then
    /// `ResyncRequired` outranks the seq filter (its envelope `seq`
    /// equals the server's hint and may sit below this cursor), then
    /// the replay filter.
    #[must_use]
    pub fn decide(&mut self, envelope: EventEnvelope) -> FrameVerdict {
        if envelope.task_id != self.task_id {
            return FrameVerdict::Drop;
        }
        if matches!(envelope.event, GatewayEvent::ResyncRequired { .. }) {
            return FrameVerdict::Resync;
        }
        if envelope.seq <= self.last_seen {
            return FrameVerdict::Drop;
        }
        self.last_seen = envelope.seq;
        FrameVerdict::Accept(envelope)
    }

    /// Repoints the cursor at another task (D3 re-`Subscribe`), resetting
    /// the cursor to that subscription's `after_seq`.
    #[cfg(test)]
    pub fn switch_to(&mut self, task_id: TaskId, after_seq: i64) {
        self.task_id = task_id;
        self.last_seen = after_seq;
    }
}

/// Events the reader task hands to the state owner / render loop.
#[derive(Debug)]
pub enum ClientEvent {
    /// Subscription acknowledged: replay rows were processed (or none
    /// were due) up to `after_seq`; the server's journal ends at `last_seq`.
    Ack {
        /// The `after_seq` the subscription resumed from.
        after_seq: i64,
        /// The server's last seq for this task at ack time.
        last_seq: i64,
        /// Optional top-level `provider_label` from the ack payload
        /// (plan G5: the fake provider's label; absent otherwise).
        provider_label: Option<String>,
    },
    /// One accepted durable event (post-cursor-rules).
    Envelope(EventEnvelope),
    /// Connection lost; bounded backoff reconnect in progress.
    Reconnecting {
        /// Failed attempts so far in this reconnect cycle.
        attempt: u32,
    },
    /// The reader gave up (bounded attempts exhausted, or a typed
    /// terminal failure). The channel closes after this.
    GaveUp {
        /// Why the reader stopped.
        reason: String,
    },
}

/// Reader-side control messages (task switches from the picker).
enum Control {
    /// Re-`Subscribe` this connection to another task from `after_seq`.
    Switch { task_id: TaskId, after_seq: i64 },
}

/// Handle to the subscription connection (the second of the two
/// connections per attach, D2). Dropping it detaches the reader — it
/// never emits a cancel of any kind.
pub struct Subscription {
    handle: JoinHandle<()>,
    events: mpsc::Receiver<ClientEvent>,
    control: mpsc::UnboundedSender<Control>,
    last_parsed: Arc<AtomicI64>,
}

impl Subscription {
    /// Opens the subscription connection and spawns the reader task
    /// attached to `task` (or waiting in the picker when `None`),
    /// resuming from `after_seq`.
    #[must_use]
    pub fn attach(
        address: &Path,
        task: Option<TaskId>,
        after_seq: i64,
        config: AttachConfig,
    ) -> Self {
        let (event_tx, events) = mpsc::channel(EVENT_BUFFER);
        let (control_tx, control) = mpsc::unbounded_channel();
        let last_parsed = Arc::new(AtomicI64::new(after_seq));
        let handle = tokio::spawn(reader_task(
            address.to_path_buf(),
            task,
            after_seq,
            config,
            event_tx,
            control,
            Arc::clone(&last_parsed),
        ));
        Self {
            handle,
            events,
            control: control_tx,
            last_parsed,
        }
    }

    /// Next event from the reader; `None` after the reader ended.
    pub async fn recv(&mut self) -> Option<ClientEvent> {
        self.events.recv().await
    }

    /// The client's own last-parsed seq (the only legal reconnect cursor).
    #[must_use]
    pub fn last_parsed_seq(&self) -> i64 {
        self.last_parsed.load(Ordering::SeqCst)
    }

    /// Repoints the subscription at another task (picker attach).
    pub fn switch(&self, task_id: TaskId, after_seq: i64) {
        let _ = self.control.send(Control::Switch { task_id, after_seq });
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Detach: stop the reader; its socket closes with the task.
        self.handle.abort();
    }
}

/// Bounds for the reader's reconnect backoff (plan D2: *bounded*
/// reconnect backoff — both the growth and the ceiling are capped).
#[derive(Clone, Copy, Debug)]
pub struct AttachConfig {
    /// Delay before the first reconnect attempt.
    pub base: Duration,
    /// Delay never exceeds this, however long the outage runs.
    pub max: Duration,
    /// Attempts before the reader gives up and reports `GaveUp`.
    pub max_attempts: u32,
}

impl AttachConfig {
    /// Production bounds: 100 ms doubling to a 2 s ceiling, 10 attempts.
    #[must_use]
    pub fn production() -> Self {
        Self {
            base: Duration::from_millis(100),
            max: Duration::from_secs(2),
            max_attempts: 10,
        }
    }
}

/// Delay before reconnect attempt number `attempt` (0-based): doubles
/// from the base and saturates at the configured ceiling.
#[must_use]
pub fn backoff_delay(attempt: u32, config: &AttachConfig) -> Duration {
    let mut delay = config.base;
    for _ in 0..attempt {
        if delay >= config.max {
            return config.max;
        }
        delay = delay.saturating_mul(2);
    }
    delay.min(config.max)
}

/// The reader task: connect (bounded backoff) → `Subscribe` from the own
/// cursor → pump frames through [`Cursor::decide`] → forward accepts on
/// the bounded channel. Reconnects on IO failure; re-`Subscribe`s from
/// the own cursor on `ResyncRequired`; switches tasks on `Control::Switch`.
#[allow(clippy::too_many_lines)]
async fn reader_task(
    address: std::path::PathBuf,
    mut attached: Option<TaskId>,
    mut after_seq: i64,
    config: AttachConfig,
    event_tx: mpsc::Sender<ClientEvent>,
    mut control: mpsc::UnboundedReceiver<Control>,
    last_parsed: Arc<AtomicI64>,
) {
    let mut cursor: Option<Cursor> = attached.map(|task| Cursor::new(task, after_seq));
    let mut stream: Option<Stream> = None;

    loop {
        // No subscription yet (picker start): wait for a switch.
        let Some(current) = attached else {
            match control.recv().await {
                Some(Control::Switch {
                    task_id,
                    after_seq: resume,
                }) => {
                    attached = Some(task_id);
                    after_seq = resume;
                    cursor = Some(Cursor::new(task_id, resume));
                    last_parsed.store(resume, Ordering::SeqCst);
                    stream = None; // fresh connection per task (D2)
                    continue;
                }
                None => return, // owner gone — detach
            }
        };

        // (Re)connect with the bounded backoff.
        if stream.is_none() {
            let mut failures: u32 = 0;
            loop {
                match transport::connect(&address).await {
                    Ok(connected) => {
                        stream = Some(connected);
                        break;
                    }
                    Err(error) => {
                        failures += 1;
                        if event_tx
                            .send(ClientEvent::Reconnecting { attempt: failures })
                            .await
                            .is_err()
                        {
                            return; // owner gone
                        }
                        if failures >= config.max_attempts {
                            let _ = event_tx
                                .send(ClientEvent::GaveUp {
                                    reason: format!(
                                        "connect failed after {failures} attempts: {error}"
                                    ),
                                })
                                .await;
                            return;
                        }
                        tokio::time::sleep(backoff_delay(failures - 1, &config)).await;
                    }
                }
            }
        }

        // Subscribe from the OWN cursor.
        let resume_from = cursor.as_ref().map_or(after_seq, Cursor::last_seen);
        let request = tachyon_protocol::RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command: Command::Subscribe {
                task_id: current,
                after_seq: resume_from,
            },
        };
        let Ok(bytes) = encode_frame(&request) else {
            return;
        };
        let active = stream.as_mut().expect("stream connected above");
        if active.write_all(&bytes).await.is_err() {
            stream = None;
            continue; // reconnect with backoff
        }

        // Read frames until the connection breaks or the task switches.
        loop {
            tokio::select! {
                incoming = read_frame(Some(stream.as_mut().expect("stream connected"))) => {
                    match incoming {
                        Ok(Some(ServerFrame::Response(response))) => {
                            let Some(cursor) = cursor.as_mut() else { continue; };
                            if handle_ack(&response, cursor, current, &event_tx, &last_parsed).await.is_err() {
                                return; // owner gone, or typed subscribe failure
                            }
                        }
                        Ok(Some(ServerFrame::Event(envelope))) => {
                            let Some(verdict) = cursor.as_mut().map(|c| c.decide(envelope)) else {
                                continue;
                            };
                            match verdict {
                                FrameVerdict::Accept(accepted) => {
                                    last_parsed.store(accepted.seq, Ordering::SeqCst);
                                    if event_tx.send(ClientEvent::Envelope(accepted)).await.is_err() {
                                        return; // owner gone — detach
                                    }
                                }
                                FrameVerdict::Drop => { /* replay/foreign: silent */ }
                                FrameVerdict::Resync => {
                                    // Re-Subscribe from the OWN cursor on
                                    // this same connection (D2).
                                    let own = cursor.as_ref().map_or(after_seq, Cursor::last_seen);
                                    let resubscribe = tachyon_protocol::RequestEnvelope {
                                        protocol_version: PROTOCOL_VERSION,
                                        request_id: EventId::generate(),
                                        command: Command::Subscribe { task_id: current, after_seq: own },
                                    };
                                    let Ok(resubscribe_bytes) = encode_frame(&resubscribe) else { return; };
                                    let active = stream.as_mut().expect("stream connected");
                                    if active.write_all(&resubscribe_bytes).await.is_err() {
                                        stream = None;
                                        break; // reconnect, same cursor
                                    }
                                }
                            }
                        }
                        // Clean EOF (no byte of the next frame) and any
                        // framing error both drop the socket into the
                        // bounded reconnect path.
                        Ok(None) | Err(_) => {
                            stream = None; // EOF/framing error → bounded reconnect
                            break;
                        }
                    }
                }
                cmd = control.recv() => {
                    match cmd {
                        Some(Control::Switch { task_id, after_seq: resume }) => {
                            attached = Some(task_id);
                            after_seq = resume;
                            cursor = Some(Cursor::new(task_id, resume));
                            last_parsed.store(resume, Ordering::SeqCst);
                            stream = None; // old task's subscription dies with its socket
                        }
                        None => return, // owner gone — detach
                    }
                    break;
                }
            }
        }
    }
}

/// Processes a response frame on the subscription connection: only the
/// `Subscribe` ack matters (rows → envelopes through the cursor); typed
/// subscribe failures end the reader.
async fn handle_ack(
    response: &ResponseEnvelope,
    cursor: &mut Cursor,
    current: TaskId,
    event_tx: &mpsc::Sender<ClientEvent>,
    last_parsed: &Arc<AtomicI64>,
) -> Result<(), ()> {
    if tachyon_protocol::check_version(response.protocol_version).is_err() {
        let _ = event_tx
            .send(ClientEvent::GaveUp {
                reason: "gateway speaks an incompatible protocol version".to_owned(),
            })
            .await;
        return Err(());
    }
    match &response.result {
        CommandResult::Ok { payload } => {
            // D3: the ack's task_id must match the attached task.
            let ack_task = payload.get("task_id").and_then(|value| value.as_str());
            let attached = current.to_string();
            if ack_task.is_some_and(|task| task != attached) {
                let _ = event_tx
                    .send(ClientEvent::GaveUp {
                        reason: "subscribe acked a different task".to_owned(),
                    })
                    .await;
                return Err(());
            }
            let after_seq = payload
                .get("after_seq")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            let last_seq = payload
                .get("last_seq")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            // Plan G5: optional top-level provider label (absent → None).
            let provider_label = payload
                .get("provider_label")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(rows) = payload.get("events").and_then(Value::as_array) {
                for row in rows {
                    let Some(envelope) = row_to_envelope(row, current) else {
                        continue; // unparseable row: skip, never panic
                    };
                    let verdict = cursor.decide(envelope);
                    if let FrameVerdict::Accept(accepted) = verdict {
                        last_parsed.store(accepted.seq, Ordering::SeqCst);
                        if event_tx
                            .send(ClientEvent::Envelope(accepted))
                            .await
                            .is_err()
                        {
                            return Err(()); // owner gone
                        }
                    }
                }
            }
            let _ = event_tx
                .send(ClientEvent::Ack {
                    after_seq,
                    last_seq,
                    provider_label,
                })
                .await;
            Ok(())
        }
        CommandResult::Err { code, message } => {
            let _ = event_tx
                .send(ClientEvent::GaveUp {
                    reason: format!("subscribe refused: {code}: {message}"),
                })
                .await;
            Err(())
        }
    }
}

/// Converts one `JournalEvent` row from the subscribe ack into a
/// protocol [`EventEnvelope`]. Rows that do not parse return `None`
/// (skipped, never a panic — unknown/partial rows are not fatal).
fn row_to_envelope(row: &serde_json::Value, task_id: TaskId) -> Option<EventEnvelope> {
    let seq = row.get("seq")?.as_i64()?;
    let kind = row.get("kind")?.as_str()?.to_owned();
    let payload = match row.get("payload")? {
        serde_json::Value::String(text) => serde_json::from_str(text).unwrap_or_default(),
        other => other.clone(),
    };
    let event_id = row
        .get("event_id")
        .and_then(|value| value.as_str())
        .and_then(|text| text.parse::<EventId>().ok())
        .unwrap_or_else(EventId::generate);
    let schema_version = row
        .get("schema_version")
        .and_then(Value::as_i64)
        .and_then(|value| u16::try_from(value).ok())
        .unwrap_or(PROTOCOL_VERSION);
    let timestamp = row
        .get("created_at")
        .and_then(Value::as_i64)
        .map_or_else(Timestamp::now, Timestamp::from_micros);
    Some(EventEnvelope {
        seq,
        event_id,
        schema_version,
        task_id,
        timestamp,
        event: GatewayEvent::Journal { kind, payload },
    })
}

/// Reads one length-prefixed [`ServerFrame`]. `Ok` with no value is a
/// clean EOF.
async fn read_frame(stream: Option<&mut Stream>) -> std::io::Result<Option<ServerFrame>> {
    let Some(stream) = stream else {
        return Ok(None);
    };
    let mut prefix = [0u8; 4];
    match stream.read_exact(&mut prefix).await {
        // tokio ≥1.45 `read_exact` returns the byte count; filling-or-error
        // semantics are unchanged, so the count is ignored here.
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let len = frame_body_len(prefix)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    // `decode_server_frame` parses the length prefix itself (the frame
    // starts at the head of the buffer), so re-attach it: prefix+body.
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&body);
    let (frame, _consumed) = decode_server_frame(&framed)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(Some(frame))
}

/// Builds a synthetic journal envelope for the unit tests.
#[cfg(test)]
pub(crate) fn journal_envelope(task_id: TaskId, seq: i64, kind: &str) -> EventEnvelope {
    EventEnvelope {
        seq,
        event_id: tachyon_types::EventId::generate(),
        schema_version: tachyon_protocol::PROTOCOL_VERSION,
        task_id,
        timestamp: tachyon_types::Timestamp::now(),
        event: GatewayEvent::Journal {
            kind: kind.to_owned(),
            payload: serde_json::json!({}),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AttachConfig, Cursor, FrameVerdict, backoff_delay, journal_envelope};
    use tachyon_protocol::{EventEnvelope, GatewayEvent};
    use tachyon_types::TaskId;

    /// D2 verbatim: the client resumes from its own last-parsed seq and
    /// drops `seq <= last_seen` on replay (replay must be idempotent).
    #[test]
    fn cursor_drops_replayed_frames_at_or_below_its_own_last_parsed_seq() {
        let task = TaskId::generate();
        let mut cursor = Cursor::new(task, 4);
        assert_eq!(
            cursor.last_seen(),
            4,
            "the cursor starts at the seq it resumed from"
        );

        let stale = journal_envelope(task, 4, "message");
        assert_eq!(
            cursor.decide(stale),
            FrameVerdict::Drop,
            "seq equal to the cursor is a replay, not new state"
        );
        let older = journal_envelope(task, 2, "message");
        assert_eq!(
            cursor.decide(older),
            FrameVerdict::Drop,
            "seq below the cursor is a replay"
        );
        assert_eq!(cursor.last_seen(), 4, "dropped frames never advance it");

        let fresh = journal_envelope(task, 5, "message");
        let seq_before = cursor.last_seen();
        let verdict = cursor.decide(fresh);
        assert!(
            matches!(&verdict, FrameVerdict::Accept(envelope) if envelope.seq == 5),
            "the first unseen seq is accepted: {verdict:?}"
        );
        assert_eq!(cursor.last_seen(), 5, "acceptance advances the cursor");
        assert!(seq_before < cursor.last_seen());
    }

    /// D2 verbatim: on re-Subscribe the client drops frames whose
    /// `task_id` is not the attached task, so a stale-task frame can never
    /// inflate the cursor and gap the new task's replay.
    #[test]
    fn cursor_drops_frames_belonging_to_a_different_task() {
        let attached = TaskId::generate();
        let foreign = TaskId::generate();
        let mut cursor = Cursor::new(attached, 0);

        let stranger = journal_envelope(foreign, 99, "message");
        assert_eq!(
            cursor.decide(stranger),
            FrameVerdict::Drop,
            "a foreign task's frame is never applied"
        );
        assert_eq!(
            cursor.last_seen(),
            0,
            "a foreign frame must not inflate the cursor"
        );

        // D3 switch: repoint, then only the new task's frames count.
        cursor.switch_to(foreign, -1);
        assert_eq!(cursor.last_seen(), -1, "switching resets the cursor");
        let now_ours = journal_envelope(foreign, 1, "message");
        assert!(
            matches!(cursor.decide(now_ours), FrameVerdict::Accept(_)),
            "the new task's frames pass after the switch"
        );
        let old_task = journal_envelope(attached, 2, "message");
        assert_eq!(
            cursor.decide(old_task),
            FrameVerdict::Drop,
            "the previous task's frames stay dropped after the switch"
        );
    }

    /// D2 verbatim: `ResyncRequired` is dispatched **before** the seq
    /// replay filter (its envelope seq equals the server hint) and the
    /// client re-Subscribes from its own cursor, not the server's hint.
    #[test]
    fn resync_is_detected_before_the_seq_filter_and_uses_the_own_cursor() {
        let task = TaskId::generate();
        let mut cursor = Cursor::new(task, 10);
        let hint_is_below_the_cursor = EventEnvelope {
            seq: 3,
            event_id: tachyon_types::EventId::generate(),
            schema_version: tachyon_protocol::PROTOCOL_VERSION,
            task_id: task,
            timestamp: tachyon_types::Timestamp::now(),
            event: GatewayEvent::ResyncRequired {
                task_id: task,
                after_seq: 3,
            },
        };

        assert_eq!(
            cursor.decide(hint_is_below_the_cursor),
            FrameVerdict::Resync,
            "the resync notice outranks the seq filter"
        );
        assert_eq!(
            cursor.last_seen(),
            10,
            "the server hint never moves the client's own cursor"
        );
        // The re-Subscribe cursor is `last_seen`, i.e. 10 — asserted again
        // live in the G4 respawn leg, where replay must be the exact gap.
        assert_eq!(cursor.last_seen(), 10);
    }

    /// D2: reconnect backoff is **bounded** — it doubles from the base,
    /// never exceeds the ceiling, and never grows without limit.
    #[test]
    fn reconnect_backoff_doubles_from_base_and_saturates_at_the_ceiling() {
        let config = AttachConfig::production();
        assert_eq!(
            backoff_delay(0, &config),
            Duration::from_millis(100),
            "first retry waits the base delay"
        );
        assert_eq!(
            backoff_delay(1, &config),
            Duration::from_millis(200),
            "the delay doubles per attempt"
        );
        assert_eq!(
            backoff_delay(5, &config),
            Duration::from_secs(2),
            "the delay caps at the 2 s ceiling"
        );
        assert_eq!(
            backoff_delay(50, &config),
            Duration::from_secs(2),
            "a long outage stays at the ceiling — bounded forever"
        );
        assert!(config.max_attempts > 0, "attempts are bounded too");
        assert!(
            backoff_delay(0, &config) <= config.max,
            "the base fits under its own ceiling"
        );
    }

    /// M11 board blocker (reader decide order): `ResyncRequired` may
    /// only outrank the **seq** filter — never the foreign-task guard
    /// (plan D2 drops frames whose `task_id` is not the attached task
    /// before anything else). A stale-task `ResyncRequired` must never
    /// trigger a re-`Subscribe`; the attached task's own resync still
    /// dispatches even when its seq sits below the cursor.
    #[test]
    fn foreign_task_resync_is_dropped_and_own_task_resync_still_dispatches() {
        let attached = TaskId::generate();
        let foreign = TaskId::generate();
        let mut cursor = Cursor::new(attached, 10);

        let stale_resync = EventEnvelope {
            seq: 3,
            event_id: tachyon_types::EventId::generate(),
            schema_version: tachyon_protocol::PROTOCOL_VERSION,
            task_id: foreign,
            timestamp: tachyon_types::Timestamp::now(),
            event: GatewayEvent::ResyncRequired {
                task_id: foreign,
                after_seq: 3,
            },
        };
        assert_eq!(
            cursor.decide(stale_resync),
            FrameVerdict::Drop,
            "a stale task's ResyncRequired must never re-Subscribe this connection"
        );
        assert_eq!(
            cursor.last_seen(),
            10,
            "a foreign resync moves nothing on this cursor"
        );

        let own_resync = EventEnvelope {
            seq: 3,
            event_id: tachyon_types::EventId::generate(),
            schema_version: tachyon_protocol::PROTOCOL_VERSION,
            task_id: attached,
            timestamp: tachyon_types::Timestamp::now(),
            event: GatewayEvent::ResyncRequired {
                task_id: attached,
                after_seq: 3,
            },
        };
        assert_eq!(
            cursor.decide(own_resync),
            FrameVerdict::Resync,
            "the attached task's resync still dispatches with its seq below the cursor"
        );
        assert_eq!(
            cursor.last_seen(),
            10,
            "the server hint never moves the client's own cursor"
        );
    }

    /// G5 ack contract: the `Subscribe` ack's optional top-level
    /// `provider_label` rides through `handle_ack` to the state owner —
    /// carried when the gateway declares the fake provider's label,
    /// `None` when the key is absent (absence renders nothing, never a
    /// panic). Synthetic payloads only: the live gateway key is the
    /// gateway writer's half of the contract.
    #[tokio::test]
    async fn subscribe_ack_carries_the_optional_provider_label() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicI64;

        use tokio::sync::mpsc;

        use tachyon_protocol::{CommandResult, ResponseEnvelope};

        use super::{ClientEvent, Cursor, handle_ack};

        let task = TaskId::generate();
        let mut cursor = Cursor::new(task, -1);
        let (tx, mut rx) = mpsc::channel::<ClientEvent>(4);
        let last_parsed = Arc::new(AtomicI64::new(-1));

        let ack = |label: Option<&str>| {
            let mut payload = serde_json::json!({
                "subscribed": true,
                "task_id": task.to_string(),
                "after_seq": -1,
                "last_seq": 0,
                "events": [],
            });
            if let Some(label) = label {
                payload["provider_label"] = serde_json::Value::String(label.to_owned());
            }
            ResponseEnvelope {
                protocol_version: tachyon_protocol::PROTOCOL_VERSION,
                request_id: tachyon_types::EventId::generate(),
                result: CommandResult::Ok { payload },
            }
        };

        handle_ack(
            &ack(Some("scripted test/replay provider")),
            &mut cursor,
            task,
            &tx,
            &last_parsed,
        )
        .await
        .expect("ack handles");
        let labelled = rx.recv().await.expect("the ack is forwarded");
        let ClientEvent::Ack {
            after_seq,
            last_seq,
            provider_label,
        } = labelled
        else {
            panic!("expected the subscribe ack: {labelled:?}")
        };
        assert_eq!(after_seq, -1, "the ack still carries its cursors");
        assert_eq!(last_seq, 0, "the ack still carries its server last");
        assert_eq!(
            provider_label.as_deref(),
            Some("scripted test/replay provider"),
            "the declared provider label rides the ack to the state owner"
        );

        handle_ack(&ack(None), &mut cursor, task, &tx, &last_parsed)
            .await
            .expect("ack without the key handles");
        let unlabelled = rx.recv().await.expect("the ack is forwarded");
        let ClientEvent::Ack { provider_label, .. } = unlabelled else {
            panic!("expected the subscribe ack: {unlabelled:?}")
        };
        assert_eq!(
            provider_label, None,
            "an ack without the key carries no label — absence, not a panic"
        );
    }

    /// T1 §2.4: a `0xFFFF_FFFF` prefix errors before any 4 GiB
    /// allocation. The shared protocol cap rejects the length right
    /// after the prefix, so the reader never builds the body.
    #[tokio::test]
    async fn oversize_prefix_errors_without_huge_allocation() {
        use tokio::io::AsyncWriteExt as _;

        use tachyon_gateway::transport::Listener;

        let sock = std::env::temp_dir().join(format!(
            "t24-reader-{}-{}.sock",
            std::process::id(),
            tachyon_types::EventId::generate()
        ));
        let _ = std::fs::remove_file(&sock);
        let listener = Listener::bind(&sock).expect("binds");
        let address = listener.local_address();
        let server = tokio::spawn(async move {
            let mut stream = listener.accept().await.expect("accepts");
            stream
                .write_all(&[0xFF; 4])
                .await
                .expect("writes the bad prefix");
            // Hold the connection open: the client must fail on the
            // prefix alone, without waiting for a body that never comes.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        });
        let mut client = tachyon_gateway::transport::connect(&address)
            .await
            .expect("connects");
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            super::read_frame(Some(&mut client)),
        )
        .await
        .expect("the oversize prefix fails fast, never hanging");
        let err = outcome.expect_err("0xFFFFFFFF must be an error, not a frame");
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "oversize prefix maps to InvalidData"
        );
        server.abort();
        let _ = std::fs::remove_file(&sock);
    }
}
