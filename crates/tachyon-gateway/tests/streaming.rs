//! M11 Phase A gates over the live gateway: G2 (streaming), G4(i)
//! (attach/disconnect/reconnect) and the G7 commit→frame measurement.
//!
//! Everything here runs over the real transport through
//! [`tachyon_gateway::start`], so the same assertions hold for the Unix
//! socket and the Windows named pipe: both present one framed byte stream.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_gateway::transport::{Stream, connect};
use tachyon_protocol::{
    Command, CommandResult, FRAME_PREFIX_LEN, GatewayEvent, PROTOCOL_VERSION, RequestEnvelope,
    ResponseEnvelope, ServerFrame, decode_server_frame, encode_frame,
};
use tachyon_types::EventId;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("tachyon-m11-{}-{id}", std::process::id()))
}

/// One gateway connection. Requests go out as framed [`RequestEnvelope`]s;
/// everything back must decode as a tagged [`ServerFrame`].
struct Client {
    stream: Stream,
    /// Event frames seen while waiting for a response.
    skipped: Vec<tachyon_protocol::EventEnvelope>,
}

impl Client {
    async fn open(address: &Path) -> Self {
        let stream = connect(address).await.expect("connect to gateway");
        Self {
            stream,
            skipped: Vec::new(),
        }
    }

    /// Sends `command` and returns the response, skipping any event frames
    /// that arrive first (they belong to a subscription on this socket).
    async fn request(&mut self, command: Command) -> ResponseEnvelope {
        let request = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command,
        };
        let bytes = encode_frame(&request).expect("encode request");
        self.stream.write_all(&bytes).await.expect("send request");
        self.skipped.clear();
        loop {
            match self.frame().await {
                ServerFrame::Response(response) => return response,
                ServerFrame::Event(envelope) => self.skipped.push(envelope),
            }
        }
    }

    /// Event frames this connection skipped while waiting for its last
    /// response, in arrival order.
    fn take_skipped(&mut self) -> Vec<tachyon_protocol::EventEnvelope> {
        std::mem::take(&mut self.skipped)
    }

    /// Sends `command` and returns its `Ok` payload, panicking on failure.
    async fn call(&mut self, command: Command) -> Value {
        match self.request(command).await.result {
            CommandResult::Ok { payload } => payload,
            CommandResult::Err { code, message } => panic!("command failed {code}: {message}"),
        }
    }

    /// Reads exactly one tagged frame.
    async fn frame(&mut self) -> ServerFrame {
        let bytes = self.frame_bytes().await;
        let (frame, _used) =
            decode_server_frame(&bytes).expect("tagged ServerFrame from the gateway");
        frame
    }

    /// Reads one frame, or `None` when nothing arrives within `timeout`.
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

    /// Half-closes this side's write half; the peer sees EOF while we can
    /// still read what it has to say. Unix-only: named pipes have no
    /// `SHUT_WR`, so the Windows close test drops the client instead.
    #[cfg(unix)]
    async fn close_write(&mut self) {
        self.stream.shutdown().await.expect("shutdown write half");
    }

    /// True when the peer closed the connection within `timeout`.
    ///
    /// Drains anything already in flight: pending frames prove the socket is
    /// still live, EOF (or a reset) proves the server dropped its half.
    async fn closed_within(&mut self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            let mut buffer = [0_u8; 4096];
            loop {
                if matches!(self.stream.read(&mut buffer).await, Ok(0) | Err(_)) {
                    return true;
                }
            }
        })
        .await
        .unwrap_or(false)
    }

    /// Sends `command` with no expectation of an answer: `true` when a
    /// response frame came back, `false` when the connection died first.
    async fn try_request(&mut self, command: Command) -> bool {
        let request = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command,
        };
        let bytes = encode_frame(&request).expect("encode request");
        if self.stream.write_all(&bytes).await.is_err() {
            return false;
        }
        // Any frame prefix arriving means the connection still answers;
        // EOF, reset or silence means it does not.
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut prefix = [0_u8; FRAME_PREFIX_LEN];
            self.stream.read_exact(&mut prefix).await.is_ok()
        })
        .await
        .unwrap_or(false)
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

/// Creates a session and a task with `messages` steering messages journalled
/// after creation (seq 0 = created, seq 1..=messages = messages).
async fn seeded_task(client: &mut Client, messages: u32) -> (String, String) {
    let session = client
        .call(Command::CreateSession {
            workspace_root: None,
        })
        .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = client
        .call(Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "stream probe".to_owned(),
        })
        .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    for index in 0..messages {
        client
            .call(Command::SendMessage {
                task_id: task_id.parse().unwrap(),
                message: format!("m{index}"),
            })
            .await;
    }
    (session_id, task_id)
}

#[tokio::test]
async fn attach_disconnect_both_then_reconnect_replays_the_gapless_suffix() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    // Attach per D2: one command connection, one subscription connection.
    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 2).await;
    let mut subscription = Client::open(&address).await;
    let ack = subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 0,
        })
        .await;
    let payload = ack_payload(ack.result);
    // The ack's replay array already carried seqs 1 and 2, so the client's
    // cursor starts at the ack's last_seq and live frames continue from there.
    let mut last_parsed = payload["last_seq"].as_i64().expect("last_seq");
    assert_eq!(last_parsed, 2);

    // Receive events while attached, remembering the last parsed seq.
    for _ in 0..3 {
        store
            .append_event(&task_id, "synthetic", "{}")
            .await
            .unwrap();
        let frame = subscription
            .frame_within(Duration::from_secs(2))
            .await
            .expect("live event while attached");
        let ServerFrame::Event(envelope) = frame else {
            panic!("expected an event frame, got {frame:?}");
        };
        assert_eq!(envelope.seq, last_parsed + 1, "events arrive in order");
        last_parsed = envelope.seq;
    }
    let before = command
        .call(Command::GetTask {
            task_id: task_id.parse().unwrap(),
        })
        .await;
    assert_eq!(before["task"]["status"], "Created");
    let revision_before = before["task"]["revision"].clone();

    // Disconnect BOTH connections (the M11 gate sentence: closing the TUI
    // must not cancel the task).
    drop(subscription);
    drop(command);

    let mut probe = Client::open(&address).await;
    let listed = probe.call(Command::ListTasks { session_id: None }).await;
    let tasks = listed["tasks"].as_array().expect("task list");
    assert_eq!(tasks.len(), 1, "the task survives both disconnects");
    assert_eq!(tasks[0]["status"], "Created", "disconnect cancels nothing");
    assert_eq!(
        tasks[0]["revision"], revision_before,
        "disconnect changes no state"
    );

    // Events keep committing while detached.
    for _ in 0..4 {
        store
            .append_event(&task_id, "synthetic", "{}")
            .await
            .unwrap();
    }

    // Reconnect the subscription from the client's own last-parsed seq: the
    // ack must carry exactly the suffix that was missed, gapless.
    let mut reconnected = Client::open(&address).await;
    let ack = reconnected
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: last_parsed,
        })
        .await;
    let payload = match ack.result {
        CommandResult::Ok { payload } => payload,
        other @ CommandResult::Err { .. } => {
            panic!("reconnect subscribe must succeed, got {other:?}")
        }
    };
    assert_eq!(payload["after_seq"], last_parsed);
    assert_eq!(
        payload["last_seq"], 9,
        "four events committed while detached"
    );
    let replayed: Vec<i64> = payload["events"]
        .as_array()
        .expect("ack carries the suffix")
        .iter()
        .map(|event| event["seq"].as_i64().expect("event seq"))
        .collect();
    assert_eq!(
        replayed,
        vec![6, 7, 8, 9],
        "exact gapless suffix from the client's last-parsed seq"
    );

    // And the reconnected subscription streams live again.
    store
        .append_event(&task_id, "synthetic", "{}")
        .await
        .unwrap();
    let frame = reconnected
        .frame_within(Duration::from_secs(2))
        .await
        .expect("live event after reconnect");
    let ServerFrame::Event(envelope) = frame else {
        panic!("expected an event frame, got {frame:?}");
    };
    assert_eq!(envelope.seq, 10);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// M11 G7 measurement artifact: commit (`t0`, `StoreWriter` return) to
/// `EventEnvelope` frame arrival (`t1`) over n ≥ 100 synthetic events,
/// reported as p50/p95 against §43's < 50 ms p95 target.
///
/// `t1` is observed where the frame is read back off the socket, which is
/// *after* the server's write completed — a deliberately conservative upper
/// bound on the number the gate names.
#[tokio::test]
async fn g7_reports_commit_to_frame_latency_percentiles() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 0).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 0,
        })
        .await;

    let samples = 200_usize;
    let mut latencies = Vec::with_capacity(samples);
    for index in 0..samples {
        store
            .append_event(&task_id, "synthetic", &format!("{{\"i\":{index}}}"))
            .await
            .unwrap();
        let committed = Instant::now(); // t0: StoreWriter commit has returned
        let frame = subscription
            .frame_within(Duration::from_secs(10))
            .await
            .expect("event frame for every committed event");
        let received = Instant::now(); // t1: frame fully read from the socket
        let ServerFrame::Event(envelope) = frame else {
            panic!("expected an event frame, got {frame:?}");
        };
        assert_eq!(envelope.seq, i64::try_from(index).expect("index fits") + 1);
        latencies.push(received.duration_since(committed));
    }

    latencies.sort();
    let n = latencies.len();
    assert!(n >= 100, "the gate needs at least 100 samples, got {n}");
    let p50 = latencies[n * 50 / 100];
    let p95 = latencies[(n * 95 / 100).min(n - 1)];
    let within_target = p95 < Duration::from_millis(50);
    println!(
        "G7 measurement: n={n} p50={:?} p95={:?} target p95<50ms={} (t0=commit return, t1=frame read off socket)",
        p50,
        p95,
        if within_target { "PASS" } else { "MISS" }
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn closing_the_subscription_tears_down_both_halves_and_leaves_the_task() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 1).await;
    let before = command
        .call(Command::GetTask {
            task_id: task_id.parse().unwrap(),
        })
        .await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 1,
        })
        .await;

    // Client disconnect must not cancel the task.
    //
    // Unix: half-close (SHUT_WR) → server reader EOF → writer torn down →
    // client observes EOF on the same connection.
    // Windows: named pipes have no half-close; tokio's NamedPipeClient
    // poll_shutdown only flushes and never signals the peer. Full drop
    // is the disconnect the server can observe.
    #[cfg(unix)]
    {
        subscription.close_write().await;
        assert!(
            subscription.closed_within(Duration::from_secs(10)).await,
            "reader exit must drop the write half as well"
        );
    }
    #[cfg(windows)]
    {
        drop(subscription);
    }

    // Closing a connection is not cancelling a task.
    let after = command
        .call(Command::GetTask {
            task_id: task_id.parse().unwrap(),
        })
        .await;
    assert_eq!(after["task"]["status"], before["task"]["status"]);
    assert_eq!(after["task"]["revision"], before["task"]["revision"]);
    assert_ne!(after["task"]["status"], "Cancelled");

    // Closing the other side (drop the command connection outright) is just
    // as harmless: a fresh connection still sees the same task.
    drop(command);
    let mut reopened = Client::open(&address).await;
    let listed = reopened.call(Command::ListTasks { session_id: None }).await;
    let tasks = listed["tasks"].as_array().expect("task list");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["status"], "Created");
    assert_eq!(tasks[0]["revision"], 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn writer_failure_tears_down_both_halves_and_stops_answering() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 1).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 1,
        })
        .await;

    // A journal row the writer cannot frame (>64 MiB): the write half dies.
    let oversized = format!("\"{}\"", "x".repeat(tachyon_protocol::MAX_FRAME_BYTES));
    store
        .append_event(&task_id, "synthetic", &oversized)
        .await
        .unwrap();

    assert!(
        subscription.closed_within(Duration::from_secs(10)).await,
        "a writer failure must close the connection, not strand it"
    );

    // Nothing may answer on a connection whose event path died.
    assert!(
        !subscription.try_request(Command::Ping).await,
        "an event-dead connection must not keep answering commands"
    );

    // The task itself is untouched.
    let listed = command.call(Command::ListTasks { session_id: None }).await;
    let tasks = listed["tasks"].as_array().expect("task list");
    assert_eq!(tasks[0]["status"], "Created");
    assert_eq!(tasks[0]["revision"], 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn forwarder_failure_tears_down_both_halves_and_leaves_the_task() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 1).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 1,
        })
        .await;

    // A journal row the forwarder cannot turn into an event payload.
    store
        .append_event(&task_id, "synthetic", "not json at all")
        .await
        .unwrap();

    assert!(
        subscription.closed_within(Duration::from_secs(5)).await,
        "a forwarder failure must close the whole connection"
    );
    assert!(
        !subscription.try_request(Command::Ping).await,
        "an event-dead connection must not keep answering commands"
    );

    let listed = command.call(Command::ListTasks { session_id: None }).await;
    let tasks = listed["tasks"].as_array().expect("task list");
    assert_eq!(tasks[0]["status"], "Created");
    assert_eq!(tasks[0]["revision"], 1);

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn commit_burst_larger_than_the_broadcast_capacity_loses_nothing() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 0).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: -1,
        })
        .await;

    // A burst well past the commit broadcast capacity: any notification the
    // forwarder's receiver overwrites is reported as `Lagged`, and the only
    // way to recover is the by-cursor journal pull. (The `Lagged` branch
    // itself is forced and asserted in tachyon-store's
    // `lagged_receiver_catches_up_from_the_journal_by_cursor`.)
    //
    // The outbound event queue (256) can also overflow if the client has not
    // drained yet — the gateway then sends `ResyncRequired`. That is correct
    // product behavior on a slow subscriber; recover by re-subscribing from
    // the cursor and folding the ack's replayed tail into the same gapless
    // set (same contract as `overflow_resyncs_…`).
    let burst = i64::try_from(tachyon_store::COMMIT_NOTIFICATION_CAPACITY)
        .expect("capacity fits i64")
        + 144;
    for index in 1..=burst {
        store
            .append_event(&task_id, "synthetic", &format!("{{\"i\":{index}}}"))
            .await
            .unwrap();
    }

    let burst_count = usize::try_from(burst).expect("burst fits usize");
    let mut received: Vec<i64> = Vec::new();
    while received.len() < burst_count {
        let frame = subscription
            .frame_within(Duration::from_secs(10))
            .await
            .unwrap_or_else(|| panic!("stream stalled after {} of {burst} events", received.len()));
        match frame {
            ServerFrame::Event(envelope) => match envelope.event {
                GatewayEvent::Journal { .. } => received.push(envelope.seq),
                GatewayEvent::ResyncRequired { after_seq, .. } => {
                    let again = subscription
                        .request(Command::Subscribe {
                            task_id: task_id.parse().unwrap(),
                            after_seq,
                        })
                        .await;
                    let payload = match again.result {
                        CommandResult::Ok { payload } => payload,
                        other @ CommandResult::Err { .. } => {
                            panic!("re-subscribe must succeed, got {other:?}")
                        }
                    };
                    assert_eq!(payload["after_seq"], after_seq, "resume cursor");
                    if let Some(events) = payload["events"].as_array() {
                        for event in events {
                            received.push(event["seq"].as_i64().expect("event seq"));
                        }
                    }
                }
                other => panic!("unexpected event {other:?}"),
            },
            ServerFrame::Response(_) => {}
        }
    }
    received.sort_unstable();
    received.dedup();
    assert_eq!(
        received,
        (1..=burst).collect::<Vec<i64>>(),
        "catch-up after lag must deliver every committed event, gapless and ordered"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn overflow_resyncs_at_the_last_written_cursor_and_replays_the_missed_tail() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 0).await;

    let mut subscription = Client::open(&address).await;
    let ack = subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: -1,
        })
        .await;
    let first = match ack.result {
        CommandResult::Ok { payload } => payload,
        other @ CommandResult::Err { .. } => panic!("subscribe must succeed, got {other:?}"),
    };
    assert_eq!(first["last_seq"], 0);

    // Commit far more than the socket can absorb while the subscriber stays
    // silent: the writer blocks on a full socket, the bounded queue fills,
    // and the forwarder overflows and stops advancing.
    let pad = "b".repeat(64 * 1024);
    let total = 600_i64;
    for _ in 0..total {
        store
            .append_event(&task_id, "synthetic", &format!("{{\"pad\":\"{pad}\"}}"))
            .await
            .unwrap();
    }
    // Give the forwarder time to reach the bound before we start draining —
    // once the client reads, the writer unblocks and the race would close.
    tokio::time::sleep(Duration::from_millis(750)).await;

    // Everything the socket actually delivered, then the resync notice.
    let mut delivered: Vec<i64> = Vec::new();
    let resync_cursor = loop {
        let frame = subscription
            .frame_within(Duration::from_secs(5))
            .await
            .expect("a frame after the commit burst");
        match frame {
            ServerFrame::Event(envelope) => match envelope.event {
                GatewayEvent::ResyncRequired {
                    task_id: affected,
                    after_seq,
                } => {
                    assert_eq!(affected.to_string(), task_id, "resync names the task");
                    break after_seq;
                }
                GatewayEvent::Journal { .. } => delivered.push(envelope.seq),
                other => panic!("unexpected event {other:?}"),
            },
            ServerFrame::Response(_) => {}
        }
    };

    assert!(
        resync_cursor < total,
        "the resync must leave a never-written tail (cursor {resync_cursor} of {total})"
    );
    assert_eq!(
        delivered.last().copied(),
        Some(resync_cursor),
        "the resync cursor is the last sequence whose write completed on the socket"
    );
    assert_eq!(
        delivered,
        (1..=resync_cursor).collect::<Vec<i64>>(),
        "delivered frames are gapless and ordered from the ack cursor"
    );

    // A follow-up Subscribe from that cursor replays exactly the tail that
    // never reached the client — never-written frames included.
    let again = subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: resync_cursor,
        })
        .await;
    let payload = match again.result {
        CommandResult::Ok { payload } => payload,
        other @ CommandResult::Err { .. } => panic!("re-subscribe must succeed, got {other:?}"),
    };
    assert_eq!(payload["after_seq"], resync_cursor);
    assert_eq!(payload["last_seq"], total, "the journal kept every commit");
    let replayed: Vec<i64> = payload["events"]
        .as_array()
        .expect("ack carries the replayed tail")
        .iter()
        .map(|event| event["seq"].as_i64().expect("event seq"))
        .collect();
    assert_eq!(
        replayed,
        ((resync_cursor + 1)..=total).collect::<Vec<i64>>(),
        "the replay is exactly the never-written tail, gapless"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn subscribed_connection_still_serves_commands() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 1).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 1,
        })
        .await;

    // Ordinary requests keep working on the subscribed connection, before,
    // between and after pushed events (G2).
    let pong = subscription.request(Command::Ping).await;
    assert!(matches!(pong.result, CommandResult::Ok { .. }));

    store
        .append_event(&task_id, "synthetic", "{}")
        .await
        .unwrap();
    let event = subscription
        .frame_within(Duration::from_secs(2))
        .await
        .expect("event frame between requests");
    assert!(matches!(event, ServerFrame::Event(_)));

    let status = subscription.request(Command::GetStatus).await;
    match status.result {
        CommandResult::Ok { payload } => assert!(payload["active_tasks"].is_u64()),
        other @ CommandResult::Err { .. } => {
            panic!("GetStatus must be served on a subscribed connection: {other:?}")
        }
    }

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn resubscribe_flushes_previous_task_frames_before_the_reack() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let mut command = Client::open(&address).await;
    let (_, task_a) = seeded_task(&mut command, 3).await;
    let (_, task_b) = seeded_task(&mut command, 2).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_a.parse().unwrap(),
            after_seq: 1,
        })
        .await;

    // Flood task A with large frames while the subscriber is not reading,
    // so some of them are still queued when the switch happens and the
    // flush order is actually exercised rather than incidental.
    let pad = "a".repeat(64 * 1024);
    for _ in 0..16 {
        store
            .append_event(&task_a, "synthetic", &format!("{{\"pad\":\"{pad}\"}}"))
            .await
            .unwrap();
    }

    let ack = subscription
        .request(Command::Subscribe {
            task_id: task_b.parse().unwrap(),
            after_seq: -1,
        })
        .await;
    let payload = match ack.result {
        CommandResult::Ok { payload } => payload,
        other @ CommandResult::Err { .. } => panic!("re-subscribe must succeed, got {other:?}"),
    };
    assert_eq!(
        payload["task_id"], task_b,
        "the ack belongs to the new task"
    );

    // Everything that reached the client before the re-ack is previous-task
    // traffic, flushed ahead of it.
    for frame in subscription.take_skipped() {
        assert_eq!(
            frame.task_id.to_string(),
            task_a,
            "previous-task frames must be flushed before the re-ack"
        );
    }

    // Nothing of task A may follow the re-ack: the next pushed frame has to
    // belong to task B.
    store
        .append_event(&task_b, "synthetic", "{}")
        .await
        .unwrap();
    let frame = subscription
        .frame_within(Duration::from_secs(2))
        .await
        .expect("new-task event frame after the re-ack");
    let ServerFrame::Event(envelope) = frame else {
        panic!("expected an event frame, got {frame:?}");
    };
    assert_eq!(
        envelope.task_id.to_string(),
        task_b,
        "no previous-task frame may follow the re-ack"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn live_events_arrive_after_subscribe_until_commits_stop() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 1).await;

    let mut subscription = Client::open(&address).await;
    subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 1,
        })
        .await;

    // A commit made after the ack must arrive as a pushed event frame.
    command
        .call(Command::SendMessage {
            task_id: task_id.parse().unwrap(),
            message: "live".to_owned(),
        })
        .await;
    let frame = subscription
        .frame_within(Duration::from_secs(2))
        .await
        .expect("live event frame after Subscribe");
    let ServerFrame::Event(envelope) = frame else {
        panic!("expected an event frame, got {frame:?}");
    };
    assert_eq!(
        envelope.seq, 2,
        "the committed seq is pushed to the subscriber"
    );
    assert_eq!(envelope.task_id.to_string(), task_id);
    match &envelope.event {
        GatewayEvent::Journal { kind, payload } => {
            assert_eq!(kind, "message", "kind is passed through opaquely");
            assert!(payload.is_object(), "payload is the journalled StateEvent");
        }
        other => panic!("expected a journal event, got {other:?}"),
    }

    // No commits means no frames: the forwarder must idle, not spin.
    assert!(
        subscription
            .frame_within(Duration::from_millis(300))
            .await
            .is_none(),
        "an idle subscription must not emit frames"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Unwraps a successful Subscribe ack payload; panics otherwise.
fn ack_payload(result: CommandResult) -> Value {
    match result {
        CommandResult::Ok { payload } => payload,
        other @ CommandResult::Err { .. } => panic!("expected an ack, got {other:?}"),
    }
}

#[tokio::test]
async fn subscribe_acks_and_replays_the_gapless_ordered_tail() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let address = gateway.address().to_owned();

    let mut command = Client::open(&address).await;
    let (_, task_id) = seeded_task(&mut command, 3).await;

    let mut subscription = Client::open(&address).await;
    let response = subscription
        .request(Command::Subscribe {
            task_id: task_id.parse().unwrap(),
            after_seq: 1,
        })
        .await;
    assert_eq!(
        response.protocol_version, PROTOCOL_VERSION,
        "responses carry the current protocol version"
    );
    let payload = match response.result {
        CommandResult::Ok { payload } => payload,
        other @ CommandResult::Err { .. } => panic!("subscribe must succeed, got {other:?}"),
    };
    assert_eq!(
        payload["subscribed"], true,
        "ack must confirm the subscription"
    );
    assert_eq!(payload["task_id"], task_id, "ack echoes the task");
    assert_eq!(payload["after_seq"], 1, "ack echoes the requested cursor");
    assert_eq!(payload["last_seq"], 3, "ack carries the current max seq");
    let replayed: Vec<i64> = payload["events"]
        .as_array()
        .expect("ack still carries the replayed events array")
        .iter()
        .map(|event| event["seq"].as_i64().expect("event seq"))
        .collect();
    assert_eq!(
        replayed,
        vec![2, 3],
        "replay must be gapless, ordered, and strictly after after_seq"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
