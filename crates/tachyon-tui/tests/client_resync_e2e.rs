//! D2 client resync E2E — the reader's overflow → re-`Subscribe` path
//! driven through the **real** tachyon-tui reader against an in-process
//! gateway (test harness only; `tachyon-gateway` is the harness dep of
//! this crate's tests, never display logic).
//!
//! The consumer deliberately stops draining: the client's 256-slot
//! event channel and the socket back up behind it, the server's own
//! 256-frame subscription event queue overflows, and the gateway owes
//! the client a `ResyncRequired`. The reader must then re-`Subscribe`
//! from its **own** last-parsed seq and the stream must catch up
//! gapless — strictly ordered accepted seqs, no hole across the resync,
//! final cursor equal to the server's last seq.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tachyon_protocol::Command;
use tachyon_tui::{AppState, AttachConfig, ClientEvent, CommandClient, Subscription};
use tachyon_types::{SessionId, TaskId};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "tachyon-m11-tui-resync-{}-{id}",
        std::process::id()
    ))
}

/// One timeout window for anything that should already be on the wire.
const WIRE: Duration = Duration::from_secs(10);

/// Synthetic events pushed while the consumer is not draining — far
/// beyond the server's 256-frame subscription event bound, so the
/// queue behind the stalled consumer must overflow into
/// `ResyncRequired` (the math: ≤256 in the client channel + the
/// socket's ~200 KiB buffering + 256 queued frames all sit *below*
/// this count, so the overflow fires with a wide margin).
const APPENDS: i64 = 4096;

/// Everything collected across the drains: accepted seqs in arrival
/// order plus every `Subscribe` ack's echoed `after_seq`.
#[derive(Debug, Default)]
struct Collected {
    /// Envelope seqs as accepted by the client.
    seqs: Vec<i64>,
    /// `after_seq` of each ack, in arrival order (first attach echoes -1;
    /// every later re-subscribe must echo a real own-cursor resume).
    acks: Vec<i64>,
}

/// Drains until `state` applies `target`. Every accepted envelope and
/// every ack is recorded (informational frames may trail replay — the
/// consumer contract skips them for state, not for this evidence).
async fn drain(
    sub: &mut Subscription,
    state: &mut AppState,
    target: i64,
    collected: &mut Collected,
    tag: &str,
) {
    while state.last_applied_seq < target {
        match tokio::time::timeout(WIRE, sub.recv()).await {
            Ok(Some(ClientEvent::Envelope(envelope))) => {
                collected.seqs.push(envelope.seq);
                state.apply_envelope(envelope);
            }
            Ok(Some(ClientEvent::Ack { after_seq, .. })) => collected.acks.push(after_seq),
            Ok(Some(_)) => {}
            Ok(None) => panic!("reader ended during {tag}: got {}", collected.seqs.len()),
            Err(error) => panic!(
                "timeout during {tag} after {} envelopes ({error})",
                collected.seqs.len()
            ),
        }
    }
    // Trailing informational frames (the replay `Ack` is pushed after
    // its rows) may sit behind the final envelope: sweep until the
    // channel goes quiet, per the skip-infrastructure contract.
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_millis(250), sub.recv()).await {
            Ok(Some(ClientEvent::Envelope(envelope))) => collected.seqs.push(envelope.seq),
            Ok(Some(ClientEvent::Ack { after_seq, .. })) => collected.acks.push(after_seq),
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break, // reader ended, or channel quiet
        }
    }
}

/// Creates the session and task (journal seq 0 = its `created` event);
/// returns the parsed task id and its wire string form.
async fn seed(client: &CommandClient) -> (TaskId, String) {
    let session = client
        .call(Command::CreateSession)
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
            objective: "resync objective".to_owned(),
        })
        .await
        .expect("CreateTask");
    let task_str = created["task_id"].as_str().expect("task_id").to_owned();
    let task: TaskId = task_str.parse().expect("task id parses");
    (task, task_str)
}

/// The gate sentence: push the journal past the server's event bound
/// while the consumer stalls, then prove the reader re-Subscribed from
/// its own cursor and caught up gapless to the server's last seq.
#[tokio::test]
async fn client_re_subscribes_from_its_own_cursor_on_overflow_and_catches_up_gapless() {
    let dir = test_dir();
    let gateway = tachyon_gateway::start(&dir)
        .await
        .expect("in-process gateway starts");
    let address = gateway.address().to_owned();
    let store = gateway.store();

    let client = CommandClient::connect(&address)
        .await
        .expect("command connection");
    let (task, task_str) = seed(&client).await;

    let mut sub = Subscription::attach(&address, Some(task), -1, AttachConfig::production());
    let mut state = AppState::new(Some(task));
    let mut collected = Collected::default();

    // (1) The initial replay (seq 0) lands through the real reader stack.
    drain(&mut sub, &mut state, 0, &mut collected, "initial replay").await;
    assert_eq!(collected.seqs, vec![0], "only the created event exists yet");
    assert_eq!(
        sub.last_parsed_seq(),
        0,
        "the reader's own last-parsed seq after the first replay"
    );

    // (2) The consumer stops draining: push the journal far beyond the
    // server's 256-frame event bound. The client's channel fills, its
    // socket stops draining, the server's queue overflows into
    // `ResyncRequired`, and the forwarder stalls until the re-Subscribe.
    for _ in 0..APPENDS {
        store
            .append_event(&task_str, "synthetic", "{}")
            .await
            .expect("append while the consumer stalls");
    }

    // (3) Drain to the server's last seq: across the overflow, the
    // re-Subscribe and its replay must deliver every missing seq once,
    // in order, with no hole.
    drain(
        &mut sub,
        &mut state,
        APPENDS,
        &mut collected,
        "post-overflow catch-up",
    )
    .await;

    let expected = i64::try_from(collected.seqs.len()).expect("length fits") - 1;
    assert_eq!(
        expected,
        APPENDS,
        "every journal frame exactly once (0..={APPENDS}), got {}",
        collected.seqs.len()
    );
    for (index, seq) in collected.seqs.iter().enumerate() {
        assert_eq!(
            *seq,
            i64::try_from(index).expect("index fits"),
            "accepted seqs are strictly ordered with no hole across the resync"
        );
    }

    assert!(
        collected.acks.len() >= 2,
        "the overflow must have produced a re-Subscribe (ack echoes the resumed cursor): {:?}",
        collected.acks
    );
    for (position, after_seq) in collected.acks.iter().enumerate() {
        assert!(
            *after_seq >= -1,
            "acks are non-negative or the initial -1: {:?}",
            collected.acks
        );
        if position > 0 {
            assert!(
                *after_seq >= 0,
                "the re-Subscribe resumed from the client's own cursor, never a full replay: {:?}",
                collected.acks
            );
        }
        if let Some(previous) = position.checked_sub(1).and_then(|p| collected.acks.get(p)) {
            assert!(
                previous <= after_seq,
                "own-cursor resumes never regress: {:?}",
                collected.acks
            );
        }
    }

    assert_eq!(
        sub.last_parsed_seq(),
        APPENDS,
        "final reader cursor == the server's last seq"
    );
    assert_eq!(
        state.last_applied_seq, APPENDS,
        "the state's applied cursor caught up gapless too"
    );

    drop(sub);
    drop(client);
    gateway.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
