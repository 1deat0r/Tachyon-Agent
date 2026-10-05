//! The command half of an attach (plan D2: two connections per attach —
//! one command connection, one subscription connection).
//!
//! [`CommandClient`] owns the command connection as a strict
//! request/response pipe: every [`Command`] is framed with a correlation
//! id and its [`ResponseEnvelope`] is routed back to the waiting caller.
//! Dropping every clone closes the connection — which is exactly what
//! detach means.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tachyon_protocol::{
    Command, PROTOCOL_VERSION, RequestEnvelope, ResponseEnvelope, decode_server_frame,
    encode_frame, frame_body_len,
};
use tachyon_types::EventId;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, split};
use tokio::sync::{mpsc, oneshot};

/// Why a [`CommandClient::call`] could not be answered.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// The connection is gone (dropped, EOF, or a framing failure).
    #[error("command connection closed before the response arrived")]
    Closed,
    /// The gateway answered with a typed error.
    #[error("gateway refused the command: {code}: {message}")]
    Command {
        /// Stable machine-readable code.
        code: String,
        /// Human-readable message.
        message: String,
    },
}

/// The waiter registry: every call still waiting for its correlated
/// response, plus the `closed` flag that seals the registry when the
/// connection task ends. The flag lives in the **same mutex** as the
/// insert so a teardown can never strand a caller: a call either
/// registers before the seal (and is drained into a dropped sender →
/// `Closed`) or is rejected at the seal — never parked forever.
#[derive(Default)]
struct Waiters {
    /// Set exactly once, under this mutex, when the connection task can
    /// never answer again. Checked by every `call_raw` before it inserts.
    closed: bool,
    /// Waiters keyed by correlation id.
    senders: HashMap<EventId, oneshot::Sender<ResponseEnvelope>>,
}

/// A caller clone of the command connection. `call` is the only send
/// path; unsolicited frames are not a thing on this connection (D2:
/// strict request/response).
#[derive(Clone)]
pub struct CommandClient {
    tx: mpsc::UnboundedSender<(EventId, Command)>,
    pending: Arc<Mutex<Waiters>>,
}

impl CommandClient {
    /// Opens the command connection to `address` and spawns its
    /// framing/routing task.
    pub async fn connect(address: &Path) -> io::Result<Self> {
        let stream = tachyon_gateway::transport::connect(address).await?;
        let (read_half, mut write_half) = split(stream);
        let (tx, mut rx) = mpsc::unbounded_channel::<(EventId, Command)>();
        let pending: Arc<Mutex<Waiters>> = Arc::new(Mutex::new(Waiters::default()));
        let waiter_registry = Arc::clone(&pending);

        tokio::spawn(async move {
            let mut read_half = read_half;
            loop {
                tokio::select! {
                    // Outgoing command: frame it with its correlation id.
                    outgoing = rx.recv() => {
                        let Some((request_id, command)) = outgoing else {
                            break; // every client clone dropped — detach.
                        };
                        let request = RequestEnvelope {
                            protocol_version: PROTOCOL_VERSION,
                            request_id,
                            command,
                        };
                        let Ok(bytes) = encode_frame(&request) else {
                            break;
                        };
                        if write_half.write_all(&bytes).await.is_err() {
                            break;
                        }
                    }
                    // Incoming response: route it to its waiter.
                    incoming = read_response(&mut read_half) => {
                        match incoming {
                            Ok(Some(response)) => {
                                let waiter = waiter_registry
                                    .lock()
                                    .expect("pending map poisoned")
                                    .senders
                                    .remove(&response.request_id);
                                if let Some(waiter) = waiter {
                                    let _ = waiter.send(response);
                                }
                                // Unmatched request ids are dropped: a
                                // caller can only be late by timing out,
                                // never by receiving someone else's reply.
                            }
                            Ok(None) | Err(_) => break, // EOF or framing error
                        }
                    }
                }
            }
            // Task end: this connection can never answer anyone. Seal the
            // registry AND drain every waiter in ONE critical section on
            // the same mutex `call_raw` inserts under — dropping each
            // sender wakes its caller with `Closed`. Because the seal is
            // checked before any insert, a call can never land in the
            // registry after the drain and be stranded there (the old
            // clear-then-drop(rx) gap parked exactly such a caller
            // forever).
            {
                let mut waiters = waiter_registry.lock().expect("pending map poisoned");
                waiters.closed = true;
                waiters.senders.clear();
            }
            // Drop the write half (half-close); then the registry Arc.
            drop(write_half);
        });

        Ok(Self { tx, pending })
    }

    /// Sends one command and waits for its correlated response payload.
    /// Typed gateway errors become [`CallError::Command`].
    pub async fn call(&self, command: Command) -> Result<serde_json::Value, CallError> {
        let response = self.call_raw(command).await?;
        match response.result {
            tachyon_protocol::CommandResult::Ok { payload } => Ok(payload),
            tachyon_protocol::CommandResult::Err { code, message } => {
                Err(CallError::Command { code, message })
            }
        }
    }

    /// Sends one command and waits for the full [`ResponseEnvelope`]
    /// (used when the raw result matters, e.g. feeding `apply_response`).
    pub async fn call_raw(&self, command: Command) -> Result<ResponseEnvelope, CallError> {
        let request_id = EventId::generate();
        let (reply, waiting) = oneshot::channel();
        {
            // The seal and the insert share this mutex: a registry that
            // already closed can never receive a new waiter.
            let mut waiters = self.pending.lock().expect("pending map poisoned");
            if waiters.closed {
                return Err(CallError::Closed);
            }
            waiters.senders.insert(request_id, reply);
        }
        if self.tx.send((request_id, command)).is_err() {
            self.pending
                .lock()
                .expect("pending map poisoned")
                .senders
                .remove(&request_id);
            return Err(CallError::Closed);
        }
        waiting.await.map_err(|_| CallError::Closed)
    }
}

/// Reads one length-prefixed response frame. `Ok(None)` is a clean EOF
/// before any byte of a frame; a short or undecodable frame is an error.
async fn read_response(
    read_half: &mut tokio::io::ReadHalf<tachyon_gateway::transport::Stream>,
) -> io::Result<Option<ResponseEnvelope>> {
    let mut prefix = [0u8; 4];
    match read_half.read_exact(&mut prefix).await {
        // tokio ≥1.45 `read_exact` returns the byte count; filling-or-error
        // semantics are unchanged, so the count is ignored here.
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let len = frame_body_len(prefix)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut body = vec![0u8; len];
    read_half.read_exact(&mut body).await?;
    // `decode_server_frame` parses the length prefix itself (the frame
    // starts at the head of the buffer), so re-attach it: prefix+body.
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&body);
    let (frame, _consumed) = decode_server_frame(&framed)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    match frame {
        tachyon_protocol::ServerFrame::Response(response) => Ok(Some(response)),
        tachyon_protocol::ServerFrame::Event(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "event frame on a command connection",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tachyon_gateway::transport::{Listener, Stream};
    use tachyon_protocol::{
        Command, CommandResult, PROTOCOL_VERSION, RequestEnvelope, ResponseEnvelope, decode_frame,
        encode_frame,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::CommandClient;

    /// One bounded window for a call that should already have been
    /// answered or closed — a caller still parked when it elapses is a
    /// hang, and the test fails instead of hanging itself.
    const CALL_WINDOW: Duration = Duration::from_millis(500);
    /// Fresh connection + teardown rounds. The pre-fix bug lives in a
    /// microsecond-wide window (between the task end clearing the waiter
    /// map and `rx` dropping), so one teardown may miss it: the test
    /// races the window repeatedly and still bounds every round.
    const ROUNDS: u32 = 60;
    /// Concurrent callers spamming across the teardown moment.
    const CALLERS: usize = 8;
    /// Healthy-spam warmup before the waiter map is pinned, so callers
    /// are mid-loop (and their next insert lands near the teardown).
    const WARMUP: Duration = Duration::from_millis(30);
    /// Settle time for the EOF to end the reader task and for callers
    /// to queue behind the pinned mutex.
    const TEARDOWN_SETTLE: Duration = Duration::from_millis(30);

    /// A gateway-lite that answers every request frame with its
    /// correlated response, ending when its stream drops — dropping it
    /// is the "kill the reader path" lever (the client task then reads
    /// EOF and runs its task-end path).
    async fn answer_requests(mut stream: Stream) {
        loop {
            let mut prefix = [0u8; 4];
            if stream.read_exact(&mut prefix).await.is_err() {
                return; // stream killed, or client gone
            }
            let len = u32::from_le_bytes(prefix) as usize;
            if len > tachyon_protocol::MAX_FRAME_BYTES - tachyon_protocol::FRAME_PREFIX_LEN {
                return; // oversized frame: this lite server refuses to guess
            }
            let mut body = vec![0u8; len];
            if stream.read_exact(&mut body).await.is_err() {
                return;
            }
            let mut framed = prefix.to_vec();
            framed.extend_from_slice(&body);
            let Ok((request, _)) = decode_frame::<RequestEnvelope>(&framed) else {
                return;
            };
            let response = ResponseEnvelope {
                protocol_version: PROTOCOL_VERSION,
                request_id: request.request_id,
                result: CommandResult::Ok {
                    payload: serde_json::json!({"answered": true}),
                },
            };
            let Ok(bytes) = encode_frame(&response) else {
                return;
            };
            if stream.write_all(&bytes).await.is_err() {
                return;
            }
        }
    }

    /// One connection round: healthy spam → pin the waiter-map mutex →
    /// kill the reader path (EOF) → release, letting the task end and
    /// every queued caller race the pre-fix window. Every call must
    /// complete with `Err(CallError::Closed)` inside its bounded window.
    async fn teardown_round(round: u32) {
        let sock = std::env::temp_dir().join(format!("tcm11-{}-{round}.sock", std::process::id()));
        let _ = std::fs::remove_file(&sock);
        let listener = Listener::bind(&sock).expect("mini-gateway binds");
        // Windows bind derives a named pipe from the parent dir; connect
        // must use local_address(), not the .sock path (NotFound otherwise).
        let address = listener.local_address();
        let client = CommandClient::connect(&address).await.expect("connect");
        let accepted = listener.accept().await.expect("accept");
        let server = tokio::spawn(answer_requests(accepted));

        // Concurrent callers: each loops for as long as the round runs.
        let stop = Arc::new(AtomicBool::new(false));
        let mut callers = Vec::with_capacity(CALLERS);
        for _ in 0..CALLERS {
            let stop = Arc::clone(&stop);
            let client = client.clone();
            callers.push(tokio::spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let outcome = tokio::time::timeout(
                        CALL_WINDOW,
                        client.call_raw(Command::ListTasks { session_id: None }),
                    )
                    .await;
                    // Answered before teardown, or Closed after it: both
                    // are completions; only a park (timeout) fails here.
                    if let Err(elapsed) = outcome {
                        panic!(
                            "a call parked forever across teardown — Closed never arrived: {elapsed}"
                        );
                    }
                }
            }));
        }
        tokio::time::sleep(WARMUP).await;

        // Pin the waiter map in a helper thread (no guard held across an
        // await), so the task end and every queued caller line up on the
        // same mutex the fix must make authoritative.
        let pending = Arc::clone(&client.pending);
        let (locked_tx, locked_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _guard = pending.lock().expect("pending map poisoned");
            let _ = locked_tx.send(());
            let _ = release_rx.recv();
        });
        locked_rx.recv().expect("holder pins the waiter map");

        // Kill the reader path: the mini-gateway's stream drops, the
        // client task reads EOF and runs its task-end teardown, which
        // now queues behind the pinned mutex.
        server.abort();
        tokio::time::sleep(TEARDOWN_SETTLE).await;

        // Release: teardown and callers race through the map mutex.
        release_tx.send(()).expect("release the holder");
        // Stop the spam now: callers already inside `call_raw` still
        // race the teardown (that is the bug under test), but no new
        // call may start — and none may busy-spin past the joins (a
        // caller looping on instant `Closed` results never yields,
        // which would hang the runtime's shutdown).
        stop.store(true, Ordering::SeqCst);
        holder.join().expect("holder thread survives");

        for (index, caller) in callers.into_iter().enumerate() {
            match tokio::time::timeout(Duration::from_secs(3), caller).await {
                Err(elapsed) => {
                    panic!("round {round} caller {index} never finished after teardown: {elapsed}")
                }
                Ok(joined) => {
                    joined.unwrap_or_else(|error| panic!("round {round} caller {index}: {error}"));
                }
            }
        }

        drop(client);
        drop(listener);
        let _ = std::fs::remove_file(&sock);
    }

    /// M11 board blocker (`CommandClient` teardown): a `call_raw` that
    /// inserts AND successfully sends between the task end's map clear
    /// and `rx` dropping is never answered — its waiter parks forever.
    /// The fix must make every failure path yield `Err(CallError::Closed)`
    /// under a closed flag checked in the same critical section as the
    /// insert; no test may ever hang (every call carries its own window).
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn teardown_completes_every_concurrent_call_with_closed_inside_a_bounded_window() {
        for round in 0..ROUNDS {
            teardown_round(round).await;
        }
    }

    /// T1 §2.4: a `0xFFFF_FFFF` prefix errors before any 4 GiB
    /// allocation. Same shared cap as the subscription reader.
    #[tokio::test]
    async fn oversize_prefix_errors_without_huge_allocation() {
        let sock = std::env::temp_dir().join(format!(
            "t24-command-{}-{}.sock",
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
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        });
        let stream = tachyon_gateway::transport::connect(&address)
            .await
            .expect("connects");
        let (mut read_half, _) = tokio::io::split(stream);
        let outcome = tokio::time::timeout(
            Duration::from_millis(500),
            super::read_response(&mut read_half),
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
