//! In-crate gateway client: endpoint discovery plus one framed `Ping`
//! round trip.
//!
//! Mirrors the private CLI loop in `crates/tachyon-app/src/client.rs`
//! (endpoint file → `tachyon_gateway::transport::connect` →
//! `RequestEnvelope`/`ResponseEnvelope` via `encode_frame`/`decode_frame`)
//! without depending on `tachyon-tui` and without extracting a shared
//! client crate (spec implementation decision Q7).

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tachyon_gateway::read_endpoint_info;
use tachyon_gateway::transport::{Stream, connect};
use tachyon_protocol::CommandResult;
use tachyon_protocol::{
    Command, FRAME_PREFIX_LEN, MAX_FRAME_BYTES, PROTOCOL_VERSION, RequestEnvelope,
    ResponseEnvelope, ServerFrame, check_version, decode_frame, decode_server_frame, encode_frame,
};
use tachyon_types::EventId;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Upper bound on one liveness probe (endpoint read + local connect +
/// `Ping` round trip), so a wedged gateway surfaces as an actionable
/// error instead of a hang.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The gateway is unreachable. `Display` is the single actionable
/// message an ACP client sees on every request (ADR-0005:35: the agent
/// never starts or restarts the gateway itself).
#[derive(Debug, Error)]
#[error("Tachyon gateway unavailable: {detail}. Start it with `tachyon gateway` and retry.")]
pub struct GatewayUnavailable {
    /// What failed: missing endpoint file, refused connect, ping
    /// failure, or timeout — with the exact path involved.
    pub detail: String,
}

impl GatewayUnavailable {
    /// An unavailability failure with a human-actionable detail.
    #[must_use]
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

/// Liveness probe seam. The server loop gates every id-bearing request
/// on it, so a downed gateway becomes one typed JSON-RPC error for any
/// request instead of a hang, a silent close, or a launch attempt.
///
/// Desugared (not `async fn`) so the future's `Send` bound is part of
/// the contract the loop relies on.
pub trait GatewayProbe {
    /// `Ok(())` once the local gateway answers a `Ping`.
    fn probe(&self) -> impl Future<Output = Result<(), GatewayUnavailable>> + Send;
}

/// Production probe: reads `data_dir/gateway.json`, connects to the
/// recorded socket, and round-trips one `Ping`.
#[derive(Clone, Debug)]
pub struct EndpointProbe {
    data_dir: PathBuf,
}

impl EndpointProbe {
    /// A probe rooted at `data_dir` (the same data dir the CLI gateway
    /// writes its endpoint file into).
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    /// One full discovery + connect + `Ping` round trip, bounded by
    /// [`PROBE_TIMEOUT`]. Never spawns anything: it only reads the
    /// endpoint file and connects to the socket that is already there.
    pub async fn probe_gateway(&self) -> Result<(), GatewayUnavailable> {
        match tokio::time::timeout(PROBE_TIMEOUT, ping(&self.data_dir)).await {
            Ok(result) => result,
            Err(_) => Err(GatewayUnavailable::new(format!(
                "timed out after {}s probing {}",
                PROBE_TIMEOUT.as_secs(),
                self.data_dir.display()
            ))),
        }
    }
}

impl GatewayProbe for EndpointProbe {
    fn probe(&self) -> impl Future<Output = Result<(), GatewayUnavailable>> + Send {
        self.probe_gateway()
    }
}

/// Endpoint discovery plus one framed `Ping`, mirroring the CLI loop.
async fn ping(data_dir: &Path) -> Result<(), GatewayUnavailable> {
    let endpoint_file = data_dir.join("gateway.json");
    let info = read_endpoint_info(&endpoint_file).map_err(|error| {
        GatewayUnavailable::new(format!(
            "no usable endpoint at {}: {error}",
            endpoint_file.display()
        ))
    })?;
    let mut stream = connect(&info.socket_path).await.map_err(|error| {
        GatewayUnavailable::new(format!(
            "cannot connect to {}: {error}",
            info.socket_path.display()
        ))
    })?;
    let request = RequestEnvelope {
        protocol_version: PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command: Command::Ping,
    };
    let bytes = encode_frame(&request)
        .map_err(|error| GatewayUnavailable::new(format!("encoding ping: {error}")))?;
    stream
        .write_all(&bytes)
        .await
        .map_err(|error| GatewayUnavailable::new(format!("sending ping: {error}")))?;
    let mut prefix = [0_u8; FRAME_PREFIX_LEN];
    stream.read_exact(&mut prefix).await.map_err(|error| {
        GatewayUnavailable::new(format!(
            "reading ping response from {}: {error}",
            info.socket_path.display()
        ))
    })?;
    let len = u32::from_le_bytes(prefix) as usize;
    if len > MAX_FRAME_BYTES - FRAME_PREFIX_LEN {
        return Err(GatewayUnavailable::new(
            "gateway response exceeds frame limit",
        ));
    }
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.map_err(|error| {
        GatewayUnavailable::new(format!(
            "reading ping response from {}: {error}",
            info.socket_path.display()
        ))
    })?;
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    let (response, _): (ResponseEnvelope, usize) = decode_frame(&framed)
        .map_err(|error| GatewayUnavailable::new(format!("decoding ping response: {error}")))?;
    check_version(response.protocol_version)
        .map_err(|error| GatewayUnavailable::new(format!("gateway protocol version: {error}")))?;
    match response.result {
        CommandResult::Ok { .. } => Ok(()),
        CommandResult::Err { code, message } => Err(GatewayUnavailable::new(format!(
            "ping refused: {code}: {message}"
        ))),
    }
}

/// Endpoint discovery for one adapter request: the same
/// `data_dir/gateway.json` read the probe performs, factored so the
/// probe and the command connection can never disagree on where the
/// gateway lives.
fn discover(data_dir: &Path) -> Result<tachyon_gateway::EndpointInfo, GatewayUnavailable> {
    let endpoint_file = data_dir.join("gateway.json");
    read_endpoint_info(&endpoint_file).map_err(|error| {
        GatewayUnavailable::new(format!(
            "no usable endpoint at {}: {error}",
            endpoint_file.display()
        ))
    })
}

/// Opens fresh framed connections to the local gateway. One prompt turn
/// runs its whole pipeline over ONE connection (`GatewayConn`); there is
/// no cross-request connection cache in this slice — the probe design
/// (one `Ping` per request) is deliberately untouched. `Sync` because a
/// spawned turn borrows the connector across its awaits.
pub trait Connector: Clone + Send + Sync + 'static {
    /// One discovery + transport connect, bounded by the caller's
    /// liveness gate. Never spawns anything.
    fn connect(&self) -> impl Future<Output = Result<GatewayConn, GatewayUnavailable>> + Send;
}

/// Production connector rooted at the adapter's data directory.
#[derive(Clone, Debug)]
pub struct EndpointConnector {
    data_dir: PathBuf,
}

impl EndpointConnector {
    /// A connector reading the endpoint file under `data_dir`.
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }
}

impl Connector for EndpointConnector {
    fn connect(&self) -> impl Future<Output = Result<GatewayConn, GatewayUnavailable>> + Send {
        let data_dir = self.data_dir.clone();
        async move {
            let info = discover(&data_dir)?;
            let stream = connect(&info.socket_path).await.map_err(|error| {
                GatewayUnavailable::new(format!(
                    "cannot connect to {}: {error}",
                    info.socket_path.display()
                ))
            })?;
            Ok(GatewayConn::new(stream))
        }
    }
}

/// One framed gateway round-trip failure: the gateway was unreachable
/// (`Transport`) or answered a command with a typed refusal (`Refused`,
/// whose `code` becomes the JSON-RPC error `data` marker).
#[derive(Debug, Error)]
pub enum GatewayCallError {
    /// Discovery, connect, write, read, or frame-decode failure.
    #[error(transparent)]
    Transport(#[from] GatewayUnavailable),
    /// The gateway validated and refused the command itself.
    #[error("gateway refused {code}: {message}")]
    Refused {
        /// Stable machine-readable gateway code (`workspace_not_found`, …).
        code: String,
        /// Gateway's human-readable detail.
        message: String,
    },
}

/// One open gateway connection: sequential request/response round trips
/// over the length-prefixed frame protocol, plus subscription streaming.
///
/// Requests are strictly sequential on one connection (the ACP adapter
/// is the only client of its own sockets), so responses are matched by
/// `request_id` as they arrive; subscription event frames may interleave
/// with responses once `Command::Subscribe` has been acked.
pub struct GatewayConn {
    stream: Stream,
}

impl GatewayConn {
    fn new(stream: Stream) -> Self {
        Self { stream }
    }

    /// Sends one command and awaits its response payload. Transport
    /// failures surface as [`GatewayCallError::Transport`]; a typed
    /// gateway refusal as [`GatewayCallError::Refused`].
    pub async fn call(&mut self, command: Command) -> Result<Value, GatewayCallError> {
        let request_id = self.send(command).await?;
        loop {
            match self.read_frame().await? {
                ServerFrame::Response(response) if response.request_id == request_id => {
                    check_version(response.protocol_version).map_err(|error| {
                        GatewayUnavailable::new(format!("gateway protocol version: {error}"))
                    })?;
                    return match response.result {
                        CommandResult::Ok { payload } => Ok(payload),
                        CommandResult::Err { code, message } => {
                            Err(GatewayCallError::Refused { code, message })
                        }
                    };
                }
                ServerFrame::Response(_) => {
                    tracing::debug!("ignoring a response for another request id");
                }
                ServerFrame::Event(_) => {
                    // Events only flow after a `Subscribe` ack; between
                    // them a `call` never has a live subscription.
                    tracing::debug!("ignoring an event frame received outside a subscription");
                }
            }
        }
    }

    /// Writes one command without waiting for its answer; returns the
    /// `request_id` the response will correlate on. Used where events
    /// and responses multiplex on one connection (`Subscribe`, the
    /// in-stream `GetTask`).
    pub async fn send(&mut self, command: Command) -> Result<EventId, GatewayUnavailable> {
        let request_id = EventId::generate();
        let request = RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            command,
        };
        let bytes = encode_frame(&request)
            .map_err(|error| GatewayUnavailable::new(format!("encoding request: {error}")))?;
        self.stream
            .write_all(&bytes)
            .await
            .map_err(|error| GatewayUnavailable::new(format!("sending request: {error}")))?;
        Ok(request_id)
    }

    /// Reads the next frame from the connection.
    pub async fn read_frame(&mut self) -> Result<ServerFrame, GatewayUnavailable> {
        let mut prefix = [0_u8; FRAME_PREFIX_LEN];
        self.stream.read_exact(&mut prefix).await.map_err(|error| {
            GatewayUnavailable::new(format!("reading gateway frame prefix: {error}"))
        })?;
        let len = u32::from_le_bytes(prefix) as usize;
        if len > MAX_FRAME_BYTES - FRAME_PREFIX_LEN {
            return Err(GatewayUnavailable::new(
                "gateway response exceeds frame limit",
            ));
        }
        let mut payload = vec![0_u8; len];
        self.stream
            .read_exact(&mut payload)
            .await
            .map_err(|error| {
                GatewayUnavailable::new(format!("reading gateway frame body: {error}"))
            })?;
        let mut framed = prefix.to_vec();
        framed.extend_from_slice(&payload);
        let (frame, _): (ServerFrame, usize) = decode_server_frame(&framed)
            .map_err(|error| GatewayUnavailable::new(format!("decoding gateway frame: {error}")))?;
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{EndpointProbe, GatewayProbe as _};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn empty_dir() -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("tachyon-acp-probe-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A data dir with no `gateway.json` fails with one actionable
    /// message that names the exact endpoint file the operator must
    /// produce — and the probe only ever reads, never launches.
    #[tokio::test]
    async fn missing_endpoint_is_an_actionable_failure() {
        let dir = empty_dir();
        let error = EndpointProbe::new(&dir).probe().await.unwrap_err();
        let message = error.to_string();
        assert!(
            message.starts_with("Tachyon gateway unavailable: no usable endpoint at "),
            "unhelpful message: {message}"
        );
        assert!(
            message.contains(&dir.join("gateway.json").display().to_string()),
            "message must name the endpoint file: {message}"
        );
        assert!(
            message.ends_with("Start it with `tachyon gateway` and retry."),
            "message must say how to fix it: {message}"
        );
        assert!(
            !dir.join("gateway.json").exists(),
            "the probe must never create the endpoint file"
        );
    }
}
