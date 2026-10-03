//! Newline-delimited JSON-RPC 2.0 codec, generic over `AsyncRead` /
//! `AsyncWrite` (ADR-0005:27 — the client launches the agent with UTF-8
//! newline-delimited JSON-RPC; stdout carries only valid ACP messages).
//!
//! One JSON object per line. Requests carry `jsonrpc: "2.0"`, an `id`
//! (string or number), `method`, and optional `params`. Responses
//! correlate by echoing the id; arrivals route through the pending-call
//! map, so out-of-order answers resolve the right waiter. Notifications
//! (no id) are consumed without reply. Malformed input yields a typed
//! error frame and never kills the loop: stdout keeps carrying only
//! valid JSON-RPC, stderr carries the logs.

use std::collections::HashMap;
use std::io;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::sync::oneshot;

/// The only `jsonrpc` value this codec accepts and emits.
pub const JSONRPC_VERSION: &str = "2.0";

/// JSON-RPC parse error.
pub const PARSE_ERROR: i32 = -32700;
/// JSON-RPC invalid request.
pub const INVALID_REQUEST: i32 = -32600;
/// JSON-RPC method not found.
pub const METHOD_NOT_FOUND: i32 = -32601;
/// JSON-RPC invalid params.
pub const INVALID_PARAMS: i32 = -32602;
/// Implementation-defined server error: the local gateway is
/// unreachable (ADR-0005:35 — one clear actionable error, never a
/// launch of the gateway from the adapter).
pub const GATEWAY_UNAVAILABLE: i32 = -32001;
/// Implementation-defined server error: the gateway answered with a
/// typed refusal (`code` travels in `error.data`).
pub const GATEWAY_REFUSED: i32 = -32002;
/// Implementation-defined server error: the session already has an
/// active turn — ACP v1 turns are sequential, never queued.
pub const TURN_CONFLICT: i32 = -32003;
/// Implementation-defined server error: a turn pipeline failure with no
/// honest verdict (ambiguous task status, timeout, stream resync).
pub const TURN_FAILED: i32 = -32004;

/// Correlation id: JSON string or number (ticket 01 contract).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    /// Numeric id.
    Number(Number),
    /// String id.
    String(String),
}

/// One JSON-RPC error object: stable code, human message, optional
/// machine-readable data.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorObject {
    /// JSON-RPC or implementation-defined error code.
    pub code: i32,
    /// Human-readable message.
    pub message: String,
    /// Optional machine-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// An id-bearing request line, fully validated.
#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    /// Correlation id echoed in the response.
    pub id: RpcId,
    /// Method name.
    pub method: String,
    /// Params as received; `None` when absent or JSON `null`.
    pub params: Option<Value>,
}

/// A notification line (no id): consumed, never answered.
#[derive(Clone, Debug, PartialEq)]
pub struct Notification {
    /// Method name.
    pub method: String,
    /// Params as received; `None` when absent or JSON `null`.
    pub params: Option<Value>,
}

/// A response line received from the peer.
#[derive(Clone, Debug, PartialEq)]
pub struct InboundResponse {
    /// The id the response correlates to.
    pub id: RpcId,
    /// The result or error payload.
    pub payload: Result<Value, ErrorObject>,
}

/// One outbound frame: success, error, or a notification, each encoded
/// as a single line.
#[derive(Clone, Debug, PartialEq)]
pub enum Outbound {
    /// A `result` frame for `id`.
    Success {
        /// Correlation id echoed verbatim.
        id: RpcId,
        /// Result value.
        result: Value,
    },
    /// An `error` frame; `id: None` serializes as `null`.
    Error {
        /// Correlation id, or `None` when it could not be detected.
        id: Option<RpcId>,
        /// The error object.
        error: ErrorObject,
    },
    /// An agent-initiated notification (no id, never answered):
    /// `session/update` frames stream through this variant.
    Notification {
        /// Notification method (`session/update`).
        method: String,
        /// Notification params.
        params: Value,
    },
    /// An agent-initiated REQUEST (id-bearing, the client answers it):
    /// `session/request_permission` goes out through this variant. The
    /// id is minted and its reply slot armed by the turn BEFORE the
    /// frame is queued, and only the serve loop ever writes it — so a
    /// client response can never arrive with nowhere to route.
    Request {
        /// Correlation id the client echoes in its response.
        id: RpcId,
        /// Request method (`session/request_permission`).
        method: String,
        /// Request params.
        params: Value,
    },
}

impl Outbound {
    /// A success frame for `id`.
    #[must_use]
    pub fn success(id: RpcId, result: Value) -> Self {
        Self::Success { id, result }
    }

    /// An error frame with no `data` field.
    #[must_use]
    pub fn error(id: Option<RpcId>, code: i32, message: impl Into<String>) -> Self {
        Self::Error {
            id,
            error: ErrorObject {
                code,
                message: message.into(),
                data: None,
            },
        }
    }

    /// An error frame carrying a machine-readable `data` marker.
    #[must_use]
    pub fn error_with_data(
        id: Option<RpcId>,
        code: i32,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self::Error {
            id,
            error: ErrorObject {
                code,
                message: message.into(),
                data: Some(data),
            },
        }
    }

    /// The standard method-not-found frame (JSON-RPC 2.0 §-32601).
    #[must_use]
    pub fn method_not_found(id: RpcId) -> Self {
        Self::error(Some(id), METHOD_NOT_FOUND, "Method not found")
    }

    /// An id-less notification frame (JSON-RPC 2.0 notification).
    #[must_use]
    pub fn notification(method: impl Into<String>, params: Value) -> Self {
        Self::Notification {
            method: method.into(),
            params,
        }
    }

    /// An id-bearing agent→client request frame (JSON-RPC 2.0 request):
    /// the client answers it with a response echoing `id`.
    #[must_use]
    pub fn request(id: RpcId, method: impl Into<String>, params: Value) -> Self {
        Self::Request {
            id,
            method: method.into(),
            params,
        }
    }

    /// Encodes the frame as one compact JSON line (no trailing newline).
    #[must_use]
    pub fn to_line(&self) -> String {
        #[derive(Serialize)]
        struct Success<'a> {
            jsonrpc: &'a str,
            id: &'a RpcId,
            result: &'a Value,
        }
        #[derive(Serialize)]
        struct ErrorLine<'a> {
            jsonrpc: &'a str,
            id: Option<&'a RpcId>,
            error: &'a ErrorObject,
        }
        #[derive(Serialize)]
        struct NotificationLine<'a> {
            jsonrpc: &'a str,
            method: &'a str,
            params: &'a Value,
        }
        let encoded = match self {
            Self::Success { id, result } => serde_json::to_string(&Success {
                jsonrpc: JSONRPC_VERSION,
                id,
                result,
            }),
            Self::Error { id, error } => serde_json::to_string(&ErrorLine {
                jsonrpc: JSONRPC_VERSION,
                id: id.as_ref(),
                error,
            }),
            Self::Notification { method, params } => serde_json::to_string(&NotificationLine {
                jsonrpc: JSONRPC_VERSION,
                method,
                params,
            }),
            Self::Request { id, method, params } => Ok(request_line(id, method, params)),
        };
        encoded.expect("JSON-RPC response serialization cannot fail")
    }
}

/// The classified outcome of one input line.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    /// An id-bearing request.
    Request(Request),
    /// A notification (no id).
    Notification(Notification),
    /// A response, routed to its pending call when the id matches.
    Response(InboundResponse),
    /// A malformed line, ready to send as an error frame.
    Failure(Outbound),
}

/// Parses one newline-stripped line into a classified message.
///
/// Malformed JSON yields a `-32700` parse failure; valid JSON that is
/// not a well-formed request, notification, or response yields a
/// `-32600` invalid-request failure (with the id echoed when it was
/// detectable, `null` otherwise).
#[must_use]
pub fn parse_line(line: &str) -> Parsed {
    let trimmed = line.trim();
    match serde_json::from_str::<Value>(trimmed) {
        Ok(Value::Object(ref map)) => parse_object(map),
        Ok(_) => invalid_request(None),
        Err(_) => Parsed::Failure(Outbound::error(None, PARSE_ERROR, "Parse error")),
    }
}

fn parse_object(map: &serde_json::Map<String, Value>) -> Parsed {
    let id = match map.get("id") {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => Some(RpcId::Number(number.clone())),
        Some(Value::String(text)) => Some(RpcId::String(text.clone())),
        // The id itself is undetectable, so it must answer as null.
        Some(_) => return invalid_request(None),
    };

    if map.contains_key("method") {
        let method = match map.get("method").and_then(Value::as_str) {
            Some(method) if map.get("jsonrpc").and_then(Value::as_str) == Some(JSONRPC_VERSION) => {
                method.to_owned()
            }
            _ => return invalid_request(id),
        };
        let params = map.get("params").cloned().filter(|value| !value.is_null());
        return match id {
            Some(id) => Parsed::Request(Request { id, method, params }),
            None => Parsed::Notification(Notification { method, params }),
        };
    }

    if map.contains_key("result") || map.contains_key("error") {
        if map.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC_VERSION) {
            return invalid_request(id);
        }
        let Some(id) = id else {
            // A response must correlate; without an id it is junk.
            return invalid_request(None);
        };
        if map.contains_key("result") && map.contains_key("error") {
            return invalid_request(Some(id));
        }
        let payload = if let Some(result) = map.get("result") {
            Ok(result.clone())
        } else {
            match map
                .get("error")
                .map(|value| serde_json::from_value::<ErrorObject>(value.clone()))
            {
                Some(Ok(error)) => Err(error),
                _ => return invalid_request(Some(id)),
            }
        };
        return Parsed::Response(InboundResponse { id, payload });
    }

    invalid_request(id)
}

fn invalid_request(id: Option<RpcId>) -> Parsed {
    Parsed::Failure(Outbound::error(id, INVALID_REQUEST, "Invalid Request"))
}

fn request_line(id: &RpcId, method: &str, params: &Value) -> String {
    #[derive(Serialize)]
    struct RequestLine<'a> {
        jsonrpc: &'a str,
        id: &'a RpcId,
        method: &'a str,
        params: &'a Value,
    }
    serde_json::to_string(&RequestLine {
        jsonrpc: JSONRPC_VERSION,
        id,
        method,
        params,
    })
    .expect("JSON-RPC request serialization cannot fail")
}

/// A bidirectional ND-JSON-RPC peer over separate read/write halves.
///
/// The pending-call map correlates responses by id, so answers that
/// arrive out of order resolve the right waiter.
pub struct Peer<R, W> {
    reader: BufReader<R>,
    writer: W,
    /// Bytes read off the wire but not yet terminated by a newline.
    /// Kept on the peer — never in a future-local buffer — so a
    /// cancelled `read()` (the serve loop's `select!` drops it whenever
    /// the writer channel wins) can never take already-extracted input
    /// bytes with it: partial lines survive across `read()` calls and
    /// no input is ever silently lost or split into a phantom empty
    /// line.
    line_buf: Vec<u8>,
    /// How much of `line_buf` has already been searched for a newline.
    line_searched: usize,
    pending: HashMap<RpcId, oneshot::Sender<Result<Value, ErrorObject>>>,
    next_id: i64,
}

impl<R, W> Peer<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// A peer reading from `reader` and writing to `writer`.
    #[must_use]
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            line_buf: Vec::new(),
            line_searched: 0,
            pending: HashMap::new(),
            next_id: 1,
        }
    }

    /// Reads and classifies the next line. `None` at EOF — including an
    /// unterminated tail at EOF, which is discarded with a log line.
    /// I/O errors surface as `Err` so the caller can end the loop.
    ///
    /// Drop-safe by construction: extracted bytes live in
    /// [`Self::line_buf`] (which outlives any cancelled poll), so a
    /// `select!` that drops this future mid-line loses nothing — the
    /// next `read()` resumes the same accumulation.
    pub async fn read(&mut self) -> Option<io::Result<Parsed>> {
        loop {
            if let Some(offset) = self.line_buf[self.line_searched..]
                .iter()
                .position(|&byte| byte == b'\n')
            {
                let newline = self.line_searched + offset;
                let tail = self.line_buf.split_off(newline + 1);
                let mut line = std::mem::replace(&mut self.line_buf, tail);
                line.pop(); // drop the newline itself
                self.line_searched = 0;
                let text = String::from_utf8_lossy(&line);
                let parsed = parse_line(&text);
                if let Parsed::Response(response) = &parsed
                    && let Some(sender) = self.pending.remove(&response.id)
                {
                    let _ = sender.send(response.payload.clone());
                }
                return Some(Ok(parsed));
            }
            self.line_searched = self.line_buf.len();
            // Top up from the reader: a Ready slice is copied into
            // `line_buf` before the next await point, so cancellation
            // can only ever park empty reads, never lose bytes.
            let filled = match self.reader.fill_buf().await {
                Ok(filled) => filled,
                Err(error) => return Some(Err(error)),
            };
            if filled.is_empty() {
                if self.line_buf.is_empty() {
                    return None; // clean EOF
                }
                tracing::warn!("discarding unterminated input line at EOF");
                return None;
            }
            self.line_buf.extend_from_slice(filled);
            let consumed = filled.len();
            self.reader.consume(consumed);
        }
    }

    /// Writes one line (plus newline) and flushes: every frame reaches
    /// the client before the next one is produced.
    pub async fn write_line(&mut self, line: &str) -> io::Result<()> {
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await
    }

    /// Sends an id-bearing request and registers its pending call. The
    /// receiver resolves when a response with this id is read, however
    /// many other responses arrive first.
    pub async fn send_request(
        &mut self,
        method: &str,
        params: Value,
    ) -> io::Result<(RpcId, oneshot::Receiver<Result<Value, ErrorObject>>)> {
        let id = RpcId::Number(Number::from(self.next_id));
        self.next_id += 1;
        let (sender, receiver) = oneshot::channel();
        self.pending.insert(id.clone(), sender);
        if let Err(error) = self.write_line(&request_line(&id, method, &params)).await {
            self.pending.remove(&id);
            return Err(error);
        }
        Ok((id, receiver))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::{Number, Value, json};
    use tokio::io::AsyncWriteExt as _;

    use super::{INVALID_PARAMS, Outbound, Parsed, Peer, RpcId, parse_line};
    use crate::test_support::{GatewayUp, drive};

    /// Framed requests are answered with responses that echo the exact
    /// id — string and number ids alike.
    #[tokio::test]
    async fn framed_requests_get_correlated_responses_by_id() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","id":7,"method":"initialize","params":{"protocolVersion":1}}"#,
                r#"{"jsonrpc":"2.0","id":"alpha","method":"initialize","params":{"protocolVersion":1}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 2, "one frame per request: {replies:?}");
        let first: Value = serde_json::from_str(&replies[0]).unwrap();
        let second: Value = serde_json::from_str(&replies[1]).unwrap();
        assert_eq!(first["id"], 7);
        assert_eq!(second["id"], "alpha");
        assert_eq!(first["result"]["protocolVersion"], 1);
        assert_eq!(second["result"]["protocolVersion"], 1);
        assert_eq!(first["jsonrpc"], "2.0");
    }

    /// Out-of-order answers route through the pending-call map by id,
    /// not by arrival order.
    #[tokio::test]
    async fn out_of_order_responses_route_by_pending_call_id() {
        let (client, mut remote) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let mut peer = Peer::new(reader, writer);

        let (first_id, mut first_call) = peer.send_request("first", json!({})).await.unwrap();
        let (second_id, mut second_call) = peer.send_request("second", json!({})).await.unwrap();
        assert_ne!(first_id, second_id);

        // The remote answers in REVERSE order.
        let second_line = Outbound::success(second_id.clone(), json!("second-result")).to_line();
        let first_line = Outbound::success(first_id.clone(), json!("first-result")).to_line();
        remote
            .write_all(format!("{second_line}\n{first_line}\n").as_bytes())
            .await
            .unwrap();
        remote.flush().await.unwrap();

        let routed_second = peer.read().await.expect("a line").expect("read ok");
        let routed_first = peer.read().await.expect("a line").expect("read ok");
        assert!(
            matches!(&routed_second, Parsed::Response(response) if response.id == second_id),
            "first arrival is the second call's response: {routed_second:?}"
        );
        assert!(
            matches!(&routed_first, Parsed::Response(response) if response.id == first_id),
            "second arrival is the first call's response: {routed_first:?}"
        );
        // Each waiter got ITS result despite reversed arrival.
        assert_eq!(
            second_call.try_recv().unwrap().unwrap(),
            json!("second-result")
        );
        assert_eq!(
            first_call.try_recv().unwrap().unwrap(),
            json!("first-result")
        );
    }

    /// Regression for the split-write bug class: one JSON-RPC line
    /// delivered in TWO writes (mid-line, newline only in the second)
    /// with the in-flight `read()` CANCELLED between them — exactly
    /// what the serve loop's `select!` does whenever the writer channel
    /// wins mid-line. Bytes the first poll already extracted must live
    /// on the peer (`line_buf`), never in a future-local buffer, or the
    /// second `read()` sees only the tail and the request is lost.
    #[tokio::test]
    async fn split_write_line_survives_a_cancelled_read() {
        let (client, mut remote) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let mut peer = Peer::new(reader, writer);

        let full = r#"{"jsonrpc":"2.0","id":42,"method":"session/new","params":{"cwd":"/abs"}}"#;
        let split = full.len() / 2;
        remote.write_all(&full.as_bytes()[..split]).await.unwrap();
        remote.flush().await.unwrap();

        // The first read takes the head off the wire, finds no
        // newline, and parks — then the timeout DROPS the future
        // mid-line (the select!-cancellation the peer must survive).
        let cancelled = tokio::time::timeout(Duration::from_millis(100), peer.read()).await;
        assert!(
            cancelled.is_err(),
            "the first read must park mid-line, got {cancelled:?}"
        );

        remote.write_all(&full.as_bytes()[split..]).await.unwrap();
        remote.write_all(b"\n").await.unwrap();
        remote.flush().await.unwrap();

        let parsed = peer
            .read()
            .await
            .expect("the line completes")
            .expect("read ok");
        match parsed {
            Parsed::Request(request) => {
                assert_eq!(request.id, RpcId::Number(42.into()), "id correlates");
                assert_eq!(request.method, "session/new");
                assert_eq!(request.params, Some(json!({"cwd": "/abs"})));
            }
            other => panic!("a split-written line must reassemble into ONE request, got {other:?}"),
        }
        // EOF after the one line: no phantom empty line was fabricated
        // from the split writes.
        drop(remote);
        assert!(peer.read().await.is_none(), "EOF after the one line");
    }

    /// A notification (no id) is consumed without any reply frame.
    #[tokio::test]
    async fn notification_without_id_is_skipped_without_reply() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(
            replies.len(),
            1,
            "exactly the request reply, nothing for the notification: {replies:?}"
        );
        let frame: Value = serde_json::from_str(&replies[0]).unwrap();
        assert_eq!(frame["id"], 1);
    }

    /// Malformed lines yield typed error frames — parse error for junk
    /// JSON, invalid request for well-formed JSON that is not a
    /// request — and the loop survives to answer the next request.
    #[tokio::test]
    async fn malformed_lines_yield_typed_errors_and_the_loop_survives() {
        let replies = drive(
            &[
                "this is not json at all",
                "[1, 2, 3]",
                r#"{"jsonrpc":"2.0","id":9,"method":42}"#,
                r#"{"jsonrpc":"2.0","id":5,"method":"initialize","params":{"protocolVersion":1}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 4, "one frame per line: {replies:?}");

        let parse_error: Value = serde_json::from_str(&replies[0]).unwrap();
        assert_eq!(parse_error["error"]["code"], -32700);
        assert_eq!(parse_error["error"]["message"], "Parse error");
        assert!(parse_error["id"].is_null(), "undetectable id is null");

        let not_object: Value = serde_json::from_str(&replies[1]).unwrap();
        assert_eq!(not_object["error"]["code"], -32600);
        assert_eq!(not_object["error"]["message"], "Invalid Request");
        assert!(not_object["id"].is_null());

        let bad_method: Value = serde_json::from_str(&replies[2]).unwrap();
        assert_eq!(bad_method["error"]["code"], -32600);
        assert_eq!(bad_method["id"], 9, "detectable id is echoed");

        let after_garbage: Value = serde_json::from_str(&replies[3]).unwrap();
        assert_eq!(after_garbage["result"]["protocolVersion"], 1);
    }

    /// Unknown methods and not-yet-implemented `session/*` methods all
    /// answer the standard method-not-found error object. `session/new`,
    /// `session/prompt`, `session/cancel`, and `session/load` have real
    /// arms — their rejection paths are covered by their own tests — so
    /// this pins the placeholder contract on the remaining `session/*`
    /// surface (`session/resume` until its own slice) plus a wholly
    /// unknown method.
    #[tokio::test]
    async fn unknown_and_unimplemented_session_methods_yield_standard_method_not_found() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","id":"m1","method":"totally/unknown"}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"session/resume","params":{"sessionId":"s"}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 2, "one frame per request: {replies:?}");
        let expected_ids: [Value; 2] = [json!("m1"), json!(3)];
        for (reply, expected_id) in replies.iter().zip(expected_ids) {
            let frame: Value = serde_json::from_str(reply).unwrap();
            assert_eq!(frame["error"]["code"], -32601, "reply: {reply}");
            assert_eq!(
                frame["error"]["message"], "Method not found",
                "reply: {reply}"
            );
            assert!(
                frame["error"].get("data").is_none(),
                "standard error object carries no data: {reply}"
            );
            assert_eq!(frame["id"], expected_id);
        }
    }

    /// `session/cancel` has a REAL arm (ticket 03): its params are
    /// validated BEFORE the liveness gate (a malformed `sessionId` is
    /// invalid params with zero gateway contact), and a well-formed one
    /// proceeds into the gateway path — here the probe says up but the
    /// connector dials nothing, so the typed gateway-unavailable error
    /// is the proof that validation passed and the cancel pipeline ran.
    #[tokio::test]
    async fn session_cancel_is_a_real_arm_validated_before_the_gate() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","id":1,"method":"session/cancel","params":{"sessionId":"not-a-uuid"}}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"session/cancel"}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"session/cancel","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000"}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 3, "one frame per request: {replies:?}");

        let bad_id: Value = serde_json::from_str(&replies[0]).unwrap();
        assert_eq!(
            bad_id["error"]["code"], INVALID_PARAMS,
            "reply: {}",
            replies[0]
        );
        assert_eq!(bad_id["error"]["data"], json!("invalid_session_id"));

        let missing: Value = serde_json::from_str(&replies[1]).unwrap();
        assert_eq!(missing["error"]["code"], INVALID_PARAMS);
        assert_eq!(missing["error"]["data"], json!("invalid_params"));

        // Valid shape, gateway "up" per the probe, but the connector in
        // this unit test dials nothing: the cancel reached its gateway
        // round trip (no active turn ⇒ GetSession) and failed typed.
        let dialed: Value = serde_json::from_str(&replies[2]).unwrap();
        assert_eq!(
            dialed["error"]["code"],
            crate::codec::GATEWAY_UNAVAILABLE,
            "reply: {}",
            replies[2]
        );
        assert_eq!(dialed["error"]["data"], json!("gateway_unavailable"));
    }

    /// `session/prompt` validates its params BEFORE the liveness gate:
    /// with the gateway up but no connection available (`NoConnector`),
    /// an invalid prompt still answers invalid params — proof the
    /// validation short-circuit never reaches the gateway.
    #[tokio::test]
    async fn invalid_session_prompt_is_refused_without_touching_the_gateway() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","id":1,"method":"session/prompt"}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","prompt":[]}}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"session/new","params":{"cwd":"relative/path"}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 3, "one frame per request: {replies:?}");
        let frames: Vec<Value> = replies
            .iter()
            .map(|reply| serde_json::from_str(reply).unwrap())
            .collect();
        assert_eq!(frames[0]["error"]["code"], INVALID_PARAMS);
        assert_eq!(frames[0]["error"]["data"], json!("invalid_params"));
        assert_eq!(frames[1]["error"]["code"], INVALID_PARAMS);
        assert_eq!(frames[1]["error"]["data"], json!("empty_prompt"));
        assert_eq!(frames[2]["error"]["code"], INVALID_PARAMS);
        assert_eq!(frames[2]["error"]["data"], json!("cwd_not_absolute"));
        assert_eq!(frames[2]["id"], 3);
    }

    /// `session/load` validates its params BEFORE the liveness gate,
    /// exactly like `session/prompt`/`session/new`: with the gateway up
    /// but no connection available (`NoConnector`), every bad shape
    /// still answers invalid params — proof the validation
    /// short-circuit never reaches the gateway (ADR-0005:40: load
    /// creates nothing, so a malformed load must touch nothing).
    #[tokio::test]
    async fn session_load_validates_before_the_gateway() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","id":1,"method":"session/load"}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"session/load","params":{"sessionId":"nope","cwd":"/tmp"}}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","cwd":"relative/dir"}}"#,
                r#"{"jsonrpc":"2.0","id":4,"method":"session/load","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","cwd":"/tmp","mcpServers":[{"transport":{"type":"stdio"},"command":"mcp-server"}]}}"#,
                r#"{"jsonrpc":"2.0","id":5,"method":"session/load","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","cwd":"/tmp"}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 5, "one frame per request: {replies:?}");
        let frames: Vec<Value> = replies
            .iter()
            .map(|reply| serde_json::from_str(reply).unwrap())
            .collect();
        assert_eq!(frames[0]["error"]["data"], json!("invalid_params"));
        assert_eq!(frames[1]["error"]["data"], json!("invalid_session_id"));
        assert_eq!(frames[2]["error"]["data"], json!("cwd_not_absolute"));
        assert_eq!(frames[3]["error"]["data"], json!("mcp_servers_unsupported"));
        // Valid params: validation passes, the NoConnector connect
        // fails — one typed gateway-unavailable error, never a
        // method-not-found.
        assert_eq!(frames[4]["error"]["data"], json!("gateway_unavailable"));
        assert_eq!(frames[4]["id"], 5);
    }

    /// The notification frame (agent → client, no id) serializes as the
    /// three-field JSON-RPC notification line — golden-pinned so the
    /// `session/update` wire shape cannot drift unnoticed.
    #[test]
    fn notification_frames_are_golden_pinned() {
        let update = Outbound::notification(
            "session/update",
            json!({
                "sessionId": "01990f9e-1111-7000-8000-000000000000",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "Hello."},
                },
            }),
        );
        assert_eq!(
            update.to_line(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","update":{"content":{"text":"Hello.","type":"text"},"sessionUpdate":"agent_message_chunk"}}}"#
        );
        // Notifications carry no id, ever.
        assert!(!update.to_line().contains("\"id\""));
    }

    /// The agent→client REQUEST frame (`session/request_permission`)
    /// serializes as the four-field JSON-RPC request line with the id
    /// echoed verbatim — golden-pinned so the permission request's wire
    /// shape (id-bearing, method + params) cannot drift unnoticed, and
    /// proven to classify back into a correlatable request.
    #[test]
    fn outbound_request_frames_are_golden_pinned() {
        let request = Outbound::request(
            RpcId::Number(Number::from(-1)),
            "session/request_permission",
            json!({
                "sessionId": "01990f9e-1111-7000-8000-000000000000",
                "toolCall": {"toolCallId": "approval-1", "title": "Approve it"},
                "options": [
                    {"optionId": "allow_once", "name": "Allow once", "kind": "allow_once"},
                    {"optionId": "reject_once", "name": "Reject once", "kind": "reject_once"},
                ],
            }),
        );
        let line = request.to_line();
        assert_eq!(
            line,
            r#"{"jsonrpc":"2.0","id":-1,"method":"session/request_permission","params":{"options":[{"kind":"allow_once","name":"Allow once","optionId":"allow_once"},{"kind":"reject_once","name":"Reject once","optionId":"reject_once"}],"sessionId":"01990f9e-1111-7000-8000-000000000000","toolCall":{"title":"Approve it","toolCallId":"approval-1"}}}"#
        );
        // The line parses back into a request carrying the same id —
        // the correlation key a client echoes in its response.
        match parse_line(&line) {
            Parsed::Request(parsed) => {
                assert_eq!(parsed.id, RpcId::Number(Number::from(-1)));
                assert_eq!(parsed.method, "session/request_permission");
                assert_eq!(parsed.params, Some(request_params(&request)));
            }
            other => panic!("an outbound request must parse as a request, got {other:?}"),
        }
    }

    /// The params of an [`Outbound::Request`] frame, as echoed back by
    /// the parser (the golden test's round-trip assertion).
    fn request_params(outbound: &Outbound) -> Value {
        let Outbound::Request { params, .. } = outbound else {
            panic!("expected an Outbound::Request");
        };
        params.clone()
    }

    /// Pure parse-classification seams used by the loop.
    #[test]
    fn parse_line_classifies_requests_responses_and_junk() {
        match parse_line(r#"{"jsonrpc":"2.0","id":1,"method":"x"}"#) {
            Parsed::Request(request) => {
                assert_eq!(request.id, RpcId::Number(1.into()));
                assert_eq!(request.method, "x");
                assert_eq!(request.params, None);
            }
            other => panic!("expected a request, got {other:?}"),
        }
        assert!(matches!(
            parse_line(r#"{"jsonrpc":"2.0","method":"x"}"#),
            Parsed::Notification(_)
        ));
        // `"id": null` behaves as no id (a notification).
        assert!(matches!(
            parse_line(r#"{"jsonrpc":"2.0","id":null,"method":"x"}"#),
            Parsed::Notification(_)
        ));
        assert!(matches!(
            parse_line(r#"{"jsonrpc":"2.0","id":1,"result":2}"#),
            Parsed::Response(_)
        ));
        assert!(matches!(
            parse_line("}{"),
            Parsed::Failure(outbound) if outbound.to_line().contains("-32700")
        ));
        // `jsonrpc` version is enforced.
        assert!(matches!(
            parse_line(r#"{"jsonrpc":"1.0","id":1,"method":"x"}"#),
            Parsed::Failure(outbound) if outbound.to_line().contains("-32600")
        ));
    }
}
