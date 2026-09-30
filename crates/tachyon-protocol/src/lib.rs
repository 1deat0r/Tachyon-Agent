//! Tachyon Protocol.
//!
//! Versioned gateway commands/events, serialization envelopes, and the
//! length-prefixed JSON framing used on local IPC (spec §16, §36).
//! This crate is transport-neutral: it defines bytes on the wire, not sockets.

#![warn(unsafe_code)]

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use tachyon_types::{ApprovalId, ArtifactId, EventId, SessionId, TaskId, Timestamp};

/// Wire protocol version. Bump on any breaking envelope change.
///
/// v2 introduced the tagged [`ServerFrame`] wrapper, the
/// `GatewayEvent::Journal` passthrough, the subscription acknowledgement
/// payload, and the task-scoped `Approve`/`Deny` fields (plan D1).
pub const PROTOCOL_VERSION: u16 = 2;

/// Maximum frame size including the 4-byte length prefix (64 MiB).
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Length prefix size in bytes (little-endian `u32`).
pub const FRAME_PREFIX_LEN: usize = 4;

/// Errors produced while framing or validating protocol messages.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Frame declares more bytes than the protocol allows.
    #[error("frame size {size} exceeds maximum {MAX_FRAME_BYTES}")]
    FrameTooLarge {
        /// Declared or actual offending size in bytes.
        size: usize,
    },
    /// Buffer ends before the declared frame is complete.
    #[error("truncated frame: need {need} bytes, have {have}")]
    Truncated {
        /// Bytes required for the full frame.
        need: usize,
        /// Bytes available.
        have: usize,
    },
    /// Payload is not valid JSON for the target type.
    #[error("invalid JSON payload: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// Peer speaks an incompatible protocol version.
    #[error("unsupported protocol version {got}, expected {PROTOCOL_VERSION}")]
    UnsupportedVersion {
        /// Version the peer sent.
        got: u16,
    },
}

impl PartialEq for ProtocolError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::FrameTooLarge { size: first }, Self::FrameTooLarge { size: second }) => {
                first == second
            }
            (
                Self::Truncated { need, have },
                Self::Truncated {
                    need: other_need,
                    have: other_have,
                },
            ) => need == other_need && have == other_have,
            (Self::InvalidJson(_), Self::InvalidJson(_)) => true,
            (Self::UnsupportedVersion { got: first }, Self::UnsupportedVersion { got: second }) => {
                first == second
            }
            _ => false,
        }
    }
}

/// A client-to-gateway request: version, correlation id, and one [`Command`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestEnvelope {
    /// Must equal [`PROTOCOL_VERSION`]; checked with [`check_version`].
    pub protocol_version: u16,
    /// Correlates retries and responses with this request.
    pub request_id: EventId,
    /// The requested operation.
    pub command: Command,
}

/// Gateway-to-client durable event (spec §16).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Per-task sequence cursor for reconnect/replay.
    pub seq: i64,
    /// Unique id of this event.
    pub event_id: EventId,
    /// Envelope schema version; currently always [`PROTOCOL_VERSION`].
    pub schema_version: u16,
    /// Task this event belongs to.
    pub task_id: TaskId,
    /// When the event was journalled.
    pub timestamp: Timestamp,
    /// The event payload.
    pub event: GatewayEvent,
}

/// Commands a gateway client may send. Every command is validated and
/// policy-checked by the core before execution; nothing here self-authorizes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Liveness probe; answered without touching task state.
    Ping,
    /// Gateway build, protocol version, and active task counts.
    GetStatus,
    /// Open a new persistent interaction context.
    CreateSession,
    /// Open a new executable task inside a session.
    CreateTask {
        /// Session that will own the task.
        session_id: SessionId,
        /// User's objective in plain text.
        objective: String,
    },
    /// List tasks, optionally restricted to one session.
    ListTasks {
        /// When set, only tasks of this session are returned.
        session_id: Option<SessionId>,
    },
    /// Fetch one task's canonical state snapshot.
    GetTask {
        /// Task to fetch.
        task_id: TaskId,
    },
    /// Steering message: new information or constraint for a live task.
    SendMessage {
        /// Task to steer.
        task_id: TaskId,
        /// User's message.
        message: String,
    },
    /// Pause execution; running nodes are cancelled per policy.
    PauseTask {
        /// Task to pause.
        task_id: TaskId,
    },
    /// Resume a paused task.
    ResumeTask {
        /// Task to resume.
        task_id: TaskId,
    },
    /// Cancel a task; no further nodes will dispatch.
    CancelTask {
        /// Task to cancel.
        task_id: TaskId,
    },
    /// Approve a pending policy-gated operation for a task (plan item 8;
    /// the task scope is new in protocol v2).
    Approve {
        /// Task whose parked operation is being granted.
        task_id: TaskId,
        /// Approval request being granted.
        approval_id: ApprovalId,
    },
    /// Deny a pending policy-gated operation for a task (plan item 8).
    Deny {
        /// Task whose parked operation is being refused.
        task_id: TaskId,
        /// Approval request being refused.
        approval_id: ApprovalId,
        /// Human-readable reason recorded in the journal.
        reason: String,
    },
    /// Start a run on an existing task through the shared driver
    /// (plan item 6): the gateway validates and pins `workspace_root`
    /// into durable task state before any policy or lease boundary, then
    /// spawns the ONE supervisor-hosted run path.
    StartRun {
        /// Task to drive (created before this command).
        task_id: TaskId,
        /// Workspace root as given by the operator; the gateway rejects
        /// roots that do not exist or do not canonicalize, then pins the
        /// canonical path durably.
        workspace_root: String,
        /// Explicit acceptance-contract JSON file. `None` asks for the
        /// detected default (Cargo projects) and fails closed elsewhere.
        acceptance: Option<String>,
    },
    /// Subscribe to a task's event stream after `after_seq`.
    Subscribe {
        /// Task to observe.
        task_id: TaskId,
        /// Replay durable events strictly after this sequence.
        after_seq: i64,
    },
    /// Fetch a content-addressed artifact from the spool.
    GetArtifact {
        /// Artifact to fetch.
        artifact_id: ArtifactId,
    },
    /// Answer a repository question deterministically: route it, index
    /// `workspace_root` fresh, and return source locations. Deliberately
    /// stateless — no task, no journal, and no model provider is ever
    /// consulted, so a route that would need one is refused rather than
    /// silently degraded.
    Query {
        /// Workspace to search, as given by the operator; the gateway
        /// rejects roots that do not exist or do not canonicalize.
        workspace_root: String,
        /// The user's question in plain text.
        question: String,
    },
}

/// Durable gateway-to-client events. Ephemeral progress (streaming tokens,
/// spinners) travels out-of-band and may be dropped; these may not.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayEvent {
    /// Canonical task state changed; clients refresh from the snapshot.
    TaskSnapshot {
        /// Task that changed.
        task_id: TaskId,
        /// New status name (the `TaskStatus` enum lives in `tachyon-core`).
        status: String,
        /// State revision after this change.
        revision: u64,
    },
    /// Human-readable progress note, safe to drop on slow clients.
    Progress {
        /// Task producing progress.
        task_id: TaskId,
        /// Progress text.
        message: String,
    },
    /// Client is too far behind; it must resubscribe from `after_seq`.
    ResyncRequired {
        /// Affected task.
        task_id: TaskId,
        /// Sequence to resubscribe from.
        after_seq: i64,
    },
    /// One journalled transition, passed through opaquely (plan D2).
    ///
    /// `kind` is the journal transition kind and `payload` is the raw
    /// `StateEvent` document as committed, so future kinds are additive and
    /// need no protocol bump; clients match on `kind` and render unknown
    /// kinds with a placeholder instead of failing.
    Journal {
        /// Journal transition kind (`created`, `message`, `stage`, …).
        kind: String,
        /// Raw `StateEvent` JSON exactly as journalled.
        payload: serde_json::Value,
    },
    /// Request failed; carries the failing task when applicable.
    Error {
        /// Task related to the failure, if any.
        task_id: Option<TaskId>,
        /// Machine-readable failure summary.
        message: String,
    },
}

/// Gateway-to-client command result. Success payloads are plain JSON so
/// new commands do not force protocol version bumps; failures carry a
/// stable machine-readable code plus a human message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    /// Must equal [`PROTOCOL_VERSION`].
    pub protocol_version: u16,
    /// Echoes the request being answered.
    pub request_id: EventId,
    /// The outcome.
    pub result: CommandResult,
}

/// Outcome of one gateway command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResult {
    /// Command succeeded; payload shape depends on the command.
    Ok {
        /// Result payload.
        payload: serde_json::Value,
    },
    /// Command failed; nothing it proposed was executed.
    Err {
        /// Stable machine-readable code (`unknown_task`, `illegal_transition`, …).
        code: String,
        /// Human-readable message.
        message: String,
    },
}

/// The tagged gateway-to-client frame (protocol v2, plan D1).
///
/// Every byte the gateway writes towards a client is one of these, and the
/// `frame` discriminator is explicit on the wire — a decoder never has to
/// guess whether it is holding a response or a pushed event.
///
/// The tag is inlined into the wrapped envelope's own fields, so a
/// [`ResponseEnvelope`] decoded straight out of a `response` frame still
/// round-trips (unknown `frame` field ignored); an `event` frame does not
/// decode as a response, and a frame with no `frame` tag fails to decode as
/// a [`ServerFrame`] at all.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "frame")]
pub enum ServerFrame {
    /// Answer to one request on this connection.
    #[serde(rename = "response")]
    Response(ResponseEnvelope),
    /// Subscription event pushed with no matching request.
    #[serde(rename = "event")]
    Event(EventEnvelope),
}

/// Encodes `frame` as JSON prefixed with its little-endian `u32` length.
///
/// Round-trips with [`decode_server_frame`].
pub fn encode_server_frame(frame: &ServerFrame) -> Result<Vec<u8>, ProtocolError> {
    encode_frame(frame)
}

/// Decodes one [`ServerFrame`] from the head of `buf`.
///
/// Returns the frame and the total bytes consumed, so callers can advance
/// and decode the next one. Round-trips with [`encode_server_frame`].
pub fn decode_server_frame(buf: &[u8]) -> Result<(ServerFrame, usize), ProtocolError> {
    decode_frame(buf)
}

/// Rejects any peer that does not speak [`PROTOCOL_VERSION`].
pub fn check_version(got: u16) -> Result<(), ProtocolError> {
    if got == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion { got })
    }
}

/// Serializes `value` as JSON prefixed with its little-endian `u32` length.
pub fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let mut json = serde_json::to_vec(value)?;
    if json.len() + FRAME_PREFIX_LEN > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge {
            size: json.len() + FRAME_PREFIX_LEN,
        });
    }
    let len = u32::try_from(json.len()).map_err(|_| ProtocolError::FrameTooLarge {
        size: json.len() + FRAME_PREFIX_LEN,
    })?;
    let mut out = Vec::with_capacity(FRAME_PREFIX_LEN + json.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.append(&mut json);
    Ok(out)
}

/// Decodes one frame from the head of `buf`.
///
/// Returns the value and the total bytes consumed, so callers can advance
/// past the frame and decode the next one.
pub fn decode_frame<T: DeserializeOwned>(buf: &[u8]) -> Result<(T, usize), ProtocolError> {
    if buf.len() < FRAME_PREFIX_LEN {
        return Err(ProtocolError::Truncated {
            need: FRAME_PREFIX_LEN,
            have: buf.len(),
        });
    }
    let mut prefix = [0_u8; FRAME_PREFIX_LEN];
    prefix.copy_from_slice(&buf[..FRAME_PREFIX_LEN]);
    let len = usize::try_from(u32::from_le_bytes(prefix)).unwrap_or(usize::MAX);
    if len > MAX_FRAME_BYTES - FRAME_PREFIX_LEN {
        return Err(ProtocolError::FrameTooLarge {
            size: len + FRAME_PREFIX_LEN,
        });
    }
    if buf.len() < FRAME_PREFIX_LEN + len {
        return Err(ProtocolError::Truncated {
            need: FRAME_PREFIX_LEN + len,
            have: buf.len(),
        });
    }
    let value = serde_json::from_slice(&buf[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + len])?;
    Ok((value, FRAME_PREFIX_LEN + len))
}

#[cfg(test)]
mod tests {
    use super::{Command, EventEnvelope, GatewayEvent, RequestEnvelope, check_version};
    use super::{CommandResult, FRAME_PREFIX_LEN, ResponseEnvelope, ServerFrame};
    use super::{PROTOCOL_VERSION, ProtocolError};
    use super::{decode_frame, decode_server_frame, encode_frame, encode_server_frame};
    use tachyon_types::{EventId, SessionId, TaskId, Timestamp};

    fn request() -> RequestEnvelope {
        RequestEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            command: Command::CreateTask {
                session_id: SessionId::generate(),
                objective: "Where is refreshToken defined and used?".to_owned(),
            },
        }
    }

    fn event() -> EventEnvelope {
        EventEnvelope {
            seq: 7,
            event_id: EventId::generate(),
            schema_version: PROTOCOL_VERSION,
            task_id: TaskId::generate(),
            timestamp: Timestamp::from_micros(1_000_000_000_123_456),
            event: GatewayEvent::TaskSnapshot {
                task_id: TaskId::generate(),
                status: "Executing".to_owned(),
                revision: 3,
            },
        }
    }

    #[test]
    fn envelopes_round_trip_through_frames() {
        let req = request();
        let bytes = encode_frame(&req).unwrap();
        let (back, used): (RequestEnvelope, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, req);

        let ev = event();
        let bytes = encode_frame(&ev).unwrap();
        let (back, used): (EventEnvelope, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, ev);
    }

    #[test]
    fn decoder_stops_at_frame_boundary() {
        let first = encode_frame(&request()).unwrap();
        let second = encode_frame(&event()).unwrap();
        let mut combined = first.clone();
        combined.extend_from_slice(&second);
        let (_, used): (RequestEnvelope, usize) = decode_frame(&combined).unwrap();
        assert_eq!(used, first.len());
        let (_, used): (EventEnvelope, usize) = decode_frame(&combined[used..]).unwrap();
        assert_eq!(used, second.len());
    }

    #[test]
    fn server_frame_round_trips_through_an_explicit_frame_tag() {
        let response = ResponseEnvelope {
            protocol_version: PROTOCOL_VERSION,
            request_id: EventId::generate(),
            result: CommandResult::Ok {
                payload: serde_json::json!({"pong": true}),
            },
        };
        let bytes = encode_server_frame(&ServerFrame::Response(response.clone())).unwrap();
        let head: serde_json::Value = serde_json::from_slice(&bytes[FRAME_PREFIX_LEN..]).unwrap();
        assert_eq!(head["frame"], "response");
        let (back, used) = decode_server_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, ServerFrame::Response(response));

        let pushed = event();
        let bytes = encode_server_frame(&ServerFrame::Event(pushed.clone())).unwrap();
        let head: serde_json::Value = serde_json::from_slice(&bytes[FRAME_PREFIX_LEN..]).unwrap();
        assert_eq!(head["frame"], "event");
        let (back, used) = decode_server_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, ServerFrame::Event(pushed));
    }

    #[test]
    fn decoder_rejects_truncated_and_foreign_frames() {
        let bytes = encode_frame(&request()).unwrap();
        let err = decode_frame::<RequestEnvelope>(&bytes[..3]).unwrap_err();
        assert_eq!(err, ProtocolError::Truncated { need: 4, have: 3 });
        let mut cut = bytes.clone();
        cut.truncate(bytes.len() - 1);
        let err = decode_frame::<RequestEnvelope>(&cut).unwrap_err();
        assert_eq!(
            err,
            ProtocolError::Truncated {
                need: bytes.len(),
                have: bytes.len() - 1
            }
        );
        let mut oversized = u32::MAX.to_le_bytes().to_vec();
        oversized.extend_from_slice(&[0_u8; 8]);
        assert!(matches!(
            decode_frame::<RequestEnvelope>(&oversized).unwrap_err(),
            ProtocolError::FrameTooLarge { .. }
        ));
        let mut bad_json = 8_u32.to_le_bytes().to_vec();
        bad_json.extend_from_slice(b"not json");
        assert!(matches!(
            decode_frame::<RequestEnvelope>(&bad_json).unwrap_err(),
            ProtocolError::InvalidJson(_)
        ));
    }

    #[test]
    fn approve_and_deny_are_task_scoped() {
        let task_id = TaskId::generate();
        let approval_id = tachyon_types::ApprovalId::generate();
        let approve = Command::Approve {
            task_id,
            approval_id,
        };
        let bytes = encode_frame(&approve).unwrap();
        let (back, used): (Command, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, approve);

        let deny = Command::Deny {
            task_id,
            approval_id,
            reason: "not this time".to_owned(),
        };
        let bytes = encode_frame(&deny).unwrap();
        let (back, used): (Command, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, deny);

        let json: serde_json::Value = serde_json::from_slice(&bytes[FRAME_PREFIX_LEN..]).unwrap();
        assert_eq!(json["Deny"]["task_id"], task_id.to_string());
    }

    #[test]
    fn start_run_round_trips_and_version_stays_two() {
        let task_id = TaskId::generate();
        let bare = Command::StartRun {
            task_id,
            workspace_root: "/srv/scratch/ws".to_owned(),
            acceptance: None,
        };
        let bytes = encode_frame(&bare).unwrap();
        let (back, used): (Command, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, bare);

        let with_acceptance = Command::StartRun {
            task_id,
            workspace_root: "/srv/scratch/ws".to_owned(),
            acceptance: Some("/srv/scratch/acceptance.json".to_owned()),
        };
        let bytes = encode_frame(&with_acceptance).unwrap();
        let (back, used): (Command, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, with_acceptance);
        let json: serde_json::Value = serde_json::from_slice(&bytes[FRAME_PREFIX_LEN..]).unwrap();
        assert_eq!(json["StartRun"]["task_id"], task_id.to_string());
        assert_eq!(json["StartRun"]["workspace_root"], "/srv/scratch/ws");
        assert_eq!(
            json["StartRun"]["acceptance"],
            "/srv/scratch/acceptance.json"
        );

        // StartRun is additive inside protocol v2: no version bump.
        assert_eq!(super::PROTOCOL_VERSION, 2);
        assert_eq!(check_version(super::PROTOCOL_VERSION), Ok(()));
    }

    #[test]
    fn query_round_trips_and_is_additive_inside_protocol_v2() {
        let query = Command::Query {
            workspace_root: "/srv/scratch/ws".to_owned(),
            question: "Where is complete_refresh defined and used?".to_owned(),
        };
        let bytes = encode_frame(&query).unwrap();
        let (back, used): (Command, usize) = decode_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(back, query);

        let json: serde_json::Value = serde_json::from_slice(&bytes[FRAME_PREFIX_LEN..]).unwrap();
        assert_eq!(json["Query"]["workspace_root"], "/srv/scratch/ws");
        assert_eq!(
            json["Query"]["question"],
            "Where is complete_refresh defined and used?"
        );

        // Query is additive inside protocol v2, like StartRun: a new
        // stateless command is not a breaking change.
        assert_eq!(super::PROTOCOL_VERSION, 2);
    }

    #[test]
    fn journal_variant_passes_kind_and_payload_through_unchanged() {
        let envelope = EventEnvelope {
            seq: 12,
            event_id: EventId::generate(),
            schema_version: 1,
            task_id: TaskId::generate(),
            timestamp: Timestamp::from_micros(42),
            event: GatewayEvent::Journal {
                kind: "stage".to_owned(),
                payload: serde_json::json!({"from": "evidence", "to": "model"}),
            },
        };
        let bytes = encode_server_frame(&ServerFrame::Event(envelope.clone())).unwrap();
        let (back, used) = decode_server_frame(&bytes).unwrap();
        assert_eq!(used, bytes.len());
        let ServerFrame::Event(back) = back else {
            panic!("expected an event frame, got {back:?}");
        };
        assert_eq!(back, envelope);
        match &back.event {
            GatewayEvent::Journal { kind, payload } => {
                assert_eq!(kind, "stage");
                assert_eq!(payload["from"], "evidence");
                assert_eq!(payload["to"], "model");
            }
            other => panic!("expected opaque journal passthrough, got {other:?}"),
        }
    }

    #[test]
    fn legacy_v1_peers_are_rejected_by_the_version_gate() {
        assert_eq!(
            check_version(1),
            Err(ProtocolError::UnsupportedVersion { got: 1 }),
            "protocol v1 must be skew now that v2 is on the wire"
        );
    }

    #[test]
    fn version_gate_accepts_current_and_rejects_other() {
        assert_eq!(check_version(PROTOCOL_VERSION), Ok(()));
        assert_eq!(
            check_version(PROTOCOL_VERSION + 1),
            Err(ProtocolError::UnsupportedVersion {
                got: PROTOCOL_VERSION + 1
            })
        );
    }
}
