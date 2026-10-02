//! The `session/new`, `session/prompt`, and `session/cancel` pipelines
//! (acp-adapter-lifecycle tickets 02+03, permission-bridge ticket 01):
//! ACP param validation, the adapter-local sequential turn guard,
//! per-call idempotency keys, journal→`session/update` mapping,
//! `stopReason` derivation, the gateway-backed prompt turn, the
//! drain-awaiting cancel, the bounded resubscribe, and the permission
//! bridge (a journalled `approval_request` → `tool_call` announcement →
//! `session/request_permission` → the client's answer → the gateway
//! `Approve` → resume; an ask that never arrives falls back to the
//! typed `approval_required` refusal).
//!
//! The adapter stays a pure gateway client (ADR-0005): every step below
//! is a framed gateway round trip or a pure mapping — no driver, no
//! tools, no execution, and no grant authority of its own: a parked ask
//! is forwarded to the ACP client and a granted answer becomes exactly
//! one `Command::Approve` into the Supervisor's one-shot registry.
//! Frame shapes follow the pinned ACP `schema-v1.23.0` artifact; where
//! this slice extends an object the schema leaves open (the prompt
//! response's `content` tail), the golden test pins the exact wire
//! bytes.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Number, Value, json};
use tachyon_protocol::{Command, GatewayEvent, ServerFrame};
use tachyon_types::{ApprovalId, EventId, SessionId, TaskId};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, watch};

use crate::client::{Connector, GatewayCallError, GatewayConn, GatewayUnavailable};
use crate::codec::{
    ErrorObject, GATEWAY_REFUSED, GATEWAY_UNAVAILABLE, INVALID_PARAMS, InboundResponse, Outbound,
    RpcId, TURN_CONFLICT, TURN_FAILED,
};

/// Bound on one whole `session/prompt` turn (connect → final response).
/// A run that dies without ever journalling a terminal status would
/// otherwise hang the client forever; the deadline converts that into a
/// typed error instead of a guessed verdict.
const TURN_TIMEOUT: Duration = Duration::from_secs(300);

/// Bound on one whole `session/cancel`: waiting for the active turn to
/// publish its task id, the `CancelTask` drain acknowledgement, or the
/// no-op `GetSession` existence check. Past it the client gets a typed
/// error instead of a wedged serve loop — the reply never claims a
/// drain that was not observed.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(120);

/// Re-subscribe budget per turn after `ResyncRequired`: one bounded
/// re-subscribe at the gateway-provided cursor; a second overflow fails
/// the prompt typed (never a silent gap, never an unbounded loop).
const MAX_RESUBSCRIBES: u32 = 1;

/// Bound on the wait for an `approval_request` frame after a park is
/// observed (the `status → WaitingApproval` journal is written BEFORE
/// the ask, so a settlement read can see the park a moment before the
/// request frame arrives). Past it the turn falls back to the typed
/// `approval_required` orphan refusal — never a silent hang, never a
/// request invented from a snapshot. Interim bound: the orphan-fallback
/// slice (ticket 03) tightens and documents it.
const APPROVAL_REQUEST_GRACE: Duration = Duration::from_secs(5);

/// Bound on the settle wait after a written `Deny`: a terminal status
/// may journal moments later; when none arrives (the gateway journals no
/// terminal status after a deny — pre-existing gap, out of scope) the
/// prompt settles `stopReason: refusal` at this deadline instead of
/// hanging to [`TURN_TIMEOUT`].
const DENY_SETTLE_GRACE: Duration = Duration::from_secs(5);

/// The `Command::Deny` reason for an explicit `reject_once` answer:
/// names the ACP client as the refusing party (ADR-0005:47).
const DENY_REASON_REJECTED: &str = "the ACP client rejected the permission request (reject_once)";

/// The `Command::Deny` reason for a STANDALONE `outcome: cancelled`
/// answer (no `session/cancel` in flight): there is no approval to
/// grant, so the exchange fails closed, naming the ACP client
/// (ADR-0005:49 — `cancelled` belongs to the cancel contract).
const DENY_REASON_CANCELLED: &str =
    "the ACP client answered cancelled with no session/cancel in flight; no approval granted";

/// Overlap refusal message: ACP v1 turns are sequential and are never
/// queued behind one another.
const TURN_IN_PROGRESS: &str = "Turn already active for this session; prompts run sequentially";

/// One typed adapter failure, already carrying its JSON-RPC wire shape.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HandlerError {
    /// JSON-RPC error code.
    pub(crate) code: i32,
    /// Human-readable message.
    pub(crate) message: String,
    /// Machine-readable `data` marker.
    pub(crate) data: Value,
}

impl HandlerError {
    /// A param-validation refusal (`-32602`).
    fn invalid(message: impl Into<String>, data: &str) -> Self {
        Self {
            code: INVALID_PARAMS,
            message: message.into(),
            data: json!(data),
        }
    }

    /// The gateway was unreachable (same actionable text the probe
    /// produces — one message shape for every request).
    pub(crate) fn unavailable(error: &GatewayUnavailable) -> Self {
        Self {
            code: GATEWAY_UNAVAILABLE,
            message: error.to_string(),
            data: json!("gateway_unavailable"),
        }
    }

    /// A typed gateway refusal; the gateway's `code` becomes the `data`
    /// marker so clients can branch without parsing prose.
    pub(crate) fn refused(code: impl Into<String>, message: impl Into<String>) -> Self {
        let code = code.into();
        Self {
            code: GATEWAY_REFUSED,
            message: format!("{code}: {}", message.into()),
            data: json!(code),
        }
    }

    /// A turn-pipeline failure with no honest verdict.
    pub(crate) fn turn_failed(data: &str, message: impl Into<String>) -> Self {
        Self {
            code: TURN_FAILED,
            message: message.into(),
            data: json!(data),
        }
    }

    /// The overlap refusal (`-32003`).
    fn turn_in_progress() -> Self {
        Self {
            code: TURN_CONFLICT,
            message: TURN_IN_PROGRESS.to_owned(),
            data: json!("turn_in_progress"),
        }
    }
}

/// Maps a transport failure from one command round trip onto its wire
/// error (unreachable vs typed refusal).
pub(crate) fn call_error(error: GatewayCallError) -> HandlerError {
    match error {
        GatewayCallError::Transport(unavailable) => HandlerError::unavailable(&unavailable),
        GatewayCallError::Refused { code, message } => HandlerError::refused(code, message),
    }
}

/// Validated `session/new` params.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionNewParams {
    /// Absolute session workspace root, exactly as the client sent it.
    pub(crate) cwd: String,
}

/// Validates `session/new` params per ACP `schema-v1.23.0`
/// (`NewSessionRequest`): `cwd` is required and must be absolute, and a
/// non-empty `mcpServers` is refused (MCP-at-setup is a later slice;
/// absent/empty is accepted). Runs BEFORE any gateway interaction, so a
/// bad request never reaches the liveness probe or opens a connection.
pub(crate) fn parse_session_new(params: Option<&Value>) -> Result<SessionNewParams, HandlerError> {
    let invalid_shape = || {
        HandlerError::invalid(
            "Invalid params: session/new requires a cwd",
            "invalid_params",
        )
    };
    let object = params
        .and_then(Value::as_object)
        .ok_or_else(invalid_shape)?;
    let cwd = object
        .get("cwd")
        .and_then(Value::as_str)
        .ok_or_else(invalid_shape)?;
    if !Path::new(cwd).is_absolute() {
        return Err(HandlerError::invalid(
            "Invalid params: cwd must be an absolute path",
            "cwd_not_absolute",
        ));
    }
    if let Some(servers) = object.get("mcpServers") {
        let list = servers.as_array().ok_or_else(|| {
            HandlerError::invalid(
                "Invalid params: mcpServers must be an array",
                "invalid_params",
            )
        })?;
        if !list.is_empty() {
            return Err(HandlerError::invalid(
                "MCP servers at session setup are not supported yet; mcpServers must be empty",
                "mcp_servers_unsupported",
            ));
        }
    }
    Ok(SessionNewParams {
        cwd: cwd.to_owned(),
    })
}

/// Validated `session/prompt` params.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromptParams {
    /// Session the turn belongs to (identity-mapped gateway session).
    pub(crate) session_id: SessionId,
    /// Concatenated text prompt — the task objective.
    pub(crate) objective: String,
}

/// Validates `session/prompt` params per ACP `schema-v1.23.0`
/// (`PromptRequest`): `sessionId` must parse as a session id and every
/// prompt block must be `text` (this slice advertises text-only prompt
/// content); an empty/whitespace-only text is a typed refusal and never
/// reaches `CreateTask`.
pub(crate) fn parse_prompt(params: Option<&Value>) -> Result<PromptParams, HandlerError> {
    let invalid_shape = || {
        HandlerError::invalid(
            "Invalid params: session/prompt requires sessionId and prompt",
            "invalid_params",
        )
    };
    let object = params
        .and_then(Value::as_object)
        .ok_or_else(invalid_shape)?;
    let raw_session = object
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(invalid_shape)?;
    let session_id: SessionId = raw_session.parse().map_err(|_| {
        HandlerError::invalid(
            "Invalid params: sessionId is not a valid session id",
            "invalid_session_id",
        )
    })?;
    let blocks = object
        .get("prompt")
        .and_then(Value::as_array)
        .ok_or_else(invalid_shape)?;
    let mut texts: Vec<&str> = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                let text = block.get("text").and_then(Value::as_str).ok_or_else(|| {
                    HandlerError::invalid(
                        "Invalid params: text prompt block missing text",
                        "invalid_prompt",
                    )
                })?;
                texts.push(text);
            }
            Some(_) => {
                return Err(HandlerError::invalid(
                    "Unsupported prompt content; this agent advertises text-only prompts",
                    "unsupported_prompt_content",
                ));
            }
            None => {
                return Err(HandlerError::invalid(
                    "Invalid params: prompt block missing type",
                    "invalid_prompt",
                ));
            }
        }
    }
    let objective = texts.join("\n");
    if objective.trim().is_empty() {
        return Err(HandlerError::invalid(
            "Invalid params: prompt text must not be empty",
            "empty_prompt",
        ));
    }
    Ok(PromptParams {
        session_id,
        objective,
    })
}

/// Validates `session/cancel` params (ACP `schema-v1.23.0`
/// `CancelNotification`): a `sessionId` that parses as a session id —
/// the same identity check `session/prompt` applies, and it runs BEFORE
/// the liveness gate, so a malformed cancel never touches the gateway.
pub(crate) fn parse_cancel(params: Option<&Value>) -> Result<SessionId, HandlerError> {
    let invalid_shape = || {
        HandlerError::invalid(
            "Invalid params: session/cancel requires sessionId",
            "invalid_params",
        )
    };
    let object = params
        .and_then(Value::as_object)
        .ok_or_else(invalid_shape)?;
    let raw = object
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(invalid_shape)?;
    raw.parse().map_err(|_| {
        HandlerError::invalid(
            "Invalid params: sessionId is not a valid session id",
            "invalid_session_id",
        )
    })
}

/// The typed failure for a turn parked on a permission approval the
/// adapter could not resolve: an orphan park (no `approval_request`
/// frame arrived within [`APPROVAL_REQUEST_GRACE`], or the ask carried
/// no usable id) or a request the client did not grant. The prompt
/// fails typed at the first read that observes it — never hangs to the
/// turn deadline, never guesses a verdict (the grant path is
/// `session/request_permission`; deny settlement is the fail-closed
/// slice's job).
pub(crate) fn approval_parked() -> HandlerError {
    HandlerError::turn_failed(
        "approval_required",
        "turn is parked awaiting a permission approval the adapter could not resolve \
         (no approval_request frame arrived within the grace bound, or the client did \
         not grant the requested permission)",
    )
}

/// Whether a task status is the permission park. Only `WaitingApproval`
/// matches: every other status keeps its existing pipeline meaning.
#[must_use]
pub(crate) fn is_approval_parked(status: &str) -> bool {
    status == "WaitingApproval"
}

/// Reply slots for adapter-minted outbound requests (agent → client
/// requests such as `session/request_permission`).
///
/// Ownership protocol — response-loss-free by construction:
/// 1. the TURN mints the id and arms the slot here BEFORE queueing the
///    frame, so a fast client response can never arrive with nowhere to
///    route;
/// 2. only the SERVE LOOP writes the frame (it is the sole holder of
///    the peer — single-writer invariant preserved);
/// 3. the serve loop's `Parsed::Response` arm routes the answer through
///    [`OutboundRequests::route`]; an id with no armed slot is
///    unsolicited and is ignored safely.
///
/// Id space: NEGATIVE integers from a dedicated counter, disjoint from
/// `Peer`'s positive request counter and from the ids clients mint for
/// their own requests — pinned by unit.
#[derive(Debug, Default)]
pub(crate) struct OutboundRequests {
    inner: Mutex<OutboundRequestsInner>,
}

#[derive(Debug)]
struct OutboundRequestsInner {
    /// Next adapter-minted outbound id: −1, −2, … (never overlaps the
    /// positive ids `Peer::send_request` mints or client request ids).
    next_outbound: i64,
    /// Armed reply slots by correlation id.
    slots: HashMap<RpcId, oneshot::Sender<Result<Value, ErrorObject>>>,
}

impl Default for OutboundRequestsInner {
    fn default() -> Self {
        Self {
            next_outbound: -1,
            slots: HashMap::new(),
        }
    }
}

impl OutboundRequests {
    /// Mints the next outbound id and arms its reply slot. The turn
    /// holds the receiver until the client answers (or the slot is
    /// cleared at EOF, which wakes it as a disconnect).
    pub(crate) fn arm(&self) -> (RpcId, oneshot::Receiver<Result<Value, ErrorObject>>) {
        let mut inner = self.inner.lock().expect("outbound-request lock poisoned");
        let id = RpcId::Number(Number::from(inner.next_outbound));
        inner.next_outbound -= 1;
        let (sender, receiver) = oneshot::channel();
        inner.slots.insert(id.clone(), sender);
        (id, receiver)
    }

    /// Routes one client response to its waiting turn. Returns `false`
    /// when no slot is armed for the id — an unsolicited or late
    /// response, which the caller ignores safely.
    pub(crate) fn route(&self, response: &InboundResponse) -> bool {
        let slot = self
            .inner
            .lock()
            .expect("outbound-request lock poisoned")
            .slots
            .remove(&response.id);
        let Some(sender) = slot else {
            return false;
        };
        let _ignored = sender.send(response.payload.clone());
        true
    }

    /// Drops a slot whose waiter went away (the turn was abandoned
    /// before the client answered).
    fn disarm(&self, id: &RpcId) {
        self.inner
            .lock()
            .expect("outbound-request lock poisoned")
            .slots
            .remove(id);
    }

    /// Drops every armed slot (client EOF): every waiting turn wakes as
    /// disconnected instead of waiting out the turn deadline.
    pub(crate) fn clear(&self) {
        self.inner
            .lock()
            .expect("outbound-request lock poisoned")
            .slots
            .clear();
    }

    /// How many reply slots are armed — test seam.
    #[cfg(test)]
    pub(crate) fn armed(&self) -> usize {
        self.inner
            .lock()
            .expect("outbound-request lock poisoned")
            .slots
            .len()
    }
}

/// RAII owner of one armed reply slot: disarms on every exit path
/// (answer routed, turn failed, future dropped), so an abandoned ask
/// never leaks its registry entry. Disarming an already-routed slot is
/// a no-op.
#[derive(Debug)]
struct ArmedRequest {
    state: Arc<SessionState>,
    id: RpcId,
}

impl Drop for ArmedRequest {
    fn drop(&mut self) {
        self.state.requests().disarm(&self.id);
    }
}

/// One permission exchange's progress within a turn. Transitions:
/// `Idle → AwaitingRequest → Outstanding → Approving|Denying →
/// Sent|Denied → Idle`; a later park in the same turn starts again from
/// `Idle`.
#[derive(Debug)]
enum PermissionPhase {
    /// No park observed (or the previous exchange settled).
    Idle,
    /// A park is observed but the `approval_request` frame has not
    /// arrived yet — bounded by [`APPROVAL_REQUEST_GRACE`], then the
    /// typed orphan refusal.
    AwaitingRequest {
        /// Instant after which the orphan fallback fires.
        deadline: Instant,
    },
    /// The ask frame is queued: `tool_call` + `session/request_permission`
    /// pushed, reply slot armed — waiting for the client's response.
    Outstanding {
        /// The client's answer, routed here by the serve loop.
        receiver: oneshot::Receiver<Result<Value, ErrorObject>>,
        /// Keeps the registry slot alive until the answer is routed.
        armed: ArmedRequest,
        /// Gateway approval id this exchange decides.
        approval_id: String,
        /// ACP tool call announced for this ask.
        tool_call_id: String,
    },
    /// The client granted (`allow_once`): the gateway `Approve` waits
    /// for the single settlement slot to free up.
    Approving {
        /// Gateway approval id to grant.
        approval_id: String,
        /// ACP tool call to close as `completed`.
        tool_call_id: String,
    },
    /// The client rejected (`reject_once`): the gateway `Deny` waits for
    /// the single settlement slot, sequenced exactly like `Approve`.
    Denying {
        /// Gateway approval id to refuse.
        approval_id: String,
        /// ACP tool call to close as `failed`.
        tool_call_id: String,
        /// Reason recorded in the gateway journal (names the client).
        reason: String,
    },
    /// `Approve` written; streaming resumes and the
    /// `WaitingApproval → Executing` bounce settles benignly.
    Sent,
    /// `Deny` written; bounded by [`DENY_SETTLE_GRACE`] for a terminal
    /// status — expiry settles the prompt `refusal` (the gateway
    /// journals no terminal status after a deny; known gap, out of
    /// scope).
    Denied {
        /// Instant after which the prompt settles `refusal`.
        deadline: Instant,
    },
}

/// The gateway decision waiting for the free settlement slot: grant or
/// refuse the parked operation (both written through the same single
/// slot, in the same loop step).
#[derive(Debug)]
enum PendingDecision {
    /// Grant: `Command::Approve`, tool call closes `completed`.
    Approve {
        /// Gateway approval id to grant.
        approval_id: String,
        /// ACP tool call to close.
        tool_call_id: String,
    },
    /// Refuse: `Command::Deny` with a reason naming the client, tool
    /// call closes `failed`.
    Deny {
        /// Gateway approval id to refuse.
        approval_id: String,
        /// ACP tool call to close.
        tool_call_id: String,
        /// Reason recorded in the gateway journal.
        reason: String,
    },
}

/// One classified answer to `session/request_permission`, as the loop
/// acts on it (grant → `Approve`, refuse → `Deny`, anything the adapter
/// cannot resolve → the typed orphan refusal).
#[derive(Debug)]
enum ClientAnswer {
    /// `selected` + `allow_once`: grant through `Command::Approve`.
    Grant,
    /// `selected` + `reject_once`, or an invalid shape: refuse through
    /// `Command::Deny`, carrying a reason that names the ACP client.
    Deny(String),
    /// A session/cancel owns this exchange: decide NOTHING — no
    /// Approve, no Deny (the cancel path resolves the request locally;
    /// the turn settles from the gateway's terminal journal).
    NoDecision,
    /// Error frame or dropped slot: no verdict from here — the turn
    /// fails typed (fail closed).
    Unresolved,
}

/// Why a bounded read expired: the park carried no ask (typed orphan
/// refusal) or the deny settled with no terminal status (the prompt's
/// `refusal` verdict).
#[derive(Debug, PartialEq)]
enum ReadExpiry {
    /// No `approval_request` frame within the orphan grace.
    Orphan,
    /// No terminal status within the deny grace.
    DenySettle,
}

/// The bound on the next gateway read, if any: which instant expires
/// and what that expiry means for the prompt. Only the two bounded
/// phases carry a bound — an outstanding client answer is human-paced
/// (no read bound), everything else waits freely.
fn read_bound(phase: &PermissionPhase) -> Option<(Instant, ReadExpiry)> {
    match phase {
        PermissionPhase::AwaitingRequest { deadline } => Some((*deadline, ReadExpiry::Orphan)),
        PermissionPhase::Denied { deadline } => Some((*deadline, ReadExpiry::DenySettle)),
        _ => None,
    }
}

/// The ask carried by one journalled `approval_request` payload: the
/// approval id plus a human-readable title. The payload is the
/// t/v-tagged `StateEvent::ApprovalRequest`
/// (`{"t":"ApprovalRequest","v":{"request":{…}}}`); an untagged legacy
/// payload is tolerated. `None` when no id is present — an approval is
/// never invented from a snapshot (ADR-0005:47 binds every decision to
/// the exact pending operation).
fn approval_ask(payload: &Value) -> Option<(String, String)> {
    let value = payload.get("v").unwrap_or(payload);
    let request = value.get("request").unwrap_or(value);
    let id = request.get("id").and_then(Value::as_str)?;
    let title = request
        .get("summary")
        .or_else(|| request.get("operation"))
        .or_else(|| request.get("description"))
        .or_else(|| request.get("capability"))
        .and_then(Value::as_str)
        .unwrap_or("permission request");
    Some((id.to_owned(), title.to_owned()))
}

/// The `session/update` `tool_call` announcement for one ask (ACP
/// schema-v1.23.0 `SessionUpdate` → `ToolCall`: `toolCallId` + `title`
/// required) — sent BEFORE the permission request frame.
fn tool_call_announcement(session_id: &str, tool_call_id: &str, title: &str) -> Outbound {
    Outbound::notification(
        "session/update",
        json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "tool_call",
                "toolCallId": tool_call_id,
                "title": title,
            },
        }),
    )
}

/// The `session/request_permission` params (ACP schema-v1.23.0
/// `RequestPermissionRequest`: `sessionId`, `toolCall`, `options` —
/// with EXACTLY the two one-shot options ADR-0005:48 allows, each
/// carrying `optionId` + `name` + `kind`).
fn permission_request_params(session_id: &str, tool_call_id: &str, title: &str) -> Value {
    json!({
        "sessionId": session_id,
        "toolCall": {
            "toolCallId": tool_call_id,
            "title": title,
        },
        "options": [
            {
                "optionId": "allow_once",
                "name": "Allow once",
                "kind": "allow_once",
            },
            {
                "optionId": "reject_once",
                "name": "Reject once",
                "kind": "reject_once",
            },
        ],
    })
}

/// The `tool_call_update` frame closing an announced tool call (ACP
/// schema-v1.23.0 `ToolCallUpdate`: `toolCallId` required; `status` ∈
/// `pending|in_progress|completed|failed`).
fn tool_call_status_update(session_id: &str, tool_call_id: &str, status: &str) -> Outbound {
    Outbound::notification(
        "session/update",
        json!({
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": tool_call_id,
                "status": status,
            },
        }),
    )
}

/// The strict allow gate: `outcome: "selected"` with the offered
/// `allow_once` option. Every other shape is NOT a grant here — deny
/// and fail-closed settlement are the fail-closed slice's job; this
/// ticket only ever grants on an exact match (ADR-0005:47-48).
fn is_allow_once(response: &Value) -> bool {
    response.get("outcome").and_then(Value::as_str) == Some("selected")
        && response.get("optionId").and_then(Value::as_str) == Some("allow_once")
}

/// The strict reject gate: `outcome: "selected"` with the offered
/// `reject_once` option (the symmetric twin of [`is_allow_once`]) —
/// the one non-grant answer this ticket settles as an explicit `Deny`.
fn is_reject_once(response: &Value) -> bool {
    response.get("outcome").and_then(Value::as_str) == Some("selected")
        && response.get("optionId").and_then(Value::as_str) == Some("reject_once")
}

/// One classified answer to `session/request_permission` (spec
/// "Response contract"): exactly `selected` + one of the two offered
/// optionIds, or `cancelled` — every other shape is `Invalid`
/// (ADR-0005:47: unknown/unoffered/malformed ⇒ fail closed).
#[derive(Debug, PartialEq)]
enum PermissionAnswer {
    /// `selected` + the offered `allow_once`.
    Allow,
    /// `selected` + the offered `reject_once`.
    Reject,
    /// `outcome: "cancelled"` — a valid shape whose decision belongs
    /// to the cancel contract (standalone ⇒ deny; see
    /// [`classify_permission_answer`]).
    Cancelled,
    /// Unknown optionId, unknown outcome, missing optionId, or a
    /// non-object response — never a grant, never Approve.
    Invalid {
        /// Why the shape failed validation (logged and carried in the
        /// fail-closed `Deny` reason).
        reason: String,
    },
}

/// The strict response validator applied to EVERY client answer before
/// any Approve path: `allow_once`/`reject_once` only under
/// `outcome: "selected"`, `outcome: "cancelled"` accepted as its own
/// shape, everything else invalid and destined to fail closed as a
/// `Deny` (ADR-0005:47 — the operation must never run).
fn classify_permission_answer(response: &Value) -> PermissionAnswer {
    if is_allow_once(response) {
        return PermissionAnswer::Allow;
    }
    if is_reject_once(response) {
        return PermissionAnswer::Reject;
    }
    match response.get("outcome").and_then(Value::as_str) {
        Some("cancelled") => PermissionAnswer::Cancelled,
        Some("selected") => match response.get("optionId").and_then(Value::as_str) {
            Some(option) => PermissionAnswer::Invalid {
                reason: format!("unknown optionId {option:?}"),
            },
            None => PermissionAnswer::Invalid {
                reason: "selected without an optionId".to_owned(),
            },
        },
        Some(outcome) => PermissionAnswer::Invalid {
            reason: format!("unknown outcome {outcome:?}"),
        },
        None => PermissionAnswer::Invalid {
            reason: "response carries no outcome".to_owned(),
        },
    }
}

/// Maps one received permission response onto the loop's action — the
/// single funnel every client answer passes through before any Approve
/// path: allow → grant, reject/invalid shape → fail-closed `Deny` +
/// log (ADR-0005:47), `cancelled` → standalone `Deny` + log when NO
/// session/cancel owns the exchange, `NoDecision` when one does (the
/// cancel path owns it — zero decision frames, never a double-decide),
/// and error frames / dropped slots → unresolved (typed refusal).
fn answer_action(
    response: Result<Result<Value, ErrorObject>, oneshot::error::RecvError>,
    cancel_owns: bool,
) -> ClientAnswer {
    match response {
        Ok(Ok(value)) => match classify_permission_answer(&value) {
            PermissionAnswer::Allow => ClientAnswer::Grant,
            PermissionAnswer::Reject => {
                tracing::info!(
                    response = %value,
                    "the ACP client answered reject_once; refusing the park"
                );
                ClientAnswer::Deny(DENY_REASON_REJECTED.to_owned())
            }
            PermissionAnswer::Cancelled if cancel_owns => {
                tracing::info!(
                    response = %value,
                    "session/cancel owns this exchange; resolving the request locally, \
                     no Approve/Deny issued"
                );
                ClientAnswer::NoDecision
            }
            PermissionAnswer::Cancelled => {
                // Standalone `cancelled`: no approval exists to grant,
                // no cancel resolves this request — fail closed as a
                // deny (ADR-0005:49; spec "Response contract").
                tracing::warn!(
                    response = %value,
                    "standalone cancelled outcome with no session/cancel in flight; \
                     no-approval ⇒ Deny (ADR-0005:49)"
                );
                ClientAnswer::Deny(DENY_REASON_CANCELLED.to_owned())
            }
            PermissionAnswer::Invalid { reason } => {
                tracing::warn!(
                    response = %value,
                    %reason,
                    "invalid permission response; failing closed as Deny (ADR-0005:47)"
                );
                ClientAnswer::Deny(format!(
                    "the ACP client sent an invalid permission response ({reason}); \
                     failing closed"
                ))
            }
        },
        Ok(Err(error)) => {
            tracing::warn!(
                ?error,
                "client answered the permission request with an error frame; \
                 refusing the park (fail-closed)"
            );
            ClientAnswer::Unresolved
        }
        Err(_) => {
            tracing::warn!(
                "permission request slot closed before an answer arrived \
                 (client gone or EOF); refusing the park"
            );
            ClientAnswer::Unresolved
        }
    }
}

/// Permission-bridge state threaded through one stream loop: the
/// exchange phase plus the approval ids already asked about, so a
/// re-delivered replay row (after a re-subscribe) can never ask twice.
#[derive(Debug)]
struct PermissionBridge {
    phase: PermissionPhase,
    asked: HashSet<String>,
}

impl Default for PermissionBridge {
    fn default() -> Self {
        Self {
            phase: PermissionPhase::Idle,
            asked: HashSet::new(),
        }
    }
}

impl PermissionBridge {
    /// Handles one journalled `approval_request`: emits the `tool_call`
    /// announcement and the `session/request_permission` frame (both
    /// through the writer channel, so stdout order holds), then arms the
    /// reply slot — the exchange is journal-driven, never derived from a
    /// `GetTask` snapshot. Returns typed errors only when the frames
    /// could not be queued at all (the client is gone).
    fn ask(
        &mut self,
        session_id: &str,
        payload: &Value,
        emit: &UnboundedSender<Outbound>,
        state: &Arc<SessionState>,
    ) -> Result<(), HandlerError> {
        let Some((approval_id, title)) = approval_ask(payload) else {
            tracing::warn!(
                "approval_request journal carries no approval id; refusing to invent \
                 an approval (orphan fallback applies)"
            );
            return Ok(());
        };
        if self.asked.contains(&approval_id) {
            tracing::debug!(%approval_id, "approval already asked about; ignoring the replayed row");
            return Ok(());
        }
        if !matches!(
            self.phase,
            PermissionPhase::Idle | PermissionPhase::AwaitingRequest { .. }
        ) {
            tracing::warn!(
                %approval_id,
                "another approval_request arrived while an exchange is in flight; ignoring it"
            );
            return Ok(());
        }
        self.asked.insert(approval_id.clone());
        let tool_call_id = approval_id.clone();
        // The announcement precedes the request frame on the writer
        // channel, so stdout order is tool_call → request_permission.
        if emit
            .send(tool_call_announcement(session_id, &tool_call_id, &title))
            .is_err()
        {
            return Err(HandlerError::turn_failed(
                "client_disconnected",
                "client went away before the permission request could be delivered",
            ));
        }
        // Arm the reply slot BEFORE the request is queued (the serve
        // loop writes it later): a response can never outrun its slot.
        let (request_id, receiver) = state.requests().arm();
        let armed = ArmedRequest {
            state: Arc::clone(state),
            id: request_id.clone(),
        };
        if emit
            .send(Outbound::request(
                request_id,
                "session/request_permission",
                permission_request_params(session_id, &tool_call_id, &title),
            ))
            .is_err()
        {
            return Err(HandlerError::turn_failed(
                "client_disconnected",
                "client went away before the permission request could be delivered",
            ));
        }
        tracing::info!(
            %approval_id,
            %tool_call_id,
            "approval parked; session/request_permission emitted (allow_once + reject_once)"
        );
        self.phase = PermissionPhase::Outstanding {
            receiver,
            armed,
            approval_id,
            tool_call_id,
        };
        Ok(())
    }

    /// A park was observed on a settlement read: wait (bounded) for the
    /// `approval_request` journal instead of failing on the snapshot
    /// alone — the ask journals AFTER the status row, so it normally
    /// arrives moments later. A request already in flight is untouched.
    fn on_parked(&mut self) {
        if matches!(self.phase, PermissionPhase::Idle) {
            self.phase = PermissionPhase::AwaitingRequest {
                deadline: Instant::now() + APPROVAL_REQUEST_GRACE,
            };
        }
    }
}

/// Adapter-wide state shared by the serve loop and its spawned turns:
/// the sequential-turn guard (whose slots also publish each active
/// turn's task id for `session/cancel`) plus the per-call
/// idempotency-key registry.
#[derive(Debug, Default)]
pub(crate) struct SessionState {
    /// Sessions with a turn in flight → the watch carrying that turn's
    /// task id once `CreateTask` published it (`None` until then).
    active: Mutex<HashMap<String, watch::Sender<Option<TaskId>>>>,
    /// Request id → idempotency key.
    calls: Mutex<HashMap<RpcId, String>>,
    /// Reply slots for adapter-minted outbound requests (the serve loop
    /// routes client answers through these).
    requests: OutboundRequests,
    /// Sessions whose `session/cancel` targeted the active turn and has
    /// not yet been consumed by that turn's `cancelled` answer — the
    /// route guard that keeps the turn from deciding an exchange the
    /// cancel path owns (ADR-0005:49: zero decision frames on cancel).
    cancels_in_flight: Mutex<HashSet<String>>,
}

impl SessionState {
    /// The outbound-request reply registry shared by turns and the
    /// serve loop.
    pub(crate) fn requests(&self) -> &OutboundRequests {
        &self.requests
    }
    /// Marks that a `session/cancel` for `session_id` targeted the
    /// active turn — called by the cancel pipeline before it awaits the
    /// drain ack, so the turn's `cancelled` answer (routed afterwards)
    /// finds the mark set. One mark per session; consumed by
    /// [`SessionState::take_cancel_resolution`].
    pub(crate) fn arm_cancel_resolution(&self, session_id: &str) {
        self.cancels_in_flight
            .lock()
            .expect("cancel-mark lock")
            .insert(session_id.to_owned());
    }
    /// Consumes the cancel mark: `true` when a `session/cancel` owns
    /// the `cancelled` answer about to be decided (the turn sends no
    /// decision), `false` for a standalone answer (fail closed as a
    /// deny). First read wins — the mark is removed, so a later
    /// spontaneous answer can never inherit it (no stale guard, no
    /// missed deny).
    pub(crate) fn take_cancel_resolution(&self, session_id: &str) -> bool {
        self.cancels_in_flight
            .lock()
            .expect("cancel-mark lock")
            .remove(session_id)
    }
    /// Takes the session's turn slot, or the overlap refusal when one
    /// is active. Callable only from the serve loop's request handler —
    /// requests are read strictly sequentially, so the first caller
    /// always wins and a second overlapping prompt can never slip in
    /// while the first connects or streams (ACP v1 turns are
    /// sequential and never queued).
    pub(crate) fn try_acquire_turn(
        self: &Arc<Self>,
        session_id: &str,
    ) -> Result<TurnGuard, HandlerError> {
        let mut active = self.active.lock().expect("active-turn lock poisoned");
        if active.contains_key(session_id) {
            return Err(HandlerError::turn_in_progress());
        }
        // A fresh turn inherits no cancel mark: any mark a prior turn
        // left unconsumed belonged to that turn's exchange.
        self.cancels_in_flight
            .lock()
            .expect("cancel-mark lock")
            .remove(session_id);
        let (notice, _) = watch::channel(None);
        active.insert(session_id.to_owned(), notice.clone());
        drop(active);
        Ok(TurnGuard {
            state: Arc::clone(self),
            session_id: session_id.to_owned(),
            notice,
        })
    }

    /// The task-id watch of the session's ACTIVE turn, or `None` when
    /// no turn is active. A receiver subscribed here stays usable after
    /// the guard drops: a published id survives in the watch, an
    /// unpublished one closes (the waiting cancel then falls through to
    /// the no-active-turn path).
    pub(crate) fn turn_task(&self, session_id: &str) -> Option<watch::Receiver<Option<TaskId>>> {
        self.active
            .lock()
            .expect("active-turn lock poisoned")
            .get(session_id)
            .map(watch::Sender::subscribe)
    }

    /// The idempotency key for ACP request `id`: a SAME-call retry
    /// reuses the key its first delivery minted (so `CreateTask`
    /// replays instead of creating a duplicate task), while a NEW call
    /// always mints a fresh UUID-style key. In-process only: the map
    /// dies with the adapter — cross-restart reconciliation is the
    /// session-load slice's problem, not this one's.
    pub(crate) fn key_for(&self, id: &RpcId) -> (String, bool) {
        let mut keys = self.calls.lock().expect("prompt-call key lock poisoned");
        if let Some(existing) = keys.get(id) {
            return (existing.clone(), true);
        }
        let key = EventId::generate().to_string();
        keys.insert(id.clone(), key.clone());
        (key, false)
    }
}

/// RAII turn-slot owner: released on every exit path (success, typed
/// error, panic unwind), so a failed turn never wedges its session.
#[derive(Debug)]
pub(crate) struct TurnGuard {
    state: Arc<SessionState>,
    session_id: String,
    notice: watch::Sender<Option<TaskId>>,
}

impl TurnGuard {
    /// The channel the turn pipeline publishes its task id into, so a
    /// concurrent `session/cancel` learns exactly which task to cancel.
    pub(crate) fn notice(&self) -> watch::Sender<Option<TaskId>> {
        self.notice.clone()
    }
}

impl Drop for TurnGuard {
    fn drop(&mut self) {
        let mut active = self.state.active.lock().expect("active-turn lock poisoned");
        active.remove(&self.session_id);
    }
}

/// The ACP `stopReason` for a terminal task status: `Completed` →
/// `end_turn`, `Cancelled` → `cancelled`. `Failed` has no schema-legal
/// verdict — the pinned ACP `schema-v1.23.0` `StopReason` enum is
/// `end_turn|max_tokens|max_turn_requests|refusal|cancelled`, with no
/// `"error"` member — so it, like every non-terminal or unknown status,
/// is `Err`: the caller answers a typed `-32004` error frame, never a
/// schema-divergent success or a guessed verdict.
pub(crate) fn stop_reason(status: &str) -> Result<&'static str, HandlerError> {
    match status {
        "Completed" => Ok("end_turn"),
        "Cancelled" => Ok("cancelled"),
        "Failed" => Err(HandlerError::turn_failed(
            "task_failed",
            format!(
                "task status {status} carries no schema-legal stopReason; \
                 refusing to guess a verdict"
            ),
        )),
        _ => Err(HandlerError::turn_failed(
            "ambiguous_task_status",
            format!("task status {status} is not terminal; refusing to guess a verdict"),
        )),
    }
}

/// Whether a task status is one of the three terminal states (a
/// terminal snapshot always reaches `final_prompt_result`, which
/// answers either the two schema-legal verdicts or a typed error).
#[must_use]
pub(crate) fn is_terminal_status(status: &str) -> bool {
    matches!(status, "Completed" | "Failed" | "Cancelled")
}

/// Builds the `session/prompt` success shape from a `GetTask` snapshot:
/// `{stopReason, content: [{type: "text", text}]}` with `text` taken
/// from the conversation tail (the last `agent` message). Only the two
/// schema-legal verdicts (`Completed`, `Cancelled`) succeed; a `Failed`
/// snapshot is a typed `task_failed` error and any non-terminal or
/// unrecognized status a typed `ambiguous_task_status` error — never a
/// guessed verdict, never a schema-divergent stopReason.
pub(crate) fn final_prompt_result(task: &Value) -> Result<Value, HandlerError> {
    let status = task.get("status").and_then(Value::as_str).ok_or_else(|| {
        HandlerError::turn_failed(
            "ambiguous_task_status",
            "task snapshot carries no status; refusing to guess a verdict",
        )
    })?;
    let stop = stop_reason(status)?;
    let tail = task
        .get("conversation")
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages
                .iter()
                .rev()
                .find(|message| message.get("speaker").and_then(Value::as_str) == Some("agent"))
                .and_then(|message| message.get("content").and_then(Value::as_str))
        });
    let content = match tail {
        Some(text) => json!([{ "type": "text", "text": text }]),
        None => json!([]),
    };
    Ok(json!({ "stopReason": stop, "content": content }))
}

/// The `session/prompt` result for a deny whose bounded grace elapsed
/// with no terminal status: the schema-legal `refusal` verdict with
/// empty content — never `turn_timed_out` (an error `data` marker, not
/// a `stopReason`) and never a guessed `end_turn`. No terminal status
/// is needed to build it (ADR-0005:47-49, spec "Deny settlement").
#[must_use]
fn refusal_prompt_result() -> Value {
    json!({ "stopReason": "refusal", "content": [] })
}

/// The `session/update` payload for one journalled event, or `None` for
/// every kind this slice does not map. ONLY `agent_message` becomes an
/// `agent_message_chunk` (Text); stage/status/progress kinds are
/// omitted — never mislabeled as chunks (ADR-0005: advertise nothing
/// unimplemented; the spec forbids fake chunks).
pub(crate) fn agent_chunk(kind: &str, payload: &Value) -> Option<Value> {
    if kind != "agent_message" {
        return None;
    }
    // Journalled `StateEvent` payloads are t/v-tagged; the raw fallback
    // tolerates an untagged payload from an older journal row.
    let value = payload.get("v").unwrap_or(payload);
    let text = value
        .get("message")
        .or_else(|| value.get("text"))
        .and_then(Value::as_str)?;
    Some(json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text },
    }))
}

/// Journal kinds that can settle the turn (they are the only events
/// after which a `GetTask` status can become terminal): `status`
/// transitions and the verification tail. Every terminal status change
/// journals one of these two kinds, so an event-driven `GetTask` is
/// complete — no polling loop.
fn is_settlement_signal(kind: &str) -> bool {
    matches!(kind, "status" | "verification_finished")
}

/// One `session/prompt` turn end to end.
///
/// [`TURN_TIMEOUT`]-bounded — including any wait on a human's
/// permission answer (timeout suspension is a later slice). `emit`
/// receives the `session/update` notification frames streamed while the
/// turn runs; the returned value is the final `session/prompt` result.
pub(crate) async fn run_prompt<C: Connector>(
    connector: &C,
    params: PromptParams,
    idempotency_key: String,
    emit: &UnboundedSender<Outbound>,
    notice: watch::Sender<Option<TaskId>>,
    state: &Arc<SessionState>,
) -> Result<Value, HandlerError> {
    let outcome = tokio::time::timeout(
        TURN_TIMEOUT,
        prompt_turn(connector, params, idempotency_key, emit, notice, state),
    )
    .await;
    match outcome {
        Ok(result) => result,
        Err(_) => Err(HandlerError::turn_failed(
            "turn_timed_out",
            format!(
                "turn did not reach a terminal task status within {}s",
                TURN_TIMEOUT.as_secs()
            ),
        )),
    }
}

/// The untimed pipeline body; see [`run_prompt`].
async fn prompt_turn<C: Connector>(
    connector: &C,
    params: PromptParams,
    idempotency_key: String,
    emit: &UnboundedSender<Outbound>,
    notice: watch::Sender<Option<TaskId>>,
    state: &Arc<SessionState>,
) -> Result<Value, HandlerError> {
    let PromptParams {
        session_id,
        objective,
    } = params;
    let mut conn = connector
        .connect()
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;

    // Session resolution doubles as the identity check: an unknown id
    // fails here with the gateway's `unknown_session`.
    let session = conn
        .call(Command::GetSession { session_id })
        .await
        .map_err(call_error)?;
    let workspace_root = session
        .get("workspace_root")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "session_has_no_workspace_root",
                "session has no workspace root to run in",
            )
        })?;

    // The ONE idempotency-keyed `CreateTask`: a same-call retry replays
    // the original task (no duplicate), a fresh call creates a new one.
    // The replayed payload carries the status captured at CREATE time,
    // so the current state is always read fresh: a replayed turn that
    // already finished answers straight from `GetTask` instead of
    // re-running a terminal task.
    let task = conn
        .call(Command::CreateTask {
            session_id,
            objective,
            idempotency_key: Some(idempotency_key),
        })
        .await
        .map_err(call_error)?;
    let task_id: TaskId = task
        .get("task_id")
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse().ok())
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "CreateTask answered without a parseable task_id",
            )
        })?;
    // Publish the task id into the turn slot's watch NOW: a concurrent
    // `session/cancel` waits on it to learn which task to cancel, so a
    // cancel arriving mid-`CreateTask` still lands on this task instead
    // of a no-op.
    notice.send_replace(Some(task_id));
    let snapshot = conn
        .call(Command::GetTask { task_id })
        .await
        .map_err(call_error)?;
    let task_state = snapshot.get("task").cloned().unwrap_or(Value::Null);
    let current_status = task_state
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if is_terminal_status(current_status) {
        return final_prompt_result(&task_state);
    }
    if is_approval_parked(current_status) {
        // A replayed turn already parked before this prompt ever
        // subscribed: there is no journal stream here to carry the
        // `approval_request` ask, so the park stays typed (orphan path;
        // the bridge only ever answers an ask it has actually seen).
        return Err(approval_parked());
    }

    conn.call(Command::StartRun {
        task_id,
        workspace_root,
        acceptance: None,
    })
    .await
    .map_err(call_error)?;

    let subscribe_id = conn
        .send(Command::Subscribe {
            task_id,
            after_seq: 0,
        })
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;
    stream_turn(&mut conn, subscribe_id, task_id, session_id, emit, state).await
}

/// One `session/cancel` end to end, [`CANCEL_TIMEOUT`]-bounded:
/// resolve the session's active turn (if any) to the task id it
/// published, await `CancelTask` — the Supervisor's synchronous drain
/// acknowledgement (ADR-0005:49), the ONLY evidence of drain — or,
/// with no active turn, prove the session exists via `GetSession` and
/// answer idempotent ok without ever issuing `CancelTask`.
///
/// `Ok(())` means "safe to reply ok": either the drain ack arrived (or
/// the task was already terminal), or there was nothing to cancel.
pub(crate) async fn run_cancel<C: Connector>(
    connector: &C,
    state: &Arc<SessionState>,
    session_id: SessionId,
) -> Result<(), HandlerError> {
    match tokio::time::timeout(
        CANCEL_TIMEOUT,
        cancel_pipeline(connector, state, session_id),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(HandlerError::turn_failed(
            "cancel_timed_out",
            format!(
                "session/cancel did not settle within {}s; the drain acknowledgement \
                 never arrived",
                CANCEL_TIMEOUT.as_secs()
            ),
        )),
    }
}

/// The untimed cancel body; see [`run_cancel`].
async fn cancel_pipeline<C: Connector>(
    connector: &C,
    state: &Arc<SessionState>,
    session_id: SessionId,
) -> Result<(), HandlerError> {
    // The active turn's published task id is the ONLY task this cancel
    // may touch: a no-op cancel never cancels a historical task
    // (reconnect/attach is a later slice).
    let target = match state.turn_task(&session_id.to_string()) {
        Some(mut received) => match received.wait_for(Option::is_some).await {
            Ok(published) => *published,
            // The turn ended before `CreateTask` published a task id:
            // there is nothing to cancel — fall through to the
            // no-active-turn path (which still proves the session).
            Err(_) => None,
        },
        None => None,
    };
    let mut conn = connector
        .connect()
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;
    if let Some(task_id) = target {
        // The cancel targets this session's active turn: mark it BEFORE
        // anything is awaited, so the turn's `cancelled` answer (whenever
        // it routes) finds the mark and sends zero decision frames
        // (ADR-0005:49; the guard the permission bridge consults).
        state.arm_cancel_resolution(&session_id.to_string());
        match conn.call(Command::CancelTask { task_id }).await {
            Ok(payload) => {
                let status = payload
                    .get("task")
                    .and_then(|task| task.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                tracing::info!(
                    %session_id,
                    %task_id,
                    status,
                    "session/cancel: drain ack received (CancelTask responded); the \
                     cancel reply follows only now"
                );
                Ok(())
            }
            // The task reached a terminal state before (or during) this
            // cancel — the desired end already holds, so this is the
            // same idempotent ok, not a failure.
            Err(GatewayCallError::Refused { code, message }) if code == "illegal_transition" => {
                tracing::info!(
                    %session_id,
                    %task_id,
                    detail = %message,
                    "session/cancel: task already terminal; idempotent ok"
                );
                Ok(())
            }
            Err(error) => Err(call_error(error)),
        }
    } else {
        // No active turn: `GetSession` proves the session exists
        // (unknown ⇒ typed refusal), then idempotent ok.
        conn.call(Command::GetSession { session_id })
            .await
            .map_err(call_error)?;
        tracing::info!(
            %session_id,
            "session/cancel: no active turn; idempotent ok (no CancelTask issued)"
        );
        Ok(())
    }
}

/// Sends one command frame, mapping a transport failure onto the typed
/// gateway-unavailable error.
async fn send_frame(conn: &mut GatewayConn, command: Command) -> Result<EventId, HandlerError> {
    conn.send(command)
        .await
        .map_err(|error| HandlerError::unavailable(&error))
}

/// The verdict of one settlement `GetTask` read: `Ok(Some(result))`
/// when the turn has a final answer, `Err` for the typed refusal of an
/// ambiguous verification tail, `Ok(None)` to keep streaming — including
/// while a permission park resolves (the ask is journal-driven: a park
/// observed here arms the bounded wait for the `approval_request`
/// frame instead of failing on the snapshot alone).
fn settlement_verdict(
    task: &Value,
    task_id: TaskId,
    trigger: Option<&str>,
    settlement: Option<&str>,
    bridge: &mut PermissionBridge,
) -> Result<Option<Value>, HandlerError> {
    let status = task
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if is_terminal_status(status) {
        return final_prompt_result(task).map(Some);
    }
    if is_approval_parked(status) {
        // Parked on a permission approval: wait for the journal-driven
        // ask (bounded by APPROVAL_REQUEST_GRACE; the orphan fallback
        // fails typed past it) — never hang to the turn deadline, never
        // guess a verdict, never read the ask from this snapshot.
        bridge.on_parked();
        return Ok(None);
    }
    if trigger == Some("verification_finished") || settlement == Some("verification_finished") {
        // The verification tail settled at a non-terminal status:
        // ambiguous, never guessed.
        return Err(HandlerError::turn_failed(
            "ambiguous_task_status",
            format!(
                "task {task_id} settled at non-terminal status {status}; \
                 refusing to guess a verdict"
            ),
        ));
    }
    Ok(None)
}

/// Reads the subscription: forwards `agent_message` journals as
/// `session/update` frames, drives the permission exchange when an
/// `approval_request` journals (announce `tool_call` → send
/// `session/request_permission` → await the client's answer → issue the
/// gateway `Approve` through the single settlement slot → close the tool
/// call → resume streaming), watches the settlement signals, fetches
/// `GetTask` when one fires, and answers with the final response (or a
/// typed error for an ambiguous status / unresolved approval park /
/// persistent resync / transport loss). On `ResyncRequired` it
/// re-subscribes once at the gateway-provided cursor (the ack's replay
/// restores continuity) and fails typed if the subscription overflows
/// again — never a silent gap.
// One narrative: settle → permission exchange → read → dispatch, kept
// together for its ordering proofs.
#[allow(clippy::too_many_lines)]
async fn stream_turn(
    conn: &mut GatewayConn,
    subscribe_id: EventId,
    task_id: TaskId,
    session_id: SessionId,
    emit: &UnboundedSender<Outbound>,
    state: &Arc<SessionState>,
) -> Result<Value, HandlerError> {
    let session_id = session_id.to_string();
    let mut awaiting: Option<EventId> = Some(subscribe_id);
    let mut awaiting_subscribe = true;
    // Latest settlement signal seen but not yet served.
    let mut settlement: Option<String> = None;
    // The signal whose `GetTask` is in flight (its answer decides).
    let mut trigger: Option<String> = None;
    // Re-subscribes performed so far (bounded by MAX_RESUBSCRIBES).
    let mut resyncs: u32 = 0;
    // A settlement read was in flight or armed across a re-subscribe:
    // re-check status right after the replay so the turn cannot stall
    // on a signal the replay did not re-deliver.
    let mut recheck = false;
    // The permission exchange (journal-driven ask → client answer →
    // Approve sequenced around the settlement slot).
    let mut bridge = PermissionBridge::default();
    loop {
        // 1. The decision (grant or refuse): written only when the
        //    single settlement slot is free, so the `Approve`/`Deny`
        //    response can never interleave with an in-flight `GetTask`.
        let deciding = if awaiting.is_none() {
            match &bridge.phase {
                PermissionPhase::Approving {
                    approval_id,
                    tool_call_id,
                } => Some(PendingDecision::Approve {
                    approval_id: approval_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                }),
                PermissionPhase::Denying {
                    approval_id,
                    tool_call_id,
                    reason,
                } => Some(PendingDecision::Deny {
                    approval_id: approval_id.clone(),
                    tool_call_id: tool_call_id.clone(),
                    reason: reason.clone(),
                }),
                _ => None,
            }
        } else {
            None
        };
        if let Some(decision) = deciding {
            match decision {
                PendingDecision::Approve {
                    approval_id,
                    tool_call_id,
                } => {
                    let approval: ApprovalId = approval_id.parse().map_err(|_| {
                        HandlerError::turn_failed(
                            "gateway_payload_invalid",
                            "the journalled approval id does not parse as an approval id",
                        )
                    })?;
                    let approve_id = send_frame(
                        conn,
                        Command::Approve {
                            task_id,
                            approval_id: approval,
                        },
                    )
                    .await?;
                    // The tool call closes only now: the decision
                    // reached the gateway (schema
                    // `ToolCallUpdate.status = completed`).
                    if emit
                        .send(tool_call_status_update(
                            &session_id,
                            &tool_call_id,
                            "completed",
                        ))
                        .is_err()
                    {
                        tracing::warn!("client went away; dropping tool_call_update");
                    }
                    awaiting = Some(approve_id);
                    awaiting_subscribe = false;
                    bridge.phase = PermissionPhase::Sent;
                    tracing::info!(
                        %approval_id,
                        %task_id,
                        "permission granted; Approve issued through the settlement slot"
                    );
                }
                PendingDecision::Deny {
                    approval_id,
                    tool_call_id,
                    reason,
                } => {
                    let approval: ApprovalId = approval_id.parse().map_err(|_| {
                        HandlerError::turn_failed(
                            "gateway_payload_invalid",
                            "the journalled approval id does not parse as an approval id",
                        )
                    })?;
                    let deny_id = send_frame(
                        conn,
                        Command::Deny {
                            task_id,
                            approval_id: approval,
                            reason: reason.clone(),
                        },
                    )
                    .await?;
                    // The tool call closes `failed`: the operation the
                    // client refused must never run (ADR-0005:47).
                    if emit
                        .send(tool_call_status_update(
                            &session_id,
                            &tool_call_id,
                            "failed",
                        ))
                        .is_err()
                    {
                        tracing::warn!("client went away; dropping tool_call_update");
                    }
                    awaiting = Some(deny_id);
                    awaiting_subscribe = false;
                    // Arm the bounded settle wait NOW: a terminal
                    // status may still journal; none ⇒ `refusal` at
                    // the deadline (never the turn deadline).
                    bridge.phase = PermissionPhase::Denied {
                        deadline: Instant::now() + DENY_SETTLE_GRACE,
                    };
                    tracing::info!(
                        %approval_id,
                        %task_id,
                        %reason,
                        "permission refused; Deny issued through the settlement slot; \
                         bounded refusal settle armed"
                    );
                }
            }
            continue;
        }
        // 2. The client's answer, awaited only when no gateway request
        //    is in flight — the exchange never occupies the settlement
        //    slot while it waits on a human.
        if awaiting.is_none() && matches!(bridge.phase, PermissionPhase::Outstanding { .. }) {
            let taken = std::mem::replace(&mut bridge.phase, PermissionPhase::Idle);
            let (receiver, armed, approval_id, tool_call_id) = match taken {
                PermissionPhase::Outstanding {
                    receiver,
                    armed,
                    approval_id,
                    tool_call_id,
                } => (receiver, armed, approval_id, tool_call_id),
                other => {
                    // Defensive: the phase changed under the matches!
                    // guard above — restore it and keep serving.
                    bridge.phase = other;
                    continue;
                }
            };
            // The single validation funnel (M2b): classified before
            // ANY branch can reach the Approve path — reject and
            // invalid shapes both become `Deny`, a dropped slot or an
            // error frame stays a typed refusal, and `cancelled` is
            // decided against the cancel mark: owned ⇒ no decision at
            // all, standalone ⇒ fail-closed `Deny`.
            let raw = receiver.await;
            let cancelled = matches!(
                &raw,
                Ok(Ok(value)) if value.get("outcome").and_then(Value::as_str) == Some("cancelled")
            );
            let cancel_owns = cancelled && state.take_cancel_resolution(&session_id);
            let answer = answer_action(raw, cancel_owns);
            drop(armed); // already routed (or abandoned): disarm is a no-op
            match answer {
                ClientAnswer::Grant => {
                    bridge.phase = PermissionPhase::Approving {
                        approval_id,
                        tool_call_id,
                    };
                }
                ClientAnswer::Deny(reason) => {
                    bridge.phase = PermissionPhase::Denying {
                        approval_id,
                        tool_call_id,
                        reason,
                    };
                }
                ClientAnswer::NoDecision => {
                    // The cancel path owns this exchange: it resolves
                    // the request locally; this turn sends NO Approve
                    // and NO Deny and keeps serving (the gateway's own
                    // terminal journal settles the prompt).
                    tracing::info!(
                        %approval_id,
                        "cancelled answer resolved by session/cancel; no decision issued"
                    );
                }
                ClientAnswer::Unresolved => return Err(approval_parked()),
            }
            continue;
        }
        // 3. Settlement read.
        if awaiting.is_none()
            && let Some(kind) = settlement.take()
        {
            trigger = Some(kind);
            awaiting = Some(send_frame(conn, Command::GetTask { task_id }).await?);
            awaiting_subscribe = false;
        }
        // 4. Read the next gateway frame. While a park waits for its
        //    `approval_request` frame the read is bounded by the orphan
        //    grace, so a park that never carries an ask fails typed
        //    instead of hanging (the buffered ask still wins: the poll
        //    runs before the timer); while a written `Deny` waits for a
        //    terminal status the read is bounded by the deny grace, so
        //    a gateway that journals no terminal status settles the
        //    prompt `refusal` instead of hanging to the turn deadline.
        let frame = match read_bound(&bridge.phase) {
            Some((deadline, expiry)) => {
                let left = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(left, conn.read_frame()).await {
                    Ok(frame) => frame.map_err(|error| HandlerError::unavailable(&error))?,
                    Err(_) => {
                        return match expiry {
                            ReadExpiry::Orphan => Err(approval_parked()),
                            ReadExpiry::DenySettle => {
                                tracing::info!(
                                    %task_id,
                                    "deny grace elapsed with no terminal status; \
                                     settling the prompt refusal"
                                );
                                Ok(refusal_prompt_result())
                            }
                        };
                    }
                }
            }
            None => conn
                .read_frame()
                .await
                .map_err(|error| HandlerError::unavailable(&error))?,
        };
        match frame {
            ServerFrame::Response(response) if Some(response.request_id) == awaiting => {
                awaiting = None;
                tachyon_protocol::check_version(response.protocol_version).map_err(|error| {
                    HandlerError::unavailable(&GatewayUnavailable::new(format!(
                        "gateway protocol version: {error}"
                    )))
                })?;
                let payload = match response.result {
                    tachyon_protocol::CommandResult::Ok { payload } => payload,
                    tachyon_protocol::CommandResult::Err { code, message } => {
                        return Err(HandlerError::refused(code, message));
                    }
                };
                if awaiting_subscribe {
                    awaiting_subscribe = false;
                    process_replay(
                        &payload,
                        &session_id,
                        emit,
                        &mut settlement,
                        &mut bridge,
                        state,
                    )?;
                    if std::mem::take(&mut recheck) {
                        // The settlement read that was in flight when the
                        // subscription dropped: re-issue it after the replay.
                        if trigger.is_none() {
                            trigger = Some("status".to_owned());
                        }
                        awaiting = Some(send_frame(conn, Command::GetTask { task_id }).await?);
                        awaiting_subscribe = false;
                    }
                } else {
                    let task = payload.get("task").cloned().unwrap_or(Value::Null);
                    if let Some(result) = settlement_verdict(
                        &task,
                        task_id,
                        trigger.as_deref(),
                        settlement.as_deref(),
                        &mut bridge,
                    )? {
                        return Ok(result);
                    }
                    // A non-terminal bounce (including the benign
                    // `WaitingApproval → Executing` after our Approve,
                    // whose response payload this may be): keep
                    // streaming; any signal that arrived mid-round-trip
                    // serves next loop.
                    if matches!(bridge.phase, PermissionPhase::Sent) {
                        // The `Approve` response just settled benignly:
                        // the exchange is over, a later park may start anew.
                        bridge.phase = PermissionPhase::Idle;
                    }
                }
            }
            ServerFrame::Response(_) => {
                if matches!(bridge.phase, PermissionPhase::Sent) {
                    // The `Approve` response whose slot a re-subscribe
                    // re-armed: release the exchange so a later park in
                    // this turn can start anew.
                    bridge.phase = PermissionPhase::Idle;
                }
                tracing::debug!("ignoring a response for another request id mid-turn");
            }
            ServerFrame::Event(envelope) => match envelope.event {
                GatewayEvent::Journal { kind, payload } => {
                    handle_journal(
                        &kind,
                        &payload,
                        &session_id,
                        emit,
                        &mut settlement,
                        &mut bridge,
                        state,
                    )?;
                }
                GatewayEvent::ResyncRequired { after_seq, .. } => {
                    resyncs += 1;
                    if resyncs > MAX_RESUBSCRIBES {
                        // Persistent failure: typed, never a silent gap.
                        return Err(HandlerError::turn_failed(
                            "resync_required",
                            format!(
                                "subscription overflowed again after {MAX_RESUBSCRIBES} \
                                 re-subscribe; failing typed rather than silently \
                                 truncating the stream"
                            ),
                        ));
                    }
                    recheck = settlement.is_some() || (awaiting.is_some() && !awaiting_subscribe);
                    if let Some(kind) = settlement.take() {
                        trigger = Some(kind);
                    }
                    awaiting =
                        Some(send_frame(conn, Command::Subscribe { task_id, after_seq }).await?);
                    awaiting_subscribe = true;
                    tracing::info!(
                        %task_id,
                        after_seq,
                        resyncs,
                        "subscription overflowed; re-subscribing at the gateway cursor"
                    );
                }
                other => {
                    tracing::debug!(event = ?other, "ignoring a non-journal gateway event");
                }
            },
        }
    }
}

/// Consumes the subscribe acknowledgement's replayed journal rows
/// (payloads arrive as JSON strings there, live frames as objects).
/// A replayed `approval_request` produces the same request emission as
/// a live one — the ask is never read from a `GetTask` snapshot.
fn process_replay(
    payload: &Value,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
    settlement: &mut Option<String>,
    bridge: &mut PermissionBridge,
    state: &Arc<SessionState>,
) -> Result<(), HandlerError> {
    let Some(rows) = payload.get("events").and_then(Value::as_array) else {
        return Ok(());
    };
    for row in rows {
        let Some(kind) = row.get("kind").and_then(Value::as_str) else {
            continue;
        };
        let value = match row.get("payload") {
            Some(Value::String(text)) => serde_json::from_str::<Value>(text).unwrap_or(Value::Null),
            Some(other) => other.clone(),
            None => Value::Null,
        };
        handle_journal(kind, &value, session_id, emit, settlement, bridge, state)?;
    }
    Ok(())
}

/// Forwards one journalled event (kind + raw payload) as ACP output:
/// `agent_message` → a `session/update` chunk, `approval_request` →
/// the permission exchange (`tool_call` announcement + request frame),
/// settlement kinds arm the `GetTask` read, every other kind is
/// omitted.
fn handle_journal(
    kind: &str,
    payload: &Value,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
    settlement: &mut Option<String>,
    bridge: &mut PermissionBridge,
    state: &Arc<SessionState>,
) -> Result<(), HandlerError> {
    if let Some(update) = agent_chunk(kind, payload) {
        let params = json!({ "sessionId": session_id, "update": update });
        if emit
            .send(Outbound::notification("session/update", params))
            .is_err()
        {
            tracing::warn!("client went away; dropping session/update chunk");
        }
    }
    if kind == "approval_request" {
        bridge.ask(session_id, payload, emit, state)?;
    }
    if is_settlement_signal(kind) {
        *settlement = Some(kind.to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::{Value, json};
    use tachyon_types::TaskId;
    use tokio::sync::mpsc;

    use super::{
        APPROVAL_REQUEST_GRACE, DENY_REASON_REJECTED, DENY_SETTLE_GRACE, ClientAnswer, HandlerError,
        OutboundRequests, PermissionAnswer, PermissionBridge, PermissionPhase, ReadExpiry,
        SessionState, agent_chunk, answer_action, approval_ask, approval_parked,
        classify_permission_answer, final_prompt_result, is_allow_once, is_approval_parked,
        is_reject_once, is_settlement_signal, is_terminal_status, parse_cancel, parse_prompt,
        parse_session_new, permission_request_params, read_bound, refusal_prompt_result,
        settlement_verdict, stop_reason, tool_call_announcement, tool_call_status_update,
    };
    use crate::codec::{
        INVALID_PARAMS, InboundResponse, Outbound, RpcId, TURN_CONFLICT, TURN_FAILED,
    };

    #[test]
    fn only_status_and_verification_events_can_settle_a_turn() {
        assert!(is_settlement_signal("status"));
        assert!(is_settlement_signal("verification_finished"));
        for kind in [
            "created",
            "stage",
            "agent_message",
            "changed_files",
            "verification_started",
            "verification_configured",
            "workspace_pinned",
        ] {
            assert!(
                !is_settlement_signal(kind),
                "{kind} must not trigger a settlement read"
            );
        }
    }

    #[test]
    fn stop_reason_maps_the_three_terminal_statuses() {
        assert_eq!(stop_reason("Completed"), Ok("end_turn"));
        assert_eq!(stop_reason("Cancelled"), Ok("cancelled"));
        // `Failed` has no schema-legal stopReason (ACP schema-v1.23.0:
        // end_turn|max_tokens|max_turn_requests|refusal|cancelled), so
        // it answers a typed -32004 `task_failed` error — never a
        // success frame carrying a schema-divergent `"error"` verdict.
        let failed = stop_reason("Failed").expect_err("Failed is not a verdict");
        assert_eq!(failed.code, TURN_FAILED);
        assert_eq!(failed.data, json!("task_failed"));
        assert!(
            failed.message.contains("no schema-legal stopReason"),
            "the message says why: {}",
            failed.message
        );
    }

    #[test]
    fn stop_reason_rejects_every_non_terminal_or_unknown_status() {
        for status in [
            "Created",
            "Routing",
            "Planning",
            "Executing",
            "Verifying",
            "WaitingApproval",
            "Paused",
            "Recovering",
            "",
            "completed",
            "bogus",
        ] {
            let error = match stop_reason(status) {
                Err(error) => error,
                Ok(stop) => panic!("{status} must never map to a stopReason, got {stop}"),
            };
            assert_eq!(error.code, TURN_FAILED, "{status}");
            assert_eq!(
                error.data,
                json!("ambiguous_task_status"),
                "{status} must never map to a stopReason"
            );
            assert!(!is_terminal_status(status), "{status} must not be terminal");
        }
        for status in ["Completed", "Cancelled", "Failed"] {
            assert!(is_terminal_status(status), "{status} is terminal");
        }
    }

    #[test]
    fn unknown_status_is_a_typed_error_not_a_guess() {
        let error = final_prompt_result(&json!({"status": "Executing", "conversation": []}))
            .expect_err("non-terminal status must not produce a verdict");
        assert_eq!(error.code, TURN_FAILED);
        assert_eq!(error.data, json!("ambiguous_task_status"));
        let missing = final_prompt_result(&json!({"conversation": []}))
            .expect_err("a snapshot without status is ambiguous");
        assert_eq!(missing.code, TURN_FAILED);
        assert_eq!(missing.data, json!("ambiguous_task_status"));
    }

    #[test]
    fn final_result_carries_the_conversation_tail_as_text_content() {
        let task = json!({
            "status": "Completed",
            "conversation": [
                {"speaker": "user", "content": "do the thing"},
                {"speaker": "agent", "content": "first answer"},
                {"speaker": "user", "content": "more"},
                {"speaker": "agent", "content": "final answer"},
            ]
        });
        let result = final_prompt_result(&task).expect("terminal status maps");
        assert_eq!(
            result,
            json!({
                "stopReason": "end_turn",
                "content": [{"type": "text", "text": "final answer"}],
            })
        );
        let cancelled = final_prompt_result(&json!({
            "status": "Cancelled",
            "conversation": [{"speaker": "agent", "content": "stopped early"}]
        }))
        .expect("cancelled maps");
        assert_eq!(cancelled["stopReason"], "cancelled");
        // `Failed` is a typed error frame, never a success frame with a
        // schema-divergent `"error"` stopReason.
        let failed = final_prompt_result(&json!({
            "status": "Failed",
            "conversation": []
        }))
        .expect_err("Failed must not produce a verdict");
        assert_eq!(failed.code, TURN_FAILED);
        assert_eq!(failed.data, json!("task_failed"));
    }

    #[test]
    fn only_agent_message_journals_become_chunks() {
        let agent = json!({"t": "agent_message", "v": {"message": "hello there"}});
        assert_eq!(
            agent_chunk("agent_message", &agent),
            Some(json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "hello there"},
            }))
        );
        // Raw (untagged) payloads still map — older journal rows.
        assert_eq!(
            agent_chunk("agent_message", &json!({"message": "raw"})),
            Some(json!({
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "raw"},
            }))
        );
        for kind in [
            "created",
            "status",
            "stage",
            "evidence_summary",
            "changed_files",
            "verification_configured",
            "verification_started",
            "verification_finished",
            "approval_request",
            "message",
            "workspace_pinned",
        ] {
            assert_eq!(
                agent_chunk(
                    kind,
                    &json!({"t": kind, "v": {"message": "decoy", "text": "decoy"}})
                ),
                None,
                "{kind} must never be mislabeled as a chunk"
            );
        }
        // An agent_message without a usable text payload maps to nothing
        // rather than an empty fake chunk.
        assert_eq!(agent_chunk("agent_message", &json!({"v": {}})), None);
        assert_eq!(
            agent_chunk("agent_message", &json!({"v": {"message": 42}})),
            None
        );
    }

    #[test]
    fn session_new_requires_an_absolute_cwd() {
        let ok = parse_session_new(Some(&json!({"cwd": "/tmp"}))).expect("absolute cwd accepted");
        assert_eq!(ok.cwd, "/tmp");
        for params in [
            json!({"cwd": "relative/dir"}),
            json!({"cwd": ""}),
            json!({"cwd": 42}),
            json!({}),
            json!([]),
            json!("nope"),
        ] {
            let error = parse_session_new(Some(&params)).expect_err("bad cwd shape rejected");
            assert_eq!(
                error.code, INVALID_PARAMS,
                "{params} must be rejected as invalid params"
            );
        }
        let relative = parse_session_new(Some(&json!({"cwd": "relative/dir"})))
            .expect_err("relative cwd is rejected");
        assert_eq!(relative.data, json!("cwd_not_absolute"));
        let missing = parse_session_new(Some(&json!({}))).expect_err("cwd is required");
        assert_eq!(missing.data, json!("invalid_params"));
        assert!(
            parse_session_new(None).is_err(),
            "absent params are invalid"
        );
    }

    #[test]
    fn session_new_refuses_only_non_empty_mcp_servers() {
        // Absent and empty are accepted (MCP-at-setup is a later slice).
        parse_session_new(Some(&json!({"cwd": "/tmp"}))).expect("absent mcpServers ok");
        parse_session_new(Some(&json!({"cwd": "/tmp", "mcpServers": []})))
            .expect("empty mcpServers ok");
        let error = parse_session_new(Some(&json!({
            "cwd": "/tmp",
            "mcpServers": [{"transport": {"type": "stdio"}, "command": "mcp-server"}]
        })))
        .expect_err("non-empty mcpServers is refused");
        assert_eq!(error.code, INVALID_PARAMS);
        assert_eq!(error.data, json!("mcp_servers_unsupported"));
        let not_an_array = parse_session_new(Some(&json!({"cwd": "/tmp", "mcpServers": {}})))
            .expect_err("mcpServers must be an array");
        assert_eq!(not_an_array.data, json!("invalid_params"));
    }

    #[test]
    fn prompt_requires_text_and_a_parseable_session_id() {
        let params = parse_prompt(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "prompt": [{"type": "text", "text": "hello"}],
        })))
        .expect("valid prompt");
        assert_eq!(params.objective, "hello");
        assert_eq!(
            params.session_id.to_string(),
            "01990f9e-1111-7000-8000-000000000000"
        );

        let bad_id = parse_prompt(Some(&json!({
            "sessionId": "not-a-uuid",
            "prompt": [{"type": "text", "text": "hello"}],
        })))
        .expect_err("sessionId must parse");
        assert_eq!(bad_id.data, json!("invalid_session_id"));

        let missing = parse_prompt(Some(&json!({"prompt": []}))).expect_err("sessionId required");
        assert_eq!(missing.code, INVALID_PARAMS);

        let unsupported = parse_prompt(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "prompt": [{"type": "resource_link", "uri": "file:///tmp/a"}],
        })))
        .expect_err("non-text content is refused this slice");
        assert_eq!(unsupported.data, json!("unsupported_prompt_content"));
    }

    #[test]
    fn empty_prompt_text_is_rejected_before_any_gateway_call() {
        for prompt in [
            json!([]),
            json!([{"type": "text", "text": ""}]),
            json!([{"type": "text", "text": "   \n\t "}]),
            json!([{"type": "text", "text": ""}, {"type": "text", "text": " "}]),
        ] {
            let error = parse_prompt(Some(&json!({
                "sessionId": "01990f9e-1111-7000-8000-000000000000",
                "prompt": prompt,
            })))
            .expect_err("empty text must be rejected");
            assert_eq!(error.code, INVALID_PARAMS, "{prompt}");
            assert_eq!(error.data, json!("empty_prompt"), "{prompt}");
        }
        // Multiple text blocks concatenate as one objective.
        let joined = parse_prompt(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "prompt": [{"type": "text", "text": "line one"}, {"type": "text", "text": "line two"}],
        })))
        .expect("two text blocks");
        assert_eq!(joined.objective, "line one\nline two");
    }

    #[test]
    fn session_cancel_requires_a_parseable_session_id() {
        let ok = parse_cancel(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
        })))
        .expect("a valid session id parses");
        assert_eq!(
            ok.to_string(),
            "01990f9e-1111-7000-8000-000000000000",
            "identity round-trip: the cancel targets exactly the given id"
        );
        for params in [
            json!({"sessionId": "not-a-uuid"}),
            json!({}),
            json!({"sessionId": 42}),
            json!([]),
            json!("nope"),
        ] {
            let error = parse_cancel(Some(&params)).expect_err("bad shape rejected");
            assert_eq!(error.code, INVALID_PARAMS, "{params}");
        }
        assert!(parse_cancel(None).is_err(), "absent params are invalid");
        let bad_id =
            parse_cancel(Some(&json!({"sessionId": "nope"}))).expect_err("sessionId must parse");
        assert_eq!(bad_id.data, json!("invalid_session_id"));
        let missing = parse_cancel(Some(&json!({}))).expect_err("sessionId is required");
        assert_eq!(missing.data, json!("invalid_params"));
    }

    #[test]
    fn approval_parked_is_typed_and_only_for_the_park_status() {
        let error = approval_parked();
        assert_eq!(error.code, TURN_FAILED);
        assert_eq!(error.data, json!("approval_required"));
        assert!(
            error.message.contains("could not resolve"),
            "the message says why the park cannot resolve: {}",
            error.message
        );
        assert!(is_approval_parked("WaitingApproval"));
        for status in [
            "Created",
            "Routing",
            "Planning",
            "Executing",
            "Verifying",
            "Paused",
            "Recovering",
            "Completed",
            "Cancelled",
            "Failed",
            "waitingapproval",
        ] {
            assert!(
                !is_approval_parked(status),
                "{status} must never take the approval-park branch"
            );
        }
    }

    #[test]
    fn sequential_turn_guard_refuses_overlap_and_releases() {
        let state = std::sync::Arc::new(SessionState::default());
        let first = state
            .try_acquire_turn("01990f9e-1111-7000-8000-000000000000")
            .expect("first turn acquires");
        let conflict = state
            .try_acquire_turn("01990f9e-1111-7000-8000-000000000000")
            .expect_err("second overlapping turn is refused");
        assert_eq!(conflict.code, TURN_CONFLICT);
        assert_eq!(conflict.data, json!("turn_in_progress"));
        // A different session is unaffected.
        state
            .try_acquire_turn("01990f9e-2222-7000-8000-000000000000")
            .expect("other sessions are independent");
        drop(first);
        state
            .try_acquire_turn("01990f9e-1111-7000-8000-000000000000")
            .expect("released slot can be taken again");
    }

    #[test]
    fn keys_are_fresh_per_call_and_reused_per_call_id() {
        let state = SessionState::default();
        let first_id = RpcId::Number(serde_json::Number::from(1));
        let (first, reused) = state.key_for(&first_id);
        assert!(!reused, "a new call mints a fresh key");
        let (again, reused) = state.key_for(&first_id);
        assert!(reused, "the same call id reuses its key");
        assert_eq!(first, again, "byte-identical key on retry");
        let second_id = RpcId::Number(serde_json::Number::from(2));
        let (second, reused) = state.key_for(&second_id);
        assert!(!reused, "a different call is a different call");
        assert_ne!(first, second, "two calls never share a key");
        assert!(first.len() <= 128, "gateway idempotency key bound");
        assert!(!first.is_empty());
    }

    #[test]
    fn handler_errors_carry_stable_wire_codes() {
        let refused = HandlerError::refused("workspace_not_found", "gone");
        assert_eq!(refused.code, crate::codec::GATEWAY_REFUSED);
        assert_eq!(refused.data, json!("workspace_not_found"));
        assert_eq!(refused.message, "workspace_not_found: gone");
        let failed = HandlerError::turn_failed("turn_timed_out", "too slow");
        assert_eq!(failed.code, TURN_FAILED);
        assert_eq!(failed.data, json!("turn_timed_out"));
    }

    /// The `approval_request` payload shape (t/v-tagged `StateEvent`)
    /// yields `(approval id, title)`; an untagged legacy row still
    /// parses; a payload without an id yields `None` — the bridge never
    /// invents an approval from a snapshot (ADR-0005:47).
    #[test]
    fn approval_ask_reads_only_the_journalled_ask() {
        let tagged = json!({
            "t": "ApprovalRequest",
            "v": {"request": {
                "id": "01990f9e-5555-7000-8000-000000000000",
                "capability": "mutation.patch",
                "scope": "workspace",
                "operation_hash": "abc123",
                "summary": "Apply patch to src/lib.rs",
            }},
        });
        assert_eq!(
            approval_ask(&tagged),
            Some((
                "01990f9e-5555-7000-8000-000000000000".to_owned(),
                "Apply patch to src/lib.rs".to_owned(),
            ))
        );
        // Untagged (legacy) row: same extraction, capability fallback.
        assert_eq!(
            approval_ask(&json!({"id": "x-1", "summary": "do it"})),
            Some(("x-1".to_owned(), "do it".to_owned()))
        );
        assert_eq!(
            approval_ask(&json!({"id": "x-2", "capability": "fs.write"})),
            Some(("x-2".to_owned(), "fs.write".to_owned()))
        );
        // No id ⇒ no ask.
        assert_eq!(approval_ask(&json!({"summary": "no id"})), None);
        assert_eq!(
            approval_ask(&json!({"t": "ApprovalRequest", "v": {"request": {}}})),
            None
        );
        assert_eq!(approval_ask(&Value::Null), None);
    }

    /// The outgoing permission frames are byte-pinned against the ACP
    /// schema-v1.23.0 shapes: `ToolCall` (`toolCallId` + `title`
    /// required), `ToolCallUpdate` (`toolCallId` + a legal status), and
    /// `RequestPermissionRequest` params carrying EXACTLY the two
    /// one-shot options ADR-0005:48 allows.
    #[test]
    fn permission_frames_are_golden_pinned_to_the_schema_shapes() {
        let session = "01990f9e-1111-7000-8000-000000000000";
        let tool_call = "01990f9e-5555-7000-8000-000000000000";
        let title = "Apply patch to src/lib.rs";

        let announce = tool_call_announcement(session, tool_call, title);
        assert_eq!(
            announce.to_line(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","update":{"sessionUpdate":"tool_call","title":"Apply patch to src/lib.rs","toolCallId":"01990f9e-5555-7000-8000-000000000000"}}}"#
        );

        let close = tool_call_status_update(session, tool_call, "completed");
        assert_eq!(
            close.to_line(),
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","update":{"sessionUpdate":"tool_call_update","status":"completed","toolCallId":"01990f9e-5555-7000-8000-000000000000"}}}"#
        );

        let params = permission_request_params(session, tool_call, title);
        assert_eq!(
            params,
            json!({
                "sessionId": session,
                "toolCall": {"toolCallId": tool_call, "title": title},
                "options": [
                    {"optionId": "allow_once", "name": "Allow once", "kind": "allow_once"},
                    {"optionId": "reject_once", "name": "Reject once", "kind": "reject_once"},
                ],
            })
        );
        // Schema contract: required keys present, exactly two options,
        // each a full `PermissionOption` (optionId + name + kind), and
        // never a persistent-grant kind (ADR-0005:48).
        assert!(params.get("sessionId").is_some_and(Value::is_string));
        assert!(params.get("toolCall").is_some_and(Value::is_object));
        let options = params["options"].as_array().expect("options array");
        assert_eq!(options.len(), 2, "exactly two one-shot options");
        let offered: Vec<&str> = options
            .iter()
            .map(|option| option["optionId"].as_str().expect("optionId"))
            .collect();
        assert_eq!(offered, ["allow_once", "reject_once"]);
        for option in options {
            assert!(option.get("optionId").is_some_and(Value::is_string));
            assert!(option.get("name").is_some_and(Value::is_string));
            assert!(option.get("kind").is_some_and(Value::is_string));
            let kind = option["kind"].as_str().expect("kind");
            assert!(
                matches!(kind, "allow_once" | "reject_once"),
                "only one-shot kinds are offered, got {kind}"
            );
        }
        assert!(
            !params.to_string().contains("always"),
            "no persistent-grant option may ever appear: {params}"
        );
    }

    /// Adapter-minted outbound ids come from a dedicated NEGATIVE
    /// counter (disjoint from client ids and `Peer`'s positive request
    /// counter), and a response only ever reaches the slot armed for
    /// ITS id: an unsolicited response routes nowhere.
    #[tokio::test]
    async fn outbound_request_slots_arm_negative_ids_and_route_only_theirs() {
        let requests = OutboundRequests::default();
        let (first, mut first_rx) = requests.arm();
        let (second, mut second_rx) = requests.arm();
        assert_eq!(first, RpcId::Number((-1).into()));
        assert_eq!(second, RpcId::Number((-2).into()));
        assert_ne!(first, second, "every request mints a fresh id");
        assert_eq!(requests.armed(), 2);

        // Unsolicited response: no slot ⇒ ignored, nothing routed.
        let unsolicited = InboundResponse {
            id: RpcId::Number(99.into()),
            payload: Ok(json!("junk")),
        };
        assert!(!requests.route(&unsolicited), "unsolicited must not route");
        assert_eq!(requests.armed(), 2);
        assert!(
            first_rx.try_recv().is_err(),
            "an unsolicited response may never reach a waiter"
        );

        // The right id routes to ITS waiter (arm order ≠ route order).
        let answer = InboundResponse {
            id: second.clone(),
            payload: Ok(json!("granted")),
        };
        assert!(requests.route(&answer));
        assert_eq!(second_rx.try_recv().unwrap().unwrap(), json!("granted"));
        assert_eq!(requests.armed(), 1);
        assert!(
            first_rx.try_recv().is_err(),
            "the other waiter is untouched"
        );

        // EOF clearing: every armed slot closes and its waiter wakes.
        requests.clear();
        assert_eq!(requests.armed(), 0);
        assert!(first_rx.try_recv().is_err());
    }

    /// A park seen on a settlement read arms the bounded wait for the
    /// journal-driven ask instead of failing typed; a request already
    /// in flight (or a written Approve) keeps streaming untouched, and
    /// terminal / ambiguous verdicts keep their existing meaning.
    #[test]
    fn settlement_verdict_waits_for_the_ask_on_a_park() {
        let task_id: TaskId = "01990f9e-4444-7000-8000-000000000000"
            .parse()
            .expect("task id");
        let parked = json!({"status": "WaitingApproval", "conversation": []});

        let mut bridge = PermissionBridge::default();
        assert_eq!(
            settlement_verdict(&parked, task_id, None, None, &mut bridge).expect("park is benign"),
            None
        );
        let PermissionPhase::AwaitingRequest { deadline } = bridge.phase else {
            panic!("an observed park arms the bounded wait: {:?}", bridge.phase);
        };
        let now = std::time::Instant::now();
        assert!(
            deadline > now && deadline <= now + APPROVAL_REQUEST_GRACE,
            "the wait is bounded by the orphan grace"
        );

        // An exchange already in flight: repeat observations are benign.
        let state = Arc::new(SessionState::default());
        let (id, receiver) = state.requests().arm();
        bridge.phase = PermissionPhase::Outstanding {
            receiver,
            armed: super::ArmedRequest {
                state: Arc::clone(&state),
                id,
            },
            approval_id: "a".to_owned(),
            tool_call_id: "t".to_owned(),
        };
        assert_eq!(
            settlement_verdict(&parked, task_id, None, None, &mut bridge).expect("in flight"),
            None
        );
        assert!(matches!(bridge.phase, PermissionPhase::Outstanding { .. }));

        // A written Approve keeps the phase (its own response payload
        // may show WaitingApproval for a beat) — never a re-ask.
        bridge.phase = PermissionPhase::Sent;
        assert_eq!(
            settlement_verdict(&parked, task_id, None, None, &mut bridge).expect("sent"),
            None
        );
        assert!(matches!(bridge.phase, PermissionPhase::Sent));

        // Existing verdicts unchanged: terminal answers, the
        // verification tail at a non-terminal status stays ambiguous.
        let completed = json!({"status": "Completed", "conversation": []});
        let mut fresh = PermissionBridge::default();
        assert!(
            settlement_verdict(&completed, task_id, None, None, &mut fresh)
                .expect("terminal answers")
                .is_some()
        );
        let executing = json!({"status": "Executing", "conversation": []});
        let error = settlement_verdict(
            &executing,
            task_id,
            Some("verification_finished"),
            None,
            &mut fresh,
        )
        .expect_err("ambiguous tail stays typed");
        assert_eq!(error.data, json!("ambiguous_task_status"));
    }

    /// The strict allow gate: only `selected` + the offered
    /// `allow_once` grants; every other shape — reject, unknown option,
    /// cancelled, malformed — is NOT a grant.
    #[test]
    fn only_selected_allow_once_grants() {
        assert!(is_allow_once(
            &json!({"outcome": "selected", "optionId": "allow_once"})
        ));
        for response in [
            json!({"outcome": "selected", "optionId": "reject_once"}),
            json!({"outcome": "selected", "optionId": "allow_always"}),
            json!({"outcome": "selected", "optionId": "nope"}),
            json!({"outcome": "selected"}),
            json!({"optionId": "allow_once"}),
            json!({"outcome": "cancelled"}),
            json!({"outcome": "rejected"}),
            json!("allow_once"),
            json!(null),
            json!(42),
        ] {
            assert!(
                !is_allow_once(&response),
                "{response} must never grant an approval"
            );
        }
    }

    /// The ask emission: `tool_call` announcement FIRST, then the
    /// `session/request_permission` frame — both queued on the writer
    /// channel (stdout order holds), the reply slot armed BEFORE the
    /// request is queued, phase `Outstanding` — and a re-delivered row
    /// never asks twice.
    #[tokio::test]
    async fn bridge_ask_emits_tool_call_then_request_and_arms_once() {
        let state = Arc::new(SessionState::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut bridge = PermissionBridge::default();
        let session = "01990f9e-1111-7000-8000-000000000000";
        let tool_call = "01990f9e-5555-7000-8000-000000000000";
        let ask = json!({
            "t": "ApprovalRequest",
            "v": {"request": {
                "id": tool_call,
                "capability": "mutation.patch",
                "scope": "workspace",
                "operation_hash": "abc",
                "summary": "Apply patch to src/lib.rs",
            }},
        });

        bridge
            .ask(session, &ask, &tx, &state)
            .expect("a well-formed ask emits");
        let announcement = rx.try_recv().expect("tool_call queued first");
        let request = rx.try_recv().expect("request follows");
        assert!(rx.try_recv().is_err(), "exactly two frames per ask");

        let Outbound::Notification { method, params } = announcement else {
            panic!("the announcement is a notification: {announcement:?}");
        };
        assert_eq!(method, "session/update");
        assert_eq!(params["sessionId"], session);
        assert_eq!(params["update"]["sessionUpdate"], "tool_call");
        assert_eq!(params["update"]["toolCallId"], tool_call);
        assert_eq!(params["update"]["title"], "Apply patch to src/lib.rs");

        let Outbound::Request { id, method, params } = request else {
            panic!("the ask is an id-bearing request: {request:?}");
        };
        assert_eq!(method, "session/request_permission");
        assert_eq!(id, RpcId::Number((-1).into()), "first minted id");
        assert_eq!(
            params,
            permission_request_params(session, tool_call, "Apply patch to src/lib.rs")
        );
        assert!(matches!(bridge.phase, PermissionPhase::Outstanding { .. }));
        assert_eq!(state.requests().armed(), 1, "the reply slot is armed");

        // A re-delivered row (replay after a re-subscribe) never asks twice.
        bridge
            .ask(session, &ask, &tx, &state)
            .expect("a duplicate is a silent no-op");
        assert!(rx.try_recv().is_err());
        assert_eq!(state.requests().armed(), 1, "no second slot");

        // A row without an approval id: no frames, no phase change.
        let mut fresh = PermissionBridge::default();
        fresh
            .ask(
                session,
                &json!({"t": "ApprovalRequest", "v": {"request": {}}}),
                &tx,
                &state,
            )
            .expect("an id-less ask is refused without error");
        assert!(rx.try_recv().is_err());
        assert!(matches!(fresh.phase, PermissionPhase::Idle));
    }

    /// N1a1 (deny settlement): a written `Deny` phase carries a read
    /// bound that expires into the `refusal` verdict — no terminal
    /// status is needed to build it; the orphan branch keeps its own
    /// typed meaning; the human-paced and free phases carry no bound.
    #[test]
    fn deny_grace_expiry_defaults_to_refusal_without_a_terminal_status() {
        let now = std::time::Instant::now();
        let denied = PermissionPhase::Denied { deadline: now };
        let (deadline, expiry) = read_bound(&denied).expect("a written deny is bounded");
        assert_eq!(expiry, ReadExpiry::DenySettle);
        assert!(
            deadline <= now + DENY_SETTLE_GRACE,
            "the settle wait never exceeds the deny grace"
        );
        assert_eq!(
            refusal_prompt_result(),
            json!({"stopReason": "refusal", "content": []}),
            "the refusal verdict needs no terminal status"
        );

        // The orphan park keeps its own branch (typed error, not a
        // verdict), and phases with no bound stay free.
        let orphan = PermissionPhase::AwaitingRequest { deadline: now };
        assert_eq!(read_bound(&orphan).expect("parked ask is bounded").1, ReadExpiry::Orphan);
        assert_eq!(read_bound(&PermissionPhase::Idle), None);
        assert_eq!(read_bound(&PermissionPhase::Sent), None);
    }

    /// N1a2 (deny settlement): the settle default is exactly `refusal`
    /// — never `turn_timed_out` (an error `data` marker, never a
    /// `stopReason`) and never a guessed `end_turn`; and no terminal
    /// status ever maps to `refusal`, so the verdict has exactly one
    /// source: the deny grace default.
    #[test]
    fn refusal_never_maps_to_turn_timed_out_or_end_turn() {
        let result = refusal_prompt_result();
        assert_eq!(result["stopReason"], "refusal");
        assert_ne!(result["stopReason"], "end_turn");
        assert_ne!(result["stopReason"], "turn_timed_out");
        assert!(result["content"].as_array().is_some_and(Vec::is_empty));
        for status in [
            "Completed",
            "Cancelled",
            "Failed",
            "WaitingApproval",
            "Executing",
        ] {
            assert_ne!(
                stop_reason(status).ok(),
                Some("refusal"),
                "{status} must never become the deny-settle verdict"
            );
        }
        let timeout = HandlerError::turn_failed("turn_timed_out", "deadline");
        assert_eq!(timeout.data, json!("turn_timed_out"));
        assert_eq!(DENY_SETTLE_GRACE, std::time::Duration::from_secs(5));
        assert_eq!(
            DENY_REASON_REJECTED,
            "the ACP client rejected the permission request (reject_once)"
        );
    }

    /// The strict reject gate (the symmetric twin of the allow gate):
    /// only `selected` + the offered `reject_once` classifies as the
    /// explicit refuse answer — every other shape is not this gate's
    /// grant-adjacent decision.
    #[test]
    fn only_selected_reject_once_refuses() {
        assert!(is_reject_once(
            &json!({"outcome": "selected", "optionId": "reject_once"})
        ));
        for response in [
            json!({"outcome": "selected", "optionId": "allow_once"}),
            json!({"outcome": "selected", "optionId": "reject_always"}),
            json!({"outcome": "selected"}),
            json!({"outcome": "cancelled"}),
            json!({"optionId": "reject_once"}),
            json!(null),
        ] {
            assert!(
                !is_reject_once(&response),
                "{response} must never classify as reject_once"
            );
        }
    }

    /// N2a1 / S2 (fail-closed validation): every invalid response
    /// shape — unknown optionId, outcome neither `selected` nor
    /// `cancelled`, `selected` without an optionId, non-object
    /// response — classifies `Invalid` and funnels into a `Deny`
    /// whose reason names the ACP client: NEVER a grant, never
    /// Approve (ADR-0005:47, the operation must never run). The two
    /// offered shapes and `cancelled` stay valid; only `allow_once`
    /// ever grants.
    #[test]
    fn invalid_responses_fail_closed_as_deny() {
        let invalid = [
            // Unknown/unoffered optionId (including the persistent
            // kinds ADR-0005:48 never offers).
            json!({"outcome": "selected", "optionId": "allow_always"}),
            json!({"outcome": "selected", "optionId": "reject_always"}),
            json!({"outcome": "selected", "optionId": "nope"}),
            // Outcome neither `selected` nor `cancelled`.
            json!({"outcome": "rejected"}),
            json!({"outcome": "dismissed"}),
            json!({"outcome": "allow_once"}),
            // `selected` without an optionId.
            json!({"outcome": "selected"}),
            json!({"optionId": "allow_once"}),
            json!({}),
            // Non-object responses.
            json!("allow_once"),
            json!(null),
            json!(42),
            json!([]),
        ];
        for response in invalid {
            assert!(
                matches!(
                    classify_permission_answer(&response),
                    PermissionAnswer::Invalid { .. }
                ),
                "{response} must classify invalid"
            );
            let action = answer_action(Ok(Ok(response.clone())), false);
            match action {
                ClientAnswer::Deny(reason) => {
                    assert!(
                        reason.contains("ACP client"),
                        "the deny reason names the ACP client: {response} => {reason}"
                    );
                    assert!(
                        reason.contains("invalid"),
                        "the deny reason says why: {response} => {reason}"
                    );
                }
                other => panic!("{response} must fail closed as Deny, got {other:?}"),
            }
        }

        // The valid shapes classify as themselves — and only the
        // offered `allow_once` ever reaches the Approve path.
        assert_eq!(
            classify_permission_answer(&json!({"outcome": "selected", "optionId": "allow_once"})),
            PermissionAnswer::Allow
        );
        assert_eq!(
            classify_permission_answer(&json!({"outcome": "selected", "optionId": "reject_once"})),
            PermissionAnswer::Reject
        );
        assert_eq!(
            classify_permission_answer(&json!({"outcome": "cancelled"})),
            PermissionAnswer::Cancelled
        );
        assert!(matches!(
            answer_action(Ok(Ok(json!({"outcome": "selected", "optionId": "allow_once"}))), false),
            ClientAnswer::Grant
        ));
        // `cancelled` is a VALID shape: never an invalid-shape deny,
        // and never a grant — its standalone/cancel-owned decision is
        // the cancel contract's branch (S3).
        assert!(!matches!(
            answer_action(Ok(Ok(json!({"outcome": "cancelled"}))), false),
            ClientAnswer::Grant
        ));
    }

    /// S3 (M3a + M3b): a STANDALONE `cancelled` answer fails closed as
    /// a `Deny` naming the ACP client (no approval exists to grant);
    /// the same answer while a `session/cancel` owns the exchange
    /// decides NOTHING (`NoDecision` — the cancel path resolves the
    /// request locally, zero decision frames). The cancel mark is
    /// per-session and consumed on first read, so a stale mark can
    /// never suppress a later standalone deny.
    #[test]
    fn standalone_cancelled_denies_but_a_cancel_mark_blocks_a_decision() {
        let standalone = answer_action(Ok(Ok(json!({"outcome": "cancelled"}))), false);
        match standalone {
            ClientAnswer::Deny(reason) => {
                assert!(
                    reason.contains("ACP client"),
                    "the deny reason names the ACP client: {reason}"
                );
                assert!(
                    reason.contains("cancelled"),
                    "the deny reason says what happened: {reason}"
                );
            }
            other => panic!("standalone cancelled must fail closed as Deny, got {other:?}"),
        }

        let owned = answer_action(Ok(Ok(json!({"outcome": "cancelled"}))), true);
        assert!(
            matches!(owned, ClientAnswer::NoDecision),
            "a cancel-owned exchange sends no decision at all: {owned:?}"
        );

        // The mark itself: per-session, consumed once (first read wins).
        let state = SessionState::default();
        state.arm_cancel_resolution("s1");
        assert!(
            state.take_cancel_resolution("s1"),
            "the cancel path's mark is found by its own session"
        );
        assert!(
            !state.take_cancel_resolution("s1"),
            "the mark is consumed; a later standalone answer can never inherit it"
        );
        assert!(
            !state.take_cancel_resolution("s2"),
            "marks are per-session"
        );
    }
}
