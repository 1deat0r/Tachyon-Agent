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
use std::future::Future;
use std::io::Read as _;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Number, Value, json};
use tachyon_protocol::{
    Command, GatewayEvent, McpArgEntry, McpEnvEntry, McpServerDescriptor, ServerFrame,
};
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
/// typed error instead of a guessed verdict. The budget is SUSPENDED
/// while a permission request is outstanding (human-paced — see
/// [`TurnBudgetState`]); `session/cancel` and client disconnect remain
/// the unblock paths there.
const TURN_TIMEOUT: Duration = Duration::from_secs(300);

/// One turn's deadline state, shared between the deadline wrapper
/// ([`run_prompt`]) and the permission bridge that suspends it:
/// `suspended` freezes the budget for as long as a
/// `session/request_permission` is outstanding (M3a — a human may take
/// minutes; the deadline must never mistake that for a dead run), and
/// RESOLVING the request grants the full [`TURN_TIMEOUT`] budget again
/// ("resume with full remaining budget"). [`TurnBudgetState::running`]
/// is the initial and the resumed state.
#[derive(Clone, Copy, Debug)]
struct TurnBudgetState {
    /// Instant at which the deadline fires while running.
    deadline: tokio::time::Instant,
    /// `true` while a permission request is outstanding: no deadline
    /// timer is armed, nothing can time out, cancel/disconnect end the
    /// wait from inside the turn.
    suspended: bool,
}

impl TurnBudgetState {
    /// A running budget: full [`TURN_TIMEOUT`] from now.
    fn running() -> Self {
        Self {
            deadline: tokio::time::Instant::now() + TURN_TIMEOUT,
            suspended: false,
        }
    }

    /// Pauses the budget, preserving the remaining time (it is never
    /// consumed while suspended; the resume starts fresh anyway).
    #[must_use]
    fn suspend(self) -> Self {
        Self {
            suspended: true,
            ..self
        }
    }
}

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

/// THE orphan bound: how long a park may wait for its
/// `approval_request` frame after the park is observed (the
/// `status → WaitingApproval` journal is written BEFORE the ask, so a
/// settlement read can see the park a moment before the request frame
/// arrives). Tightened to 2 s by the orphan-fallback slice (ticket 03):
/// the ask journals in the SAME supervisor command as the status —
/// milliseconds in practice — so this is orders of magnitude above the
/// real gap while keeping the typed fallback fast. Past it the turn
/// falls back to the typed `approval_required` orphan refusal — never a
/// silent hang, never a request invented from a snapshot.
const APPROVAL_REQUEST_GRACE: Duration = Duration::from_secs(2);

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

    /// The recorded-turn gate refusal (`-32003`): the session's last
    /// `session/load` recorded a turn that is still running (ADR-
    /// 0005:39). SAME wire code and `turn_in_progress` data marker as
    /// [`HandlerError::turn_in_progress`] (one refusal class for one
    /// overlap hazard), message names the recorded cause so the client
    /// knows the recovery: wait, or `session/cancel`.
    pub(crate) fn recorded_turn_in_progress() -> Self {
        Self {
            code: TURN_CONFLICT,
            message: "A previous turn for this session is still in progress; wait for it to \
                 finish, or session/cancel it"
                .to_owned(),
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
    /// Raw `mcpServers` entries (shape-checked only); deep validation
    /// and mapping happen in [`map_mcp_servers`], and the gateway
    /// revalidates authoritatively at registration.
    pub(crate) servers: Vec<Value>,
}

/// Validates `session/new` params per ACP `schema-v1.23.0`
/// (`NewSessionRequest`): `cwd` is required and must be absolute;
/// `mcpServers` must be an absent/empty array or a list of stdio server
/// entries (shape-checked here, mapped by [`map_mcp_servers`]). Runs
/// BEFORE any gateway interaction, so a bad request never reaches the
/// liveness probe or opens a connection.
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
    let servers = match object.get("mcpServers") {
        None => Vec::new(),
        Some(servers) => servers
            .as_array()
            .ok_or_else(|| {
                HandlerError::invalid(
                    "Invalid params: mcpServers must be an array",
                    "invalid_params",
                )
            })?
            .iter()
            .map(|entry| {
                entry.as_object().ok_or_else(|| {
                    HandlerError::invalid(
                        "Invalid params: mcpServers entries must be objects",
                        "invalid_params",
                    )
                })?;
                Ok(entry.clone())
            })
            .collect::<Result<Vec<Value>, HandlerError>>()?,
    };
    Ok(SessionNewParams {
        cwd: cwd.to_owned(),
        servers,
    })
}

/// Maps one ACP stdio server entry to a gateway [`McpServerDescriptor`]
/// (TASK 01 S1): untrusted external input, so every field is validated
/// against the ticket-01 rules before anything is pinned or launched.
/// ALL env/arg values pin `secret: true` — the adapter is an external
/// trust boundary and ACP carries no secret marker (spec §Q3). The
/// gateway revalidates authoritatively at registration regardless.
pub(crate) fn map_mcp_server(entry: &Value) -> Result<McpServerDescriptor, HandlerError> {
    let invalid =
        |detail: &str| HandlerError::invalid(detail.to_string(), "invalid_mcp_descriptor");
    let object = entry
        .as_object()
        .ok_or_else(|| invalid("mcpServers entries must be objects"))?;
    // Only stdio transport is supported; anything else (or absent) is
    // refused rather than guessed.
    let stdio = object
        .get("transport")
        .and_then(Value::as_object)
        .and_then(|transport| transport.get("type"))
        .and_then(Value::as_str)
        == Some("stdio");
    if !stdio {
        return Err(invalid(
            "mcpServers entries must declare transport {type: stdio}",
        ));
    }
    // No name derivation: a missing/unusable name is refused, never guessed.
    let server_id = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("mcpServers entries require a name"))?;
    if server_id.is_empty()
        || server_id.len() > 64
        || server_id.contains('\0')
        || server_id.contains('/')
    {
        return Err(invalid(
            "mcpServers name must be 1..=64 bytes, NUL-free, without `/`",
        ));
    }
    let command = object
        .get("command")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("mcpServers entries require a command"))?;
    if !Path::new(command).is_absolute() {
        return Err(invalid("mcpServers command must be an absolute path"));
    }
    let mut args = Vec::new();
    if let Some(raw_args) = object.get("args") {
        let list = raw_args
            .as_array()
            .ok_or_else(|| invalid("mcpServers args must be an array"))?;
        if list.len() > 32 {
            return Err(invalid("mcpServers args holds at most 32 entries"));
        }
        for arg in list {
            args.push(map_mcp_arg(arg, &invalid)?);
        }
    }
    let mut env = Vec::new();
    if let Some(raw_env) = object.get("env") {
        let list = raw_env
            .as_array()
            .ok_or_else(|| invalid("mcpServers env must be an array"))?;
        for entry in list {
            env.push(map_mcp_env_entry(entry, &invalid)?);
        }
    }
    Ok(McpServerDescriptor {
        server_id: server_id.to_owned(),
        command: command.to_owned(),
        args,
        env,
    })
}

/// Maps one argv entry, pinning `secret: true` at the external
/// trust boundary (spec §Q3).
fn map_mcp_arg(
    arg: &Value,
    invalid: &dyn Fn(&str) -> HandlerError,
) -> Result<McpArgEntry, HandlerError> {
    let value = arg
        .as_str()
        .ok_or_else(|| invalid("mcpServers args must be strings of at most 4KiB"))?;
    if value.len() > 4096 || value.contains('\0') {
        return Err(invalid("mcpServers args must be strings of at most 4KiB"));
    }
    Ok(McpArgEntry {
        value: value.to_owned(),
        secret: true,
    })
}

/// Maps one env entry against the shared ticket-01 rule set, pinning
/// `secret: true` (spec §Q3). Inherited location keys are refused like
/// loader/interpreter state: the adapter never overrides them.
fn map_mcp_env_entry(
    entry: &Value,
    invalid: &dyn Fn(&str) -> HandlerError,
) -> Result<McpEnvEntry, HandlerError> {
    let pair = entry
        .as_object()
        .ok_or_else(|| invalid("mcpServers env entries must be {name, value} objects"))?;
    let name = pair
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("mcpServers env entries require a name"))?;
    let value = pair
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("mcpServers env entries require a string value"))?;
    if !tachyon_gateway::is_valid_mcp_env_name(name) {
        return Err(invalid(
            "mcpServers env names must match [A-Za-z_][A-Za-z0-9_]*",
        ));
    }
    if value.len() > 16384 {
        return Err(invalid("mcpServers env values hold at most 16KiB"));
    }
    if tachyon_gateway::is_dangerous_mcp_env(name) {
        return Err(invalid(
            "mcpServers env names must not override loader state",
        ));
    }
    if tachyon_gateway::INHERITED_ENV_KEYS.contains(&name) {
        return Err(invalid(
            "mcpServers env must not override inherited location keys",
        ));
    }
    Ok(McpEnvEntry {
        name: name.to_owned(),
        value: value.to_owned(),
        secret: true,
    })
}

/// Maps every `session/new` server entry, failing the whole call on the
/// first invalid descriptor (fail closed: no partial pin downstream).
/// Duplicate server ids in one call are refused like the gateway: the
/// whole call fails before any session exists.
pub(crate) fn map_mcp_servers(entries: &[Value]) -> Result<Vec<McpServerDescriptor>, HandlerError> {
    let mut mapped = Vec::with_capacity(entries.len());
    for entry in entries {
        let descriptor = map_mcp_server(entry)?;
        if mapped
            .iter()
            .any(|known: &McpServerDescriptor| known.server_id == descriptor.server_id)
        {
            return Err(HandlerError::invalid(
                format!(
                    "server '{}': duplicate server id in one call",
                    descriptor.server_id
                ),
                "invalid_mcp_descriptor",
            ));
        }
        mapped.push(descriptor);
    }
    Ok(mapped)
}

/// Validated `session/prompt` params.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PromptParams {
    /// Session the turn belongs to (identity-mapped gateway session).
    pub(crate) session_id: SessionId,
    /// Concatenated text prompt — the task objective.
    pub(crate) objective: String,
    /// Asserted `resource_link` URIs in block order (TASK 01 S1):
    /// resolved downstream, never executed.
    pub(crate) links: Vec<String>,
}

/// Validated `session/load` params.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SessionLoadParams {
    /// Session to replay (identity-mapped gateway session).
    pub(crate) session_id: SessionId,
    /// Absolute cwd the client asks to load with — verified against the
    /// session's pinned workspace root before any replay (ADR-0005:40).
    pub(crate) cwd: String,
    /// Raw `mcpServers` entries (shape-checked only): non-empty lists
    /// assert set-equality against the pinned set before any replay
    /// (TASK 01 load slice); empty asserts nothing.
    pub(crate) servers: Vec<Value>,
}

/// Validates `session/load` params per ACP `schema-v1.23.0`
/// (`LoadSessionRequest`): `sessionId` must parse as a session id,
/// `cwd` is required and must be absolute, and `mcpServers` entries
/// are shape-checked (absent/empty accepted with no assertion;
/// non-empty asserts set-equality against the pinned set before any
/// replay). Runs BEFORE the liveness gate, so a bad request never
/// reaches the gateway (ADR-0005:40 — load creates nothing).
pub(crate) fn parse_load(params: Option<&Value>) -> Result<SessionLoadParams, HandlerError> {
    let invalid_shape = || {
        HandlerError::invalid(
            "Invalid params: session/load requires sessionId and cwd",
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
    let servers = match object.get("mcpServers") {
        None => Vec::new(),
        Some(servers) => servers
            .as_array()
            .ok_or_else(|| {
                HandlerError::invalid(
                    "Invalid params: mcpServers must be an array",
                    "invalid_params",
                )
            })?
            .iter()
            .map(|entry| {
                entry.as_object().ok_or_else(|| {
                    HandlerError::invalid(
                        "Invalid params: mcpServers entries must be objects",
                        "invalid_params",
                    )
                })?;
                Ok(entry.clone())
            })
            .collect::<Result<Vec<Value>, HandlerError>>()?,
    };
    Ok(SessionLoadParams {
        session_id,
        cwd: cwd.to_owned(),
        servers,
    })
}

/// Asserts an asserted MCP server set against the pinned set (TASK 01
/// load slice): `server_id` set equality in both directions — a subset,
/// superset, or drifted id fails closed. Pure: the caller fetches the
/// pinned ids read-only via `ListMCPServers`.
pub(crate) fn check_mcp_set(asserted: &[String], pinned: &[String]) -> Result<(), HandlerError> {
    let asserted: std::collections::HashSet<&str> = asserted.iter().map(String::as_str).collect();
    let pinned: std::collections::HashSet<&str> = pinned.iter().map(String::as_str).collect();
    if asserted == pinned {
        return Ok(());
    }
    Err(HandlerError::invalid(
        "session MCP set does not match the session's pinned servers",
        "mcp_set_mismatch",
    ))
}

/// The pinned-workspace check for `session/load` (ADR-0005:40:
/// "validates the pinned workspace"): the requested `cwd` must equal
/// the session's `workspace_root` exactly — load never rebinds a
/// pinned root. A mismatch is a typed `-32602 workspace_mismatch`
/// refusal; equality passes untouched.
pub(crate) fn check_load_workspace(cwd: &str, workspace_root: &str) -> Result<(), HandlerError> {
    if cwd == workspace_root {
        return Ok(());
    }
    Err(HandlerError::invalid(
        "Invalid params: cwd does not match the session's pinned workspace root",
        "workspace_mismatch",
    ))
}

/// Validated `session/attach` params (ADR-0019, spec `.scratch/acp-attach/spec.md`):
/// `sessionId` identity-maps the gateway session, `taskId` optionally pins
/// the exact turn (default: the latest non-terminal turn from `GetSession`),
/// `from_seq` optionally sets the replay cursor (default 0: replay-then-tail).
/// Shape validation runs BEFORE the liveness gate, so a malformed attach
/// never touches the gateway. Takes no turn slot and mints no idempotency
/// key — attach is a read-only subscribe-reader.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AttachParams {
    /// Session owning the live turn.
    pub(crate) session_id: SessionId,
    /// Explicit turn target, if the client pins one.
    pub(crate) task_id: Option<TaskId>,
    /// Cursor to replay from (gateway cursor pushdown reuses it verbatim).
    pub(crate) from_seq: u64,
}

/// Validates `session/attach` params: `sessionId` must parse as a session
/// id; `taskId`, when present (non-null), must parse as a task id;
/// `from_seq`, when present (non-null), must be a non-negative integer
/// JSON number (default 0). Every shape failure is a typed `-32602`
/// refusal with a taxonomy `data` marker — never a guess.
pub(crate) fn parse_attach(params: Option<&Value>) -> Result<AttachParams, HandlerError> {
    let invalid_shape = || {
        HandlerError::invalid(
            "Invalid params: session/attach requires sessionId",
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
    let task_id = match object.get("taskId") {
        None | Some(Value::Null) => None,
        Some(raw) => {
            let raw = raw.as_str().ok_or_else(|| {
                HandlerError::invalid(
                    "Invalid params: taskId must be a string task id",
                    "invalid_task_id",
                )
            })?;
            Some(raw.parse().map_err(|_| {
                HandlerError::invalid(
                    "Invalid params: taskId is not a valid task id",
                    "invalid_task_id",
                )
            })?)
        }
    };
    let from_seq = match object.get("from_seq") {
        None | Some(Value::Null) => 0,
        Some(raw) => {
            let cursor = raw.as_u64().ok_or_else(|| {
                HandlerError::invalid(
                    "Invalid params: from_seq must be a non-negative integer",
                    "invalid_from_seq",
                )
            })?;
            // The gateway cursor is an `i64`; a larger cursor has no
            // subscription to resume from, so it is a shape refusal —
            // never a saturating guess at the call site.
            if cursor > i64::MAX as u64 {
                return Err(HandlerError::invalid(
                    "Invalid params: from_seq exceeds the gateway cursor range",
                    "invalid_from_seq",
                ));
            }
            cursor
        }
    };
    Ok(AttachParams {
        session_id,
        task_id,
        from_seq,
    })
}

/// Resolves the attach target from one `GetSession` turn history (ADR-0019
/// §2): an explicit `taskId` pins the exact turn (an id outside the history
/// ⇒ typed `unknown_task`); otherwise the latest non-terminal turn (the
/// `recorded_turn` convention — replay order is `turn_seq ASC`, so only the
/// last entry can be live) is the target. A terminal target — explicit or
/// latest — is refused typed `attach_target_terminal`: history is
/// `session/load`'s job, attach is live-tail only. A corrupt row (missing
/// status, unparseable id) is typed `gateway_payload_invalid`, never a
/// guessed target. Pure: the caller issues the single `GetSession` and the
/// subscription reuses the returned cursor baseline.
pub(crate) fn resolve_attach_target(
    turns: &[Value],
    explicit: Option<TaskId>,
) -> Result<TaskId, HandlerError> {
    if let Some(wanted) = explicit {
        let row = turns
            .iter()
            .find(|turn| {
                turn.get("task_id")
                    .and_then(Value::as_str)
                    .and_then(|raw| raw.parse::<TaskId>().ok())
                    .is_some_and(|id| id == wanted)
            })
            .ok_or_else(|| {
                HandlerError::invalid(
                    "session/attach target task is not part of this session",
                    "unknown_task",
                )
            })?;
        let status = row.get("status").and_then(Value::as_str).ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "attach target turn carries no status",
            )
        })?;
        if is_terminal_status(status) {
            return Err(HandlerError::invalid(
                "session/attach target is terminal; use session/load for replay",
                "attach_target_terminal",
            ));
        }
        return Ok(wanted);
    }
    let Some(last) = turns.last() else {
        return Err(HandlerError::invalid(
            "session/attach found no turns to attach to",
            "unknown_task",
        ));
    };
    let status = last.get("status").and_then(Value::as_str).ok_or_else(|| {
        HandlerError::turn_failed("gateway_payload_invalid", "latest turn carries no status")
    })?;
    if is_terminal_status(status) {
        return Err(HandlerError::invalid(
            "session/attach target is terminal; use session/load for replay",
            "attach_target_terminal",
        ));
    }
    recorded_turn(std::slice::from_ref(last))?.ok_or_else(|| {
        HandlerError::turn_failed(
            "gateway_payload_invalid",
            "latest turn is live but carries no task id",
        )
    })
}

/// The read-only parked-state surface for one journalled `approval_request`
/// (ADR-0019 §4): the `tool_call` announcement ONLY — never a
/// `session/request_permission` frame, never an armed reply slot, never an
/// `Approve`/`Deny`. Returns the approval id (for the caller's announce-once
/// set) with the frame, or `None` when the journal carries no approval id —
/// refusing to invent an approval, like the turn pipeline's orphan rule.
fn attach_parked_announcement(session_id: &str, payload: &Value) -> Option<(String, Outbound)> {
    let (approval_id, title) = approval_ask(payload)?;
    Some((
        approval_id.clone(),
        tool_call_announcement(session_id, &approval_id, &title),
    ))
}

/// The final notification closing an attach stream: the terminal status
/// REPORTED as an observed `task_status` update — never a verdict (no
/// `stopReason` ownership; the driving turn / Supervisor owns the verdict,
/// ADR-0019 §3). The method result carries the same `{status}` object right
/// after this frame on the writer channel.
fn attach_final_notification(session_id: &str, status: &str) -> Outbound {
    Outbound::notification(
        "session/update",
        json!({
            "sessionId": session_id,
            "update": { "sessionUpdate": "task_status", "status": status },
        }),
    )
}

/// Observes one attach-stream journal event: `agent_message` rows forward as
/// chunks (the turn/load mapping), `approval_request` rows announce the park
/// (announcement ONLY — [`attach_parked_announcement`], deduplicated through
/// `announced`), `approval` rows are observed without effect, and every other
/// kind is seen but never chunked. Returns true for settlement signals: the
/// caller then owes a `GetTask` read (only `GetTask` decides the status —
/// the adapter never parses it from the journal).
fn observe_attach_event(
    kind: &str,
    payload: &Value,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
    announced: &mut HashSet<String>,
) -> bool {
    if let Some(update) = agent_chunk(kind, payload) {
        let params = json!({ "sessionId": session_id, "update": update });
        if emit
            .send(Outbound::notification("session/update", params))
            .is_err()
        {
            tracing::warn!("client went away; dropping attach chunk");
        }
    }
    if kind == "approval_request" {
        if let Some((approval_id, frame)) = attach_parked_announcement(session_id, payload) {
            if announced.insert(approval_id.clone()) {
                if emit.send(frame).is_err() {
                    tracing::warn!("client went away; dropping attach park announcement");
                }
            } else {
                tracing::debug!(%approval_id, "park already announced; ignoring the replayed row");
            }
        } else {
            tracing::warn!(
                "approval_request journal carries no approval id; refusing to invent an approval"
            );
        }
    }
    if kind == "approval" {
        observe_approval_journal(payload);
    }
    is_settlement_signal(kind)
}

/// Consumes one subscribe-acknowledgement replay for an attach stream: rows
/// arrive store-shaped (`payload` as a JSON string) and forward through the
/// same observation as live frames. Returns true when the replay carried a
/// settlement signal (the caller then owes a `GetTask` read, so an attach
/// racing settlement cannot stall on a signal the replay did not re-deliver
/// live).
fn process_attach_replay(
    payload: &Value,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
    announced: &mut HashSet<String>,
) -> bool {
    let Some(rows) = payload.get("events").and_then(Value::as_array) else {
        return false;
    };
    let mut signal = false;
    for row in rows {
        let Some(kind) = row.get("kind").and_then(Value::as_str) else {
            continue;
        };
        let value = match row.get("payload") {
            Some(Value::String(text)) => serde_json::from_str::<Value>(text).unwrap_or(Value::Null),
            Some(other) => other.clone(),
            None => Value::Null,
        };
        signal |= observe_attach_event(kind, &value, session_id, emit, announced);
    }
    signal
}

/// Reads one attach subscription to its terminal status: forwards journalled
/// `agent_message` rows as `session/update` chunks, announces approval parks
/// read-only, serves settlement signals with a `GetTask` read through the
/// single request slot, re-subscribes ONCE at the gateway cursor on
/// `ResyncRequired` (then fails typed `resync_required` — never a silent
/// gap), and ends with the final status notification. Takes no turn slot,
/// arms no request slot, issues no `Approve`/`Deny`/`CreateTask` — the
/// gateway task is unaffected by everything here except being observed.
/// Returns the terminal status for the method result.
async fn stream_attach(
    conn: &mut GatewayConn,
    subscribe_id: EventId,
    task_id: TaskId,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
) -> Result<String, HandlerError> {
    let mut awaiting: Option<EventId> = Some(subscribe_id);
    let mut awaiting_subscribe = true;
    // A settlement check is owed: a signal arrived while a request was in
    // flight, or a re-subscribe orphaned one.
    let mut pending = false;
    let mut announced: HashSet<String> = HashSet::new();
    // Re-subscribes performed so far (bounded by MAX_RESUBSCRIBES, the same
    // budget turns use).
    let mut resyncs: u32 = 0;
    loop {
        let frame: ServerFrame =
            read_gateway_frame(conn.read_frame(), None::<&watch::Sender<TurnBudgetState>>).await?;
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
                    pending |= process_attach_replay(&payload, session_id, emit, &mut announced);
                    if pending {
                        pending = false;
                        awaiting = Some(send_frame(conn, Command::GetTask { task_id }).await?);
                    }
                } else if let Some(status) = payload
                    .get("task")
                    .and_then(|task| task.get("status"))
                    .and_then(Value::as_str)
                {
                    if is_terminal_status(status) {
                        if emit
                            .send(attach_final_notification(session_id, status))
                            .is_err()
                        {
                            tracing::warn!("client went away; dropping attach final status");
                        }
                        return Ok(status.to_owned());
                    }
                    // A non-terminal bounce (a signal that arrived
                    // mid-round-trip serves next loop): keep streaming.
                    if pending {
                        pending = false;
                        awaiting = Some(send_frame(conn, Command::GetTask { task_id }).await?);
                    }
                } else {
                    return Err(HandlerError::turn_failed(
                        "gateway_payload_invalid",
                        "GetTask answered without a task status",
                    ));
                }
            }
            ServerFrame::Response(_) => {
                tracing::debug!("ignoring a response for another request id mid-attach");
            }
            ServerFrame::Event(envelope) => match envelope.event {
                GatewayEvent::Journal { kind, payload } => {
                    if observe_attach_event(&kind, &payload, session_id, emit, &mut announced) {
                        pending = true;
                    }
                    if pending && awaiting.is_none() {
                        pending = false;
                        awaiting = Some(send_frame(conn, Command::GetTask { task_id }).await?);
                    }
                }
                GatewayEvent::ResyncRequired { after_seq, .. } => {
                    resyncs += 1;
                    if resyncs > MAX_RESUBSCRIBES {
                        // Persistent failure: typed, never a silent gap.
                        return Err(HandlerError::turn_failed(
                            "resync_required",
                            format!(
                                "attach subscription overflowed again after {MAX_RESUBSCRIBES} \
                                 re-subscribe; failing typed rather than silently \
                                 truncating the stream"
                            ),
                        ));
                    }
                    if awaiting.is_some() && !awaiting_subscribe {
                        // A GetTask read orphaned by the re-subscribe:
                        // re-check right after the replay.
                        pending = true;
                    }
                    awaiting =
                        Some(send_frame(conn, Command::Subscribe { task_id, after_seq }).await?);
                    awaiting_subscribe = true;
                    tracing::info!(
                        %task_id,
                        after_seq,
                        resyncs,
                        "attach subscription overflowed; re-subscribing at the gateway cursor"
                    );
                }
                other => {
                    tracing::debug!(event = ?other, "ignoring a non-journal gateway event mid-attach");
                }
            },
        }
    }
}

/// One `session/attach` end to end (ADR-0019): `GetSession` resolves the
/// target (explicit pin or latest live turn — terminal targets refuse typed
/// BEFORE any subscription), then the read-only subscription tails the task
/// to its terminal status. Creates nothing, restarts nothing, holds no
/// idempotency key and no turn slot. Unbounded by any adapter deadline — the
/// tailed task's own lifetime bounds the stream (its driving turn carries
/// the timeout, and EOF teardown mirrors prompt turns). `emit` receives the
/// `session/update` frames; the returned value is the `{status}` result.
/// Unknown sessions fail with the gateway's typed `unknown_session` via
/// [`call_error`].
pub(crate) async fn run_attach<C: Connector>(
    connector: &C,
    params: AttachParams,
    emit: &UnboundedSender<Outbound>,
) -> Result<Value, HandlerError> {
    let session_id = params.session_id;
    let mut conn = connector
        .connect()
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;
    let payload = conn
        .call(Command::GetSession { session_id })
        .await
        .map_err(call_error)?;
    let turns = payload
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "GetSession answered without a turns array",
            )
        })?;
    let task_id = resolve_attach_target(turns, params.task_id)?;
    // Exact: `parse_attach` refuses cursors above `i64::MAX`, so this
    // cast cannot wrap — `cast_signed` states the intent explicitly.
    let subscribe_id = conn
        .send(Command::Subscribe {
            task_id,
            after_seq: params.from_seq.cast_signed(),
        })
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;
    let status = stream_attach(
        &mut conn,
        subscribe_id,
        task_id,
        &session_id.to_string(),
        emit,
    )
    .await?;
    Ok(json!({ "status": status }))
}

/// The replay frames for one `session/load`: every recorded
/// conversation entry in `turn_seq` order, mapped to the ACP
/// `session/update` notifications the protocol requires BEFORE the
/// load result (ADR-0005:29) — `speaker: user` → `user_message_chunk`,
/// `agent` → `agent_message_chunk`, text verbatim. A malformed row
/// (unknown speaker, non-string content) is logged and skipped: one
/// corrupt entry must never wedge the whole load, and the adapter
/// never invents a role for data it cannot read.
fn load_replay_frames(session_id: &str, turns: &[Value]) -> Vec<Outbound> {
    let mut frames = Vec::new();
    for turn in turns {
        let Some(conversation) = turn.get("conversation").and_then(Value::as_array) else {
            tracing::warn!(%session_id, "recorded turn without a conversation; skipped in load replay");
            continue;
        };
        for entry in conversation {
            let speaker = entry.get("speaker").and_then(Value::as_str);
            let content = entry.get("content").and_then(Value::as_str);
            let (Some(speaker), Some(content)) = (speaker, content) else {
                tracing::warn!(%session_id, "malformed conversation row skipped in load replay");
                continue;
            };
            let update = match speaker {
                "user" => "user_message_chunk",
                "agent" => "agent_message_chunk",
                other => {
                    tracing::warn!(speaker = other, %session_id, "unknown speaker skipped in load replay");
                    continue;
                }
            };
            frames.push(Outbound::notification(
                "session/update",
                json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": update,
                        "content": { "type": "text", "text": content },
                    },
                }),
            ));
        }
    }
    frames
}

/// The recorded turn carried by one `GetSession` history: the LAST
/// entry's task id when its status is NON-terminal (replay order is
/// `turn_seq ASC`, so the last entry is the latest turn), `None` when
/// the history is empty or the last turn already finished. A corrupt
/// row on that last entry (missing status or an unparseable id) is a
/// typed `gateway_payload_invalid` — never a guessed reconciliation
/// (ADR-0005:39). This is the stateless reconciliation seam: the
/// prompt gate and the cancel fallback both read it from the
/// `GetSession` each pipeline ALREADY issues — no stored record, no
/// extra gateway call, always fresh gateway truth.
fn recorded_turn(turns: &[Value]) -> Result<Option<TaskId>, HandlerError> {
    let Some(last) = turns.last() else {
        return Ok(None);
    };
    let status = last.get("status").and_then(Value::as_str).ok_or_else(|| {
        HandlerError::turn_failed("gateway_payload_invalid", "recorded turn carries no status")
    })?;
    if is_terminal_status(status) {
        return Ok(None);
    }
    let raw = last.get("task_id").and_then(Value::as_str).ok_or_else(|| {
        HandlerError::turn_failed(
            "gateway_payload_invalid",
            "recorded in-progress turn carries no task_id",
        )
    })?;
    raw.parse().map(Some).map_err(|_| {
        HandlerError::turn_failed(
            "gateway_payload_invalid",
            "recorded in-progress turn task_id does not parse",
        )
    })
}

/// One read-only `session/load` (ADR-0005:40): `GetSession` doubles as
/// the identity check (unknown id fails with the gateway's typed
/// `unknown_session`), the requested `cwd` must equal the session's
/// pinned workspace root (load never rebinds), then the ordered turns
/// become the replay notification frames. Creates nothing, restarts
/// nothing, holds no idempotency key — every fallible step runs before
/// any frame exists, so an error path can never emit a partial replay.
pub(crate) async fn run_load<C: Connector>(
    connector: &C,
    params: SessionLoadParams,
) -> Result<Vec<Outbound>, HandlerError> {
    let session_id = params.session_id;
    let mut conn = connector
        .connect()
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;
    let payload = conn
        .call(Command::GetSession { session_id })
        .await
        .map_err(call_error)?;
    let root = payload
        .get("workspace_root")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "GetSession answered without a workspace_root",
            )
        })?;
    check_load_workspace(&params.cwd, root)?;
    if !params.servers.is_empty() {
        assert_mcp_set(&mut conn, session_id, &params.servers).await?;
    }
    let turns = payload
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "GetSession answered without a turns array",
            )
        })?;
    Ok(load_replay_frames(&session_id.to_string(), turns))
}

/// Asserts the load-asserted MCP set against the pinned set (TASK 01
/// load slice): read-only `ListMCPServers`, server_id-set equality in
/// both directions. The load path stays read-only — this pins nothing,
/// approves nothing, and emits no frames.
async fn assert_mcp_set(
    conn: &mut GatewayConn,
    session_id: SessionId,
    asserted: &[Value],
) -> Result<(), HandlerError> {
    let mut names = Vec::with_capacity(asserted.len());
    for entry in asserted {
        names.push(
            entry
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    HandlerError::invalid(
                        "session/load MCP entries require a name",
                        "invalid_mcp_descriptor",
                    )
                })?
                .to_owned(),
        );
    }
    let payload = conn
        .call(Command::ListMCPServers { session_id })
        .await
        .map_err(call_error)?;
    let pinned: Vec<String> = payload
        .get("servers")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "ListMCPServers answered without a servers array",
            )
        })?
        .iter()
        .map(|server| {
            server
                .get("server_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| {
                    HandlerError::turn_failed(
                        "gateway_payload_invalid",
                        "ListMCPServers row without a server_id",
                    )
                })
        })
        .collect::<Result<Vec<String>, HandlerError>>()?;
    check_mcp_set(&names, &pinned)
}

/// Upper bound for one resolved resource body (spec: 64KiB).
const MAX_RESOURCE_BYTES: u64 = 64 * 1024;

/// Percent-decodes `%XX` sequences (both hex cases); a lone `%` or
/// non-hex pair fails closed — the URI cannot be resolved as stated.
fn percent_decode(uri_path: &str) -> Option<String> {
    let bytes = uri_path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return None;
            }
            let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Resolves one `resource_link` URI to its fenced evidence block
/// (TASK 01 S2): `file://` only, canonicalized against the pinned
/// session root with a separator-boundary prefix rule, leaf-symlink
/// refusal, regular-files-only, UTF-8 text, delimiter scan. Every
/// failure is a typed whole-prompt refusal (fail closed).
///
/// Residual TOCTOU note: the check-then-open window is accepted for
/// local IPC — exploiting it needs write access to the session root,
/// at which point the requester already owns the evidence.
/// Splits a `resource_link` URI into a candidate absolute path
/// (TASK 01 S2): `file://` scheme gate, backslash rejection
/// post-percent-decode, absolute-path rule. Pure string handling —
/// no filesystem contact.
fn parse_resource_uri(uri: &str) -> Result<std::path::PathBuf, HandlerError> {
    let refused = |data: &str, message: String| HandlerError::invalid(message, data);
    let path_text = uri.strip_prefix("file://").ok_or_else(|| {
        refused(
            "unsupported_scheme",
            "resource_link URIs must use the file:// scheme".to_owned(),
        )
    })?;
    let decoded = percent_decode(path_text).ok_or_else(|| {
        refused(
            "unresolvable",
            "resource_link path percent-encoding is malformed".to_owned(),
        )
    })?;
    // Backslash check AFTER decoding: an encoded `%5C` must not slip
    // past a pre-decode scan.
    if decoded.contains('\\') {
        return Err(refused(
            "unresolvable",
            "resource_link paths must not contain backslashes".to_owned(),
        ));
    }
    let candidate = std::path::PathBuf::from(&decoded);
    if !candidate.is_absolute() {
        return Err(refused(
            "unresolvable",
            "resource_link paths must be absolute".to_owned(),
        ));
    }
    Ok(candidate)
}

pub(crate) fn resolve_resource_link(root: &Path, uri: &str) -> Result<String, HandlerError> {
    let refused = |data: &str, message: String| HandlerError::invalid(message, data);
    let candidate = parse_resource_uri(uri)?;
    // Leaf-symlink refusal (no-follow on the final component) plus a
    // regular-file gate: directories, fifos, sockets, and devices can
    // neither be bounded nor read as text.
    let meta = std::fs::symlink_metadata(&candidate).map_err(|_| {
        refused(
            "unresolvable",
            format!("resource_link target is not readable: {uri}"),
        )
    })?;
    // A symlink leaf is treated as an escape attempt even when its
    // target would resolve inside: no-follow is the whole point.
    if meta.file_type().is_symlink() {
        return Err(refused(
            "out_of_root",
            format!("resource_link target is a symlink: {uri}"),
        ));
    }
    if !meta.file_type().is_file() {
        return Err(refused(
            "unresolvable",
            format!("resource_link target is not a regular file: {uri}"),
        ));
    }
    // Canonicalize (resolves parent-dir symlinks and `.`/`..`), then
    // enforce the separator-boundary prefix against the canonical root.
    // Hmm — symlink check above covered the leaf; parents resolve here.
    let canonical_root = std::fs::canonicalize(root).map_err(|_| {
        refused(
            "unresolvable",
            "resource_link session root is not readable".to_owned(),
        )
    })?;
    let canonical = std::fs::canonicalize(candidate).map_err(|_| {
        refused(
            "unresolvable",
            format!("resource_link target is not readable: {uri}"),
        )
    })?;
    let mut boundary = canonical_root.as_os_str().to_owned();
    boundary.push(std::path::MAIN_SEPARATOR_STR);
    let inside = canonical == canonical_root
        || canonical
            .as_os_str()
            .as_encoded_bytes()
            .starts_with(boundary.as_encoded_bytes());
    if !inside {
        return Err(refused(
            "out_of_root",
            format!("resource_link target escapes the session root: {uri}"),
        ));
    }
    if meta.len() > MAX_RESOURCE_BYTES {
        return Err(refused(
            "oversized",
            format!("resource_link target exceeds 64KiB: {uri}"),
        ));
    }
    // Open-then-verify: the fd's own metadata re-checks regular-file
    // (a swap between the checks above and this open still lands on a
    // checked type), and the read itself is hard-capped at 64KiB + 1 so
    // a racing growth can never inflate memory past the bound no matter
    // what the pre-check saw. Residual TOCTOU (content swapped inside
    // the authorized root between check and read) is accepted for local
    // IPC: exploiting it needs write access to the session root.
    let file = std::fs::File::open(&canonical).map_err(|error| {
        refused(
            "unresolvable",
            format!("resource_link target is not readable: {uri}: {error}"),
        )
    })?;
    let fd_meta = file.metadata().map_err(|_| {
        refused(
            "unresolvable",
            format!("resource_link target is not readable: {uri}"),
        )
    })?;
    if !fd_meta.file_type().is_file() {
        return Err(refused(
            "unresolvable",
            format!("resource_link target is not a regular file: {uri}"),
        ));
    }
    let mut content = String::new();
    file.take(MAX_RESOURCE_BYTES + 1)
        .read_to_string(&mut content)
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::InvalidData => refused(
                "invalid_encoding",
                format!("resource_link target is not UTF-8 text: {uri}"),
            ),
            _ => refused(
                "unresolvable",
                format!("resource_link target is not readable: {uri}"),
            ),
        })?;
    if content.len() as u64 > MAX_RESOURCE_BYTES {
        return Err(refused(
            "oversized",
            format!("resource_link target exceeds 64KiB: {uri}"),
        ));
    }
    // Delimiter collision: a bare footer/header line would spoof block
    // provenance — refuse instead of emitting ambiguous framing.
    if content
        .lines()
        .any(|line| line == "[/resource]" || line.starts_with("[resource "))
    {
        return Err(refused(
            "unsafe_content",
            format!("resource_link content collides with block framing: {uri}"),
        ));
    }
    Ok(format!("[resource {uri}]\n{content}\n[/resource]"))
}

/// Hard cap on `resource_link` blocks per prompt (spec: ≤8): checked
/// at parse time, before any resolution work.
const MAX_LINKS_PER_PROMPT: usize = 8;

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
    let mut links: Vec<String> = Vec::new();
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
            Some("resource_link") => {
                let uri = block.get("uri").and_then(Value::as_str).ok_or_else(|| {
                    HandlerError::invalid(
                        "Invalid params: resource_link block missing uri",
                        "invalid_prompt",
                    )
                })?;
                if links.len() >= MAX_LINKS_PER_PROMPT {
                    return Err(HandlerError::invalid(
                        "Too many resource_link blocks: at most 8 per prompt",
                        "too_many_resources",
                    ));
                }
                links.push(uri.to_owned());
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
    if objective.trim().is_empty() && links.is_empty() {
        return Err(HandlerError::invalid(
            "Invalid params: prompt text must not be empty",
            "empty_prompt",
        ));
    }
    Ok(PromptParams {
        session_id,
        objective,
        links,
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

    /// Abandons one armed slot whose waiter went away (TASK 01): a
    /// late answer finds no slot and is diagnosed by the caller.
    pub(crate) fn abandon_slot(&self, id: &RpcId) {
        self.disarm(id);
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
pub(crate) fn is_allow_once(response: &Value) -> bool {
    response.get("outcome").and_then(Value::as_str) == Some("selected")
        && response.get("optionId").and_then(Value::as_str) == Some("allow_once")
}

/// The strict reject gate: `outcome: "selected"` with the offered
/// `reject_once` option (the symmetric twin of [`is_allow_once`]) —
/// the one non-grant answer this ticket settles as an explicit `Deny`.
pub(crate) fn is_reject_once(response: &Value) -> bool {
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
/// re-delivered replay row (after a re-subscribe) can never ask twice —
/// plus the turn budget handle an outstanding request suspends (M3a).
#[derive(Debug)]
struct PermissionBridge {
    phase: PermissionPhase,
    asked: HashSet<String>,
    /// The shared turn-deadline state. `Some` in production (the
    /// wrapper's watch sender); `None` for units that drive the bridge
    /// directly and have no deadline to suspend.
    budget: Option<watch::Sender<TurnBudgetState>>,
}

impl Default for PermissionBridge {
    fn default() -> Self {
        Self {
            phase: PermissionPhase::Idle,
            asked: HashSet::new(),
            budget: None,
        }
    }
}

impl PermissionBridge {
    /// The production bridge, wired to the turn's deadline wrapper.
    fn new(budget: watch::Sender<TurnBudgetState>) -> Self {
        Self {
            phase: PermissionPhase::Idle,
            asked: HashSet::new(),
            budget: Some(budget),
        }
    }
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
        // Register the session's outstanding request before the frame
        // is queued: a `session/cancel` resolving locally looks the id
        // up by session, and one landing in this window must find it.
        state.mark_outstanding(session_id, request_id.clone());
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
        // Arm-time race: a cancel that marked this session BEFORE the
        // ask armed will never get an answer from that client — resolve
        // the fresh slot locally right now (the mark stays for the turn
        // to consume as the cancel-owned guard). The reply slot's
        // receiver buffers the payload until the turn awaits it, so the
        // phase can be set after this.
        if state.cancel_marked(session_id) {
            state.resolve_outstanding_locally(session_id);
            tracing::info!(
                %approval_id,
                "session/cancel marked before the ask armed; the request resolved \
                 locally as cancelled"
            );
        }
        // M3a: the request is human-paced — SUSPEND the turn budget
        // now; the wrapper arms no deadline until the exchange resolves
        // (cancel, answer, or EOF), which resumes with a full budget.
        if let Some(budget) = &self.budget {
            let current = *budget.borrow();
            budget.send_replace(current.suspend());
            tracing::info!(
                %approval_id,
                "permission request outstanding; the turn deadline is suspended"
            );
        }
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
    /// In-flight + completed MCP-at-setup outcomes by call id (TASK
    /// 01): a retried `session/new` coalesces onto the pending setup or
    /// replays the stored answer instead of creating a second session.
    mcp_setups: Mutex<HashMap<RpcId, watch::Receiver<Option<Outbound>>>>,
    /// Consumed MCP approval request ids (TASK 01): a late client
    /// answer for one is answered with the typed `stale_mcp_approval`
    /// diagnostic instead of dropping silently.
    consumed_mcp_approvals: Mutex<HashSet<RpcId>>,
    /// Session → the reply-slot id its turn currently has OUTSTANDING
    /// (`session/request_permission` awaiting the client). The cancel
    /// path resolves this slot locally through
    /// [`SessionState::resolve_outstanding_locally`] — it never waits
    /// for an answer the cancelling client will not send.
    outstanding: Mutex<HashMap<String, RpcId>>,
}

impl std::fmt::Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("active", &self.active)
            .field("calls", &self.calls)
            .field("requests", &self.requests)
            .field("cancels_in_flight", &self.cancels_in_flight)
            .field("outstanding", &self.outstanding)
            .field(
                "mcp_setups",
                &self.mcp_setups.lock().map(|setups| setups.len()),
            )
            .field(
                "consumed_mcp_approvals",
                &self.consumed_mcp_approvals.lock().map(|ids| ids.len()),
            )
            .finish()
    }
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
            calls: Mutex::new(HashMap::new()),
            requests: OutboundRequests::default(),
            cancels_in_flight: Mutex::new(HashSet::new()),
            outstanding: Mutex::new(HashMap::new()),
            mcp_setups: Mutex::new(HashMap::new()),
            consumed_mcp_approvals: Mutex::new(HashSet::new()),
        }
    }
}

impl SessionState {
    /// Subscribe to an in-flight or completed MCP-at-setup outcome
    /// (TASK 01): `None` when no setup runs for this call id.
    pub(crate) fn mcp_setup_subscribe(
        &self,
        id: &RpcId,
    ) -> Option<watch::Receiver<Option<Outbound>>> {
        self.mcp_setups
            .lock()
            .expect("mcp setup lock poisoned")
            .get(id)
            .cloned()
    }

    /// Publish a fresh MCP-at-setup outcome channel for this call id;
    /// returns `false` when one already exists (caller coalesces).
    pub(crate) fn mcp_setup_publish(&self, id: RpcId) -> Option<watch::Sender<Option<Outbound>>> {
        let mut setups = self.mcp_setups.lock().expect("mcp setup lock poisoned");
        if setups.contains_key(&id) {
            return None;
        }
        let (sender, receiver) = watch::channel(None);
        setups.insert(id, receiver);
        Some(sender)
    }

    /// Remember a consumed MCP approval request id so a late client
    /// answer is diagnosed instead of dropped silently (TASK 01).
    pub(crate) fn remember_mcp_approval(&self, id: RpcId) {
        self.consumed_mcp_approvals
            .lock()
            .expect("consumed approvals lock poisoned")
            .insert(id);
    }

    /// Abandons one armed outbound slot (TASK 01 setup waits).
    pub(crate) fn abandon_mcp_slot(&self, id: &RpcId) {
        self.requests.abandon_slot(id);
    }

    /// Take a remembered consumed id once (stale-answer diagnostic
    /// fires exactly once per id; later duplicates drop silently).
    pub(crate) fn take_consumed_mcp_approval(&self, id: &RpcId) -> bool {
        self.consumed_mcp_approvals
            .lock()
            .expect("consumed approvals lock poisoned")
            .remove(id)
    }

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
    /// Whether a `session/cancel` mark is set for `session_id` WITHOUT
    /// consuming it — the arm-time race check: a request that arms
    /// AFTER the cancel mark exists must resolve itself immediately
    /// (its cancelling client will never answer it).
    pub(crate) fn cancel_marked(&self, session_id: &str) -> bool {
        self.cancels_in_flight
            .lock()
            .expect("cancel-mark lock")
            .contains(session_id)
    }
    /// Registers the reply-slot id the session's turn now waits on.
    pub(crate) fn mark_outstanding(&self, session_id: &str, id: RpcId) {
        self.outstanding
            .lock()
            .expect("outstanding-request lock")
            .insert(session_id.to_owned(), id);
    }
    /// Forgets the session's outstanding request (its exchange ended:
    /// the answer arrived, or the slot resolved locally). A no-op when
    /// nothing is registered.
    pub(crate) fn clear_outstanding(&self, session_id: &str) {
        self.outstanding
            .lock()
            .expect("outstanding-request lock")
            .remove(session_id);
    }
    /// Resolves the session's outstanding permission request LOCALLY as
    /// `{"outcome":"cancelled"}` and disarms its reply slot — the
    /// cancel path's answer to M1a: no client round trip, no
    /// Approve/Deny, no deadlock against an answer the cancelling
    /// client will never send. Returns `true` when a slot was resolved
    /// (idempotent: a second call finds nothing).
    pub(crate) fn resolve_outstanding_locally(&self, session_id: &str) -> bool {
        let id = self
            .outstanding
            .lock()
            .expect("outstanding-request lock")
            .remove(session_id);
        let Some(id) = id else {
            return false;
        };
        let cancelled = InboundResponse {
            id,
            payload: Ok(json!({ "outcome": "cancelled" })),
        };
        // `route` drops the slot as it delivers: resolving and
        // disarming are one operation.
        self.requests.route(&cancelled)
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
        // left unconsumed belonged to that turn's exchange. The same
        // holds for a stale outstanding-request registration (its slot
        // died with that turn; resolving it later would find nothing).
        self.cancels_in_flight
            .lock()
            .expect("cancel-mark lock")
            .remove(session_id);
        self.outstanding
            .lock()
            .expect("outstanding-request lock")
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

/// Resolves every asserted link against the pinned root and
/// returns the fenced blocks as one appendable suffix (TASK 01 S3).
fn resolve_prompt_links(root: &str, links: &[String]) -> Result<String, HandlerError> {
    let root = Path::new(root);
    let mut suffix = String::new();
    for uri in links {
        let block = resolve_resource_link(root, uri)?;
        suffix.push('\n');
        suffix.push_str(&block);
    }
    Ok(suffix)
}

/// One `session/prompt` turn end to end.
///
/// [`TURN_TIMEOUT`]-bounded — EXCEPT while a permission request is
/// outstanding, where the budget is suspended for that human-paced
/// window (M3a) and `session/cancel` / client disconnect remain the
/// unblock paths. `emit` receives the `session/update` notification
/// frames streamed while the turn runs; the returned value is the
/// final `session/prompt` result.
pub(crate) async fn run_prompt<C: Connector>(
    connector: &C,
    params: PromptParams,
    idempotency_key: String,
    emit: &UnboundedSender<Outbound>,
    notice: watch::Sender<Option<TaskId>>,
    state: &Arc<SessionState>,
) -> Result<Value, HandlerError> {
    let (budget, budget_rx) = watch::channel(TurnBudgetState::running());
    turn_deadline(
        prompt_turn(
            connector,
            params,
            idempotency_key,
            emit,
            notice,
            state,
            budget,
        ),
        budget_rx,
    )
    .await
}

/// The typed `-32004 turn_timed_out` verdict: the turn did not reach a
/// terminal task status within the 300 s budget. ONE construction for
/// both sources — the deadline wrapper below, and a gateway read that
/// outlives the FROZEN budget while a permission request is
/// outstanding (see [`read_gateway_frame`]).
fn turn_timed_out() -> HandlerError {
    HandlerError::turn_failed(
        "turn_timed_out",
        format!(
            "turn did not reach a terminal task status within {}s",
            TURN_TIMEOUT.as_secs()
        ),
    )
}

/// One gateway read, suspension-aware: while the turn budget runs the
/// deadline wrapper owns the bound (the read completes inside the turn
/// future), so this is a plain read. While a permission request is
/// outstanding the wrapper's paused branch has NO timer — but a
/// gateway round trip in flight at that moment (the settlement read
/// the ask armed on top of) is machine-paced, never human-paced: it
/// keeps the FROZEN pre-suspension deadline and fails typed
/// [`turn_timed_out`] on expiry. Otherwise a wedged gateway would hang
/// the turn forever — and client EOF could never end it either (the
/// serve loop's drain waits on this turn).
async fn read_gateway_frame<F, T>(
    read: F,
    budget: Option<&watch::Sender<TurnBudgetState>>,
) -> Result<T, HandlerError>
where
    F: Future<Output = Result<T, GatewayUnavailable>>,
{
    let frozen = budget.and_then(|budget| {
        let state = *budget.borrow();
        state.suspended.then_some(state.deadline)
    });
    match frozen {
        Some(deadline) => match tokio::time::timeout_at(deadline, read).await {
            Ok(frame) => frame.map_err(|error| HandlerError::unavailable(&error)),
            Err(_) => Err(turn_timed_out()),
        },
        None => read
            .await
            .map_err(|error| HandlerError::unavailable(&error)),
    }
}

/// The turn-deadline wrapper around any turn future — the budget
/// logic of [`run_prompt`] factored out as the unit seam for the
/// paused-clock suspension proof.
///
/// `timeout_at` polls the turn first, so a turn completing in the same
/// wakeup as its deadline still wins. A suspend landing mid-wait does
/// not wake the running branch — the fired deadline re-reads the
/// budget: suspended ⇒ the paused branch (no timer at all); a resumed
/// (later) deadline ⇒ re-arm; unchanged ⇒ timed out. The budget
/// SENDER lives inside the turn future (the bridge writes through it),
/// so `budget_rx.changed()` fires on every bridge update and can only
/// close when the turn has already completed.
async fn turn_deadline<F>(
    turn: F,
    mut budget_rx: watch::Receiver<TurnBudgetState>,
) -> Result<Value, HandlerError>
where
    F: Future<Output = Result<Value, HandlerError>>,
{
    tokio::pin!(turn);
    loop {
        let snapshot = *budget_rx.borrow_and_update();
        if snapshot.suspended {
            // No deadline exists while a request is outstanding: only
            // the turn finishing (a cancel resolves the request, EOF
            // closes the slot) or a budget change ends this wait.
            tokio::select! {
                result = &mut turn => break result,
                changed = budget_rx.changed() => {
                    let _ = changed;
                    continue;
                }
            };
        }
        // Running: wait for the deadline or the turn.
        if let Ok(result) = tokio::time::timeout_at(snapshot.deadline, &mut turn).await {
            break result;
        }
        let current = *budget_rx.borrow_and_update();
        if current.suspended || current.deadline > snapshot.deadline {
            continue;
        }
        break Err(turn_timed_out());
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
    budget: watch::Sender<TurnBudgetState>,
) -> Result<Value, HandlerError> {
    let PromptParams {
        session_id,
        objective,
        links,
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

    // ADR-0005:39 reconciliation — the stateless recorded-turn gate,
    // read from the GetSession this pipeline ALREADY issued (zero
    // extra gateway calls): while the session's LAST recorded turn is
    // non-terminal, a later prompt would be a second live turn for one
    // session (the gateway's CreateTask has NO overlap guard — the
    // adapter is the only guard), so it is refused typed `-32003`
    // BEFORE any CreateTask. Covers every path — post-restart
    // re-prompt (no `session/load` in this process), post-timeout
    // re-prompt, load-then-prompt — because it holds no memory: the
    // fresh `GetSession` IS the reconciliation (terminal or absent ⇒
    // release; corrupt ⇒ typed fail, never a guessed release).
    let turns = session
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "GetSession answered without a turns array",
            )
        })?;
    if recorded_turn(turns)?.is_some() {
        return Err(HandlerError::recorded_turn_in_progress());
    }

    // ResourceLink resolution (TASK 01 S3): bounded file reads
    // against the pinned root from the GetSession above (zero extra
    // gateway calls), embedded as fenced provenance blocks AFTER the
    // recorded-turn gate (refused turns do no resolution work) and
    // BEFORE CreateTask (the verified objective is what runs).
    let mut objective = objective;
    if !links.is_empty() {
        objective.push_str(&resolve_prompt_links(&workspace_root, &links)?);
    }
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
    stream_turn(
        &mut conn,
        subscribe_id,
        task_id,
        session_id,
        emit,
        state,
        budget,
    )
    .await
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
    // The target: the ACTIVE turn's published task id first (a local
    // turn outranks everything and arms this process's cancel marks);
    // with no local turn, the session's own `GetSession` below derives
    // whether a NON-TERMINAL turn outlived this process — the
    // stateless ADR-0005:41 explicit stop. Never a terminal task:
    // `recorded_turn` only ever reports a non-terminal last turn.
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
        // The cancel targets this session's ACTIVE turn: mark it BEFORE
        // anything is awaited, so the turn's `cancelled` answer (whenever
        // it routes) finds the mark and sends zero decision frames
        // (ADR-0005:49; the guard the permission bridge consults).
        state.arm_cancel_resolution(&session_id.to_string());
        // Resolve an outstanding permission request LOCALLY as
        // `cancelled` (M1a): the cancelling client will never answer
        // it, and the inline cancel must not wait on that answer (M1c
        // — no deadlock). The gateway expires the parked row itself
        // during `CancelTask`; a late client answer finds a disarmed
        // slot and is ignored.
        if state.resolve_outstanding_locally(&session_id.to_string()) {
            tracing::info!(
                %session_id,
                "outstanding permission request resolved locally as cancelled; \
                 no Approve/Deny issued"
            );
        }
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
        cancel_outlived_turn(&mut conn, session_id).await
    }
}

/// The no-local-turn `session/cancel` path: `GetSession` proves the
/// session exists (unknown ⇒ typed refusal) AND — statelessly —
/// derives whether a NON-TERMINAL turn outlived this process
/// (ADR-0005:41's explicit stop for post-restart work). No such turn
/// ⇒ idempotent ok; one found ⇒ `CancelTask` on it, where
/// `illegal_transition` (already terminal) and `unknown_task` (gone)
/// both mean "not running" — the cancel's goal — so both are ok.
async fn cancel_outlived_turn(
    conn: &mut GatewayConn,
    session_id: SessionId,
) -> Result<(), HandlerError> {
    let payload = conn
        .call(Command::GetSession { session_id })
        .await
        .map_err(call_error)?;
    let turns = payload
        .get("turns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "GetSession answered without a turns array",
            )
        })?;
    let Some(task_id) = recorded_turn(turns)? else {
        tracing::info!(
            %session_id,
            "session/cancel: no active turn; idempotent ok (no CancelTask issued)"
        );
        return Ok(());
    };
    tracing::info!(
        %session_id,
        %task_id,
        "session/cancel: stopping the turn that outlived this process (ADR-0005:41)"
    );
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
        Err(GatewayCallError::Refused { code, message }) if code == "illegal_transition" => {
            tracing::info!(
                %session_id,
                %task_id,
                detail = %message,
                "session/cancel: task already terminal; idempotent ok"
            );
            Ok(())
        }
        Err(GatewayCallError::Refused { code, .. }) if code == "unknown_task" => {
            tracing::info!(
                %session_id,
                %task_id,
                "session/cancel: recorded task no longer exists; nothing to stop"
            );
            Ok(())
        }
        Err(error) => Err(call_error(error)),
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
    budget: watch::Sender<TurnBudgetState>,
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
    // Approve sequenced around the settlement slot) plus the turn
    // budget it suspends/resumes around each outstanding request.
    let mut bridge = PermissionBridge::new(budget);
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
            // The exchange is over the moment the receiver resolved:
            // the session no longer has an outstanding request for the
            // cancel path to find (its local resolution already ran, or
            // this was the client's own answer).
            state.clear_outstanding(&session_id);
            // M3a resume: the human-paced window closed — the turn
            // gets its FULL budget again from now.
            if let Some(budget) = &bridge.budget {
                budget.send_replace(TurnBudgetState::running());
                tracing::info!("permission request resolved; the turn deadline resumes");
            }
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
                    // terminal journal settles the prompt). The ask WAS
                    // announced (announcement precedes arming, and only
                    // `Outstanding` reaches this arm), so close it here:
                    // a locally resolved call must never stay pending
                    // for the session's lifetime (TASK 04).
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
        //    With no phase bound the deadline wrapper bounds the read
        //    while the budget runs — EXCEPT during the suspension:
        //    there the wrapper has no timer, so a gateway round trip
        //    already in flight keeps the frozen pre-suspension deadline
        //    (`read_gateway_frame`), never an unbounded read.
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
            None => read_gateway_frame(conn.read_frame(), bridge.budget.as_ref()).await?,
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

/// One journalled `approval` decision row: OBSERVATION ONLY — the
/// adapter never decides from a journal (decisions come only from the
/// client's validated answer through the single settlement slot). A
/// late deny row arriving with no outstanding request therefore changes
/// nothing: no panic, no second decision, no double-settle; the status
/// journals that follow drive settlement as always.
fn observe_approval_journal(payload: &Value) {
    let value = payload.get("v").unwrap_or(payload);
    let approval = value
        .get("approval")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match value.get("granted").and_then(Value::as_bool) {
        Some(true) => tracing::debug!(
            %approval,
            "journalled grant observed; the adapter never decides from a journal"
        ),
        Some(false) => tracing::info!(
            %approval,
            "journalled deny observed; the adapter never decides from a journal \
             (no double-settle)"
        ),
        None => tracing::debug!(
            %approval,
            "approval journal carries no granted flag; ignored"
        ),
    }
}

/// Forwards one journalled event (kind + raw payload) as ACP output:
/// `agent_message` → a `session/update` chunk, `approval_request` →
/// the permission exchange (`tool_call` announcement + request frame),
/// `approval` → observation only (never a decision), settlement kinds
/// arm the `GetTask` read, every other kind is omitted.
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
    if kind == "approval" {
        observe_approval_journal(payload);
    }
    if is_settlement_signal(kind) {
        *settlement = Some(kind.to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::{Value, json};
    use tachyon_types::TaskId;
    use tokio::sync::{mpsc, watch};

    use super::{
        APPROVAL_REQUEST_GRACE, ClientAnswer, DENY_REASON_REJECTED, DENY_SETTLE_GRACE,
        HandlerError, OutboundRequests, PermissionAnswer, PermissionBridge, PermissionPhase,
        ReadExpiry, SessionState, TurnBudgetState, agent_chunk, answer_action, approval_ask,
        approval_parked, check_load_workspace, check_mcp_set, classify_permission_answer,
        final_prompt_result, is_allow_once, is_approval_parked, is_reject_once,
        is_settlement_signal, is_terminal_status, load_replay_frames, map_mcp_server,
        map_mcp_servers, parse_attach, parse_cancel, parse_load, parse_prompt, parse_session_new,
        permission_request_params, read_bound, read_gateway_frame, recorded_turn,
        refusal_prompt_result, resolve_attach_target, resolve_resource_link, settlement_verdict,
        stop_reason, tool_call_announcement, tool_call_status_update, turn_deadline,
    };
    use std::str::FromStr as _;

    use crate::client::GatewayUnavailable;
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
    fn session_new_carries_mcp_servers_shape_for_mapping() {
        // Absent and empty carry nothing (TASK 01 S1: shape at parse,
        // depth at map, authority at the gateway).
        let bare = parse_session_new(Some(&json!({"cwd": "/tmp"}))).expect("absent ok");
        assert!(bare.servers.is_empty());
        let empty =
            parse_session_new(Some(&json!({"cwd": "/tmp", "mcpServers": []}))).expect("empty ok");
        assert!(empty.servers.is_empty());
        // Non-empty arrays of objects are carried (deep refusal moved
        // to map_mcp_servers + the S2 standing refusal in server.rs).
        let carried = parse_session_new(Some(&json!({
            "cwd": "/tmp",
            "mcpServers": [{"transport": {"type": "stdio"}, "command": "/usr/bin/mcp-server", "name": "s1"}]
        })))
        .expect("shaped entries carried");
        assert_eq!(carried.servers.len(), 1);
        let not_an_array = parse_session_new(Some(&json!({"cwd": "/tmp", "mcpServers": {}})))
            .expect_err("mcpServers must be an array");
        assert_eq!(not_an_array.data, json!("invalid_params"));
        let non_objects = parse_session_new(Some(&json!({
            "cwd": "/tmp",
            "mcpServers": ["mcp-server"]
        })))
        .expect_err("entries must be objects");
        assert_eq!(non_objects.data, json!("invalid_params"));
    }

    fn mcp_entry(pairs: &serde_json::Value) -> Value {
        let mut base = serde_json::Map::new();
        base.insert("transport".to_owned(), json!({"type": "stdio"}));
        base.insert("name".to_owned(), json!("s1"));
        base.insert("command".to_owned(), json!("/usr/bin/mcp-server"));
        if let Some(object) = pairs.as_object() {
            for (key, value) in object {
                base.insert(key.clone(), value.clone());
            }
        }
        Value::Object(base)
    }

    #[test]
    fn map_mcp_server_accepts_valid_pins_secrets() {
        let descriptor = map_mcp_server(&mcp_entry(&json!({
            "args": ["--flag"],
            "env": [{"name": "MCP_TOKEN", "value": "raw-secret-value"}],
        })))
        .expect("valid entry maps");
        assert_eq!(descriptor.server_id, "s1");
        assert_eq!(descriptor.command, "/usr/bin/mcp-server");
        assert_eq!(descriptor.args.len(), 1);
        assert!(descriptor.args.iter().all(|arg| arg.secret));
        assert_eq!(descriptor.env.len(), 1);
        assert!(descriptor.env.iter().all(|entry| entry.secret));
        assert!(
            !descriptor.env.iter().any(|entry| entry.value.is_empty()),
            "raw values carried for broker pinning, never dropped"
        );
    }

    #[test]
    fn map_mcp_server_refuses_every_malicious_shape() {
        // Each row: tampered entry JSON, expectation that mapping fails
        // with the typed descriptor refusal (fail closed, pins nothing).
        let rows: Vec<(Value, &str)> = vec![
            (
                mcp_entry(&json!({"transport": {"type": "tcp"}})),
                "non-stdio transport",
            ),
            (
                mcp_entry(&json!({"transport": {}})),
                "absent transport type",
            ),
            (
                {
                    let mut entry = mcp_entry(&json!({}));
                    entry.as_object_mut().unwrap().remove("name");
                    entry
                },
                "missing name is never derived",
            ),
            (mcp_entry(&json!({"name": "has/slash"})), "slash in name"),
            (mcp_entry(&json!({"name": ""})), "empty name"),
            (
                mcp_entry(&json!({"command": "relative/bin"})),
                "relative command",
            ),
            (
                {
                    let mut entry = mcp_entry(&json!({}));
                    entry.as_object_mut().unwrap().remove("command");
                    entry
                },
                "missing command",
            ),
            (
                mcp_entry(&json!({"args": vec!["x".to_string(); 33]})),
                "33 args over the bound",
            ),
            (
                mcp_entry(&json!({"args": ["x".repeat(4097)]})),
                "arg over 4KiB",
            ),
            (mcp_entry(&json!({"args": ["a\0b"]})), "NUL arg"),
            (mcp_entry(&json!({"args": "nope"})), "non-array args"),
            (
                mcp_entry(&json!({"env": [{"name": "9BAD", "value": "v"}]})),
                "bad env name",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "LD_PRELOAD", "value": "v"}]})),
                "loader hijack",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "PYTHONPATH", "value": "v"}]})),
                "interpreter injection",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "GIT_CONFIG_X", "value": "v"}]})),
                "config prefix",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "OK", "value": "x".repeat(16385)}]})),
                "env value over 16KiB",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "OK", "value": 42}]})),
                "non-string env value",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "OK"}]})),
                "env entry without value",
            ),
            (
                mcp_entry(&json!({"env": [{"name": "PATH", "value": "/x"}]})),
                "inherited location key override",
            ),
        ];
        for (entry, what) in rows {
            let error = map_mcp_server(&entry).expect_err(&format!("{what} refused"));
            assert_eq!(error.data, json!("invalid_mcp_descriptor"), "{what}");
        }
        // Batch mapping fails closed on the first bad row.
        let mixed = vec![
            mcp_entry(&json!({})),
            mcp_entry(&json!({"command": "relative/bin"})),
        ];
        assert!(map_mcp_servers(&mixed).is_err());
        assert_eq!(
            map_mcp_servers(&[mcp_entry(&json!({}))])
                .expect("all-valid batch maps")
                .len(),
            1
        );
        let duplicate = vec![mcp_entry(&json!({})), mcp_entry(&json!({}))];
        let error = map_mcp_servers(&duplicate).expect_err("duplicate ids refused");
        assert_eq!(error.data, json!("invalid_mcp_descriptor"), "duplicate");
    }

    /// S1 (ticket 01): `parse_load` refuses every bad shape BEFORE the
    /// gateway — the param table is the whole pre-gate contract
    /// (sessionId / cwd / mcpServers), one marker per refusal class.
    #[test]
    fn load_refuses_bad_params_before_the_gateway() {
        let ok = parse_load(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "cwd": "/tmp",
        })))
        .expect("absolute cwd + parseable id accepted");
        assert_eq!(
            ok.session_id.to_string(),
            "01990f9e-1111-7000-8000-000000000000"
        );
        assert_eq!(ok.cwd, "/tmp");
        // mcpServers follows the session/new contract: absent/empty ok.
        parse_load(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "cwd": "/tmp",
            "mcpServers": [],
        })))
        .expect("empty mcpServers ok");

        let relative = parse_load(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "cwd": "relative/dir",
        })))
        .expect_err("relative cwd is rejected");
        assert_eq!(relative.data, json!("cwd_not_absolute"));

        let bad_id = parse_load(Some(&json!({"sessionId": "nope", "cwd": "/tmp"})))
            .expect_err("unparseable sessionId is rejected");
        assert_eq!(bad_id.data, json!("invalid_session_id"));

        let no_cwd = parse_load(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
        })))
        .expect_err("cwd is required");
        assert_eq!(no_cwd.data, json!("invalid_params"));

        let no_id = parse_load(Some(&json!({"cwd": "/tmp"}))).expect_err("sessionId is required");
        assert_eq!(no_id.data, json!("invalid_params"));

        // Shaped entries are carried for the pinned-set assertion
        // (TASK 01 load slice); unshaped entries still stop pre-gateway.
        let carried = parse_load(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "cwd": "/tmp",
            "mcpServers": [{"transport": {"type": "stdio"}, "command": "/usr/bin/srv", "name": "s1"}],
        })))
        .expect("shaped entries carried");
        assert_eq!(carried.servers.len(), 1);
        let unshaped = parse_load(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "cwd": "/tmp",
            "mcpServers": ["srv"],
        })))
        .expect_err("non-object entries refused pre-gateway");
        assert_eq!(unshaped.data, json!("invalid_params"));

        for params in [json!({}), json!([]), json!("nope")] {
            let error = parse_load(Some(&params)).expect_err("bad shape rejected");
            assert_eq!(error.code, INVALID_PARAMS, "{params}");
        }
        assert!(parse_load(None).is_err(), "absent params are invalid");
    }

    #[test]
    fn check_mcp_set_matches_equal_sets_only() {
        let ok = |asserted: &[&str], pinned: &[&str]| {
            check_mcp_set(
                &asserted.iter().map(ToString::to_string).collect::<Vec<_>>(),
                &pinned.iter().map(ToString::to_string).collect::<Vec<_>>(),
            )
        };
        ok(&[], &[]).expect("empty sets match");
        ok(&["s1"], &["s1"]).expect("equal singletons match");
        ok(&["s1", "s2"], &["s2", "s1"]).expect("order is irrelevant");
        for (asserted, pinned, what) in [
            (vec!["s1", "s2"], vec!["s1"], "subset asserted"),
            (vec!["s1"], vec!["s1", "s2"], "superset asserted"),
            (vec!["s1"], vec!["s2"], "drifted id"),
            (vec![], vec!["s1"], "empty asserted against pins"),
            (vec!["s1"], vec![], "pins asserted against empty"),
        ] {
            let error = ok(&asserted, &pinned).expect_err(&format!("{what} fails"));
            assert_eq!(error.data, json!("mcp_set_mismatch"), "{what}");
            assert_eq!(error.code, INVALID_PARAMS, "{what}");
        }
    }

    /// S1 (ticket 01): load validates the PINNED workspace
    /// (ADR-0005:40) — exact equality passes, any other cwd is the one
    /// NEW marker this slice adds (`workspace_mismatch`); every other
    /// refusal reuses an existing marker (N1a1).
    #[test]
    fn load_requires_the_pinned_workspace_root() {
        check_load_workspace("/tmp/ws", "/tmp/ws").expect("equal cwd passes");
        let mismatch = check_load_workspace("/tmp/other", "/tmp/ws")
            .expect_err("a different cwd never rebinds the pinned root");
        assert_eq!(mismatch.code, INVALID_PARAMS);
        assert_eq!(mismatch.data, json!("workspace_mismatch"));
        // Trailing-slash drift is a mismatch too: the pinned root is
        // byte-compared, never normalized at the ACP boundary.
        let slash = check_load_workspace("/tmp/ws/", "/tmp/ws")
            .expect_err("byte equality, no normalization");
        assert_eq!(slash.data, json!("workspace_mismatch"));
    }

    /// S1 (ticket 01): the replay mapper — turn order preserved,
    /// speakers mapped to the two ACP chunk kinds, and corrupt rows
    /// (missing conversation, non-string content, unknown speaker)
    /// logged-and-skipped so one bad entry never wedges the load.
    #[test]
    fn replay_maps_speakers_and_skips_corrupt_rows() {
        let turns = json!([
            {
                "turn_seq": 1,
                "status": "Completed",
                "conversation": [
                    {"speaker": "user", "content": "first question"},
                    {"speaker": "agent", "content": "first answer"},
                ],
            },
            {
                "turn_seq": 2,
                "status": "Completed",
                "conversation": [
                    {"speaker": "user", "content": 42},
                    {"speaker": "assistant", "content": "wrong role name"},
                    {"speaker": "user", "content": "still replayed"},
                ],
            },
            {"turn_seq": 3, "status": "Failed"}
        ]);
        let session = "01990f9e-1111-7000-8000-000000000000";
        let frames = load_replay_frames(session, turns.as_array().expect("turns array"));
        let updates: Vec<Value> = frames
            .iter()
            .map(|frame| {
                let Outbound::Notification { method, params } = frame else {
                    panic!("replay frames are notifications: {frame:?}");
                };
                assert_eq!(method, "session/update");
                assert_eq!(params["sessionId"], json!(session));
                params["update"].clone()
            })
            .collect();
        // 3 of 5 rows survive; order preserved; corrupt rows skipped.
        assert_eq!(updates.len(), 3, "{updates:?}");
        assert_eq!(updates[0]["sessionUpdate"], "user_message_chunk");
        assert_eq!(updates[0]["content"]["text"], "first question");
        assert_eq!(updates[1]["sessionUpdate"], "agent_message_chunk");
        assert_eq!(updates[1]["content"]["text"], "first answer");
        assert_eq!(updates[2]["sessionUpdate"], "user_message_chunk");
        assert_eq!(updates[2]["content"]["text"], "still replayed");
        // No messageId: optional in the schema and nothing durable
        // maps to an ACP message id (an honest absence over invention).
        assert!(
            updates.iter().all(|u| u.get("messageId").is_none()),
            "messageId stays absent"
        );
        // Empty history ⇒ zero frames (the `{}` result is the arm's).
        assert!(load_replay_frames(session, &[]).is_empty());
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

        // resource_link blocks are carried as link assertions (TASK 01
        // S1); other non-text content stays refused.
        let linked = parse_prompt(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "prompt": [
                {"type": "text", "text": "hello"},
                {"type": "resource_link", "uri": "file:///tmp/a"},
            ],
        })))
        .expect("resource_link carried");
        assert_eq!(linked.objective, "hello");
        assert_eq!(linked.links, vec!["file:///tmp/a".to_owned()]);
        let unsupported = parse_prompt(Some(&json!({
            "sessionId": "01990f9e-1111-7000-8000-000000000000",
            "prompt": [{"type": "image", "uri": "file:///tmp/a"}],
        })))
        .expect_err("non-text non-link content is refused");
        assert_eq!(unsupported.data, json!("unsupported_prompt_content"));
    }

    #[test]
    fn resource_link_taxonomy_is_typed() {
        let session = || {
            json!({
                "sessionId": "01990f9e-1111-7000-8000-000000000000",
                "prompt": [{"type": "text", "text": "hello"}],
            })
        };
        // Missing/non-string uri never reaches resolution.
        for (prompt, what) in [
            (json!([{"type": "resource_link"}]), "missing uri"),
            (
                json!([{"type": "resource_link", "uri": 42}]),
                "non-string uri",
            ),
        ] {
            let mut params = session();
            params["prompt"] = prompt;
            let error = parse_prompt(Some(&params)).expect_err(&format!("{what} refused"));
            assert_eq!(error.data, json!("invalid_prompt"), "{what}");
        }
        // Nine links exceed the per-prompt bound.
        let many: Vec<Value> = (0..9)
            .map(|i| json!({"type": "resource_link", "uri": format!("file:///tmp/{i}")}))
            .collect();
        let mut params = session();
        params["prompt"] = Value::Array(many);
        let error = parse_prompt(Some(&params)).expect_err("ninth link refused");
        assert_eq!(error.data, json!("too_many_resources"));
    }

    fn reslink_dir(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("tachyon-reslink-{}-{tag}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("reslink dir");
        dir
    }

    fn reslink_file(dir: &std::path::Path, name: &str, bytes: &[u8]) -> String {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("reslink parent");
        }
        std::fs::write(&path, bytes).expect("reslink file");
        format!("file://{}", path.display())
    }

    #[test]
    fn resolve_resource_link_reads_bounded_text() {
        let dir = reslink_dir("ok");
        let uri = reslink_file(&dir, "a.txt", b"hello evidence");
        let block = resolve_resource_link(&dir, &uri).expect("readable file resolves");
        assert!(
            block.starts_with(&format!("[resource {uri}]\n")),
            "fenced header"
        );
        assert!(block.contains("hello evidence"));
        assert!(block.ends_with("\n[/resource]"));
    }

    /// Leaf-symlink escape case (Unix-only: symlink creation).
    #[cfg(unix)]
    fn symlink_escape_refused(dir: &std::path::Path, outside: &std::path::Path) {
        std::os::unix::fs::symlink(outside.join("secret.txt"), dir.join("link.txt"))
            .expect("symlink");
        let link_uri = format!("file://{}/link.txt", dir.display());
        let error = resolve_resource_link(dir, &link_uri).expect_err("symlink refused");
        assert_eq!(error.data, serde_json::json!("out_of_root"));
    }

    /// Non-Unix platforms have no symlink primitive here; the leaf gate
    /// is still covered by the non-regular-file refusal below.
    #[cfg(not(unix))]
    fn symlink_escape_refused(_dir: &std::path::Path, _outside: &std::path::Path) {}

    #[test]
    fn resolve_resource_link_refuses_every_escape() {
        let dir = reslink_dir("escape");
        reslink_file(&dir, "inside.txt", b"ok");
        let outside = reslink_dir("outside");
        let outside_uri = reslink_file(&outside, "secret.txt", b"nope");
        // Absolute outside-root.
        let error = resolve_resource_link(&dir, &outside_uri).expect_err("outside refused");
        assert_eq!(error.data, serde_json::json!("out_of_root"));
        // Prefix-boundary trap: sibling whose name extends the root.
        // (Materialized: nonexistent paths rightly fail earlier as
        // unresolvable.)
        let sib = format!("{}-suffix", dir.display());
        std::fs::create_dir_all(&sib).expect("sibling");
        std::fs::write(format!("{sib}/x"), b"nope").expect("sibling file");
        let trap = format!("file://{sib}/x");
        let error = resolve_resource_link(&dir, &trap).expect_err("boundary trap refused");
        assert_eq!(error.data, serde_json::json!("out_of_root"));
        // Dot-dot escape to a materialized outside file.
        let outside_file =
            std::env::temp_dir().join(format!("tachyon-reslink-outside-{}", std::process::id()));
        std::fs::write(&outside_file, b"nope").expect("outside file");
        let dots = format!(
            "file://{}/sub/../../{}",
            dir.display(),
            outside_file.file_name().expect("name").to_string_lossy()
        );
        std::fs::create_dir_all(dir.join("sub")).expect("sub");
        let error = resolve_resource_link(&dir, &dots).expect_err("dot-dot refused");
        assert_eq!(error.data, serde_json::json!("out_of_root"));
        symlink_escape_refused(&dir, &outside);
        // Non-file scheme.
        let error = resolve_resource_link(&dir, "https://example.com/x")
            .expect_err("network scheme refused");
        assert_eq!(error.data, serde_json::json!("unsupported_scheme"));
        // Host form: no path to confine.
        let error =
            resolve_resource_link(&dir, "file://host/tmp/x").expect_err("host form refused");
        assert_eq!(error.data, serde_json::json!("unresolvable"));
        // Backslash, plain and percent-encoded (post-decode scan).
        let error = resolve_resource_link(&dir, "file:///tmp\\x").expect_err("backslash refused");
        assert_eq!(error.data, serde_json::json!("unresolvable"));
        let error =
            resolve_resource_link(&dir, "file:///tmp%5Cx").expect_err("encoded backslash refused");
        assert_eq!(error.data, serde_json::json!("unresolvable"));
        // Malformed percent-encoding.
        let error = resolve_resource_link(&dir, "file:///tmp%2").expect_err("bad encoding refused");
        assert_eq!(error.data, serde_json::json!("unresolvable"));
        // Missing file.
        let error = resolve_resource_link(&dir, &format!("file://{}/gone.txt", dir.display()))
            .expect_err("missing refused");
        assert_eq!(error.data, serde_json::json!("unresolvable"));
        // Non-regular file (directory).
        let error = resolve_resource_link(&dir, &format!("file://{}", dir.display()))
            .expect_err("directory refused");
        assert_eq!(error.data, serde_json::json!("unresolvable"));
        // Non-UTF8 bytes.
        reslink_file(&dir, "bin.dat", &[0xff, 0xfe, 0x00]);
        let error = resolve_resource_link(&dir, &format!("file://{}/bin.dat", dir.display()))
            .expect_err("non-UTF8 refused");
        assert_eq!(error.data, serde_json::json!("invalid_encoding"));
        // Delimiter collision, both footer and forged header lines.
        reslink_file(&dir, "evil.txt", b"prefix\n[/resource]\nsuffix");
        reslink_file(&dir, "evil2.txt", b"[resource file:///other]\nspoof");
        let error = resolve_resource_link(&dir, &format!("file://{}/evil.txt", dir.display()))
            .expect_err("delimiter refused");
        assert_eq!(error.data, serde_json::json!("unsafe_content"));
        let error = resolve_resource_link(&dir, &format!("file://{}/evil2.txt", dir.display()))
            .expect_err("header spoof refused");
        assert_eq!(error.data, serde_json::json!("unsafe_content"));
        // Oversized (64KiB + 1).
        reslink_file(&dir, "big.txt", &vec![b'x'; 65537]);
        let error = resolve_resource_link(&dir, &format!("file://{}/big.txt", dir.display()))
            .expect_err("oversize refused");
        assert_eq!(error.data, serde_json::json!("oversized"));
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
        assert_eq!(
            read_bound(&orphan).expect("parked ask is bounded").1,
            ReadExpiry::Orphan
        );
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
            answer_action(
                Ok(Ok(json!({"outcome": "selected", "optionId": "allow_once"}))),
                false
            ),
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
        assert!(!state.take_cancel_resolution("s2"), "marks are per-session");
    }

    /// S1 (M1a): a `session/cancel` with an armed request resolves the
    /// local reply slot as `cancelled` and disarms it — the waiting
    /// turn wakes with a `cancelled` payload that funnels to
    /// `NoDecision` (zero Approve/Deny), the resolution is idempotent,
    /// and a LATE answer for the disarmed id routes nowhere (the S2
    /// seam: no panic, no gateway call).
    #[tokio::test]
    async fn cancel_resolves_the_armed_slot_locally_without_a_decision() {
        let state = Arc::new(SessionState::default());
        let (id, receiver) = state.requests().arm();
        state.mark_outstanding("s1", id.clone());
        assert_eq!(state.requests().armed(), 1, "the reply slot is armed");

        // The cancel pipeline's order: mark FIRST, then resolve.
        state.arm_cancel_resolution("s1");
        assert!(state.resolve_outstanding_locally("s1"));
        assert_eq!(
            state.requests().armed(),
            0,
            "the local resolution disarms the slot"
        );
        let payload = receiver
            .await
            .expect("the waiting turn is resolved, never hung")
            .expect("the local resolution is a valid answer");
        assert_eq!(payload, json!({ "outcome": "cancelled" }));

        // The turn's funnel over that payload: the mark it observed
        // routes it to NoDecision — no Approve, no Deny.
        assert!(state.take_cancel_resolution("s1"), "the mark was set");
        assert!(matches!(
            answer_action(Ok(Ok(payload)), true),
            ClientAnswer::NoDecision
        ));

        // Idempotent: nothing left to resolve or route.
        assert!(!state.resolve_outstanding_locally("s1"));
        let late = InboundResponse {
            id,
            payload: Ok(json!({"outcome": "selected", "optionId": "allow_once"})),
        };
        assert!(
            !state.requests().route(&late),
            "a late answer for the disarmed id routes nowhere"
        );
    }

    /// S1 (arm-time race): a cancel mark set BEFORE the ask arms makes
    /// the ask resolve its own fresh slot immediately — the exchange
    /// can never wait on an answer a cancelling client will not send
    /// (the hang the timeout suspension would otherwise make permanent).
    #[tokio::test]
    async fn ask_after_a_cancel_mark_self_resolves_its_fresh_slot() {
        let state = Arc::new(SessionState::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = "01990f9e-1111-7000-8000-000000000000";
        let ask = json!({
            "t": "ApprovalRequest",
            "v": {"request": {
                "id": "01990f9e-5555-7000-8000-000000000000",
                "summary": "Apply patch to src/lib.rs",
            }},
        });
        state.arm_cancel_resolution(session);

        let mut bridge = PermissionBridge::default();
        bridge
            .ask(session, &ask, &tx, &state)
            .expect("the ask still emits its frames");
        let _announcement = rx.try_recv().expect("tool_call queued");
        let _request = rx.try_recv().expect("request queued");
        assert_eq!(
            state.requests().armed(),
            0,
            "the fresh slot resolved locally at arm time"
        );

        let taken = std::mem::replace(&mut bridge.phase, PermissionPhase::Idle);
        let PermissionPhase::Outstanding { receiver, .. } = taken else {
            panic!("the ask still lands in Outstanding: {taken:?}");
        };
        let raw = receiver.await;
        let cancelled = matches!(
            &raw,
            Ok(Ok(value)) if value.get("outcome").and_then(Value::as_str) == Some("cancelled")
        );
        assert!(cancelled, "the local resolution carries cancelled");
        assert!(state.take_cancel_resolution(session));
        assert!(matches!(answer_action(raw, true), ClientAnswer::NoDecision));
    }

    /// S3 Verify + N3a1 (bound crossing under a paused clock): a
    /// permission request held PAST the 300 s window does not time the
    /// turn out — the budget is suspended while the request is
    /// outstanding, so the only armed timer is the holder's own sleep;
    /// virtual time crosses t+300 s and the turn still answers its
    /// result instead of `turn_timed_out`.
    #[tokio::test(start_paused = true)]
    async fn turn_timeout_is_suspended_during_an_outstanding_request() {
        let (budget, budget_rx) = watch::channel(TurnBudgetState::running());
        let turn = async move {
            // What `PermissionBridge::ask` does when the request arms:
            let current = *budget.borrow();
            budget.send_replace(current.suspend());
            // A human holds the request beyond the whole deadline
            // window (N3a1: 301 s > TURN_TIMEOUT's 300 s).
            tokio::time::sleep(Duration::from_secs(301)).await;
            // What the exchange resolution does (step 2): full budget.
            budget.send_replace(TurnBudgetState::running());
            Ok(json!({ "stopReason": "cancelled", "content": [] }))
        };
        let outcome = turn_deadline(turn, budget_rx).await;
        let result = outcome.expect("a held request never surfaces turn_timed_out");
        assert_eq!(result["stopReason"], "cancelled");
    }

    /// S3 (M3a resume): a budget RESUMED mid-turn gets a FULL window
    /// again — the original deadline fires, the wrapper re-arms at the
    /// resumed deadline, and a turn finishing 540 s in (past the
    /// original 300 s) still answers normally.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_turn_budget_gets_a_full_window() {
        let (budget, budget_rx) = watch::channel(TurnBudgetState::running());
        let turn = async move {
            let current = *budget.borrow();
            budget.send_replace(current.suspend());
            tokio::time::sleep(Duration::from_secs(250)).await;
            budget.send_replace(TurnBudgetState::running());
            tokio::time::sleep(Duration::from_secs(290)).await;
            Ok(json!({ "stopReason": "end_turn", "content": [] }))
        };
        let result = turn_deadline(turn, budget_rx)
            .await
            .expect("the resumed window covers the turn's tail");
        assert_eq!(result["stopReason"], "end_turn");
    }

    /// S3 negative control: a turn that never suspends its budget
    /// still times out at the deadline with the typed `turn_timed_out`
    /// error — suspension is what disables the timer, nothing else
    /// (this is the exact failure the suspension mutation produces).
    #[tokio::test(start_paused = true)]
    async fn an_unsuspended_turn_still_times_out() {
        let (_budget, budget_rx) = watch::channel(TurnBudgetState::running());
        let turn = async {
            tokio::time::sleep(Duration::from_secs(301)).await;
            Ok(json!({ "stopReason": "end_turn", "content": [] }))
        };
        let error = turn_deadline(turn, budget_rx)
            .await
            .expect_err("an unsuspended turn hits its deadline");
        assert_eq!(error.data, json!("turn_timed_out"));
    }

    /// S3 wiring (M3a pause): arming a request through the production
    /// bridge suspends the shared budget — the transition
    /// `turn_timeout_is_suspended_during_an_outstanding_request`
    /// relies on, observed on the real `ask` path.
    #[tokio::test]
    async fn arming_a_request_suspends_the_bridge_budget() {
        let (budget, budget_rx) = watch::channel(TurnBudgetState::running());
        assert!(!budget_rx.borrow().suspended, "a fresh budget runs");
        let state = Arc::new(SessionState::default());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let session = "01990f9e-1111-7000-8000-000000000000";
        let ask = json!({
            "t": "ApprovalRequest",
            "v": {"request": {"id": "01990f9e-5555-7000-8000-000000000000", "summary": "Apply"}},
        });
        let mut bridge = PermissionBridge::new(budget);
        bridge
            .ask(session, &ask, &tx, &state)
            .expect("the ask arms");
        assert!(
            budget_rx.borrow().suspended,
            "an outstanding request suspends the turn deadline"
        );
        let _announcement = rx.try_recv().expect("tool_call queued");
        let _request = rx.try_recv().expect("request queued");
        // The full cycle: resolution runs `running()` (step 2's call).
        let resumed = TurnBudgetState::running();
        assert!(!resumed.suspended, "resolution resumes with a full budget");
    }

    /// Phase 7 review fix: a gateway read issued while the budget is
    /// SUSPENDED keeps the FROZEN pre-suspension deadline — the
    /// wrapper's paused branch has no timer, so without this local
    /// bound a wedged gateway would hang the turn (and the serve
    /// loop's EOF drain) forever. Expiry is the SAME typed
    /// `turn_timed_out` verdict the wrapper produces.
    #[tokio::test(start_paused = true)]
    async fn a_gateway_read_while_suspended_keeps_the_frozen_deadline() {
        let (budget, _rx) = watch::channel(TurnBudgetState {
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            suspended: true,
        });
        let read = async {
            tokio::time::sleep(Duration::from_secs(300)).await;
            Ok::<_, GatewayUnavailable>("a frame the wedged gateway sends too late")
        };
        let error = read_gateway_frame(read, Some(&budget))
            .await
            .expect_err("a wedged gateway read fails typed at the frozen deadline");
        assert_eq!(error.data, json!("turn_timed_out"));
    }

    /// Negative control for the review fix: while the budget RUNS (or
    /// no budget is wired at all), `read_gateway_frame` adds NO local
    /// timer — the deadline wrapper owns the bound, so a slow gateway
    /// read still completes on its own schedule.
    #[tokio::test(start_paused = true)]
    async fn a_gateway_read_while_running_adds_no_local_timer() {
        let (budget, _rx) = watch::channel(TurnBudgetState {
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
            suspended: false,
        });
        let read = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<_, GatewayUnavailable>("frame")
        };
        let frame = read_gateway_frame(read, Some(&budget))
            .await
            .expect("a running budget leaves the bound to the wrapper");
        assert_eq!(frame, "frame");
        // Unit bridges wire no budget at all — same plain-read contract.
        let plain = read_gateway_frame(std::future::ready(Ok::<_, GatewayUnavailable>("ok")), None)
            .await
            .expect("a budget-less read is plain");
        assert_eq!(plain, "ok");
    }

    /// S1 (ticket 02): the recorded-turn record — only the LAST turn
    /// can gate (replay order is `turn_seq ASC`), terminal/empty ⇒ no
    /// record, in-progress ⇒ its id, corrupt row ⇒ typed (never a
    /// guessed record: ADR-0005:39 reconciliation must be exact).
    #[test]
    fn recorded_turn_tracks_only_the_last_turn() {
        let running = json!([
            {"turn_seq": 1, "task_id": "01990f9e-7000-7000-8000-000000000001", "status": "Completed"},
            {"turn_seq": 2, "task_id": "01990f9e-7000-7000-8000-000000000002", "status": "Executing"}
        ]);
        assert_eq!(
            recorded_turn(running.as_array().unwrap()).unwrap(),
            Some(TaskId::from_str("01990f9e-7000-7000-8000-000000000002").unwrap()),
            "the in-progress LAST turn is the record"
        );
        let finished = json!([
            {"turn_seq": 1, "task_id": "01990f9e-7000-7000-8000-000000000002", "status": "Executing"},
            {"turn_seq": 2, "task_id": "01990f9e-7000-7000-8000-000000000003", "status": "Cancelled"}
        ]);
        assert_eq!(
            recorded_turn(finished.as_array().unwrap()).unwrap(),
            None,
            "a terminal LAST turn records nothing (earlier turns never gate)"
        );
        assert_eq!(
            recorded_turn(&[]).unwrap(),
            None,
            "empty history records nothing"
        );
        let corrupt = json!([
            {"turn_seq": 1, "task_id": "not-a-task-id", "status": "Executing"}
        ]);
        let error = recorded_turn(corrupt.as_array().unwrap()).unwrap_err();
        assert_eq!(error.data, json!("gateway_payload_invalid"));
        let no_status = json!([{"turn_seq": 1, "task_id": "01990f9e-7000-7000-8000-000000000002"}]);
        let error = recorded_turn(no_status.as_array().unwrap()).unwrap_err();
        assert_eq!(error.data, json!("gateway_payload_invalid"));
    }

    /// Attach TASK 01: `parse_attach` shape validation — sessionId required
    /// and parsed, `taskId` optional but parsed when present, `from_seq` optional
    /// defaulting to 0 and refusing non-integer cursors. Every failure is a
    /// typed `-32602` with a taxonomy marker (never a guess, never a gateway
    /// call — validation runs before the gate).
    #[test]
    fn parse_attach_validates_shape_before_the_gate() {
        let session = "01990f9e-1111-7000-8000-000000000000";
        let task = "01990f9e-7000-7000-8000-000000000002";
        let params = parse_attach(Some(&json!({"sessionId": session}))).unwrap();
        assert_eq!(params.session_id.to_string(), session);
        assert_eq!(params.task_id, None);
        assert_eq!(params.from_seq, 0, "from_seq defaults to replay-then-tail");
        let params = parse_attach(Some(
            &json!({"sessionId": session, "taskId": task, "from_seq": 41}),
        ))
        .unwrap();
        assert_eq!(params.task_id, Some(TaskId::from_str(task).unwrap()));
        assert_eq!(params.from_seq, 41);
        // Explicit nulls behave as absent (ACP optional-field convention).
        let params = parse_attach(Some(
            &json!({"sessionId": session, "taskId": null, "from_seq": null}),
        ))
        .unwrap();
        assert_eq!(params.task_id, None);
        assert_eq!(params.from_seq, 0);
        for (params, data) in [
            (json!({}), "invalid_params"),
            (json!({"sessionId": 7}), "invalid_params"),
            (json!({"sessionId": "not-a-session"}), "invalid_session_id"),
            (
                json!({"sessionId": session, "taskId": 7}),
                "invalid_task_id",
            ),
            (
                json!({"sessionId": session, "taskId": "nope"}),
                "invalid_task_id",
            ),
            (
                json!({"sessionId": session, "from_seq": -1}),
                "invalid_from_seq",
            ),
            (
                json!({"sessionId": session, "from_seq": 1.5}),
                "invalid_from_seq",
            ),
            (
                json!({"sessionId": session, "from_seq": "41"}),
                "invalid_from_seq",
            ),
        ] {
            let error = parse_attach(Some(&params)).unwrap_err();
            assert_eq!(error.code, INVALID_PARAMS, "shape first: {params}");
            assert_eq!(error.data, json!(data), "taxonomy marker: {params}");
        }
        let error = parse_attach(None).unwrap_err();
        assert_eq!(error.data, json!("invalid_params"));
    }

    /// Attach TASK 01: `resolve_attach_target` — explicit pins win when live,
    /// terminal targets refuse typed (replay is `session/load`'s job), ids
    /// outside the history are `unknown_task`, and the default is the latest
    /// non-terminal turn (`recorded_turn` convention). Corrupt rows fail
    /// closed as `gateway_payload_invalid`, never a guessed target.
    #[test]
    fn resolve_attach_target_pins_live_and_refuses_terminal() {
        let live = "01990f9e-7000-7000-8000-000000000002";
        let done = "01990f9e-7000-7000-8000-000000000001";
        let history = json!([
            {"turn_seq": 1, "task_id": done, "status": "Completed"},
            {"turn_seq": 2, "task_id": live, "status": "Executing"}
        ]);
        let turns = history.as_array().unwrap();
        // Default: the latest non-terminal turn.
        assert_eq!(
            resolve_attach_target(turns, None).unwrap(),
            TaskId::from_str(live).unwrap(),
        );
        // Explicit live pin resolves to itself.
        assert_eq!(
            resolve_attach_target(turns, Some(TaskId::from_str(live).unwrap())).unwrap(),
            TaskId::from_str(live).unwrap(),
        );
        // Explicit terminal pin refuses typed (use session/load).
        let error =
            resolve_attach_target(turns, Some(TaskId::from_str(done).unwrap())).unwrap_err();
        assert_eq!(error.code, INVALID_PARAMS);
        assert_eq!(error.data, json!("attach_target_terminal"));
        // Id outside the history is unknown, not terminal.
        let error = resolve_attach_target(
            turns,
            Some(TaskId::from_str("01990f9e-7000-7000-8000-000000000009").unwrap()),
        )
        .unwrap_err();
        assert_eq!(error.data, json!("unknown_task"));
        // Latest-terminal history refuses the default too (nothing live).
        let settled = json!([
            {"turn_seq": 1, "task_id": live, "status": "Executing"},
            {"turn_seq": 2, "task_id": done, "status": "Cancelled"}
        ]);
        let error = resolve_attach_target(settled.as_array().unwrap(), None).unwrap_err();
        assert_eq!(error.data, json!("attach_target_terminal"));
        // Empty history has no target at all.
        let error = resolve_attach_target(&[], None).unwrap_err();
        assert_eq!(error.data, json!("unknown_task"));
        // Corrupt rows fail closed, never guessed.
        let no_status = json!([
            {"turn_seq": 1, "task_id": live, "status": "Executing"},
            {"turn_seq": 2, "task_id": done}
        ]);
        let error = resolve_attach_target(no_status.as_array().unwrap(), None).unwrap_err();
        assert_eq!(error.data, json!("gateway_payload_invalid"));
    }

    /// S4 (M4a): the orphan bound is TIGHTENED to 2 s and is the one
    /// documented bound — a park with no usable ask fails typed there
    /// (the request-less stub test pins it end-to-end from inside the
    /// bound; this unit pins the value the docs promise).
    #[test]
    fn the_orphan_bound_is_tightened_to_two_seconds() {
        assert_eq!(APPROVAL_REQUEST_GRACE, Duration::from_secs(2));
        let now = std::time::Instant::now();
        let orphan = PermissionPhase::AwaitingRequest {
            deadline: now + APPROVAL_REQUEST_GRACE,
        };
        let (deadline, expiry) = read_bound(&orphan).expect("a parked ask is bounded");
        assert_eq!(expiry, ReadExpiry::Orphan);
        assert!(deadline <= now + Duration::from_secs(2));
    }
}
