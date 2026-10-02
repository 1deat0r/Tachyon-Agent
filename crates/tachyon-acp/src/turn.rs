//! The `session/new`, `session/prompt`, and `session/cancel` pipelines
//! (acp-adapter-lifecycle tickets 02+03): ACP param validation, the
//! adapter-local sequential turn guard, per-call idempotency keys,
//! journal→`session/update` mapping, `stopReason` derivation, the
//! gateway-backed prompt turn, the drain-awaiting cancel, the bounded
//! resubscribe, and the typed `approval_required` refusal for a turn
//! parked on a permission approval.
//!
//! The adapter stays a pure gateway client (ADR-0005): every step below
//! is a framed gateway round trip or a pure mapping — no driver, no
//! tools, no execution, and no grant authority of its own: an approval
//! park is refused typed instead of being resolved by the adapter.
//! Frame shapes follow the pinned ACP `schema-v1.23.0` artifact; where
//! this slice extends an object the schema leaves open (the prompt
//! response's `content` tail), the golden test pins the exact wire
//! bytes.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tachyon_protocol::{Command, GatewayEvent, ServerFrame};
use tachyon_types::{EventId, SessionId, TaskId};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;

use crate::client::{Connector, GatewayCallError, GatewayConn, GatewayUnavailable};
use crate::codec::{
    GATEWAY_REFUSED, GATEWAY_UNAVAILABLE, INVALID_PARAMS, Outbound, RpcId, TURN_CONFLICT,
    TURN_FAILED,
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
/// adapter could not resolve: approvals are not answerable over ACP
/// yet (the `session/request_permission` bridge is a later slice), so
/// the prompt fails typed at the first read that observes the park —
/// never hangs to the turn deadline, never guesses a verdict.
pub(crate) fn approval_parked() -> HandlerError {
    HandlerError::turn_failed(
        "approval_required",
        "turn is parked awaiting a permission approval the adapter could not resolve \
         (no permission bridge ships this slice)",
    )
}

/// Whether a task status is the permission park. Only `WaitingApproval`
/// matches: every other status keeps its existing pipeline meaning.
#[must_use]
pub(crate) fn is_approval_parked(status: &str) -> bool {
    status == "WaitingApproval"
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
}

impl SessionState {
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
/// [`TURN_TIMEOUT`]-bounded. `emit` receives the `session/update`
/// notification frames streamed while the turn runs; the returned value
/// is the final `session/prompt` result.
pub(crate) async fn run_prompt<C: Connector>(
    connector: &C,
    params: PromptParams,
    idempotency_key: String,
    emit: &UnboundedSender<Outbound>,
    notice: watch::Sender<Option<TaskId>>,
) -> Result<Value, HandlerError> {
    let outcome = tokio::time::timeout(
        TURN_TIMEOUT,
        prompt_turn(connector, params, idempotency_key, emit, notice),
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
        // subscribed: no permission exchange can run without a journal
        // stream, and none ships this slice — the park stays typed.
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
    stream_turn(&mut conn, subscribe_id, task_id, session_id, emit).await
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
/// ambiguous verification tail or of an approval park, `Ok(None)` to
/// keep streaming.
fn settlement_verdict(
    task: &Value,
    task_id: TaskId,
    trigger: Option<&str>,
    settlement: Option<&str>,
) -> Result<Option<Value>, HandlerError> {
    let status = task
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if is_terminal_status(status) {
        return final_prompt_result(task).map(Some);
    }
    if is_approval_parked(status) {
        // Parked on a permission approval: no bridge ships this slice,
        // so the prompt fails typed at the first read that observes the
        // park — never hangs to the turn deadline, never guesses a
        // verdict.
        return Err(approval_parked());
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
/// `session/update` frames, watches the settlement signals, fetches
/// `GetTask` when one fires, and answers with the final response (or a
/// typed error for an ambiguous status / approval park / persistent
/// resync / transport loss). On `ResyncRequired` it re-subscribes once
/// at the gateway-provided cursor (the ack's replay restores
/// continuity) and fails typed if the subscription overflows again —
/// never a silent gap.
// One narrative: settle → read → dispatch, kept together for its
// ordering proofs.
#[allow(clippy::too_many_lines)]
async fn stream_turn(
    conn: &mut GatewayConn,
    subscribe_id: EventId,
    task_id: TaskId,
    session_id: SessionId,
    emit: &UnboundedSender<Outbound>,
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
    loop {
        // 1. Settlement read.
        if awaiting.is_none()
            && let Some(kind) = settlement.take()
        {
            trigger = Some(kind);
            awaiting = Some(send_frame(conn, Command::GetTask { task_id }).await?);
            awaiting_subscribe = false;
        }
        // 2. Read the next gateway frame.
        let frame = conn
            .read_frame()
            .await
            .map_err(|error| HandlerError::unavailable(&error))?;
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
                    process_replay(&payload, &session_id, emit, &mut settlement);
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
                    )? {
                        return Ok(result);
                    }
                    // A non-terminal bounce: keep streaming; any signal
                    // that arrived mid-round-trip serves next loop.
                }
            }
            ServerFrame::Response(_) => {
                tracing::debug!("ignoring a response for another request id mid-turn");
            }
            ServerFrame::Event(envelope) => match envelope.event {
                GatewayEvent::Journal { kind, payload } => {
                    handle_journal(&kind, &payload, &session_id, emit, &mut settlement);
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
fn process_replay(
    payload: &Value,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
    settlement: &mut Option<String>,
) {
    let Some(rows) = payload.get("events").and_then(Value::as_array) else {
        return;
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
        handle_journal(kind, &value, session_id, emit, settlement);
    }
}

/// Forwards one journalled event (kind + raw payload) as ACP output:
/// `agent_message` → a `session/update` chunk, settlement kinds arm the
/// `GetTask` read, every other kind is omitted.
fn handle_journal(
    kind: &str,
    payload: &Value,
    session_id: &str,
    emit: &UnboundedSender<Outbound>,
    settlement: &mut Option<String>,
) {
    if let Some(update) = agent_chunk(kind, payload) {
        let params = json!({ "sessionId": session_id, "update": update });
        if emit
            .send(Outbound::notification("session/update", params))
            .is_err()
        {
            tracing::warn!("client went away; dropping session/update chunk");
        }
    }
    if is_settlement_signal(kind) {
        *settlement = Some(kind.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        HandlerError, SessionState, agent_chunk, approval_parked, final_prompt_result,
        is_approval_parked, is_settlement_signal, is_terminal_status, parse_cancel, parse_prompt,
        parse_session_new, stop_reason,
    };
    use crate::codec::{INVALID_PARAMS, RpcId, TURN_CONFLICT, TURN_FAILED};

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
}
