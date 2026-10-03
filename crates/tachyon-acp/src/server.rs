//! The ACP stdio server loop: ND-JSON-RPC dispatch, the gateway
//! liveness gate, the `initialize` handshake, the `session/new` /
//! `session/prompt` / `session/cancel` arms (acp-adapter-lifecycle
//! tickets 02+03), and the read-only `session/load` replay
//! (session-load ticket 01).
//!
//! Param validation runs BEFORE the [`GatewayProbe`] gate (a malformed
//! `session/new`/`session/prompt`/`session/cancel`/`session/load` is
//! refused without ever touching the gateway); once params validate,
//! every id-bearing request is gated on the probe first (ADR-0005:35: gateway down ⇒
//! one clear actionable typed error for any request; the adapter never
//! starts the gateway). Notifications are consumed without reply —
//! except `session/cancel`, whose ACP notification form runs the same
//! drain-awaiting pipeline and simply writes no frame. `session/prompt`
//! runs spawned — ACP v1 turns are sequential per session but the loop
//! must keep serving (overlap refusal, other methods) while a turn
//! streams — and streams `session/update` frames back through the
//! single writer. `session/cancel` runs INLINE: the loop awaits the
//! Supervisor's drain acknowledgement before answering, and while it
//! does, nothing else is read or written — which is what pins the frame
//! order (cancel reply first, the resolved prompt verdict after it).
//! `session/load` answers through the writer channel too: its replay
//! notifications must precede its `{}` result in FIFO order.
//! Remaining `session/*` arms (`session/resume`, `session/close`, …)
//! answer the standard JSON-RPC method-not-found error.

use std::io;
use std::sync::Arc;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinSet;

use crate::client::{Connector, GatewayProbe, GatewayUnavailable};
use crate::codec::{
    GATEWAY_UNAVAILABLE, INVALID_PARAMS, Notification, Outbound, Parsed, Peer, Request, RpcId,
};
use crate::turn::{
    GateVerdict, HandlerError, PromptParams, SessionNewParams, SessionState, TurnGuard, call_error,
    parse_cancel, parse_load, parse_prompt, parse_session_new, recorded_gate_verdict, run_cancel,
    run_load, run_prompt,
};

/// ACP wire protocol version Tachyon speaks (ADR-0005:21 — negotiated
/// as integer `protocolVersion: 1` during `initialize`).
const ACP_PROTOCOL_VERSION: u64 = 1;

/// Invalid-params message for `initialize` (its params are the only
/// ones this slice validates).
const INVALID_INITIALIZE_PARAMS: &str =
    "Invalid params: initialize requires an integer protocolVersion";

/// Serves the ACP loop on `reader`/`writer` until EOF.
///
/// Generic over `AsyncRead`/`AsyncWrite` so `main` drives real stdio
/// while tests drive one tokio duplex. stdout (here: `writer`) carries
/// only valid ACP frames; logs go to the tracing subscriber (stderr in
/// the binary). Inline methods answer in read order; spawned
/// `session/prompt` frames join the same single writer through a
/// channel, so stdout never interleaves partial lines.
pub async fn serve<R, W, P, C>(reader: R, writer: W, probe: P, connector: C) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    P: GatewayProbe,
    C: Connector,
{
    let mut peer = Peer::new(reader, writer);
    let (tx, mut rx) = mpsc::unbounded_channel::<Outbound>();
    let handler = Handler {
        probe,
        connector,
        state: Arc::new(SessionState::default()),
        tx,
    };
    let mut turns: JoinSet<()> = JoinSet::new();
    let mut outcome = Ok(());
    loop {
        tokio::select! {
            next = peer.read() => {
                let Some(parsed) = next.transpose()? else {
                    break; // EOF: client closed stdin
                };
                match parsed {
                    Parsed::Request(request) => {
                        if let Some(outbound) = handler.handle_request(request, &mut turns).await {
                            peer.write_line(&outbound.to_line()).await?;
                        }
                    }
                    Parsed::Notification(notification) => {
                        handler.handle_notification(notification).await;
                    }
                    Parsed::Response(response) => {
                        // An answer to an adapter-minted outbound request
                        // (`session/request_permission`) goes to the turn
                        // waiting on its armed slot; anything else — a
                        // LATE answer after a cancel resolved its slot,
                        // or a purely unsolicited frame — is dropped
                        // locally at info level: no panic, no gateway
                        // call, never routed to a different waiter.
                        if handler.state.requests().route(&response) {
                            tracing::debug!(
                                id = ?response.id,
                                "routed an outbound request response to its waiting turn"
                            );
                        } else {
                            tracing::info!(
                                id = ?response.id,
                                "response frame has no armed slot; dropping it \
                                 (late or unsolicited)"
                            );
                        }
                    }
                    Parsed::Failure(failure) => {
                        peer.write_line(&failure.to_line()).await?;
                    }
                }
            }
            outbound = rx.recv() => {
                let Some(outbound) = outbound else {
                    break; // unreachable while the handler holds a sender
                };
                peer.write_line(&outbound.to_line()).await?;
            }
        }
    }
    // EOF (or read error): release the loop's sender, wake every turn
    // waiting on an outstanding permission request (no client is left
    // to answer it), then drain every frame an in-flight turn still
    // owes the client, bounded by the turn deadline; then reap the
    // turn tasks.
    let state = Arc::clone(&handler.state);
    drop(handler);
    state.requests().clear();
    while let Some(outbound) = rx.recv().await {
        if let Err(error) = peer.write_line(&outbound.to_line()).await {
            outcome = outcome.and(Err(error));
            break;
        }
    }
    while let Some(joined) = turns.join_next().await {
        if let Err(error) = joined {
            tracing::error!(%error, "session/prompt task failed unexpectedly");
        }
    }
    outcome
}

/// Dispatch: param validation, the liveness gate, inline answers, and
/// the spawned `session/prompt` pipeline.
struct Handler<P, C> {
    probe: P,
    connector: C,
    state: Arc<SessionState>,
    tx: UnboundedSender<Outbound>,
}

impl<P, C> Handler<P, C>
where
    P: GatewayProbe,
    C: Connector,
{
    /// Handles one id-bearing request. `Ok(None)` means the answer will
    /// arrive through the writer channel (a spawned turn); `Ok(Some)`
    /// is written inline, in read order.
    async fn handle_request(&self, request: Request, turns: &mut JoinSet<()>) -> Option<Outbound> {
        let Request { id, method, params } = request;
        match method.as_str() {
            // Validation first, then the liveness gate: a malformed
            // initialize never reaches the gateway (its params are
            // client-side truth, gateway-independent).
            "initialize" => match initialize_result(params.as_ref()) {
                Ok(result) => {
                    if let Some(error) = self.gate(id.clone(), &method).await {
                        return Some(error);
                    }
                    tracing::info!(
                        "initialize: negotiated protocolVersion={ACP_PROTOCOL_VERSION} \
                         (loadSession=true, text-only prompts)"
                    );
                    Some(Outbound::success(id, result))
                }
                Err(message) => Some(Outbound::error(Some(id), INVALID_PARAMS, message)),
            },
            "session/new" => {
                let SessionNewParams { cwd } = match parse_session_new(params.as_ref()) {
                    Ok(params) => params,
                    Err(error) => return Some(error_outbound(id, error)),
                };
                if let Some(error) = self.gate(id.clone(), &method).await {
                    return Some(error);
                }
                let outcome = self.session_new(cwd).await;
                Some(match outcome {
                    Ok(session_id) => Outbound::success(id, json!({ "sessionId": session_id })),
                    Err(error) => error_outbound(id, error),
                })
            }
            "session/prompt" => {
                let params: PromptParams = match parse_prompt(params.as_ref()) {
                    Ok(params) => params,
                    Err(error) => return Some(error_outbound(id, error)),
                };
                // The turn slot is taken HERE — synchronously, inside
                // the sequential read loop — so a second overlapping
                // prompt is refused before it can ever observe the
                // gateway. The guard drops with this handler on a gate
                // failure and lives in the spawned turn otherwise.
                let guard = match self.state.try_acquire_turn(&params.session_id.to_string()) {
                    Ok(guard) => guard,
                    Err(error) => return Some(error_outbound(id, error)),
                };
                if let Some(error) = self.gate(id.clone(), &method).await {
                    return Some(error);
                }
                // ADR-0005:39 reconciliation: a load-recorded turn that
                // is still running refuses this prompt typed (same
                // `-32003` overlap class as the sequential guard — the
                // guard above drops on this return, so the slot never
                // wedges). No record ⇒ zero-cost early return, no
                // connection opened.
                if let Err(error) = self
                    .recorded_turn_gate(&params.session_id.to_string())
                    .await
                {
                    return Some(error_outbound(id, error));
                }
                // A same-call retry reuses the key its first delivery
                // minted; a fresh call id always mints a new one.
                let (key, reused) = self.state.key_for(&id);
                if reused {
                    tracing::info!(
                        session = %params.session_id,
                        "same-call retry: reusing the original idempotency key"
                    );
                }
                self.spawn_turn(id, params, key, guard, turns);
                None
            }
            "session/cancel" => {
                let session_id = match parse_cancel(params.as_ref()) {
                    Ok(session_id) => session_id,
                    Err(error) => return Some(error_outbound(id, error)),
                };
                if let Some(error) = self.gate(id.clone(), &method).await {
                    return Some(error);
                }
                // Awaited INLINE, before the reply: the drain
                // acknowledgement (the awaited `CancelTask` response)
                // always precedes this frame on the wire, and because
                // the loop serves nothing else while this awaits, the
                // resolved prompt's `cancelled` verdict can only be
                // written after this reply — the pinned frame order.
                match run_cancel(&self.connector, &self.state, session_id).await {
                    Ok(()) => Some(Outbound::success(id, json!({}))),
                    Err(error) => Some(error_outbound(id, error)),
                }
            }
            "session/load" => return self.session_load(id, method, params).await,
            // Unimplemented `session/*` methods (`session/resume`,
            // `session/close`, … — `session/load` has a real arm since
            // its slice) keep the standard method-not-found slot.
            method if method.starts_with("session/") => {
                if let Some(error) = self.gate(id.clone(), method).await {
                    return Some(error);
                }
                tracing::warn!(method, "session method not implemented in this slice");
                Some(Outbound::method_not_found(id))
            }
            method => {
                if let Some(error) = self.gate(id.clone(), method).await {
                    return Some(error);
                }
                tracing::warn!(method, "unknown method");
                Some(Outbound::method_not_found(id))
            }
        }
    }

    /// Consumes one notification. ACP models `session/cancel` as a
    /// client NOTIFICATION (schema `CancelNotification`): it runs the
    /// same validation → gate → drain-awaiting pipeline as the
    /// id-bearing form but NEVER writes a reply frame — the in-flight
    /// prompt's `cancelled` verdict is the observable outcome. Every
    /// other notification is consumed without reply (ACP: notifications
    /// never get a response frame).
    async fn handle_notification(&self, notification: Notification) {
        if notification.method.as_str() != "session/cancel" {
            consume_notification(&notification);
            return;
        }
        let session_id = match parse_cancel(notification.params.as_ref()) {
            Ok(session_id) => session_id,
            Err(error) => {
                tracing::warn!(
                    code = error.code,
                    message = %error.message,
                    "invalid session/cancel notification dropped (never answered)"
                );
                return;
            }
        };
        if let Err(unavailable) = self.probe.probe().await {
            tracing::warn!(
                detail = %unavailable.detail,
                "session/cancel notification: gateway unavailable; dropped"
            );
            return;
        }
        if let Err(error) = run_cancel(&self.connector, &self.state, session_id).await {
            tracing::warn!(
                code = error.code,
                message = %error.message,
                "session/cancel notification failed (logged, never answered)"
            );
        }
    }

    /// The liveness gate: `Some` carries the one typed gateway-down
    /// error every request fails with (ADR-0005:35); `None` opens the
    /// request to its gateway work.
    async fn gate(&self, id: RpcId, method: &str) -> Option<Outbound> {
        if let Err(unavailable) = self.probe.probe().await {
            tracing::warn!(
                method,
                detail = %unavailable.detail,
                "gateway unavailable; failing request with one typed error"
            );
            return Some(gateway_unavailable_outbound(id, &unavailable));
        }
        None
    }

    /// `session/new` → gateway `CreateSession` with the absolute root;
    /// the response `sessionId` IS the durable gateway session id
    /// (identity mapping, no alias store).
    async fn session_new(&self, cwd: String) -> Result<String, HandlerError> {
        let mut conn = self
            .connector
            .connect()
            .await
            .map_err(|error| HandlerError::unavailable(&error))?;
        let payload = conn
            .call(tachyon_protocol::Command::CreateSession {
                workspace_root: Some(cwd),
            })
            .await
            .map_err(call_error)?;
        payload
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                HandlerError::turn_failed(
                    "gateway_payload_invalid",
                    "CreateSession answered without a session_id",
                )
            })
    }

    /// The read-only `session/load` arm: validate → liveness gate →
    /// replay. Creates nothing (ADR-0005:40), so no turn slot and no
    /// idempotency key. EVERY frame — the replay notifications and the
    /// `{}` result — goes through the single writer channel, never the
    /// inline return: an inline write would land BEFORE the queued
    /// notifications, and ADR-0005:29 requires the replay to precede
    /// the response. `None` = "answered through the writer channel";
    /// any failure answers inline with zero frames queued.
    async fn session_load(
        &self,
        id: RpcId,
        method: String,
        params: Option<Value>,
    ) -> Option<Outbound> {
        let params = match parse_load(params.as_ref()) {
            Ok(params) => params,
            Err(error) => return Some(error_outbound(id, error)),
        };
        if let Some(error) = self.gate(id.clone(), &method).await {
            return Some(error);
        }
        let session_key = params.session_id.to_string();
        match run_load(&self.connector, params).await {
            Ok(outcome) => {
                // ADR-0005:39: the record is RE-DERIVED per successful
                // load — a terminal (or empty) history clears a stale
                // record so the prompt gate never reads a dead gate.
                self.state.set_recorded(&session_key, outcome.recorded);
                let mut frames = outcome.frames;
                frames.push(Outbound::success(id, json!({})));
                for frame in frames {
                    if self.tx.send(frame).is_err() {
                        tracing::warn!("client went away; dropping session/load frames");
                        break;
                    }
                }
                None
            }
            Err(error) => Some(error_outbound(id, error)),
        }
    }

    /// The ADR-0005:39 recorded-turn gate: if `session/load` recorded a
    /// non-terminal turn for this session, one fresh `GetTask` decides —
    /// terminal/`unknown_task` ⇒ clear the record (scoped to session +
    /// task) and accept; still running ⇒ typed `-32003
    /// turn_in_progress`; any gateway failure ⇒ typed error (fail
    /// closed, never a guessed release). No record ⇒ `Ok(())` without
    /// touching the gateway.
    async fn recorded_turn_gate(&self, session_id: &str) -> Result<(), HandlerError> {
        let Some(task_id) = self.state.recorded_task(session_id) else {
            return Ok(());
        };
        let mut conn = self
            .connector
            .connect()
            .await
            .map_err(|error| HandlerError::unavailable(&error))?;
        let task = conn
            .call(tachyon_protocol::Command::GetTask { task_id })
            .await;
        match recorded_gate_verdict(task) {
            GateVerdict::Release => {
                self.state.clear_recorded_if(session_id, task_id);
                tracing::info!(
                    %task_id,
                    "recorded turn reached a terminal status; prompt gate released"
                );
                Ok(())
            }
            GateVerdict::Block => Err(HandlerError::recorded_turn_in_progress()),
            GateVerdict::Fail(error) => Err(error),
        }
    }

    /// Spawns one `session/prompt` turn: the pipeline runs detached
    /// (so the loop keeps serving) while its guard keeps the session's
    /// turn slot until the final frame is written through the writer
    /// channel — one final response or error, plus any `session/update`
    /// chunks streamed first.
    fn spawn_turn(
        &self,
        id: RpcId,
        params: PromptParams,
        key: String,
        guard: TurnGuard,
        turns: &mut JoinSet<()>,
    ) {
        let connector = self.connector.clone();
        let tx = self.tx.clone();
        let state = Arc::clone(&self.state);
        let notice = guard.notice();
        turns.spawn(async move {
            let _guard = guard;
            let outcome = run_prompt(&connector, params, key, &tx, notice, &state).await;
            let outbound = match outcome {
                Ok(result) => Outbound::success(id, result),
                Err(error) => error_outbound(id, error),
            };
            let _ignored = tx.send(outbound);
        });
    }
}

/// The one typed gateway-down error, byte-stable (ticket 01 contract).
fn gateway_unavailable_outbound(id: RpcId, unavailable: &GatewayUnavailable) -> Outbound {
    Outbound::error_with_data(
        Some(id),
        GATEWAY_UNAVAILABLE,
        unavailable.to_string(),
        json!("gateway_unavailable"),
    )
}

/// A typed adapter error as its JSON-RPC error frame.
fn error_outbound(id: RpcId, error: HandlerError) -> Outbound {
    Outbound::error_with_data(Some(id), error.code, error.message, error.data)
}

/// Notifications are consumed without reply, whatever their method
/// (ACP: notifications never get a response frame).
fn consume_notification(notification: &Notification) {
    match notification.method.as_str() {
        "notifications/initialized" => tracing::info!("accepted notifications/initialized"),
        method => tracing::debug!(method, "notification consumed without reply"),
    }
}

/// Builds the `initialize` result: negotiate `protocolVersion: 1` and
/// advertise exactly the implemented capability set (ADR-0005:27-31).
fn initialize_result(params: Option<&Value>) -> Result<Value, String> {
    let protocol_version = params
        .and_then(Value::as_object)
        .and_then(|object| object.get("protocolVersion"))
        .and_then(Value::as_u64)
        .ok_or_else(|| INVALID_INITIALIZE_PARAMS.to_owned())?;
    // ACP version negotiation: when the client's latest version is not
    // the one we speak, the agent answers with the latest version it
    // supports — Tachyon speaks v1 only (ADR-0005:21).
    tracing::debug!(protocol_version, "initialize version negotiation");
    Ok(serde_json::to_value(InitializeResult::new()).expect("initialize result serializes"))
}

/// The `initialize` result shape (ACP v1 initialization response),
/// pinned byte-exact by the golden test. Only implemented capabilities
/// appear; wire order is the alphabetical `serde_json` map order.
#[derive(Serialize)]
struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    protocol_version: u64,
    #[serde(rename = "agentCapabilities")]
    agent_capabilities: AgentCapabilities,
    #[serde(rename = "agentInfo")]
    agent_info: Implementation,
    #[serde(rename = "authMethods")]
    auth_methods: Vec<Value>,
}

#[derive(Serialize)]
struct AgentCapabilities {
    /// `loadSession: true` — BOTH halves of the load contract ship
    /// (replay arm + recorded-turn prompt gate, tickets 01/02), so the
    /// advertise-only-implemented rule (ADR-0005:29/31) permits the
    /// flip.
    #[serde(rename = "loadSession")]
    load_session: bool,
    #[serde(rename = "promptCapabilities")]
    prompt_capabilities: PromptCapabilities,
}

#[derive(Serialize)]
struct PromptCapabilities {
    image: bool,
    audio: bool,
    #[serde(rename = "embeddedContext")]
    embedded_context: bool,
}

#[derive(Serialize)]
struct Implementation {
    name: &'static str,
    title: &'static str,
    version: &'static str,
}

impl InitializeResult {
    fn new() -> Self {
        Self {
            protocol_version: ACP_PROTOCOL_VERSION,
            agent_capabilities: AgentCapabilities {
                load_session: true,
                prompt_capabilities: PromptCapabilities {
                    image: false,
                    audio: false,
                    embedded_context: false,
                },
            },
            agent_info: Implementation {
                name: "tachyon-acp",
                title: "Tachyon ACP adapter",
                version: env!("CARGO_PKG_VERSION"),
            },
            auth_methods: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{ACP_PROTOCOL_VERSION, serve};
    use crate::client::{GatewayProbe, GatewayUnavailable};
    use crate::codec::GATEWAY_UNAVAILABLE;
    use crate::test_support::{GatewayDown, GatewayUp, drive};

    fn initialize_line(id: &str, version: u64) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"initialize","params":{{"protocolVersion":{version}}}}}"#
        )
    }

    /// Golden advertisement: byte-exact, so any capability drift (a new
    /// claim, a renamed field, a reordered object) fails review here
    /// first. The version token tracks the crate version instead of
    /// pinning `0.0.1` literally. Wire order is the deterministic
    /// alphabetical order of `serde_json` maps (the result crosses a
    /// `Value` before encoding), not the struct declaration order.
    #[tokio::test]
    async fn initialize_advertisement_is_byte_exact() {
        let replies = drive(&[&initialize_line("0", 1)], GatewayUp).await;
        let golden = r#"{"jsonrpc":"2.0","id":0,"result":{"agentCapabilities":{"loadSession":true,"promptCapabilities":{"audio":false,"embeddedContext":false,"image":false}},"agentInfo":{"name":"tachyon-acp","title":"Tachyon ACP adapter","version":"@VERSION@"},"authMethods":[],"protocolVersion":1}}"#
            .replace("@VERSION@", env!("CARGO_PKG_VERSION"));
        assert_eq!(replies.len(), 1, "exactly one frame: {replies:?}");
        assert_eq!(replies[0], golden);
    }

    /// Protocol negotiation: we always answer with the version we
    /// speak (v1), per ACP initialization rules.
    #[tokio::test]
    async fn initialize_always_negotiates_protocol_version_one() {
        let replies = drive(
            &[&initialize_line("1", 1), &initialize_line("2", 2)],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 2);
        for reply in &replies {
            let frame: Value = serde_json::from_str(reply).unwrap();
            assert_eq!(frame["result"]["protocolVersion"], ACP_PROTOCOL_VERSION);
        }
    }

    /// Malformed `initialize` params are a typed invalid-params error,
    /// not a panic or a guessed advertisement.
    #[tokio::test]
    async fn initialize_with_bad_params_is_invalid_params() {
        let replies = drive(
            &[
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
                r#"{"jsonrpc":"2.0","id":2,"method":"initialize","params":[1]}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"initialize","params":{"protocolVersion":"one"}}"#,
            ],
            GatewayUp,
        )
        .await;
        assert_eq!(replies.len(), 3);
        for reply in &replies {
            let frame: Value = serde_json::from_str(reply).unwrap();
            assert_eq!(frame["error"]["code"], crate::codec::INVALID_PARAMS);
            assert_eq!(
                frame["error"]["message"],
                "Invalid params: initialize requires an integer protocolVersion"
            );
        }
    }

    /// The gateway-down contract: every id-bearing request — any
    /// method — fails with ONE clear actionable typed error, and the
    /// notification still gets no reply. No process is spawned by
    /// construction here (a stub probe cannot spawn); the subprocess
    /// test proves the same for the real binary and endpoint file.
    #[tokio::test]
    async fn gateway_down_fails_every_request_with_one_typed_error() {
        let replies = drive(
            &[
                &initialize_line("1", 1),
                r#"{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}"#,
                r#"{"jsonrpc":"2.0","id":3,"method":"bogus/method"}"#,
                r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            ],
            GatewayDown,
        )
        .await;
        assert_eq!(
            replies.len(),
            3,
            "one error per request, none for the notification: {replies:?}"
        );
        let expected_ids = [
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ];
        for (reply, expected_id) in replies.iter().zip(expected_ids) {
            let frame: Value = serde_json::from_str(reply).unwrap();
            assert_eq!(frame["error"]["code"], GATEWAY_UNAVAILABLE);
            assert_eq!(frame["error"]["data"], "gateway_unavailable");
            let message = frame["error"]["message"].as_str().unwrap();
            assert!(
                message.starts_with("Tachyon gateway unavailable:"),
                "unhelpful message: {message}"
            );
            assert!(
                message.contains("`tachyon gateway`"),
                "message must say how to start the gateway: {message}"
            );
            assert_eq!(frame["id"], expected_id, "each error echoes its request id");
        }
    }

    /// The liveness gate is the ONLY reason `initialize` can fail when
    /// the gateway is down — with a live gateway the same line succeeds.
    #[tokio::test]
    async fn initialize_succeeds_once_the_gateway_is_up() {
        let replies = drive(&[&initialize_line("4", 1)], GatewayUp).await;
        assert_eq!(replies.len(), 1);
        let frame: Value = serde_json::from_str(&replies[0]).unwrap();
        assert!(frame.get("error").is_none());
        assert_eq!(frame["result"]["protocolVersion"], 1);
    }

    /// Probe errors are surfaced verbatim in the message detail.
    #[tokio::test]
    async fn probe_detail_is_carried_into_the_error_message() {
        #[derive(Clone)]
        struct CustomDown;
        impl GatewayProbe for CustomDown {
            fn probe(
                &self,
            ) -> impl std::future::Future<Output = Result<(), GatewayUnavailable>> + Send
            {
                std::future::ready(Err(GatewayUnavailable::new(
                    "cannot connect to /nowhere/gateway.sock",
                )))
            }
        }
        let replies = drive(&[&initialize_line("5", 1)], CustomDown).await;
        let frame: Value = serde_json::from_str(&replies[0]).unwrap();
        assert!(
            frame["error"]["message"]
                .as_str()
                .unwrap()
                .contains("cannot connect to /nowhere/gateway.sock")
        );
    }

    /// The loop returns cleanly on EOF (client closed its stdin).
    #[tokio::test]
    async fn serve_returns_cleanly_at_eof() {
        use tokio::io::{AsyncWriteExt as _, duplex};
        let (a, mut client) = duplex(4096);
        let (reader, writer) = tokio::io::split(a);
        client.shutdown().await.unwrap();
        serve(reader, writer, GatewayUp, crate::test_support::NoConnector)
            .await
            .unwrap();
    }
}
