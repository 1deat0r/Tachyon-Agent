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
use std::time::Duration;
use tachyon_types::{ApprovalId, SessionId};
use tokio::io::{AsyncRead, AsyncWrite};

use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::JoinSet;

use crate::client::{Connector, GatewayConn, GatewayProbe, GatewayUnavailable};
use crate::codec::{
    GATEWAY_UNAVAILABLE, INVALID_PARAMS, Notification, Outbound, Parsed, Peer, Request, RpcId,
    TURN_FAILED,
};
use crate::turn::{
    HandlerError, PromptParams, SessionNewParams, SessionState, TurnGuard, call_error,
    is_allow_once, is_reject_once, map_mcp_servers, parse_cancel, parse_load, parse_prompt,
    parse_session_new, run_cancel, run_load, run_prompt,
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
                        } else if handler
                            .state
                            .take_consumed_mcp_approval(&response.id)
                        {
                            // A late answer for a consumed MCP approval:
                            // diagnosed once with the typed taxonomy
                            // marker instead of dropping silently
                            // (TASK 01 S3).
                            tracing::info!(
                                id = ?response.id,
                                "late MCP approval answer after its approval resolved"
                            );
                            peer.write_line(
                                &Outbound::error_with_data(
                                    Some(response.id.clone()),
                                    TURN_FAILED,
                                    "the MCP approval already resolved",
                                    json!("stale_mcp_approval"),
                                )
                                .to_line(),
                            )
                            .await?;
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

/// Emits one `session/request_mcp_approval` for a registered set
/// and returns its armed reply slot (TASK 01 S2): the slot arms before
/// the request queues, so an answer can never outrun it. A dead writer
/// denies best-effort (no orphaned park) and answers the disconnect
/// typed.
async fn emit_mcp_approval(
    conn: &mut GatewayConn,
    state: &Arc<SessionState>,
    tx: &UnboundedSender<Outbound>,
    session_id: &str,
    gateway_session: SessionId,
    approval_id: &ApprovalId,
    register_payload: Value,
) -> Result<
    (
        RpcId,
        tokio::sync::oneshot::Receiver<Result<Value, crate::codec::ErrorObject>>,
    ),
    HandlerError,
> {
    let server_list: Vec<Value> = register_payload
        .get("servers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let (request_id, receiver) = state.requests().arm();
    if tx
        .send(Outbound::request(
            request_id.clone(),
            "session/request_mcp_approval",
            serde_json::json!({
                "sessionId": session_id,
                "approvalId": approval_id.to_string(),
                "servers": server_list,
            }),
        ))
        .is_err()
    {
        let _ = conn
            .call(tachyon_protocol::Command::DenyMCPServers {
                session_id: gateway_session,
                approval_id: *approval_id,
                reason: "setup client went away before answering".to_owned(),
            })
            .await;
        return Err(HandlerError::turn_failed(
            "mcp_approval_eof",
            "client went away before answering the MCP approval",
        ));
    }
    Ok((request_id, receiver))
}

/// Creates the gateway session and registers the mapped server set
/// (TASK 01 S2): returns the session id, its typed form, the parked
/// approval id, and the register payload (server inventory for the
/// approval ask). Any failure pins nothing — registration never runs
/// before a session exists, and mapping never runs before validation.
async fn create_and_register(
    conn: &mut GatewayConn,
    cwd: String,
    descriptors: Vec<tachyon_protocol::McpServerDescriptor>,
) -> Result<(String, SessionId, ApprovalId, Value), HandlerError> {
    let payload = conn
        .call(tachyon_protocol::Command::CreateSession {
            workspace_root: Some(cwd),
        })
        .await
        .map_err(call_error)?;
    let session_id = payload
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "CreateSession answered without a session_id",
            )
        })?;
    let gateway_session: SessionId = session_id.parse().map_err(|_| {
        HandlerError::turn_failed(
            "gateway_payload_invalid",
            "CreateSession session_id does not parse",
        )
    })?;
    let payload = conn
        .call(tachyon_protocol::Command::RegisterMCPServers {
            session_id: gateway_session,
            servers: descriptors,
        })
        .await
        .map_err(call_error)?;
    let approval_id: ApprovalId = payload
        .get("approval_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "RegisterMCPServers answered without an approval_id",
            )
        })?
        .parse()
        .map_err(|_| {
            HandlerError::turn_failed(
                "gateway_payload_invalid",
                "RegisterMCPServers approval_id does not parse",
            )
        })?;
    Ok((session_id, gateway_session, approval_id, payload))
}

/// Issues the fail-closed `DenyMCPServers` for a refused setup answer,
/// then answers the setup with the typed taxonomy error. The deny
/// result itself is best-effort (a deny failure must not mask the
/// refusal verdict); the parked rows stay auditable either way.
async fn deny_setup(
    conn: &mut GatewayConn,
    session_id: SessionId,
    approval_id: ApprovalId,
    code: &'static str,
    reason: &str,
) -> Result<(), HandlerError> {
    let client_reason = format!("setup client: {reason}");
    if let Err(error) = conn
        .call(tachyon_protocol::Command::DenyMCPServers {
            session_id,
            approval_id,
            reason: client_reason,
        })
        .await
    {
        tracing::warn!(%error, "setup deny failed; the refusal verdict still stands");
    }
    Err(HandlerError::turn_failed(code, reason.to_owned()))
}

/// Dispatch: param validation, the liveness gate, inline answers, and
/// the spawned `session/prompt` pipeline.
/// Bound for one MCP-at-setup approval answer (spec §Q2: human-paced
/// approval with a bounded client wait; fail closed on expiry).
const MCP_SETUP_APPROVAL_WAIT: Duration = Duration::from_secs(120);

/// What one MCP-at-setup approval wait produced (TASK 01): pure wait
/// classification, no gateway calls — unit-tested with short bounds.
#[derive(Debug)]
enum McpApprovalAnswer {
    /// The client's answer payload (result or error frame).
    Answered(Result<Value, crate::codec::ErrorObject>),
    /// The bound elapsed with no answer.
    Timeout,
    /// The exchange ended without an answer (disconnect): the slot's
    /// waiter is gone.
    Eof,
}

/// Waits for one MCP approval answer without touching the gateway.
async fn await_mcp_answer(
    receiver: tokio::sync::oneshot::Receiver<Result<Value, crate::codec::ErrorObject>>,
    wait: Duration,
) -> McpApprovalAnswer {
    match tokio::time::timeout(wait, receiver).await {
        Ok(Ok(answer)) => McpApprovalAnswer::Answered(answer),
        Ok(Err(_)) => McpApprovalAnswer::Eof,
        Err(_) => McpApprovalAnswer::Timeout,
    }
}

/// Runs one MCP-at-setup approval exchange to completion (TASK 01):
/// map → register → ask → decide. Runs in a spawned setup task, never
/// inline: awaiting a client answer in the read loop would deadlock
/// the routing that delivers it.
async fn setup_run<C: Connector>(
    connector: &C,
    state: &Arc<SessionState>,
    tx: &UnboundedSender<Outbound>,
    cwd: String,
    servers: Vec<Value>,
    wait: Duration,
) -> Result<String, HandlerError> {
    // Fail fast pre-gateway: mapping touches no state.
    let descriptors = map_mcp_servers(&servers)?;
    let mut conn = connector
        .connect()
        .await
        .map_err(|error| HandlerError::unavailable(&error))?;
    let (session_id, gateway_session, approval_id, register_payload) =
        create_and_register(&mut conn, cwd, descriptors).await?;
    let (request_id, receiver) = emit_mcp_approval(
        &mut conn,
        state,
        tx,
        &session_id,
        gateway_session,
        &approval_id,
        register_payload,
    )
    .await?;
    // From here the slot is live: every exit abandons it and remembers
    // the id, so a late answer is diagnosed instead of dropped.
    let abandon = |state: &Arc<SessionState>| {
        state.abandon_mcp_slot(&request_id);
        state.remember_mcp_approval(request_id.clone());
    };
    // Every answered resolution retires its id into the consumed
    // set first: any later duplicate is diagnosed stale exactly once
    // instead of routing anywhere (TASK 01 S3).
    let answer = await_mcp_answer(receiver, wait).await;
    if matches!(answer, McpApprovalAnswer::Answered(_)) {
        state.remember_mcp_approval(request_id.clone());
    }
    match answer {
        McpApprovalAnswer::Answered(Ok(answer)) if is_allow_once(&answer) => {
            conn.call(tachyon_protocol::Command::ApproveMCPServers {
                session_id: gateway_session,
                approval_id,
            })
            .await
            .map_err(call_error)?;
            Ok(session_id)
        }
        McpApprovalAnswer::Answered(Ok(answer)) => {
            let reason = if is_reject_once(&answer) {
                "setup client rejected the MCP servers"
            } else {
                "setup client answer failed closed"
            };
            deny_setup(
                &mut conn,
                gateway_session,
                approval_id,
                "mcp_approval_denied",
                reason,
            )
            .await
            .map(|()| session_id.clone())
        }
        McpApprovalAnswer::Answered(Err(_)) => deny_setup(
            &mut conn,
            gateway_session,
            approval_id,
            "mcp_approval_denied",
            "setup client answered the MCP approval with an error",
        )
        .await
        .map(|()| session_id.clone()),
        McpApprovalAnswer::Timeout => {
            abandon(state);
            deny_setup(
                &mut conn,
                gateway_session,
                approval_id,
                "mcp_approval_timeout",
                "MCP approval answer did not arrive in time",
            )
            .await
            .map(|()| session_id.clone())
        }
        McpApprovalAnswer::Eof => {
            abandon(state);
            deny_setup(
                &mut conn,
                gateway_session,
                approval_id,
                "mcp_approval_eof",
                "MCP approval exchange ended without an answer",
            )
            .await
            .map(|()| session_id.clone())
        }
    }
}

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
                let SessionNewParams { cwd, servers } = match parse_session_new(params.as_ref()) {
                    Ok(params) => params,
                    Err(error) => return Some(error_outbound(id, error)),
                };
                if let Some(error) = self.gate(id.clone(), &method).await {
                    return Some(error);
                }
                if servers.is_empty() {
                    let outcome = self.session_new(cwd).await;
                    return Some(match outcome {
                        Ok(session_id) => Outbound::success(id, json!({ "sessionId": session_id })),
                        Err(error) => error_outbound(id, error),
                    });
                }
                self.setup_or_coalesce_mcp(id, cwd, servers, turns);
                None
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
        let session_id = payload
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| {
                HandlerError::turn_failed(
                    "gateway_payload_invalid",
                    "CreateSession answered without a session_id",
                )
            })?;
        Ok(session_id)
    }

    /// Spawns one MCP-at-setup approval flow (TASK 01 S2): the answer
    /// arrives later through the writer channel, so the read loop never
    /// blocks awaiting a client answer it must itself route. Tracked in
    /// `turns` for the EOF drain like prompt turns. The success shape
    /// stays exactly `{sessionId}`.
    /// MCP-bearing `session/new` (TASK 01 S2): a retried call id
    /// coalesces onto its setup (or replays its answer) instead of
    /// creating a second session; otherwise a fresh setup spawns.
    fn setup_or_coalesce_mcp(
        &self,
        id: RpcId,
        cwd: String,
        servers: Vec<Value>,
        turns: &mut JoinSet<()>,
    ) {
        if let Some(waiter) = self.state.mcp_setup_subscribe(&id) {
            let tx = self.tx.clone();
            turns.spawn(async move {
                let mut waiter = waiter;
                let stored = waiter.wait_for(Option::is_some).await.map_or_else(
                    |_| {
                        Outbound::error_with_data(
                            Some(id.clone()),
                            TURN_FAILED,
                            "the setup this call retries went away",
                            json!("mcp_setup_unknown"),
                        )
                    },
                    |slot| slot.clone().expect("waited slot holds a frame"),
                );
                let _ = tx.send(stored);
            });
            return;
        }
        self.spawn_mcp_setup(id, cwd, servers, turns);
    }

    fn spawn_mcp_setup(
        &self,
        id: RpcId,
        cwd: String,
        servers: Vec<Value>,
        turns: &mut JoinSet<()>,
    ) {
        let Some(sender) = self.state.mcp_setup_publish(id.clone()) else {
            // Unreachable on the sequential read loop (the arm
            // coalesces duplicates first); fail closed, never
            // duplicate the setup.
            let tx = self.tx.clone();
            turns.spawn(async move {
                let _ = tx.send(Outbound::error_with_data(
                    Some(id),
                    TURN_FAILED,
                    "duplicate MCP setup racing its first delivery",
                    json!("mcp_setup_unknown"),
                ));
            });
            return;
        };
        let connector = self.connector.clone();
        let tx = self.tx.clone();
        let state = Arc::clone(&self.state);
        turns.spawn(async move {
            let outcome = match setup_run(
                &connector,
                &state,
                &tx,
                cwd,
                servers,
                MCP_SETUP_APPROVAL_WAIT,
            )
            .await
            {
                Ok(session_id) => Outbound::success(id.clone(), json!({ "sessionId": session_id })),
                Err(error) => error_outbound(id.clone(), error),
            };
            // Publish before sending: coalesced waiters wake from the
            // channel; this call's frame goes through the writer.
            sender.send_replace(Some(outcome.clone()));
            let _ = tx.send(outcome);
        });
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
        match run_load(&self.connector, params).await {
            Ok(mut frames) => {
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

#[cfg(test)]
mod mcp_setup_wait_tests {
    //! TASK 01 S3: the approval wait classifies without gateway calls —
    //! answered values pass through, silence times out on a short bound,
    //! a dropped slot reads as disconnect.

    use super::{McpApprovalAnswer, await_mcp_answer};
    use std::time::Duration;

    #[tokio::test]
    async fn answered_value_passes_through() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let payload = serde_json::json!({"outcome": "selected"});
        sender.send(Ok(payload.clone())).expect("send answers");
        match await_mcp_answer(receiver, Duration::from_secs(5)).await {
            McpApprovalAnswer::Answered(Ok(value)) => assert_eq!(value, payload),
            other => panic!(
                "expected the answer through, got {}",
                matches!(other, McpApprovalAnswer::Answered(_))
            ),
        }
    }

    #[tokio::test]
    async fn silence_times_out_on_a_short_bound() {
        let (_sender, receiver) =
            tokio::sync::oneshot::channel::<Result<serde_json::Value, crate::codec::ErrorObject>>();
        match await_mcp_answer(receiver, Duration::from_millis(50)).await {
            McpApprovalAnswer::Timeout => (),
            _ => panic!("expected Timeout"),
        }
    }

    #[tokio::test]
    async fn dropped_slot_reads_as_disconnect() {
        let (sender, receiver) =
            tokio::sync::oneshot::channel::<Result<serde_json::Value, crate::codec::ErrorObject>>();
        drop(sender);
        match await_mcp_answer(receiver, Duration::from_secs(5)).await {
            McpApprovalAnswer::Eof => (),
            _ => panic!("expected Eof"),
        }
    }
}
