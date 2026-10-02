//! Supervised stdio MCP child actor (ADR-0005 blocker 3, ticket 02).
//!
//! One [`LiveMcpServer`] owns a single client-supplied MCP server child:
//! an owned process group (`process_group(0)` on Unix, so grandchildren
//! die with the leader), `kill_on_drop` (dropping the handle always
//! signals the child), a cleared environment rebuilt from the inherited
//! allowlist plus the pinned descriptor's explicit entries (secrets
//! injected from broker handles at spawn, never persisted), the pinned
//! session root as cwd (no second resolution, no other scope), and stderr
//! drained to logs through the broker redactor.
//!
//! The gateway drives `initialize` (the negotiated version is recorded;
//! a mismatch reaps the child) before `tools/list` (the inventory is
//! recorded before any call is admitted). Every failure after spawn kills
//! and reaps the child synchronously, so a failed launch never orphans a
//! process.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tachyon_protocol::{McpArgEntry, McpEnvEntry, McpToolInfo};
use tachyon_tools::credential::CredentialBroker;
use tachyon_tools::process::INHERITED_ENV_KEYS;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio_util::sync::CancellationToken;

/// MCP wire version the gateway offers in `initialize` and requires back
/// in the child's response: any other `protocolVersion` is a typed
/// `mcp_version_mismatch` and the child is reaped.
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// Bound on one handshake read: local children answer in milliseconds;
/// a silent child fails closed instead of parking the approve command.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bound on one `tools/call` round trip: tool work may legitimately take
/// seconds, but a silent child still fails closed instead of parking the
/// granted approve command forever.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// JSON-RPC ids for `tools/call` frames: a process-wide counter past the
/// launch ids (1, 2), so servers never share an id shape and a mismatched
/// reply always fails the call instead of crossing streams.
static MCP_CALL_ID: AtomicU64 = AtomicU64::new(3);

/// A failed launch: a stable machine-readable `code` the gateway returns
/// verbatim plus a human `message` that never carries secret material
/// (child-supplied text is redacted through the broker at construction).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpLaunchError {
    /// Typed refusal (`mcp_spawn_failed`, `mcp_handshake_failed`,
    /// `mcp_version_mismatch`).
    pub code: &'static str,
    /// Human detail, safe to log and return.
    pub message: String,
}

impl McpLaunchError {
    fn spawn(message: impl Into<String>) -> Self {
        Self {
            code: "mcp_spawn_failed",
            message: message.into(),
        }
    }

    fn handshake(message: impl Into<String>) -> Self {
        Self {
            code: "mcp_handshake_failed",
            message: message.into(),
        }
    }

    /// The child answered `initialize` but negotiated something else.
    /// `reported` is child-controlled, so it passes through the broker
    /// redactor (a hostile child must not be able to launder an env
    /// secret into an error string) and is length-capped.
    fn version_mismatch(reported: &str, secrets: &CredentialBroker) -> Self {
        let scrubbed = secrets.redact(reported);
        let capped: String = scrubbed.chars().take(64).collect();
        let capped = if capped.len() < scrubbed.len() {
            format!("{capped}…")
        } else {
            capped
        };
        Self {
            code: "mcp_version_mismatch",
            message: format!(
                "mcp server negotiated protocol version {capped:?}, \
                 gateway requires {MCP_PROTOCOL_VERSION:?}"
            ),
        }
    }
}

/// A handshaked, inventoried MCP child. Dropping kills the child
/// (`kill_on_drop`); the held pipes keep the child's stdio open so it
/// never observes EOF while parked as live.
#[derive(Debug)]
pub struct LiveMcpServer {
    // Held, never read: ownership IS the use — dropping kills the child
    // through `kill_on_drop`.
    #[allow(dead_code)]
    child: Child,
    #[allow(dead_code)]
    stdin: ChildStdin,
    #[allow(dead_code)]
    stdout: BufReader<ChildStdout>,
    /// Version the child reported in `initialize` (always
    /// [`MCP_PROTOCOL_VERSION`] — anything else fails the launch).
    pub version: String,
    /// Tools the child reported in `tools/list`.
    pub tools: Vec<McpToolInfo>,
}

/// A failed `tools/call`: a stable machine-readable `code` the gateway
/// returns verbatim plus a human `message` that never carries secret
/// material (child-supplied text is redacted through the broker at
/// construction, exactly like [`McpLaunchError`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpCallError {
    /// Typed refusal (`mcp_call_failed`, `mcp_tool_error`).
    pub code: &'static str,
    /// Human detail, safe to log and return.
    pub message: String,
}

impl McpCallError {
    /// The transport failed: EOF, timeout, invalid JSON, id mismatch.
    /// A dead child is this code, never a success — the §19
    /// `UnknownAfterCrash` analogue at the gateway seam.
    fn failed(message: impl Into<String>) -> Self {
        Self {
            code: "mcp_call_failed",
            message: message.into(),
        }
    }

    /// The child answered with a JSON-RPC error. `detail` is
    /// child-controlled, so it passes through the broker redactor (a
    /// hostile child must not launder an env secret into an error
    /// string) and is length-capped.
    fn tool_error(detail: &serde_json::Value, secrets: &CredentialBroker) -> Self {
        let scrubbed = secrets.redact(&detail.to_string());
        let capped: String = scrubbed.chars().take(512).collect();
        let capped = if capped.len() < scrubbed.len() {
            format!("{capped}…")
        } else {
            capped
        };
        Self {
            code: "mcp_tool_error",
            message: format!("mcp tool reported an error: {capped}"),
        }
    }
}

/// Redacts registered secrets from a child-supplied `tools/call`
/// result: every string in the JSON tree passes through the broker, so
/// a tool echoing its environment (or its arguments) cannot launder a
/// secret into the receipt. Structure is preserved — only string
/// contents change.
#[must_use]
pub fn redact_value(value: &serde_json::Value, secrets: &CredentialBroker) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(secrets.redact(text)),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| redact_value(item, secrets))
                .collect(),
        ),
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, item)| (secrets.redact(key), redact_value(item, secrets)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

impl LiveMcpServer {
    /// One mediated `tools/call` round trip against the owned child.
    /// One request, one matching-id response whose `result` returns
    /// broker-redacted: raw secrets never reach the receipt. Id-less
    /// server notifications on the way are skipped within the call
    /// bound. EOF, timeout, invalid JSON, or an id mismatch fails as
    /// `mcp_call_failed` — a child that dies mid-call is never silent
    /// success (the caller marks the server `stopped`). A JSON-RPC
    /// `error` answer fails as `mcp_tool_error` with the redacted detail.
    /// Aborting is cooperative through `cancel` (the gateway cancels it
    /// when the owning session's task is cancelled or the server is
    /// reaped mid-call): the child is killed so the pipe read returns
    /// promptly instead of waiting out the call bound, and the call
    /// fails as `mcp_call_failed` — never success, even when the abort
    /// wins the race after the answer already sat in the pipe.
    pub async fn call_tool(
        &mut self,
        server_id: &str,
        tool: &str,
        arguments: &serde_json::Value,
        secrets: &CredentialBroker,
        cancel: &CancellationToken,
    ) -> Result<serde_json::Value, McpCallError> {
        let id = MCP_CALL_ID.fetch_add(1, Ordering::Relaxed);
        let mut line = serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        }))
        .map_err(|err| {
            McpCallError::failed(format!("server '{server_id}': call encode failed: {err}"))
        })?;
        line.push('\n');
        self.stdin.write_all(line.as_bytes()).await.map_err(|err| {
            McpCallError::failed(format!(
                "server '{server_id}': child stdin unwritable: {err}"
            ))
        })?;
        self.stdin.flush().await.map_err(|err| {
            McpCallError::failed(format!(
                "server '{server_id}': child stdin unflushable: {err}"
            ))
        })?;
        // MCP 2024-11-05 permits id-less server notifications at any
        // time (e.g. `notifications/tools/list_changed`): loop within
        // the call bound, skipping notifications until the matching-id
        // reply or timeout. A reply carrying another id is still a
        // protocol violation — only id-less frames are skippable.
        let deadline = tokio::time::Instant::now() + CALL_TIMEOUT;
        let reply: serde_json::Value = loop {
            let mut reply_text = String::new();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            // Driver cancel aborts the in-flight call: kill the child so
            // the pipe read returns promptly instead of waiting out the
            // 60 s bound. Biased toward the abort — a cancel that lands
            // while the answer sits in the pipe still fails the call.
            let read = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    let _ = self.child.kill().await;
                    let _ = self.stdin.shutdown().await;
                    return Err(McpCallError::failed(format!(
                        "server '{server_id}': tools/call aborted on cancel — outcome unknown, \
                         retry needs a fresh approved call"
                    )));
                }
                read = tokio::time::timeout(remaining, self.stdout.read_line(&mut reply_text)) => read,
            };
            let bytes = read.map_err(|_| {
                McpCallError::failed(format!(
                    "server '{server_id}': tools/call timed out after {}s",
                    CALL_TIMEOUT.as_secs()
                ))
            })?;
            let bytes = bytes.map_err(|err| {
                McpCallError::failed(format!(
                    "server '{server_id}': child stdout unreadable: {err}"
                ))
            })?;
            if bytes == 0 {
                return Err(McpCallError::failed(format!(
                    "server '{server_id}': child died mid-call (EOF) — outcome unknown, \
                     retry needs a fresh approved call"
                )));
            }
            // The reply is child-controlled and may echo secrets: shape
            // failures never quote it, and the admitted result is redacted.
            let candidate: serde_json::Value = serde_json::from_str(&reply_text).map_err(|_| {
                McpCallError::failed(format!(
                    "server '{server_id}': tools/call reply was not valid JSON"
                ))
            })?;
            if candidate.get("id").is_none() {
                continue;
            }
            if candidate.get("id") != Some(&serde_json::json!(id)) {
                return Err(McpCallError::failed(format!(
                    "server '{server_id}': tools/call reply did not match request id {id}"
                )));
            }
            break candidate;
        };
        if let Some(error) = reply.get("error") {
            return Err(McpCallError::tool_error(error, secrets));
        }
        // The abort may have fired while the answer sat in the pipe: a
        // cancelled call never reports success, even when the child
        // answered before the kill landed.
        if cancel.is_cancelled() {
            return Err(McpCallError::failed(format!(
                "server '{server_id}': tools/call aborted on cancel — outcome unknown, \
                 retry needs a fresh approved call"
            )));
        }
        let Some(result) = reply.get("result") else {
            return Err(McpCallError::failed(format!(
                "server '{server_id}': tools/call reply carried no result"
            )));
        };
        Ok(redact_value(result, secrets))
    }
}

/// Spawns one pinned server, handshakes it, and records its inventory.
/// The full launch contract in one place: absolute-path command, cleared
/// environment (allowlist + explicit entries with broker-injected
/// secrets), secret argv entries resolved from the vault exactly like
/// secret env (a handle this gateway's vault never saw refuses the
/// launch before any process starts), cwd forced to the pinned session
/// root, owned process group, stderr drained redacted to logs,
/// `initialize` version check, then the `tools/list` inventory — the
/// child is admitted `live` only once both round trips answer within
/// their bounds.
pub async fn launch_mcp_server(
    server_id: &str,
    command: &str,
    args: &[McpArgEntry],
    env: &[McpEnvEntry],
    secrets: &CredentialBroker,
    cwd: &Path,
) -> Result<LiveMcpServer, McpLaunchError> {
    if !Path::new(command).is_absolute() {
        return Err(McpLaunchError::spawn(format!(
            "server '{server_id}': pinned command is not an absolute path"
        )));
    }
    let mut literal: BTreeMap<OsString, OsString> = BTreeMap::new();
    for key in INHERITED_ENV_KEYS {
        if let Some(value) = std::env::var_os(key) {
            literal.insert(key.into(), value);
        }
    }
    for entry in env {
        let value = if entry.secret {
            let handle = tachyon_tools::credential::CredentialHandle(entry.value.clone());
            match secrets.use_handle(&handle) {
                Some(secret) => os_string_from_secret(&secret),
                None => {
                    return Err(McpLaunchError::spawn(format!(
                        "server '{server_id}': secret env '{}' is not in this \
                         gateway's vault (register again to re-arm it)",
                        entry.name
                    )));
                }
            }
        } else {
            entry.value.clone().into()
        };
        literal.insert(entry.name.clone().into(), value);
    }
    // Secret args resolve from the vault exactly like secret env, into
    // the child's argv: a handle this gateway's vault never saw fails
    // the launch BEFORE any process starts (typed `mcp_spawn_failed`),
    // so a stale post-restart row never execs with a missing secret.
    // The refusal names the argument position only — never its value.
    let mut child_args: Vec<OsString> = Vec::with_capacity(args.len());
    for (index, arg) in args.iter().enumerate() {
        let value = if arg.secret {
            let handle = tachyon_tools::credential::CredentialHandle(arg.value.clone());
            match secrets.use_handle(&handle) {
                Some(secret) => os_string_from_secret(&secret),
                None => {
                    return Err(McpLaunchError::spawn(format!(
                        "server '{server_id}': secret arg #{} is not in this \
                         gateway's vault (register again to re-arm it)",
                        index + 1
                    )));
                }
            }
        } else {
            arg.value.clone().into()
        };
        child_args.push(value);
    }

    let mut spawn = Command::new(command);
    spawn
        .args(&child_args)
        .env_clear()
        .envs(&literal)
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // An owned process group: the child's PID is the PGID, so process
    // tools can address the whole tree the untrusted child may grow.
    #[cfg(unix)]
    spawn.process_group(0);
    let mut child = spawn.spawn().map_err(|err| {
        McpLaunchError::spawn(format!("server '{server_id}': spawn failed: {err}"))
    })?;
    let stdin = child.stdin.take().ok_or_else(|| {
        McpLaunchError::spawn(format!("server '{server_id}': child stdin unavailable"))
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        McpLaunchError::spawn(format!("server '{server_id}': child stdout unavailable"))
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        McpLaunchError::spawn(format!("server '{server_id}': child stderr unavailable"))
    })?;
    drain_stderr(stderr, server_id.to_owned(), secrets.clone());

    // Every fallible step below reaps through the single `reap` below,
    // so no error path leaks a process; the `Drop` backstop covers only
    // a dropped approve future mid-handshake.
    let mut pending = PendingLaunch {
        server_id,
        child: Some(child),
        stdin: Some(stdin),
        stdout: Some(BufReader::new(stdout)),
        secrets,
    };
    let version = match pending.handshake().await {
        Ok(version) => version,
        Err(err) => return Err(pending.reap(err).await),
    };
    let tools = match pending.inventory().await {
        Ok(tools) => tools,
        Err(err) => return Err(pending.reap(err).await),
    };
    Ok(pending.live(version, tools))
}

/// A child past spawn but before admission. Pipes ride in `Option`s so a
/// fully admitted launch can move them into [`LiveMcpServer`] past the
/// `Drop` backstop (a type with `Drop` cannot be destructured).
struct PendingLaunch<'a> {
    server_id: &'a str,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Option<BufReader<ChildStdout>>,
    secrets: &'a CredentialBroker,
}

impl PendingLaunch<'_> {
    /// `initialize` round trip: one request, one matching-id response
    /// whose `protocolVersion` equals [`MCP_PROTOCOL_VERSION`]. Id-less
    /// server notifications on the way are skipped within the handshake
    /// bound. Anything else — EOF, timeout, invalid JSON, JSON-RPC
    /// error, wrong id shape, version drift — fails typed (the caller reaps).
    async fn handshake(&mut self) -> Result<String, McpLaunchError> {
        let server_id = self.server_id;
        self.send(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "clientInfo": {"name": "tachyon-gateway", "version": "0.0.1"},
                "capabilities": {},
            },
        }))
        .await?;
        let reply = self.recv().await?;
        if reply.get("id") != Some(&serde_json::json!(1)) {
            return Err(McpLaunchError::handshake(format!(
                "server '{server_id}': initialize reply did not match request id 1"
            )));
        }
        if reply.get("error").is_some() {
            return Err(McpLaunchError::handshake(format!(
                "server '{server_id}': initialize was refused by the child"
            )));
        }
        let reported = reply
            .pointer("/result/protocolVersion")
            .and_then(|version| version.as_str())
            .unwrap_or("");
        if reported != MCP_PROTOCOL_VERSION {
            return Err(McpLaunchError::version_mismatch(reported, self.secrets));
        }
        // Best-effort per MCP: a child that already answered `initialize`
        // but dies on the notification still fails the launch below.
        let _ = self
            .send(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
            }))
            .await;
        Ok(MCP_PROTOCOL_VERSION.to_owned())
    }

    /// `tools/list` round trip: the recorded inventory gates every later
    /// call (ticket 03 authorizes unknown tools as `UnknownCapability`
    /// against exactly this list). Names are the authorization keys, so
    /// an empty name fails the launch now rather than confusing the
    /// authorizer later.
    async fn inventory(&mut self) -> Result<Vec<McpToolInfo>, McpLaunchError> {
        let server_id = self.server_id;
        self.send(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        }))
        .await?;
        let reply = self.recv().await?;
        if reply.get("id") != Some(&serde_json::json!(2)) {
            return Err(McpLaunchError::handshake(format!(
                "server '{server_id}': tools/list reply did not match request id 2"
            )));
        }
        if reply.get("error").is_some() {
            return Err(McpLaunchError::handshake(format!(
                "server '{server_id}': tools/list was refused by the child"
            )));
        }
        let Some(tools) = reply.pointer("/result/tools") else {
            return Err(McpLaunchError::handshake(format!(
                "server '{server_id}': tools/list reply carried no tools"
            )));
        };
        // Never echo child text into an error: shape failures name the
        // server and the redacted serde cause, not the payload. Serde's
        // Display for invalid-type errors embeds the offending literal,
        // so the formatted message passes through the broker exactly
        // like the version-mismatch path.
        let tools: Vec<McpToolInfo> = serde_json::from_value(tools.clone()).map_err(|err| {
            let scrubbed = self.secrets.redact(&format!(
                "server '{}': tools/list inventory unreadable: {err}",
                self.server_id
            ));
            McpLaunchError::handshake(scrubbed)
        })?;
        if tools.iter().any(|tool| tool.name.is_empty()) {
            return Err(McpLaunchError::handshake(format!(
                "server '{server_id}': tools/list advertised an unnamed tool"
            )));
        }
        Ok(tools)
    }

    async fn send(&mut self, frame: &serde_json::Value) -> Result<(), McpLaunchError> {
        let server_id = self.server_id;
        let mut line = serde_json::to_string(frame).map_err(|err| {
            McpLaunchError::handshake(format!(
                "server '{server_id}': request encode failed: {err}"
            ))
        })?;
        line.push('\n');
        let stdin = self.stdin.as_mut().expect("launch owns its pipes");
        stdin.write_all(line.as_bytes()).await.map_err(|err| {
            McpLaunchError::handshake(format!(
                "server '{server_id}': child stdin unwritable: {err}"
            ))
        })?;
        stdin.flush().await.map_err(|err| {
            McpLaunchError::handshake(format!(
                "server '{server_id}': child stdin unflushable: {err}"
            ))
        })
    }

    async fn recv(&mut self) -> Result<serde_json::Value, McpLaunchError> {
        // MCP 2024-11-05 permits id-less server notifications at any
        // time: loop within the handshake bound, skipping notifications
        // until the first id-carrying reply. The caller checks the id.
        let server_id = self.server_id;
        let deadline = tokio::time::Instant::now() + HANDSHAKE_TIMEOUT;
        loop {
            let mut line = String::new();
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let read = tokio::time::timeout(
                remaining,
                self.stdout
                    .as_mut()
                    .expect("launch owns its pipes")
                    .read_line(&mut line),
            )
            .await;
            let bytes = read.map_err(|_| {
                McpLaunchError::handshake(format!(
                    "server '{server_id}': handshake timed out after {}s",
                    HANDSHAKE_TIMEOUT.as_secs()
                ))
            })?;
            let bytes = bytes.map_err(|err| {
                McpLaunchError::handshake(format!(
                    "server '{server_id}': child stdout unreadable: {err}"
                ))
            })?;
            if bytes == 0 {
                return Err(McpLaunchError::handshake(format!(
                    "server '{server_id}': child exited before the handshake completed"
                )));
            }
            // The line itself is child-controlled and may echo secrets: shape
            // failures never quote it.
            let value = serde_json::from_str::<serde_json::Value>(&line).map_err(|_| {
                McpLaunchError::handshake(format!(
                    "server '{server_id}': handshake reply was not valid JSON"
                ))
            })?;
            if value.get("id").is_none() {
                continue;
            }
            return Ok(value);
        }
    }

    /// Kill and synchronously reap: after this returns the PID is gone
    /// (no zombie, no orphan), and the caller's `Err` carries on.
    async fn reap(mut self, err: McpLaunchError) -> McpLaunchError {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        err
    }

    /// A fully admitted launch: handshake + inventory succeeded, so the
    /// pipes move into the owned live handle.
    fn live(mut self, version: String, tools: Vec<McpToolInfo>) -> LiveMcpServer {
        LiveMcpServer {
            child: self.child.take().expect("launch owns its child"),
            stdin: self.stdin.take().expect("launch owns its pipes"),
            stdout: self.stdout.take().expect("launch owns its pipes"),
            version,
            tools,
        }
    }
}

/// Secret bytes into an env value: exact on Unix, lossy where the
/// platform has no byte-exact `OsString`. The value only reaches
/// `execve` — never logs or rows.
fn os_string_from_secret(secret: &[u8]) -> OsString {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;
        OsString::from_vec(secret.to_vec())
    }
    #[cfg(not(unix))]
    {
        OsString::from(String::from_utf8_lossy(secret).into_owned())
    }
}

/// Stderr belongs to logs, never to silent pipes (a chatty child must
/// not wedge on a full pipe): each line is redacted through the broker
/// before it reaches `tracing`, so a child echoing its own secret env
/// cannot launder it into the logs.
fn drain_stderr(mut stderr: ChildStderr, server_id: String, secrets: CredentialBroker) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(&mut stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(
                server_id = %server_id,
                line = %secrets.redact(&line),
                "mcp server stderr"
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The broker's own redaction, pinned where the gateway depends on
    /// it: a secret echoing as a JSON object KEY (not just a value)
    /// still comes out handle-only, so an untrusted child cannot
    /// launder env secrets into receipts through key position.
    #[test]
    fn redact_value_scrubs_object_keys_as_well_as_values() {
        let mut broker = CredentialBroker::default();
        let handle = broker.register(b"unit-test-secret-abc", "mcp-secret");
        let expected = tachyon_tools::credential::redaction_for(&handle);
        let value = serde_json::json!({
            "unit-test-secret-abc": "unit-test-secret-abc",
            "nested": {"unit-test-secret-abc": ["unit-test-secret-abc"]},
            "clean": "clean",
        });
        let scrubbed = redact_value(&value, &broker);
        assert_eq!(
            scrubbed,
            serde_json::json!({
                expected.clone(): expected.clone(),
                "nested": {expected.clone(): [expected.clone()]},
                "clean": "clean",
            }),
            "keys and values both redact to the broker handle"
        );
    }

    /// The post-restart state without a re-arm: the durable row still
    /// names the broker handle, but this gateway's vault never saw it
    /// (grants never survive a restart, and neither does the vault).
    /// The launch fails closed as `mcp_spawn_failed` BEFORE any spawn —
    /// nothing executes with a missing secret.
    #[tokio::test]
    async fn unarmed_vault_refuses_spawn_before_any_process_starts() {
        let broker = CredentialBroker::default();
        let dir = std::env::temp_dir();
        let err = launch_mcp_server(
            "worker",
            "/bin/definitely-not-a-real-mcp-server",
            &[],
            &[McpEnvEntry {
                name: "API_TOKEN".to_owned(),
                value: "mcp-secret-1".to_owned(),
                secret: true,
            }],
            &broker,
            &dir,
        )
        .await
        .expect_err("an unarmed secret must fail the launch");
        assert_eq!(err.code, "mcp_spawn_failed");
        assert!(
            err.message.contains("not in this gateway's vault"),
            "the refusal names the vault loss: {err:?}"
        );
    }

    /// The argv twin of `unarmed_vault_refuses_spawn_before_any_process_starts`:
    /// a `secret: true` arg whose handle this gateway's vault never saw
    /// fails the launch BEFORE any spawn — typed `mcp_spawn_failed` —
    /// so a stale row can never exec with a missing secret.
    #[tokio::test]
    async fn unarmed_vault_refuses_spawn_for_secret_args_before_any_process() {
        let broker = CredentialBroker::default();
        let dir = std::env::temp_dir();
        let err = launch_mcp_server(
            "worker",
            "/bin/definitely-not-a-real-mcp-server",
            &[McpArgEntry {
                value: "mcp-secret-1".to_owned(),
                secret: true,
            }],
            &[],
            &broker,
            &dir,
        )
        .await
        .expect_err("an unarmed secret arg must fail the launch");
        assert_eq!(err.code, "mcp_spawn_failed");
        assert!(
            err.message.contains("not in this gateway's vault"),
            "the refusal names the vault loss: {err:?}"
        );
    }
}
