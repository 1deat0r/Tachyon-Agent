//! Shared helpers for tachyon-acp integration tests (tickets 01+03):
//! the fake ACP client over real piped stdio, an interactive frame
//! reader for interleaved send/read flows (overlap, idempotency,
//! concurrent cancels), a direct gateway command client for
//! identity/turn assertions, the armed fake-provider runtime, a fast
//! Cargo workspace fixture whose default acceptance
//! (`cargo test --offline --locked`) passes, and the scripted gateway
//! fixture for the stream-edge tests.
#![allow(dead_code)] // each test target imports only what it needs

pub mod scripted;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tachyon_gateway::transport::connect;
use tachyon_gateway::{FAKE_PROVIDER_LABEL, GatewayRuntime};
use tachyon_models::ModelProvider;
use tachyon_models::fake::{FakeModelProvider, FakeResponse};
use tachyon_protocol::{Command, CommandResult, RequestEnvelope, ResponseEnvelope};
use tachyon_tools::credential::CredentialBroker;
use tachyon_types::EventId;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command as ProcessCommand};
use tokio::sync::Notify;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// One unique, existing directory per call: never call twice inside
/// one test.
pub fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("tachyon-acp-{}-{id}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The adapter under test, wired to real piped stdio.
pub struct Adapter {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    stderr: ChildStderr,
}

impl Adapter {
    /// Spawns the `tachyon-acp` binary with `TACHYON_DATA_DIR` set to
    /// `data_dir`.
    pub fn spawn(data_dir: &std::path::Path) -> Self {
        Self::spawn_with_config(None, Some(data_dir))
    }

    /// Spawns the adapter with an explicit config file (`TACHYON_CONFIG`)
    /// and an OPTIONAL `TACHYON_DATA_DIR` (removed when `None`, so the
    /// config-file layer is the only source left) — the precedence seams
    /// the data-dir config-override tests pin. `XDG_DATA_HOME` is always
    /// repointed at a fresh empty dir so the platform-default branch can
    /// never accidentally find a gateway.
    pub fn spawn_with_config(
        config_path: Option<&std::path::Path>,
        data_dir: Option<&std::path::Path>,
    ) -> Self {
        let isolated_xdg = test_dir();
        let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_tachyon-acp"));
        command
            .env_remove("TACHYON_DATA_DIR")
            .env("XDG_DATA_HOME", &isolated_xdg)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(data_dir) = data_dir {
            command.env("TACHYON_DATA_DIR", data_dir);
        }
        if let Some(config_path) = config_path {
            command.env("TACHYON_CONFIG", config_path);
        }
        let mut child = command.spawn().expect("spawn tachyon-acp");
        let stdin = child.stdin.take().expect("stdin piped");
        let stdout = BufReader::new(child.stdout.take().expect("stdout piped"));
        let stderr = child.stderr.take().expect("stderr piped");
        Self {
            child,
            stdin: Some(stdin),
            stdout,
            stderr,
        }
    }

    /// Writes one request or notification line. The line and its
    /// newline go out as ONE `write_all` — a single write of ≤ `PIPE_BUF`
    /// is atomic on a pipe, so the adapter can never observe a newline
    /// without the line it terminates (split writes proved racy here:
    /// the adapter intermittently read the `\n` of one send before that
    /// send's bytes).
    pub async fn send(&mut self, line: &str) {
        let stdin = self.stdin.as_mut().expect("stdin still open");
        let mut framed = Vec::with_capacity(line.len() + 1);
        framed.extend_from_slice(line.as_bytes());
        framed.push(b'\n');
        stdin.write_all(&framed).await.unwrap();
        stdin.flush().await.unwrap();
    }

    /// Closes stdin (EOF): the adapter's loop ends and the process
    /// exits on its own.
    pub fn close_stdin(&mut self) {
        drop(self.stdin.take());
    }

    /// Reads reply lines until the adapter closes stdout (post-exit),
    /// then waits for exit and returns `(replies, stderr, exit_ok)`.
    pub async fn finish(mut self) -> (Vec<String>, String, bool) {
        let mut replies = Vec::new();
        loop {
            let mut line = String::new();
            let read = self.stdout.read_line(&mut line).await.unwrap();
            if read == 0 {
                break;
            }
            replies.push(line.trim_end().to_owned());
        }
        let exit = self.child.wait().await.unwrap();
        let mut logs = Vec::new();
        self.stderr.read_to_end(&mut logs).await.unwrap();
        let logs = String::from_utf8_lossy(&logs).into_owned();
        (replies, logs, exit.success())
    }
}

/// Asserts one line is a well-formed JSON-RPC frame and returns it
/// parsed. Every stdout byte the adapter ever emits must pass this.
pub fn parse_frame(line: &str) -> Value {
    let frame: Value = serde_json::from_str(line)
        .unwrap_or_else(|error| panic!("stdout line is not valid JSON: {line}: {error}"));
    assert_eq!(frame["jsonrpc"], "2.0", "not a JSON-RPC 2.0 frame: {line}");
    assert!(frame.get("id").is_some(), "frame carries no id: {line}");
    assert!(
        frame.get("result").is_some() ^ frame.get("error").is_some(),
        "frame must carry exactly one of result/error: {line}"
    );
    frame
}

impl Adapter {
    /// Reads one stdout line, bounded by `timeout`. `None` at EOF
    /// (adapter closed stdout). `label` names what is being waited for;
    /// a timeout panics with the adapter's recent stderr so a hung
    /// frame fails the test loudly with its explaining log lines.
    pub async fn read_line(&mut self, timeout: Duration, label: &str) -> Option<String> {
        let mut line = String::new();
        match tokio::time::timeout(timeout, self.stdout.read_line(&mut line)).await {
            Ok(Ok(0)) => None,
            Ok(Ok(_)) => Some(line.trim_end().to_owned()),
            Ok(Err(error)) => panic!("reading adapter stdout: {error}"),
            Err(_) => {
                // Surface the adapter's own logs in the panic: a hung
                // frame almost always has an explaining stderr line.
                let mut log = String::new();
                let _ = tokio::time::timeout(
                    Duration::from_millis(250),
                    self.stderr.read_to_string(&mut log),
                )
                .await;
                panic!("timed out waiting for {label}; stderr:\n{log}");
            }
        }
    }

    /// Reads frames until the response for `id` arrives. Returns the
    /// raw non-response lines seen before it (streamed `session/update`
    /// notifications and agent→client request frames) and the raw
    /// response line itself. A response for any other id is a test bug
    /// and panics.
    pub async fn read_until_response(
        &mut self,
        id: Value,
        timeout: Duration,
    ) -> (Vec<String>, String) {
        let mut notifications = Vec::new();
        loop {
            let line = self
                .read_line(timeout, &format!("the response for {id}"))
                .await
                .unwrap_or_else(|| panic!("adapter closed stdout before responding to {id}"));
            match classify(&line) {
                AdapterFrame::Notification(_) | AdapterFrame::Request(_) => {
                    notifications.push(line);
                }
                AdapterFrame::Response(frame) => {
                    assert_eq!(
                        frame["id"], id,
                        "expected the response for {id}, got: {line}"
                    );
                    return (notifications, line);
                }
            }
        }
    }

    /// Reads frames until EVERY id in `wanted` has its response, in
    /// whatever order they arrive, and returns all raw lines in arrival
    /// order. A response for an id not in `wanted`, a duplicate
    /// response, or a missing id before the timeout panics — the
    /// per-id "exactly one response each" and the observed frame
    /// ordering are both pinned by the returned sequence.
    pub async fn read_until_all(&mut self, wanted: &[Value], timeout: Duration) -> Vec<String> {
        let mut lines = Vec::new();
        let mut seen: Vec<Value> = Vec::new();
        while seen.len() < wanted.len() {
            let line = self
                .read_line(timeout, &format!("the responses for {wanted:?}"))
                .await
                .unwrap_or_else(|| {
                    panic!("adapter closed stdout before all of {wanted:?} answered")
                });
            if let AdapterFrame::Response(frame) = classify(&line) {
                let id = frame["id"].clone();
                assert!(
                    wanted.contains(&id),
                    "a response for {id} was not expected here: {line} (prior lines: {lines:?})"
                );
                assert!(!seen.contains(&id), "duplicate response for {id}: {line}");
                seen.push(id);
            }
            lines.push(line);
        }
        lines
    }
}

/// One stdout line classified: agent→client notification (no id), an
/// agent→client REQUEST (method + id, the client must answer it), or a
/// response (id-bearing, exactly one of result/error). Every line the
/// adapter emits must pass this — stdout stays frame-pure.
#[derive(Debug)]
pub enum AdapterFrame {
    /// A notification frame (`session/update`).
    Notification(Value),
    /// An id-bearing request frame (`session/request_permission`).
    Request(Value),
    /// A response frame.
    Response(Value),
}

/// Classifies and validates one stdout line (see [`AdapterFrame`]).
pub fn classify(line: &str) -> AdapterFrame {
    let frame: Value = serde_json::from_str(line)
        .unwrap_or_else(|error| panic!("stdout line is not valid JSON: {line}: {error}"));
    assert_eq!(frame["jsonrpc"], "2.0", "not a JSON-RPC 2.0 frame: {line}");
    if frame.get("method").is_some() {
        if frame.get("id").is_some() {
            // An agent→client request: id + method + params, and
            // never a result/error (the client answers it).
            assert!(
                frame.get("result").is_none() && frame.get("error").is_none(),
                "a request frame carries neither result nor error: {line}"
            );
            assert!(
                frame.get("params").is_some(),
                "request frames carry params: {line}"
            );
            AdapterFrame::Request(frame)
        } else {
            assert!(
                frame.get("params").is_some(),
                "notifications carry params: {line}"
            );
            AdapterFrame::Notification(frame)
        }
    } else {
        assert!(frame.get("id").is_some(), "response carries no id: {line}");
        assert!(
            frame.get("result").is_some() ^ frame.get("error").is_some(),
            "response must carry exactly one of result/error: {line}"
        );
        AdapterFrame::Response(frame)
    }
}

/// Sends one command straight to the gateway socket (the test's own
/// direct view of durable state) and returns the payload, or
/// `Err("code|message")` for a typed refusal.
pub async fn gw_call(socket: &Path, command: Command) -> Result<Value, String> {
    let mut stream = connect(socket).await.expect("connect to gateway");
    let request = RequestEnvelope {
        protocol_version: tachyon_protocol::PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command,
    };
    let bytes = tachyon_protocol::encode_frame(&request).expect("encode request");
    stream.write_all(&bytes).await.expect("send request");
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.expect("read prefix");
    let len = u32::from_le_bytes(prefix) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.expect("read payload");
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    let (response, _): (ResponseEnvelope, usize) =
        tachyon_protocol::decode_frame(&framed).expect("decode response");
    match response.result {
        CommandResult::Ok { payload } => Ok(payload),
        CommandResult::Err { code, message } => Err(format!("{code}|{message}")),
    }
}

/// [`gw_call`] that panics on a typed refusal (assert-ok helper).
pub async fn gw_ok(socket: &Path, command: Command) -> Value {
    match gw_call(socket, command).await {
        Ok(payload) => payload,
        Err(error) => panic!("gateway refused the command: {error}"),
    }
}

/// Gateway runtime with the scripted fake provider armed (label
/// included) — the `run_path`/`g5_e2e` fixture shape.
pub fn armed_runtime(provider: Arc<dyn ModelProvider>) -> GatewayRuntime {
    GatewayRuntime {
        provider: Some(provider),
        label: FAKE_PROVIDER_LABEL.to_owned(),
        model: "scripted-replay-1".to_owned(),
        redactor: CredentialBroker::default(),
    }
}

/// The scripted patch response a full happy-path turn replays: one
/// `propose_execution` mutation of `path` whose `base_hash` is the
/// BLAKE3 of the file's current bytes (mirrors `g5_e2e`'s fixture).
pub fn patch_response(path: &str, base: &[u8], new_content: &str) -> FakeResponse {
    let script = json!({
        "decision": "propose_execution",
        "operations": [{
            "capability": "mutation.patch",
            "reason": "scripted change for the ACP turn test",
            "args": {
                "path": path,
                "base_hash": blake3::hash(base).to_hex().to_string(),
                "new_content": new_content,
            },
        }],
    });
    FakeResponse {
        text: script.to_string(),
        decision: serde_json::from_value(script).expect("typed proposal fixture"),
        input_tokens: 0,
        output_tokens: 0,
    }
}

/// A real Cargo package with one passing test: default acceptance
/// detection resolves to `cargo test --offline --locked`, which passes
/// (the lockfile is generated up front so `--locked` never needs to
/// modify it). Returns the canonical package root.
pub fn cargo_package(root: &Path) -> PathBuf {
    std::fs::create_dir_all(root.join("src")).unwrap();
    std::fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"acp-ws\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n",
    )
    .unwrap();
    std::fs::write(
        root.join("src/lib.rs"),
        "pub mod greeting;\n\npub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn adds() {\n        assert_eq!(crate::add(1, 2), 3);\n    }\n}\n",
    )
    .unwrap();
    // A second, independent patch target: two turns in one workspace
    // must not fight over one file's base hash.
    std::fs::write(
        root.join("src/greeting.rs"),
        "pub fn hello() -> &'static str {\n    \"hello\"\n}\n",
    )
    .unwrap();
    let generated = std::process::Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .current_dir(root)
        .status()
        .expect("spawn cargo generate-lockfile");
    assert!(generated.success(), "cargo generate-lockfile failed");
    std::fs::canonicalize(root).unwrap()
}

/// New content for the scripted patch: a prepended comment (real byte
/// change, test still passes).
pub fn patched_content(original: &str) -> String {
    format!("// scripted by the ACP adapter test\n{original}")
}

/// Fake provider that counts invocations and PARKS inside `invoke`
/// until released, then delegates to its scripted queue — the
/// `startrun_retry`/`run_path` parked-provider shape, for keeping a turn
/// live across an overlap assertion.
pub struct GatedProvider {
    /// Inner scripted provider serving after release.
    pub inner: Arc<FakeModelProvider>,
    /// Incremented on entry (the turn has reached the model stage).
    pub entered: Arc<AtomicU64>,
    /// Parking lot: released by the test.
    pub release: Arc<Notify>,
}

#[async_trait::async_trait]
impl ModelProvider for GatedProvider {
    fn id(&self) -> tachyon_types::ProviderId {
        self.inner.id()
    }

    fn capabilities(&self) -> tachyon_models::ModelCapabilities {
        self.inner.capabilities()
    }

    fn estimate(&self, request: &tachyon_models::ModelRequest) -> tachyon_models::ProviderEstimate {
        self.inner.estimate(request)
    }

    async fn invoke(
        &self,
        request: tachyon_models::ModelRequest,
        sink: tachyon_models::ModelEventSink,
    ) -> Result<tachyon_models::ModelResult, tachyon_models::ModelError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        self.inner.invoke(request, sink).await
    }
}

/// Polls `condition` until true or panics after `secs`.
pub async fn wait_until(what: &str, secs: u64, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(secs), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}
