//! M11 plan item 5 + item 6 (gateway part): `Command::StartRun`
//! dispatch — refusal taxonomy, canonicalization before any policy or
//! lease boundary, durable workspace pin, and honest provider refusal.
//!
//! Every refusal below must fire BEFORE any durable pin or spawned run:
//! the gateway rejects what `ToolsContext` would otherwise accept
//! best-effort (tachyon-tools/src/lib.rs canonicalizes only if it can),
//! so there is no create-through-symlink window.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use serde_json::Value;
use tachyon_gateway::transport::connect;
use tachyon_gateway::{FAKE_PROVIDER_LABEL, GatewayRuntime, start, start_with};
use tachyon_models::fake::FakeModelProvider;
use tachyon_models::{
    ModelCapabilities, ModelError, ModelEventSink, ModelProvider, ModelRequest, ModelResult,
    ProviderEstimate,
};
use tachyon_protocol::{Command, CommandResult, RequestEnvelope, ResponseEnvelope};
use tachyon_tools::credential::CredentialBroker;
use tachyon_tools::workspace::WorkspaceLease;
use tachyon_types::{EventId, ProviderId};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_dir() -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("tachyon-m11-startrun-{}-{id}", std::process::id()))
}

/// Gateway runtime with the scripted fake provider armed (label included).
fn armed_runtime(provider: Arc<dyn ModelProvider>) -> GatewayRuntime {
    GatewayRuntime {
        provider: Some(provider),
        label: FAKE_PROVIDER_LABEL.to_owned(),
        model: "scripted-replay-1".to_owned(),
        redactor: CredentialBroker::default(),
    }
}

fn fake() -> Arc<dyn ModelProvider> {
    Arc::new(FakeModelProvider::new(ProviderId("bench-script".into())))
}

async fn send(socket: &Path, command: Command) -> (u16, Value, String) {
    let mut stream = connect(socket).await.unwrap();
    let request = RequestEnvelope {
        protocol_version: tachyon_protocol::PROTOCOL_VERSION,
        request_id: EventId::generate(),
        command,
    };
    let bytes = tachyon_protocol::encode_frame(&request).unwrap();
    stream.write_all(&bytes).await.unwrap();
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let len = u32::from_le_bytes(prefix) as usize;
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).await.unwrap();
    let mut framed = prefix.to_vec();
    framed.extend_from_slice(&payload);
    let (response, _): (ResponseEnvelope, usize) = tachyon_protocol::decode_frame(&framed).unwrap();
    match response.result {
        CommandResult::Ok { payload } => (200, payload, String::new()),
        CommandResult::Err { code, message } => (400, Value::Null, format!("{code}|{message}")),
    }
}

async fn ok(socket: &Path, command: Command) -> Value {
    let (status, payload, err) = send(socket, command).await;
    assert_eq!(status, 200, "expected success, got {err}");
    payload
}

async fn err(socket: &Path, command: Command) -> String {
    let (status, _, err) = send(socket, command).await;
    assert_eq!(status, 400, "expected a typed refusal");
    err
}

async fn new_task(socket: &Path) -> String {
    let session = ok(
        socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id = session["session_id"].as_str().unwrap().to_owned();
    let task = ok(
        socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "start a run".to_owned(),
        },
    )
    .await;
    task["task_id"].as_str().unwrap().to_owned()
}

async fn workspace_root_of(socket: &Path, task_id: &str) -> Option<String> {
    let task_id = task_id.to_owned();
    let got = ok(
        socket,
        Command::GetTask {
            task_id: task_id.parse().unwrap(),
        },
    )
    .await;
    got["task"]["workspace_root"].as_str().map(str::to_owned)
}

fn code_of(err: &str) -> &str {
    err.split('|').next().unwrap_or(err)
}

const VALID_ACCEPTANCE: &str = r#"{"clauses":[{"kind":"CommandPasses","command":{"program":"cargo","args":["test","--offline","--locked"],"cwd":".","env":{},"timeout_ms":60000}}]}"#;

/// A plain directory with no Cargo.toml: default acceptance must refuse.
fn non_cargo_workspace() -> PathBuf {
    let ws = test_dir().join("plain-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("notes.txt"), "not cargo\n").unwrap();
    ws
}

#[tokio::test]
async fn start_run_refuses_before_a_provider_is_configured() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = non_cargo_workspace();

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "provider_not_configured");
    assert_eq!(
        workspace_root_of(&socket, &task).await,
        None,
        "a refused StartRun must not pin anything"
    );
    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_refuses_a_workspace_root_that_does_not_exist() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let missing = test_dir().join("never-created");

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: missing.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "workspace_not_found");
    assert_eq!(workspace_root_of(&socket, &task).await, None);
    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_refuses_a_file_that_is_not_a_directory() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let base = test_dir();
    std::fs::create_dir_all(&base).unwrap();
    let file = base.join("a-file.txt");
    std::fs::write(&file, "not a dir\n").unwrap();

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: file.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "workspace_not_a_dir");
    assert_eq!(workspace_root_of(&socket, &task).await, None);
    gateway.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn start_run_refuses_a_root_that_does_not_canonicalize() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    // A self-referential symlink never canonicalizes (ELOOP): the
    // gateway must reject it rather than hand the raw path to
    // ToolsContext's best-effort canonicalize.
    let base = test_dir();
    std::fs::create_dir_all(&base).unwrap();
    let ws = base.join("symlink-loop");
    std::os::unix::fs::symlink("symlink-loop", &ws).unwrap();

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "workspace_not_canonical");
    assert_eq!(workspace_root_of(&socket, &task).await, None);
    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_refuses_a_non_cargo_workspace_without_acceptance() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = non_cargo_workspace();

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "acceptance_required", "fail closed: {got}");
    assert_eq!(
        workspace_root_of(&socket, &task).await,
        None,
        "acceptance must resolve before the pin"
    );
    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_refuses_an_unreadable_acceptance_file() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = non_cargo_workspace();
    let missing_contract = test_dir().join("no-such-acceptance.json");

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: Some(missing_contract.display().to_string()),
        },
    )
    .await;
    assert_eq!(code_of(&got), "acceptance_unreadable", "{got}");
    assert_eq!(workspace_root_of(&socket, &task).await, None);
    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_refuses_an_acceptance_file_that_fails_validation() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = non_cargo_workspace();
    // Syntactically JSON, semantically vacuous: no clauses may never pass.
    let base = test_dir();
    std::fs::create_dir_all(&base).unwrap();
    let contract = base.join("empty-acceptance.json");
    std::fs::write(&contract, r#"{"clauses":[]}"#).unwrap();

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: Some(contract.display().to_string()),
        },
    )
    .await;
    assert_eq!(code_of(&got), "acceptance_invalid", "{got}");
    assert_eq!(workspace_root_of(&socket, &task).await, None);
    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_pins_the_canonical_root_only_after_checks_pass() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = non_cargo_workspace();
    let base = test_dir();
    std::fs::create_dir_all(&base).unwrap();
    let contract = base.join("acceptance.json");
    std::fs::write(&contract, VALID_ACCEPTANCE).unwrap();

    assert_eq!(
        workspace_root_of(&socket, &task).await,
        None,
        "nothing is pinned before StartRun"
    );

    let started = ok(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: Some(contract.display().to_string()),
        },
    )
    .await;
    let expected = std::fs::canonicalize(&ws).unwrap();
    assert_eq!(started["workspace_root"], expected.display().to_string());
    assert_eq!(started["provider"], FAKE_PROVIDER_LABEL);
    assert_eq!(
        workspace_root_of(&socket, &task).await.as_deref(),
        Some(expected.display().to_string().as_str()),
        "the canonical root is durable in task state"
    );

    gateway.shutdown().await;
}

/// M11 slice 5 (pin/policy single-source): a SYMLINKED workspace pins the
/// canonical target (prepare step 3), and that exact value — not a second
/// filesystem resolution across the pin round-trip — builds the
/// `ToolsContext`: policy scopes, evidence collection and mutation all
/// read one root (`ToolsContext::new_from_canonical` performs no
/// canonicalize; prepare's `debug_assert_eq!` fires on any divergence and
/// is active for THIS very run — the test binary is a debug build).
#[cfg(unix)]
#[tokio::test]
async fn start_run_scopes_one_canonical_root_from_pin_through_policy() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let base = test_dir();
    let real = base.join("pin-real-ws");
    std::fs::create_dir_all(&real).unwrap();
    std::fs::write(real.join("Cargo.toml"), "[package]\nname = \"w\"\n").unwrap();
    let link = base.join("pin-link-ws");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    let started = ok(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: link.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    let canonical = std::fs::canonicalize(&real).unwrap();
    assert_eq!(
        started["workspace_root"],
        canonical.display().to_string(),
        "the durable pin is the symlink's canonical target, and that SAME \
         value builds the ToolsContext — one root for pin, policy, \
         evidence and mutation: {started}"
    );
    assert_eq!(
        workspace_root_of(&socket, &task).await.as_deref(),
        Some(canonical.display().to_string().as_str()),
        "the durable pin agrees with the StartRun acknowledgement"
    );

    gateway.shutdown().await;
}

#[tokio::test]
async fn start_run_refuses_a_terminal_task() {
    let dir = test_dir();
    let gateway = start_with(&dir, armed_runtime(fake())).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    let ws = non_cargo_workspace();
    ok(
        &socket,
        Command::CancelTask {
            task_id: task.parse().unwrap(),
        },
    )
    .await;

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "illegal_transition", "{got}");
    gateway.shutdown().await;
}

/// Provider that never answers: keeps a run in flight so a concurrent
/// `StartRun` can be observed refusing with a typed code.
struct BlockingProvider;

static BLOCKING_CAPABILITIES: OnceLock<ModelCapabilities> = OnceLock::new();

#[async_trait::async_trait]
impl ModelProvider for BlockingProvider {
    fn id(&self) -> ProviderId {
        ProviderId("bench-block".into())
    }

    fn capabilities(&self) -> ModelCapabilities {
        BLOCKING_CAPABILITIES
            .get_or_init(|| ModelCapabilities {
                context_window_tokens: 128_000,
                ..ModelCapabilities::default()
            })
            .clone()
    }

    fn estimate(&self, request: &ModelRequest) -> ProviderEstimate {
        ProviderEstimate {
            latency_ms: 1.0,
            input_tokens: request.estimated_input_tokens(),
        }
    }

    async fn invoke(
        &self,
        _request: ModelRequest,
        _sink: ModelEventSink,
    ) -> Result<ModelResult, ModelError> {
        std::future::pending::<()>().await;
        unreachable!("pending forever");
    }
}

#[tokio::test]
async fn start_run_refuses_a_second_run_while_one_is_in_flight() {
    let dir = test_dir();
    let runtime = armed_runtime(Arc::new(BlockingProvider));
    let gateway = start_with(&dir, runtime).await.unwrap();
    let socket = gateway.address().to_owned();
    let task = new_task(&socket).await;
    // A Cargo workspace (bare Cargo.toml) so the default acceptance
    // resolves and the run actually spawns into the blocking provider.
    let ws = test_dir().join("cargo-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("Cargo.toml"), "[package]\nname = \"w\"\n").unwrap();

    let first = ok(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(
        first["workspace_root"],
        std::fs::canonicalize(&ws).unwrap().display().to_string()
    );

    let got = err(
        &socket,
        Command::StartRun {
            task_id: task.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(code_of(&got), "run_already_active", "{got}");
    gateway.shutdown().await;
}

/// M11 slice 1 (plan checklist `StartRun` row: resource claim = "workspace
/// lease (existing M9/M10 lease) after canonicalization"): while one
/// task's run holds the lease on the pinned canonical root, a COMPETING
/// task's `StartRun` on the same root is refused with the typed
/// `workspace_busy` code before it can spawn — it never reaches mutation —
/// and the held lease is observably unavailable to any other holder.
/// This is the run-path proof behind this file's header claim: the lease
/// boundary is drawn only after canonicalization, and a refused
/// `StartRun` spawns no run at all.
///
/// (Formerly quarantined on the grounds that a run-long holder would
/// deadlock verification's per-stage acquire. The M11 slice-1 rework
/// ordered by the owner closes exactly that: prepare attaches the guard
/// to the run's `ToolsContext` and every drive-reachable inner
/// acquisition reuses it instead of re-acquiring the non-reentrant
/// lock — proven by tachyon-core's `run_lease` test.)
#[tokio::test]
async fn start_run_refuses_a_competing_run_while_another_holds_the_workspace_lease() {
    let dir = test_dir();
    let runtime = armed_runtime(Arc::new(BlockingProvider));
    let gateway = start_with(&dir, runtime).await.unwrap();
    let socket = gateway.address().to_owned();
    // A Cargo workspace so default acceptance resolves and the first run
    // actually spawns (and parks in the blocking provider, holding the lease).
    let ws = test_dir().join("leased-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("Cargo.toml"), "[package]\nname = \"w\"\n").unwrap();
    let canonical = std::fs::canonicalize(&ws).unwrap();

    let first = new_task(&socket).await;
    ok(
        &socket,
        Command::StartRun {
            task_id: first.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;

    // The in-flight run holds the lease on the pinned canonical root:
    // no other holder can acquire it while the run lives.
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if WorkspaceLease::try_acquire(&canonical)
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the in-flight run never took the workspace lease");

    // A competing task on the same root is refused typed at prepare…
    let competing = new_task(&socket).await;
    let got = err(
        &socket,
        Command::StartRun {
            task_id: competing.parse().unwrap(),
            workspace_root: ws.display().to_string(),
            acceptance: None,
        },
    )
    .await;
    assert_eq!(
        code_of(&got),
        "workspace_busy",
        "competing same-workspace run must not be admitted: {got}"
    );

    // …and spawns no run: the refused task journalled no run stage.
    let state = ok(
        &socket,
        Command::GetTask {
            task_id: competing.parse().unwrap(),
        },
    )
    .await;
    assert!(
        state["task"]["stages"].as_array().is_none_or(Vec::is_empty),
        "a workspace_busy refusal must spawn no run: {}",
        state["task"]["stages"]
    );

    // The first run's lease is still held (exclusion lasts the whole run).
    assert!(
        WorkspaceLease::try_acquire(&canonical)
            .await
            .unwrap()
            .is_none(),
        "the first run must keep holding the lease after the refusal"
    );
    gateway.shutdown().await;
}
