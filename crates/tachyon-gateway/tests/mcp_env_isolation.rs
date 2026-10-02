//! Ticket 01 (ACP env-secrets slice): env-isolation regression pin.
//!
//! External contract under test —
//!
//!   * a spawned MCP child's env is exactly the inherited allowlist
//!     plus the pinned descriptor entries: a registered provider API
//!     key sitting in the gateway's own environment is absent from
//!     the child;
//!   * the raw MCP secret reaches exactly one sink (the child's
//!     `execve`) — proven by the set-equality above and the
//!     expected-secret handshake — and the ambient provider key
//!     appears in neither a `process.spawn` inline receipt nor the
//!     artifact spool its stream overflows into.
//!
//! The MCP child is always a fake test script speaking
//! newline-delimited JSON-RPC over stdin/stdout — never a real server
//! binary. It dumps its whole environment to a file at startup, so the
//! isolation claims are file assertions, not guesses.

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use tachyon_gateway::start;
use tachyon_policy::{DefaultPosture, Policy};
use tachyon_protocol::Command;
use tachyon_tools::ToolsContext;
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_tools::credential::CredentialBroker;
use tachyon_tools::process::{self, INHERITED_ENV_KEYS, ProcessSpec};

mod common;
use common::{
    create_rooted_session, ok, public_env, script_server, secret_env, send, test_dir, write_script,
};

/// The provider API key the gateway process itself carries (registered
/// with the redactor at config load in production): it must never
/// reach an MCP child or a `process.spawn` receipt.
const PROVIDER_KEY_ENV: &str = "TACHYON_TEST_PROVIDER_API_KEY";
const PROVIDER_KEY: &str = "provider-key-never-for-mcp-children-0123456789";
/// The MCP secret: it may exist in exactly one place outside the
/// vault — the child's `execve` — and nowhere durable or receipted.
const MCP_SECRET: &str = "mcp-secret-reaches-only-the-child-4242";

/// The fake child: dumps its full environment as JSON at startup (the
/// isolation evidence), records spawn evidence, then handshakes like
/// `fake_ok` and sleeps until killed. A baked-in `__EXPECTED_SECRET__`
/// mismatch on `API_TOKEN` exits before the handshake, so the raw
/// secret provably reached the child only when launch succeeds.
const FAKE_DUMP_TEMPLATE: &str = r#"
import json, os, sys, time
dumpfile = os.environ.get("ENV_DUMP_FILE", "")
if dumpfile:
    with open(dumpfile, "w") as f:
        json.dump(dict(os.environ), f)
marker = os.environ.get("MARKER_FILE", "")
if marker:
    with open(marker, "a") as f:
        f.write("spawn\n")
pidfile = os.environ.get("PID_FILE", "")
if pidfile:
    with open(pidfile, "w") as f:
        f.write(str(os.getpid()))
expected = __EXPECTED_SECRET__
if expected and os.environ.get("API_TOKEN", "") != expected:
    sys.exit(1)
version = os.environ.get("REPORT_VERSION", "2024-11-05")
def readline():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    return json.loads(line)
req = readline()
assert req.get("method") == "initialize", req
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": req.get("id"),
    "result": {"protocolVersion": version,
               "serverInfo": {"name": "fake-mcp", "version": "0.1"}},
}) + "\n")
sys.stdout.flush()
msg = readline()
if "id" not in msg:
    msg = readline()
sys.stdout.write(json.dumps({
    "jsonrpc": "2.0", "id": msg.get("id"),
    "result": {"tools": [{"name": "echo", "description": "echo input"},
                          {"name": "add", "description": "add numbers"}]},
}) + "\n")
sys.stdout.flush()
while True:
    time.sleep(60)
"#;

async fn register(
    socket: &Path,
    session_id: &str,
    servers: Vec<tachyon_protocol::McpServerDescriptor>,
) -> Value {
    ok(
        socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers,
        },
    )
    .await
}

async fn approve(socket: &Path, session_id: &str, approval_id: &str) -> (u16, Value, String) {
    send(
        socket,
        Command::ApproveMCPServers {
            session_id: session_id.parse().unwrap(),
            approval_id: approval_id.parse().unwrap(),
        },
    )
    .await
}

fn child_pid(pidfile: &Path) -> Option<i32> {
    std::fs::read_to_string(pidfile)
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

#[cfg(target_os = "linux")]
fn process_alive(pid: i32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
// SAFETY: this is the only test in its own integration-test binary, so
// no other thread in this process reads the environment concurrently —
// the parent environment is the seam under test (the same pattern the
// `secret_env_allowlist` fixture uses).
#[allow(unsafe_code)]
// One narrative: pin → launch → child-env assertions → spawn-receipt
// assertions; splitting it would hide the single isolation contract.
#[allow(clippy::too_many_lines)]
async fn mcp_child_env_is_allowlist_plus_pins_and_secrets_stay_out_of_process_spawn() {
    unsafe { std::env::set_var(PROVIDER_KEY_ENV, PROVIDER_KEY) };
    // Mirror config load: the provider key is registered material in
    // the gateway's own environment, never in a child's. Probe the
    // broker the way the runtime does: the registered key scrubs to
    // its handle marker, never to itself.
    let mut redactor = CredentialBroker::default();
    let handle = redactor.register(PROVIDER_KEY.as_bytes(), "provider-api-key");
    let scrubbed = redactor.redact(PROVIDER_KEY);
    assert!(
        scrubbed.contains(&tachyon_tools::credential::redaction_for(&handle))
            && !scrubbed.contains(PROVIDER_KEY),
        "the registered provider key scrubs to a handle marker, never to itself: {scrubbed}"
    );

    let dir = test_dir();
    let root = dir.join("session-root");
    std::fs::create_dir_all(&root).unwrap();
    let script_body = FAKE_DUMP_TEMPLATE.replace(
        "__EXPECTED_SECRET__",
        &serde_json::to_string(MCP_SECRET).unwrap(),
    );
    let script = write_script(&dir, "fake_dump.py", &script_body);
    let marker = dir.join("spawns.log");
    let dump_file = dir.join("child-env.json");
    let pidfile = dir.join("child.pid");

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_rooted_session(&socket, &root).await;

    let registered = register(
        &socket,
        &session_id,
        vec![script_server(
            "isolated",
            &script,
            vec![
                public_env("MARKER_FILE", marker.to_str().unwrap()),
                public_env("ENV_DUMP_FILE", dump_file.to_str().unwrap()),
                public_env("PID_FILE", pidfile.to_str().unwrap()),
                secret_env("API_TOKEN", MCP_SECRET),
            ],
        )],
    )
    .await;
    let approval_id = registered["approval_id"].as_str().unwrap().to_owned();
    let (status, payload, message) = approve(&socket, &session_id, &approval_id).await;
    assert_eq!(status, 200, "approve launches the pinned set: {message}");
    assert_eq!(payload["servers"][0]["status"], "live");

    // 1. Child env = inherited allowlist + pinned entries ONLY.
    let dump: std::collections::HashMap<String, String> =
        serde_json::from_str(&std::fs::read_to_string(&dump_file).expect("child dumped its env"))
            .expect("env dump is a JSON object");
    let mut expected: HashSet<&str> = INHERITED_ENV_KEYS
        .iter()
        .copied()
        .filter(|key| std::env::var_os(key).is_some())
        .collect();
    expected.extend(["MARKER_FILE", "ENV_DUMP_FILE", "PID_FILE", "API_TOKEN"]);
    let actual: HashSet<&str> = dump.keys().map(String::as_str).collect();
    assert_eq!(
        actual, expected,
        "child env = inherited allowlist + pinned entries only"
    );
    assert!(
        !dump.contains_key(PROVIDER_KEY_ENV),
        "the registered provider API key is absent from the MCP child env"
    );
    assert!(
        !dump.values().any(|value| value == PROVIDER_KEY),
        "the provider key value reaches no MCP child"
    );
    assert_eq!(
        dump.get("API_TOKEN").map(String::as_str),
        Some(MCP_SECRET),
        "the raw MCP secret reaches exactly one sink: the child's execve"
    );

    // 2. The ambient provider key never appears in a `process.spawn`
    // receipt or the artifact spool its stream overflows into. `env`
    // prints first so any leaked secret would land in the inline
    // body; the padding pushes the stream past the inline cap so the
    // spool is exercised too. (The MCP secret is never ambient in
    // this process — its receipt absence is structural, pinned by the
    // child-env set-equality above and the vault greps elsewhere.)
    let mut policy = Policy::new(DefaultPosture::Deny);
    policy.allow("process.spawn", "/bin/sh");
    let context = ToolsContext::new(
        dir.clone(),
        policy,
        ArtifactSpool::new(dir.join("spawn-artifacts")),
    );
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec![
        "-c".to_owned(),
        "env; head -c 1200000 /dev/zero | tr '\\0' 'a'".to_owned(),
    ];
    spec.cwd = Some(dir.clone());
    spec.timeout = Duration::from_secs(20);
    let receipt = process::run(&context, &spec)
        .await
        .expect("spawn child runs");
    assert_eq!(receipt.exit_code, Some(0), "spawn child completed");
    assert!(
        receipt.stdout_truncated,
        "padding pushes the env past the inline cap so the spool is exercised"
    );
    let inline = String::from_utf8_lossy(&receipt.stdout);
    assert!(
        !inline.contains(PROVIDER_KEY),
        "provider key leaked into the inline receipt"
    );
    let artifact = receipt.stdout_artifact.expect("overflow stream is spooled");
    let spooled = context
        .artifacts
        .fetch(&artifact)
        .expect("fetch the overflow artifact");
    let spooled = String::from_utf8_lossy(&spooled);
    assert!(
        !spooled.contains(PROVIDER_KEY),
        "provider key leaked into the artifact spool"
    );

    // The live child is torn down with the gateway; wait for the reap
    // so no fake child outlives the test process.
    gateway.shutdown().await;
    #[cfg(target_os = "linux")]
    {
        if let Some(pid) = child_pid(&pidfile) {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while process_alive(pid) && std::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(!process_alive(pid), "the MCP child is reaped at shutdown");
        }
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
