//! Ticket 01 (ACP MCP-stdio slice): pinned MCP server descriptors.
//!
//! External contract under test —
//!
//!   * `RegisterMCPServers` validates each descriptor as untrusted input
//!     (opaque id, absolute command, bounded NUL-free args, well-formed
//!     env names, bounded values, no dangerous variables) and pins the
//!     valid set durably with status `awaiting_approval` — the launch
//!     parks until ticket-02 `ApproveMCPServers` consumes the register
//!     call's approval id; nothing launches here;
//!   * any rejection fails with typed `invalid_mcp_descriptor` and pins
//!     nothing (no partial rows, no registered secrets);
//!   * `secret: true` env values persist and list as broker handles only;
//!   * `ListMCPServers` reports the parked descriptors with handles-only
//!     secrets and status `awaiting_approval`, surviving a gateway restart;
//!   * unknown sessions fail both commands with typed `unknown_session`;
//!   * re-registering upserts the named `server_id` rows and leaves
//!     unnamed rows untouched.

use tachyon_gateway::start;
use tachyon_protocol::{Command, McpEnvEntry, McpServerDescriptor};
use tachyon_types::SessionId;

mod common;
use common::{code_of, err, ok, public_env, secret_arg, secret_env, test_dir};

fn plain(server_id: &str) -> McpServerDescriptor {
    McpServerDescriptor {
        server_id: server_id.to_owned(),
        command: "/usr/bin/fake-mcp-server".to_owned(),
        args: vec!["--stdio".into()],
        env: vec![],
    }
}

fn with_env(server_id: &str, env: Vec<McpEnvEntry>) -> McpServerDescriptor {
    let mut server = plain(server_id);
    server.env = env;
    server
}

async fn create_session(socket: &std::path::Path) -> String {
    ok(
        socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn register(
    socket: &std::path::Path,
    session_id: &str,
    servers: Vec<McpServerDescriptor>,
) -> serde_json::Value {
    ok(
        socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers,
        },
    )
    .await
}

async fn list(socket: &std::path::Path, session_id: &str) -> serde_json::Value {
    ok(
        socket,
        Command::ListMCPServers {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await
}

#[tokio::test]
async fn pin_then_list_reports_handles_only_and_survives_restart() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_session(&socket).await;

    let raw_secret = "super-secret-token-xyz-123";
    let registered = register(
        &socket,
        &session_id,
        vec![
            plain("alpha"),
            with_env(
                "beta",
                vec![
                    public_env("LOG_LEVEL", "debug"),
                    secret_env("API_TOKEN", raw_secret),
                ],
            ),
        ],
    )
    .await;
    assert_eq!(
        registered["servers"],
        serde_json::json!([
            {"server_id": "alpha", "status": "awaiting_approval"},
            {"server_id": "beta", "status": "awaiting_approval"},
        ]),
        "register parks the launch and reports the wait"
    );
    assert!(
        registered["approval_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "register returns the session-scoped approval id the launch waits on"
    );

    let listed = list(&socket, &session_id).await;
    let servers = listed["servers"].as_array().unwrap();
    assert_eq!(servers.len(), 2);
    let alpha = &servers[0];
    assert_eq!(alpha["server_id"], "alpha");
    assert_eq!(alpha["command"], "/usr/bin/fake-mcp-server");
    assert_eq!(
        alpha["args"],
        serde_json::json!([{"value": "--stdio", "secret": false}])
    );
    assert_eq!(alpha["status"], "awaiting_approval");
    let beta = &servers[1];
    assert_eq!(beta["status"], "awaiting_approval");
    let beta_env = beta["env"].as_array().unwrap();
    assert_eq!(beta_env.len(), 2);
    assert_eq!(beta_env[0]["name"], "LOG_LEVEL");
    assert_eq!(beta_env[0]["value"], "debug");
    assert_eq!(beta_env[0]["secret"], false);
    let handle = beta_env[1]["value"].as_str().unwrap();
    assert_eq!(beta_env[1]["name"], "API_TOKEN");
    assert_eq!(beta_env[1]["secret"], true);
    assert_ne!(handle, raw_secret, "secret lists as a handle, never raw");
    assert!(
        handle.contains("mcp-secret"),
        "handle names the broker vault: {handle}"
    );
    let listed_raw = serde_json::to_string(&listed).unwrap();
    assert!(
        !listed_raw.contains(raw_secret),
        "raw secret appears nowhere in list output"
    );

    gateway.shutdown().await;
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let again = list(&socket, &session_id).await;
    assert_eq!(again, listed, "pins are durable across a gateway restart");
    let again_raw = serde_json::to_string(&again).unwrap();
    assert!(
        !again_raw.contains(raw_secret),
        "handles — not raw secrets — survive the reopen"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Ticket 02 (ACP env-secrets slice): `secret: true` argv entries pin,
/// list, and restart exactly like secret env — the raw value registers
/// with the vault at pin time, every list frame carries only the broker
/// handle, and a non-secret neighbour arg still echoes raw.
#[tokio::test]
async fn secret_arg_pins_as_handle_and_never_lists_raw_bytes() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_session(&socket).await;

    let raw_secret = "raw-arg-secret-token-777";
    let mut guarded = plain("guarded");
    guarded.args = vec!["--stdio".into(), secret_arg(raw_secret)];
    register(&socket, &session_id, vec![guarded]).await;

    let listed = list(&socket, &session_id).await;
    let args = listed["servers"][0]["args"].as_array().unwrap();
    assert_eq!(
        args[0],
        serde_json::json!({"value": "--stdio", "secret": false}),
        "a non-secret arg still echoes its raw value"
    );
    assert_eq!(args[1]["secret"], true);
    let handle = args[1]["value"].as_str().unwrap();
    assert_ne!(
        handle, raw_secret,
        "secret arg lists as a handle, never raw"
    );
    assert!(
        handle.contains("mcp-secret"),
        "handle names the broker vault: {handle}"
    );
    let listed_raw = serde_json::to_string(&listed).unwrap();
    assert!(
        !listed_raw.contains(raw_secret),
        "raw secret arg appears nowhere in list output"
    );

    gateway.shutdown().await;
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let again = list(&socket, &session_id).await;
    assert_eq!(
        again, listed,
        "secret-arg pins are durable across a gateway restart"
    );
    let again_raw = serde_json::to_string(&again).unwrap();
    assert!(
        !again_raw.contains(raw_secret),
        "handles — not raw secret args — survive the reopen"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
// One rejection matrix: splitting the register-refusal catalogue would
// hide the single untrusted-input contract it pins.
#[allow(clippy::too_many_lines)]
async fn malicious_descriptors_are_rejected_and_pin_nothing() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_session(&socket).await;

    let mut relative = plain("bad-command");
    relative.command = "relative/server".to_owned();
    let mut empty_id = plain("x");
    empty_id.server_id = String::new();
    let mut long_id = plain("long");
    long_id.server_id = "s".repeat(65);
    let mut many_args = plain("many-args");
    many_args.args = vec!["a".into(); 33];
    let mut big_arg = plain("big-arg");
    big_arg.args = vec!["x".repeat(4097).into()];
    let mut nul_arg = plain("nul-arg");
    nul_arg.args = vec!["a\0b".into()];
    let mut big_env = plain("big-env");
    big_env.env = vec![public_env("BLOB", &"v".repeat(16385))];

    let mut slash_id = plain("x");
    slash_id.server_id = "a/b".to_owned();

    let mut cases: Vec<(&str, McpServerDescriptor)> = vec![
        ("relative command", relative),
        ("empty server id", empty_id),
        ("65-byte server id", long_id),
        ("slash server id", slash_id),
        ("33 args", many_args),
        ("oversized arg", big_arg),
        ("NUL arg", nul_arg),
        ("oversized env value", big_env),
    ];
    for var in [
        // Loader hijack.
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "LD_AUDIT",
        "GCONV_PATH",
        "DYLD_INSERT_LIBRARIES",
        "DYLD_FALLBACK_LIBRARY_PATH",
        // Interpreter / startup injection.
        "BASH_ENV",
        "ENV",
        "SHELLOPTS",
        "PS4",
        "IFS",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "NODE_OPTIONS",
        "PERL5OPT",
        "OPENSSL_CONF",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "GIT_SSH_COMMAND",
        "GIT_SSH",
        "GIT_EXEC_PATH",
        "GIT_TEMPLATE_DIR",
        "JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
        "RUBYOPT",
        "GIT_CONFIG_COUNT",
    ] {
        let mut server = plain("dangerous");
        server.env = vec![public_env(var, "/tmp/evil.so")];
        cases.push(("dangerous env", server));
    }
    // An explicit entry may never override an inherited allowlist
    // location key: refused by NAME, whatever the value claims.
    for var in tachyon_tools::process::INHERITED_ENV_KEYS {
        let mut server = plain("inherited-override");
        server.env = vec![public_env(var, "/hostile/location")];
        cases.push(("inherited allowlist override", server));
    }
    for bad_name in ["9LIVES", "HAS-DASH", "HAS SPACE", "a$b", ""] {
        let mut server = plain("bad-env-name");
        server.env = vec![public_env(bad_name, "x")];
        cases.push(("bad env name", server));
    }

    for (what, server) in &cases {
        let failure = err(
            &socket,
            Command::RegisterMCPServers {
                session_id: session_id.parse().unwrap(),
                servers: vec![server.clone()],
            },
        )
        .await;
        assert_eq!(
            code_of(&failure),
            "invalid_mcp_descriptor",
            "{what} must fail typed"
        );
        for entry in &server.env {
            assert!(
                failure.contains(&entry.name),
                "{what}: the refusal names '{}': {failure}",
                entry.name
            );
            assert!(
                !failure.contains(&entry.value),
                "{what}: the refusal never echoes the value: {failure}"
            );
        }
    }
    let mut evil = plain("evil");
    evil.env = vec![public_env("LD_PRELOAD", "/tmp/evil.so")];
    let mixed = err(
        &socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![plain("good"), evil],
        },
    )
    .await;
    assert_eq!(code_of(&mixed), "invalid_mcp_descriptor");
    let duplicate = err(
        &socket,
        Command::RegisterMCPServers {
            session_id: session_id.parse().unwrap(),
            servers: vec![plain("same"), plain("same")],
        },
    )
    .await;
    assert_eq!(code_of(&duplicate), "invalid_mcp_descriptor");

    let listed = list(&socket, &session_id).await;
    assert_eq!(
        listed["servers"],
        serde_json::json!([]),
        "every rejection pins nothing — no partial rows"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn unknown_session_is_typed_for_both_commands() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let missing: SessionId = SessionId::generate();

    let register_failure = err(
        &socket,
        Command::RegisterMCPServers {
            session_id: missing,
            servers: vec![plain("ghost")],
        },
    )
    .await;
    assert_eq!(code_of(&register_failure), "unknown_session");

    let list_failure = err(
        &socket,
        Command::ListMCPServers {
            session_id: missing,
        },
    )
    .await;
    assert_eq!(code_of(&list_failure), "unknown_session");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn reregister_upserts_named_rows_only() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();
    let session_id = create_session(&socket).await;

    register(&socket, &session_id, vec![plain("a"), plain("b")]).await;

    let mut a_v2 = plain("a");
    a_v2.command = "/usr/bin/fake-mcp-server-v2".to_owned();
    register(&socket, &session_id, vec![a_v2, plain("c")]).await;

    let listed = list(&socket, &session_id).await;
    let servers = listed["servers"].as_array().unwrap();
    assert_eq!(servers.len(), 3, "unnamed rows survive a re-register");
    assert_eq!(servers[0]["server_id"], "a");
    assert_eq!(servers[0]["command"], "/usr/bin/fake-mcp-server-v2");
    assert_eq!(servers[0]["status"], "awaiting_approval");
    assert_eq!(servers[1]["server_id"], "b");
    assert_eq!(servers[1]["command"], "/usr/bin/fake-mcp-server");
    assert_eq!(servers[2]["server_id"], "c");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
