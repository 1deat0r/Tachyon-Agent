//! Ticket 02 acceptance: `session/new` against a live test gateway —
//! typed refusals happen BEFORE any gateway interaction, accepted
//! sessions round-trip the gateway's own `session_id` byte-for-byte
//! (identity mapping, no alias store), and gateway-side refusals
//! surface as typed errors.

use serde_json::json;

mod common;
use common::{Adapter, gw_ok, parse_frame, test_dir};

fn new_line(id: u32, params: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"session/new","params":{params}}}"#)
}

/// Rejection proof with NO gateway at all: if validation ran after the
/// liveness probe, these would answer `-32001` gateway-unavailable
/// instead of the typed param errors — so the typed answers prove no
/// gateway call was issued. The data dir stays untouched.
#[tokio::test]
async fn session_new_rejects_bad_params_before_any_gateway_call() {
    let dir = test_dir();
    assert!(!dir.join("gateway.json").exists());

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&new_line(1, r#"{"cwd":"relative/dir"}"#))
        .await;
    adapter
        .send(&new_line(
            2,
            r#"{"cwd":"/tmp","mcpServers":[{"transport":{"type":"stdio"},"command":"srv"}]}"#,
        ))
        .await;
    adapter.send(&new_line(3, r#"{"cwd":42}"#)).await;
    adapter.close_stdin();
    let (replies, _stderr, exit_ok) = adapter.finish().await;

    assert!(exit_ok, "validation refusals are not a crash");
    assert_eq!(replies.len(), 3, "one frame per request: {replies:?}");

    let relative = parse_frame(&replies[0]);
    assert_eq!(relative["id"], 1);
    assert_eq!(relative["error"]["code"], -32602);
    assert_eq!(relative["error"]["data"], json!("cwd_not_absolute"));

    let mcp = parse_frame(&replies[1]);
    assert_eq!(mcp["id"], 2);
    assert_eq!(mcp["error"]["code"], -32602);
    assert_eq!(mcp["error"]["data"], json!("mcp_servers_unsupported"));

    let missing = parse_frame(&replies[2]);
    assert_eq!(missing["id"], 3);
    assert_eq!(missing["error"]["code"], -32602);
    assert_eq!(missing["error"]["data"], json!("invalid_params"));

    // Nothing was ever written: the adapter never dialed, let alone
    // started, a gateway.
    let entries: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(entries.is_empty(), "adapter wrote nothing: {entries:?}");
}

/// Accept/reject against a live gateway: absolute+existing cwd creates
/// the session and the ACP `sessionId` IS the gateway's `session_id`
/// byte-for-byte; empty `mcpServers` is accepted; a gateway refusal
/// (nonexistent root) surfaces typed with the gateway code.
#[tokio::test]
async fn session_new_round_trips_the_gateway_session_id() {
    let dir = test_dir();
    let workspace = dir.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let gateway = tachyon_gateway::start(&dir).await.expect("gateway starts");
    let socket = gateway.address().to_owned();

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(&new_line(
            1,
            &format!(r#"{{"cwd":{}}}"#, json!(workspace.display().to_string())),
        ))
        .await;
    adapter
        .send(&new_line(
            2,
            &format!(
                r#"{{"cwd":{},"mcpServers":[]}}"#,
                json!(workspace.display().to_string())
            ),
        ))
        .await;
    adapter
        .send(&new_line(
            3,
            r#"{"cwd":"/definitely/not/a/real/tachyon/workspace/dir"}"#,
        ))
        .await;
    adapter.close_stdin();
    let (replies, _stderr, exit_ok) = adapter.finish().await;

    assert!(exit_ok);
    assert_eq!(replies.len(), 3, "one frame per request: {replies:?}");

    // Accepted: exactly {sessionId}, nothing else.
    let created = parse_frame(&replies[0]);
    assert_eq!(created["id"], 1);
    assert!(created.get("error").is_none(), "reply: {}", replies[0]);
    let session_id = created["result"]["sessionId"].as_str().expect("sessionId");
    let keys: Vec<&str> = created["result"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["sessionId"], "reply: {}", replies[0]);

    // Identity mapping: the gateway's own row echoes the SAME id
    // byte-for-byte (no alias store anywhere).
    let gateway_view = gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await;
    assert_eq!(
        gateway_view["session_id"].as_str(),
        Some(session_id),
        "the ACP sessionId IS the durable gateway session id"
    );
    assert_eq!(
        gateway_view["workspace_root"].as_str(),
        Some(workspace.display().to_string().as_str()),
        "the session pins the exact root the adapter forwarded"
    );

    // Empty mcpServers accepted too.
    let second = parse_frame(&replies[1]);
    assert_eq!(second["id"], 2);
    assert!(second.get("error").is_none(), "reply: {}", replies[1]);
    let second_id = second["result"]["sessionId"].as_str().unwrap();
    assert_ne!(second_id, session_id, "each session/new mints a new id");
    gw_ok(
        &socket,
        tachyon_protocol::Command::GetSession {
            session_id: second_id.parse().unwrap(),
        },
    )
    .await;

    // Gateway-side refusal (root does not exist) surfaces typed, with
    // the gateway's own code as the machine-readable marker.
    let refused = parse_frame(&replies[2]);
    assert_eq!(refused["id"], 3);
    assert_eq!(refused["error"]["code"], -32002, "reply: {}", replies[2]);
    assert_eq!(refused["error"]["data"], json!("workspace_not_found"));

    gateway.shutdown().await;
}
