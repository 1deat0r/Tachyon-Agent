//! Live-gateway round trip (ticket 01 acceptance): the real
//! `tachyon-acp` binary negotiates `initialize` against a test gateway,
//! unknown and unimplemented session methods answer standard
//! method-not-found, stderr carries logs, and stdout carries only valid
//! ACP JSON-RPC frames (ADR-0005:27).

use serde_json::{Value, json};

mod common;
use common::{Adapter, parse_frame, test_dir};

#[tokio::test]
async fn initialize_succeeds_against_a_live_gateway_with_clean_framing() {
    let dir = test_dir();
    let gateway = tachyon_gateway::start(&dir).await.expect("start gateway");

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{"fs":{"readTextFile":true,"writeTextFile":true},"terminal":true},"clientInfo":{"name":"test-client","title":"Test Client","version":"0.0.0"}}}"#,
        )
        .await;
    adapter
        .send(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
        .await;
    adapter
        .send(r#"{"jsonrpc":"2.0","id":2,"method":"no/such/method"}"#)
        .await;
    adapter
        .send(
            r#"{"jsonrpc":"2.0","id":3,"method":"session/load","params":{"sessionId":"01990f9e-1111-7000-8000-000000000000","cwd":"/tmp"}}"#,
        )
        .await;
    adapter.close_stdin();
    let (replies, stderr, exit_ok) = adapter.finish().await;
    gateway.shutdown().await;

    assert!(exit_ok, "adapter must exit cleanly on stdin EOF");
    assert_eq!(
        replies.len(),
        3,
        "one reply per request, none for the notification: {replies:?}"
    );

    // stdout carries ONLY valid ACP JSON-RPC frames.
    let frames: Vec<Value> = replies.iter().map(|line| parse_frame(line)).collect();

    // initialize: negotiated version + exactly the implemented set.
    assert_eq!(frames[0]["id"], 1);
    let result = &frames[0]["result"];
    assert_eq!(result["protocolVersion"], 1);
    let capabilities = &result["agentCapabilities"];
    assert_eq!(capabilities["loadSession"], false);
    assert_eq!(
        capabilities["promptCapabilities"],
        json!({"image": false, "audio": false, "embeddedContext": false})
    );
    assert!(
        capabilities.get("mcpCapabilities").is_none(),
        "unimplemented capabilities stay unadvertised: {capabilities}"
    );
    assert!(
        capabilities.get("fs").is_none() && capabilities.get("terminal").is_none(),
        "no client filesystem/terminal claims: {capabilities}"
    );
    assert_eq!(result["agentInfo"]["name"], "tachyon-acp");
    assert_eq!(result["agentInfo"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(result["authMethods"], json!([]));
    let mut result_keys: Vec<&str> = result
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    result_keys.sort_unstable();
    assert_eq!(
        result_keys,
        [
            "agentCapabilities",
            "agentInfo",
            "authMethods",
            "protocolVersion"
        ],
        "the advertisement is exactly the documented shape: {result}"
    );

    // Unknown method: standard -32601. `session/load` has a real arm:
    // valid params + a session that does not exist in the live gateway
    // answers the typed `-32002 unknown_session` (identity check first,
    // replay never starts).
    assert_eq!(frames[1]["error"]["code"], -32601);
    assert_eq!(frames[1]["error"]["message"], "Method not found");
    assert_eq!(frames[1]["id"], 2);
    assert_eq!(frames[2]["error"]["code"], -32002);
    assert_eq!(frames[2]["error"]["data"], json!("unknown_session"));
    assert_eq!(frames[2]["id"], 3);

    // stderr carries logs while stdout stayed frame-clean.
    assert!(!stderr.trim().is_empty(), "stderr must carry logs");
    assert!(
        stderr.contains("negotiated protocolVersion=1"),
        "stderr logs the handshake: {stderr}"
    );
    assert!(
        stderr.contains("accepted notifications/initialized"),
        "stderr logs the notification: {stderr}"
    );
}
