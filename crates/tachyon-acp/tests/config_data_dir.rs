//! Config-file `data_dir` override (review fix 2): the adapter resolves
//! the data dir with the SAME precedence as `tachyon-app`
//! (env > config file > platform default), so a gateway found via a
//! config-declared `data_dir` is found by the adapter too — and the env
//! layer still beats the config file.

use serde_json::json;

mod common;
use common::{Adapter, parse_frame, test_dir};

const INITIALIZE: &str =
    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#;

/// A gateway lives ONLY in the config file's `data_dir`; the platform
/// default is isolated to an empty `XDG_DATA_HOME`. The adapter must
/// probe the CONFIG-declared dir (initialize succeeds) — if the config
/// layer were ignored, the platform default would be empty and every
/// request would fail `-32001`.
#[tokio::test]
async fn config_file_data_dir_drives_endpoint_discovery() {
    let gateway_dir = test_dir();
    let gateway = tachyon_gateway::start(&gateway_dir)
        .await
        .expect("gateway starts");

    let config_dir = test_dir();
    let config_path = config_dir.join("config.json");
    std::fs::write(&config_path, json!({ "data_dir": gateway_dir }).to_string())
        .expect("config file writes");

    let mut adapter = Adapter::spawn_with_config(Some(&config_path), None);
    adapter.send(INITIALIZE).await;
    adapter.close_stdin();
    let (replies, stderr, exit_ok) = adapter.finish().await;

    assert!(exit_ok, "gateway-down would not crash the adapter");
    assert_eq!(replies.len(), 1, "one frame: {replies:?}");
    let frame = parse_frame(&replies[0]);
    assert!(
        frame.get("error").is_none(),
        "the config-declared data dir must be probed: {replies:?} (stderr: {stderr})"
    );
    assert_eq!(frame["result"]["protocolVersion"], 1);
    // The startup log names the resolved dir — the config layer won.
    assert!(
        stderr.contains(&gateway_dir.display().to_string()),
        "stderr must log the config-resolved data_dir: {stderr}"
    );

    gateway.shutdown().await;
}

/// Precedence pin: `TACHYON_DATA_DIR` still beats the config file —
/// the adapter points at the env dir (empty), so the config-declared
/// gateway is NOT found and the error names the env dir's endpoint.
#[tokio::test]
async fn data_dir_env_still_beats_the_config_file() {
    let gateway_dir = test_dir();
    let gateway = tachyon_gateway::start(&gateway_dir)
        .await
        .expect("gateway starts");

    let config_dir = test_dir();
    let config_path = config_dir.join("config.json");
    std::fs::write(&config_path, json!({ "data_dir": gateway_dir }).to_string())
        .expect("config file writes");

    let env_dir = test_dir();
    let mut adapter = Adapter::spawn_with_config(Some(&config_path), Some(&env_dir));
    adapter.send(INITIALIZE).await;
    adapter.close_stdin();
    let (replies, _stderr, exit_ok) = adapter.finish().await;

    assert!(exit_ok);
    assert_eq!(replies.len(), 1, "one frame: {replies:?}");
    let frame = parse_frame(&replies[0]);
    assert_eq!(frame["error"]["code"], -32001, "reply: {}", replies[0]);
    let message = frame["error"]["message"].as_str().unwrap();
    assert!(
        message.contains(&env_dir.join("gateway.json").display().to_string()),
        "the env dir wins over the config file: {message}"
    );

    gateway.shutdown().await;
}
