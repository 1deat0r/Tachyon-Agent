//! Gateway-down contract (ticket 01 acceptance, ADR-0005:35): with no
//! usable endpoint, EVERY request fails with one clear actionable typed
//! error, the notification still gets no reply, and the adapter never
//! starts, restarts, or creates anything for a gateway.

use serde_json::json;

mod common;
use common::{Adapter, parse_frame, test_dir};

/// No endpoint file at all: the missing-`gateway.json` case.
#[tokio::test]
async fn every_request_fails_actionably_and_nothing_is_ever_spawned() {
    let dir = test_dir();
    assert!(!dir.join("gateway.json").exists());

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#)
        .await;
    adapter
        .send(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
        .await;
    adapter
        .send(r#"{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}"#)
        .await;
    adapter
        .send(r#"{"jsonrpc":"2.0","id":3,"method":"bogus/method"}"#)
        .await;
    adapter.close_stdin();
    let (replies, stderr, exit_ok) = adapter.finish().await;

    assert!(exit_ok, "gateway-down is not an adapter crash");
    assert_eq!(
        replies.len(),
        3,
        "ONE error per request, none for the notification: {replies:?}"
    );

    let expected_ids = [json!(1), json!(2), json!(3)];
    for (reply, expected_id) in replies.iter().zip(expected_ids) {
        let frame = parse_frame(reply);
        assert!(
            frame.get("result").is_none(),
            "no request can succeed without a gateway: {reply}"
        );
        assert_eq!(frame["error"]["code"], -32001, "reply: {reply}");
        assert_eq!(frame["error"]["data"], "gateway_unavailable");
        let message = frame["error"]["message"].as_str().unwrap();
        assert!(
            message.starts_with("Tachyon gateway unavailable: no usable endpoint at "),
            "unhelpful message: {message}"
        );
        assert!(
            message.contains(&dir.join("gateway.json").display().to_string()),
            "message names the missing endpoint file: {message}"
        );
        assert!(
            message.ends_with("Start it with `tachyon gateway` and retry."),
            "message tells the operator what to do: {message}"
        );
        assert_eq!(frame["id"], expected_id, "each error echoes its request id");
    }

    // The failure is logged to stderr as well.
    assert!(
        stderr.contains("gateway unavailable"),
        "stderr logs the outage: {stderr}"
    );

    // No gateway process was ever started and nothing was created:
    // no endpoint file, no socket, no writes at all.
    assert!(
        !dir.join("gateway.json").exists(),
        "the adapter must never create the endpoint file"
    );
    assert!(
        !dir.join("gateway.sock").exists(),
        "the adapter must never bind a gateway socket"
    );
    let entries: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        entries.is_empty(),
        "the adapter wrote nothing into the data dir: {entries:?}"
    );
}

/// Stale endpoint file (points at a dead socket): same typed error,
/// still no process and no socket of our own.
#[tokio::test]
async fn stale_endpoint_fails_actionably_without_a_listener() {
    let dir = test_dir();
    let dead_socket = dir.join("dead.sock");
    let endpoint = serde_json::json!({
        "socket_path": dead_socket,
        "pid": 4242,
        "started_at_micros": 0_i64,
        "protocol_version": 2_u32,
    });
    std::fs::write(
        dir.join("gateway.json"),
        serde_json::to_vec_pretty(&endpoint).unwrap(),
    )
    .unwrap();

    let mut adapter = Adapter::spawn(&dir);
    adapter
        .send(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#)
        .await;
    adapter.close_stdin();
    let (replies, _stderr, exit_ok) = adapter.finish().await;

    assert!(exit_ok);
    assert_eq!(replies.len(), 1, "one error: {replies:?}");
    let frame = parse_frame(&replies[0]);
    assert_eq!(frame["error"]["code"], -32001);
    let message = frame["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("cannot connect to"),
        "detail names the refused connect: {message}"
    );
    assert!(
        message.contains(&dead_socket.display().to_string()),
        "detail names the dead socket: {message}"
    );
    assert!(!dead_socket.exists(), "the adapter never binds anything");
}
