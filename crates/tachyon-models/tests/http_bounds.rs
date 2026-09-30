//! Bounded HTTP read (G6/SECURITY.md): a hostile or broken server must
//! not OOM the process, so the transport stops at
//! [`tachyon_models::openai_compat::MAX_RESPONSE_BYTES`].
//!
//! This test lives here rather than in `openai_compat`'s inline test
//! module because `scripts/m14_suites.sh` (G6) forbids a `TcpListener`
//! anywhere in shipped `crates/*/src`: a loopback actor belongs in
//! `tests/`, which is exactly where that gate's positive control looks.

use tachyon_models::openai_compat::{MAX_RESPONSE_BYTES, round_trip};

#[tokio::test]
async fn huge_response_read_is_bounded() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        let (mut socket, _peer) = listener.accept().await.expect("accept");
        // Drain the request head so the eventual close is a clean FIN;
        // otherwise the kernel resets the connection and the test would
        // measure reset handling instead of the read bound.
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut chunk).await.expect("read request");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 2097152\r\nConnection: close\r\n\r\n";
        let oversized_body = "x".repeat(2 * 1024 * 1024);
        // The client may stop reading at the bound and hang up; a
        // failed late write is exactly the scenario under test.
        let _ignored = socket.write_all(head.as_bytes()).await;
        let _ignored = socket.write_all(oversized_body.as_bytes()).await;
    });
    let request =
        "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    let raw = round_trip(&address.to_string(), request)
        .await
        .expect("bounded read must not error on an oversized response");
    assert!(
        raw.len() <= MAX_RESPONSE_BYTES,
        "buffered {} bytes, bound is {MAX_RESPONSE_BYTES}",
        raw.len()
    );
    let _ignored = server.await;
}
