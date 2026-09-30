//! Real-socket transport tests: bounds, incremental streaming, TLS.
//!
//! Every case here binds a loopback listener, which G6 allows only in
//! `tests/` — shipped `src/` may not contain a TCP listener type.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tachyon_models::openai_compat::{HttpTransport, MAX_RESPONSE_BYTES, TcpHttpTransport};
use tachyon_models::{ModelError, ModelEvent};

/// Test CA, valid to 2126: the trust anchor this file hands to
/// [`TcpHttpTransport::with_extra_root_pem`]. Nothing in `src/` trusts it.
const TEST_CA: &str = concat!(
    "-----BEGIN CERTIFICATE-----\n",
    "MIIBmzCCAUGgAwIBAgIUTHJiXHR+9W+/NwnKEPq//WoVVW0wCgYIKoZIzj0EAwIw\n",
    "GjEYMBYGA1UEAwwPVGFjaHlvbiBUZXN0IENBMCAXDTI2MDkzMDAxNTU0NVoYDzIx\n",
    "MjYwOTA2MDE1NTQ1WjAaMRgwFgYDVQQDDA9UYWNoeW9uIFRlc3QgQ0EwWTATBgcq\n",
    "hkjOPQIBBggqhkjOPQMBBwNCAAT5J45avz/zaF99fzaJGIWhsfpnwSxIqvRjag8z\n",
    "tjx7jl4Emh41BveYzxcswEXnGz4vdtc0fd6Vg9XhGkhfzPYNo2MwYTAdBgNVHQ4E\n",
    "FgQUWbqV1WiLJbkBj/oYgmp2KklaxNUwHwYDVR0jBBgwFoAUWbqV1WiLJbkBj/oY\n",
    "gmp2KklaxNUwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMCAQYwCgYIKoZI\n",
    "zj0EAwIDSAAwRQIgMIqDjQGBbfHArF+PMoQMPJK2dulVOuvtswvZeiW7BSwCIQDE\n",
    "PQVyv5Juo7khRIjM19A/CzgkIW3SzGyHv3Jm3/PSwQ==\n",
    "-----END CERTIFICATE-----\n",
);

/// Its leaf, `CN=localhost` (SAN: DNS:localhost, IP:127.0.0.1).
const TEST_LEAF: &str = concat!(
    "-----BEGIN CERTIFICATE-----\n",
    "MIIBxTCCAWugAwIBAgIUKgLe4pI05WWD2qN9aoZbuHXhqz8wCgYIKoZIzj0EAwIw\n",
    "GjEYMBYGA1UEAwwPVGFjaHlvbiBUZXN0IENBMCAXDTI2MDkzMDAxNTU0NVoYDzIx\n",
    "MjYwOTA2MDE1NTQ1WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjOPQIB\n",
    "BggqhkjOPQMBBwNCAAR5TQWlnc7NdxAvmOOygSl2yOI3+aWbjgRiyfP7bdv4VtaI\n",
    "wxGj/UKXwEuuL1G39ELWHF8y25Wa2O/nacBgWUTRo4GSMIGPMAwGA1UdEwEB/wQC\n",
    "MAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBoGA1UdEQQT\n",
    "MBGCCWxvY2FsaG9zdIcEfwAAATAdBgNVHQ4EFgQUCHwfKk1OYDBeItnV1PGdneR2\n",
    "32gwHwYDVR0jBBgwFoAUWbqV1WiLJbkBj/oYgmp2KklaxNUwCgYIKoZIzj0EAwID\n",
    "SAAwRQIhAMKlsJU4nOg/yPTx75OfBxyFx8bvsF9+JCNQtGdus9xiAiASpseVniuz\n",
    "WtoBjiFMsnaEoGJgImdZX1Pl7PbIeOP+fg==\n",
    "-----END CERTIFICATE-----\n",
);

/// The leaf's EC P-256 private key.
const TEST_KEY: &str = concat!(
    "-----BEGIN PRIVATE KEY-----\n",
    "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg81pzZSAVIK204TBD\n",
    "nAieZsR+5d6KF+9qS0RH4/IFEW6hRANCAAR5TQWlnc7NdxAvmOOygSl2yOI3+aWb\n",
    "jgRiyfP7bdv4VtaIwxGj/UKXwEuuL1G39ELWHF8y25Wa2O/nacBgWUTR\n",
    "-----END PRIVATE KEY-----\n",
);

/// Reads one request head. The tests never send a body the server needs.
async fn take_request_head<R>(reader: &mut R)
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut head = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        let Ok(read) = reader.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        head.extend_from_slice(&chunk[..read]);
    }
}

const COMPLETION: &str = r#"{"choices":[{"message":{"content":"{\"decision\":\"respond\",\"message\":\"hi\"}"}}],"usage":{"prompt_tokens":11,"completion_tokens":7}}"#;

fn json_head(len: usize) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
    )
}

#[tokio::test]
async fn a_plain_loopback_response_is_read_end_to_end() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (mut socket, _) = listener.accept().await.expect("accept");
        take_request_head(&mut socket).await;
        let head = json_head(COMPLETION.len());
        let _ignored = socket.write_all(head.as_bytes()).await;
        let _ignored = socket.write_all(COMPLETION.as_bytes()).await;
    });

    let transport = TcpHttpTransport::new(false);
    let body = transport
        .post_json(
            &format!("http://{address}/v1/chat/completions"),
            Some("k"),
            "{}",
            5_000,
        )
        .await
        .expect("a loopback completion is readable");
    assert_eq!(body, COMPLETION);
    server.await.expect("server task");
}

#[tokio::test]
async fn an_oversized_response_is_refused_at_the_bound() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (mut socket, _) = listener.accept().await.expect("accept");
        take_request_head(&mut socket).await;
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 2097152\r\nConnection: close\r\n\r\n";
        let _ignored = socket.write_all(head.as_bytes()).await;
        // Keep feeding past the bound; the client must stop us.
        let chunk = "x".repeat(64 * 1024);
        for _ in 0..64 {
            if socket.write_all(chunk.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    let transport = TcpHttpTransport::new(false);
    let error = transport
        .post_json(
            &format!("http://{address}/v1/chat/completions"),
            None,
            "{}",
            5_000,
        )
        .await
        .expect_err("a response past the bound must fail closed");
    match error {
        ModelError::Transport(message) => assert!(
            message.contains("exceeds") && message.contains(&MAX_RESPONSE_BYTES.to_string()),
            "the refusal must name the bound: {message}"
        ),
        other => panic!("expected a bound refusal, got {other:?}"),
    }
    let _ = server.await;
}

#[tokio::test]
async fn content_length_is_the_body_length_not_a_hint() {
    // A server that sends more than it declared (a proxy that ignores
    // `Connection: close`) must not have the surplus become our body.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (mut socket, _) = listener.accept().await.expect("accept");
        take_request_head(&mut socket).await;
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n";
        let _ignored = socket.write_all(head.as_bytes()).await;
        let _ignored = socket.write_all(b"1234567890").await;
    });

    let transport = TcpHttpTransport::new(false);
    let body = transport
        .post_json(
            &format!("http://{address}/v1/chat/completions"),
            None,
            "{}",
            5_000,
        )
        .await
        .expect("the declared body is readable");
    assert_eq!(body, "12345", "surplus past Content-Length is discarded");
    server.await.expect("server task");
}

#[tokio::test]
async fn server_sent_events_reach_the_sink_before_the_stream_ends() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (mut socket, _) = listener.accept().await.expect("accept");
        take_request_head(&mut socket).await;
        let head =
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";
        let _ignored = socket.write_all(head.as_bytes()).await;
        let _ignored = socket
            .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
            .await;
        let _ignored = socket.flush().await;
        // Hold the stream open: the first delta must arrive anyway.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ignored = socket
            .write_all(
                b"data: {\"choices\":[{\"delta\":{\"content\":\" second\"}}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n",
            )
            .await;
        let _ignored = socket.write_all(b"data: [DONE]\n\n").await;
        let _ignored = socket.flush().await;
    });

    let transport = TcpHttpTransport::new(false);
    let (sink, mut events) = tokio::sync::mpsc::unbounded_channel();
    let started = Instant::now();
    let url = format!("http://{address}/v1/chat/completions");
    let transport_task = tokio::spawn(async move {
        transport
            .post_json_stream(&url, None, "{}", 5_000, sink)
            .await
    });

    let first = tokio::time::timeout(Duration::from_millis(200), events.recv())
        .await
        .expect("the first delta must arrive before the server's pause")
        .expect("a delta event");
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "the first delta waited for the whole stream: {:?}",
        started.elapsed()
    );
    match first {
        ModelEvent::Delta(text) => assert_eq!(text, "first"),
        other @ ModelEvent::Done => panic!("expected a delta, got {other:?}"),
    }

    let reply = transport_task
        .await
        .expect("transport task")
        .expect("the streamed reply completes");
    assert_eq!(reply.streamed_text.as_deref(), Some("first second"));
    let usage = reply.streamed_usage.expect("the final chunk carries usage");
    assert_eq!(usage["prompt_tokens"], 3);
    assert_eq!(usage["completion_tokens"], 2);
    let _ = server.await;
}

#[tokio::test]
async fn an_https_target_is_routed_through_tls_not_plaintext() {
    // A plain listener cannot complete a TLS handshake: the point is that
    // https:// opens TLS instead of speaking HTTP into the socket.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.expect("accept");
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let transport = TcpHttpTransport::new(false);
    let error = transport
        .post_json(
            &format!("https://{address}/v1/chat/completions"),
            None,
            "{}",
            5_000,
        )
        .await
        .expect_err("a TLS handshake against a plaintext peer must fail");
    match error {
        ModelError::Transport(message) => {
            assert!(message.contains("TLS handshake"), "got: {message}");
        }
        other => panic!("expected a TLS handshake failure, got {other:?}"),
    }
    let _ = server.await;
}

#[tokio::test]
async fn an_operator_supplied_root_trusts_a_private_certificate() {
    use rustls::pki_types::pem::PemObject;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("local addr");
    let cert = rustls::pki_types::CertificateDer::from_pem_slice(TEST_LEAF.as_bytes())
        .expect("test leaf parses");
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(TEST_KEY.as_bytes())
        .expect("test key parses");
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .expect("server config");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let server = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let (socket, _) = listener.accept().await.expect("accept");
        let mut tls = acceptor.accept(socket).await.expect("handshake");
        take_request_head(&mut tls).await;
        let head = json_head(COMPLETION.len());
        let _ignored = tls.write_all(head.as_bytes()).await;
        let _ignored = tls.write_all(COMPLETION.as_bytes()).await;
    });

    let transport = TcpHttpTransport::new(false).with_extra_root_pem(TEST_CA);
    let body = transport
        .post_json(
            &format!("https://localhost:{}/v1/chat/completions", address.port()),
            Some("k"),
            "{}",
            5_000,
        )
        .await
        .expect("a certificate from the operator's root pool is trusted");
    assert_eq!(body, COMPLETION);
    server.await.expect("server task");
}
