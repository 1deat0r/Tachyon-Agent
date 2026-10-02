//! Tachyon ACP adapter (`tachyon-acp`): an Agent Client Protocol v1
//! stdio agent that is a pure client of the already-running local
//! gateway (ADR-0005 — it never starts or restarts the gateway).
//!
//! The adapter speaks UTF-8 newline-delimited JSON-RPC 2.0 over its own
//! stdio: stdout carries only valid ACP frames, logs go to stderr. This
//! slice (acp-adapter-lifecycle tickets 01–03) ships the codec, the
//! stdio server loop, the gateway liveness probe, the `initialize`
//! handshake, `session/new`, the full `session/prompt` turn pipeline,
//! and `session/cancel` (both the id-bearing and ACP notification
//! forms, drain-acknowledged); `session/load` answers standard
//! method-not-found until its own slice.

pub mod client;
pub mod codec;
pub mod config;
pub mod server;
pub mod turn;

/// Unit-test support: stub probes plus a duplex driver that runs
/// [`server::serve`] the way a client would drive it.
#[cfg(test)]
pub(crate) mod test_support {
    use std::future::Future;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

    use crate::client::{Connector, GatewayConn, GatewayProbe, GatewayUnavailable};
    use crate::server::serve;

    /// A probe that always finds the gateway up.
    #[derive(Clone)]
    pub(crate) struct GatewayUp;

    impl GatewayProbe for GatewayUp {
        fn probe(&self) -> impl Future<Output = Result<(), GatewayUnavailable>> + Send {
            std::future::ready(Ok(()))
        }
    }

    /// A probe that always finds the gateway down.
    #[derive(Clone)]
    pub(crate) struct GatewayDown;

    impl GatewayProbe for GatewayDown {
        fn probe(&self) -> impl Future<Output = Result<(), GatewayUnavailable>> + Send {
            std::future::ready(Err(GatewayUnavailable::new(
                "no usable endpoint at /nonexistent/gateway.json: No such file or directory",
            )))
        }
    }

    /// A connector that never dials: unit tests reach the validation,
    /// guard, and method-not-found paths without a gateway, and any
    /// success path that would need one fails with one typed error.
    #[derive(Clone)]
    pub(crate) struct NoConnector;

    impl Connector for NoConnector {
        fn connect(&self) -> impl Future<Output = Result<GatewayConn, GatewayUnavailable>> + Send {
            std::future::ready(Err(GatewayUnavailable::new(
                "no gateway connector configured in this unit test",
            )))
        }
    }

    /// Drives `serve` over one tokio duplex: writes every `requests`
    /// line, then collects replies until the loop has been idle for
    /// 250 ms (in-process duplex delivery is immediate, so this is a
    /// generous bound). Panics after 10 s so a hung loop fails loudly.
    pub(crate) async fn drive<P: GatewayProbe>(requests: &[&str], probe: P) -> Vec<String> {
        let (server, client) = tokio::io::duplex(64 * 1024);
        let (reader, writer) = tokio::io::split(server);
        let service = serve(reader, writer, probe, NoConnector);
        let client = async move {
            let (read_half, mut write_half) = tokio::io::split(client);
            for request in requests {
                write_half
                    .write_all(request.as_bytes())
                    .await
                    .expect("write request");
                write_half.write_all(b"\n").await.expect("write newline");
            }
            write_half.flush().await.expect("flush requests");
            let mut reader = BufReader::new(read_half);
            let mut replies = Vec::new();
            loop {
                let mut line = String::new();
                match tokio::time::timeout(Duration::from_millis(250), reader.read_line(&mut line))
                    .await
                {
                    Ok(Ok(0) | Err(_)) => break,
                    Ok(Ok(_)) => {
                        if !line.trim().is_empty() {
                            replies.push(line.trim_end().to_owned());
                        }
                    }
                    // Idle: the loop has caught up. Before any reply we
                    // keep waiting (bounded by the 10 s outer timeout).
                    Err(_) if !replies.is_empty() => break,
                    Err(_) => {}
                }
            }
            replies
        };
        let (replies, service_result) = tokio::time::timeout(Duration::from_secs(10), async {
            let (replies, service_result) = tokio::join!(client, service);
            (replies, service_result)
        })
        .await
        .expect("drive timed out: the server loop hung");
        service_result.expect("serve I/O error");
        replies
    }
}
