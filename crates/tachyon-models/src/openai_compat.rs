//! First real provider adapter: OpenAI-compatible HTTP (spec §25, M6).
//!
//! Talks to any OpenAI-style `chat/completions` endpoint. The default target
//! is a local inference server (`http://localhost:11434` covers `Ollama`-style
//! deployments); remote hosts select through the same configuration, and TLS
//! termination for them is M-later — this adapter speaks plain `http://` via
//! [`TcpHttpTransport`] so M6 adds no TLS dependencies. `https://` URLs are
//! rejected as [`ModelError::InvalidRequest`], never silently downgraded.
//!
//! The transport is a trait: unit tests inject a stub, production uses TCP.
//! Incremental SSE streaming is deferred (no `Streaming` feature is
//! advertised); the full completion still flows through the event
//! sink as one `Delta` plus `Done`, so the sink path is identical for fake
//! and real providers.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tachyon_types::ProviderId;

use crate::{
    AgentDecision, ContextBlock, ContextKind, HistorySpeaker, ModelCapabilities, ModelError,
    ModelEvent, ModelFeature, ModelProvider, ModelRequest, ModelResult, ModelUsage,
    ProviderEstimate, UsageProvenance, parse_decision,
};

/// Configuration selecting this adapter (operator-owned, never model-chosen).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenAiCompatConfig {
    /// Base URL, e.g. `http://localhost:11434`. Plain HTTP only in M6.
    pub base_url: String,
    /// Model name sent in every request.
    pub model: String,
    /// Environment variable holding the API key. `None` for local servers
    /// without auth; a missing variable means no header, and a 401 still
    /// surfaces as [`ModelError::Unauthorized`].
    pub api_key_env: Option<String>,
    /// Per-request deadline, milliseconds.
    pub request_timeout_ms: u64,
    /// Advertised context window, tokens.
    pub context_window_tokens: u32,
}

impl Default for OpenAiCompatConfig {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:11434".to_owned(),
            model: "default".to_owned(),
            api_key_env: None,
            request_timeout_ms: 120_000,
            context_window_tokens: 32_768,
        }
    }
}

/// Pluggable HTTP layer. Production uses [`TcpHttpTransport`]; tests inject
/// canned responses without sockets.
#[async_trait]
pub trait HttpTransport: Send + Sync {
    /// POSTs `body` as `json` to `url` and returns the response body.
    /// Transport failures map to [`ModelError`]; HTTP error statuses map to
    /// the matching taxonomy variant.
    async fn post_json(
        &self,
        url: &str,
        api_key: Option<&str>,
        body: &str,
        timeout_ms: u64,
    ) -> Result<String, ModelError>;
}

/// One chat message in the wire format.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct WireMessage {
    /// `system`, `user`, or `assistant`.
    role: String,
    /// Message text.
    content: String,
}

/// Minimal `http://` POST transport over `TcpStream`.
///
/// Sends `Connection: close` and reads to EOF — sufficient for local
/// OpenAI-compatible servers. Anything else (TLS, chunked upgrades, proxies)
/// is out of scope for M6 and fails loudly instead of half-working.
pub struct TcpHttpTransport;

#[async_trait]
impl HttpTransport for TcpHttpTransport {
    async fn post_json(
        &self,
        url: &str,
        api_key: Option<&str>,
        body: &str,
        timeout_ms: u64,
    ) -> Result<String, ModelError> {
        let (host, port, path) = parse_http_url(url)?;
        let request = build_http_request(&host, &path, api_key, body);
        let address = format!("{host}:{port}");
        let raw = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            round_trip(&address, &request),
        )
        .await
        .map_err(|_| ModelError::Timeout { timeout_ms })?
        .map_err(ModelError::Transport)?;
        let (status, headers, response_body) = split_http_response(&raw)?;
        status_to_result(status, &headers, &response_body)
    }
}

/// Runs one blocking-style TCP exchange. Kept small: connect, write, read.
async fn round_trip(address: &str, request: &str) -> Result<String, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .map_err(|error| format!("connect {address}: {error}"))?;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| format!("write: {error}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|error| format!("read: {error}"))?;
    String::from_utf8(raw).map_err(|error| format!("response is not UTF-8: {error}"))
}

/// Accepts only plain `http://` URLs. Anything else is configuration error.
/// Control characters in host or path are rejected: config values reach the
/// wire verbatim, so CRLF injection fails closed here.
fn parse_http_url(url: &str) -> Result<(String, u16, String), ModelError> {
    let rest = url.strip_prefix("http://").ok_or_else(|| {
        ModelError::InvalidRequest(format!("M6 adapter supports http:// URLs only: {url}"))
    })?;
    if rest.chars().any(|char| char == '\r' || char == '\n') {
        return Err(ModelError::InvalidRequest(
            "base URL contains control characters".to_owned(),
        ));
    }
    let (authority, path) = match rest.find('/') {
        Some(index) => (rest[..index].to_owned(), rest[index..].to_owned()),
        None => (rest.to_owned(), "/".to_owned()),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|_| ModelError::InvalidRequest(format!("bad port in base URL: {url}")))?;
            (host.to_owned(), port)
        }
        None => (authority, 80),
    };
    if host.is_empty() {
        return Err(ModelError::InvalidRequest(format!(
            "empty host in base URL: {url}"
        )));
    }
    Ok((host, port, path))
}

/// Renders a minimal HTTP/1.1 POST.
fn build_http_request(host: &str, path: &str, api_key: Option<&str>, body: &str) -> String {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(key) = api_key {
        let _ignored = write!(request, "Authorization: Bearer {key}\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    request
}

/// Parsed HTTP response headers: lowercased names with trimmed values.
type Headers = Vec<(String, String)>;

/// Splits a raw HTTP/1.x response into status code, headers, and body.
/// Header names are lowercased; continuation lines are out of scope for M6
/// and fail as malformed rather than half-parsing.
fn split_http_response(raw: &str) -> Result<(u16, Headers, String), ModelError> {
    let (head, body) = raw.split_once("\r\n\r\n").ok_or_else(|| {
        ModelError::Transport("malformed HTTP response: no header/body split".to_owned())
    })?;
    let mut lines = head.lines();
    let status_line = lines.next().ok_or_else(|| {
        ModelError::Transport("malformed HTTP response: no status line".to_owned())
    })?;
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            ModelError::Transport(format!("malformed HTTP status line: {status_line}"))
        })?;
    let mut headers = Vec::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| ModelError::Transport(format!("malformed HTTP header line: {line}")))?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
    Ok((code, headers, body.to_owned()))
}

/// Reads `Retry-After` seconds from response headers, if present in bare
/// seconds form. HTTP-date form is out of scope and ignored (default applies).
fn retry_after_ms(headers: &Headers) -> Option<u64> {
    headers
        .iter()
        .find(|(name, _)| name == "retry-after")
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .map(|seconds| seconds.saturating_mul(1_000))
}

/// Maps HTTP status to the model error taxonomy (spec §40).
///
/// Context overflow is matched narrowly: status 413, or a 400 whose body
/// carries a known context-exhaustion marker. Anything else stays
/// `InvalidRequest` — providers must not launder unknown 400s into retryable
/// errors.
fn status_to_result(status: u16, headers: &Headers, body: &str) -> Result<String, ModelError> {
    match status {
        200..=299 => Ok(body.to_owned()),
        401 | 403 => Err(ModelError::Unauthorized),
        429 => Err(ModelError::RateLimited {
            retry_after_ms: retry_after_ms(headers).unwrap_or(1_000),
        }),
        400 if is_context_overflow(body) => Err(ModelError::ContextOverflow {
            detail: snippet(body),
        }),
        413 => Err(ModelError::ContextOverflow {
            detail: snippet(body),
        }),
        400..=499 => Err(ModelError::InvalidRequest(format!("HTTP {status}: {body}"))),
        _ => Err(ModelError::ProviderUnavailable(format!(
            "HTTP {status}: {body}"
        ))),
    }
}

/// Whether a 400 body reports context exhaustion. Narrow markers only.
fn is_context_overflow(body: &str) -> bool {
    body.contains("context_length_exceeded") || body.contains("maximum context length")
}

/// First 200 characters of `text`, for error diagnostics.
fn snippet(text: &str) -> String {
    text.chars().take(200).collect()
}

/// Builds the `chat/completions` body for `request`. Pure function, unit
/// tested: context kinds map to roles, structured output requests the `json`
/// object format only when the provider offers it.
fn build_request_body(
    config: &OpenAiCompatConfig,
    capabilities: &ModelCapabilities,
    request: &ModelRequest,
) -> String {
    let messages: Vec<WireMessage> = request.context.iter().map(wire_message).collect();
    let mut body = serde_json::json!({
        "model": config.model,
        "messages": messages,
        "max_tokens": request.max_output_tokens,
        "stream": false,
    });
    if request.require_structured_output && capabilities.supports(ModelFeature::StructuredOutput) {
        body["response_format"] = serde_json::json!({"type": "json_object"});
    }
    body.to_string()
}

/// Maps one context block to a wire message. History speakers survive;
/// everything else is user text with provenance already inlined.
fn wire_message(block: &ContextBlock) -> WireMessage {
    let role = match &block.kind {
        ContextKind::System => "system",
        ContextKind::History(HistorySpeaker::Assistant) => "assistant",
        ContextKind::Objective
        | ContextKind::Constraint
        | ContextKind::Evidence
        | ContextKind::History(HistorySpeaker::User) => "user",
    };
    WireMessage {
        role: role.to_owned(),
        content: block.content.clone(),
    }
}

/// Extracts the assistant text from a `chat/completions` body. Pure and
/// tested: missing choices or content is `MalformedOutput`, never silently
/// treated as empty success. Usage metadata is separate from legacy numeric
/// counters: unavailable counts must not be mistaken for reported zeroes.
fn parse_completions(body: &str) -> Result<(String, u32, u32, ModelUsage), ModelError> {
    let value: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| ModelError::MalformedOutput(format!("response is not JSON: {error}")))?;
    let content = value
        .pointer("/choices/0/message/content")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            ModelError::MalformedOutput("response has no choices[0].message.content".to_owned())
        })?;
    let prompt_tokens = usage_tokens(&value, "prompt_tokens");
    let completion_tokens = usage_tokens(&value, "completion_tokens");
    let usage = ModelUsage {
        input_tokens: prompt_tokens.and_then(|count| u32::try_from(count).ok()),
        output_tokens: completion_tokens.and_then(|count| u32::try_from(count).ok()),
        provenance: if value.get("usage").is_some_and(serde_json::Value::is_object) {
            UsageProvenance::ProviderReported
        } else {
            UsageProvenance::Unknown
        },
    };
    Ok((
        content,
        // Keep legacy zero-filling/saturation without claiming those values
        // were valid reported counts in the authoritative usage metadata.
        prompt_tokens.map_or(0, |count| u32::try_from(count).unwrap_or(u32::MAX)),
        completion_tokens.map_or(0, |count| u32::try_from(count).unwrap_or(u32::MAX)),
        usage,
    ))
}

/// Reads one raw usage counter. Absent or malformed usage is unavailable.
fn usage_tokens(value: &serde_json::Value, key: &str) -> Option<u64> {
    value
        .pointer(&format!("/usage/{key}"))
        .and_then(serde_json::Value::as_u64)
}

/// OpenAI-compatible provider over any [`HttpTransport`].
pub struct OpenAiCompatProvider<T = TcpHttpTransport> {
    id: ProviderId,
    config: OpenAiCompatConfig,
    capabilities: ModelCapabilities,
    transport: T,
}

impl<T> OpenAiCompatProvider<T> {
    /// Creates the adapter with an explicit transport (stub in tests).
    #[must_use]
    pub fn new(id: ProviderId, config: OpenAiCompatConfig, transport: T) -> Self {
        let capabilities = ModelCapabilities {
            features: BTreeSet::from([ModelFeature::StructuredOutput]),
            context_window_tokens: config.context_window_tokens,
            latency_class: crate::LatencyClass::Medium,
            cost_class: crate::CostClass::Medium,
        };
        Self {
            id,
            config,
            capabilities,
            transport,
        }
    }

    /// Renders the wire body for `request` (exposed for tests).
    #[must_use]
    pub fn request_body(&self, request: &ModelRequest) -> String {
        build_request_body(&self.config, &self.capabilities, request)
    }
}

impl OpenAiCompatProvider<TcpHttpTransport> {
    /// Creates the production adapter speaking plain HTTP.
    #[must_use]
    pub fn local(id: ProviderId, config: OpenAiCompatConfig) -> Self {
        Self::new(id, config, TcpHttpTransport)
    }
}

#[async_trait]
impl<T: HttpTransport> ModelProvider for OpenAiCompatProvider<T> {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn capabilities(&self) -> ModelCapabilities {
        self.capabilities.clone()
    }

    fn estimate(&self, request: &ModelRequest) -> ProviderEstimate {
        // Uncalibrated seed: hosted-class base plus per-token slope. No
        // model-call observation path feeds router EWMA yet (M7/M13 close
        // that loop); providers must not invent precision here.
        let input_tokens = request.estimated_input_tokens();
        ProviderEstimate {
            latency_ms: 800.0 + 2.0 * f64::from(input_tokens),
            input_tokens,
        }
    }

    async fn invoke(
        &self,
        request: ModelRequest,
        sink: crate::ModelEventSink,
    ) -> Result<ModelResult, ModelError> {
        let started = Instant::now();
        let api_key = self
            .config
            .api_key_env
            .as_deref()
            .and_then(|name| std::env::var(name).ok());
        let url = format!("{}/v1/chat/completions", self.config.base_url);
        // Validate scheme and host before touching the transport: stub
        // transports in tests must see the same rejection a real socket would.
        parse_http_url(&url).map(|_| ())?;
        let body = self.request_body(&request);
        let raw = self
            .transport
            .post_json(
                &url,
                api_key.as_deref(),
                &body,
                self.config.request_timeout_ms,
            )
            .await?;
        let (content, prompt_tokens, completion_tokens, usage) = parse_completions(&raw)?;
        let decision: AgentDecision = parse_decision(&content)?;
        // Same sink path as the fake: one Delta, then Done. Ephemeral
        // progress may drop; the committed result still returns.
        let _ignored = sink.send(ModelEvent::Delta(content));
        let _ignored = sink.send(ModelEvent::Done);
        Ok(ModelResult {
            decision,
            input_tokens: prompt_tokens,
            output_tokens: completion_tokens,
            usage,
            latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
            provider: self.id.clone(),
            model: self.config.model.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ContextBlock, TrustLevel};

    struct StubTransport {
        response: Result<String, ModelError>,
    }

    #[async_trait]
    impl HttpTransport for StubTransport {
        async fn post_json(
            &self,
            _url: &str,
            _api_key: Option<&str>,
            _body: &str,
            _timeout_ms: u64,
        ) -> Result<String, ModelError> {
            match &self.response {
                Ok(body) => Ok(body.clone()),
                Err(error) => Err(error.clone()),
            }
        }
    }

    fn block(kind: ContextKind, content: &str) -> ContextBlock {
        ContextBlock {
            kind,
            provenance: "test".to_owned(),
            trust: TrustLevel::WorkspaceData,
            content: content.to_owned(),
            priority: 100,
            created_at: tachyon_types::Timestamp::from_micros(0),
        }
    }

    fn request() -> ModelRequest {
        ModelRequest {
            role: crate::Role::Primary,
            model: "stub".to_owned(),
            context: vec![
                block(ContextKind::System, "sys"),
                block(ContextKind::Objective, "why?"),
            ],
            max_output_tokens: 256,
            require_structured_output: true,
        }
    }

    #[test]
    fn body_maps_roles_and_structured_format() {
        let provider = OpenAiCompatProvider::new(
            ProviderId("stub".to_owned()),
            OpenAiCompatConfig::default(),
            StubTransport {
                response: Ok(String::new()),
            },
        );
        let body: serde_json::Value =
            serde_json::from_str(&provider.request_body(&request())).expect("JSON body");
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][1]["role"], "user");
        assert_eq!(body["response_format"]["type"], "json_object");
        assert_eq!(body["stream"], false);
    }

    #[test]
    fn plain_http_only_never_downgrades() {
        assert!(parse_http_url("https://example.com/v1").is_err());
        let (host, port, path) = parse_http_url("http://localhost:11434/v1").expect("http");
        assert_eq!(
            (host.as_str(), port, path.as_str()),
            ("localhost", 11434, "/v1")
        );
    }

    #[test]
    fn statuses_map_to_taxonomy() {
        let no_headers = Headers::new();
        assert!(status_to_result(200, &no_headers, "ok").is_ok());
        assert!(matches!(
            status_to_result(401, &no_headers, ""),
            Err(ModelError::Unauthorized)
        ));
        assert!(matches!(
            status_to_result(429, &no_headers, ""),
            Err(ModelError::RateLimited { .. })
        ));
        assert!(
            status_to_result(429, &no_headers, "")
                .expect_err("limited")
                .is_retryable()
        );
        assert!(matches!(
            status_to_result(500, &no_headers, ""),
            Err(ModelError::ProviderUnavailable(_))
        ));
        let retry: Headers = vec![("retry-after".to_owned(), "3".to_owned())];
        assert!(matches!(
            status_to_result(429, &retry, ""),
            Err(ModelError::RateLimited {
                retry_after_ms: 3_000
            })
        ));
        let raw = "HTTP/1.1 429 Quiet\r\nRetry-After: 7\r\nX-Other: z\r\n\r\nlimited";
        let (status, headers, body) = split_http_response(raw).expect("headers parse");
        assert_eq!(status, 429);
        assert_eq!(body, "limited");
        assert!(matches!(
            status_to_result(status, &headers, &body),
            Err(ModelError::RateLimited {
                retry_after_ms: 7_000
            })
        ));
        assert!(matches!(
            status_to_result(429, &retry, ""),
            Err(ModelError::RateLimited {
                retry_after_ms: 3_000
            })
        ));
        let overflow = serde_json::json!({
            "error": {"code": "context_length_exceeded", "message": "too long"}
        })
        .to_string();
        let error = status_to_result(400, &no_headers, &overflow).expect_err("overflow");
        assert!(matches!(error, ModelError::ContextOverflow { .. }));
        assert!(error.is_retryable());
        assert!(matches!(
            status_to_result(400, &no_headers, "plain bad request"),
            Err(ModelError::InvalidRequest(_))
        ));
    }

    #[test]
    fn wire_request_carries_api_key_and_length() {
        let wire = build_http_request("local", "/v1", Some("probe-key"), "{}");
        assert!(wire.contains("Authorization: Bearer probe-key"));
        assert!(wire.contains("Content-Length: 2\r\n"));
        assert!(wire.ends_with("\r\n\r\n{}"));
    }

    #[test]
    fn control_characters_in_base_url_fail_closed() {
        assert!(parse_http_url("http://host/x\r\nInjected: yes").is_err());
    }

    #[tokio::test]
    async fn stub_response_parses_to_decision() {
        let completion = serde_json::json!({
            "choices": [{"message": {"content": "{\"decision\":\"respond\",\"message\":\"differs\"}"}}]
        })
        .to_string();
        let provider = OpenAiCompatProvider::new(
            ProviderId("stub".to_owned()),
            OpenAiCompatConfig::default(),
            StubTransport {
                response: Ok(completion),
            },
        );
        let (sink, mut events) = tokio::sync::mpsc::unbounded_channel();
        let result = provider.invoke(request(), sink).await.expect("stub");
        assert_eq!(
            result.decision,
            AgentDecision::Respond {
                message: "differs".to_owned()
            }
        );
        assert!(matches!(events.recv().await, Some(ModelEvent::Delta(_))));
        assert!(matches!(events.recv().await, Some(ModelEvent::Done)));
    }

    #[tokio::test]
    async fn usage_counts_ride_along_when_reported() {
        let completion = serde_json::json!({
            "choices": [{"message": {"content": "{\"decision\":\"respond\",\"message\":\"m\"}"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        })
        .to_string();
        let provider = OpenAiCompatProvider::new(
            ProviderId("stub".to_owned()),
            OpenAiCompatConfig::default(),
            StubTransport {
                response: Ok(completion),
            },
        );
        let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
        let result = provider.invoke(request(), sink).await.expect("stub");
        assert_eq!(result.input_tokens, 10);
        assert_eq!(result.output_tokens, 5);
    }

    #[tokio::test]
    async fn invoke_rejects_https_before_transport() {
        let config = OpenAiCompatConfig {
            base_url: "https://example.com".to_owned(),
            ..OpenAiCompatConfig::default()
        };
        let provider = OpenAiCompatProvider::new(
            ProviderId("stub".to_owned()),
            config,
            StubTransport {
                response: Ok(String::new()),
            },
        );
        let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
        let error = provider.invoke(request(), sink).await.expect_err("https");
        assert!(matches!(error, ModelError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn transport_failure_routes_around() {
        let provider = OpenAiCompatProvider::new(
            ProviderId("stub".to_owned()),
            OpenAiCompatConfig::default(),
            StubTransport {
                response: Err(ModelError::ProviderUnavailable("down".to_owned())),
            },
        );
        let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
        let error = provider.invoke(request(), sink).await.expect_err("down");
        assert!(error.is_retryable());
    }
}
