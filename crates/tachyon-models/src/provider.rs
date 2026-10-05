//! Provider-neutral model interface (spec §25).
//!
//! Core code depends on capabilities and roles, never provider names. The
//! [`ModelProvider`] trait here is the `async_trait` spelling of the spec's
//! `BoxFuture` interface — same object-safe shape, less boilerplate.
//! Provider-native types never cross into core state or IR (frozen
//! invariant); adapters translate at this boundary.

use std::pin::Pin;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tachyon_types::ProviderId;

pub use crate::capabilities::{CapabilityRequirements, ModelCapabilities};
pub use crate::context::{ContextBlock, estimate_tokens};
pub use crate::decision::AgentDecision;

/// Object-safe boxed future for transport customization.
pub type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// One model invocation: role-selected, context-bounded, token-capped.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    /// Which role this call serves (`fast`, `primary`, `specialist`, `vision`).
    pub role: crate::Role,
    /// Model name within the serving provider.
    pub model: String,
    /// Assembled trusted context (spec §27).
    pub context: Vec<ContextBlock>,
    /// Hard cap on generated tokens.
    pub max_output_tokens: u32,
    /// Request native structured output; providers without it are not
    /// selected for such calls (capability negotiation).
    pub require_structured_output: bool,
}

impl ModelRequest {
    /// Estimated input tokens across all context blocks.
    #[must_use]
    pub fn estimated_input_tokens(&self) -> u32 {
        self.context
            .iter()
            .map(|block| estimate_tokens(&block.content))
            .sum()
    }
}

/// A latency/cost guess for one request. Uncalibrated seeds: the router folds
/// real observations into EWMA estimates; providers must not invent
/// precision here.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderEstimate {
    /// Expected wall latency, milliseconds.
    pub latency_ms: f64,
    /// Estimated input tokens.
    pub input_tokens: u32,
}

/// One streaming event. Ephemeral UI progress (spec §16): may drop under
/// backpressure, durable state replays from the journal instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ModelEvent {
    /// An incremental output fragment.
    Delta(String),
    /// The provider closed the stream. The final [`ModelResult`] still
    /// arrives through the call return, not the sink.
    Done,
}

/// Where streaming events go. Unbounded so model progress never blocks the
/// execution critical path; slow consumers drop, correctness never waits.
pub type ModelEventSink = tokio::sync::mpsc::UnboundedSender<ModelEvent>;

/// Source of token usage, independent of provider identity or counter values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageProvenance {
    /// No usage metadata was supplied, including legacy serialized results.
    #[default]
    Unknown,
    /// The provider supplied a usage object; individual counts may be unavailable.
    ProviderReported,
    /// Counts came from a deterministic test/replay script, not measured usage.
    Scripted,
}

/// Authoritative token-usage availability for one result, never an estimate.
///
/// `None` means absent, malformed, or outside the `u32` range; `Some(0)` is an
/// explicitly supplied zero. Consult provenance before treating a count as
/// reported usage rather than scripted data. Estimates remain separate in
/// [`ProviderEstimate`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsage {
    /// Input tokens, if supplied as a valid count.
    pub input_tokens: Option<u32>,
    /// Output tokens, if supplied as a valid count.
    pub output_tokens: Option<u32>,
    /// Where these counts came from, even when neither count is available.
    pub provenance: UsageProvenance,
}

/// Outcome and usage of one call, including a rejected model response.
#[derive(Clone, Debug)]
pub struct ModelInvocation {
    pub result: Result<ModelResult, ModelError>,
    pub usage: ModelUsage,
}

/// Safe accounting record. Contains no model text or provider error body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelCallRecord {
    pub attempt: u32,
    pub latency_ms: f64,
    pub usage: ModelUsage,
    pub error: Option<String>,
    /// Safe malformed-output class; never contains model text.
    pub output_failure: Option<String>,
}

impl ModelUsage {
    /// Totals are available only when every call supplied that counter.
    #[must_use]
    pub fn total(calls: &[ModelCallRecord]) -> Self {
        if calls.is_empty() {
            return Self::default();
        }
        let provenance = calls
            .first()
            .map_or(UsageProvenance::Unknown, |c| c.usage.provenance);
        Self {
            input_tokens: calls
                .iter()
                .try_fold(0u32, |sum, c| sum.checked_add(c.usage.input_tokens?)),
            output_tokens: calls
                .iter()
                .try_fold(0u32, |sum, c| sum.checked_add(c.usage.output_tokens?)),
            provenance: if calls.iter().any(|c| c.usage.provenance != provenance) {
                UsageProvenance::Unknown
            } else {
                provenance
            },
        }
    }
}

/// The committed outcome of one model call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelResult {
    /// Parsed structured decision.
    pub decision: AgentDecision,
    /// Legacy input counter; use `usage` for availability and provenance.
    pub input_tokens: u32,
    /// Legacy output counter; use `usage` for availability and provenance.
    pub output_tokens: u32,
    /// Authoritative usage metadata. Legacy results deserialize as unknown,
    /// regardless of their numeric counters or provider name.
    #[serde(default)]
    pub usage: ModelUsage,
    /// Measured wall latency, milliseconds.
    pub latency_ms: f64,
    /// Which provider served the call.
    pub provider: ProviderId,
    /// Which model served the call.
    pub model: String,
}

/// Model failure taxonomy (spec §40). Every failure exposes retryability;
/// the scheduler owns retry policy — providers never retry internally.
#[derive(Clone, Debug, PartialEq, thiserror::Error, Serialize, Deserialize)]
pub enum ModelError {
    /// Bad request or configuration. Fix the caller, not the network.
    #[error("invalid model request: {0}")]
    InvalidRequest(String),
    /// Rejected credentials.
    #[error("model provider unauthorized")]
    Unauthorized,
    /// Throttled. Retry after the given milliseconds.
    #[error("model provider rate limited, retry after {retry_after_ms}ms")]
    RateLimited {
        /// Server-advised or default backoff.
        retry_after_ms: u64,
    },
    /// Provider unreachable or erroring. Fail over, don't fail the harness.
    #[error("model provider unavailable: {0}")]
    ProviderUnavailable(String),
    /// The call exceeded its deadline; safe to rerun while no result was
    /// committed (spec §41).
    #[error("model call timed out after {timeout_ms}ms")]
    Timeout {
        /// Deadline that fired.
        timeout_ms: u64,
    },
    /// Output framing repair failed. A provider result failure, never
    /// permission to execute text heuristically.
    #[error("malformed model output: {0}")]
    MalformedOutput(String),
    /// The request exceeded the provider's context window. Retryable only
    /// with a smaller context — the scheduler must never retry this verbatim.
    #[error("model context overflow: {detail}")]
    ContextOverflow {
        /// Provider-reported detail (truncated).
        detail: String,
    },
    /// HTTP/IPC/process plumbing failure below the provider API.
    #[error("model transport failure: {0}")]
    Transport(String),
    /// Caller cancelled before a result committed.
    #[error("model call cancelled")]
    Cancelled,
    /// Broken harness-side invariant (empty fake script, missing role map).
    #[error("internal model error: {0}")]
    Internal(String),
}

impl ModelError {
    /// Stable classification for receipts; never includes an error body.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "invalid_request",
            Self::Unauthorized => "unauthorized",
            Self::RateLimited { .. } => "rate_limited",
            Self::ProviderUnavailable(_) => "provider_unavailable",
            Self::Timeout { .. } => "timeout",
            Self::MalformedOutput(_) => "malformed_output",
            Self::ContextOverflow { .. } => "context_overflow",
            Self::Transport(_) => "transport",
            Self::Cancelled => "cancelled",
            Self::Internal(_) => "internal",
        }
    }

    /// Bounded diagnostic class for malformed output, with no response text.
    #[must_use]
    pub fn output_failure(&self) -> Option<&'static str> {
        let Self::MalformedOutput(detail) = self else {
            return None;
        };
        Some(if detail == "empty model content" {
            "empty_content"
        } else if detail.starts_with("invalid decision JSON: Data") {
            "invalid_decision"
        } else if detail.starts_with("invalid decision JSON:") {
            "invalid_json"
        } else if detail.starts_with("response has no choices") {
            "missing_content"
        } else if detail.starts_with("response is not JSON:") {
            "invalid_response"
        } else if detail.starts_with("stream chunk is not JSON:") {
            "invalid_stream"
        } else {
            "unclassified"
        })
    }

    /// Whether the scheduler may retry the call. Mirrors the crash-recovery
    /// rule: rerun only while no result committed (spec §41).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::RateLimited { .. }
            | Self::ProviderUnavailable(_)
            | Self::Timeout { .. }
            | Self::ContextOverflow { .. }
            | Self::Transport(_) => true,
            Self::InvalidRequest(_)
            | Self::Unauthorized
            | Self::MalformedOutput(_)
            | Self::Cancelled
            | Self::Internal(_) => false,
        }
    }
}

/// Provider-neutral model interface (spec §25).
#[async_trait]
pub trait ModelProvider: Send + Sync {
    /// Stable provider identity for telemetry and role maps.
    fn id(&self) -> ProviderId;

    /// What this backend can do; drives capability negotiation.
    fn capabilities(&self) -> ModelCapabilities;

    /// Latency/cost guess for one request. Uncalibrated seed: no model-call
    /// observation path feeds router EWMA yet (M7/M13 close that loop), so
    /// providers must not invent precision here.
    fn estimate(&self, request: &ModelRequest) -> ProviderEstimate;

    /// Runs exactly one call and preserves any reported usage on failure.
    /// Existing providers default to unknown usage when they return an error.
    async fn invoke_observed(
        &self,
        request: ModelRequest,
        sink: ModelEventSink,
    ) -> ModelInvocation {
        let result = self.invoke(request, sink).await;
        let usage = result
            .as_ref()
            .map_or_else(|_| ModelUsage::default(), |r| r.usage);
        ModelInvocation { result, usage }
    }

    /// Runs one call. The caller decides fallback; providers never retry.
    async fn invoke(
        &self,
        request: ModelRequest,
        sink: ModelEventSink,
    ) -> Result<ModelResult, ModelError>;
}
