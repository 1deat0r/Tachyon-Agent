//! Provider-neutral model layer (spec §25–§27, M6).
//!
//! Core selects capabilities and roles, never provider names. Evidence
//! becomes bounded [`ContextBlock`]s with [`TrustLevel`]s; providers return
//! [`AgentDecision`]s that later milestones compile to validated IR.
//!
//! # New-capability checklist (model reasoning call)
//!
//! - Why deterministic code cannot solve it: semantic explanation of behavior
//!   differences (Vertical Slice B) is judgment over evidence, not lookup.
//! - Input schema: [`ModelRequest`] (role, model, trusted context, token cap).
//! - Output schema: [`AgentDecision`] (`respond`, `request_evidence`,
//!   `propose_execution`, `need_user_input`, `complete`).
//! - Access set: none — providers receive context text only; they hold no
//!   file, process, or network capabilities of their own.
//! - Effect class: metered remote inference with no Tachyon-persisted effect.
//! - Idempotency: safe to rerun while no result committed (spec §41); a
//!   returned result is never re-requested for the same revision.
//! - Resource claim: one network slot plus the token budget on the request.
//! - Cancellation: the caller drops the call; no partial result commits.
//! - Retry policy: scheduler-owned, guided by
//!   [`ModelError::is_retryable`]; providers never retry internally.
//! - Verification: Slice B — one call over repo evidence explains the two
//!   implementations, citing both provenances (see `tests/model_gate.rs`).
//! - Crash-recovery: uncommitted calls rerun; committed [`ModelResult`]s are
//!   journaled by the supervisor, never re-invoked.
//! - Expected latency class: hosted seconds; local-adapter sub-second. No
//!   measured claims yet — M13 calibrates.

#![warn(unsafe_code)]

pub mod capabilities;
pub mod context;
pub mod decision;
pub mod fake;
pub mod openai_compat;
pub mod provider;
pub mod registry;

pub use capabilities::{
    CapabilityRequirements, CostClass, LatencyClass, ModelCapabilities, ModelFeature,
};
pub use context::{
    AssembleInput, CHARS_PER_TOKEN, ConstraintOrigin, ConstraintStrength, ContextBlock,
    ContextConstraint, ContextKind, HistorySpeaker, HistoryTurn, MAX_EXCERPT_CHARS, TrustLevel,
    assemble, collapse_repeated_lines, estimate_tokens,
};
pub use decision::{AgentDecision, CapabilityRequest, ProposedOperation, parse_decision};
pub use fake::{FakeModelProvider, FakeResponse};
pub use openai_compat::{
    HttpTransport, OpenAiCompatConfig, OpenAiCompatProvider, TcpHttpTransport,
};
pub use provider::{
    BoxFuture, ModelError, ModelEvent, ModelEventSink, ModelProvider, ModelRequest, ModelResult,
    ModelUsage, ProviderEstimate, UsageProvenance,
};
pub use registry::{ModelRegistry, RegisteredProvider, Role, RoleMap, SelectedProvider};
