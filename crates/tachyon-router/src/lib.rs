//! Predictive router: cheapest sufficient path first (spec §22–§23, M5).
//!
//! Stages: explicit subcommand handling (callers), deterministic intent
//! rules ([`classifier`]), workspace capability matching (capability ids on
//! [`EvidenceOp`]), historical EWMA estimates, and bounded judgment only if
//! classification stays ambiguous — which, until the M7 provider exists,
//! resolves to evidence-first. Large-model inference is never spent just to
//! decide whether to invoke a large model.

pub mod classifier;
pub mod plan;

pub use classifier::{Classification, classify, requested_symbol};
pub use plan::{EvidenceOp, RoutePlan};
use tachyon_telemetry::{Ewma, Recorder, RouteRecord};

/// Evidence grace window: launch cheap evidence first, seal model context
/// after this long. Initial candidate 75 ms (spec §23) — distributions are
/// recorded so p50/p95 can retune it later, never a permanent magic number.
pub const GRACE_MS: u64 = 75;

/// EWMA alpha for latency estimates.
pub const ESTIMATE_ALPHA: f64 = 0.3;

/// Default latency estimate (ms) for never-observed operations.
pub const DEFAULT_ESTIMATE_MS: f64 = 50.0;

/// Route class (spec §22).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RouteClass {
    /// Pure native/tool answer; no model call planned.
    #[default]
    DirectNative,
    /// Cheap evidence first; model only if evidence proves insufficient.
    EvidenceFirst,
    /// Ambiguous: bounded judgment decides (M7 provider; evidence until then).
    JudgmentFirst,
    /// Reasoning needed, but cheap evidence still launches first.
    ReasoningFirst,
    /// Evidence and model preparation start concurrently.
    Hybrid,
}

impl RouteClass {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::DirectNative => "direct_native",
            Self::EvidenceFirst => "evidence_first",
            Self::JudgmentFirst => "judgment_first",
            Self::ReasoningFirst => "reasoning_first",
            Self::Hybrid => "hybrid",
        }
    }
}

/// What to do when the plan meets reality.
#[derive(Clone, Debug, PartialEq)]
pub struct EscalationPolicy {
    /// Escalate when evidence comes back empty.
    pub on_empty_evidence: RouteClass,
    /// Maximum model calls this plan may cause (0 = none planned).
    pub max_model_calls: u32,
}

impl Default for EscalationPolicy {
    fn default() -> Self {
        Self {
            on_empty_evidence: RouteClass::EvidenceFirst,
            max_model_calls: 0,
        }
    }
}

/// The router: deterministic classification plus estimate-driven planning.
/// Owns its telemetry so every decision is recorded and trainable.
pub struct Router {
    estimates: Ewma,
    recorder: Recorder,
}

impl Router {
    #[must_use]
    pub fn new() -> Self {
        Self {
            estimates: Ewma::new(ESTIMATE_ALPHA, DEFAULT_ESTIMATE_MS),
            recorder: Recorder::new(1024),
        }
    }

    /// Classifies `request` and builds the executable plan for `task`.
    /// Never calls a model or a judge: pure functions of text + history.
    #[must_use]
    pub fn route(&mut self, request: &str) -> RoutePlan {
        let classification = classify(request);
        let plan = plan::build(&classification, &self.estimates);
        self.recorder.record(
            &format!("route.{}", plan.class.name()),
            plan.predicted_evidence_ms,
        );
        plan
    }

    /// Folds one observed evidence latency into the estimates.
    pub fn observe_evidence_ms(&mut self, capability: &str, sample_ms: f64) {
        self.estimates.observe(capability, sample_ms);
        self.recorder.record(capability, sample_ms);
    }

    /// Latest route telemetry snapshot (counts per class + raw records).
    #[must_use]
    pub fn telemetry(&self) -> &Recorder {
        &self.recorder
    }

    #[must_use]
    pub fn estimate(&self, capability: &str) -> f64 {
        self.estimates.estimate(capability)
    }

    /// Builds the audit record for a plan (exported to telemetry stores).
    #[must_use]
    pub fn audit(plan: &RoutePlan, rules_fired: &[String]) -> RouteRecord {
        RouteRecord {
            class: plan.class.name().to_owned(),
            confidence: plan.confidence,
            rules_fired: rules_fired.to_owned(),
            evidence_ops: plan.evidence.len(),
            model_calls_planned: plan.escalation.max_model_calls,
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
