//! Plan building: classification plus estimates into executable graphs.
//!
//! Every class launches cheap evidence first (spec §22–§23). `DirectNative`
//! plans no model call; `EvidenceFirst` seals model context only after the
//! grace window; `ReasoningFirst` and `Hybrid` start model preparation
//! concurrently with evidence; `JudgmentFirst` resolves to evidence until
//! the M7 provider exists. Serial mode runs evidence one op at a time.

use crate::classifier::Classification;
use crate::{DEFAULT_ESTIMATE_MS, EscalationPolicy, GRACE_MS, RouteClass};
use tachyon_ir::{
    AccessSet, CancellationPolicy, EffectClass, ExecutionNode, ExecutorKind, Invocation,
    NodePriority, ResourceClaim, RetryPolicy, SpeculationPolicy, TimeoutPolicy,
};
use tachyon_telemetry::Ewma;
use tachyon_types::{CapabilityId, NodeId, TaskId};

/// One cheap evidence operation the plan launches.
#[derive(Clone, Debug, PartialEq)]
pub enum EvidenceOp {
    /// `repo.symbol.search`-style definition+use lookup.
    SymbolLookup { name: String },
    /// Lexical search for a pattern.
    LexicalSearch { pattern: String },
    /// Read-only git subcommand (`status`, `diff`, `log`).
    GitRead { subcommand: String },
    /// Read a workspace file.
    FileRead { path: String },
}

impl EvidenceOp {
    #[must_use]
    pub fn capability(&self) -> CapabilityId {
        CapabilityId(
            match self {
                Self::SymbolLookup { .. } | Self::LexicalSearch { .. } => "search.lexical",
                Self::GitRead { .. } => "git.read",
                Self::FileRead { .. } => "fs.read",
            }
            .to_owned(),
        )
    }

    #[must_use]
    pub fn args(&self) -> serde_json::Value {
        match self {
            Self::SymbolLookup { name } | Self::LexicalSearch { pattern: name } => {
                serde_json::json!({"pattern": name})
            }
            Self::GitRead { subcommand } => serde_json::json!({"subcommand": subcommand}),
            Self::FileRead { path } => serde_json::json!({"path": path}),
        }
    }

    /// Lowers the op to a validated-shape native IR node.
    #[must_use]
    pub fn to_node(&self, task_id: TaskId) -> ExecutionNode {
        ExecutionNode {
            id: NodeId::generate(),
            task_id,
            planned_revision: 0,
            executor: ExecutorKind::Native,
            invocation: Invocation {
                capability: self.capability(),
                args: self.args(),
                contract_version: tachyon_ir::CAPABILITY_CONTRACT_NONE,
            },
            inputs: vec![],
            expected_outputs: vec![],
            access: AccessSet {
                reads: vec![],
                writes: vec![],
            },
            resources: ResourceClaim::default(),
            effect_class: EffectClass::ReadOnlyLocal,
            idempotency: tachyon_ir::Idempotency::Pure,
            speculation: SpeculationPolicy::Forbidden,
            timeout: TimeoutPolicy {
                hard_ms: Some(30_000),
            },
            retry: RetryPolicy::default(),
            cancellation: CancellationPolicy::Immediate,
            verification: vec![],
            priority: NodePriority::High,
        }
    }
}

/// An executable route plan (spec §22).
#[derive(Clone, Debug)]
pub struct RoutePlan {
    pub class: RouteClass,
    pub confidence: f64,
    pub evidence: Vec<EvidenceOp>,
    /// Speculative nodes: model-prep work started concurrently (`Hybrid` and
    /// `ReasoningFirst` only). Empty until the M6 model layer defines them.
    pub speculative_nodes: Vec<ExecutionNode>,
    pub escalation: EscalationPolicy,
    /// Grace window (ms) before model context seals.
    pub grace_ms: u64,
    /// Serial reference mode: run evidence one op at a time.
    pub serial: bool,
    /// Sum of predicted evidence latency (ms) from EWMA estimates.
    pub predicted_evidence_ms: f64,
}

impl RoutePlan {
    /// True when the plan schedules model work (reasoning or judgment).
    /// The M5 gate asserts this is false for simple repo/search/git routes.
    #[must_use]
    pub fn requires_model(&self) -> bool {
        self.escalation.max_model_calls > 0 || !self.speculative_nodes.is_empty()
    }

    /// All evidence lowered to IR nodes, in execution order.
    #[must_use]
    pub fn initial_nodes(&self, task_id: TaskId) -> Vec<ExecutionNode> {
        self.evidence.iter().map(|op| op.to_node(task_id)).collect()
    }
}

/// Builds the plan for `classification`, pricing evidence with `estimates`.
#[must_use]
pub fn build(classification: &Classification, estimates: &Ewma) -> RoutePlan {
    let mut evidence = evidence_for(classification);
    // Price each op; drop nothing, but record the predicted total so the
    // caller can see whether evidence fits the grace window.
    let predicted_evidence_ms: f64 = evidence
        .iter()
        .map(|op| estimates.estimate(&op.capability().0))
        .sum();
    let (class, max_model_calls, serial) = match classification.class {
        RouteClass::DirectNative => (RouteClass::DirectNative, 0, false),
        RouteClass::EvidenceFirst => (RouteClass::EvidenceFirst, 0, false),
        RouteClass::JudgmentFirst => {
            // No judge until M7: resolve to evidence-first, keep the label
            // so telemetry shows where judgment would have run.
            (RouteClass::JudgmentFirst, 0, false)
        }
        RouteClass::ReasoningFirst => (RouteClass::ReasoningFirst, 1, false),
        RouteClass::Hybrid => (RouteClass::Hybrid, 1, false),
    };
    if evidence.is_empty() {
        evidence.push(EvidenceOp::LexicalSearch {
            pattern: classification
                .candidates
                .first()
                .cloned()
                .unwrap_or_else(String::new),
        });
    }
    RoutePlan {
        class,
        confidence: classification.confidence,
        evidence,
        speculative_nodes: vec![],
        escalation: EscalationPolicy {
            on_empty_evidence: RouteClass::EvidenceFirst,
            max_model_calls,
        },
        grace_ms: GRACE_MS,
        serial,
        predicted_evidence_ms: predicted_evidence_ms.max(DEFAULT_ESTIMATE_MS),
    }
}

/// Enables serial reference mode on a plan (constrained contexts).
#[must_use]
pub fn serial(mut plan: RoutePlan) -> RoutePlan {
    plan.serial = true;
    plan
}

fn evidence_for(classification: &Classification) -> Vec<EvidenceOp> {
    let mut ops = Vec::new();
    for name in &classification.candidates {
        ops.push(EvidenceOp::SymbolLookup { name: name.clone() });
    }
    for rule in &classification.rules_fired {
        match rule.as_str() {
            "show-status" => ops.push(EvidenceOp::GitRead {
                subcommand: "status".to_owned(),
            }),
            "run-command" => ops.push(EvidenceOp::GitRead {
                subcommand: "log".to_owned(),
            }),
            "what-changed" => ops.push(EvidenceOp::GitRead {
                subcommand: "diff".to_owned(),
            }),
            _ => {}
        }
    }
    ops.truncate(6);
    ops
}
