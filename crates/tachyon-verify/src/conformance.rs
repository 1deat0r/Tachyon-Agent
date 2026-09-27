//! Advisory intent-conformance checks after verification (ADR 0004, slice 3,
//! issue #42).
//!
//! [`check_conformance`] observes whether the human objective was met; it
//! never decides completion — that stays with [`VerificationReport`]. Only
//! [`ConformanceStatus::Violated`] fails conformance; `Unverifiable` items
//! are explicit unknowns, never silent passes.
//!
//! Coverage: acceptance criteria and attributed requirements/assumptions
//! are compiled with [`compile_criterion`](crate::compile_criterion) and
//! structural clauses are evaluated against the given snapshots;
//! constraints read Satisfied only off a passing report (their hard
//! bindings were evaluated in-run) and Unverifiable otherwise — a failed
//! gate blames nothing; non-goals have no machine check and stay
//! Unverifiable for operator review. Goal, outcome, preferences,
//! surfaces, ambiguities, and confidence are context, not checkables.
//!
//! # New-capability checklist
//!
//! - Why deterministic code cannot already solve it: mapping open-ended
//!   requirements to verdicts needs judgment; this module only owns the
//!   deterministic checking of compilable statements, never the mapping.
//! - Input schema: [`IntentSpec`](tachyon_intent::IntentSpec),
//!   [`VerificationReport`], two snapshots. Output schema:
//!   [`IntentConformanceReport`] (serde, `deny_unknown_fields`).
//! - Access set: none — in-memory over caller-provided values, no I/O.
//! - Effect class: none. Idempotency: n/a. Resource claim: none.
//! - Cancellation/retry: n/a (synchronous pure function).
//! - Verification method: integration tests over real runs
//!   (`tests/conformance.rs`).
//! - Crash-recovery behavior: n/a — reports are recomputed, never
//!   journaled as authority.
//! - Expected latency class: microseconds plus snapshot comparison.

use serde::{Deserialize, Serialize};
use tachyon_intent::{IntentSpec, Provenance};

use crate::compile::compile_criterion;
use crate::contract::Clause;
use crate::plan::evaluate_clause;
use crate::runner::VerificationReport;
use crate::snapshot::WorkspaceSnapshot;

/// Verdict for one checked statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceStatus {
    /// A machine check confirmed the statement.
    Satisfied,
    /// A machine check refuted the statement.
    Violated,
    /// No machine check exists; explicit unknown, never a silent pass.
    Unverifiable,
}

/// One statement checked against execution evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConformanceItem {
    /// The checked statement.
    pub statement: String,
    /// Provenance of the statement, when it had one (criteria and
    /// constraints are unattributed free text).
    pub provenance: Option<Provenance>,
    /// The verdict.
    pub status: ConformanceStatus,
    /// Evidence references behind the verdict; never empty.
    pub evidence: Vec<String>,
}

/// Advisory conformance of one execution to one [`IntentSpec`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentConformanceReport {
    /// True when no item is [`ConformanceStatus::Violated`].
    pub conforms: bool,
    /// Per-statement verdicts, in spec order.
    pub items: Vec<ConformanceItem>,
}

/// Checks one compiled statement against two snapshots.
fn check_statement(
    statement: &str,
    provenance: Option<Provenance>,
    baseline: &WorkspaceSnapshot,
    current: &WorkspaceSnapshot,
) -> ConformanceItem {
    let clause = compile_criterion(statement);
    let kind = match &clause {
        Clause::CommandPasses { .. } => "command",
        Clause::FileUnchanged { .. } => "file-unchanged",
        Clause::ChangedPathsWithin { .. } => "changed-within",
        Clause::HardConstraint { .. } => "hard-constraint",
        Clause::Unresolved { .. } => "unresolved",
    };
    match &clause {
        Clause::CommandPasses { .. } => ConformanceItem {
            statement: statement.into(),
            provenance,
            status: ConformanceStatus::Unverifiable,
            evidence: vec!["commands are never evidence of passing".into()],
        },
        Clause::Unresolved { .. } => ConformanceItem {
            statement: statement.into(),
            provenance,
            status: ConformanceStatus::Unverifiable,
            evidence: vec![format!("no machine check for: {statement}")],
        },
        Clause::HardConstraint { .. } => ConformanceItem {
            statement: statement.into(),
            provenance,
            status: ConformanceStatus::Unverifiable,
            evidence: vec!["hard bindings are checked by the gate, not here".into()],
        },
        structural => {
            let evidence = format!("compiled to {kind}");
            match evaluate_clause(structural, baseline, current) {
                Ok(()) => ConformanceItem {
                    statement: statement.into(),
                    provenance,
                    status: ConformanceStatus::Satisfied,
                    evidence: vec![evidence, "evaluation: satisfied".into()],
                },
                Err(reason) => ConformanceItem {
                    statement: statement.into(),
                    provenance,
                    status: ConformanceStatus::Violated,
                    evidence: vec![evidence, format!("evaluation: {reason}")],
                },
            }
        }
    }
}

/// Checks an [`IntentSpec`] against a finished [`VerificationReport`.
///
/// Pure and deterministic: same inputs always yield the same report.
/// Advisory only — the returned `conforms` never authorizes completion.
#[must_use]
pub fn check_conformance(
    spec: &IntentSpec,
    report: &VerificationReport,
    baseline: &WorkspaceSnapshot,
    current: &WorkspaceSnapshot,
) -> IntentConformanceReport {
    let mut items = Vec::new();
    for criterion in &spec.acceptance_criteria {
        items.push(check_statement(criterion, None, baseline, current));
    }
    for requirement in spec.requirements.iter().chain(spec.assumptions.iter()) {
        items.push(check_statement(
            &requirement.text,
            Some(requirement.provenance),
            baseline,
            current,
        ));
    }
    let verified = report.passed();
    for constraint in &spec.constraints {
        items.push(ConformanceItem {
            statement: constraint.clone(),
            provenance: None,
            status: if verified {
                ConformanceStatus::Satisfied
            } else {
                ConformanceStatus::Unverifiable
            },
            evidence: vec![if verified {
                "upheld by passing verification".into()
            } else {
                "verification did not pass; see gate failures".into()
            }],
        });
    }
    for non_goal in &spec.non_goals {
        items.push(ConformanceItem {
            statement: non_goal.clone(),
            provenance: None,
            status: ConformanceStatus::Unverifiable,
            evidence: vec!["no machine check; operator review".into()],
        });
    }
    let conforms = !items
        .iter()
        .any(|item| item.status == ConformanceStatus::Violated);
    IntentConformanceReport { conforms, items }
}
