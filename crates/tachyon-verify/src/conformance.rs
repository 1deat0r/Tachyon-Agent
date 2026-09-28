//! Advisory intent-conformance checks after verification (ADR 0004, slice 3,
//! issue #42).
//!
//! [`check_conformance`] observes whether the human objective was met; it
//! never decides completion — that stays with [`VerificationReport`]. Only
//! [`ConformanceStatus::Violated`] fails conformance; `Unverifiable` items
//! are explicit unknowns, never silent passes.
//!
//! Coverage: acceptance criteria and attributed requirements, assumptions,
//! and compatibility requirements are compiled with
//! [`compile_criterion`](crate::compile_criterion) and structural clauses
//! are evaluated against the given snapshots; a spec
//! constraint reads Satisfied only when its text matches a
//! [`Clause::HardConstraint`] in the evaluated contract *and* the report
//! passed (the binding was actually evaluated in-run) — any other case is
//! Unverifiable, never an implied pass; non-goals have no machine check
//! and stay Unverifiable for operator review. Goal, outcome, preferences,
//! surfaces, ambiguities, and confidence are context, not checkables.
//!
//! Wiring contract: callers pass the real [`VerificationReport`] and the
//! real evaluated [`AcceptanceContract`](crate::AcceptanceContract) —
//! never a boolean — so the verdicts below always trace to an actual run.
//! The gate is untouched: this module calls nothing in the runner.
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
use crate::contract::{AcceptanceContract, Clause};
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

/// Maps an evaluation outcome to a verdict plus evidence.
fn eval_outcome(result: Result<(), String>, compiled: String) -> (ConformanceStatus, Vec<String>) {
    match result {
        Ok(()) => (
            ConformanceStatus::Satisfied,
            vec![compiled, "evaluation: satisfied".into()],
        ),
        Err(reason) => (
            ConformanceStatus::Violated,
            vec![compiled, format!("evaluation: {reason}")],
        ),
    }
}

/// Checks one compiled statement against two snapshots. One match, one
/// construction site: every arm yields a verdict plus evidence, and the
/// item is built once below.
fn check_statement(
    statement: &str,
    provenance: Option<Provenance>,
    baseline: &WorkspaceSnapshot,
    current: &WorkspaceSnapshot,
) -> ConformanceItem {
    let clause = compile_criterion(statement);
    let (status, evidence) = match &clause {
        Clause::CommandPasses { .. } => (
            ConformanceStatus::Unverifiable,
            vec!["commands are never evidence of passing".into()],
        ),
        Clause::Unresolved { .. } => (
            ConformanceStatus::Unverifiable,
            vec![format!("no machine check for: {statement}")],
        ),
        Clause::HardConstraint { .. } => (
            ConformanceStatus::Unverifiable,
            vec!["hard bindings are checked by the gate, not here".into()],
        ),
        Clause::FileUnchanged { path } => eval_outcome(
            evaluate_clause(&clause, baseline, current),
            format!("compiled to file-unchanged:{path}"),
        ),
        Clause::ChangedPathsWithin { paths } => eval_outcome(
            evaluate_clause(&clause, baseline, current),
            format!("compiled to changed-within:{}", paths.join(",")),
        ),
    };
    ConformanceItem {
        statement: statement.into(),
        provenance,
        status,
        evidence,
    }
}

/// Checks an [`IntentSpec`] against a finished [`VerificationReport`] and
/// the [`AcceptanceContract`](crate::AcceptanceContract) that report
/// evaluated.
///
/// Pure and deterministic: same inputs always yield the same report.
/// Advisory only — the returned `conforms` never authorizes completion.
#[must_use]
pub fn check_conformance(
    spec: &IntentSpec,
    contract: &AcceptanceContract,
    report: &VerificationReport,
    baseline: &WorkspaceSnapshot,
    current: &WorkspaceSnapshot,
) -> IntentConformanceReport {
    let mut items = Vec::new();
    for criterion in &spec.acceptance_criteria {
        items.push(check_statement(criterion, None, baseline, current));
    }
    for requirement in spec
        .requirements
        .iter()
        .chain(spec.assumptions.iter())
        .chain(spec.compatibility_requirements.iter())
    {
        items.push(check_statement(
            &requirement.text,
            Some(requirement.provenance),
            baseline,
            current,
        ));
    }
    let verified = report.passed();
    let bound: Vec<&String> = contract
        .clauses
        .iter()
        .filter_map(|clause| match clause {
            Clause::HardConstraint { text, .. } => Some(text),
            _ => None,
        })
        .collect();
    for constraint in &spec.constraints {
        // Satisfied only when this exact text was bound as a hard
        // constraint in the evaluated contract and the report passed —
        // the binding was actually evaluated in-run. Every other case is
        // Unverifiable with its reason stated, never an implied pass.
        let (status, reason) = if !bound.contains(&constraint) {
            (
                ConformanceStatus::Unverifiable,
                "not bound as a hard constraint in the evaluated contract",
            )
        } else if verified {
            (
                ConformanceStatus::Satisfied,
                "bound as hard constraint; upheld by passing verification",
            )
        } else {
            (
                ConformanceStatus::Unverifiable,
                "bound as hard constraint; verification did not pass",
            )
        };
        items.push(ConformanceItem {
            statement: constraint.clone(),
            provenance: None,
            status,
            evidence: vec![reason.into()],
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
