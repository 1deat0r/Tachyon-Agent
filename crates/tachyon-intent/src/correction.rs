//! Correction classification: which operator corrections may become durable knowledge (ADR 0004, slice 4, issue #43).
//!
//! A [`Correction`] is a model/judge *proposal*: the caller assigns the
//! [`CorrectionClass`], Tachyon validates the shape and gates persistence.
//! Deterministic code cannot map open-ended operator language to one of
//! the six classes — that mapping needs judgment — so this module owns
//! only the representation, the validation, and the persistence gate.
//! Only the middle four classes (project convention, persistent
//! preference, model misunderstanding, missing project context) may
//! become [`DurableKnowledgeItem`](crate::knowledge::DurableKnowledgeItem)s;
//! task-specific corrections die with the task and bad-evidence
//! corrections lower confidence instead (see
//! [`apply_bad_evidence`](crate::knowledge::apply_bad_evidence)).
//!
//! # New-capability checklist
//!
//! - Why deterministic code cannot already solve it: assigning free-form
//!   operator feedback to a class is judgment over phrasing, not lookup;
//!   this module only gates caller-proposed classes, never assigns them.
//! - Input/output schema: [`Correction`] (serde, `deny_unknown_fields`).
//! - Access set: none — pure data, no I/O, no workspace access.
//! - Effect class: none. Idempotency: n/a (no effects).
//! - Resource claim: none. Cancellation/retry: n/a.
//! - Verification method: unit tests below.
//! - Crash-recovery behavior: n/a — corrections are re-proposed, never
//!   journaled as authoritative state in this slice.
//! - Expected latency class: microseconds (in-memory validation).

use serde::{Deserialize, Serialize};

/// The six correction classes (ADR 0004 decision 4), in canonical order.
/// Only the middle four may write durable knowledge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrectionClass {
    /// Applies to this task only; never persists.
    TaskSpecific,
    /// A repo convention worth remembering (e.g. "we use `/` scopes").
    ProjectConvention,
    /// A standing operator preference (e.g. "always explain the diff").
    PersistentPreference,
    /// The model misread something; the fix generalizes.
    ModelMisunderstanding,
    /// Repo context was missing; once recorded it stays useful.
    MissingProjectContext,
    /// The evidence behind a belief was wrong; lowers confidence instead
    /// of creating prohibitions.
    BadEvidence,
}

impl CorrectionClass {
    /// Whether a correction of this class may become durable knowledge.
    /// Exactly the middle four return true; [`CorrectionClass::TaskSpecific`]
    /// dies with the task and [`CorrectionClass::BadEvidence`] routes to
    /// confidence penalties instead of new items.
    #[must_use]
    pub fn may_persist(self) -> bool {
        matches!(
            self,
            Self::ProjectConvention
                | Self::PersistentPreference
                | Self::ModelMisunderstanding
                | Self::MissingProjectContext
        )
    }
}

/// Errors produced while validating or gating a [`Correction`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CorrectionError {
    /// `statement` is empty.
    #[error("correction statement is empty")]
    EmptyStatement,
    /// `confidence` is outside the unit range (or NaN).
    #[error("correction confidence {0} is outside 0.0..=1.0")]
    ConfidenceOutOfRange(f64),
    /// A persistable correction carries no evidence refs; durable
    /// knowledge without provenance is hearsay, so it is rejected.
    #[error("correction carries no evidence refs")]
    EmptyEvidenceRefs,
    /// This class may never become a durable item.
    #[error("correction class {class:?} never persists")]
    NotPersistable {
        /// The refused class.
        class: CorrectionClass,
    },
}

/// One operator correction, proposed by a model or judge and validated here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Correction {
    /// Caller-proposed class; Tachyon gates on it but never assigns it.
    pub class: CorrectionClass,
    /// What was corrected, in one statement; must be non-empty.
    pub statement: String,
    /// Evidence behind the correction as plain-string refs (source tags,
    /// repo paths); mandatory for durable proposals.
    pub evidence_refs: Vec<String>,
    /// Confidence in this reading, 0.0..=1.0.
    pub confidence: f64,
}

impl Correction {
    /// Validates structural well-formedness: non-empty statement,
    /// unit-range confidence, and evidence refs present exactly when the
    /// class may persist (task-specific notes need no provenance because
    /// they die with the task; bad-evidence penalties cite the item they
    /// contradict instead).
    pub fn validate(&self) -> Result<(), CorrectionError> {
        if self.statement.trim().is_empty() {
            return Err(CorrectionError::EmptyStatement);
        }
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err(CorrectionError::ConfidenceOutOfRange(self.confidence));
        }
        if self.class.may_persist() && self.evidence_refs.is_empty() {
            return Err(CorrectionError::EmptyEvidenceRefs);
        }
        Ok(())
    }

    /// Gates the durable-knowledge write: validates, refuses
    /// non-persistable classes, and builds the item. Task-specific
    /// corrections and bad-evidence penalties always fail here by design.
    pub fn propose_durable(
        self,
        policy: crate::knowledge::RevalidationPolicy,
    ) -> Result<crate::knowledge::DurableKnowledgeItem, CorrectionError> {
        self.validate()?;
        if !self.class.may_persist() {
            return Err(CorrectionError::NotPersistable { class: self.class });
        }
        Ok(crate::knowledge::DurableKnowledgeItem {
            id: uuid::Uuid::now_v7(),
            class: self.class,
            statement: self.statement,
            evidence_refs: self.evidence_refs,
            confidence: self.confidence,
            contradicting_observations: Vec::new(),
            policy,
            uses: 0,
            active: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn correction(class: CorrectionClass) -> Correction {
        Correction {
            class,
            statement: "follow existing CSS tokens".into(),
            evidence_refs: vec!["repo:styles/".into()],
            confidence: 0.7,
        }
    }

    #[test]
    fn persistability_gate_allows_only_the_middle_four() {
        assert!(!CorrectionClass::TaskSpecific.may_persist());
        assert!(CorrectionClass::ProjectConvention.may_persist());
        assert!(CorrectionClass::PersistentPreference.may_persist());
        assert!(CorrectionClass::ModelMisunderstanding.may_persist());
        assert!(CorrectionClass::MissingProjectContext.may_persist());
        assert!(!CorrectionClass::BadEvidence.may_persist());
    }

    #[test]
    fn task_specific_corrections_never_persist() {
        let policy = crate::knowledge::RevalidationPolicy::default();
        let result = correction(CorrectionClass::TaskSpecific).propose_durable(policy);
        assert_eq!(
            result,
            Err(CorrectionError::NotPersistable {
                class: CorrectionClass::TaskSpecific
            })
        );
    }

    #[test]
    fn bad_evidence_corrections_never_create_items() {
        // Bad evidence routes to apply_bad_evidence (confidence penalty on
        // the contradicted item), never to a new prohibition item.
        let policy = crate::knowledge::RevalidationPolicy::default();
        let result = correction(CorrectionClass::BadEvidence).propose_durable(policy);
        assert_eq!(
            result,
            Err(CorrectionError::NotPersistable {
                class: CorrectionClass::BadEvidence
            })
        );
    }

    #[test]
    fn durable_proposals_require_evidence_refs() {
        let mut proposal = correction(CorrectionClass::ProjectConvention);
        proposal.evidence_refs.clear();
        assert_eq!(proposal.validate(), Err(CorrectionError::EmptyEvidenceRefs));
        let policy = crate::knowledge::RevalidationPolicy::default();
        assert_eq!(
            proposal.propose_durable(policy),
            Err(CorrectionError::EmptyEvidenceRefs)
        );
    }

    #[test]
    fn confidence_outside_unit_range_is_rejected() {
        for bad in [-0.1, 1.1, f64::NAN] {
            let mut proposal = correction(CorrectionClass::ProjectConvention);
            proposal.confidence = bad;
            assert!(
                matches!(
                    proposal.validate(),
                    Err(CorrectionError::ConfidenceOutOfRange(_))
                ),
                "confidence {bad} must be rejected"
            );
        }
    }

    #[test]
    fn empty_statement_is_rejected() {
        let mut proposal = correction(CorrectionClass::ProjectConvention);
        proposal.statement = "   ".into();
        assert_eq!(proposal.validate(), Err(CorrectionError::EmptyStatement));
    }

    #[test]
    fn correction_round_trips_through_json() {
        let proposal = correction(CorrectionClass::MissingProjectContext);
        let raw = serde_json::to_value(&proposal).expect("serialize");
        let back: Correction = serde_json::from_value(raw).expect("deserialize");
        assert_eq!(proposal, back);
    }
}
