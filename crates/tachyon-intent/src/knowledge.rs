//! Durable knowledge lifecycle: from gated correction to retired item (ADR 0004, slice 4, issue #43).
//!
//! A [`DurableKnowledgeItem`] is born only through
//! [`Correction::propose_durable`](crate::correction::Correction::propose_durable),
//! so the class gate is unskippable: no caller can mint a durable item
//! from a task-specific note or a bad-evidence report. Every item carries
//! evidence refs, a unit-range confidence, contradicting observations,
//! and a [`RevalidationPolicy`]; storage is the caller\'s JSON document
//! (see [`DurableKnowledgeItem::storage_key`]) written through the
//! existing `tachyon-store` shapes — this crate takes no store dependency
//! so the belief layer stays effect-free.
//!
//! Evidence refs are plain strings (source tags, repo paths), the same
//! shape as [`IntentSpec::evidence`](crate::IntentSpec::evidence): a
//! knowledge item cites evidence, it never snapshots it. Full
//! [`EvidenceItem`](https://docs.rs/tachyon-retrieval/latest/tachyon_retrieval/struct.EvidenceItem.html)
//! values stay in `tachyon-retrieval`; snapshotting their content here
//! would freeze stale hashes into durable rules, which the retrieval
//! layer forbids as mutation authority.
//!
//! Bad evidence never creates prohibitions: [`apply_bad_evidence`] lowers
//! the contradicted item\'s confidence and records the observation on
//! that same item.
//!
//! Known prerequisite (issue #43): cost tracking. `tachyon-telemetry`
//! carries no cost field yet, so no future cost-per-verified-task
//! objective can consume these items; recording that gap here keeps this
//! slice honest about what it does not do.
//!
//! # New-capability checklist
//!
//! - Why deterministic code cannot already solve it: the correction text
//!   and its class arrive as model/judge proposals; this module only owns
//!   the deterministic lifecycle (confirm, contradict, retire) around them.
//! - Input/output schema: [`DurableKnowledgeItem`] (serde,
//!   `deny_unknown_fields`).
//! - Access set: none — in-memory over caller-held values, no I/O.
//! - Effect class: none. Idempotency: n/a. Resource claim: none.
//! - Cancellation/retry: n/a (synchronous pure logic).
//! - Verification method: unit tests below.
//! - Crash-recovery behavior: n/a — items are reloaded from the
//!   caller\'s persisted JSON, never journaled as task authority.
//! - Expected latency class: microseconds.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::correction::{CorrectionClass, CorrectionError};

/// Confidence gained per confirmation, capped at 1.0.
pub const CONFIRM_STEP: f64 = 0.05;
/// Confidence lost per contradiction, floored at 0.0.
pub const CONTRADICT_STEP: f64 = 0.15;

/// Revalidation and expiry policy carried by every durable item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevalidationPolicy {
    /// Uses after which the item wants operator revalidation;
    /// `None` means no schedule (standing preferences).
    pub revalidate_after_uses: Option<u32>,
    /// Contradictions after which the item retires itself.
    pub retire_after_contradictions: u32,
}

impl Default for RevalidationPolicy {
    /// Standing knowledge: no revalidation schedule, retires on the
    /// third contradiction.
    fn default() -> Self {
        Self {
            revalidate_after_uses: None,
            retire_after_contradictions: 3,
        }
    }
}

/// Durable knowledge distilled from one persistable correction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableKnowledgeItem {
    /// Stable identity; also the storage key suffix.
    pub id: Uuid,
    /// The persistable class this was proposed under.
    pub class: CorrectionClass,
    /// The remembered statement.
    pub statement: String,
    /// Evidence behind it as plain-string refs (source tags, repo
    /// paths, matching [`IntentSpec::evidence`](crate::IntentSpec::evidence));
    /// never empty.
    pub evidence_refs: Vec<String>,
    /// Current confidence, 0.0..=1.0.
    pub confidence: f64,
    /// Observations that contradicted it, oldest first.
    pub contradicting_observations: Vec<String>,
    /// Revalidation/expiry schedule.
    pub policy: RevalidationPolicy,
    /// Successful applications since proposal.
    pub uses: u32,
    /// False once retired; retired items are kept for audit, never applied.
    pub active: bool,
}

impl DurableKnowledgeItem {
    /// Records one successful application: bumps the use count, cites
    /// the new evidence, and nudges confidence up to a 1.0 cap.
    /// Retired items stay retired — confirming a dead item is a no-op.
    pub fn confirm(&mut self, evidence_ref: &str) {
        if !self.active {
            return;
        }
        self.uses = self.uses.saturating_add(1);
        self.evidence_refs.push(evidence_ref.to_owned());
        self.confidence = (self.confidence + CONFIRM_STEP).min(1.0);
    }

    /// Records one contradiction: appends the observation, drops
    /// confidence toward a 0.0 floor, and retires the item once
    /// contradictions reach the policy threshold.
    pub fn contradict(&mut self, observation: &str) {
        if !self.active {
            return;
        }
        self.contradicting_observations.push(observation.to_owned());
        self.confidence = (self.confidence - CONTRADICT_STEP).max(0.0);
        if self.contradicting_observations.len() >= self.policy.retire_after_contradictions as usize
        {
            self.active = false;
        }
    }

    /// True once uses reach the configured schedule. Items without a
    /// schedule never need revalidation.
    #[must_use]
    pub fn needs_revalidation(&self) -> bool {
        match self.policy.revalidate_after_uses {
            Some(limit) => self.uses >= limit,
            None => false,
        }
    }

    /// Caller-side storage key (`knowledge/<uuid>`); the caller persists
    /// the item\'s JSON under this key through existing store shapes.
    #[must_use]
    pub fn storage_key(&self) -> String {
        format!("knowledge/{}", self.id)
    }
}

/// Applies a bad-evidence correction to the item it contradicts: lowers
/// confidence by `penalty` (floored at 0.0), records the observation on
/// that same item, and retires it at the policy threshold. Creates
/// nothing — the prohibition path does not exist by construction, so
/// callers cannot mistake a broken-evidence report for a new rule.
pub fn apply_bad_evidence(
    item: &mut DurableKnowledgeItem,
    observation: &str,
    penalty: f64,
) -> Result<(), CorrectionError> {
    if !penalty.is_finite() || !(0.0..=1.0).contains(&penalty) {
        return Err(CorrectionError::ConfidenceOutOfRange(penalty));
    }
    if !item.active {
        return Ok(());
    }
    item.contradicting_observations.push(observation.to_owned());
    item.confidence = (item.confidence - penalty).max(0.0);
    if item.contradicting_observations.len() >= item.policy.retire_after_contradictions as usize {
        item.active = false;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item() -> DurableKnowledgeItem {
        DurableKnowledgeItem {
            id: Uuid::now_v7(),
            class: CorrectionClass::ProjectConvention,
            statement: "follow existing CSS tokens".into(),
            evidence_refs: vec!["repo:styles/".into()],
            confidence: 0.7,
            contradicting_observations: Vec::new(),
            policy: RevalidationPolicy {
                revalidate_after_uses: Some(2),
                retire_after_contradictions: 2,
            },
            uses: 0,
            active: true,
        }
    }

    #[test]
    fn confirm_raises_confidence_and_records_use() {
        let mut knowledge = item();
        knowledge.confirm("repo:tokens.css");
        assert_eq!(knowledge.uses, 1);
        assert_eq!(
            knowledge.evidence_refs,
            vec!["repo:styles/".to_string(), "repo:tokens.css".to_string()]
        );
        assert!((knowledge.confidence - 0.75).abs() < f64::EPSILON);
        assert!(knowledge.active);
    }

    #[test]
    fn confirm_caps_confidence_at_one() {
        let mut knowledge = item();
        knowledge.confidence = 0.99;
        knowledge.confirm("repo:more/");
        assert!((knowledge.confidence - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn contradict_lowers_confidence_and_records_observation() {
        let mut knowledge = item();
        knowledge.contradict("v2 theme drops the token file");
        assert_eq!(
            knowledge.contradicting_observations,
            vec!["v2 theme drops the token file".to_string()]
        );
        assert!((knowledge.confidence - 0.55).abs() < f64::EPSILON);
        assert!(knowledge.active, "first contradiction must not retire");
    }

    #[test]
    fn contradictions_retire_the_item_at_threshold() {
        let mut knowledge = item();
        knowledge.contradict("first");
        knowledge.contradict("second");
        assert!(!knowledge.active);
        assert_eq!(knowledge.contradicting_observations.len(), 2);
        // Retired items stay retired and keep their history.
        knowledge.confirm("repo:late/");
        assert!(!knowledge.active);
        assert_eq!(knowledge.uses, 0);
    }

    #[test]
    fn revalidation_flags_after_configured_uses() {
        let mut knowledge = item();
        assert!(!knowledge.needs_revalidation());
        knowledge.confirm("repo:a/");
        assert!(!knowledge.needs_revalidation());
        knowledge.confirm("repo:b/");
        assert!(knowledge.needs_revalidation());
    }

    #[test]
    fn items_without_a_schedule_never_need_revalidation() {
        let mut knowledge = item();
        knowledge.policy.revalidate_after_uses = None;
        for _ in 0..10 {
            knowledge.confirm("repo:x/");
        }
        assert!(!knowledge.needs_revalidation());
    }

    #[test]
    fn bad_evidence_lowers_confidence_without_creating_prohibitions() {
        let mut knowledge = item();
        let id_before = knowledge.id;
        let refs_before = knowledge.evidence_refs.clone();
        apply_bad_evidence(&mut knowledge, "token file was generated output", 0.4)
            .expect("penalty applies");
        // Same item, lowered — nothing new minted, no new evidence claimed.
        assert_eq!(knowledge.id, id_before);
        assert_eq!(knowledge.evidence_refs, refs_before);
        assert!((knowledge.confidence - 0.3).abs() < f64::EPSILON);
        assert_eq!(
            knowledge.contradicting_observations,
            vec!["token file was generated output".to_string()]
        );
    }

    #[test]
    fn bad_evidence_penalty_outside_unit_range_is_rejected() {
        let mut knowledge = item();
        for bad in [-0.1, 1.1, f64::NAN] {
            assert!(
                matches!(
                    apply_bad_evidence(&mut knowledge, "obs", bad),
                    Err(CorrectionError::ConfidenceOutOfRange(_))
                ),
                "penalty {bad} must be rejected"
            );
        }
        assert!((knowledge.confidence - 0.7).abs() < f64::EPSILON);
    }

    #[test]
    fn item_round_trips_through_json_with_its_key() {
        let knowledge = item();
        let raw = serde_json::to_value(&knowledge).expect("serialize");
        let back: DurableKnowledgeItem = serde_json::from_value(raw).expect("deserialize");
        assert_eq!(knowledge, back);
        assert_eq!(
            knowledge.storage_key(),
            format!("knowledge/{}", knowledge.id)
        );
    }
}
