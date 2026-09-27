//! Intent representation: what Tachyon believes the human wants (ADR 0004).
//!
//! [`IntentSpec`] is a belief record, not an execution plan. Inferred items
//! carry [`Provenance`] so user-stated facts, repository-derived facts, and
//! model hypotheses are never confused; nothing inferred can outrank a hard
//! constraint (structural quarantine here, behavioral precedence in the
//! slice-2 criteria compiler).
//!
//! # New-capability checklist
//!
//! - Why deterministic code cannot already solve it: mapping open-ended
//!   human language to a typed objective requires model inference; this
//!   crate only owns the *representation* and its validation, never the
//!   inference itself.
//! - Input/output schema: [`IntentSpec`] (serde, `deny_unknown_fields`).
//! - Access set: none — pure data, no I/O, no workspace access.
//! - Effect class: none. Idempotency: n/a (no effects).
//! - Resource claim: none. Cancellation/retry: n/a.
//! - Verification method: [`IntentSpec::validate`] + unit tests.
//! - Crash-recovery behavior: n/a — specs are re-derived or reloaded, never
//!   journaled as authoritative state in this slice.
//! - Expected latency class: microseconds (in-memory validation).

use serde::{Deserialize, Serialize};

/// Where an attributed intent item came from. User-stated facts outrank
/// repository-derived facts, which outrank model hypotheses; the ordering
/// is structural (separate lists + accessors) so inference can never
/// silently join the authoritative set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Stated by the human directly.
    UserStated,
    /// Derived deterministically from repository evidence.
    RepoDerived,
    /// Guessed by a model; must carry evidence before anything trusts it.
    ModelHypothesis,
}

/// One intent statement with mandatory provenance. There is no
/// unattributed text: deserialization without `provenance` fails.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttributedText {
    /// The statement itself; must be non-empty.
    pub text: String,
    /// Where the statement came from.
    pub provenance: Provenance,
}

impl AttributedText {
    /// A user-stated item.
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            provenance: Provenance::UserStated,
        }
    }

    /// A repository-derived item.
    pub fn repo(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            provenance: Provenance::RepoDerived,
        }
    }

    /// A model-hypothesized item.
    pub fn inferred(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            provenance: Provenance::ModelHypothesis,
        }
    }
}

/// Errors produced while validating an [`IntentSpec`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum IntentError {
    /// `goal` is empty.
    #[error("intent goal is empty")]
    EmptyGoal,
    /// `desired_outcome` is empty.
    #[error("intent desired_outcome is empty")]
    EmptyDesiredOutcome,
    /// `confidence` is outside the unit range (or NaN).
    #[error("intent confidence {0} is outside 0.0..=1.0")]
    ConfidenceOutOfRange(f64),
    /// An attributed item in `field` at index `index` has empty text.
    #[error("intent {field}[{index}] has empty text")]
    EmptyAttributedText {
        /// Which list held the empty item.
        field: &'static str,
        /// Position inside that list.
        index: usize,
    },
    /// A plain-text entry in `field` at index `index` is empty: even a
    /// hard constraint is meaningless blank, so it fails validation.
    #[error("intent {field}[{index}] has an empty entry")]
    EmptyListEntry {
        /// Which list held the empty entry.
        field: &'static str,
        /// Position inside that list.
        index: usize,
    },
}

/// What Tachyon believes the human wants (ADR 0004).
///
/// A belief record, not an execution plan: it never authorizes work on its
/// own. Hard constraints live in the plain-text `constraints` list, kept
/// structurally separate from provenance-tagged belief lists so inferred
/// items stay quarantined until the slice-2 criteria compiler checks them
/// against authoritative requirements.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntentSpec {
    /// What the human wants, in one line.
    pub goal: String,
    /// The observable end state.
    pub desired_outcome: String,
    /// Required behaviors, each with provenance.
    pub requirements: Vec<AttributedText>,
    /// Hard constraints; authoritative over any inferred item.
    pub constraints: Vec<String>,
    /// Soft preferences, each with provenance.
    pub preferences: Vec<AttributedText>,
    /// Explicit non-goals; preserved by verification.
    pub non_goals: Vec<String>,
    /// Surfaces the work may touch.
    pub affected_surfaces: Vec<String>,
    /// Checkable acceptance criteria (compiled to `Clause` in slice 2).
    pub acceptance_criteria: Vec<String>,
    /// Open questions.
    pub ambiguities: Vec<String>,
    /// Working assumptions, each with provenance.
    pub assumptions: Vec<AttributedText>,
    /// Evidence references backing inferred items.
    pub evidence: Vec<String>,
    /// Overall confidence in this reading, 0.0..=1.0.
    pub confidence: f64,
}

impl IntentSpec {
    /// Validates structural well-formedness: non-empty goal/outcome,
    /// unit-range confidence, non-empty attributed text, and no blank
    /// entries in the plain-text lists.
    pub fn validate(&self) -> Result<(), IntentError> {
        if self.goal.trim().is_empty() {
            return Err(IntentError::EmptyGoal);
        }
        if self.desired_outcome.trim().is_empty() {
            return Err(IntentError::EmptyDesiredOutcome);
        }
        if !(0.0..=1.0).contains(&self.confidence) {
            return Err(IntentError::ConfidenceOutOfRange(self.confidence));
        }
        for (field, items) in [
            ("requirements", &self.requirements),
            ("preferences", &self.preferences),
            ("assumptions", &self.assumptions),
        ] {
            for (index, item) in items.iter().enumerate() {
                if item.text.trim().is_empty() {
                    return Err(IntentError::EmptyAttributedText { field, index });
                }
            }
        }
        for (field, items) in [
            ("constraints", &self.constraints),
            ("non_goals", &self.non_goals),
            ("affected_surfaces", &self.affected_surfaces),
            ("acceptance_criteria", &self.acceptance_criteria),
            ("ambiguities", &self.ambiguities),
            ("evidence", &self.evidence),
        ] {
            for (index, item) in items.iter().enumerate() {
                if item.trim().is_empty() {
                    return Err(IntentError::EmptyListEntry { field, index });
                }
            }
        }
        Ok(())
    }

    /// Requirements with one provenance, the shared partition behind the
    /// user/inferred accessors.
    fn filter_by(&self, provenance: Provenance) -> Vec<&AttributedText> {
        self.requirements
            .iter()
            .filter(|item| item.provenance == provenance)
            .collect()
    }

    /// Requirements stated by the human.
    #[must_use]
    pub fn user_requirements(&self) -> Vec<&AttributedText> {
        self.filter_by(Provenance::UserStated)
    }

    /// Requirements hypothesized by a model; quarantined from the
    /// user-stated set until evidence and compilation admit them.
    #[must_use]
    pub fn inferred_requirements(&self) -> Vec<&AttributedText> {
        self.requirements
            .iter()
            .filter(|item| item.provenance == Provenance::ModelHypothesis)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_spec() -> IntentSpec {
        IntentSpec {
            goal: "migrate the blog to the new theme".into(),
            desired_outcome: "identical content, new look, no broken links".into(),
            requirements: vec![
                AttributedText::user("preserve all post URLs"),
                AttributedText::inferred("keep the RSS feed working"),
            ],
            constraints: vec!["no downtime during migration".into()],
            preferences: vec![AttributedText::repo("follow existing CSS tokens")],
            non_goals: vec!["redesigning the logo".into()],
            affected_surfaces: vec!["blog".into()],
            acceptance_criteria: vec!["all post URLs return 200".into()],
            ambiguities: vec![],
            assumptions: vec![AttributedText::inferred("theme supports RSS")],
            evidence: vec!["repo:themes/".into()],
            confidence: 0.8,
        }
    }

    #[test]
    fn complete_spec_validates() {
        full_spec().validate().expect("complete spec validates");
    }

    #[test]
    fn empty_goal_is_rejected() {
        let mut spec = full_spec();
        spec.goal.clear();
        assert!(matches!(spec.validate(), Err(IntentError::EmptyGoal)));
    }

    #[test]
    fn empty_desired_outcome_is_rejected() {
        let mut spec = full_spec();
        spec.desired_outcome.clear();
        assert!(matches!(
            spec.validate(),
            Err(IntentError::EmptyDesiredOutcome)
        ));
    }

    #[test]
    fn confidence_outside_unit_range_is_rejected() {
        for bad in [-0.1, -1.0, 1.1, 2.0, f64::NAN] {
            let mut spec = full_spec();
            spec.confidence = bad;
            assert!(
                matches!(spec.validate(), Err(IntentError::ConfidenceOutOfRange(_))),
                "confidence {bad} must be rejected"
            );
        }
    }

    #[test]
    fn empty_attributed_text_is_rejected() {
        let mut spec = full_spec();
        spec.requirements.push(AttributedText::inferred(""));
        assert!(matches!(
            spec.validate(),
            Err(IntentError::EmptyAttributedText { .. })
        ));
    }

    #[test]
    fn blank_entries_in_plain_lists_are_rejected() {
        let mut spec = full_spec();
        spec.constraints.push("   ".into());
        assert!(matches!(
            spec.validate(),
            Err(IntentError::EmptyListEntry {
                field: "constraints",
                index: 1,
            })
        ));

        let mut spec = full_spec();
        spec.acceptance_criteria.push(String::new());
        assert!(matches!(
            spec.validate(),
            Err(IntentError::EmptyListEntry {
                field: "acceptance_criteria",
                index: 1,
            })
        ));
    }

    #[test]
    fn missing_provenance_fails_deserialization() {
        let raw = serde_json::json!({
            "goal": "g",
            "desired_outcome": "o",
            "requirements": [{"text": "no provenance"}],
            "constraints": [],
            "preferences": [],
            "non_goals": [],
            "affected_surfaces": [],
            "acceptance_criteria": [],
            "ambiguities": [],
            "assumptions": [],
            "evidence": [],
            "confidence": 0.5,
        });
        let result: Result<IntentSpec, _> = serde_json::from_value(raw);
        assert!(
            result.is_err(),
            "attributed text without provenance must not parse"
        );
    }

    #[test]
    fn unknown_fields_fail_deserialization() {
        let mut raw = serde_json::to_value(full_spec()).expect("serialize");
        raw["surprise"] = serde_json::json!("injected");
        let result: Result<IntentSpec, _> = serde_json::from_value(raw);
        assert!(result.is_err(), "unknown fields must not parse");
    }

    #[test]
    fn spec_round_trips_through_json() {
        let spec = full_spec();
        let raw = serde_json::to_value(&spec).expect("serialize");
        let back: IntentSpec = serde_json::from_value(raw).expect("deserialize");
        assert_eq!(spec, back);
    }

    #[test]
    fn inferred_items_stay_quarantined_from_constraints() {
        // Structural half of the precedence rule: a model hypothesis that
        // contradicts a hard constraint must appear ONLY in the inferred
        // set, never alongside user constraints. Behavioral precedence
        // (the hypothesis loses at compile time) lands in slice 2 (#41).
        let spec = IntentSpec {
            constraints: vec!["no downtime during migration".into()],
            requirements: vec![AttributedText::inferred(
                "take the site down briefly to migrate faster",
            )],
            ..full_spec()
        };
        spec.validate().expect("quarantined spec still validates");
        let inferred = spec.inferred_requirements();
        assert_eq!(inferred.len(), 1);
        assert!(spec.constraints.iter().all(|c| c != &inferred[0].text));
        assert!(
            spec.user_requirements()
                .iter()
                .all(|r| r.text != inferred[0].text),
            "inferred text must not leak into the user-stated set"
        );
    }

    #[test]
    fn provenance_accessors_partition_requirements() {
        let spec = full_spec();
        assert_eq!(spec.user_requirements().len(), 1);
        assert_eq!(spec.inferred_requirements().len(), 1);
        assert!(
            spec.user_requirements()
                .iter()
                .all(|r| r.provenance == Provenance::UserStated)
        );
        assert!(
            spec.inferred_requirements()
                .iter()
                .all(|r| r.provenance == Provenance::ModelHypothesis)
        );
    }
}
