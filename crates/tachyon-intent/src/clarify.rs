//! Clarification policy: ask or skip, reusing the bounded-judgment pattern (ADR 0004, slice 4, issue #43).
//!
//! When intent is uncertain, Tachyon either asks a closed question or
//! skips the interruption. The question shape mirrors
//! `tachyon-judgment` exactly — [`Clarification::judgment_mapping`]
//! yields the field-for-field counterpart of a [`JudgmentItem`] with a
//! [`Choice`] kind, a [`CertaintyPolicy`], and [`AskUser`] on doubt —
//! without taking a dependency on that crate, so the belief layer stays
//! effect-free and the judgment crate stays the only place that spends
//! inference. The ask/skip call weighs information value against
//! interruption cost ([`decide`]); high-confidence low-risk reads below
//! cost skip the question, everything else asks. Non-finite inputs ask:
//! doubt never silently skips.
//!
//! [`JudgmentItem`]: https://docs.rs/tachyon-judgment/latest/tachyon_judgment/struct.JudgmentItem.html
//! [`Choice`]: https://docs.rs/tachyon-judgment/latest/tachyon_judgment/enum.JudgmentKind.html
//! [`CertaintyPolicy`]: https://docs.rs/tachyon-judgment/latest/tachyon_judgment/struct.CertaintyPolicy.html
//! [`AskUser`]: https://docs.rs/tachyon-judgment/latest/tachyon_judgment/enum.UncertainAction.html
//!
//! # New-capability checklist
//!
//! - Why deterministic code cannot already solve it: whether the operator
//!   should be interrupted over an ambiguity is a value judgment; this
//!   module only owns the deterministic threshold policy around it.
//! - Input schema: [`Clarification`] plus value/cost/confidence/risk.
//!   Output schema: [`ClarificationDecision`].
//! - Access set: none — pure policy, no I/O.
//! - Effect class: none. Idempotency: n/a. Resource claim: none.
//! - Cancellation/retry: n/a (synchronous pure function).
//! - Verification method: unit tests below.
//! - Crash-recovery behavior: n/a — decisions are recomputed per turn.
//! - Expected latency class: microseconds.

use serde::{Deserialize, Serialize};

/// Confidence at or above which a low-risk read may skip the question.
pub const HIGH_CONFIDENCE_THRESHOLD: f64 = 0.8;
/// Default interruption cost: asking costs attention, so the bar clears 0.5.
pub const DEFAULT_INTERRUPTION_COST: f32 = 0.5;

/// Risk of acting on the current reading without asking.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    /// Getting it wrong is cheap to undo.
    Low,
    /// Getting it wrong is expensive or irreversible.
    High,
}

/// Errors building a [`Clarification`].
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ClarifyError {
    /// `question` is empty.
    #[error("clarification question is empty")]
    EmptyQuestion,
    /// Fewer than two options: a closed question needs a real choice.
    #[error("clarification needs at least two options")]
    TooFewOptions,
    /// Option `index` is empty.
    #[error("clarification option {index} is empty")]
    EmptyOption {
        /// Position of the empty option.
        index: usize,
    },
}

/// One closed question posed to the operator when asking is worth it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Clarification {
    /// The question, in one line.
    pub question: String,
    /// Candidate answers, at least two, none empty.
    pub options: Vec<String>,
    /// Minimum judge confidence to accept (mirrors `CertaintyPolicy`
    /// `min_confidence`).
    pub min_confidence: f32,
}

impl Clarification {
    /// Builds a closed question: non-empty question, at least two
    /// non-empty options.
    pub fn new(
        question: &str,
        options: &[&str],
        min_confidence: f32,
    ) -> Result<Self, ClarifyError> {
        if question.trim().is_empty() {
            return Err(ClarifyError::EmptyQuestion);
        }
        if options.len() < 2 {
            return Err(ClarifyError::TooFewOptions);
        }
        for (index, option) in options.iter().enumerate() {
            if option.trim().is_empty() {
                return Err(ClarifyError::EmptyOption { index });
            }
        }
        // min_confidence is stored as-is, mirroring JudgmentItem: a
        // non-finite threshold accepts nothing downstream rather than
        // failing construction.
        Ok(Self {
            question: question.to_owned(),
            options: options.iter().map(ToString::to_string).collect(),
            min_confidence,
        })
    }

    /// Field-for-field counterpart of a judgment item: the question and
    /// options become a `Choice` kind, `min_confidence` becomes the
    /// `CertaintyPolicy` threshold, and doubt routes to `AskUser` — the
    /// operator is already the audience, so uncertainty escalates to
    /// them rather than to evidence or a model call.
    #[must_use]
    pub fn judgment_mapping(&self) -> JudgmentMapping {
        JudgmentMapping {
            question: self.question.clone(),
            options: self.options.clone(),
            min_confidence: self.min_confidence,
            on_uncertain: "ask_user".to_owned(),
        }
    }
}

/// The judgment-crate shape of a [`Clarification`], without the
/// dependency: field names and semantics match `JudgmentKind::Choice`,
/// `CertaintyPolicy`, and `UncertainAction` one to one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JudgmentMapping {
    /// Becomes `Choice.question`.
    pub question: String,
    /// Becomes `Choice.options`.
    pub options: Vec<String>,
    /// Becomes `CertaintyPolicy.min_confidence`.
    pub min_confidence: f32,
    /// Becomes `CertaintyPolicy.on_uncertain`; always `ask_user` here.
    pub on_uncertain: String,
}

/// Ask the question or skip the interruption.
#[derive(Clone, Debug, PartialEq)]
pub enum ClarificationDecision {
    /// Worth interrupting: pose the closed question.
    Ask(Clarification),
    /// Not worth it: high-confidence low-risk read below cost.
    Skip {
        /// Why the question was skipped; one of the `SKIP_*` reasons.
        reason: &'static str,
    },
}

/// Skip reason: confidence clears the bar, risk is low, and the answer
/// is worth less than the interruption.
pub const SKIP_BELOW_COST: &str = "high-confidence low-risk read worth less than the interruption";

/// Weighs information value against interruption cost for one validated
/// [`Clarification`]. Skips only when confidence clears
/// [`HIGH_CONFIDENCE_THRESHOLD`], risk is [`RiskLevel::Low`], and the
/// information value does not exceed the interruption cost; every other
/// combination — low confidence, high risk, valuable answer, or a
/// non-finite input anywhere — asks. Doubt never silently skips.
#[must_use]
pub fn decide(
    clarification: &Clarification,
    info_value: f32,
    interruption_cost: f32,
    confidence: f64,
    risk: RiskLevel,
) -> ClarificationDecision {
    // Fail closed: any doubt in the inputs asks rather than silently
    // skipping the operator.
    if !info_value.is_finite() || !interruption_cost.is_finite() || !confidence.is_finite() {
        return ClarificationDecision::Ask(clarification.clone());
    }
    if confidence >= HIGH_CONFIDENCE_THRESHOLD
        && risk == RiskLevel::Low
        && info_value <= interruption_cost
    {
        return ClarificationDecision::Skip {
            reason: SKIP_BELOW_COST,
        };
    }
    ClarificationDecision::Ask(clarification.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn question() -> Clarification {
        Clarification::new(
            "which theme should the blog use?",
            &["keep the current theme", "switch to the new theme"],
            0.6,
        )
        .expect("valid fixture question")
    }

    #[test]
    fn high_confidence_low_risk_below_cost_skips() {
        let decision = decide(&question(), 0.2, 0.5, 0.9, RiskLevel::Low);
        assert_eq!(
            decision,
            ClarificationDecision::Skip {
                reason: SKIP_BELOW_COST
            }
        );
    }

    #[test]
    fn low_confidence_asks() {
        let decision = decide(&question(), 0.2, 0.5, 0.4, RiskLevel::Low);
        assert!(matches!(decision, ClarificationDecision::Ask(_)));
    }

    #[test]
    fn high_risk_asks_despite_confidence() {
        let decision = decide(&question(), 0.1, 0.9, 0.95, RiskLevel::High);
        assert!(matches!(decision, ClarificationDecision::Ask(_)));
    }

    #[test]
    fn valuable_answers_ask_despite_confidence() {
        let decision = decide(&question(), 0.9, 0.5, 0.95, RiskLevel::Low);
        assert!(matches!(decision, ClarificationDecision::Ask(_)));
    }

    #[test]
    fn non_finite_inputs_ask_fail_closed() {
        for (info, cost, confidence) in [
            (f32::NAN, 0.5, 0.9),
            (0.2, f32::NAN, 0.9),
            (0.2, 0.5, f64::NAN),
            (f32::INFINITY, 0.5, 0.9),
        ] {
            let decision = decide(&question(), info, cost, confidence, RiskLevel::Low);
            assert!(
                matches!(decision, ClarificationDecision::Ask(_)),
                "non-finite input ({info}, {cost}, {confidence}) must ask, never skip"
            );
        }
    }

    #[test]
    fn empty_question_and_options_are_rejected() {
        assert_eq!(
            Clarification::new("", &["a", "b"], 0.5),
            Err(ClarifyError::EmptyQuestion)
        );
        assert_eq!(
            Clarification::new("q?", &["only"], 0.5),
            Err(ClarifyError::TooFewOptions)
        );
        assert_eq!(
            Clarification::new("q?", &["a", "  "], 0.5),
            Err(ClarifyError::EmptyOption { index: 1 })
        );
    }

    #[test]
    fn judgment_mapping_mirrors_closed_question_policy() {
        let mapping = question().judgment_mapping();
        assert_eq!(mapping.question, "which theme should the blog use?");
        assert_eq!(
            mapping.options,
            vec![
                "keep the current theme".to_string(),
                "switch to the new theme".to_string()
            ]
        );
        assert!((mapping.min_confidence - 0.6).abs() < f32::EPSILON);
        assert_eq!(mapping.on_uncertain, "ask_user");
    }
}
