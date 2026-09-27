//! Acceptance-criteria compiler: free-text criteria to [`Clause`] (ADR 0004,
//! slice 2, issue #41).
//!
//! Total, deterministic, and narrow: a criterion compiles to a permissive
//! clause only when it matches a documented structured form exactly;
//! everything else becomes [`Clause::Unresolved`], which the existing
//! evaluation fails closed. Narrowness is the precedence mechanism — a
//! conflicting model hypothesis can never compile into something that
//! overrides a [`Clause::HardConstraint`], while every spec constraint is
//! lifted to a hard binding by construction.
//!
//! Structured forms (exact lowercase prefix, value trimmed):
//!
//! - `file-unchanged: <path>` → [`Clause::FileUnchanged`] when the path
//!   passes source-path validation, else [`Clause::Unresolved`].
//! - `changed-within: <path>[, <path>...]` → [`Clause::ChangedPathsWithin`]
//!   when every path validates, at least one is present, no segment is
//!   empty, and none is `.` (a lone allow-everything from trivial text
//!   would be over-permissive); else [`Clause::Unresolved`].
//!
//! No effects or I/O anywhere here: pure total functions, hence no
//! resource, cancellation, retry, or crash-recovery claims.

use tachyon_intent::IntentSpec;
use uuid::Uuid;

use crate::contract::{AcceptanceContract, Clause, validate_source_path};
#[cfg(test)]
use crate::plan::evaluate_clause;
#[cfg(test)]
use crate::snapshot::WorkspaceSnapshot;

/// Compiles one free-text criterion to a [`Clause`].
///
/// Deterministic and total: structured forms that validate become
/// permissive clauses, everything else becomes [`Clause::Unresolved`]
/// carrying the original text.
#[must_use]
pub fn compile_criterion(criterion: &str) -> Clause {
    if let Some(path) = criterion.strip_prefix("file-unchanged:") {
        let path = path.trim();
        if !path.is_empty() && validate_source_path(path, false).is_ok() {
            return Clause::FileUnchanged { path: path.into() };
        }
    } else if let Some(raw) = criterion.strip_prefix("changed-within:") {
        let parts: Vec<&str> = raw.split(',').map(str::trim).collect();
        if !parts.is_empty()
            && parts.iter().all(|part| {
                !part.is_empty() && *part != "." && validate_source_path(part, true).is_ok()
            })
        {
            return Clause::ChangedPathsWithin {
                paths: parts.iter().map(ToString::to_string).collect(),
            };
        }
    }
    Clause::Unresolved {
        description: criterion.into(),
    }
}

/// Compiles every criterion in order; see [`compile_criterion`].
#[must_use]
pub fn compile_criteria(criteria: &[String]) -> Vec<Clause> {
    criteria.iter().map(|c| compile_criterion(c)).collect()
}

/// Deterministic hard-binding id for a constraint text: the first 16
/// bytes of its BLAKE3 digest. Same text always yields the same id, so
/// compilation is a pure function of the spec.
fn constraint_id(text: &str) -> Uuid {
    let digest = blake3::hash(text.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Compiles an [`IntentSpec`] to an [`AcceptanceContract`].
///
/// Spec constraints lead as [`Clause::HardConstraint`] bindings (each
/// wrapping [`Clause::Unresolved` — a text-only constraint authorizes
/// nothing on its own), followed by the compiled acceptance criteria in
/// order. Identical constraint texts collapse to one binding so the
/// contract's duplicate-id validation holds. Blank constraints are skipped
/// (intent validation already rejects them). The result always passes
/// [`AcceptanceContract::validate`], except the degenerate all-empty spec,
/// which compiles to zero clauses and fails validation fail-closed.
#[must_use]
pub fn compile_spec(spec: &IntentSpec) -> AcceptanceContract {
    let mut seen: Vec<String> = Vec::new();
    let mut clauses: Vec<Clause> = Vec::new();
    for text in &spec.constraints {
        // Trim-normalize so " x " and "x" collapse to one binding with one
        // id; blank constraints are skipped (intent validation rejects
        // them upstream).
        let normalized = text.trim();
        if normalized.is_empty() || seen.iter().any(|known| known == normalized) {
            continue;
        }
        seen.push(normalized.to_string());
        clauses.push(Clause::HardConstraint {
            id: constraint_id(normalized),
            text: normalized.to_string(),
            check: Box::new(Clause::Unresolved {
                description: normalized.to_string(),
            }),
        });
    }
    clauses.extend(compile_criteria(&spec.acceptance_criteria));
    AcceptanceContract { clauses }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with(constraints: &[&str], criteria: &[&str]) -> IntentSpec {
        IntentSpec {
            goal: "migrate the blog".into(),
            desired_outcome: "same content, new theme".into(),
            requirements: vec![],
            constraints: constraints.iter().map(ToString::to_string).collect(),
            preferences: vec![],
            non_goals: vec![],
            affected_surfaces: vec![],
            acceptance_criteria: criteria.iter().map(ToString::to_string).collect(),
            ambiguities: vec![],
            assumptions: vec![],
            evidence: vec![],
            confidence: 0.9,
        }
    }

    #[test]
    fn structured_file_unchanged_compiles() {
        assert_eq!(
            compile_criterion("file-unchanged: index.html"),
            Clause::FileUnchanged {
                path: "index.html".into(),
            }
        );
    }

    #[test]
    fn structured_changed_within_compiles() {
        assert_eq!(
            compile_criterion("changed-within: blog, assets"),
            Clause::ChangedPathsWithin {
                paths: vec!["blog".into(), "assets".into()],
            }
        );
    }

    #[test]
    fn free_text_becomes_unresolved() {
        let text = "take the site down briefly to migrate faster";
        assert_eq!(
            compile_criterion(text),
            Clause::Unresolved {
                description: text.into(),
            }
        );
    }

    #[test]
    fn blank_criterion_becomes_unresolved() {
        assert!(matches!(
            compile_criterion("   "),
            Clause::Unresolved { .. }
        ));
    }

    #[test]
    fn traversal_path_does_not_compile() {
        let text = "file-unchanged: ../../etc/passwd";
        assert_eq!(
            compile_criterion(text),
            Clause::Unresolved {
                description: text.into(),
            },
            "a path that fails source validation must not become FileUnchanged"
        );
    }

    #[test]
    fn lone_dot_changed_within_is_unresolved() {
        for text in ["changed-within: .", "changed-within: ., blog"] {
            assert!(
                matches!(compile_criterion(text), Clause::Unresolved { .. }),
                "{text:?} must not compile to allow-everything"
            );
        }
    }

    #[test]
    fn empty_segments_reject_the_criterion() {
        let text = "changed-within: blog,,assets";
        assert_eq!(
            compile_criterion(text),
            Clause::Unresolved {
                description: text.into(),
            },
            "empty segments fail strict, like file-unchanged"
        );
    }

    #[test]
    fn constraint_whitespace_normalizes_to_one_binding() {
        let contract = compile_spec(&spec_with(&[" no downtime ", "no downtime"], &[]));
        assert_eq!(contract.clauses.len(), 1);
        assert!(
            matches!(
                &contract.clauses[0],
                Clause::HardConstraint { text, .. } if text == "no downtime"
            ),
            "got: {:?}",
            contract.clauses[0]
        );
        contract.validate().expect("compiled contract validates");
    }

    #[test]
    fn command_like_text_never_compiles_to_effects_or_bindings() {
        // The Unverifiable arms for CommandPasses/HardConstraint in
        // conformance checking are provably unreachable-with-failure:
        // free text can only ever degrade to Unresolved or narrow
        // structural clauses, never authorize commands or bindings.
        for text in [
            "command: rm -rf /",
            "command-passes: cargo test",
            "run the tests",
            "CommandPasses { command }",
            "hard-constraint: no downtime",
            "HardConstraint { text }",
        ] {
            let clause = compile_criterion(text);
            assert!(
                matches!(
                    clause,
                    Clause::Unresolved { .. }
                        | Clause::FileUnchanged { .. }
                        | Clause::ChangedPathsWithin { .. }
                ),
                "{text:?} must never compile to commands or bindings, got {clause:?}"
            );
        }
    }

    #[test]
    fn compile_is_deterministic() {
        let spec = spec_with(
            &["no downtime during migration"],
            &["file-unchanged: index.html", "take the site down briefly"],
        );
        assert_eq!(compile_spec(&spec), compile_spec(&spec));
    }

    #[test]
    fn spec_constraints_become_hard_bindings() {
        let contract = compile_spec(&spec_with(&["no downtime during migration"], &[]));
        assert_eq!(contract.clauses.len(), 1);
        let (id, text) = match &contract.clauses[0] {
            Clause::HardConstraint { id, text, check } => {
                assert!(
                    matches!(check.as_ref(), Clause::Unresolved { .. }),
                    "a text-only constraint binds Unresolved, never a permissive check"
                );
                (*id, text.clone())
            }
            other => panic!("constraint must compile to HardConstraint, got {other:?}"),
        };
        assert_eq!(text, "no downtime during migration");
        let again = compile_spec(&spec_with(&["no downtime during migration"], &[]));
        let again_id = match &again.clauses[0] {
            Clause::HardConstraint { id, .. } => *id,
            other => panic!("expected HardConstraint, got {other:?}"),
        };
        assert_eq!(id, again_id, "hard binding ids must be deterministic");
        contract.validate().expect("compiled contract validates");
    }

    #[test]
    fn conflicting_inferred_criterion_loses() {
        // Behavioral half of #40(c): the hypothesis "take the site down"
        // contradicts the hard "no downtime" binding. It must survive only
        // as fail-closed Unresolved while the hard binding stands intact —
        // never as something that weakens or replaces it.
        let contract = compile_spec(&spec_with(
            &["no downtime during migration"],
            &["take the site down briefly", "file-unchanged: index.html"],
        ));
        assert_eq!(contract.clauses.len(), 3);
        assert!(
            matches!(&contract.clauses[0], Clause::HardConstraint { text, .. } if text == "no downtime during migration"),
            "hard binding leads, intact"
        );
        assert!(
            matches!(
                &contract.clauses[1],
                Clause::Unresolved { description } if description == "take the site down briefly"
            ),
            "conflicting hypothesis degrades to Unresolved, never permissive"
        );
        assert_eq!(
            contract.clauses[2],
            Clause::FileUnchanged {
                path: "index.html".into(),
            }
        );
        for clause in &contract.clauses {
            if let Clause::HardConstraint { .. } = clause {
                continue;
            }
            assert!(
                !matches!(clause, Clause::CommandPasses { .. }),
                "no compiled clause may authorize new effects"
            );
        }
        contract.validate().expect("compiled contract validates");
    }

    #[test]
    fn unresolved_in_compiled_contract_fails_evaluation() {
        // Fail-closed chain: compiled Unresolved must fail evaluation, so
        // an uncompilable criterion can never authorize completion.
        let dir = std::env::temp_dir().join(format!("tachyon-compile-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let baseline = WorkspaceSnapshot::capture(&dir).unwrap();
        let current = WorkspaceSnapshot::capture(&dir).unwrap();
        let contract = compile_spec(&spec_with(&[], &["redesign the logo a bit"]));
        assert!(matches!(&contract.clauses[..], [Clause::Unresolved { .. }]));
        let error = evaluate_clause(&contract.clauses[0], &baseline, &current)
            .expect_err("Unresolved must fail evaluation");
        assert!(error.contains("unresolved requirement"), "got: {error}");
    }

    #[test]
    fn empty_spec_compiles_to_empty_contract_that_fails_validation() {
        let contract = compile_spec(&spec_with(&[], &[]));
        assert!(contract.clauses.is_empty());
        assert!(
            contract.validate().is_err(),
            "an empty compiled contract must fail validation (fail closed)"
        );
    }
}
