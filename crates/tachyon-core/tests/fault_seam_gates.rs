//! M12 six-domain fault-seam gates: every named seam exists, is inert
//! unless armed, and is reachable from this process's default env.
//!
//! Domains: native reads · model/Jev · mutation · verifier · approval ·
//! effect (effect §19 matrix lives in `effect_fixture.rs`).

use tachyon_tools::fault::{is_armed, reach, reach_blocking};

/// Native reads: evidence.read seam.
#[tokio::test]
async fn native_read_gates_evidence_seam_inert_unless_armed() {
    assert!(!is_armed("evidence.read"));
    tokio::time::timeout(std::time::Duration::from_millis(50), reach("evidence.read"))
        .await
        .expect("unarmed evidence.read must not park");
}

/// Model/Jev: model.enter seam.
#[tokio::test]
async fn model_call_gates_model_enter_seam_inert_unless_armed() {
    assert!(!is_armed("model.enter"));
    tokio::time::timeout(std::time::Duration::from_millis(50), reach("model.enter"))
        .await
        .expect("unarmed model.enter must not park");
}

/// Mutation: mutation.commit seam (blocking).
#[test]
fn mutation_gates_commit_seam_inert_unless_armed() {
    assert!(!is_armed("mutation.commit"));
    reach_blocking("mutation.commit");
}

/// Verifier: verify.command seam.
#[tokio::test]
async fn verifier_gates_command_seam_inert_unless_armed() {
    assert!(!is_armed("verify.command"));
    tokio::time::timeout(
        std::time::Duration::from_millis(50),
        reach("verify.command"),
    )
    .await
    .expect("unarmed verify.command must not park");
}

/// Approval wait: approval.park seam.
#[tokio::test]
async fn approval_gates_park_seam_inert_unless_armed() {
    assert!(!is_armed("approval.park"));
    tokio::time::timeout(std::time::Duration::from_millis(50), reach("approval.park"))
        .await
        .expect("unarmed approval.park must not park");
}

/// Effect protocol seams are inert unless explicitly armed.
#[tokio::test]
async fn effect_barrier_seams_are_inert_unless_armed() {
    for seam in [
        "effect.prepared",
        "effect.remote_return",
        "effect.committed",
    ] {
        assert!(!is_armed(seam), "default env must leave {seam} inert");
        tokio::time::timeout(std::time::Duration::from_millis(50), reach(seam))
            .await
            .unwrap_or_else(|_| panic!("unarmed {seam} must not park"));
    }
}
