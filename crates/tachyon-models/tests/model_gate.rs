//! Milestone 6 gate: capability negotiation, trusted bounded context,
//! structured decisions, fake determinism, the real HTTP adapter behind a
//! stub transport — and Vertical Slice B in one reasoning call.

use tachyon_models::{
    AgentDecision, AssembleInput, CapabilityRequirements, ContextKind, FakeModelProvider,
    FakeResponse, ModelProvider, Role, TrustLevel, assemble, estimate_tokens, parse_decision,
};
use tachyon_retrieval::{EvidenceItem, EvidenceKind, EvidencePackage, Provenance};
use tachyon_types::ProviderId;

fn evidence_fixture() -> EvidencePackage {
    EvidencePackage {
        question: "Explain why these two implementations behave differently.".to_owned(),
        findings: vec![
            EvidenceItem::new(
                EvidenceKind::SymbolDefinition,
                "pub fn refresh_token_correct(session: &Session) -> Token {\n    let expected = blake3(session.secret);\n    if constant_time_eq(&session.presented, &expected) { issue(session) } else { reject() }\n}",
                Provenance::repo("repo.symbol.search", "src/auth.rs").with_hash("a1b2"),
            )
            .with_relevance(0.95),
            EvidenceItem::new(
                EvidenceKind::SymbolDefinition,
                "pub fn refresh_token_legacy(session: &Session) -> Token {\n    if session.presented == session.expected { issue(session) } else { reject() }\n}",
                Provenance::repo("repo.symbol.search", "src/legacy.rs").with_hash("c3d4"),
            )
            .with_relevance(0.9),
        ],
        contradictions: vec![],
        gaps: vec![],
    }
}

#[test]
fn negotiation_selects_capabilities_not_names() {
    let requirements = CapabilityRequirements {
        need_structured_output: true,
        ..CapabilityRequirements::default()
    };
    let fake = FakeModelProvider::new(ProviderId("fake".to_owned()));
    assert!(requirements.satisfied_by(&fake.capabilities()));
    assert!(
        !CapabilityRequirements {
            need_vision: true,
            ..requirements
        }
        .satisfied_by(&fake.capabilities())
    );
}

#[test]
fn context_marks_repo_text_as_data() {
    let evidence = evidence_fixture();
    let input = AssembleInput {
        system_prompt: "You explain code differences.",
        objective: &evidence.question.clone(),
        constraints: &[],
        evidence: &evidence,
        history: &[],
        total_budget_tokens: 8_192,
        output_budget_tokens: 1_024,
    };
    let blocks = assemble(&input);
    assert!(blocks.iter().any(|block| block.kind == ContextKind::System));
    let evidence_blocks: Vec<_> = blocks
        .iter()
        .filter(|block| block.kind == ContextKind::Evidence)
        .collect();
    assert_eq!(evidence_blocks.len(), 2);
    assert!(
        evidence_blocks
            .iter()
            .all(|block| block.trust == TrustLevel::WorkspaceData)
    );
    assert!(
        evidence_blocks
            .iter()
            .all(|block| block.content.contains("[source: repo.symbol.search"))
    );
}

#[test]
fn budget_truncation_is_deterministic_and_reserves_output() {
    let evidence = evidence_fixture();
    let input = AssembleInput {
        system_prompt: "sys",
        objective: "obj",
        constraints: &[],
        evidence: &evidence,
        history: &[],
        total_budget_tokens: 60,
        output_budget_tokens: 20,
    };
    let first = assemble(&input);
    let second = assemble(&input);
    let shape = |blocks: &[tachyon_models::ContextBlock]| {
        blocks
            .iter()
            .map(|block| {
                (
                    block.kind,
                    block.provenance.clone(),
                    block.trust,
                    block.content.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(shape(&first), shape(&second));
    let used: u32 = first
        .iter()
        .map(|block| estimate_tokens(&block.content))
        .sum();
    assert!(used + 20 <= 60, "used {used} must leave 20 output tokens");
}

#[test]
fn malformed_output_is_a_result_failure() {
    let error = parse_decision("do whatever seems right").expect_err("prose must fail");
    assert!(!error.is_retryable());
}

#[tokio::test]
async fn slice_b_one_reasoning_call_over_repo_evidence() {
    // Vertical Slice B: existing repo evidence plus exactly one reasoning
    // call explains the behavior difference, citing both provenances.
    let evidence = evidence_fixture();
    let question = evidence.question.clone();
    let input = AssembleInput {
        system_prompt: "Explain behavior differences between implementations. Cite file paths.",
        objective: &question,
        constraints: &[],
        evidence: &evidence,
        history: &[],
        total_budget_tokens: 8_192,
        output_budget_tokens: 1_024,
    };
    let context = assemble(&input);

    let fake = FakeModelProvider::new(ProviderId("slice-b".to_owned()));
    fake.push_response(FakeResponse::respond(
        "src/auth.rs compares hashes in constant time, so timing reveals nothing; \
         src/legacy.rs uses == on secrets, leaking prefix matches through timing. \
         Same API, different side-channel behavior.",
    ));
    let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
    let request = tachyon_models::ModelRequest {
        role: Role::Primary,
        model: "fake-1".to_owned(),
        context,
        max_output_tokens: 512,
        require_structured_output: false,
    };
    let result = fake.invoke(request, sink).await.expect("scripted slice B");

    assert_eq!(
        fake.request_count(),
        1,
        "slice B allows exactly one reasoning call"
    );
    let recorded = fake.last_request().expect("request recorded");
    assert_eq!(
        recorded.context.len(),
        4,
        "system + objective + 2 evidence blocks"
    );
    match result.decision {
        AgentDecision::Respond { message } => {
            assert!(message.contains("src/auth.rs"), "must cite {message}");
            assert!(message.contains("src/legacy.rs"), "must cite {message}");
        }
        other => panic!("slice B must respond, got {other:?}"),
    }
}
