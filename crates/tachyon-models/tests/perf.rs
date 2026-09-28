//! M13 `comp[model_wait]`: model-call harness overhead with a scripted fake
//! provider (report-only, no §43 number).
//!
//! Real model wait is provider/network dominated and must not be claimed
//! from this machine. What Tachyon owns is the harness around the call:
//! trusted context assembly (budget fitting, trust marking, provenance)
//! plus provider dispatch and decision handling. Both halves are measured
//! separately over a representative evidence package. Ignore-gated; the
//! M13 ledger runs it with `cargo test --release … -- --ignored --nocapture`.

use std::time::{Duration, Instant};

use tachyon_models::{
    AgentDecision, AssembleInput, ContextKind, FakeModelProvider, FakeResponse, ModelProvider,
    ModelRequest, Role, TrustLevel, assemble,
};
use tachyon_retrieval::{EvidenceItem, EvidenceKind, EvidencePackage, Provenance};
use tachyon_types::ProviderId;

const WARMUP: usize = 10;
const SAMPLES: usize = 100;

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

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    (p50, p95)
}

#[tokio::test]
#[ignore = "M13 perf component: release mode, run with --ignored"]
async fn comp_model_wait_harness_overhead() {
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

    // Warmup + sanity: assembly is deterministic and marks repo text as data.
    for _ in 0..WARMUP {
        let blocks = assemble(&input);
        assert!(blocks.iter().any(|block| block.kind == ContextKind::System));
        assert!(
            blocks
                .iter()
                .filter(|block| block.kind == ContextKind::Evidence)
                .all(|block| block.trust == TrustLevel::WorkspaceData)
        );
    }

    let mut assemble_latencies = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let context = assemble(&input);
        assemble_latencies.push(start.elapsed());
        assert!(!context.is_empty());
    }

    let fake = FakeModelProvider::new(ProviderId("m13-wait".to_owned()));
    for _ in 0..(WARMUP + SAMPLES) {
        fake.push_response(FakeResponse::respond("scripted reply"));
    }
    let (sink, mut events) = tokio::sync::mpsc::unbounded_channel();

    let mut invoke_latencies = Vec::with_capacity(SAMPLES);
    for _ in 0..(WARMUP + SAMPLES) {
        let context = assemble(&input);
        let request = ModelRequest {
            role: Role::Primary,
            model: "fake-1".to_owned(),
            context,
            max_output_tokens: 512,
            require_structured_output: false,
        };
        let start = Instant::now();
        let result = fake.invoke(request, sink.clone()).await.expect("scripted");
        invoke_latencies.push(start.elapsed());
        assert!(
            matches!(result.decision, AgentDecision::Respond { .. }),
            "scripted response stays a plain respond"
        );
    }
    while events.try_recv().is_ok() {}
    assert_eq!(fake.request_count(), WARMUP + SAMPLES);

    let (assemble_p50, assemble_p95) = percentiles(assemble_latencies);
    let (invoke_p50, invoke_p95) = percentiles(invoke_latencies);
    println!(
        "comp[model_wait.assemble] n={SAMPLES} p50={assemble_p50:?} p95={assemble_p95:?} (trusted context assembly)"
    );
    println!(
        "comp[model_wait.invoke_fake] n={SAMPLES} p50={invoke_p50:?} p95={invoke_p95:?} (fake dispatch, no network)"
    );
}
