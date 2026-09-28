//! M14 §44 benchmark matrix legs: two measured compositions over the
//! checked-in `fixtures/auth-refresh` corpus.
//!
//! - Class A: route a lookup request, then answer it from `tachyon_repo`
//!   (`definition_use` + lexical search) — zero model calls, zero judges.
//! - Class B: route an evidence-first question, collect evidence from the
//!   same fixture, assemble context, and make exactly one scripted
//!   reasoning call whose answer cites both competing implementations.
//!
//! Ignore-gated and meant for release mode; the matrix runs it with
//! `cargo test --release … --test matrix_legs -- --ignored --nocapture`.
//! stdout carries only the JSON result lines; diagnostics go to stderr.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tachyon_models::{
    AgentDecision, AssembleInput, FakeModelProvider, FakeResponse, ModelProvider, ModelRequest,
    Role, assemble,
};
use tachyon_repo::language::HeuristicBackend;
use tachyon_repo::{Inventory, SearchOptions, SymbolIndex, lexical_search};
use tachyon_retrieval::{EvidenceItem, EvidenceKind, EvidencePackage, Provenance};
use tachyon_router::{RouteClass, Router};
use tachyon_types::ProviderId;

const CORPUS: &str = "fixtures/auth-refresh";
const REQUEST_A: &str = "Where is complete_refresh defined and used?";
const SYMBOL: &str = "complete_refresh";
const REQUEST_B: &str =
    "Explain why auth-session/src/session.rs and auth-session/src/reference.rs behave differently";
const SESSION_PATH: &str = "auth-session/src/session.rs";
const REFERENCE_PATH: &str = "auth-session/src/reference.rs";
const SYSTEM_B: &str = "Explain behavior differences between implementations. Cite file paths.";
const ANSWER_B: &str = "\
auth-session/src/session.rs applies ticket.generation unconditionally in complete_refresh, so a \
stale completion overwrites a newer active_generation; auth-session/src/reference.rs only accepts \
an incoming generation strictly greater than the current one, rejecting stale and duplicate \
responses. The behavior difference is the missing greater-than comparison in session.rs.";

const WARMUP_A: usize = 10;
const SAMPLES_A: usize = 50;
const WARMUP_B: usize = 5;
const SAMPLES_B: usize = 20;
/// Inventory walk budget for the 15-file fixture corpus.
const FIXTURE_SCAN_LIMIT: usize = 10_000;
/// Evidence ranking weights: raw file excerpts outrank derived symbol rows.
const RELEVANCE_FILE_EXCERPT_PRIMARY: f32 = 0.95;
const RELEVANCE_FILE_EXCERPT_SECONDARY: f32 = 0.9;
/// Symbol definition/reference rows carry the provenance signal.
const RELEVANCE_SYMBOL_DEFINITION: f32 = 0.85;
const RELEVANCE_SYMBOL_REFERENCE: f32 = 0.8;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/auth-refresh")
}

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    (p50, p95)
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn class_a_step(
    router: &mut Router,
    fixture: &Path,
    inventory: &Inventory,
    index: &SymbolIndex,
) -> (usize, usize) {
    let plan = router.route(REQUEST_A);
    assert_eq!(plan.class, RouteClass::DirectNative, "class A route");
    assert!(!plan.requires_model(), "class A plans no model call");
    assert!(!plan.evidence.is_empty(), "class A still plans evidence");
    let answer = index.definition_use(SYMBOL);
    assert!(!answer.definitions.is_empty(), "{answer:?}");
    assert!(answer.references.len() >= 2, "{answer:?}");
    let hits = lexical_search(
        fixture,
        inventory,
        index.projection(),
        SYMBOL,
        &SearchOptions::default(),
    );
    assert!(!hits.is_empty(), "lexical search backs the answer");
    let structured = serde_json::json!({
        "symbol": answer.name,
        "definitions": answer.definitions,
        "references": answer.references,
        "lexical_hits": hits.len(),
    });
    let serialized = serde_json::to_string(&structured).expect("answer serializes");
    assert!(!serialized.is_empty());
    (answer.definitions.len(), answer.references.len())
}

#[test]
#[ignore = "M14 matrix leg: release mode, run with --ignored"]
fn class_a_zero_llm_repo_query() {
    let fixture = fixture_root();
    let inventory = Inventory::scan(&fixture, FIXTURE_SCAN_LIMIT).expect("fixture inventory");
    let mut index = SymbolIndex::new(&fixture, HeuristicBackend);
    index.build(&inventory);
    let corpus_files = inventory.files.len();
    let mut router = Router::new();
    let route_class = router.route(REQUEST_A).class.name();
    assert_eq!(route_class, "direct_native");

    for _ in 0..WARMUP_A {
        class_a_step(&mut router, &fixture, &inventory, &index);
    }

    let mut samples = Vec::with_capacity(SAMPLES_A);
    let (mut definitions, mut references) = (0, 0);
    for _ in 0..SAMPLES_A {
        let started = Instant::now();
        let counts = class_a_step(&mut router, &fixture, &inventory, &index);
        samples.push(started.elapsed());
        definitions = counts.0;
        references = counts.1;
    }

    let (p50, p95) = percentiles(samples);
    eprintln!(
        "matrix[leg.A] n={SAMPLES_A} p50={p50:?} p95={p95:?} \
         definitions={definitions} references={references}"
    );
    let line = serde_json::json!({
        "leg": "A",
        "request": REQUEST_A,
        "corpus": CORPUS,
        "corpus_files": corpus_files,
        "route_class": route_class,
        "n": SAMPLES_A,
        "p50_us": micros(p50),
        "p95_us": micros(p95),
        "model_calls": 0,
        "judgment_calls": 0,
        "definitions": definitions,
        "references": references,
        "verified": true,
        "zero_llm_basis": "plan.requires_model()==false asserted on every sample; no provider constructed",
    });
    println!("{line}");
}

async fn class_b_step(router: &mut Router, index: &SymbolIndex) {
    let plan = router.route(REQUEST_B);
    assert!(!plan.evidence.is_empty(), "evidence must launch first");
    assert!(plan.requires_model(), "leg B plans one reasoning call");

    let session = index
        .read_projected(SESSION_PATH)
        .expect("session.rs readable at the indexed generation");
    let reference = index
        .read_projected(REFERENCE_PATH)
        .expect("reference.rs readable at the indexed generation");
    let provenance = index.definition_use(SYMBOL);
    assert!(!provenance.definitions.is_empty(), "{provenance:?}");

    let mut package = EvidencePackage::new(REQUEST_B);
    package.findings.push(
        EvidenceItem::new(
            EvidenceKind::FileExcerpt,
            &session,
            Provenance::repo("fs.read", SESSION_PATH),
        )
        .with_relevance(RELEVANCE_FILE_EXCERPT_PRIMARY),
    );
    package.findings.push(
        EvidenceItem::new(
            EvidenceKind::FileExcerpt,
            &reference,
            Provenance::repo("fs.read", REFERENCE_PATH),
        )
        .with_relevance(RELEVANCE_FILE_EXCERPT_SECONDARY),
    );
    for location in &provenance.definitions {
        package.findings.push(
            EvidenceItem::new(
                EvidenceKind::SymbolDefinition,
                &location.excerpt,
                Provenance::repo("repo.symbol.search", &location.file),
            )
            .with_relevance(RELEVANCE_SYMBOL_DEFINITION),
        );
    }
    for location in &provenance.references {
        package.findings.push(
            EvidenceItem::new(
                EvidenceKind::SymbolReference,
                &location.excerpt,
                Provenance::repo("repo.symbol.search", &location.file),
            )
            .with_relevance(RELEVANCE_SYMBOL_REFERENCE),
        );
    }

    let input = AssembleInput {
        system_prompt: SYSTEM_B,
        objective: REQUEST_B,
        constraints: &[],
        evidence: &package,
        history: &[],
        total_budget_tokens: 8_192,
        output_budget_tokens: 1_024,
    };
    let context = assemble(&input);
    for path in [SESSION_PATH, REFERENCE_PATH] {
        let label = format!("fs.read:{path}");
        assert!(
            context.iter().any(|block| block.provenance == label),
            "evidence for {path} must reach the model context"
        );
    }

    let provider = FakeModelProvider::new(ProviderId("matrix-leg-b".to_owned()));
    provider.push_response(FakeResponse::respond(ANSWER_B));
    let (sink, _events) = tokio::sync::mpsc::unbounded_channel();
    let request = ModelRequest {
        role: Role::Primary,
        model: "fake-1".to_owned(),
        context,
        max_output_tokens: 512,
        require_structured_output: false,
    };
    let result = provider
        .invoke(request, sink)
        .await
        .expect("scripted leg B");
    assert_eq!(provider.request_count(), 1, "exactly one reasoning call");
    match result.decision {
        AgentDecision::Respond { message } => {
            assert!(message.contains(SESSION_PATH), "{message}");
            assert!(message.contains(REFERENCE_PATH), "{message}");
        }
        other => panic!("leg B must respond, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "M14 matrix leg: release mode, run with --ignored"]
async fn class_b_evidence_first_one_call() {
    let fixture = fixture_root();
    let inventory = Inventory::scan(&fixture, FIXTURE_SCAN_LIMIT).expect("fixture inventory");
    let mut index = SymbolIndex::new(&fixture, HeuristicBackend);
    index.build(&inventory);
    let mut router = Router::new();

    let plan = router.route(REQUEST_B);
    assert!(!plan.evidence.is_empty(), "evidence-first plan");
    assert!(plan.requires_model(), "leg B plans a model call");
    let route_class = plan.class.name();

    for _ in 0..WARMUP_B {
        class_b_step(&mut router, &index).await;
    }

    let mut samples = Vec::with_capacity(SAMPLES_B);
    for _ in 0..SAMPLES_B {
        let started = Instant::now();
        class_b_step(&mut router, &index).await;
        samples.push(started.elapsed());
    }

    let (p50, p95) = percentiles(samples);
    eprintln!("matrix[leg.B] n={SAMPLES_B} p50={p50:?} p95={p95:?} route_class={route_class}");
    let line = serde_json::json!({
        "leg": "B",
        "request": REQUEST_B,
        "corpus": CORPUS,
        "route_class": route_class,
        "evidence_first": true,
        "n": SAMPLES_B,
        "p50_us": micros(p50),
        "p95_us": micros(p95),
        "model_calls": 1,
        "judgment_calls": 0,
        "answer_cites_both": true,
        "verified": true,
    });
    println!("{line}");
}
