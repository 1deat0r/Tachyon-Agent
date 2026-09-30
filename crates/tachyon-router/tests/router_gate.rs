//! Milestone 5 gate: simple routes never plan a model call; complex routes
//! still launch evidence first; telemetry records everything.

use tachyon_router::{GRACE_MS, RouteClass, Router, plan, requested_symbol};

#[test]
fn simple_lookup_never_plans_a_model() {
    let mut router = Router::new();
    for request in [
        "Where is refreshToken defined?",
        "Find references to SessionStore.",
        "Show git status.",
        "Run the tests.",
    ] {
        let task = tachyon_types::TaskId::generate();
        let plan = router.route(request);
        assert_eq!(plan.class, RouteClass::DirectNative, "{request}");
        assert!(!plan.requires_model(), "{request}");
        assert!(!plan.evidence.is_empty(), "{request}");
        // Lowers to real IR nodes owned by the task.
        let nodes = plan.initial_nodes(task);
        assert_eq!(nodes.len(), plan.evidence.len());
        assert!(nodes.iter().all(|node| node.task_id == task));
    }
}

#[test]
fn diagnosis_routes_evidence_first_with_grace() {
    let mut router = Router::new();
    let plan = router.route("Why is this test failing?");
    assert_eq!(plan.class, RouteClass::EvidenceFirst);
    assert!(!plan.requires_model());
    assert_eq!(plan.grace_ms, GRACE_MS);
    assert_eq!(plan.grace_ms, 75);
}

#[test]
fn hard_problems_plan_reasoning_but_evidence_first() {
    let mut router = Router::new();
    let plan = router.route("Find the root cause of this intermittent race and fix it.");
    assert_eq!(plan.class, RouteClass::ReasoningFirst);
    assert!(plan.requires_model());
    // Evidence still launches first: the plan is never evidence-free.
    assert!(!plan.evidence.is_empty());
    assert_eq!(plan.escalation.max_model_calls, 1);
}

#[test]
fn ambiguity_resolves_to_evidence_before_m7() {
    let mut router = Router::new();
    let plan = router.route("why is the redesign broken");
    // No rule fires strongly: JudgmentFirst label, but no judge exists, so
    // the executable plan is evidence-only.
    assert_eq!(plan.class, RouteClass::JudgmentFirst);
    assert!(!plan.requires_model());
    assert!(!plan.evidence.is_empty());
}

#[test]
fn estimates_price_evidence() {
    let mut router = Router::new();
    assert!((router.estimate("search.lexical") - 50.0).abs() < f64::EPSILON);
    router.observe_evidence_ms("search.lexical", 10.0);
    router.observe_evidence_ms("search.lexical", 10.0);
    let priced = router.route("Where is Foo defined?");
    let unpriced_estimate = 50.0;
    assert!(router.estimate("search.lexical") < unpriced_estimate);
    assert!(priced.predicted_evidence_ms > 0.0);
}

#[test]
fn serial_mode_orders_evidence() {
    let mut router = Router::new();
    let plan = router.route("Where is Foo defined?");
    assert!(!plan.serial);
    let serial = plan::serial(plan);
    assert!(serial.serial);
    assert_eq!(
        serial.evidence.len(),
        serial
            .initial_nodes(tachyon_types::TaskId::generate())
            .len()
    );
}

#[test]
fn telemetry_records_every_route() {
    let mut router = Router::new();
    let _ = router.route("Where is Foo defined?");
    let _ = router.route("Why is this test failing?");
    let telemetry = router.telemetry();
    assert_eq!(telemetry.latest("route.direct_native"), Some(50.0));
    assert!(telemetry.mean("route.evidence_first").is_some());
    let plan = router.route("Show git status.");
    let audit = Router::audit(&plan, &["show-status".to_owned()]);
    assert_eq!(audit.class, "direct_native");
    assert_eq!(audit.model_calls_planned, 0);
}

#[test]
fn symbol_binding_prefers_a_classifier_candidate() {
    assert_eq!(
        requested_symbol("Where is complete_refresh defined and used?").as_deref(),
        Some("complete_refresh")
    );
    assert_eq!(
        requested_symbol("Where is SessionStore defined?").as_deref(),
        Some("SessionStore")
    );
    // The first candidate wins: candidates are already ordered by the
    // text, so binding never jumps ahead of what the user named first.
    assert_eq!(
        requested_symbol("compare SessionStore and TokenStore").as_deref(),
        Some("SessionStore")
    );
}

#[test]
fn symbol_binding_falls_back_to_the_question_cue() {
    // No CamelCase, no snake_case: the classifier finds nothing, so the
    // cue after "where is" has to carry the symbol.
    assert_eq!(
        requested_symbol("where is serve defined").as_deref(),
        Some("serve")
    );
    assert_eq!(
        requested_symbol("Who calls flush?").as_deref(),
        Some("flush")
    );
    // Stopwords between the cue and the symbol are skipped.
    assert_eq!(
        requested_symbol("where is the parse function defined").as_deref(),
        Some("parse")
    );
}

#[test]
fn symbol_binding_refuses_to_invent_a_symbol() {
    assert_eq!(requested_symbol("where is it"), None);
    assert_eq!(requested_symbol(""), None);
    assert_eq!(requested_symbol("redesign the scheduler"), None);
    // Deterministic and pure: same text, same answer, every time.
    assert_eq!(
        requested_symbol("Where is serve defined"),
        requested_symbol("Where is serve defined")
    );
}
