//! M10 runtime slice RED: stage compiler, bounds, freshness, proposal gates.
//! Fails until `tachyon_core::runtime` exists with the pinned API.
use tachyon_core::runtime::{
    BoundContract, EvidenceItem, EvidenceRequest, HardBinding, ModelProposal, MutationIntent,
    MutationOutcome, ProposedFile, RetryBudget, RunMeasurements, RuntimeBounds,
    SelectionResolution, StageStatus, SteeringState, TokenProvenance, bind_contract, blocked_kind,
    collect_evidence, compile_evidence_graph, compile_operation, complete_grants_success,
    gate_proposal_writes, hash_bytes, is_protected_path, load_intent, lower_router_placeholders,
    manifest_of, mark_recovering, max_overlap, parse_proposal, persist_intent,
    resolve_check_selection, verify_manifest_freshness,
};
use tachyon_policy::Policy;
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::TaskId;

fn ctx(root: &std::path::Path) -> ToolsContext {
    ToolsContext::new(
        root.to_path_buf(),
        Policy::trusted_workspace(),
        ArtifactSpool::new(root.join("artifacts")),
    )
}

#[test]
fn evidence_ir_compiles_with_real_declarations() {
    let task = TaskId::generate();
    let graph = compile_evidence_graph(
        task,
        0,
        &[
            EvidenceRequest {
                capability: "fs.read".into(),
                path: "src/a.rs".into(),
            },
            EvidenceRequest {
                capability: "fs.read".into(),
                path: "src/b.rs".into(),
            },
        ],
        &RuntimeBounds::default(),
    )
    .expect("compile");
    assert_eq!(graph.nodes.len(), 2);
    for node in graph.nodes.values() {
        assert!(!node.access.reads.is_empty(), "real access declarations");
        assert!(node.resources.cpu_units > 0, "real resource claim");
    }
    graph.validate(task).expect("valid IR");
}

#[test]
fn router_placeholders_are_lowered_and_revalidated() {
    let task = TaskId::generate();
    let graph = compile_evidence_graph(
        task,
        0,
        &[EvidenceRequest {
            capability: "fs.read".into(),
            path: "src/a.rs".into(),
        }],
        &RuntimeBounds::default(),
    )
    .expect("compile");
    // Simulate an M5 router placeholder: strip access declarations.
    let mut hollow = graph.clone();
    for node in hollow.nodes.values_mut() {
        node.access.reads.clear();
        node.access.writes.clear();
    }
    let lowered = lower_router_placeholders(hollow, task, 0).expect("lower");
    for node in lowered.nodes.values() {
        assert!(!node.access.reads.is_empty(), "placeholder lowered");
    }
    lowered.validate(task).expect("revalidated");
}

#[test]
fn unknown_capability_untyped_args_and_shell_fail_closed() {
    let task = TaskId::generate();
    assert!(compile_operation(task, 0, "shell.exec", &serde_json::json!({"cmd": "rm"})).is_err());
    assert!(compile_operation(task, 0, "evil.custom", &serde_json::json!({"path": "x"})).is_err());
    assert!(compile_operation(task, 0, "fs.read", &serde_json::json!("src/a.rs")).is_err());
    assert!(
        compile_operation(
            task,
            0,
            "fs.read",
            &serde_json::json!({"path": "src/a.rs", "access": ["all"]})
        )
        .is_err(),
        "model-supplied access metadata fails closed"
    );
    assert!(
        compile_operation(
            task,
            0,
            "credential.use",
            &serde_json::json!({"handle": "x"})
        )
        .is_err()
    );
}

#[test]
fn bounds_reject_excess_and_oversize_before_allocation() {
    let task = TaskId::generate();
    let bounds = RuntimeBounds {
        max_evidence_requests: 1,
        ..RuntimeBounds::default()
    };
    let reqs = vec![
        EvidenceRequest {
            capability: "fs.read".into(),
            path: "a".into(),
        },
        EvidenceRequest {
            capability: "fs.read".into(),
            path: "b".into(),
        },
    ];
    assert!(compile_evidence_graph(task, 0, &reqs, &bounds).is_err());
    let big = serde_json::json!({
        "path": "a.rs", "base_hash": "h",
        "new_content": "x".repeat(2 * 1024 * 1024)
    });
    assert!(
        parse_proposal(
            &serde_json::json!({"decision": "propose_execution",
        "operations": [{"capability": "mutation.patch", "args": big, "reason": "x"}]}),
            &RuntimeBounds::default()
        )
        .is_err()
    );
    let empty = serde_json::json!({"decision": "propose_execution", "operations": []});
    assert!(parse_proposal(&empty, &RuntimeBounds::default()).is_err());
}

#[test]
fn complete_is_never_completion_authority_but_blocked_outcomes_are_durable() {
    assert!(!complete_grants_success());
    let bounds = RuntimeBounds::default();
    let complete = parse_proposal(
        &serde_json::json!({"decision": "complete", "summary": "done"}),
        &bounds,
    )
    .expect("parses");
    assert!(matches!(complete, ModelProposal::Complete { .. }));
    let ev = parse_proposal(
        &serde_json::json!({"decision": "request_evidence",
            "requests": [{"capability": "fs.read", "args": {"path": "a"}}]}),
        &bounds,
    )
    .expect("parses");
    assert!(
        blocked_kind(&ev).is_some(),
        "RequestEvidence yields durable blocked"
    );
    let need = parse_proposal(
        &serde_json::json!({"decision": "need_user_input", "question": "which?"}),
        &bounds,
    )
    .expect("parses");
    assert!(blocked_kind(&need).is_some());
    assert!(blocked_kind(&complete).is_none());
}

#[test]
fn exactly_one_bounded_retry() {
    let mut budget = RetryBudget::new();
    assert!(budget.take_retry(), "first retry allowed");
    assert!(!budget.take_retry(), "no unbounded loop");
    assert!(!budget.take_retry());
}

#[test]
fn real_evidence_reads_and_freshness_recheck() {
    let root = std::env::temp_dir().join(format!("tachyon-m10-ev-{}", uuid::Uuid::now_v7()));
    let ws = root.join("ws");
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::write(ws.join("src/a.rs"), b"fn a() {}").unwrap();
    let context = ctx(&ws);
    let bounds = RuntimeBounds::default();
    let items = collect_evidence(
        &context,
        &[EvidenceRequest {
            capability: "fs.read".into(),
            path: "src/a.rs".into(),
        }],
        &bounds,
    )
    .expect("read");
    assert_eq!(items.len(), 1);
    let manifest = manifest_of(&items);
    // Fresh: same hashes pass.
    let current: Vec<(String, String)> = items
        .iter()
        .map(|i| (i.path.clone(), i.hash.clone()))
        .collect();
    verify_manifest_freshness(&manifest, &current).expect("fresh");
    // Changed non-target evidence invalidates everything.
    let mut stale = current.clone();
    stale[0].1 = "deadbeef".into();
    assert!(verify_manifest_freshness(&manifest, &stale).is_err());
    // Missing member invalidates.
    assert!(verify_manifest_freshness(&manifest, &[]).is_err());
    // base_hash binds the supplied version, never a substitution.
    let file = ProposedFile {
        path: "src/a.rs".into(),
        base_hash: Some("fresh-substitution".into()),
        new_content: b"x".to_vec(),
    };
    let bound = bind_test_contract();
    assert!(
        gate_proposal_writes(&bound, &[file], &manifest, &[]).is_err(),
        "stale base_hash refused"
    );
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn denied_evidence_path_refuses_with_zero_reads() {
    let root = std::env::temp_dir().join(format!("tachyon-m10-deny-{}", uuid::Uuid::now_v7()));
    let ws = root.join("ws");
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::write(ws.join("src/secret.rs"), b"secret").unwrap();
    let mut policy = Policy::trusted_workspace();
    policy.deny("fs.read", "workspace/src/secret.rs");
    let context = ToolsContext::new(ws, policy, ArtifactSpool::new(root.join("art")));
    let err = collect_evidence(
        &context,
        &[EvidenceRequest {
            capability: "fs.read".into(),
            path: "src/secret.rs".into(),
        }],
        &RuntimeBounds::default(),
    )
    .expect_err("denied read must fail");
    assert!(err.to_string().contains("denied") || err.to_string().contains("Denied"));
    std::fs::remove_dir_all(&root).unwrap();
}

fn bind_test_contract() -> BoundContract {
    use tachyon_verify::{AcceptanceContract, Clause};
    bind_contract(
        AcceptanceContract {
            clauses: vec![Clause::ChangedPathsWithin {
                paths: vec!["src/a.rs".into()],
            }],
        },
        3,
    )
}

#[test]
fn hard_constraints_gate_writes_before_mutation() {
    let bound = bind_test_contract();
    let manifest_items = vec![EvidenceItem {
        path: "src/a.rs".into(),
        hash: hash_bytes(b"fn a() {}"),
        bytes: b"fn a() {}".to_vec(),
    }];
    let manifest = manifest_of(&manifest_items);
    let good = ProposedFile {
        path: "src/a.rs".into(),
        base_hash: Some(manifest_items[0].hash.clone()),
        new_content: b"fn a() { 1 }".to_vec(),
    };
    gate_proposal_writes(&bound, &[good], &manifest, &[]).expect("in-scope write passes");
    // Outside ChangedPathsWithin: refused before mutation.
    let out = ProposedFile {
        path: "src/other.rs".into(),
        base_hash: Some("h".into()),
        new_content: b"x".to_vec(),
    };
    let manifest2 = manifest_of(&[EvidenceItem {
        path: "src/other.rs".into(),
        hash: "h".into(),
        bytes: b"x".to_vec(),
    }]);
    assert!(gate_proposal_writes(&bound, &[out], &manifest2, &[]).is_err());
    // Migration write refused.
    assert!(is_protected_path("migrations/001.sql"));
    let mig = ProposedFile {
        path: "migrations/001.sql".into(),
        base_hash: Some("h".into()),
        new_content: b"x".to_vec(),
    };
    let manifest3 = manifest_of(&[EvidenceItem {
        path: "migrations/001.sql".into(),
        hash: "h".into(),
        bytes: b"x".to_vec(),
    }]);
    assert!(gate_proposal_writes(&bound, &[mig], &manifest3, &[]).is_err());
    // Escaped path refused.
    let esc = ProposedFile {
        path: "../outside.rs".into(),
        base_hash: Some("h".into()),
        new_content: b"x".to_vec(),
    };
    assert!(gate_proposal_writes(&bound, &[esc], &manifest3, &[]).is_err());
    // New unbound hard constraint stops writes fail-closed.
    let unbound = HardBinding {
        id: "new-rule".into(),
        bound_check: None,
    };
    let good2 = ProposedFile {
        path: "src/a.rs".into(),
        base_hash: Some(manifest_items[0].hash.clone()),
        new_content: b"fn a() { 2 }".to_vec(),
    };
    assert!(gate_proposal_writes(&bound, &[good2], &manifest, &[unbound]).is_err());
}

#[test]
fn every_hard_binding_is_enforced_not_just_the_first() {
    // Regression: the gate once checked only extra_hard[0], so a write
    // inside a broad first binding but outside a narrower second one passed.
    // Bindings conjoin: the write must satisfy each of them.
    use tachyon_verify::{AcceptanceContract, Clause};
    let bound = bind_contract(
        AcceptanceContract {
            clauses: vec![Clause::ChangedPathsWithin {
                paths: vec!["src/".into()],
            }],
        },
        3,
    );
    let items = vec![
        EvidenceItem {
            path: "src/one.rs".into(),
            hash: hash_bytes(b"one"),
            bytes: b"one".to_vec(),
        },
        EvidenceItem {
            path: "src/two.rs".into(),
            hash: hash_bytes(b"two"),
            bytes: b"two".to_vec(),
        },
    ];
    let manifest = manifest_of(&items);
    let bindings = vec![
        HardBinding {
            id: "h1".into(),
            bound_check: Some("src/".into()),
        },
        HardBinding {
            id: "h2".into(),
            bound_check: Some("src/one.rs".into()),
        },
    ];
    // Inside both bindings: passes.
    let in_both = ProposedFile {
        path: "src/one.rs".into(),
        base_hash: Some(items[0].hash.clone()),
        new_content: b"one!".to_vec(),
    };
    gate_proposal_writes(&bound, &[in_both], &manifest, &bindings)
        .expect("write inside every bound scope passes");
    // Inside h1 but outside h2: refused (the old bypass shape).
    let outside_second = ProposedFile {
        path: "src/two.rs".into(),
        base_hash: Some(items[1].hash.clone()),
        new_content: b"two!".to_vec(),
    };
    assert!(
        gate_proposal_writes(&bound, &[outside_second], &manifest, &bindings).is_err(),
        "write outside any one bound scope must be refused"
    );
}

#[test]
fn hard_constraint_path_scope_is_conjoined_with_the_acceptance_scope() {
    use tachyon_verify::{AcceptanceContract, Clause};
    let items = vec![
        EvidenceItem {
            path: "src/one.rs".into(),
            hash: hash_bytes(b"one"),
            bytes: b"one".to_vec(),
        },
        EvidenceItem {
            path: "src/two.rs".into(),
            hash: hash_bytes(b"two"),
            bytes: b"two".to_vec(),
        },
    ];
    let manifest = manifest_of(&items);
    let bound = bind_contract(
        AcceptanceContract {
            clauses: vec![
                Clause::ChangedPathsWithin {
                    paths: vec!["src/".into()],
                },
                Clause::HardConstraint {
                    id: uuid::Uuid::now_v7(),
                    text: "only edit one.rs".into(),
                    check: Box::new(Clause::ChangedPathsWithin {
                        paths: vec!["src/one.rs".into()],
                    }),
                },
            ],
        },
        3,
    );

    let inside_both = ProposedFile {
        path: "src/one.rs".into(),
        base_hash: Some(items[0].hash.clone()),
        new_content: b"one!".to_vec(),
    };
    gate_proposal_writes(&bound, &[inside_both], &manifest, &[])
        .expect("write inside acceptance and hard scopes passes");

    let inside_acceptance_only = ProposedFile {
        path: "src/two.rs".into(),
        base_hash: Some(items[1].hash.clone()),
        new_content: b"two!".to_vec(),
    };
    assert!(
        gate_proposal_writes(&bound, &[inside_acceptance_only], &manifest, &[]).is_err(),
        "broad acceptance scope must not widen a narrower hard constraint"
    );
}

#[test]
fn scopeless_contract_refuses_writes_fail_closed() {
    // A writes-capable contract with no path clause (only CommandPasses)
    // must not silently authorize any non-protected path.
    use tachyon_verify::{AcceptanceContract, Clause};
    let bound = bind_contract(
        AcceptanceContract {
            clauses: vec![Clause::CommandPasses {
                command: tachyon_verify::CommandCheck {
                    program: "cargo".into(),
                    args: vec!["test".into()],
                    cwd: ".".into(),
                    env: std::collections::BTreeMap::default(),
                    timeout_ms: 60_000,
                },
            }],
        },
        3,
    );
    let manifest = manifest_of(&[EvidenceItem {
        path: "src/any.rs".into(),
        hash: "h".into(),
        bytes: b"x".to_vec(),
    }]);
    let file = ProposedFile {
        path: "src/any.rs".into(),
        base_hash: Some("h".into()),
        new_content: b"y".to_vec(),
    };
    assert!(
        gate_proposal_writes(&bound, &[file], &manifest, &[]).is_err(),
        "no bound write scope must refuse the proposal"
    );
}

#[test]
fn measurements_overlap_serial_and_outcome_authority() {
    // IDENTITY BOUNDARY PIN: evidence freshness tokens are FNV-1a, never
    // M8/artifact BLAKE3 identity. A byte string's freshness token must not
    // equal its content-identity hash, so crossing the two is observable.
    assert_ne!(
        hash_bytes(b"boundary"),
        tachyon_mutation::blake3_hex(b"boundary")
    );
    assert_eq!(max_overlap(&[(0, 10), (5, 15), (20, 30)]), 2);
    assert_eq!(max_overlap(&[(0, 10), (10, 20)]), 1, "touching is serial");
    assert_eq!(max_overlap(&[]), 0);
    assert!(MutationOutcome::Committed.is_success());
    assert!(!MutationOutcome::Compensated.is_success());
    assert!(!MutationOutcome::ProvenNoEffect.is_success());
    assert!(!MutationOutcome::Unknown.is_success());
    let m = RunMeasurements::default();
    let v = serde_json::to_value(&m).expect("serializes");
    assert!(v.get("billed_tokens").is_some());
    assert!(v.get("usage_provenance").is_some());
    let _ = TokenProvenance::ProviderReported;
}

#[test]
fn intent_persisted_before_prepare_and_stage_never_completes_on_crash() {
    let dir = std::env::temp_dir().join(format!("tachyon-m10-intent-{}", uuid::Uuid::now_v7()));
    let file = ProposedFile {
        path: "src/a.rs".into(),
        base_hash: Some("h".into()),
        new_content: b"x".to_vec(),
    };
    let intent =
        MutationIntent::authorized("batch-1", std::slice::from_ref(&file)).expect("intent");
    persist_intent(&dir, "batch-1", &intent).expect("persist");
    let loaded = load_intent(&dir, "batch-1").expect("load");
    assert_eq!(loaded, intent);
    // A caller cannot smuggle provider config or secrets through intent:
    // the schema carries only batch identity, paths and base hashes.
    let raw = std::fs::read_to_string(dir.join("intent-batch-1.json")).expect("raw");
    assert!(!raw.contains("secret") && !raw.contains("provider"));
    assert_eq!(
        mark_recovering(StageStatus::InFlight),
        StageStatus::Recovering
    );
    assert_ne!(
        mark_recovering(StageStatus::InFlight),
        StageStatus::Completed
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn steering_bumps_revision_and_discards_stale_proposals() {
    let mut steering = SteeringState::new();
    assert!(!steering.should_discard(0));
    steering.note_steering();
    assert_eq!(steering.revision(), 1);
    assert!(steering.should_discard(0), "old proposal discarded");
    assert!(!steering.should_discard(1));
    steering.cancel();
    assert!(steering.cancelled());
}

#[test]
fn renamed_dependency_resolves_or_broadens_never_ignores() {
    // alpha -> client counterexample: renamed dependent must not be ignored.
    let available = vec!["client:preserves_client_contract".to_string()];
    match resolve_check_selection("alpha", &available) {
        SelectionResolution::Exact(hit) => assert!(hit.contains("client")),
        SelectionResolution::BroadenedWorkspace => {}
        SelectionResolution::Ignored => panic!("must never silently ignore aliases"),
    }
    match resolve_check_selection("missing-check", &available) {
        SelectionResolution::BroadenedWorkspace => {}
        other => panic!("unknown deps broaden conservatively, got {other:?}"),
    }
    match resolve_check_selection("client:preserves_client_contract", &available) {
        SelectionResolution::Exact(_) => {}
        other => panic!("exact names resolve exactly, got {other:?}"),
    }
}
