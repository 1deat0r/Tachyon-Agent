#![cfg(unix)]
//! Advisory intent-conformance checks after verification (ADR 0004, slice 3,
//! issue #42): the verification gate decides completion; this report only
//! observes whether the human objective was met.

use std::collections::BTreeMap;
use std::sync::Arc;
use tachyon_intent::{AttributedText, IntentSpec};
use tachyon_policy::{DefaultPosture, Policy};
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::TaskId;
use tachyon_verify::{
    AcceptanceContract, Clause, CommandCheck, IntentConformanceReport, VerificationPlan,
    VerificationRisk, WorkspaceSnapshot, check_conformance, run,
};
use tokio_util::sync::CancellationToken;

mod common;
use common::Workspace;

fn python(script: &str) -> CommandCheck {
    CommandCheck {
        program: "python3".into(),
        args: vec!["-c".into(), script.into()],
        cwd: ".".into(),
        env: BTreeMap::new(),
        timeout_ms: 2_000,
    }
}

fn granted() -> Policy {
    let mut policy = Policy::new(DefaultPosture::Deny);
    policy.allow("verify.command", "workspace/**");
    policy.allow("process.spawn", "python3");
    policy
}

fn context(ws: &Workspace, artifacts: &Workspace, mut policy: Policy) -> Arc<ToolsContext> {
    for capability in ["fs.read", "fs.metadata", "fs.list"] {
        policy.allow(capability, "workspace/**");
    }
    Arc::new(ToolsContext::new(
        ws.path().to_path_buf(),
        policy,
        ArtifactSpool::new(artifacts.path().to_path_buf()),
    ))
}

fn plan(baseline: &WorkspaceSnapshot, clauses: Vec<Clause>) -> VerificationPlan {
    VerificationPlan::build(
        TaskId::generate(),
        7,
        &AcceptanceContract { clauses },
        baseline,
        &[],
        VerificationRisk::Affected,
    )
    .unwrap()
}

fn spec(
    constraints: &[&str],
    criteria: &[&str],
    inferred: &[&str],
    non_goals: &[&str],
) -> IntentSpec {
    IntentSpec {
        goal: "migrate the blog".into(),
        desired_outcome: "same content, new theme".into(),
        requirements: inferred
            .iter()
            .map(|text| AttributedText::inferred(text.to_string()))
            .collect(),
        constraints: constraints.iter().map(ToString::to_string).collect(),
        preferences: vec![],
        non_goals: non_goals.iter().map(ToString::to_string).collect(),
        affected_surfaces: vec![],
        acceptance_criteria: criteria.iter().map(ToString::to_string).collect(),
        ambiguities: vec![],
        assumptions: vec![],
        evidence: vec![],
        confidence: 0.9,
    }
}

fn statuses(report: &IntentConformanceReport) -> Vec<(&str, &str)> {
    report
        .items
        .iter()
        .map(|item| {
            (
                item.statement.as_str(),
                match item.status {
                    tachyon_verify::ConformanceStatus::Satisfied => "satisfied",
                    tachyon_verify::ConformanceStatus::Violated => "violated",
                    tachyon_verify::ConformanceStatus::Unverifiable => "unverifiable",
                },
            )
        })
        .collect()
}

#[tokio::test]
async fn inferred_miss_flags_nonconformance_while_verification_passes() {
    // Headline advisory case: the gate checks a.txt and passes, but the
    // inferred extra.txt expectation broke. Conformance must flag it
    // without touching the gate's verdict.
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("a.txt", "stable");
    ws.write("extra.txt", "v1");
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    ws.write("extra.txt", "v2");
    let current = WorkspaceSnapshot::capture(ws.path()).unwrap();

    let report = run(
        plan(
            &baseline,
            vec![
                Clause::CommandPasses {
                    command: python("pass"),
                },
                Clause::FileUnchanged {
                    path: "a.txt".into(),
                },
            ],
        ),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.passed(), "gate passes: a.txt unchanged");

    let spec = spec(
        &["no downtime during migration"],
        &["file-unchanged: a.txt"],
        &["file-unchanged: extra.txt"],
        &["redesigning the logo"],
    );
    let conformance = check_conformance(&spec, &report, &baseline, &current);
    assert!(
        !conformance.conforms,
        "inferred miss must flag non-conformance"
    );
    let map: std::collections::HashMap<_, _> = statuses(&conformance).into_iter().collect();
    assert_eq!(map["file-unchanged: a.txt"], "satisfied");
    assert_eq!(map["file-unchanged: extra.txt"], "violated");
    assert_eq!(map["no downtime during migration"], "satisfied");
    assert_eq!(map["redesigning the logo"], "unverifiable");
    assert!(
        conformance
            .items
            .iter()
            .all(|item| !item.evidence.is_empty()),
        "every item carries evidence refs"
    );
}

#[tokio::test]
async fn fully_satisfied_spec_conforms() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("a.txt", "stable");
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    let current = WorkspaceSnapshot::capture(ws.path()).unwrap();

    let report = run(
        plan(
            &baseline,
            vec![
                Clause::CommandPasses {
                    command: python("pass"),
                },
                Clause::FileUnchanged {
                    path: "a.txt".into(),
                },
            ],
        ),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.passed());

    let conformance = check_conformance(
        &spec(
            &[],
            &["file-unchanged: a.txt"],
            &[],
            &["redesigning the logo"],
        ),
        &report,
        &baseline,
        &current,
    );
    assert!(conformance.conforms);
    let map: std::collections::HashMap<_, _> = statuses(&conformance).into_iter().collect();
    assert_eq!(map["file-unchanged: a.txt"], "satisfied");
    assert_eq!(map["redesigning the logo"], "unverifiable");
}

#[tokio::test]
async fn failed_verification_leaves_constraints_unverifiable() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("a.txt", "v1");
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    ws.write("a.txt", "v2");
    let current = WorkspaceSnapshot::capture(ws.path()).unwrap();

    let report = run(
        plan(
            &baseline,
            vec![Clause::FileUnchanged {
                path: "a.txt".into(),
            }],
        ),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed(), "gate fails: a.txt changed");

    let conformance = check_conformance(
        &spec(
            &["no downtime during migration"],
            &["file-unchanged: a.txt"],
            &[],
            &[],
        ),
        &report,
        &baseline,
        &current,
    );
    assert!(!conformance.conforms);
    let map: std::collections::HashMap<_, _> = statuses(&conformance).into_iter().collect();
    assert_eq!(map["file-unchanged: a.txt"], "violated");
    assert_eq!(
        map["no downtime during migration"], "unverifiable",
        "a failed gate blames nothing"
    );
}

#[tokio::test]
async fn free_text_criteria_are_unverifiable_not_violations() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("a.txt", "stable");
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    let current = WorkspaceSnapshot::capture(ws.path()).unwrap();

    let report = run(
        plan(
            &baseline,
            vec![Clause::CommandPasses {
                command: python("pass"),
            }],
        ),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.passed());

    let conformance = check_conformance(
        &spec(&[], &["make it feel snappy"], &[], &[]),
        &report,
        &baseline,
        &current,
    );
    assert!(
        conformance.conforms,
        "unverifiable items never fail conformance"
    );
    let map: std::collections::HashMap<_, _> = statuses(&conformance).into_iter().collect();
    assert_eq!(map["make it feel snappy"], "unverifiable");
}

#[tokio::test]
async fn conformance_is_deterministic() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("a.txt", "stable");
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    let current = WorkspaceSnapshot::capture(ws.path()).unwrap();

    let report = run(
        plan(
            &baseline,
            vec![Clause::CommandPasses {
                command: python("pass"),
            }],
        ),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let spec = spec(&["c"], &["file-unchanged: a.txt"], &["redesign x"], &["y"]);
    assert_eq!(
        check_conformance(&spec, &report, &baseline, &current),
        check_conformance(&spec, &report, &baseline, &current)
    );
}
