#[tokio::test]
async fn missing_required_nodes_never_form_a_passing_report() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    let mut plan = VerificationPlan::build(
        TaskId::generate(),
        0,
        &AcceptanceContract {
            clauses: vec![Clause::ChangedPathsWithin { paths: vec![] }],
        },
        &baseline,
        &[],
        VerificationRisk::Affected,
    )
    .unwrap();
    plan.graph.nodes.clear();
    let context = Arc::new(ToolsContext::new(
        ws.path().to_path_buf(),
        Policy::trusted_workspace(),
        ArtifactSpool::new(artifacts.path().to_path_buf()),
    ));
    assert!(run(plan, context, CancellationToken::new()).await.is_err());
}

use super::*;
use crate::{AcceptanceContract, CommandCheck, VerificationRisk, test_support::Workspace};
use tachyon_policy::Policy;
use tachyon_scheduler::OutcomeStatus;
use tachyon_tools::artifact::ArtifactSpool;

#[tokio::test]
async fn forged_node_schema_access_and_retries_cannot_reach_processes() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    let command = CommandCheck { program: "python3".into(), args: vec!["-c".into(), "import pathlib; pathlib.Path('target').mkdir(exist_ok=True); pathlib.Path('target/marker').write_text('ran')".into()], cwd: ".".into(), env: BTreeMap::new(), timeout_ms: 1_000 };
    let plan = VerificationPlan::build(
        TaskId::generate(),
        0,
        &AcceptanceContract {
            clauses: vec![Clause::CommandPasses { command }],
        },
        &baseline,
        &[],
        VerificationRisk::Affected,
    )
    .unwrap();
    let mut policy = Policy::trusted_workspace();
    policy.allow("verify.command", "workspace/**");
    policy.allow("process.spawn", "python3");
    let context = Arc::new(ToolsContext::new(
        ws.path().to_path_buf(),
        policy,
        ArtifactSpool::new(artifacts.path().to_path_buf()),
    ));
    for variant in 0..5 {
        let mut forged_plan = plan.clone();
        let node = forged_plan.graph.nodes.values_mut().next().unwrap();
        match variant {
            0 => node.invocation.args["unexpected"] = serde_json::json!(true),
            1 => node.access.writes.clear(),
            2 => node.retry.attempts = 2,
            3 => node.idempotency = tachyon_ir::Idempotency::Pure,
            _ => node.resources.process_slots = 0,
        }
        let node = node.clone();
        let runner = CheckRunner {
            plan: Arc::new(forged_plan),
            context: context.clone(),
            evidence: Mutex::new(BTreeMap::new()),
            lease: WorkspaceLease::acquire(ws.path(), &CancellationToken::new())
                .await
                .unwrap(),
            lifetime: Arc::new(()),
            asked: Mutex::new(None),
        };
        let outcome = runner
            .execute_owned(&node, serde_json::Map::new(), CancellationToken::new())
            .await;
        assert!(
            matches!(outcome.status, OutcomeStatus::Failed { .. }),
            "accepted forged declaration {variant}"
        );
        assert!(!ws.path().join("target/marker").exists());
    }
}

#[test]
fn only_rust_build_tools_ask_for_toolchain_locations() {
    for program in ["cargo", "cargo.exe", "/usr/local/bin/rustc", "rustup"] {
        assert!(
            toolchain_env_keys(program).is_some(),
            "{program} is a build tool and needs its toolchain locations"
        );
    }
    for program in ["bash", "/bin/sh", "python3", "cargo-build", "cargo-x"] {
        assert_eq!(
            toolchain_env_keys(program),
            None,
            "{program} must not inherit toolchain locations"
        );
    }
}

#[test]
fn toolchain_locations_are_disjoint_from_the_process_allowlist() {
    // The point of the fix is an opt-in at one call site, not a wider
    // allowlist (SECURITY.md §2.1(a)). If these sets ever overlap, the
    // opt-in has silently become the allowlist.
    for key in TOOLCHAIN_ENV_KEYS {
        assert!(
            !tachyon_tools::process::INHERITED_ENV_KEYS.contains(key),
            "{key} is already inherited by every child"
        );
    }
    // And nothing here may be a credential: these are locations only.
    for key in TOOLCHAIN_ENV_KEYS {
        let upper = key.to_ascii_uppercase();
        assert!(
            !upper.contains("KEY") && !upper.contains("TOKEN") && !upper.contains("SECRET"),
            "{key} looks like a credential and does not belong here"
        );
    }
}

#[test]
fn a_declared_check_env_always_wins_over_inherited_locations() {
    let mut declared = BTreeMap::new();
    declared.insert("RUSTUP_TOOLCHAIN".to_owned(), "declared-wins".to_owned());
    let merged = merge_command_env("cargo", &declared);
    assert_eq!(
        merged.get("RUSTUP_TOOLCHAIN").map(String::as_str),
        Some("declared-wins"),
        "the contract is the caller's explicit decision"
    );

    // A non-build tool inherits nothing extra: only its declared entries.
    let mut plain = BTreeMap::new();
    plain.insert("JUST_MINE".to_owned(), "1".to_owned());
    let merged = merge_command_env("python3", &plain);
    assert_eq!(merged.len(), 1, "no toolchain passthrough for python3");
    assert_eq!(merged.get("JUST_MINE").map(String::as_str), Some("1"));
    assert!(
        !merged.contains_key("USERPROFILE"),
        "a non-build tool must not receive toolchain locations"
    );
}
