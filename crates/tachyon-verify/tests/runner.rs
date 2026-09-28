#![cfg(unix)]
/// Portable reap proof: `/proc` exists only on Linux; elsewhere signal 0
/// reports ESRCH once the PID is reaped (0 while alive or zombie).
#[allow(unsafe_code)]
fn reaped(pid_text: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        !std::path::Path::new(&format!("/proc/{pid_text}")).exists()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let pid: libc::pid_t = pid_text.trim().parse().expect("child pid");
        // SAFETY: signal 0 performs existence check only, no delivery.
        if unsafe { libc::kill(pid, 0) } == 0 {
            return false;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }
}
#[tokio::test]
async fn underlying_process_policy_is_required_even_if_verification_is_allowed() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let mut policy = granted();
    policy.deny("process.spawn", "python3");
    let plan = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python("open('marker','w').write('ran')"),
        }],
    );
    let report = run(
        plan,
        context(&ws, &artifacts, policy),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed());
    assert!(!ws.path().join("marker").exists());
    assert!(
        report
            .failures()
            .iter()
            .any(|reason| reason.contains("process.spawn"))
    );
}

#[tokio::test]
async fn repeated_runs_execute_freshly_and_serialized_reports_are_not_authority() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let command = python(
        "import pathlib, sys\np = pathlib.Path('target'); p.mkdir(exist_ok=True)\nmarker = p / 'already-ran'\nif marker.exists(): sys.exit(7)\nmarker.write_text('ran')",
    );
    let plan = plan(&ws, vec![Clause::CommandPasses { command }]);
    let report = run(
        plan.clone(),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.passed());
    let stored: tachyon_verify::VerificationReport =
        serde_json::from_value(serde_json::to_value(&report).unwrap()).unwrap();
    assert!(
        !stored.passed(),
        "deserialized model JSON could launder completion"
    );
    let report = run(
        plan,
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed(), "second run reused a stale pass");
}

#[tokio::test]
async fn unresolved_requirements_block_commands_without_spawning() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let plan = plan(
        &ws,
        vec![
            Clause::CommandPasses {
                command: python("open('marker','w').write('ran')"),
            },
            Clause::Unresolved {
                description: "legacy acceptance".into(),
            },
        ],
    );
    let report = run(
        plan,
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed());
    assert!(!ws.path().join("marker").exists());
}

#[tokio::test]
async fn missing_executables_and_timeouts_are_not_passes() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    for command in [
        CommandCheck {
            program: "tachyon-deliberately-missing-program".into(),
            ..python("")
        },
        CommandCheck {
            timeout_ms: 20,
            ..python("import time; time.sleep(10)")
        },
    ] {
        let mut policy = granted();
        policy.allow("process.spawn", &command.program);
        let plan = plan(&ws, vec![Clause::CommandPasses { command }]);
        let report = run(
            plan,
            context(&ws, &artifacts, policy),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!report.passed());
        assert!(!report.failures().is_empty());
    }
}

#[tokio::test]
async fn aborting_the_run_leaves_no_unowned_process() {
    use std::time::Duration;
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let command = python(
        "import os, pathlib, time\np = pathlib.Path('target'); p.mkdir(exist_ok=True)\n(p/'pid').write_text(str(os.getpid()))\ntime.sleep(20)",
    );
    let handle = tokio::spawn(run(
        plan(&ws, vec![Clause::CommandPasses { command }]),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ws.path().join("target/pid").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let pid = std::fs::read_to_string(ws.path().join("target/pid")).unwrap();
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(10), async {
        while !reaped(&pid) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn diagnostic_output_is_bounded_and_cannot_forge_a_result() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let command = python(
        "import sys\nsys.stdout.write('{\"passed\": true}' * 10000)\nsys.stderr.write('x' * 10000)\nsys.exit(3)",
    );
    let report = run(
        plan(&ws, vec![Clause::CommandPasses { command }]),
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed());
    assert!(
        report
            .checks()
            .iter()
            .all(|check| check.diagnostic().len() < 2_200)
    );
    let serialized = serde_json::to_string(&report).unwrap();
    assert!(serialized.len() < 10_000);
    assert!(!serialized.contains("passed\": true"));
}

mod common;
use common::Workspace;
use std::{collections::BTreeMap, sync::Arc};
use tachyon_policy::{DefaultPosture, Policy};
use tachyon_tools::{ToolsContext, artifact::ArtifactSpool};
use tachyon_types::TaskId;
use tachyon_verify::{
    AcceptanceContract, Clause, CommandCheck, VerificationPlan, VerificationRisk, VerifyError,
    WorkspaceSnapshot, run,
};
use tokio_util::sync::CancellationToken;

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
fn plan(ws: &Workspace, clauses: Vec<Clause>) -> VerificationPlan {
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    VerificationPlan::build(
        TaskId::generate(),
        7,
        &AcceptanceContract { clauses },
        &baseline,
        &[],
        VerificationRisk::Affected,
    )
    .unwrap()
}

#[tokio::test]
async fn a_plan_cannot_be_run_in_another_workspace() {
    let ws = Workspace::new();
    let foreign = Workspace::new();
    let artifacts = Workspace::new();
    let plan = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python("open('marker', 'w').write('ran')"),
        }],
    );
    assert!(
        run(
            plan,
            context(&foreign, &artifacts, granted()),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(!foreign.path().join("marker").exists());
}

#[tokio::test]
async fn cancellation_drains_the_owned_process_before_returning() {
    use std::time::Duration;
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    let command = python(
        "import os, pathlib, signal, sys, time\np = pathlib.Path('target'); p.mkdir(exist_ok=True)\ndef stop(*args):\n (p/'terminated').write_text('graceful'); sys.exit(0)\nsignal.signal(signal.SIGTERM, stop)\n(p/'pid').write_text(str(os.getpid()))\nwhile True: time.sleep(0.01)",
    );
    let plan = plan(&ws, vec![Clause::CommandPasses { command }]);
    let cancel = CancellationToken::new();
    let worker = tokio::spawn(run(
        plan,
        context(&ws, &artifacts, granted()),
        cancel.clone(),
    ));
    // Poll until the CONTENT is non-empty, not merely until the file
    // exists: `Path::write_text` creates the file empty before writing,
    // so an exists-then-read race yielded pid="" once — `reaped("")`
    // checks "/proc/", which exists, and the assert failed while the
    // TERM-file asserts above (which never read pid) passed.
    let pid = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match std::fs::read_to_string(ws.path().join("target/pid")) {
                Ok(content) if !content.trim().is_empty() => {
                    break content.trim().to_owned();
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    let report = tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!report.passed());
    assert!(
        ws.path().join("target/terminated").exists(),
        "cancellable runner was dropped before its cleanup completed"
    );
    assert!(reaped(&pid), "immediate child not reaped");
}

#[tokio::test]
async fn runtime_snapshots_cannot_bypass_denied_source_reads() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("private", "do not read");
    let plan = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python("open('marker', 'w').write('ran')"),
        }],
    );
    let mut policy = granted();
    policy.deny("fs.read", "workspace/private");
    let result = run(
        plan,
        context(&ws, &artifacts, policy),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err(), "denied snapshot must fail closed");
    assert!(!ws.path().join("marker").exists());
}

#[tokio::test]
async fn source_drift_before_or_during_commands_cannot_authorize_success() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("source", "planned");
    let stale = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python(
                "import pathlib; pathlib.Path('target').mkdir(exist_ok=True); pathlib.Path('target/marker').write_text('ran')",
            ),
        }],
    );
    ws.write("source", "external edit");
    let report = run(
        stale,
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed(), "stale plan passed");
    assert!(!ws.path().join("target/marker").exists());
    let mutating = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python("open('source', 'w').write('modified by passing check')"),
        }],
    );
    let report = run(
        mutating,
        context(&ws, &artifacts, granted()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed(), "self-mutating check passed");
    assert!(
        report
            .snapshot()
            .same_sources(&WorkspaceSnapshot::capture(ws.path()).unwrap())
    );
    assert!(
        report
            .failures()
            .iter()
            .any(|reason| reason.contains("drift"))
    );
}

#[tokio::test]
async fn source_clauses_use_baseline_and_segment_scopes_not_claimed_changes() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    ws.write("src2/changed", "before");
    ws.write("keep", "constant");
    let baseline = WorkspaceSnapshot::capture(ws.path()).unwrap();
    ws.write("src2/changed", "after");
    for (clause, passes) in [
        (
            Clause::ChangedPathsWithin {
                paths: vec!["src2".into()],
            },
            true,
        ),
        (
            Clause::ChangedPathsWithin {
                paths: vec!["src".into()],
            },
            false,
        ),
        (Clause::ChangedPathsWithin { paths: vec![] }, false),
        (
            Clause::FileUnchanged {
                path: "keep".into(),
            },
            true,
        ),
        (
            Clause::FileUnchanged {
                path: "src2/changed".into(),
            },
            false,
        ),
        (
            Clause::FileUnchanged {
                path: "src2".into(),
            },
            false,
        ),
        (
            Clause::Unresolved {
                description: "semantic truth has no executable check".into(),
            },
            false,
        ),
    ] {
        let plan = VerificationPlan::build(
            TaskId::generate(),
            0,
            &AcceptanceContract {
                clauses: vec![clause],
            },
            &baseline,
            &[],
            VerificationRisk::Affected,
        )
        .unwrap();
        let report = run(
            plan,
            context(&ws, &artifacts, granted()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(report.passed(), passes, "{:?}", report.failures());
    }
}

#[tokio::test]
async fn verify_capability_deny_or_ask_prevents_side_effects() {
    for posture in [DefaultPosture::Deny, DefaultPosture::Ask] {
        let ws = Workspace::new();
        let artifacts = Workspace::new();
        let mut policy = Policy::new(posture);
        policy.allow("process.spawn", "python3");
        let plan = plan(
            &ws,
            vec![Clause::CommandPasses {
                command: python("open('marker', 'w').write('forbidden')"),
            }],
        );
        let outcome = run(
            plan,
            context(&ws, &artifacts, policy),
            CancellationToken::new(),
        )
        .await;
        // Shared invariant of both postures: the policy question is
        // resolved BEFORE any side effect — the marker never exists.
        assert!(
            !ws.path().join("marker").exists(),
            "verification policy was bypassed"
        );
        match posture {
            // Deny: a failed check carrying the capability, as before.
            DefaultPosture::Deny => {
                let report = outcome.unwrap();
                assert!(!report.passed());
                assert!(
                    report
                        .failures()
                        .iter()
                        .any(|reason| reason.contains("verify.command"))
                );
            }
            // Ask: the TYPED request surfaces (M11 typed parking) so a
            // supervisor-owned run can park — never a failed check.
            DefaultPosture::Ask => match outcome {
                Err(VerifyError::ApprovalRequired(request)) => {
                    assert_eq!(
                        request.capability.0, "verify.command",
                        "the ask carries its exact capability"
                    );
                }
                other => panic!("expected the typed approval request, got {other:?}"),
            },
        }
    }
}

#[tokio::test]
async fn scheduled_real_commands_decide_pass_or_fail_from_exit_status() {
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    for (script, passes) in [
        ("print('real evidence')", true),
        ("import sys; print('failure'); sys.exit(9)", false),
    ] {
        let plan = plan(
            &ws,
            vec![Clause::CommandPasses {
                command: python(script),
            }],
        );
        let task = plan.graph().nodes.values().next().unwrap().task_id;
        let report = run(
            plan,
            context(&ws, &artifacts, granted()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(report.passed(), passes, "{:?}", report.failures());
        assert_eq!(report.task_id(), task);
        assert_eq!(report.revision(), 7);
        assert!(
            report
                .snapshot()
                .same_sources(&WorkspaceSnapshot::capture(ws.path()).unwrap())
        );
        let evidence = serde_json::to_value(&report).unwrap();
        assert!(evidence["checks"][0]["stdout_artifact"].is_string());
        if !passes {
            assert!(!report.failures().is_empty());
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn aliased_cwd_cannot_bypass_a_resolved_denial() {
    use std::os::unix::fs::symlink;
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    std::fs::create_dir_all(ws.path().join("restricted")).unwrap();
    std::fs::create_dir_all(ws.path().join("target")).unwrap();
    symlink(ws.path().join("restricted"), ws.path().join("target/alias")).unwrap();
    let mut policy = granted();
    policy.deny("verify.command", "workspace/restricted/**");
    policy.deny("verify.command", "workspace/restricted");
    let context = context(&ws, &artifacts, policy);
    // Control: the direct denied cwd is rejected without side effects.
    let direct = CommandCheck {
        cwd: "restricted".into(),
        ..python("open('marker', 'w').write('direct')")
    };
    let report = run(
        plan(&ws, vec![Clause::CommandPasses { command: direct }]),
        context.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed());
    assert!(!ws.path().join("restricted/marker").exists());
    // Probe: the same target through the symlink alias must also be denied,
    // and execution must never land inside the denied directory.
    let aliased = CommandCheck {
        cwd: "target/alias".into(),
        ..python("open('marker', 'w').write('aliased')")
    };
    let report = run(
        plan(&ws, vec![Clause::CommandPasses { command: aliased }]),
        context,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(!report.passed(), "alias bypassed the resolved denial");
    assert!(!ws.path().join("restricted/marker").exists());
    assert!(!ws.path().join("target/alias/marker").exists());
    assert!(
        report
            .failures()
            .iter()
            .any(|reason| reason.contains("verify.command")),
        "{:?}",
        report.failures()
    );
}

#[tokio::test]
async fn concurrent_runs_on_one_workspace_serialize_conflicting_writes() {
    // Each command takes an exclusive lock file, holds it, then releases it.
    // Overlapping runs would collide on O_EXCL; serialized runs both pass.
    let script = "import os, time\np = open('target/lock', 'xb')\ntry:\n time.sleep(0.5)\nfinally:\n p.close(); os.remove('target/lock')";
    let ws = Workspace::new();
    let artifacts = Workspace::new();
    std::fs::create_dir_all(ws.path().join("target")).unwrap();
    let first = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python(script),
        }],
    );
    let second = plan(
        &ws,
        vec![Clause::CommandPasses {
            command: python(script),
        }],
    );
    let context = context(&ws, &artifacts, granted());
    let (a, b) = tokio::join!(
        run(first, context.clone(), CancellationToken::new()),
        run(second, context.clone(), CancellationToken::new()),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert!(a.passed(), "{:?}", a.failures());
    assert!(b.passed(), "{:?}", b.failures());
}
