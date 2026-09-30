//! M11 writer C, deliverable 5: the CLI surface e2e — `tachyon run`,
//! `tachyon ps`, the `pause`/`resume`/`cancel` aliases against a live
//! gateway, the fake-provider label the plan requires, honest refusal
//! without provider config, `attach`'s endpoint wiring, and the kept
//! `task` subcommands. Help text assertions pin each subcommand's flags.
//!
//! The TUI itself is not driven here (it needs a terminal); `attach` is
//! covered up to its endpoint-info read, which is the part this writer
//! wires.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn tachyon() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tachyon"))
}

/// Fresh scratch dir for this test invocation. Deliberately SHORT: the
/// gateway binds a Unix socket under `data/`, and socket paths must fit
/// `sockaddr_un.sun_path` (`SUN_LEN`) — TMPDIR here is already deep.
fn scratch(tag: &str) -> PathBuf {
    // UUIDv7's first chars are TIME; take the random tail (chars 16..24)
    // so back-to-back runs never share a directory, and clear a stale
    // collision defensively — a stale data dir would carry old tasks.
    let unique = uuid::Uuid::now_v7().simple().to_string();
    let dir = std::env::temp_dir().join(format!("m11-{tag}-{}", &unique[16..24]));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path, provider: bool) -> PathBuf {
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    // serde_json so Windows backslashes in data_dir are escaped — raw
    // display() interpolation produces invalid JSON (`\U` escape).
    let mut root = serde_json::json!({
        "data_dir": data.display().to_string(),
        "log_level": "warn",
    });
    if provider {
        root["provider"] = serde_json::json!({
            "kind": "fake",
            "model": "scripted-replay-1",
        });
    }
    let path = dir.join("config.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&root).unwrap()).unwrap();
    path
}

fn wait_for_gateway(config: &Path, data: &Path, child: &mut Child) {
    let endpoint = data.join("gateway.json");
    for _ in 0..200 {
        if endpoint.exists() {
            return;
        }
        if let Some(status) = child.try_wait().expect("gateway child") {
            let err = std::fs::read(data.join("gateway.stderr")).unwrap_or_default();
            panic!(
                "gateway exited early with {status}: {} (cfg {})",
                String::from_utf8_lossy(&err),
                config.display()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    panic!("gateway endpoint never appeared");
}

fn kill_gateway(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Runs `tachyon` with the config; returns (success, stdout, stderr).
fn run_cli(config: &Path, args: &[&str]) -> (bool, String, String) {
    let config_flag = config.display().to_string();
    let mut cmdline: Vec<&str> = vec!["--config", &config_flag];
    cmdline.extend_from_slice(args);
    let out = tachyon()
        .args(&cmdline)
        .output()
        .expect("tachyon child runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn help_text_documents_the_new_surface_and_keeps_task() {
    let help = |args: &[&str]| -> String {
        let out = tachyon().args(args).output().expect("tachyon runs");
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        text
    };

    let root = help(&["--help"]);
    for word in [
        "run", "ps", "query", "attach", "pause", "resume", "cancel", "task",
    ] {
        assert!(root.contains(word), "`--help` missing {word:?}:\n{root}");
    }

    let query = help(&["query", "--help"]);
    assert!(query.contains("--workspace"), "query help:\n{query}");
    assert!(query.contains("QUESTION"), "query help:\n{query}");

    let run = help(&["run", "--help"]);
    assert!(run.contains("--workspace"), "run help:\n{run}");
    assert!(run.contains("--acceptance"), "run help:\n{run}");
    assert!(run.contains("OBJECTIVE"), "run help:\n{run}");

    let attach = help(&["attach", "--help"]);
    assert!(attach.contains("--task"), "attach help:\n{attach}");

    let ps = help(&["ps", "--help"]);
    assert!(ps.to_lowercase().contains("task"), "ps help:\n{ps}");

    for word in ["pause", "resume", "cancel"] {
        let text = help(&[word, "--help"]);
        assert!(text.contains("task"), "{word} help:\n{text}");
    }

    let task = help(&["task", "--help"]);
    for word in ["create", "list", "get", "send", "pause", "resume", "cancel"] {
        assert!(task.contains(word), "task help missing {word:?}:\n{task}");
    }
}

#[test]
fn run_ps_and_the_aliases_drive_a_live_gateway() {
    let dir = scratch("flow");
    let config = write_config(&dir, true);
    let data = dir.join("data");

    let mut gateway = tachyon()
        .args(["--config", &config.display().to_string(), "gateway"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("gateway spawns");
    wait_for_gateway(&config, &data, &mut gateway);

    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("Cargo.toml"),
        "[package]\nname=\"w\"\nversion=\"0.0.0\"\n",
    )
    .unwrap();

    // `run`: session + task + StartRun; prints the fake provider label.
    let (ok, stdout, stderr) = run_cli(
        &config,
        &[
            "run",
            "--workspace",
            &ws.display().to_string(),
            "fix",
            "the",
            "bug",
        ],
    );
    assert!(ok, "run must accept: {stderr}");
    assert!(
        stdout.contains("scripted test/replay provider"),
        "fake provider label not printed:\n{stdout}"
    );

    // `ps`: human table carrying the task.
    let (ok, stdout, stderr) = run_cli(&config, &["ps"]);
    assert!(ok, "ps must succeed: {stderr}");
    assert!(stdout.contains("ID"), "table header missing:\n{stdout}");
    assert!(stdout.contains("STATUS"), "table header missing:\n{stdout}");

    // Recover the task id from JSON `ps` (exactly one task exists).
    let (_, json, _) = run_cli(&config, &["--json", "ps"]);
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("json ps output");
    let tasks = parsed["Ok"]["payload"]["tasks"]
        .as_array()
        .expect("tasks array");
    assert_eq!(tasks.len(), 1, "one task after run: {json}");
    let task_id = tasks[0]["id"].as_str().expect("task id").to_owned();

    // Aliases map onto the existing task machinery, end to end.
    let (ok, _, stderr) = run_cli(&config, &["pause", &task_id]);
    assert!(ok, "alias pause must succeed: {stderr}");
    let (_, json, _) = run_cli(&config, &["task", "get", &task_id]);
    assert!(json.contains("Paused"), "pause did not land: {json}");

    let (ok, _, stderr) = run_cli(&config, &["resume", &task_id]);
    assert!(ok, "alias resume must succeed: {stderr}");

    let (ok, _, stderr) = run_cli(&config, &["cancel", &task_id]);
    assert!(ok, "alias cancel must succeed: {stderr}");
    let (_, json, _) = run_cli(&config, &["task", "get", &task_id]);
    assert!(json.contains("Cancelled"), "cancel did not land: {json}");

    // Existing task subcommands still work.
    let (ok, stdout, stderr) = run_cli(&config, &["task", "list"]);
    assert!(ok, "task list must succeed: {stderr}");
    assert!(stdout.contains(task_id.as_str()), "task list:\n{stdout}");

    kill_gateway(&mut gateway);
}

#[test]
fn run_refuses_honestly_when_no_provider_is_configured() {
    let dir = scratch("noprovider");
    let config = write_config(&dir, false);
    let data = dir.join("data");

    let mut gateway = tachyon()
        .args(["--config", &config.display().to_string(), "gateway"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("gateway spawns");
    wait_for_gateway(&config, &data, &mut gateway);

    let ws = dir.join("workspace");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("Cargo.toml"),
        "[package]\nname=\"w\"\nversion=\"0.0.0\"\n",
    )
    .unwrap();

    let (ok, _stdout, stderr) = run_cli(
        &config,
        &[
            "run",
            "--workspace",
            &ws.display().to_string(),
            "do",
            "something",
        ],
    );
    assert!(!ok, "run without provider must exit non-zero");
    assert!(
        stderr.contains("provider_not_configured"),
        "honest typed refusal expected on stderr: {stderr}"
    );

    kill_gateway(&mut gateway);
}

#[test]
fn query_answers_a_lookup_without_any_provider_configured() {
    let dir = scratch("query");
    // No provider at all: `tachyon run` refuses on this gateway (see the
    // test above), so a lookup that succeeds here structurally never
    // consulted one.
    let config = write_config(&dir, false);
    let data = dir.join("data");

    let mut gateway = tachyon()
        .args(["--config", &config.display().to_string(), "gateway"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("gateway spawns");
    wait_for_gateway(&config, &data, &mut gateway);

    let ws = dir.join("workspace");
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::write(
        ws.join("src/lib.rs"),
        "pub fn refresh_session(token: &str) -> String {\n    token.to_owned()\n}\n",
    )
    .unwrap();
    std::fs::write(
        ws.join("src/use.rs"),
        "fn boot() {\n    let _ = refresh_session(\"x\");\n}\n",
    )
    .unwrap();

    let (ok, stdout, stderr) = run_cli(
        &config,
        &[
            "query",
            "--workspace",
            &ws.display().to_string(),
            "Where",
            "is",
            "refresh_session",
            "defined",
            "and",
            "used?",
        ],
    );
    assert!(ok, "query must succeed without a provider: {stderr}");
    assert!(
        stdout.contains("model calls: 0"),
        "the answer must state its zero-model contract:\n{stdout}"
    );
    assert!(
        stdout.contains("refresh_session"),
        "the symbol must be named:\n{stdout}"
    );
    assert!(
        stdout.contains("src/lib.rs"),
        "fresh definition location expected:\n{stdout}"
    );

    kill_gateway(&mut gateway);
}

#[test]
fn attach_fails_fast_without_a_running_gateway_and_never_hangs() {
    let dir = scratch("attach");
    let config = write_config(&dir, true);
    // No gateway started: endpoint-info read must fail fast.
    // Bare `tachyon` is covered too — it defaults to `attach`.
    for args in [vec!["attach"], vec![]] {
        let mut cmd = tachyon();
        cmd.args(["--config", &config.display().to_string()]);
        cmd.args(&args);
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("attach spawns");
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = child.try_wait().expect("attach child") {
                let out = child.wait_with_output().ok();
                assert!(
                    !status.success(),
                    "attach without gateway must exit non-zero"
                );
                let text = out.map(|o| {
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&o.stdout),
                        String::from_utf8_lossy(&o.stderr)
                    )
                });
                let text = text.unwrap_or_default();
                assert!(
                    text.contains("gateway endpoint") || text.contains("gateway"),
                    "endpoint wiring message expected: {text}"
                );
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "attach hung instead of failing fast"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}
