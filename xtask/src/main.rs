use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

fn main() -> ExitCode {
    match dispatch() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("verify: {error}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "verify".to_owned());
    if args.next().is_some() {
        return Err(usage().to_owned());
    }

    let checks: fn() -> Result<(), String> = match mode.as_str() {
        "-h" | "--help" | "help" => {
            println!("{}", usage());
            return Ok(());
        }
        "fast" => fast_checks,
        "platform" => platform_checks,
        "verify" => verify_checks,
        "full" => full_run,
        _ => return Err(usage().to_owned()),
    };

    println!("root: {}", workspace_root()?.display());
    checks()
}

fn full_run() -> Result<(), String> {
    ensure_full_tools()?;
    verify_checks()?;
    full_checks()
}

fn usage() -> &'static str {
    "usage: cargo verify [fast|platform|full]\n\
     fast: formatting and workspace compile checks\n\
     verify (default): fast checks, workspace tests, and strict Clippy\n\
     platform: workspace tests for supported-platform CI runners\n\
     full: verify plus every GATES.json gate (security/recovery, fixture, benchmark\n\
     matrix, projection, report/progress/changelog reconciliation) and the perf gate"
}

fn fast_checks() -> Result<(), String> {
    run_cargo(&["fmt", "--all", "--", "--check"])?;
    run_cargo(&[
        "fmt",
        "--manifest-path",
        "xtask/Cargo.toml",
        "--",
        "--check",
    ])?;
    run_cargo(&["check", "--workspace", "--locked"])
}

fn verify_checks() -> Result<(), String> {
    fast_checks()?;
    run_cargo(&["test", "--workspace", "--locked"])?;
    run_cargo(&[
        "clippy",
        "--workspace",
        "--all-targets",
        "--locked",
        "--",
        "-D",
        "warnings",
    ])?;
    run_cargo(&[
        "clippy",
        "--manifest-path",
        "xtask/Cargo.toml",
        "--locked",
        "--",
        "-D",
        "warnings",
    ])
}

fn platform_checks() -> Result<(), String> {
    run_cargo(&["test", "--workspace", "--locked"])
}

fn full_checks() -> Result<(), String> {
    // G6, G3, G4, G5
    run_shell_script("scripts/m14_suites.sh", None)?;
    run_shell_script("scripts/m14_fixture_gate.sh", None)?;
    run_shell_script(
        "scripts/m14_matrix.sh",
        env::var_os("M14_SAMPLES").as_deref(),
    )?;
    let mut matrix_check = Command::new("node");
    matrix_check
        .arg("scripts/m14_matrix_check.mjs")
        .env("M14_MATRIX_OUT", "target/m14/M14_MATRIX.json");
    run_process(
        matrix_check,
        "node scripts/m14_matrix_check.mjs (target/m14/M14_MATRIX.json)".to_owned(),
    )?;
    // G7–G11: report gates that assert their own success string, so a
    // checker that exits 0 without checking anything still fails here.
    run_gate(
        cargo(&[
            "test",
            "-p",
            "tachyon-repo",
            "--test",
            "projection",
            "--release",
            "--",
            "--ignored",
            "--nocapture",
        ]),
        "cargo test -p tachyon-repo --test projection --release (--ignored)  # G7",
        "projection ok",
    )?;
    run_gate(
        node(&["scripts/m14_report_check.mjs"]),
        "node scripts/m14_report_check.mjs  # G8",
        "m14 report ok",
    )?;
    run_gate(
        node(&["scripts/m14_progress_check.mjs"]),
        "node scripts/m14_progress_check.mjs  # G9",
        "progress ok",
    )?;
    run_gate(
        node(&["-e", G10_CHANGELOG]),
        "node -e <CHANGELOG.md has an M14 entry>  # G10",
        "changelog ok",
    )?;
    run_gate(
        node(&["scripts/m14_reconcile_check.mjs"]),
        "node scripts/m14_reconcile_check.mjs  # G11",
        "m14 reconcile ok",
    )?;
    run_shell_script("scripts/perf_gate.sh", None)
}

/// G10's check, kept byte-identical to the `check` field in `GATES.json`.
const G10_CHANGELOG: &str = r#"const t=require("fs").readFileSync("CHANGELOG.md","utf8");if(!/- Milestone 14:/.test(t)){process.exit(1)};console.log("changelog ok")"#;

fn ensure_full_tools() -> Result<(), String> {
    if cfg!(windows) {
        return Err("the full acceptance scripts require a POSIX shell and Node.js; run them on Linux, macOS, or WSL".to_owned());
    }

    check_tool("sh", &["-c", "exit 0"])?;
    check_tool("node", &["--version"])
}

fn check_tool(tool: &str, args: &[&str]) -> Result<(), String> {
    let available = Command::new(tool)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success());
    if available {
        Ok(())
    } else {
        Err(format!(
            "the full acceptance tier requires `{tool}`; install it or run `cargo verify` for the normal local gate"
        ))
    }
}

fn run_cargo(args: &[&str]) -> Result<(), String> {
    run_command("cargo", args)
}

fn cargo(args: &[&str]) -> Command {
    let mut command = Command::new("cargo");
    command.args(args);
    command
}

fn node(args: &[&str]) -> Command {
    let mut command = Command::new("node");
    command.args(args);
    command
}

fn run_shell_script(script: &str, samples: Option<&std::ffi::OsStr>) -> Result<(), String> {
    let mut command = Command::new("sh");
    command.arg(script);
    if let Some(samples) = samples {
        command.env("M14_SAMPLES", samples);
    }
    run_process(command, format!("sh {script}"))
}

fn run_command(program: &str, args: &[&str]) -> Result<(), String> {
    let display = std::iter::once(program)
        .chain(args.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    let mut command = Command::new(program);
    command.args(args);
    run_process(command, display)
}

/// Resolve the workspace root at runtime, so a binary that outlives a
/// checkout move gates the tree it actually sits in. The compile-time path
/// is the last resort, never the first choice.
fn workspace_root() -> Result<PathBuf, String> {
    let mut starts: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        starts.push(dir.to_path_buf());
    }
    if let Ok(cwd) = env::current_dir() {
        starts.push(cwd);
    }
    if let Some(dir) = Path::new(env!("CARGO_MANIFEST_DIR")).parent() {
        starts.push(dir.to_path_buf());
    }
    for start in &starts {
        if let Some(root) = start.ancestors().find(|dir| is_workspace_root(dir)) {
            return Ok(root.to_path_buf());
        }
    }
    let from = starts
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "could not locate the workspace root (searched up from {from})"
    ))
}

fn is_workspace_root(dir: &Path) -> bool {
    dir.join("Cargo.toml").is_file() && dir.join("xtask").join("Cargo.toml").is_file()
}

fn run_process(mut command: Command, display: String) -> Result<(), String> {
    println!("\n$ {display}");
    let status = command
        .current_dir(workspace_root()?)
        .status()
        .map_err(|error| format!("could not start `{display}`: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{display}` exited with {status}"))
    }
}

/// Run a gate, then require the success marker `GATES.json` records as its
/// `expect`. A checker that exits 0 without checking anything fails here.
fn run_gate(mut command: Command, display: &str, expect: &str) -> Result<(), String> {
    println!("\n$ {display}");
    let output = command
        .current_dir(workspace_root()?)
        .output()
        .map_err(|error| format!("could not start `{display}`: {error}"))?;
    let _ = std::io::stdout().write_all(&output.stdout);
    let _ = std::io::stderr().write_all(&output.stderr);
    if !output.status.success() {
        return Err(format!("`{display}` exited with {}", output.status));
    }
    if !String::from_utf8_lossy(&output.stdout).contains(expect) {
        return Err(format!(
            "`{display}` exited 0 without reporting `{expect}`; the gate did not confirm its own result"
        ));
    }
    Ok(())
}
