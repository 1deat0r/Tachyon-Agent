use std::env;
use std::path::Path;
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

    match mode.as_str() {
        "-h" | "--help" | "help" => {
            println!("{}", usage());
            Ok(())
        }
        "fast" => fast_checks(),
        "platform" => platform_checks(),
        "verify" => verify_checks(),
        "full" => {
            ensure_full_tools()?;
            verify_checks()?;
            full_checks()
        }
        _ => Err(usage().to_owned()),
    }
}

fn usage() -> &'static str {
    "usage: cargo verify [fast|platform|full]\n\
     fast: formatting and workspace compile checks\n\
     verify (default): fast checks, workspace tests, and strict Clippy\n\
     platform: workspace tests for supported-platform CI runners\n\
     full: verify plus M14 security/recovery, fixture, benchmark-matrix, and perf gates"
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
    run_shell_script("scripts/perf_gate.sh", None)
}

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

fn run_process(mut command: Command, display: String) -> Result<(), String> {
    println!("\n$ {display}");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "could not locate the workspace root".to_owned())?;
    let status = command
        .current_dir(root)
        .status()
        .map_err(|error| format!("could not start `{display}`: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("`{display}` exited with {status}"))
    }
}
