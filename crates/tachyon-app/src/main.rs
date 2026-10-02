#![warn(unsafe_code)]

//! `tachyon`: CLI client of the Tachyon runtime.
//!
//! The binary owns argument parsing, configuration, tracing, and output
//! formatting. It owns no agent decision logic: local subcommands inspect
//! the environment (`doctor`, `config`); task subcommands talk to the
//! gateway as a client; `gateway` runs the runtime in the foreground.

mod client;
mod config;
mod doctor;
mod logging;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tachyon_gateway::FAKE_PROVIDER_LABEL;
use tachyon_protocol::{Command, CommandResult};

use client::{render, send};
use config::{CliOverrides, Config};
use doctor::{all_ok, run_checks};

/// Tachyon: high-performance AI agent harness.
#[derive(Debug, Parser)]
#[command(name = "tachyon", version, about)]
struct Cli {
    /// Config file path. Overrides `TACHYON_CONFIG` and the default path.
    #[arg(long, env = "TACHYON_CONFIG")]
    config: Option<PathBuf>,
    /// Tracing filter directive. Overrides `TACHYON_LOG_LEVEL`.
    #[arg(long, env = "TACHYON_LOG_LEVEL")]
    log_level: Option<String>,
    /// Runtime state directory. Overrides `TACHYON_DATA_DIR`.
    #[arg(long, env = "TACHYON_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Evidence grace window in ms. Overrides `TACHYON_EVIDENCE_GRACE_MS`.
    #[arg(long, env = "TACHYON_EVIDENCE_GRACE_MS")]
    evidence_grace_ms: Option<u64>,
    /// Prints machine-readable JSON instead of human lines.
    #[arg(long, global = true)]
    json: bool,
    /// Subcommand to run; bare `tachyon` opens the TUI (`attach`).
    #[command(subcommand)]
    command: Option<Commands>,
}

/// Subcommands. `gateway` runs the runtime; `session`/`task` are clients.
#[derive(Debug, Subcommand)]
enum Commands {
    /// Runs deterministic environment self-checks.
    Doctor,
    /// Prints the resolved configuration and its source.
    Config,
    /// Runs the local gateway in the foreground (Ctrl-C stops it).
    Gateway,
    /// Opens a new persistent session.
    Session {
        /// Session action.
        #[command(subcommand)]
        action: SessionAction,
    },
    /// Task operations against the running gateway.
    Task {
        /// Task action.
        #[command(subcommand)]
        action: TaskAction,
    },
    /// Starts a run: session + task + `StartRun` round trips, plumbing
    /// to the gateway's shared driver (no agent logic lives here).
    Run {
        /// Workspace root pinned by the gateway (canonicalized there);
        /// defaults to the current directory.
        #[arg(long)]
        workspace: Option<PathBuf>,
        /// Explicit acceptance contract JSON file; wins over detection.
        #[arg(long)]
        acceptance: Option<PathBuf>,
        /// User's objective; the remaining words are joined.
        objective: Vec<String>,
    },
    /// Answers a repository question deterministically: routes it, indexes
    /// the workspace fresh, and prints source locations. Zero model calls
    /// by construction — a question that would need one is refused.
    Query {
        /// Workspace to search (canonicalized by the gateway); defaults to
        /// the current directory.
        #[arg(long)]
        workspace: Option<PathBuf>,
        /// The question; the remaining words are joined.
        question: Vec<String>,
    },
    /// Lists tasks as a human-readable table (`--json` keeps JSON).
    Ps,
    /// Opens the TUI against the running gateway, optionally on a task.
    Attach {
        /// Task id to attach to.
        #[arg(long)]
        task: Option<String>,
    },
    /// Pauses a task (alias of `tachyon task pause`).
    Pause {
        /// Task id.
        task_id: String,
    },
    /// Resumes a paused task (alias of `tachyon task resume`).
    Resume {
        /// Task id.
        task_id: String,
    },
    /// Cancels a task (alias of `tachyon task cancel`).
    Cancel {
        /// Task id.
        task_id: String,
    },
}

/// Session actions.
#[derive(Debug, Subcommand)]
enum SessionAction {
    /// Creates a session and prints its id.
    Create,
}

/// Task actions.
#[derive(Debug, Subcommand)]
enum TaskAction {
    /// Creates a task in a session.
    Create {
        /// Session that will own the task.
        #[arg(long)]
        session: String,
        /// User's objective.
        objective: String,
    },
    /// Lists tasks, optionally restricted to one session.
    List {
        /// Session to filter by.
        #[arg(long)]
        session: Option<String>,
    },
    /// Prints one task's canonical state.
    Get {
        /// Task id.
        task_id: String,
    },
    /// Sends a steering message to a task.
    Send {
        /// Task id.
        task_id: String,
        /// Message words.
        message: Vec<String>,
    },
    /// Pauses a task.
    Pause {
        /// Task id.
        task_id: String,
    },
    /// Resumes a paused task.
    Resume {
        /// Task id.
        task_id: String,
    },
    /// Cancels a task.
    Cancel {
        /// Task id.
        task_id: String,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(healthy) => {
            if healthy {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(err) => {
            eprintln!("tachyon: {err:?}");
            ExitCode::FAILURE
        }
    }
}

/// Bare `tachyon` (spec §38's first-class command) opens the TUI picker.
fn resolve_command(command: Option<Commands>) -> Commands {
    command.unwrap_or(Commands::Attach { task: None })
}

/// Runs the CLI. Returns `Ok(false)` for check/command failures; hard
/// errors (unreadable config, no gateway) are `Err`.
fn run() -> Result<bool> {
    let cli = Cli::parse();
    let overrides = CliOverrides {
        log_level: cli.log_level,
        data_dir: cli.data_dir,
        evidence_grace_ms: cli.evidence_grace_ms,
    };
    let config = Config::load(cli.config, overrides).context("loading configuration")?;
    logging::init_tracing(&config.log_level);
    let json = cli.json;
    match resolve_command(cli.command) {
        Commands::Doctor => run_doctor(&config, json),
        Commands::Config => run_config(&config, json),
        Commands::Gateway => run_gateway(&config),
        Commands::Session {
            action: SessionAction::Create,
        } => run_session(&config, json),
        Commands::Task { action } => run_task(&config, action, json),
        Commands::Run {
            workspace,
            acceptance,
            objective,
        } => run_run(&config, workspace, acceptance, objective.join(" "), json),
        Commands::Query {
            workspace,
            question,
        } => run_query(&config, workspace, question.join(" "), json),
        Commands::Ps => run_ps(&config, json),
        Commands::Attach { task } => run_attach(&config, task),
        // Top-level aliases: same TaskAction machinery, same commands.
        Commands::Pause { task_id } => run_task(&config, TaskAction::Pause { task_id }, json),
        Commands::Resume { task_id } => run_task(&config, TaskAction::Resume { task_id }, json),
        Commands::Cancel { task_id } => run_task(&config, TaskAction::Cancel { task_id }, json),
    }
}

fn run_doctor(config: &Config, json: bool) -> Result<bool> {
    let checks = run_checks(config);
    if json {
        println!("{}", serde_json::to_string_pretty(&checks)?);
    } else {
        for check in &checks {
            let status = if check.ok { "ok" } else { "FAIL" };
            println!("{status:4} {:12} {}", check.name, check.detail);
        }
    }
    Ok(all_ok(&checks))
}

fn run_config(config: &Config, json: bool) -> Result<bool> {
    if json {
        println!("{}", serde_json::to_string_pretty(config)?);
    } else {
        println!("log_level:         {}", config.log_level);
        println!("data_dir:          {}", config.data_dir.display());
        println!("evidence_grace_ms: {}", config.evidence_grace_ms);
        match &config.source_file {
            Some(path) => println!("config_file:       {}", path.display()),
            None => println!("config_file:       (none; defaults and environment)"),
        }
    }
    Ok(true)
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting async runtime")
}

fn socket_path(config: &Config) -> Result<PathBuf> {
    let info = tachyon_gateway::read_endpoint_info(&config.data_dir.join("gateway.json")).context(
        "reading gateway endpoint (is the gateway running? start it with `tachyon gateway`)",
    )?;
    Ok(info.socket_path)
}

fn run_gateway(config: &Config) -> Result<bool> {
    runtime()?.block_on(async {
        let gateway = tachyon_gateway::start_with(&config.data_dir, config.gateway_runtime())
            .await
            .context("starting gateway")?;
        println!("gateway listening on {}", gateway.socket_path().display());
        tokio::signal::ctrl_c()
            .await
            .context("waiting for Ctrl-C")?;
        println!("shutting down gateway");
        gateway.shutdown().await;
        Ok(true)
    })
}

fn run_session(config: &Config, json: bool) -> Result<bool> {
    let address = socket_path(config)?;
    runtime()?.block_on(async {
        let result = send(
            &address,
            Command::CreateSession {
                workspace_root: None,
            },
        )
        .await?;
        render(&result, json)
    })
}

/// Unwraps a successful command result; typed gateway refusals become
/// honest errors (e.g. `provider_not_configured` before any work).
fn expect_ok(result: Result<CommandResult>, doing: &str) -> Result<serde_json::Value> {
    match result? {
        CommandResult::Ok { payload } => Ok(payload),
        CommandResult::Err { code, message } => {
            anyhow::bail!("gateway refused {doing}: {code}: {message}")
        }
    }
}

/// `tachyon run`: session + task + `StartRun` round trips. Plumbing
/// only — the gateway validates the workspace, pins it, and spawns the
/// ONE shared driver; this binary owns no agent decision logic.
fn run_run(
    config: &Config,
    workspace: Option<PathBuf>,
    acceptance: Option<PathBuf>,
    objective: String,
    json: bool,
) -> Result<bool> {
    if objective.trim().is_empty() {
        anyhow::bail!("OBJECTIVE is required");
    }
    let address = socket_path(config)?;
    let workspace = match workspace {
        Some(path) => path,
        None => std::env::current_dir().context("resolving current directory")?,
    };
    // Plan item 5/CLI: the fake provider announces its label honestly.
    if !json && config.provider.as_ref().is_some_and(|p| p.kind == "fake") {
        println!("provider: {FAKE_PROVIDER_LABEL}");
    }
    runtime()?.block_on(async move {
        let session = expect_ok(
            send(
                &address,
                Command::CreateSession {
                    workspace_root: None,
                },
            )
            .await,
            "creating a session",
        )?;
        let session_id = session["session_id"]
            .as_str()
            .context("gateway returned no session id")?
            .to_owned();
        let task = expect_ok(
            send(
                &address,
                Command::CreateTask {
                    session_id: session_id.parse()?,
                    objective,
                    idempotency_key: None,
                },
            )
            .await,
            "creating the task",
        )?;
        let task_id = task["task_id"]
            .as_str()
            .context("gateway returned no task id")?
            .to_owned();
        let result = send(
            &address,
            Command::StartRun {
                task_id: task_id.parse()?,
                workspace_root: workspace.display().to_string(),
                acceptance: acceptance.map(|path| path.display().to_string()),
            },
        )
        .await?;
        render(&result, json)
    })
}

/// `tachyon query`: one deterministic lookup over a workspace.
///
/// Plumbing only: it sends `Command::Query` and renders what the gateway
/// returns. The routing decision, the index and the "zero model calls"
/// guarantee all live on the gateway side; this function adds no agent
/// logic of its own.
fn run_query(
    config: &Config,
    workspace: Option<PathBuf>,
    question: String,
    json: bool,
) -> Result<bool> {
    if question.trim().is_empty() {
        anyhow::bail!("QUESTION is required, e.g. 'Where is complete_refresh defined and used?'");
    }
    let address = socket_path(config)?;
    let workspace = match workspace {
        Some(path) => path,
        None => std::env::current_dir().context("resolving current directory")?,
    };
    runtime()?.block_on(async move {
        let result = send(
            &address,
            Command::Query {
                workspace_root: workspace.display().to_string(),
                question,
            },
        )
        .await?;
        if json {
            return render(&result, json);
        }
        match &result {
            CommandResult::Ok { payload } => {
                print_lookup(payload);
                Ok(true)
            }
            CommandResult::Err { .. } => render(&result, json),
        }
    })
}

/// Renders one lookup payload as a readable answer. Output is bounded so
/// a dense symbol cannot flood the terminal.
fn print_lookup(payload: &serde_json::Value) {
    const MAX_DEFINITIONS: usize = 20;
    const MAX_REFERENCES: usize = 40;

    let symbol = payload["symbol"].as_str().unwrap_or("?");
    let route = payload["route_class"].as_str().unwrap_or("?");
    let confidence = payload["route_confidence"].as_f64().unwrap_or(0.0);
    let model_calls = payload["model_calls"].as_u64().unwrap_or(1);
    let files = payload["files_indexed"].as_u64().unwrap_or(0);
    println!(
        "{} — {symbol} ({route}, confidence {confidence:.2})",
        payload["question"].as_str().unwrap_or("")
    );
    println!(
        "model calls: {model_calls} · files indexed: {files} · generation {}",
        payload["index_generation"].as_u64().unwrap_or(0)
    );
    if payload["found"] != serde_json::Value::Bool(true) {
        println!(
            "
no definition or reference; {} lexical match(es) below",
            { payload["lexical_hits"].as_u64().unwrap_or(0) }
        );
    }

    let definitions = payload["definitions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    println!(
        "
definitions ({})",
        definitions.len()
    );
    for location in definitions.iter().take(MAX_DEFINITIONS) {
        println!("  {}", format_location(location));
    }
    if definitions.len() > MAX_DEFINITIONS {
        println!("  … {} more", definitions.len() - MAX_DEFINITIONS);
    }

    let references = payload["references"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    println!(
        "
references ({})",
        references.len()
    );
    for location in references.iter().take(MAX_REFERENCES) {
        println!("  {}", format_location(location));
    }
    if references.len() > MAX_REFERENCES {
        println!("  … {} more", references.len() - MAX_REFERENCES);
    }
    let lexical = payload["lexical_hits"].as_u64().unwrap_or(0);
    if lexical > 0 {
        println!(
            "
lexical matches: {lexical}"
        );
    }
}

/// One `file:line` location with its excerpt, trimmed to one line.
fn format_location(location: &serde_json::Value) -> String {
    let file = location["file"].as_str().unwrap_or("?");
    let line = location["line"].as_u64().unwrap_or(0);
    let end = location["end_line"].as_u64().unwrap_or(line);
    let excerpt = location["excerpt"].as_str().unwrap_or("").trim();
    let excerpt: String = excerpt.chars().take(110).collect();
    let range = if end > line {
        format!("{line}-{end}")
    } else {
        line.to_string()
    };
    if excerpt.is_empty() {
        format!("{file}:{range}")
    } else {
        format!("{file}:{range}  {excerpt}")
    }
}

/// `tachyon ps`: `ListTasks` rendered as a human table.
fn run_ps(config: &Config, json: bool) -> Result<bool> {
    let address = socket_path(config)?;
    runtime()?.block_on(async move {
        let result = send(&address, Command::ListTasks { session_id: None }).await?;
        if json {
            return render(&result, json);
        }
        match &result {
            CommandResult::Ok { payload } => {
                let tasks = payload["tasks"].as_array().cloned().unwrap_or_default();
                println!("{:<38} {:<14} OBJECTIVE", "TASK ID", "STATUS");
                for task in &tasks {
                    let id = task["id"].as_str().unwrap_or("-");
                    let status = task["status"].as_str().unwrap_or("-");
                    let objective = task["objective"].as_str().unwrap_or("");
                    let objective: String = objective.chars().take(48).collect();
                    println!("{id:<38} {status:<14} {objective}");
                }
                Ok(true)
            }
            CommandResult::Err { .. } => render(&result, json),
        }
    })
}

/// `tachyon attach`: endpoint-info read, then the frozen TUI entrypoint.
fn run_attach(config: &Config, task: Option<String>) -> Result<bool> {
    let address = socket_path(config)?;
    runtime()?.block_on(async move {
        // Frozen writer-B entrypoint: `AttachOptions.task_id` is the raw
        // string; the TUI produces its own typed parse error.
        tachyon_tui::run(&address, tachyon_tui::AttachOptions { task_id: task })
            .await
            .map_err(|err| anyhow::anyhow!("tui: {err}"))?;
        Ok(true)
    })
}

fn run_task(config: &Config, action: TaskAction, json: bool) -> Result<bool> {
    let address = socket_path(config)?;
    runtime()?.block_on(async {
        let command = task_command(action)?;
        let result = send(&address, command).await?;
        render(&result, json)
    })
}

fn task_command(action: TaskAction) -> Result<Command> {
    match action {
        TaskAction::Create { session, objective } => Ok(Command::CreateTask {
            session_id: parse_session(&session)?,
            objective,
            idempotency_key: None,
        }),
        TaskAction::List { session } => Ok(Command::ListTasks {
            session_id: session.map(|raw| parse_session(&raw)).transpose()?,
        }),
        TaskAction::Get { task_id } => Ok(Command::GetTask {
            task_id: parse_task(&task_id)?,
        }),
        TaskAction::Send { task_id, message } => Ok(Command::SendMessage {
            task_id: parse_task(&task_id)?,
            message: message.join(" "),
        }),
        TaskAction::Pause { task_id } => Ok(Command::PauseTask {
            task_id: parse_task(&task_id)?,
        }),
        TaskAction::Resume { task_id } => Ok(Command::ResumeTask {
            task_id: parse_task(&task_id)?,
        }),
        TaskAction::Cancel { task_id } => Ok(Command::CancelTask {
            task_id: parse_task(&task_id)?,
        }),
    }
}

fn parse_session(raw: &str) -> Result<tachyon_types::SessionId> {
    uuid::Uuid::parse_str(raw)
        .map(tachyon_types::SessionId)
        .with_context(|| format!("invalid session id {raw:?}"))
}

fn parse_task(raw: &str) -> Result<tachyon_types::TaskId> {
    uuid::Uuid::parse_str(raw)
        .map(tachyon_types::TaskId)
        .with_context(|| format!("invalid task id {raw:?}"))
}

/// M11 writer C: the new surface parses — `run`/`ps`/`attach` and the
/// top-level `pause`/`resume`/`cancel` aliases — while every existing
/// `task` subcommand keeps parsing exactly as before.
#[cfg(test)]
mod cli_surface_parse {
    use clap::Parser;

    use super::{Cli, Commands, TaskAction};

    #[test]
    fn run_parses_workspace_acceptance_and_objective() {
        let cli = Cli::try_parse_from([
            "tachyon",
            "run",
            "--workspace",
            "/tmp/ws",
            "--acceptance",
            "/tmp/acc.json",
            "fix",
            "the",
            "login",
            "bug",
        ])
        .expect("run parses");
        match cli.command.expect("run subcommand") {
            Commands::Run {
                workspace,
                acceptance,
                objective,
            } => {
                assert_eq!(
                    workspace.map(|p| p.display().to_string()),
                    Some("/tmp/ws".to_owned())
                );
                assert_eq!(
                    acceptance.map(|p| p.display().to_string()),
                    Some("/tmp/acc.json".to_owned())
                );
                assert_eq!(objective.join(" "), "fix the login bug");
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn run_defaults_workspace_and_acceptance_to_none() {
        let cli = Cli::try_parse_from(["tachyon", "run", "just", "do", "it"]).expect("run parses");
        match cli.command.expect("run subcommand") {
            Commands::Run {
                workspace,
                acceptance,
                objective,
            } => {
                assert!(workspace.is_none());
                assert!(acceptance.is_none());
                assert_eq!(objective.join(" "), "just do it");
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn ps_parses() {
        let cli = Cli::try_parse_from(["tachyon", "ps"]).expect("ps parses");
        assert!(matches!(cli.command, Some(Commands::Ps)));
    }

    #[test]
    fn bare_invocation_defaults_to_the_tui() {
        let cli = Cli::try_parse_from(["tachyon"]).expect("bare tachyon parses");
        assert!(
            matches!(
                super::resolve_command(cli.command),
                Commands::Attach { task: None }
            ),
            "bare tachyon must open the TUI picker (attach)"
        );
    }

    #[test]
    fn attach_parses_with_optional_task() {
        let cli = Cli::try_parse_from(["tachyon", "attach"]).expect("attach parses");
        match cli.command.expect("attach subcommand") {
            Commands::Attach { task } => assert!(task.is_none()),
            other => panic!("expected Attach, got {other:?}"),
        }
        let cli = Cli::try_parse_from([
            "tachyon",
            "attach",
            "--task",
            "0193f0c2-0000-7000-8000-000000000000",
        ])
        .expect("attach with task parses");
        match cli.command.expect("attach subcommand") {
            Commands::Attach { task } => {
                assert_eq!(
                    task.as_deref(),
                    Some("0193f0c2-0000-7000-8000-000000000000")
                );
            }
            other => panic!("expected Attach, got {other:?}"),
        }
    }

    #[test]
    fn pause_resume_cancel_are_top_level_commands() {
        for (word, expect) in [
            ("pause", "pause"),
            ("resume", "resume"),
            ("cancel", "cancel"),
        ] {
            let cli =
                Cli::try_parse_from(["tachyon", word, "0193f0c2-0000-7000-8000-000000000000"])
                    .unwrap_or_else(|err| panic!("{word} parses: {err}"));
            let ok = match cli.command.expect("alias subcommand") {
                Commands::Pause { .. } => expect == "pause",
                Commands::Resume { .. } => expect == "resume",
                Commands::Cancel { .. } => expect == "cancel",
                other => panic!("unexpected command for {word}: {other:?}"),
            };
            assert!(ok, "{word} mapped wrong");
        }
    }

    #[test]
    fn every_existing_task_subcommand_still_parses() {
        let id = "0193f0c2-0000-7000-8000-000000000000";
        let session = "0193f0c2-0000-7000-8000-000000000001";
        for args in [
            vec![
                "tachyon",
                "task",
                "create",
                "--session",
                session,
                "objective",
            ],
            vec!["tachyon", "task", "list"],
            vec!["tachyon", "task", "get", id],
            vec!["tachyon", "task", "send", id, "hello", "world"],
            vec!["tachyon", "task", "pause", id],
            vec!["tachyon", "task", "resume", id],
            vec!["tachyon", "task", "cancel", id],
        ] {
            let cli = Cli::try_parse_from(&args)
                .unwrap_or_else(|err| panic!("{args:?} still parses: {err}"));
            assert!(
                matches!(cli.command, Some(Commands::Task { .. })),
                "{args:?} must stay a task subcommand"
            );
        }
        // The alias helpers map onto the same TaskAction machinery.
        let action = super::task_command(TaskAction::Pause {
            task_id: id.to_owned(),
        })
        .unwrap();
        assert!(matches!(
            action,
            tachyon_protocol::Command::PauseTask { .. }
        ));
    }
}
