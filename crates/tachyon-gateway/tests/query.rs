//! Handoff priority 2: a real user question answered deterministically.
//!
//! The gateway runs `Command::Query` with **no provider configured at
//! all**, which is the structural half of the zero-model guarantee — a
//! path that consulted a model could not even start here. The payload
//! asserts the behavioural half: `model_calls == 0`, a deterministic
//! route class, and fresh source locations for the symbol the question
//! named.

use std::path::PathBuf;

use tachyon_gateway::{GatewayRuntime, start_with};
use tachyon_protocol::Command;
use tachyon_tools::credential::CredentialBroker;

mod common;
use common::{code_of, err, ok, send, test_dir};

/// A gateway with no model provider: `StartRun` would refuse at its own
/// preflight (`provider_not_configured`), so anything that answers here
/// provably never went near a provider.
fn lookup_only_runtime() -> GatewayRuntime {
    GatewayRuntime {
        provider: None,
        label: "openai_compat".to_owned(),
        model: "none".to_owned(),
        redactor: CredentialBroker::default(),
    }
}

/// A tiny workspace with a symbol defined once and used elsewhere.
fn workspace(dir: &std::path::Path) -> PathBuf {
    let ws = dir.join("ws");
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::write(
        ws.join("src/refresh.rs"),
        "pub async fn complete_refresh(session: &Session) -> Token {\n\
         \x20   refresh_token(session)\n\
         }\n\n\
         fn refresh_token(session: &Session) -> Token {\n\
         \x20   session.token.clone()\n\
         }\n",
    )
    .unwrap();
    std::fs::write(
        ws.join("src/main.rs"),
        "fn main() {\n\
         \x20   let _ = complete_refresh(&session);\n\
         \x20   let _ = complete_refresh(&other);\n\
         }\n",
    )
    .unwrap();
    ws
}

fn query(workspace_root: &std::path::Path, question: &str) -> Command {
    Command::Query {
        workspace_root: workspace_root.display().to_string(),
        question: question.to_owned(),
    }
}

#[tokio::test]
async fn a_lookup_question_is_answered_with_locations_and_zero_model_calls() {
    let dir = test_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let ws = workspace(&dir);
    let gateway = start_with(&dir, lookup_only_runtime()).await.unwrap();

    let payload = ok(
        gateway.address(),
        query(&ws, "Where is complete_refresh defined and used?"),
    )
    .await;

    assert_eq!(
        payload["model_calls"], 0,
        "the contract this command exists to keep: {payload}"
    );
    assert_eq!(
        payload["route_class"], "direct_native",
        "a definition lookup routes deterministically: {payload}"
    );
    assert_eq!(payload["symbol"], "complete_refresh");
    assert_eq!(payload["found"], true);

    let definitions = payload["definitions"].as_array().expect("definitions");
    assert!(
        !definitions.is_empty(),
        "the definition site must be found: {payload}"
    );
    assert_eq!(definitions[0]["file"], "src/refresh.rs");
    assert!(definitions[0]["line"].as_u64().unwrap_or(0) >= 1);

    let references = payload["references"].as_array().expect("references");
    assert!(
        references.len() >= 2,
        "uses outside the definition file must be found: {payload}"
    );
    assert!(
        references
            .iter()
            .any(|location| location["file"] == "src/main.rs"),
        "references point at real fresh locations: {payload}"
    );
    assert!(
        payload["files_indexed"].as_u64().unwrap_or(0) >= 2,
        "the workspace was indexed for this request: {payload}"
    );

    gateway.shutdown().await;
}

#[tokio::test]
async fn plain_word_symbols_bind_through_the_question_cue() {
    let dir = test_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let ws = workspace(&dir);
    // `refresh_token` is snake_case, so it is a classifier candidate;
    // `complete_refresh` likewise. Ask about one with no underscore and
    // no capital to prove the cue fallback binds it.
    std::fs::write(ws.join("src/serve.rs"), "fn serve() {}\n").unwrap();
    std::fs::write(
        ws.join("src/use_serve.rs"),
        "fn boot() {\n    serve();\n}\n",
    )
    .unwrap();

    let gateway = start_with(&dir, lookup_only_runtime()).await.unwrap();
    let payload = ok(gateway.address(), query(&ws, "where is serve defined")).await;
    assert_eq!(payload["symbol"], "serve", "{payload}");
    assert_eq!(payload["model_calls"], 0);
    assert_eq!(payload["found"], true, "{payload}");
    gateway.shutdown().await;
}

#[tokio::test]
async fn a_question_that_would_need_a_model_is_refused_not_degraded() {
    let dir = test_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let ws = workspace(&dir);
    let gateway = start_with(&dir, lookup_only_runtime()).await.unwrap();

    let refusal = err(
        gateway.address(),
        query(
            &ws,
            "Redesign the scheduler to fix the race condition in the run loop",
        ),
    )
    .await;
    assert_eq!(
        code_of(&refusal),
        "requires_model",
        "reasoning work must be refused, never answered as a lookup: {refusal}"
    );
    gateway.shutdown().await;
}

#[tokio::test]
async fn a_question_with_no_symbol_is_refused() {
    let dir = test_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let ws = workspace(&dir);
    let gateway = start_with(&dir, lookup_only_runtime()).await.unwrap();

    let refusal = err(gateway.address(), query(&ws, "where is it")).await;
    assert_eq!(code_of(&refusal), "no_symbol", "{refusal}");

    let refusal = err(
        gateway.address(),
        query(&ws, "Where is definitely_not_here_at_all defined?"),
    )
    .await;
    assert_eq!(code_of(&refusal), "not_found", "{refusal}");
    gateway.shutdown().await;
}

#[tokio::test]
async fn an_empty_question_or_a_missing_workspace_is_refused() {
    let dir = test_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let ws = workspace(&dir);
    let gateway = start_with(&dir, lookup_only_runtime()).await.unwrap();

    let refusal = err(gateway.address(), query(&ws, "   ")).await;
    assert_eq!(code_of(&refusal), "invalid_question", "{refusal}");

    let missing = dir.join("no-such-workspace");
    let refusal = err(
        gateway.address(),
        query(&missing, "Where is serve defined?"),
    )
    .await;
    assert_eq!(code_of(&refusal), "workspace_not_found", "{refusal}");

    // The refusals above must not have needed a provider either, and the
    // working lookup still answers on the same gateway.
    let (status, _, detail) = send(
        gateway.address(),
        query(&ws, "Where is complete_refresh defined and used?"),
    )
    .await;
    assert_eq!(status, 200, "the working lookup still answers: {detail}");
    gateway.shutdown().await;
}
