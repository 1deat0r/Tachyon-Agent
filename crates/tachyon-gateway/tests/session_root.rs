//! Ticket 01 (ACP slice a): durable Session root through the gateway.
//!
//! External contract under test —
//!
//!   * `CreateSession` with a workspace root canonicalizes it through the
//!     existing workspace validation, rejects roots that fail that
//!     validation with a typed error, and persists the bound root;
//!   * `CreateSession` requires a supplied root to be an absolute path —
//!     a relative input is refused with `workspace_not_absolute` before
//!     any workspace validation runs;
//!   * `CreateSession` without a root keeps legacy behavior and reports
//!     an absent root on fetch;
//!   * `GetSession` returns session identity + optional root, refuses an
//!     unknown id with a typed error, and is strictly read-only;
//!   * the bound root survives a gateway restart over the same store dir.

use std::path::{Path, PathBuf};

use tachyon_gateway::start;
use tachyon_protocol::Command;
use tachyon_types::SessionId;

mod common;
use common::{code_of, err, ok, test_dir};

/// Creates the workspace directory a test binds as Session root and
/// returns it canonicalized (the exact value the gateway must persist).
fn make_workspace(dir: &Path) -> PathBuf {
    let ws = dir.join("session-ws");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::canonicalize(&ws).unwrap()
}

async fn fetch_session(socket: &Path, session_id: &str) -> serde_json::Value {
    ok(
        socket,
        Command::GetSession {
            session_id: session_id.parse().unwrap(),
        },
    )
    .await
}

#[tokio::test]
async fn session_root_round_trips_and_survives_restart() {
    let dir = test_dir();
    let ws = make_workspace(&dir);

    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: Some(ws.display().to_string()),
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();

    let fetched = fetch_session(&socket, &session_id).await;
    assert_eq!(fetched["session_id"], session_id.as_str());
    assert_eq!(
        fetched["workspace_root"],
        ws.display().to_string().as_str(),
        "the bound Session root is the canonical workspace path"
    );

    // Real stop over the same store dir — the same shape a process kill
    // leaves on disk (recovery.rs pattern).
    gateway.shutdown().await;
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let again = fetch_session(&socket, &session_id).await;
    assert_eq!(
        again, fetched,
        "session identity and root must be identical across a gateway restart"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn create_session_without_root_keeps_legacy_behavior() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: None,
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();

    let fetched = fetch_session(&socket, &session_id).await;
    assert_eq!(fetched["session_id"], session_id.as_str());
    assert!(
        fetched["workspace_root"].is_null(),
        "legacy sessions report an absent Session root: {fetched}"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn create_session_rejects_roots_that_fail_workspace_validation() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    // Missing root: never existed -> typed workspace_not_found.
    let got = err(
        &socket,
        Command::CreateSession {
            workspace_root: Some(dir.join("no-such-root").display().to_string()),
        },
    )
    .await;
    assert_eq!(code_of(&got), "workspace_not_found", "{got}");

    // A file is not a workspace root -> typed workspace_not_a_dir.
    let file = dir.join("not-a-dir");
    std::fs::write(&file, b"not a workspace").unwrap();
    let got = err(
        &socket,
        Command::CreateSession {
            workspace_root: Some(file.display().to_string()),
        },
    )
    .await;
    assert_eq!(code_of(&got), "workspace_not_a_dir", "{got}");

    // Non-canonicalizable: a self-referential symlink loop never resolves
    // -> typed workspace_not_canonical.
    #[cfg(unix)]
    {
        let loop_link = dir.join("loop");
        std::os::unix::fs::symlink("loop", &loop_link).unwrap();
        let got = err(
            &socket,
            Command::CreateSession {
                workspace_root: Some(loop_link.display().to_string()),
            },
        )
        .await;
        assert_eq!(code_of(&got), "workspace_not_canonical", "{got}");
    }

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A supplied root that is not an absolute path is refused with the
/// typed `workspace_not_absolute` BEFORE workspace validation (a
/// relative input would otherwise canonicalize against the gateway's
/// cwd); an absolute valid root in the same session still succeeds.
#[tokio::test]
async fn create_session_rejects_relative_workspace_root() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    // Relative input — regardless of existence — is a typed refusal.
    let got = err(
        &socket,
        Command::CreateSession {
            workspace_root: Some("./relative-root".to_owned()),
        },
    )
    .await;
    assert_eq!(code_of(&got), "workspace_not_absolute", "{got}");

    // Absolute valid root still succeeds in the same gateway.
    let ws = make_workspace(&dir);
    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: Some(ws.display().to_string()),
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();
    let fetched = fetch_session(&socket, &session_id).await;
    assert_eq!(
        fetched["workspace_root"],
        ws.display().to_string().as_str(),
        "an absolute root still binds the canonical workspace path"
    );

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn get_session_unknown_id_is_a_typed_error() {
    let dir = test_dir();
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let unknown = SessionId::generate();
    let got = err(
        &socket,
        Command::GetSession {
            session_id: unknown,
        },
    )
    .await;
    assert_eq!(code_of(&got), "unknown_session", "{got}");

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn get_session_is_read_only() {
    let dir = test_dir();
    let ws = make_workspace(&dir);
    let gateway = start(&dir).await.unwrap();
    let socket = gateway.address().to_owned();

    let created = ok(
        &socket,
        Command::CreateSession {
            workspace_root: Some(ws.display().to_string()),
        },
    )
    .await;
    let session_id: String = created["session_id"].as_str().unwrap().to_owned();

    let before = gateway
        .store()
        .load_session(&session_id)
        .await
        .unwrap()
        .expect("the created session exists");
    let first = fetch_session(&socket, &session_id).await;
    let second = fetch_session(&socket, &session_id).await;
    let after = gateway
        .store()
        .load_session(&session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, second, "repeated fetches return identical payloads");
    assert_eq!(
        before, after,
        "fetching a session must not mutate its durable row"
    );

    // No state was minted: the session lists no tasks, and it still
    // accepts work normally after the fetches.
    let listed = ok(
        &socket,
        Command::ListTasks {
            session_id: Some(session_id.parse().unwrap()),
        },
    )
    .await;
    assert!(listed["tasks"].as_array().unwrap().is_empty());
    let task = ok(
        &socket,
        Command::CreateTask {
            session_id: session_id.parse().unwrap(),
            objective: "work after fetch".to_owned(),
            idempotency_key: None,
        },
    )
    .await;
    assert!(task["task_id"].is_string());

    gateway.shutdown().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
