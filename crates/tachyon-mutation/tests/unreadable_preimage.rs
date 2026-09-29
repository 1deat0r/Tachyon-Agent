//! §2.2: an existing-but-unreadable file is NOT an absent file. A create
//! spec (`base_hash: None`, "must not exist") must be refused, never
//! prepared, when the target exists but cannot be read.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;

use tachyon_mutation::{MutationEngine, MutationError, PatchSpec};

fn fixture() -> (PathBuf, PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "tachyon-unreadable-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let ws = root.join("ws");
    let state = root.join("state");
    std::fs::create_dir_all(&ws).expect("ws");
    (ws, state)
}

#[test]
fn unreadable_existing_file_refuses_a_create_spec() {
    let (ws, state) = fixture();
    let target = ws.join("create.rs");
    std::fs::write(&target, "existing content").expect("write target");

    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o000)).expect("chmod 0000");
    // Skip rather than lie: root bypasses the mode bits, so the unreadable
    // condition this test depends on cannot be created here.
    if std::fs::read(&target).is_ok() {
        eprintln!("skipping: effective uid ignores file modes");
        return;
    }

    let engine = MutationEngine::open(&ws, &state).expect("open engine");
    let spec = PatchSpec {
        path: "create.rs".to_owned(),
        base_hash: None,
        new_content: b"replacement".to_vec(),
    };

    match engine.prepare(&[spec]) {
        Err(MutationError::StalePreimage { .. } | MutationError::InvalidPath(_)) => {}
        Err(other) => panic!("expected a preimage/path refusal, got: {other}"),
        Ok(_) => panic!(
            "prepare accepted a create spec over an unreadable existing file: \
             an unreadable file was treated as absent"
        ),
    }
}

/// `file_hash` is the helper every other preimage check compares against, so
/// its own contract gets its own test: absent is `Ok(None)`, unreadable is
/// an `Err`, and the two are never interchangeable.
#[test]
fn file_hash_reports_unreadable_as_an_error_not_as_absence() {
    let (ws, _state) = fixture();
    let target = ws.join("present.rs");
    std::fs::write(&target, "content").expect("write target");
    let readable = std::fs::read(&target).expect("readable before chmod");
    assert_eq!(
        tachyon_mutation::file_hash(&target).expect("readable file hashes"),
        Some(tachyon_mutation::blake3_hex(&readable))
    );

    assert_eq!(
        tachyon_mutation::file_hash(&ws.join("never-existed.rs")).expect("missing file"),
        None,
        "a path that does not exist is absence"
    );

    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o000)).expect("chmod 0000");
    if std::fs::read(&target).is_ok() {
        eprintln!("skipping the unreadable half: effective uid ignores file modes");
        return;
    }
    let error = tachyon_mutation::file_hash(&target)
        .expect_err("an existing-but-unreadable file must be an error, not None");
    assert!(
        matches!(error, MutationError::InvalidPath(_)),
        "expected a refusal, got: {error}"
    );
}
