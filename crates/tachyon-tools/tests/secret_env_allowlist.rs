//! §2.1 (a): a secret already in the parent environment must never reach a
//! spawned child, so it cannot appear in the receipt's inline body or in the
//! artifact spool that the inline body overflows into.
//!
//! The padding pushes the secret past `INLINE_CAP`, so an unfiltered
//! environment fails on the *spool* assertion, not just the inline one.
#![cfg(unix)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tachyon_policy::{DefaultPosture, Policy};
use tachyon_tools::ToolsContext;
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_tools::process::{self, ProcessSpec};

const SECRET_VAR: &str = "TACHYON_TEST_SECRET";
const SECRET: &str = "hunter2";
/// Past `INLINE_CAP` (1 MiB), so the tail of the stream lands in the spool.
const PADDING: usize = 1_200_000;

fn fixture() -> (PathBuf, ToolsContext) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "tachyon-secret-env-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).expect("create fixture root");
    let mut policy = Policy::new(DefaultPosture::Deny);
    policy.allow("process.spawn", "/bin/sh");
    let context = ToolsContext::new(
        root.clone(),
        policy,
        ArtifactSpool::new(root.join("artifacts")),
    );
    (root, context)
}

#[tokio::test]
// SAFETY: this is the only test in its own integration-test binary, so no
// other thread in this process reads the environment concurrently. The
// parent environment is the seam under test: the runner must not pass it on.
#[allow(unsafe_code)]
async fn inherited_parent_secret_reaches_neither_inline_receipt_nor_artifact_spool() {
    unsafe { std::env::set_var(SECRET_VAR, SECRET) };

    let (root, context) = fixture();
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec![
        "-c".to_owned(),
        format!("head -c {PADDING} /dev/zero | tr '\\0' 'a'; printf '%s' \"${SECRET_VAR}\""),
    ];
    spec.cwd = Some(root.clone());
    spec.timeout = Duration::from_secs(20);

    let receipt = process::run(&context, &spec).await.expect("child runs");
    assert_eq!(receipt.exit_code, Some(0), "child completed");

    let body = |bytes: &[u8]| String::from_utf8_lossy(bytes).contains(SECRET);
    assert!(
        receipt.stdout_truncated,
        "padding must push the secret past the inline cap so the spool is exercised"
    );
    assert!(
        !body(&receipt.stdout),
        "secret leaked into the inline receipt body"
    );
    assert!(
        !body(&receipt.stderr),
        "secret leaked into the inline receipt stderr"
    );

    let artifact = receipt.stdout_artifact.expect("overflow stream is spooled");
    let spooled = context
        .artifacts
        .fetch(&artifact)
        .expect("fetch the overflow artifact");
    assert!(!body(&spooled), "secret leaked into the artifact spool");
}
