//! Mutation batches: specs, hashes, and per-file state (spec §20).
//!
//! Filesystem multi-file mutation is **recoverable**, not globally atomic.
//! Each file moves `Planned → Prepared → Committed`, with `RolledBack` and
//! `Diverged` as recovery outcomes. A batch is coherent when every file is
//! terminal (`Committed`, `RolledBack`, or explicitly `Diverged`) and the
//! journal agrees — never when a model says so.

use serde::{Deserialize, Serialize};
use tachyon_types::ArtifactId;

/// BLAKE3 hex of bytes.
#[must_use]
pub fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// BLAKE3 hex of a file's current content, `None` only when the file does
/// not exist. Any other read failure is an error: an unreadable file must
/// never be mistaken for an absent one, or a `base_hash: None` create spec
/// would sail through the preimage check over a file it is about to clobber.
pub fn file_hash(path: &std::path::Path) -> Result<Option<String>, MutationError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(blake3_hex(&bytes))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(MutationError::InvalidPath(format!(
            "{}: {e}",
            path.display()
        ))),
    }
}

/// Normalizes a workspace-relative path to journal-key form: `/`-separated,
/// `.` and empty segments dropped, so `sub/./f.rs` and `sub/f.rs` are the
/// same file and duplicate specs cannot alias. Absolute paths and `..`
/// are rejected here; deeper traversal and symlink escapes are rejected
/// by policy containment at the engine boundary.
pub fn normalize_rel(path: &str) -> Result<String, MutationError> {
    let rel = path.replace('\\', "/");
    let rel = rel.strip_prefix("./").unwrap_or(&rel);
    if rel.is_empty() || rel.starts_with('/') {
        return Err(MutationError::InvalidPath(path.to_owned()));
    }
    let mut parts = Vec::new();
    for part in rel.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(MutationError::InvalidPath(path.to_owned()));
        }
        parts.push(part);
    }
    if parts.is_empty() {
        return Err(MutationError::InvalidPath(path.to_owned()));
    }
    Ok(parts.join("/"))
}

/// One file replacement: full new content plus the base hash the author
/// saw. `base_hash` is `None` only when creating a file that must not
/// exist; anything else present-or-changed is [`MutationError::StalePreimage`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PatchSpec {
    /// Workspace-relative path in journal-key form.
    pub path: String,
    /// BLAKE3 the author based this patch on (`None` = must not exist).
    pub base_hash: Option<String>,
    /// Complete replacement content.
    pub new_content: Vec<u8>,
}

/// Per-file commit state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileState {
    /// Planned, temp not yet written.
    #[default]
    Planned,
    /// Temp written, journaled, not yet renamed.
    Prepared,
    /// Renamed into place and journaled.
    Committed,
    /// Preimage restored after abort (recovery outcome).
    RolledBack,
    /// Current content matches neither pre- nor postimage: external hands
    /// touched the file. Recovery never writes diverged files.
    Diverged,
}

impl FileState {
    /// Whether the file needs no further work.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        match self {
            Self::Planned | Self::Prepared => false,
            Self::Committed | Self::RolledBack | Self::Diverged => true,
        }
    }
}

/// One file inside a batch: hashes, preimage pointer, retained postimage,
/// temp location, state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMutation {
    /// Workspace-relative path in journal-key form.
    pub path: String,
    /// Hash observed at prepare (`None` = file must be created).
    pub pre_hash: Option<String>,
    /// Artifact holding the preimage bytes (`None` for creates).
    pub pre_artifact: Option<ArtifactId>,
    /// Intended hash after commit.
    pub post_hash: String,
    /// Artifact holding the staged postimage bytes, for re-staging a lost
    /// temp without re-asking the author.
    pub post_artifact: ArtifactId,
    /// Temp file name (same directory as target, same filesystem).
    pub temp_name: String,
    /// Current state.
    pub state: FileState,
}

/// Mutation failures. `StalePreimage` is the core guard: never silently
/// patch different content.
#[derive(Clone, Debug, PartialEq, thiserror::Error, Serialize, Deserialize)]
pub enum MutationError {
    /// Current content differs from the recorded base. Fail closed.
    #[error("stale preimage for {path}: expected {}, found {}",
        .expected.as_deref().unwrap_or("<absent>"),
        .actual.as_deref().unwrap_or("<absent>"))]
    StalePreimage {
        /// File in journal-key form.
        path: String,
        /// Hash the patch was based on (`None` = must not exist).
        expected: Option<String>,
        /// Hash actually on disk (`None` = missing).
        actual: Option<String>,
    },
    /// Content changed under a commit: post-rename verification found
    /// non-postimage bytes. The batch aborts; recovery reports the file
    /// diverged. Never retried — the interleaving is already lost.
    #[error("diverged during commit for {path}")]
    Diverged {
        /// File in journal-key form.
        path: String,
    },
    /// Batch id unknown to the journal: the plan was never persisted, so
    /// there is nothing safe to commit. Covers fabricated descriptors.
    #[error("unknown mutation batch: {0}")]
    UnknownBatch(String),
    /// Batch was compensated (rolled back); its descriptors are dead.
    /// Resume with a fresh prepare, never by recommitting.
    #[error("mutation batch compensated, descriptor dead: {0}")]
    Compensated(String),
    /// Batch already completed; further commits are refused.
    #[error("mutation batch already completed: {0}")]
    AlreadyCompleted(String),
    /// Path rejected before containment, or a caller-shape bug
    /// (empty batch, duplicate path). Never transient.
    #[error("invalid mutation path: {0}")]
    InvalidPath(String),
    /// Strict recovery refused before an unsafe or unauthorized effect.
    #[error("mutation recovery blocked: {0}")]
    RecoveryBlocked(String),
    /// Transient IO below the batch logic (disk full, permission flap).
    /// Retryable only before anything committed.
    #[error("mutation IO failure: {0}")]
    Io(String),
    /// Journal tail corrupt beyond the torn-write repair.
    #[error("mutation journal corrupt at line {0}")]
    JournalCorrupt(usize),
    /// The policy asked for a human decision before this exact operation
    /// may proceed (M11 typed parking). Surfaced typed so a
    /// supervisor-owned run parks instead of failing; carries the pending
    /// approval request itself.
    #[error("approval required for {} on {}", .0.capability.0, .0.scope)]
    ApprovalRequired(tachyon_policy::ApprovalRequest),
}

impl MutationError {
    /// Whether the caller may retry the same call verbatim. Commits are
    /// never blindly retried after a crash: run recovery first, then act
    /// on its report. Only `Io` (transient, pre-commit) is retryable, and
    /// only before anything committed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Io(_) => true,
            Self::StalePreimage { .. }
            | Self::Diverged { .. }
            | Self::UnknownBatch(_)
            | Self::Compensated(_)
            | Self::AlreadyCompleted(_)
            | Self::InvalidPath(_)
            | Self::RecoveryBlocked(_)
            | Self::JournalCorrupt(_)
            | Self::ApprovalRequired(_) => false,
        }
    }
}

impl From<std::io::Error> for MutationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<tachyon_policy::ContainmentError> for MutationError {
    fn from(error: tachyon_policy::ContainmentError) -> Self {
        // Caller bug (bad path), never transient: InvalidPath, not Io.
        Self::InvalidPath(format!("containment: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_rejects_escape() {
        assert!(normalize_rel("src/a.rs").is_ok());
        assert!(normalize_rel("./src/a.rs").is_ok());
        assert!(normalize_rel("/abs").is_err());
        assert!(normalize_rel("../out").is_err());
        assert!(normalize_rel("a/../../b").is_err());
        assert!(normalize_rel("").is_err());
    }

    #[test]
    fn terminal_states_cover_outcomes() {
        assert!(!FileState::Planned.is_terminal());
        assert!(!FileState::Prepared.is_terminal());
        assert!(FileState::Committed.is_terminal());
        assert!(FileState::RolledBack.is_terminal());
        assert!(FileState::Diverged.is_terminal());
    }

    #[test]
    fn stale_preimage_never_retries() {
        let error = MutationError::StalePreimage {
            path: "a".to_owned(),
            expected: None,
            actual: None,
        };
        assert!(!error.is_retryable());
        assert!(error.to_string().contains("stale preimage"));
    }
}
