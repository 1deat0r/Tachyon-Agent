//! Provider-neutral, authoritative completion terms. All clauses are required.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use tachyon_types::NodeId;
use thiserror::Error;
use uuid::Uuid;

/// Failure to prepare or evaluate verification; never interpreted as success.
#[derive(Debug, Error)]
pub enum VerifyError {
    #[error("invalid acceptance contract: {0}")]
    InvalidContract(String),
    #[error("verification IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("verification blocked: {0}")]
    Blocked(String),
    /// A check worker ended without proving whether its process effect settled.
    /// The supervisor must persist interruption and must not replay it blindly.
    #[error("verification outcome is unknown for node {node_id:?}: {reason}")]
    UnknownOutcome {
        node_id: Option<NodeId>,
        reason: String,
    },
    /// The policy asked for a human decision before the exact acceptance
    /// operation may proceed (M11 typed parking). Typed through the
    /// runner and the core so the driver parks instead of recording a
    /// failed check; carries the pending approval request itself.
    #[error("approval required for {} on {}", .0.capability.0, .0.scope)]
    ApprovalRequired(tachyon_policy::ApprovalRequest),
}

/// A direct command. No shell interpolation; argv is passed verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCheck {
    pub program: String,
    pub args: Vec<String>,
    /// Normalized workspace-relative directory (`.` means the root).
    pub cwd: String,
    pub env: BTreeMap<String, String>,
    pub timeout_ms: u64,
}

/// Machine-checkable terms plus explicit unresolved semantic requirements.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum Clause {
    CommandPasses {
        command: CommandCheck,
    },
    /// An exact relative path must retain its baseline bytes/existence.
    FileUnchanged {
        path: String,
    },
    /// Each actual changed path must be one of these paths or descendants.
    ChangedPathsWithin {
        paths: Vec<String>,
    },
    /// A trusted runtime binds a user constraint to an executable requirement.
    HardConstraint {
        id: Uuid,
        text: String,
        check: Box<Clause>,
    },
    /// Legacy text and semantic requirements fail closed, not silently pass.
    Unresolved {
        description: String,
    },
}

/// The supervisor owns this contract. Model proposals cannot replace it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceContract {
    #[serde(deserialize_with = "read_clauses")]
    pub clauses: Vec<Clause>,
}

impl AcceptanceContract {
    /// Reject vacuous completion before planning or executing anything.
    pub fn validate(&self) -> Result<(), VerifyError> {
        if self.clauses.is_empty() {
            return Err(VerifyError::InvalidContract("no required clauses".into()));
        }
        let mut hard_ids = BTreeSet::new();
        for clause in &self.clauses {
            validate_clause(clause, false)?;
            if let Clause::HardConstraint { id, .. } = clause
                && !hard_ids.insert(*id)
            {
                return Err(VerifyError::InvalidContract(
                    "duplicate hard constraint".into(),
                ));
            }
        }
        Ok(())
    }
}

fn validate_clause(clause: &Clause, nested: bool) -> Result<(), VerifyError> {
    match clause {
        Clause::CommandPasses { command } => command.validate(),
        Clause::FileUnchanged { path } => validate_source_path(path, false),
        Clause::ChangedPathsWithin { paths } => {
            for path in paths {
                validate_source_path(path, true)?;
            }
            Ok(())
        }
        Clause::HardConstraint { text, check, .. } => {
            // Hard bindings exist only at the contract's top level. Refuse
            // nesting rather than allowing recursive bypasses or deep stacks.
            if nested || text.trim().is_empty() {
                return Err(VerifyError::InvalidContract(
                    "invalid nested/empty hard constraint".into(),
                ));
            }
            validate_clause(check, true)
        }
        Clause::Unresolved { .. } => Ok(()),
    }
}

pub(crate) fn validate_source_path(path: &str, allow_root: bool) -> Result<(), VerifyError> {
    validate_path(path, allow_root)?;
    if path
        .split('/')
        .any(|part| matches!(part, ".git" | "target"))
    {
        return Err(VerifyError::InvalidContract(
            "excluded path has no source coverage".into(),
        ));
    }
    Ok(())
}

/// Maximum direct command wall time (ten minutes).
pub const MAX_COMMAND_TIMEOUT_MS: u64 = 600_000;

impl CommandCheck {
    pub fn validate(&self) -> Result<(), VerifyError> {
        if self.program.trim().is_empty()
            || self.program.contains('\0')
            || self.args.iter().any(|arg| arg.contains('\0'))
            || self.env.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            })
        {
            return Err(VerifyError::InvalidContract(
                "invalid direct command".into(),
            ));
        }
        if !(1..=MAX_COMMAND_TIMEOUT_MS).contains(&self.timeout_ms) {
            return Err(VerifyError::InvalidContract(
                "unbounded command timeout".into(),
            ));
        }
        validate_path(&self.cwd, true)
    }
}

pub(crate) fn validate_path(path: &str, allow_root: bool) -> Result<(), VerifyError> {
    if allow_root && path == "." {
        return Ok(());
    }
    if path.is_empty()
        || path.contains(['\\', '\0', ':'])
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(VerifyError::InvalidContract(format!(
            "non-normalized relative path: {path:?}"
        )));
    }
    Ok(())
}

fn read_clauses<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Vec<Clause>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StoredClause {
        Typed(Clause),
        Legacy(String),
    }
    Vec::<StoredClause>::deserialize(de).map(|items| {
        items
            .into_iter()
            .map(|item| match item {
                StoredClause::Typed(clause) => clause,
                StoredClause::Legacy(description) => Clause::Unresolved { description },
            })
            .collect()
    })
}
