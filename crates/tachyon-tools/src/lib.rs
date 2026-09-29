//! Native capability registry and built-in tools (spec §28, M3).
//!
//! Every action flows through policy first: containment resolves the path,
//! the [`Policy`] decides, and [`Approvals`] resolve asks against the exact
//! operation hash. Tools never see raw credentials — only handles.

pub mod artifact;
pub mod credential;
pub mod fault;
pub mod fs;
pub mod git;
pub mod process;
pub mod registry;
pub mod workspace;

use std::path::{Path, PathBuf};
use tachyon_policy::{ApprovalRequest, Approvals, Policy, PolicyDecision, contain};
use tachyon_types::CapabilityId;
use thiserror::Error;

/// Tool failures.
#[derive(Debug, Error)]
pub enum ToolError {
    #[error("policy denied {capability} on {scope}: {reason}")]
    Denied {
        capability: String,
        scope: String,
        reason: String,
    },
    #[error("approval required for {capability} on {scope}")]
    ApprovalRequired {
        capability: String,
        scope: String,
        request: Box<ApprovalRequest>,
    },
    #[error("containment: {0}")]
    Containment(#[from] tachyon_policy::ContainmentError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("process cancelled before it started")]
    ProcessCancelledBeforeStart,
    #[error("process could not start: {0}")]
    ProcessStartFailed(String),
    #[error("process cancelled after it started")]
    ProcessCancelled,
    #[error("workspace lease cancelled")]
    WorkspaceLeaseCancelled,
    #[error("process timed out after {0:?}")]
    ProcessTimeout(std::time::Duration),
    #[error("process exited with {0}")]
    ProcessFailed(i32),
    #[error("git failed: {0}")]
    Git(String),
    #[error("unknown capability: {0}")]
    UnknownCapability(String),
    #[error("invalid arguments: {0}")]
    InvalidArgs(String),
}

/// Execution context shared by all tools.
pub struct ToolsContext {
    pub workspace_root: PathBuf,
    pub policy: Policy,
    pub approvals: Approvals,
    pub artifacts: artifact::ArtifactSpool,
    pub credentials: credential::CredentialBroker,
    /// M11 slice 1: the run-held workspace lease, attached by hosts that
    /// took the lease on the pinned canonical root before spawning a run
    /// (gateway `StartRun` prepare). Private on purpose: drive-reachable
    /// inner acquisitions must go through [`ToolsContext::workspace_lease`]
    /// and reuse this guard — the lock is not reentrant, so a second
    /// acquisition on the same root would self-deadlock.
    held_lease: Option<workspace::WorkspaceLease>,
}

impl ToolsContext {
    #[must_use]
    pub fn new(
        workspace_root: PathBuf,
        policy: Policy,
        artifacts: artifact::ArtifactSpool,
    ) -> Self {
        // Canonicalize once: scope resolution strips this prefix from
        // canonical file paths, so a symlinked root (macOS /var → /private/var)
        // would otherwise silently miss deny scopes. Best-effort — a root that
        // does not exist yet stays as given until it does.
        let workspace_root = std::fs::canonicalize(&workspace_root).unwrap_or(workspace_root);
        Self {
            workspace_root,
            policy,
            approvals: Approvals::default(),
            artifacts,
            credentials: credential::CredentialBroker::default(),
            held_lease: None,
        }
    }

    /// Single-source constructor (M11 slice 5): the caller already
    /// canonicalized the root (gateway `StartRun` prepare step 3, which
    /// also pins it durably), so this value IS the policy, evidence and
    /// mutation root — NO filesystem resolution happens here. A second
    /// `canonicalize` across the pin round-trip would be an await window
    /// where the policy root could diverge from the durable pin; this
    /// constructor makes that divergence impossible by construction.
    #[must_use]
    pub fn new_from_canonical(
        workspace_root: PathBuf,
        policy: Policy,
        artifacts: artifact::ArtifactSpool,
    ) -> Self {
        Self {
            workspace_root,
            policy,
            approvals: Approvals::default(),
            artifacts,
            credentials: credential::CredentialBroker::default(),
            held_lease: None,
        }
    }

    /// Attaches the run-held workspace lease. The guard's lifetime is the
    /// context's: every stage the run reaches through this context is
    /// protected, and the lease releases when the last holder drops it.
    #[must_use]
    pub fn with_workspace_lease(mut self, lease: workspace::WorkspaceLease) -> Self {
        self.held_lease = Some(lease);
        self
    }

    /// Attaches a credential broker. Registered provider keys must be
    /// scrubbed before any process output this context produces is
    /// recorded, so hosts pass their runtime redactor instead of leaving
    /// the empty default broker the constructors install.
    #[must_use]
    pub fn with_credentials(mut self, broker: credential::CredentialBroker) -> Self {
        self.credentials = broker;
        self
    }

    /// The lease this context carries, if any. Inner workspace stages
    /// (verification capture/plan, the verify runner) must clone this
    /// guard instead of acquiring: the registry lock is per-canonical-root
    /// and NOT reentrant.
    #[must_use]
    pub fn workspace_lease(&self) -> Option<&workspace::WorkspaceLease> {
        self.held_lease.as_ref()
    }
}

/// Resolves `requested` against the workspace and maps it to a policy
/// scope: inside → `workspace/<rel>`; absolute-outside → `external:<abs>`;
/// traversal → hard [`ToolError::Containment`] (never policy-bypassable).
pub fn resolve_scope(
    workspace_root: &Path,
    requested: &Path,
) -> Result<(PathBuf, String), ToolError> {
    let depth_check = || -> Result<(), ToolError> {
        if tachyon_policy::lexical_contained(workspace_root, requested) {
            Ok(())
        } else {
            Err(ToolError::Containment(
                tachyon_policy::ContainmentError::Traversal(requested.display().to_string()),
            ))
        }
    };
    depth_check()?;
    match contain(workspace_root, requested) {
        Ok(resolved) => {
            let rel = resolved
                .strip_prefix(workspace_root)
                .unwrap_or(Path::new(""))
                .to_string_lossy()
                .replace('\\', "/");
            Ok((resolved, format!("workspace/{rel}")))
        }
        Err(tachyon_policy::ContainmentError::AbsoluteOutsideRequest(_)) => {
            let abs = if requested.is_absolute() {
                requested.to_path_buf()
            } else {
                workspace_root.join(requested)
            };
            Ok((
                abs.clone(),
                format!("external:{}", abs.to_string_lossy().replace('\\', "/")),
            ))
        }
        Err(other) => Err(ToolError::Containment(other)),
    }
}

/// Enforces policy for one operation. Returns `Ok(())` on allow
/// (directly or via a bound approval); maps deny/ask to [`ToolError`].
pub fn authorize(
    policy: &Policy,
    approvals: &Approvals,
    capability: &str,
    scope: &str,
    operation: &serde_json::Value,
    summary: &str,
) -> Result<(), ToolError> {
    authorize_inner(
        policy, approvals, capability, scope, operation, summary, false,
    )
}

/// [`authorize`] for a PRE-EFFECT gate (M11): identical policy decision
/// and identical typed ask, but a present one-shot grant is SATISFIED
/// without being consumed — the grant's single use stays available for
/// the per-effect recheck that actually mutates. Without this split a
/// gate that re-runs after each grant burns the grant of every earlier
/// checked operation and the run re-parks exponentially instead of
/// proceeding on exactly-one re-run.
pub fn authorize_peek(
    policy: &Policy,
    approvals: &Approvals,
    capability: &str,
    scope: &str,
    operation: &serde_json::Value,
    summary: &str,
) -> Result<(), ToolError> {
    authorize_inner(
        policy, approvals, capability, scope, operation, summary, true,
    )
}

#[allow(clippy::too_many_arguments)]
fn authorize_inner(
    policy: &Policy,
    approvals: &Approvals,
    capability: &str,
    scope: &str,
    operation: &serde_json::Value,
    summary: &str,
    peek: bool,
) -> Result<(), ToolError> {
    let capability_id = CapabilityId(capability.to_owned());
    match policy.decide(&capability_id, scope, operation, summary) {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::Deny { reason } => Err(ToolError::Denied {
            capability: capability.to_owned(),
            scope: scope.to_owned(),
            reason,
        }),
        PolicyDecision::Ask { request } => {
            let resolution = if peek {
                approvals.resolve_peek(&request, operation)
            } else {
                approvals.resolve(&request, operation)
            };
            match resolution {
                PolicyDecision::Allow => Ok(()),
                PolicyDecision::Deny { reason } => Err(ToolError::Denied {
                    capability: capability.to_owned(),
                    scope: scope.to_owned(),
                    reason,
                }),
                PolicyDecision::Ask { request } => Err(ToolError::ApprovalRequired {
                    capability: capability.to_owned(),
                    scope: scope.to_owned(),
                    request: Box::new(request),
                }),
            }
        }
    }
}
