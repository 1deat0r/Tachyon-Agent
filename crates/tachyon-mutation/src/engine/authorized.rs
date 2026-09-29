//! Policy-bound normal preparation and authorized per-file commit boundaries.
//!
//! The trusted runtime reserves a batch identity, persists it, and calls
//! [`MutationEngine::prepare_authorized`] under the shared workspace lease.
//! Every required capability on the exact target and derived temp path is
//! authorized — with the batch identity, plan paths and pre/post hashes bound
//! into the operation — before any journal, spool or workspace write.
//! [`MutationEngine::commit_authorized_up_to`] then authorizes each pending
//! file before its real rename, deriving the pending set from strict
//! task-scoped journal truth so the original descriptor can be reused across
//! per-file cancellation boundaries. Neither entry grants task completion:
//! a returned receipt still requires fresh verification by the runtime.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tachyon_tools::{ToolError, ToolsContext, artifact::ArtifactSpool, authorize, authorize_peek};
use tachyon_types::MutationBatchId;

use super::scoped::valid_hash;
use super::{
    ChangedFile, CommitReport, FileMutation, FileState, JournalRecord, MutationEngine,
    MutationError, PatchSpec, PreparedBatch, TEMP_MARKER, Timestamp, Transition, blake3_hex,
    file_hash, normalize_rel, sync_parent, write_new_synced,
};
use crate::journal::ReplayedBatch;

/// Capabilities required on each exact target path for a normal write.
const TARGET_CAPABILITIES: [&str; 4] = ["fs.metadata", "fs.read", "mutation.patch", "fs.write"];

/// Capabilities required on the exact derived temp path before preparation.
const TEMP_PREPARE_CAPABILITIES: [&str; 3] = ["fs.metadata", "fs.write", "fs.delete"];

/// Capabilities required on the exact derived temp path before each commit:
/// reading verifies the staged postimage, writing re-stages a lost temp, and
/// deleting consumes it.
const TEMP_COMMIT_CAPABILITIES: [&str; 4] = ["fs.metadata", "fs.read", "fs.write", "fs.delete"];

/// One exact policy operation the engine authorizes, in check order.
///
/// `operation` is the JSON bound into any approval hash: it carries the batch
/// identity, the scope, the capability and the plan's paths and hashes, so a
/// materially different batch or file cannot satisfy an approval. The runtime
/// may use these lists (see [`MutationEngine::prepare_authorizations`] and
/// [`MutationEngine::commit_authorizations`]) to display or pre-approve exactly
/// what a call will ask for; the calls rebuild the same list internally.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AuthorizedOp {
    /// Capability, e.g. `mutation.patch`.
    pub capability: String,
    /// Policy scope, e.g. `workspace/src/lib.rs` or `external:<abs path>`.
    pub scope: String,
    /// Exact operation JSON an approval must bind to.
    pub operation: serde_json::Value,
}

impl MutationEngine {
    /// Preflights exact policy for a caller-reserved batch identity, then
    /// prepares it. Refusals leave the workspace, the artifact spool and the
    /// journal untouched: a failed preflight performs zero workspace mutation.
    ///
    /// The journal of this attempt directory must be strict-clean and must not
    /// already hold this identity (reused id or sibling batch), so a crash
    /// before the complete `BatchStarted` record stays `Unknown` and is never
    /// blindly replayed. Existing unowned temp occupants are refused even when
    /// their bytes match the intended postimage.
    pub fn prepare_authorized(
        &self,
        context: &ToolsContext,
        batch_id: MutationBatchId,
        specs: &[PatchSpec],
    ) -> Result<PreparedBatch, MutationError> {
        let scope = AuthorizedScope::open(self, context)?;
        let ops = self.prepare_authorizations(batch_id, specs)?;
        // 1. Exact authorization for every target and derived temp, first.
        //    Pre-effect gate: a present grant satisfies without consuming
        //    (M11) — the single use belongs to the per-effect recheck.
        for op in &ops {
            scope.gate(op)?;
        }
        // 2. Attempt identity from strict journal truth.
        match self.journal.replay_scoped(batch_id) {
            Err(MutationError::UnknownBatch(_)) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(blocked(
                    "batch identity is already journaled in this attempt directory",
                ));
            }
        }
        // 3. Literal sources, unoccupied derived temps, exact preimages.
        for spec in specs {
            scope.preflight_spec(spec, &batch_id)?;
        }
        // Preferred staging re-verifies every preimage and writes temps with
        // `create_new`, so no path is ever truncated into ownership.
        self.prepare_with_id(batch_id, specs)
    }

    /// The exact policy operations a normal preparation requires, in check
    /// order: for each file the target capabilities, then the derived temp
    /// capabilities. Read-only; no journal, policy or filesystem side effects.
    pub fn prepare_authorizations(
        &self,
        batch_id: MutationBatchId,
        specs: &[PatchSpec],
    ) -> Result<Vec<AuthorizedOp>, MutationError> {
        let plan = specs_plan(specs);
        let mut seen = BTreeSet::new();
        let mut ops = Vec::new();
        for spec in specs {
            let rel = normalize_rel(&spec.path)?;
            if rel != spec.path {
                return Err(MutationError::InvalidPath(format!(
                    "path is not in normalized journal-key form: {}",
                    spec.path
                )));
            }
            if !seen.insert(rel.clone()) {
                return Err(MutationError::InvalidPath(format!("duplicate path: {rel}")));
            }
            let temp_rel = temp_rel(&rel, &batch_id)?;
            if !seen.insert(temp_rel.clone()) {
                return Err(MutationError::InvalidPath(format!(
                    "derived temp aliases another path: {temp_rel}"
                )));
            }
            for (scope_rel, capabilities) in [
                (&rel, &TARGET_CAPABILITIES[..]),
                (&temp_rel, &TEMP_PREPARE_CAPABILITIES[..]),
            ] {
                for capability in capabilities {
                    let scope = format!("workspace/{scope_rel}");
                    ops.push(AuthorizedOp {
                        capability: (*capability).to_owned(),
                        operation: prepare_operation(batch_id, capability, &scope, &plan),
                        scope,
                    });
                }
            }
        }
        if ops.is_empty() {
            return Err(MutationError::InvalidPath(
                "batch holds no files".to_owned(),
            ));
        }
        Ok(ops)
    }

    /// The exact policy operations the next commit boundary requires: for each
    /// pending file authorized by this boundary, the target capabilities then
    /// the derived temp capabilities. Pending files come from strict
    /// task-scoped journal truth, never from the descriptor's state fields.
    pub fn commit_authorizations(
        &self,
        prepared: &PreparedBatch,
        limit: usize,
    ) -> Result<Vec<AuthorizedOp>, MutationError> {
        let batch = self.strict_batch(prepared)?;
        let plan = batch_plan(&batch);
        let mut ops = Vec::new();
        for file in pending_files(&batch, limit) {
            ops.extend(commit_file_ops(prepared.id, file, limit, &plan)?);
        }
        Ok(ops)
    }

    /// Commits at most `limit` pending files of `prepared`, authorizing every
    /// operation for that boundary before the first rename and re-authorizing,
    /// re-hashing and re-reading the journal immediately before each real
    /// rename. `limit = 1` is the runtime's per-file cancellation boundary.
    ///
    /// The descriptor is validated against strict task-scoped journal truth and
    /// may be the original prepared batch: already committed files are derived
    /// from the journal. A refusal — unknown/sibling/malformed identity, policy
    /// denial or unresolved ask, completed or compensated batch, stale source,
    /// or a foreign temp occupant — changes nothing and writes no receipt.
    /// `completed: true` is a batch receipt, not task completion authority, and
    /// an unknown effect after a crash still requires `recover_scoped`.
    pub fn commit_authorized_up_to(
        &self,
        context: &ToolsContext,
        prepared: &PreparedBatch,
        limit: usize,
    ) -> Result<CommitReport, MutationError> {
        let scope = AuthorizedScope::open(self, context)?;
        if prepared.files.is_empty() {
            return Err(MutationError::InvalidPath(
                "batch holds no files".to_owned(),
            ));
        }
        let batch = self.strict_batch(prepared)?;
        let plan = batch_plan(&batch);
        // 1. The whole boundary, from journal truth, before any effect.
        let boundary: Vec<&FileMutation> = pending_files(&batch, limit).collect();
        let mut ops = Vec::new();
        for file in &boundary {
            ops.extend(commit_file_ops(prepared.id, file, limit, &plan)?);
        }
        // 2. Exact authorization for every pending operation in the boundary.
        //    Pre-effect gate (no effect yet in this step): peek, do not
        //    consume — step 4's per-effect recheck is the one-shot use.
        for op in &ops {
            scope.gate(op)?;
        }
        // 3. Read-only inspection of every source and owned temp image.
        let mut group = Vec::new();
        for file in &boundary {
            group.push(scope.inspect(prepared.id, file)?);
        }
        // 4. Consequential use, file by file, rechecked each time. `expected`
        // tracks the journal including this call's own receipts, so the check
        // detects a foreign journal change rather than our own progress.
        let mut expected = batch;
        let mut committed = Vec::new();
        for item in &group {
            self.check_strict_journal(&expected)?;
            let repeat = commit_file_ops(prepared.id, &item.file, limit, &plan)?;
            for op in &repeat {
                scope.authorize(op)?;
            }
            if file_hash(&item.target) != item.observed {
                return Err(blocked("source changed after the boundary preflight"));
            }
            if file_hash(&item.temp) != item.temp_hash {
                return Err(blocked("owned temp changed after the boundary preflight"));
            }
            if item.temp_hash.is_none() {
                let bytes = scope.postimage_bytes(prepared.id, limit, &item.file)?;
                write_new_synced(&item.temp, &bytes)?;
            }
            if file_hash(&item.temp) != Some(item.file.post_hash.clone()) {
                return Err(blocked(
                    "owned temp content does not match the journaled postimage",
                ));
            }
            std::fs::rename(&item.temp, &item.target)?;
            sync_parent(&item.target);
            // Post-rename verification: an interleaving writer must surface as
            // divergence, never as a silent clobber journaled committed.
            if file_hash(&item.target) != Some(item.file.post_hash.clone()) {
                self.journal.append(&JournalRecord::BatchAborted {
                    batch_id: prepared.id,
                    reason: format!("diverged during authorized commit for {}", item.file.path),
                    at: Timestamp::now(),
                })?;
                return Err(MutationError::Diverged {
                    path: item.file.path.clone(),
                });
            }
            self.journal.append(&JournalRecord::FileCommitted {
                batch_id: prepared.id,
                path: item.file.path.clone(),
                at: Timestamp::now(),
            })?;
            if let Some(file) = expected
                .files
                .iter_mut()
                .find(|file| file.path == item.file.path)
            {
                file.state = FileState::Committed;
            }
            committed.push(ChangedFile {
                path: item.file.path.clone(),
                batch_id: prepared.id,
                transition: Transition::Committed,
            });
        }
        // Completion is derived from the journal, not from the descriptor.
        let after = self.journal.replay_scoped(prepared.id)?;
        let completed = after
            .files
            .iter()
            .all(|file| file.state == FileState::Committed);
        if completed {
            self.journal.append(&JournalRecord::BatchCompleted {
                batch_id: prepared.id,
                at: Timestamp::now(),
            })?;
        }
        Ok(CommitReport {
            id: prepared.id,
            committed,
            completed,
        })
    }

    /// Strict task-scoped truth for `prepared`: the descriptor must equal the
    /// journaled plan exactly (its state fields are journal-derived, not
    /// authority) and the batch must still be commit-able.
    fn strict_batch(&self, prepared: &PreparedBatch) -> Result<ReplayedBatch, MutationError> {
        let batch = self.journal.replay_scoped(prepared.id)?;
        if batch.files.len() != prepared.files.len()
            || batch
                .files
                .iter()
                .zip(&prepared.files)
                .any(|(known, given)| {
                    known.path != given.path
                        || known.pre_hash != given.pre_hash
                        || known.post_hash != given.post_hash
                        || known.post_artifact != given.post_artifact
                        || known.temp_name != given.temp_name
                })
        {
            return Err(MutationError::UnknownBatch(prepared.id.to_string()));
        }
        if batch.completed {
            return Err(MutationError::AlreadyCompleted(prepared.id.to_string()));
        }
        if batch
            .files
            .iter()
            .any(|file| file.state == FileState::RolledBack)
        {
            return Err(MutationError::Compensated(prepared.id.to_string()));
        }
        Ok(batch)
    }

    /// Re-reads the journal: no effect proceeds from a boundary whose truth
    /// changed underneath it.
    fn check_strict_journal(&self, expected: &ReplayedBatch) -> Result<(), MutationError> {
        let current = self.journal.replay_scoped(expected.id)?;
        if current.files != expected.files
            || current.completed != expected.completed
            || current.aborted != expected.aborted
        {
            return Err(blocked("journal changed after the authorization preflight"));
        }
        Ok(())
    }
}

/// One pending file with its preflighted images.
struct InspectedFile {
    file: FileMutation,
    target: PathBuf,
    temp: PathBuf,
    observed: Option<String>,
    temp_hash: Option<String>,
}

/// Canonical roots plus the exact policy binding for one engine call.
struct AuthorizedScope<'a> {
    engine: &'a MutationEngine,
    context: &'a ToolsContext,
    workspace: PathBuf,
    state: PathBuf,
}

impl<'a> AuthorizedScope<'a> {
    /// Canonical identity and disjointness, read-only. The workspace and the
    /// task/attempt state directory must agree with the context and must not
    /// overlap in either direction.
    fn open(engine: &'a MutationEngine, context: &'a ToolsContext) -> Result<Self, MutationError> {
        let workspace = std::fs::canonicalize(&engine.workspace_root)?;
        if workspace != std::fs::canonicalize(&context.workspace_root)? {
            return Err(blocked("engine/context workspace identity mismatch"));
        }
        let state = std::fs::canonicalize(
            engine
                .journal
                .path()
                .parent()
                .ok_or_else(|| blocked("missing state directory"))?,
        )?;
        if state.starts_with(&workspace) || workspace.starts_with(&state) {
            return Err(blocked(
                "mutation state and workspace must be disjoint in both directions",
            ));
        }
        Ok(Self {
            engine,
            context,
            workspace,
            state,
        })
    }

    fn authorize(&self, op: &AuthorizedOp) -> Result<(), MutationError> {
        authorize(
            &self.context.policy,
            &self.context.approvals,
            &op.capability,
            &op.scope,
            &op.operation,
            "authorized mutation patch",
        )
        .map_err(map_authorization)
    }

    /// Pre-effect gate (M11): the same policy decision as
    /// [`Self::authorize`], but a present one-shot grant SATISFIES
    /// without consuming it. Preparation and the commit boundary execute
    /// nothing yet, so burning grants there would make every gate re-run
    /// after a grant re-ask the operations granted earlier (exponential
    /// re-parking); the grant's single use belongs to the per-effect
    /// recheck in `commit_authorized_up_to`, which still calls
    /// [`Self::authorize`]. The typed ask parks exactly the same.
    fn gate(&self, op: &AuthorizedOp) -> Result<(), MutationError> {
        authorize_peek(
            &self.context.policy,
            &self.context.approvals,
            &op.capability,
            &op.scope,
            &op.operation,
            "authorized mutation patch",
        )
        .map_err(map_authorization)
    }

    /// Read-only structural and preimage preflight for one spec: literal
    /// target, no occupied derived temp, exact current content.
    fn preflight_spec(
        &self,
        spec: &PatchSpec,
        batch_id: &MutationBatchId,
    ) -> Result<(), MutationError> {
        let rel = normalize_rel(&spec.path)?;
        let target = self.engine.resolve(&rel)?;
        if target != self.workspace.join(&rel) {
            return Err(MutationError::InvalidPath(format!(
                "target path is an alias or symlink: {}",
                spec.path
            )));
        }
        plain_path(&self.workspace, &target, true)?;
        let temp_rel = temp_rel(&rel, batch_id)?;
        let temp = self.workspace.join(&temp_rel);
        plain_path(&self.workspace, &temp, true)?;
        if std::fs::symlink_metadata(&temp).is_ok() {
            return Err(blocked(&format!(
                "derived temp path already exists and is never adopted: {temp_rel}"
            )));
        }
        let actual = file_hash(&target);
        if actual != spec.base_hash {
            return Err(MutationError::StalePreimage {
                path: rel,
                expected: spec.base_hash.clone(),
                actual,
            });
        }
        Ok(())
    }

    /// Read-only inspection of one journaled pending file: literal paths, the
    /// owned temp descriptor, and current source/temp images.
    fn inspect(
        &self,
        batch_id: MutationBatchId,
        file: &FileMutation,
    ) -> Result<InspectedFile, MutationError> {
        if normalize_rel(&file.path)? != file.path {
            return Err(blocked(
                "journal path is not in normalized journal-key form",
            ));
        }
        let target = self.workspace.join(&file.path);
        if self.engine.resolve(&file.path)? != target {
            return Err(blocked("journal target is an alias or symlink"));
        }
        plain_path(&self.workspace, &target, true)?;
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| blocked("invalid target filename"))?;
        if file.temp_name != MutationEngine::temp_name(name, &batch_id, TEMP_MARKER) {
            return Err(blocked("temp descriptor is not owned by this batch/target"));
        }
        let temp = target.with_file_name(&file.temp_name);
        if temp != self.workspace.join(temp_rel(&file.path, &batch_id)?) {
            return Err(blocked("derived temp path escapes its target directory"));
        }
        plain_path(&self.workspace, &temp, true)?;
        let observed = file_hash(&target);
        if observed != file.pre_hash {
            return Err(MutationError::StalePreimage {
                path: file.path.clone(),
                expected: file.pre_hash.clone(),
                actual: observed,
            });
        }
        let temp_hash = file_hash(&temp);
        if let Some(hash) = &temp_hash
            && *hash != file.post_hash
        {
            return Err(blocked("owned temp holds foreign content"));
        }
        Ok(InspectedFile {
            file: file.clone(),
            target,
            temp,
            observed,
            temp_hash,
        })
    }

    /// Verified postimage bytes from the retained artifact spool. Only needed
    /// when an owned temp was lost; the read is authorized exactly like strict
    /// scoped recovery (`external:<canonical artifact path>`).
    fn postimage_bytes(
        &self,
        batch_id: MutationBatchId,
        limit: usize,
        file: &FileMutation,
    ) -> Result<Vec<u8>, MutationError> {
        let expected = file.post_hash.as_str();
        if !valid_hash(expected) || file.post_artifact.0 != expected {
            return Err(blocked("postimage descriptor/content identity mismatch"));
        }
        let path = self
            .state
            .join("artifacts")
            .join(&expected[..2])
            .join(expected);
        let scope = format!("external:{}", path.to_string_lossy().replace('\\', "/"));
        let operation = serde_json::json!({
            "action": "commit", "batch_id": batch_id, "limit": limit, "op": "fs.read",
            "scope": scope, "path": file.path, "expected_hash": file.pre_hash,
            "content_hash": file.post_hash,
        });
        for capability in ["fs.metadata", "fs.read"] {
            self.authorize(&AuthorizedOp {
                capability: capability.to_owned(),
                scope: scope.clone(),
                operation: operation.clone(),
            })?;
        }
        plain_path(&self.state, &path, true)?;
        let bytes = ArtifactSpool::new(self.state.join("artifacts"))
            .fetch_legacy_unbounded(&file.post_artifact)
            .map_err(|error| blocked(&error.to_string()))?;
        if blake3_hex(&bytes) != expected {
            return Err(blocked("retained postimage content changed"));
        }
        Ok(bytes)
    }
}

/// Exact derived temp path in journal-key form for one target.
fn temp_rel(rel: &str, batch_id: &MutationBatchId) -> Result<String, MutationError> {
    let name = Path::new(rel)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| MutationError::InvalidPath(rel.to_owned()))?;
    let temp =
        Path::new(rel).with_file_name(MutationEngine::temp_name(name, batch_id, TEMP_MARKER));
    Ok(temp.to_string_lossy().replace('\\', "/"))
}

/// Plan binding for a specification list, in commit order.
fn specs_plan(specs: &[PatchSpec]) -> serde_json::Value {
    serde_json::Value::Array(
        specs
            .iter()
            .map(|spec| {
                serde_json::json!({
                    "path": spec.path, "pre_hash": spec.base_hash,
                    "post_hash": blake3_hex(&spec.new_content),
                })
            })
            .collect(),
    )
}

/// Plan binding for journaled truth, in commit order.
fn batch_plan(batch: &ReplayedBatch) -> serde_json::Value {
    serde_json::Value::Array(
        batch
            .files
            .iter()
            .map(|file| {
                serde_json::json!({
                    "path": file.path, "pre_hash": file.pre_hash,
                    "post_hash": file.post_hash,
                })
            })
            .collect(),
    )
}

/// Pending (not yet committed) journaled files for the next boundary.
fn pending_files(batch: &ReplayedBatch, limit: usize) -> impl Iterator<Item = &FileMutation> {
    batch
        .files
        .iter()
        .filter(|file| file.state != FileState::Committed)
        .take(limit)
}

fn prepare_operation(
    batch_id: MutationBatchId,
    capability: &str,
    scope: &str,
    plan: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "action": "prepare", "batch_id": batch_id, "op": capability,
        "scope": scope, "plan": plan,
    })
}

fn commit_operation(
    batch_id: MutationBatchId,
    limit: usize,
    capability: &str,
    scope: &str,
    file: &FileMutation,
    plan: &serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "action": "commit", "batch_id": batch_id, "limit": limit, "op": capability,
        "scope": scope, "path": file.path, "expected_hash": file.pre_hash,
        "content_hash": file.post_hash, "plan": plan,
    })
}

/// Exact operations for one pending file's commit boundary.
fn commit_file_ops(
    batch_id: MutationBatchId,
    file: &FileMutation,
    limit: usize,
    plan: &serde_json::Value,
) -> Result<Vec<AuthorizedOp>, MutationError> {
    let target_scope = format!("workspace/{}", file.path);
    let temp_scope = format!("workspace/{}", temp_rel(&file.path, &batch_id)?);
    let mut ops = Vec::with_capacity(TARGET_CAPABILITIES.len() + TEMP_COMMIT_CAPABILITIES.len());
    for capability in TARGET_CAPABILITIES {
        ops.push(AuthorizedOp {
            capability: capability.to_owned(),
            operation: commit_operation(batch_id, limit, capability, &target_scope, file, plan),
            scope: target_scope.clone(),
        });
    }
    for capability in TEMP_COMMIT_CAPABILITIES {
        ops.push(AuthorizedOp {
            capability: capability.to_owned(),
            operation: commit_operation(batch_id, limit, capability, &temp_scope, file, plan),
            scope: temp_scope.clone(),
        });
    }
    Ok(ops)
}

/// Refuses symlinked or non-regular workspace paths before any effect. Every
/// existing component must be a literal directory and, when `allow_file_leaf`
/// is set, an existing leaf must be a regular file. Missing trailing
/// components are allowed: they are about to be created, so no symlink can
/// already redirect them.
fn plain_path(root: &Path, path: &Path, allow_file_leaf: bool) -> Result<(), MutationError> {
    let rel = path
        .strip_prefix(root)
        .map_err(|_| blocked("path escapes the authorized root"))?;
    let components: Vec<_> = rel.components().collect();
    if components.is_empty() {
        return Err(blocked("path names a root"));
    }
    let mut current = root.to_path_buf();
    for (index, component) in components.iter().enumerate() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(blocked("non-literal mutation path"));
        }
        current.push(component);
        let last = index + 1 == components.len();
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if last && allow_file_leaf && meta.is_file() => {}
            Ok(meta) if !last && meta.is_dir() => {}
            Ok(_) => return Err(blocked("symlink, directory or non-regular mutation path")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn blocked(reason: &str) -> MutationError {
    MutationError::RecoveryBlocked(reason.to_owned())
}

/// One policy-authorization outcome → one typed mutation error: an ask
/// keeps its exact pending request (M11 typed parking — the
/// supervisor-owned run parks instead of failing), every other failure
/// collapses to the stringy guard as before.
fn map_authorization(error: ToolError) -> MutationError {
    match error {
        ToolError::ApprovalRequired { request, .. } => MutationError::ApprovalRequired(*request),
        other => blocked(&other.to_string()),
    }
}
