//! Exact task/attempt journal reconciliation. The caller holds the workspace lease.

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use tachyon_tools::{ToolsContext, artifact::ArtifactSpool, authorize};
use tachyon_types::{ArtifactId, MutationBatchId, Timestamp};

use super::{
    ChangedFile, FileMutation, FileState, JournalRecord, MutationEngine, MutationError,
    RESTORE_MARKER, TEMP_MARKER, Transition, blake3_hex, normalize_rel, sync_parent,
};
use crate::ReplayedBatch;

/// Trusted caller's recovery intent; never inferred from model output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAction {
    /// Finish a still-valid batch, then prove all postimages and receipts.
    Finish,
    /// Restore preimages, then prove every rollback receipt.
    Compensate,
}

/// A proven effect disposition, not authority to complete the task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryDisposition {
    /// Completed receipts and fresh postimages agree.
    Committed,
    /// Rollback receipts and fresh preimages agree; not a successful repair.
    Compensated,
}

/// Successful exact-batch reconciliation; errors leave the caller's effect unknown.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScopedRecoveryReport {
    /// The caller-selected batch, never an adopted sibling.
    pub batch_id: MutationBatchId,
    /// Freshly verified effect state.
    pub disposition: RecoveryDisposition,
    /// Transitions newly recorded by this invocation, in journal order.
    pub changed: Vec<ChangedFile>,
    /// Exact journal-owned files deleted by this invocation.
    pub cleaned: Vec<PathBuf>,
}

struct RecoveryScope<'a> {
    context: &'a ToolsContext,
    workspace: PathBuf,
    state: PathBuf,
    batch_id: MutationBatchId,
    action: RecoveryAction,
}

struct RecoveryPlan<'a> {
    scope: RecoveryScope<'a>,
    batch: ReplayedBatch,
    checked: Vec<CheckedFile>,
}

struct CheckedFile {
    target: PathBuf,
    temp: PathBuf,
    observed: Option<String>,
    temp_hash: Option<String>,
    desired_hash: Option<String>,
    desired_bytes: Option<Vec<u8>>,
    stage: Option<PathBuf>,
}

impl MutationEngine {
    /// Reconciles exactly one batch under the caller's task/attempt binding.
    ///
    /// The caller must hold the shared workspace lease throughout this synchronous
    /// call and supply exact allowed target paths. All source/temp policy and
    /// retained artifact reads are preflighted before any write or deletion.
    /// Artifact scopes are `external:<canonical path>` when outside the workspace.
    /// Every error leaves the caller's effect disposition unknown; IO can fail
    /// after per-file effects, so this is recoverable, not globally atomic.
    pub fn recover_scoped(
        &self,
        context: &ToolsContext,
        batch_id: MutationBatchId,
        action: RecoveryAction,
        allowed_paths: &[String],
    ) -> Result<ScopedRecoveryReport, MutationError> {
        let RecoveryPlan {
            scope,
            mut batch,
            mut checked,
        } = self.preflight_scoped(context, batch_id, action, allowed_paths)?;
        let finish = action == RecoveryAction::Finish;
        let mut report = ScopedRecoveryReport {
            batch_id,
            disposition: if finish {
                RecoveryDisposition::Committed
            } else {
                RecoveryDisposition::Compensated
            },
            changed: Vec::new(),
            cleaned: Vec::new(),
        };
        for index in 0..checked.len() {
            self.check_recovery_journal(&batch)?;
            scope.check_snapshot(&checked)?;
            scope.apply(&mut checked[index])?;
            let file = &mut batch.files[index];
            let terminal = if finish {
                FileState::Committed
            } else {
                FileState::RolledBack
            };
            if file.state != terminal {
                let record = if finish {
                    JournalRecord::FileCommitted {
                        batch_id,
                        path: file.path.clone(),
                        at: Timestamp::now(),
                    }
                } else {
                    JournalRecord::FileRolledBack {
                        batch_id,
                        path: file.path.clone(),
                        at: Timestamp::now(),
                    }
                };
                self.journal.append(&record)?;
                file.state = terminal;
                report.changed.push(ChangedFile {
                    path: file.path.clone(),
                    batch_id,
                    transition: if finish {
                        Transition::Committed
                    } else {
                        Transition::RolledBack
                    },
                });
            }
            let item = &mut checked[index];
            if let Some(hash) = &item.temp_hash {
                scope.delete(&item.temp, hash)?;
                report.cleaned.push(item.temp.clone());
                item.temp_hash = None;
            }
        }
        self.check_recovery_journal(&batch)?;
        scope.check_snapshot(&checked)?;
        if finish && !batch.completed {
            self.journal.append(&JournalRecord::BatchCompleted {
                batch_id,
                at: Timestamp::now(),
            })?;
            batch.completed = true;
        }
        // A successful future alone is not proof: require receipts plus fresh
        // desired images (including absent preimages for rolled-back creates).
        self.check_recovery_journal(&batch)?;
        scope.check_snapshot(&checked)?;
        Ok(report)
    }

    /// Pure preflight: no journal writes, source writes, staging or cleanup.
    fn preflight_scoped<'a>(
        &self,
        context: &'a ToolsContext,
        batch_id: MutationBatchId,
        action: RecoveryAction,
        allowed_paths: &[String],
    ) -> Result<RecoveryPlan<'a>, MutationError> {
        let workspace = std::fs::canonicalize(&self.workspace_root)?;
        if workspace != std::fs::canonicalize(&context.workspace_root)? {
            return Err(blocked("engine/context workspace identity mismatch"));
        }
        let state = std::fs::canonicalize(
            self.journal
                .path()
                .parent()
                .ok_or_else(|| blocked("missing state directory"))?,
        )?;
        if state.starts_with(&workspace) {
            return Err(blocked("recovery state must be outside the workspace"));
        }
        plain_file(&state, self.journal.path())?;
        let scope = RecoveryScope {
            context,
            workspace,
            state,
            batch_id,
            action,
        };
        let batch = self.journal.replay_scoped(batch_id)?;
        let finish = action == RecoveryAction::Finish;
        if batch.completed && !finish {
            return Err(MutationError::AlreadyCompleted(batch_id.to_string()));
        }
        if finish
            && batch
                .files
                .iter()
                .any(|file| file.state == FileState::RolledBack)
        {
            return Err(MutationError::Compensated(batch_id.to_string()));
        }
        let allowed = allowed_paths
            .iter()
            .map(|path| normalize_rel(path))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let checked = batch
            .files
            .iter()
            .map(|file| scope.preflight(file, &allowed))
            .collect::<Result<Vec<_>, _>>()?;
        let mut unique_paths = BTreeSet::new();
        for file in &checked {
            for path in [&file.target, &file.temp] {
                if !unique_paths.insert(path) {
                    return Err(blocked("recovery target/temp paths alias each other"));
                }
            }
            if let Some(stage) = &file.stage
                && stage != &file.temp
                && !unique_paths.insert(stage)
            {
                return Err(blocked("recovery staging path aliases another target"));
            }
        }
        Ok(RecoveryPlan {
            scope,
            batch,
            checked,
        })
    }

    fn check_recovery_journal(&self, expected: &ReplayedBatch) -> Result<(), MutationError> {
        let current = self.journal.replay_scoped(expected.id)?;
        if current.files != expected.files
            || current.completed != expected.completed
            || current.aborted != expected.aborted
        {
            return Err(blocked("journal changed after recovery preflight"));
        }
        Ok(())
    }
}

impl RecoveryScope<'_> {
    fn authorize(
        &self,
        capability: &str,
        path: &Path,
        before: Option<&str>,
        after: Option<&str>,
    ) -> Result<(), MutationError> {
        let scope = if let Ok(rel) = path.strip_prefix(&self.workspace) {
            format!("workspace/{}", rel.to_string_lossy().replace('\\', "/"))
        } else {
            format!("external:{}", path.to_string_lossy().replace('\\', "/"))
        };
        let operation = serde_json::json!({
            "op": capability, "scope": scope, "batch_id": self.batch_id,
            "action": self.action, "expected_hash": before, "content_hash": after,
        });
        authorize(
            &self.context.policy,
            &self.context.approvals,
            capability,
            &scope,
            &operation,
            "scoped mutation recovery",
        )
        .map_err(|error| blocked(&error.to_string()))
    }

    fn hash(&self, path: &Path) -> Result<Option<String>, MutationError> {
        self.authorize("fs.metadata", path, None, None)?;
        plain_file(&self.workspace, path)?;
        self.authorize("fs.read", path, None, None)?;
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(blake3_hex(&bytes))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn artifact(&self, id: &ArtifactId, expected: &str) -> Result<Vec<u8>, MutationError> {
        if !valid_hash(expected) || id.0 != expected {
            return Err(blocked("artifact descriptor/content identity mismatch"));
        }
        let path = self.state.join("artifacts").join(&id.0[..2]).join(&id.0);
        self.authorize("fs.metadata", &path, None, None)?;
        plain_file(&self.state, &path)?;
        self.authorize("fs.read", &path, None, None)?;
        let bytes = ArtifactSpool::new(self.state.join("artifacts"))
            .fetch_legacy_unbounded(id)
            .map_err(|error| blocked(&error.to_string()))?;
        if blake3_hex(&bytes) != expected {
            return Err(blocked("retained artifact content changed"));
        }
        Ok(bytes)
    }

    fn preflight(
        &self,
        file: &FileMutation,
        allowed: &BTreeSet<String>,
    ) -> Result<CheckedFile, MutationError> {
        if normalize_rel(&file.path)? != file.path || !allowed.contains(&file.path) {
            return Err(blocked(
                "target must match an exact normalized allowed path",
            ));
        }
        let target = self.workspace.join(&file.path);
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| blocked("invalid target filename"))?;
        if file.temp_name != MutationEngine::temp_name(name, &self.batch_id, TEMP_MARKER) {
            return Err(blocked("temp descriptor is not owned by this batch/target"));
        }
        let observed = self.hash(&target)?;
        if observed != file.pre_hash && observed.as_ref() != Some(&file.post_hash) {
            return Err(MutationError::Diverged {
                path: file.path.clone(),
            });
        }
        if (file.state == FileState::RolledBack && observed != file.pre_hash)
            || (self.action == RecoveryAction::Finish
                && file.state == FileState::Committed
                && observed.as_ref() != Some(&file.post_hash))
        {
            return Err(blocked("terminal receipt disagrees with current image"));
        }
        let post = self.artifact(&file.post_artifact, &file.post_hash)?;
        let pre = match (&file.pre_hash, &file.pre_artifact) {
            (Some(hash), Some(id)) => Some(self.artifact(id, hash)?),
            (None, None) => None,
            _ => return Err(blocked("preimage hash/artifact presence mismatch")),
        };
        let (desired_hash, desired_bytes) = if self.action == RecoveryAction::Finish {
            (Some(file.post_hash.clone()), Some(post))
        } else {
            (file.pre_hash.clone(), pre)
        };
        self.authorize(
            "mutation.patch",
            &target,
            observed.as_deref(),
            desired_hash.as_deref(),
        )?;
        self.authorize(
            "fs.write",
            &target,
            observed.as_deref(),
            desired_hash.as_deref(),
        )?;
        let temp = target.with_file_name(&file.temp_name);
        let temp_hash = self.hash(&temp)?;
        if temp_hash.is_some() && temp_hash.as_ref() != Some(&file.post_hash) {
            return Err(blocked("journal-owned temp has foreign content"));
        }
        if temp_hash.is_some() {
            self.authorize("fs.delete", &temp, Some(&file.post_hash), None)?;
        }
        let mut stage = None;
        if observed != desired_hash {
            if desired_bytes.is_some() {
                let path = if self.action == RecoveryAction::Finish {
                    temp.clone()
                } else {
                    // Never overwrite an unjournaled restore orphan. This new
                    // path is owned by create_new for this invocation only;
                    // after a crash any unrenamed orphan is deliberately retained.
                    target.with_file_name(MutationEngine::temp_name(
                        name,
                        &MutationBatchId::generate(),
                        RESTORE_MARKER,
                    ))
                };
                let hash = self.hash(&path)?;
                if path != temp && hash.is_some() {
                    return Err(blocked("restore staging path already exists"));
                }
                self.authorize("fs.write", &path, hash.as_deref(), desired_hash.as_deref())?;
                self.authorize("fs.delete", &path, desired_hash.as_deref(), None)?;
                stage = Some(path);
            } else {
                self.authorize("fs.delete", &target, observed.as_deref(), None)?;
            }
        }
        Ok(CheckedFile {
            target,
            temp,
            observed,
            temp_hash,
            desired_hash,
            desired_bytes,
            stage,
        })
    }

    fn check_snapshot(&self, checked: &[CheckedFile]) -> Result<(), MutationError> {
        for file in checked {
            if self.hash(&file.target)? != file.observed || self.hash(&file.temp)? != file.temp_hash
            {
                return Err(blocked("source or owned temp changed after preflight"));
            }
        }
        Ok(())
    }

    fn apply(&self, file: &mut CheckedFile) -> Result<(), MutationError> {
        if file.observed == file.desired_hash {
            return Ok(());
        }
        if let (Some(bytes), Some(stage)) = (&file.desired_bytes, &file.stage) {
            let current = self.hash(stage)?;
            if stage != &file.temp && current.is_some() {
                return Err(blocked(
                    "unowned restore staging path appeared after preflight",
                ));
            }
            self.authorize(
                "fs.write",
                stage,
                current.as_deref(),
                file.desired_hash.as_deref(),
            )?;
            if current.is_none() {
                // No truncate/open-existing path: only a verified journal temp
                // can be reused. Unowned paths are never overwritten.
                let mut output = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(stage)?;
                output.write_all(bytes)?;
                output.sync_all()?;
            }
            if self.hash(stage)? != file.desired_hash || self.hash(&file.target)? != file.observed {
                return Err(blocked("image changed before consequential rename"));
            }
            self.authorize(
                "mutation.patch",
                &file.target,
                file.observed.as_deref(),
                file.desired_hash.as_deref(),
            )?;
            self.authorize(
                "fs.write",
                &file.target,
                file.observed.as_deref(),
                file.desired_hash.as_deref(),
            )?;
            self.authorize("fs.delete", stage, file.desired_hash.as_deref(), None)?;
            std::fs::rename(stage, &file.target)?;
            if stage == &file.temp {
                file.temp_hash = None;
            }
            sync_parent(&file.target);
        } else if let Some(hash) = &file.observed {
            self.delete(&file.target, hash)?;
        }
        if self.hash(&file.target)? != file.desired_hash {
            return Err(blocked(
                "post-effect image differs from intended disposition",
            ));
        }
        file.observed.clone_from(&file.desired_hash);
        Ok(())
    }

    fn delete(&self, path: &Path, expected: &str) -> Result<(), MutationError> {
        self.authorize("fs.delete", path, Some(expected), None)?;
        if self.hash(path)?.as_deref() != Some(expected) {
            return Err(blocked(
                "refusing deletion without fresh owned content identity",
            ));
        }
        std::fs::remove_file(path)?;
        sync_parent(path);
        Ok(())
    }
}

/// Refuse symlinks (including dangling links) and non-regular files. Only a
/// missing final component is allowed; recovery never creates directories.
fn plain_file(root: &Path, path: &Path) -> Result<(), MutationError> {
    let rel = path
        .strip_prefix(root)
        .map_err(|_| blocked("path escapes recovery root"))?;
    let mut current = root.to_path_buf();
    let components: Vec<_> = rel.components().collect();
    if components.is_empty() {
        return Err(blocked("file path names a root"));
    }
    for (index, component) in components.iter().enumerate() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(blocked("non-normal recovery path"));
        }
        current.push(component);
        let last = index + 1 == components.len();
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if (last && meta.is_file()) || (!last && meta.is_dir()) => {}
            Ok(_) => return Err(blocked("symlink or non-regular recovery path")),
            Err(error) if last && error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Lowercase-hex hash shape check, shared with the authorized write boundary.
pub(super) fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn blocked(reason: &str) -> MutationError {
    MutationError::RecoveryBlocked(reason.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn late_foreign_restore_temp_is_not_adopted_even_with_matching_bytes() {
        let root = std::env::temp_dir().join(format!(
            "tachyon-restore-collision-{}",
            MutationBatchId::generate()
        ));
        let workspace = root.join("workspace");
        let state = root.join("state");
        std::fs::create_dir_all(&workspace).expect("workspace");
        std::fs::write(workspace.join("a.rs"), b"before").expect("source");
        std::fs::write(workspace.join("b.rs"), b"before").expect("source");
        let engine = MutationEngine::open(&workspace, &state).expect("engine");
        let prepared = engine
            .prepare(&["a.rs", "b.rs"].map(|path| crate::PatchSpec {
                path: path.to_owned(),
                base_hash: Some(blake3_hex(b"before")),
                new_content: b"after".to_vec(),
            }))
            .expect("prepare");
        engine.commit_up_to(&prepared, 1).expect("partial");
        let mut policy = tachyon_policy::Policy::trusted_workspace();
        policy.allow("mutation.patch", "workspace/**");
        policy.allow("fs.delete", "workspace/**");
        for capability in ["fs.read", "fs.metadata"] {
            policy.allow(
                capability,
                &format!(
                    "external:{}/artifacts/**",
                    state.display().to_string().replace('\\', "/")
                ),
            );
        }
        let context = ToolsContext::new(
            workspace.clone(),
            policy,
            ArtifactSpool::new(root.join("tool-artifacts")),
        );
        let scope = RecoveryScope {
            context: &context,
            workspace: workspace.clone(),
            state,
            batch_id: prepared.id,
            action: RecoveryAction::Compensate,
        };
        let file = &engine
            .journal
            .replay_scoped(prepared.id)
            .expect("journal")
            .files[0];
        let mut checked = scope
            .preflight(file, &BTreeSet::from(["a.rs".to_owned()]))
            .expect("preflight");
        let restore_temp = checked.stage.clone().expect("restore stage");
        // Simulate replacement after preflight, before the consequential use.
        std::fs::write(&restore_temp, b"before").expect("foreign file with matching bytes");
        let result = scope.apply(&mut checked);
        let preserved = std::fs::read(&restore_temp).ok() == Some(b"before".to_vec())
            && std::fs::read(workspace.join("a.rs")).expect("source") == b"after";
        std::fs::remove_dir_all(root).expect("cleanup fixture");
        assert!(
            result.is_err() && preserved,
            "cannot adopt an unjournaled path after preflight"
        );
    }
}
