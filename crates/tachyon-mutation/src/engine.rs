//! Mutation engine: prepare, commit, recover (spec §20).
//!
//! The engine enforces the §20 procedure exactly: verify preimages, stage
//! temp files on the target filesystem, journal the plan, rename per file,
//! journal each commit, then complete. Commit re-verifies every preimage
//! before its rename — a file that moved under us aborts the batch with
//! [`MutationError::StalePreimage`] instead of clobbering.
//!
//! Recovery never guesses. It replays the journal, reconciles each file
//! against its recorded pre/post hashes, catches up missing commit records,
//! and then either finishes a still-valid batch or compensates it back to
//! preimages. Content matching neither hash is `Diverged`: reported, never
//! written.
//!
//! Effect declaration: [`tachyon_ir::EffectClass::ReversibleLocalMutation`]
//! with [`tachyon_ir::Idempotency::Compensatable`] — rollback is the
//! compensation. Commits are never auto-retried after a crash: recover
//! first, then act on the report.

mod authorized;
mod scoped;
pub use authorized::AuthorizedOp;
pub use scoped::{RecoveryAction, RecoveryDisposition, ScopedRecoveryReport};

use std::path::{Path, PathBuf};

use tachyon_ir::{EffectClass, Idempotency};
use tachyon_tools::artifact::ArtifactSpool;
use tachyon_types::{ArtifactId, MutationBatchId, Timestamp};

use crate::{
    FileMutation, FileState, MutationError, PatchSpec, blake3_hex, file_hash,
    journal::{BatchJournal, JournalRecord},
    normalize_rel,
};

/// Effect class declared by every batch this engine runs.
pub const EFFECT_CLASS: EffectClass = EffectClass::ReversibleLocalMutation;

/// Idempotency declared by every batch: rollback compensates.
pub const IDEMPOTENCY: Idempotency = Idempotency::Compensatable;

/// Temp-file marker, always beside its target so renames stay on one
/// filesystem. Full batch IDs distinguish ownership; names alone prove nothing.
const TEMP_MARKER: &str = "tachyon-tmp";

/// Restore-temp marker for atomic compensation (same placement rules).
const RESTORE_MARKER: &str = "tachyon-restore";

/// One observed file transition, emitted in commit and recovery reports
/// (changed-file events for later milestones).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChangedFile {
    /// File in journal-key form.
    pub path: String,
    /// Owning batch.
    pub batch_id: MutationBatchId,
    /// What happened.
    pub transition: Transition,
}

/// Transition kinds for [`ChangedFile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transition {
    /// Temp renamed into place.
    Committed,
    /// Preimage restored (or created file removed).
    RolledBack,
}

/// A prepared batch: plan journaled, temps staged, nothing renamed yet.
#[derive(Clone, Debug)]
pub struct PreparedBatch {
    /// Batch identity.
    pub id: MutationBatchId,
    /// Files in commit order with recorded hashes.
    pub files: Vec<FileMutation>,
}

/// Outcome of a commit call (possibly partial — see `completed`).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CommitReport {
    /// Batch identity.
    pub id: MutationBatchId,
    /// Files committed by this call, in order.
    pub committed: Vec<ChangedFile>,
    /// Whether every file is committed and completion journaled.
    pub completed: bool,
}

/// Outcome of a recovery run. Serde-stable for M9 verification over
/// reports.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryReport {
    /// Batches completed by finishing.
    pub finished: Vec<MutationBatchId>,
    /// Batches compensated by rollback.
    pub rolled_back: Vec<MutationBatchId>,
    /// Batches seen aborted in the journal (informational; still processed).
    pub aborted: Vec<MutationBatchId>,
    /// Batches that failed recovery, with reasons; siblings proceeded.
    pub batch_errors: Vec<(MutationBatchId, String)>,
    /// Exact owned-temp deletions that failed; unproven paths are retained.
    pub sweep_errors: Vec<String>,
    /// Journal lines skipped as corrupt (lenient replay).
    pub journal_gaps: Vec<usize>,
    /// Files whose content matches neither pre- nor postimage.
    pub diverged: Vec<String>,
    /// Exact journal-owned postimage temps removed (never a workspace sweep).
    pub swept_tmps: Vec<PathBuf>,
    /// Transitions performed by this run.
    pub changed: Vec<ChangedFile>,
}

/// The engine: workspace root for containment, artifact spool for
/// preimages, journal for crash truth.
pub struct MutationEngine {
    workspace_root: PathBuf,
    spool: ArtifactSpool,
    journal: BatchJournal,
}

impl MutationEngine {
    /// Opens the engine: `workspace_root` for containment, `state_dir`
    /// for the journal (`mutation.log`) and artifact spool (`artifacts/`).
    pub fn open(workspace_root: &Path, state_dir: &Path) -> Result<Self, MutationError> {
        Ok(Self {
            workspace_root: workspace_root.to_owned(),
            spool: ArtifactSpool::new(state_dir.join("artifacts")),
            journal: BatchJournal::open(state_dir)?,
        })
    }

    /// Resolves a journal-key path against the workspace (containment).
    fn resolve(&self, rel: &str) -> Result<PathBuf, MutationError> {
        Ok(tachyon_policy::contain(
            &self.workspace_root,
            Path::new(rel),
        )?)
    }

    /// Temp name beside `file_name`, unique per batch. Uses the full batch
    /// `id`: `UUIDv7` prefixes share timestamp bits, so truncation collides
    /// for batches born in the same millisecond.
    fn temp_name(file_name: &str, id: &MutationBatchId, infix: &str) -> String {
        format!(".{file_name}.{infix}-{id}")
    }

    /// Verifies all specs, stashes preimages, stages temps, journals the
    /// plan. Returns `StalePreimage` before writing anything when any base
    /// mismatches — prepare is all-or-nothing.
    pub fn prepare(&self, specs: &[PatchSpec]) -> Result<PreparedBatch, MutationError> {
        self.prepare_with_id(MutationBatchId::generate(), specs)
    }

    fn prepare_with_id(
        &self,
        id: MutationBatchId,
        specs: &[PatchSpec],
    ) -> Result<PreparedBatch, MutationError> {
        if specs.is_empty() {
            return Err(MutationError::InvalidPath(
                "batch holds no files".to_owned(),
            ));
        }
        // Verify everything before writing anything. Each file is read
        // exactly once: the bytes feed the hash check, the preimage spool,
        // and nothing else — no verify-then-reread window.
        let mut checked = Vec::with_capacity(specs.len());
        for spec in specs {
            let rel = normalize_rel(&spec.path)?;
            if checked
                .iter()
                .any(|(path, _, _): &(String, _, _)| *path == rel)
            {
                return Err(MutationError::InvalidPath(format!("duplicate path: {rel}")));
            }
            let target = self.resolve(&rel)?;
            // One read per file feeds the hash check AND the preimage spool.
            // It mirrors `file_hash`'s NotFound-vs-error split (that helper
            // cannot be called here without rereading, which would reopen the
            // verify-then-reread window this loop is written to avoid).
            let current = match std::fs::read(&target) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(MutationError::InvalidPath(format!(
                        "{}: {error}",
                        target.display()
                    )));
                }
            };
            let actual = current.as_deref().map(blake3_hex);
            if spec.base_hash != actual {
                return Err(MutationError::StalePreimage {
                    path: rel,
                    expected: spec.base_hash.clone(),
                    actual,
                });
            }
            checked.push((rel, target, current));
        }
        // Stage: preimages and postimages to the spool, postimages to temps.
        let mut files = Vec::with_capacity(specs.len());
        for (spec, (rel, target, current)) in specs.iter().zip(checked) {
            let pre_artifact: Option<ArtifactId> = match &current {
                Some(bytes) => Some(self.spool.store(bytes).map_err(|error| io_error(&error))?),
                None => None,
            };
            let post_artifact = self
                .spool
                .store(&spec.new_content)
                .map_err(|error| io_error(&error))?;
            let file_name = target.file_name().map_or_else(
                || "file".to_owned(),
                |name| name.to_string_lossy().into_owned(),
            );
            let temp_name = Self::temp_name(&file_name, &id, TEMP_MARKER);
            let temp = target.with_file_name(&temp_name);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            // `create_new`: an unowned occupant of the derived temp path is
            // never truncated into ownership. Authorized preparation refuses
            // such a path before reaching here; the trusted caller excludes
            // concurrent writers (see RECOVERY.md).
            write_new_synced(&temp, &spec.new_content)?;
            files.push(FileMutation {
                path: rel,
                pre_hash: spec.base_hash.clone(),
                pre_artifact,
                post_hash: blake3_hex(&spec.new_content),
                post_artifact,
                temp_name,
                state: FileState::Prepared,
            });
        }
        self.journal.append(&JournalRecord::BatchStarted {
            batch_id: id,
            files: files.clone(),
            at: Timestamp::now(),
        })?;
        Ok(PreparedBatch { id, files })
    }

    /// Commits every pending file, re-verifying each preimage before its
    /// rename. A mismatch aborts the batch (`BatchAborted` journaled) with
    /// `StalePreimage`: already-committed files stay committed and
    /// journaled — recovery finishes or compensates the rest.
    pub fn commit(&self, prepared: &PreparedBatch) -> Result<CommitReport, MutationError> {
        self.commit_up_to(prepared, usize::MAX)
    }

    /// Commits at most `limit` pending files, then returns. Besides
    /// crash-simulation in tests, this is the resume primitive recovery
    /// uses file-by-file.
    pub fn commit_up_to(
        &self,
        prepared: &PreparedBatch,
        limit: usize,
    ) -> Result<CommitReport, MutationError> {
        if prepared.files.is_empty() {
            return Err(MutationError::InvalidPath(
                "batch holds no files".to_owned(),
            ));
        }
        if self.is_completed(&prepared.id)? {
            return Err(MutationError::AlreadyCompleted(prepared.id.to_string()));
        }
        // No-resurrect, commit side: any journaled rollback is terminal
        // operator intent. A stale descriptor held across compensation is
        // dead — resume with a fresh prepare (new id), never by
        // recommitting. Mirrors the finish-path guard in recovery.
        let compensated = self
            .journal
            .replay()?
            .get(&prepared.id)
            .is_some_and(|batch| {
                batch
                    .files
                    .iter()
                    .any(|file| file.state == FileState::RolledBack)
            });
        if compensated {
            return Err(MutationError::Compensated(prepared.id.to_string()));
        }
        // The journal is truth: the plan must match what prepare persisted,
        // or this descriptor is fabricated and there is nothing safe to do.
        self.check_plan(&prepared.id, &prepared.files)?;
        let mut committed = Vec::new();
        let mut done = 0;
        for file in &prepared.files {
            if done >= limit {
                break;
            }
            if file.state == FileState::Committed {
                continue;
            }
            let target = self.resolve(&file.path)?;
            let actual = file_hash(&target)?;
            if actual != file.pre_hash {
                self.journal.append(&JournalRecord::BatchAborted {
                    batch_id: prepared.id,
                    reason: format!("stale preimage for {}", file.path),
                    at: Timestamp::now(),
                })?;
                return Err(MutationError::StalePreimage {
                    path: file.path.clone(),
                    expected: file.pre_hash.clone(),
                    actual,
                });
            }
            let temp = target.with_file_name(&file.temp_name);
            if !temp.exists() {
                // Temp lost (crash cleanup, concurrent sweep, fs rollback):
                // re-stage from the retained postimage and proceed.
                let bytes = self
                    .spool
                    .fetch_legacy_unbounded(&file.post_artifact)
                    .map_err(|error| {
                        MutationError::Io(format!("temp lost for {}: {error}", file.path))
                    })?;
                write_synced(&temp, &bytes)?;
            }
            std::fs::rename(&temp, &target)?;
            sync_parent(&target);
            // Post-rename verification: an interleaving write between the
            // check and the rename (or just after it) must surface as
            // divergence, never as a silent clobber journaled committed.
            if file_hash(&target)? != Some(file.post_hash.clone()) {
                self.journal.append(&JournalRecord::BatchAborted {
                    batch_id: prepared.id,
                    reason: format!("diverged during commit for {}", file.path),
                    at: Timestamp::now(),
                })?;
                return Err(MutationError::Diverged {
                    path: file.path.clone(),
                });
            }
            // M12 fault point: kill after rename, before FileCommitted journal.
            tachyon_tools::fault::reach_blocking("mutation.commit");
            self.journal.append(&JournalRecord::FileCommitted {
                batch_id: prepared.id,
                path: file.path.clone(),
                at: Timestamp::now(),
            })?;
            committed.push(ChangedFile {
                path: file.path.clone(),
                batch_id: prepared.id,
                transition: Transition::Committed,
            });
            done += 1;
        }
        let completed = self
            .replay_files(&prepared.id)?
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

    /// Recovers crashed work: reconciles lenient journal replay with disk,
    /// then finishes still-valid files (`finish = true`) or compensates
    /// batches back to preimages (`finish = false`). Each batch is
    /// isolated: one batch's failure is recorded, siblings proceed.
    /// Diverged files are reported, never written. Cleanup visits only exact
    /// journal-owned terminal temps with matching postimage hashes; foreign,
    /// changed and orphan marker files survive. Syntax gaps block all effects.
    /// This legacy entry has no task policy context; runtimes use `recover_scoped`.
    pub fn recover(&self, finish: bool) -> Result<RecoveryReport, MutationError> {
        let mut report = RecoveryReport::default();
        let (batches, gaps) = self.journal.replay_lenient()?;
        report.journal_gaps = gaps;
        if !report.journal_gaps.is_empty() {
            // A damaged journal cannot authorize compensation or cleanup.
            return Ok(report);
        }
        for (id, replayed) in &batches {
            if replayed.completed {
                continue;
            }
            if replayed.aborted {
                report.aborted.push(*id);
            }
            if let Err(error) = self.recover_one(*id, replayed, finish, &mut report) {
                report.batch_errors.push((*id, error.to_string()));
            }
        }
        let (current, _) = self.journal.replay_lenient()?;
        report.swept_tmps = self.sweep_tmps(&current, &mut report.sweep_errors);
        Ok(report)
    }

    /// Recovers one batch: reconcile, then finish or compensate.
    fn recover_one(
        &self,
        id: MutationBatchId,
        replayed: &crate::journal::ReplayedBatch,
        finish: bool,
        report: &mut RecoveryReport,
    ) -> Result<(), MutationError> {
        // Reconcile each file against disk truth, honoring terminal
        // journal states first.
        let mut states: Vec<(FileMutation, FileState)> = Vec::new();
        for file in &replayed.files {
            let target = self.resolve(&file.path)?;
            let actual = file_hash(&target)?;
            let observed = if file.state == FileState::RolledBack && actual == file.pre_hash {
                FileState::RolledBack
            } else if actual == Some(file.post_hash.clone()) {
                if file.state != FileState::Committed {
                    self.journal.append(&JournalRecord::FileCommitted {
                        batch_id: id,
                        path: file.path.clone(),
                        at: Timestamp::now(),
                    })?;
                    report.changed.push(ChangedFile {
                        path: file.path.clone(),
                        batch_id: id,
                        transition: Transition::Committed,
                    });
                }
                FileState::Committed
            } else if actual == file.pre_hash {
                FileState::Prepared
            } else {
                FileState::Diverged
            };
            states.push((file.clone(), observed));
        }
        for (file, observed) in &states {
            if *observed == FileState::Diverged {
                report.diverged.push(file.path.clone());
            }
        }
        if finish {
            // Finish never resurrects a compensated batch: any journaled
            // rollback is terminal operator intent. Resume via a fresh
            // prepare instead (commit refuses compensated batches too).
            if replayed
                .files
                .iter()
                .any(|file| file.state == FileState::RolledBack)
            {
                return Ok(());
            }
            // Finish every still-valid file even when siblings diverged:
            // per-file atomicity, no batch-wide veto. Compensated files
            // stay compensated.
            let pending: Vec<FileMutation> = states
                .iter()
                .filter(|(_, observed)| *observed == FileState::Prepared)
                .map(|(file, state)| {
                    let mut file = file.clone();
                    file.state = *state;
                    file
                })
                .collect();
            if pending.is_empty() {
                // Complete only genuine all-committed batches: non-empty,
                // zero diverged, every file committed. Compensated batches
                // return above; crafted empty plans never complete.
                if !states.is_empty()
                    && states
                        .iter()
                        .all(|(_, observed)| *observed == FileState::Committed)
                {
                    self.journal.append(&JournalRecord::BatchCompleted {
                        batch_id: id,
                        at: Timestamp::now(),
                    })?;
                    report.finished.push(id);
                }
                return Ok(());
            }
            let prepared = PreparedBatch { id, files: pending };
            let commit = self.commit_up_to(&prepared, usize::MAX)?;
            report.changed.extend(commit.committed);
            if commit.completed {
                report.finished.push(id);
            }
        } else {
            // Compensate: restore committed files to preimages, mark
            // untouched pendings terminal. Diverged files are reported,
            // never written.
            for (file, observed) in &states {
                if *observed == FileState::Diverged || *observed == FileState::RolledBack {
                    continue;
                }
                if *observed == FileState::Committed {
                    self.restore(file)?;
                }
                self.journal.append(&JournalRecord::FileRolledBack {
                    batch_id: id,
                    path: file.path.clone(),
                    at: Timestamp::now(),
                })?;
                report.changed.push(ChangedFile {
                    path: file.path.clone(),
                    batch_id: id,
                    transition: Transition::RolledBack,
                });
            }
            report.rolled_back.push(id);
        }
        Ok(())
    }

    /// Restores one file atomically: preimage bytes go to a restore temp
    /// beside the target, sync, then rename over it — a crash mid-restore
    /// leaves the target untouched and an orphan temp (retained), so compensation
    /// is retryable. Files this batch created (no preimage) are removed.
    fn restore(&self, file: &FileMutation) -> Result<(), MutationError> {
        let target = self.resolve(&file.path)?;
        match &file.pre_artifact {
            Some(artifact) => {
                let bytes = self
                    .spool
                    .fetch_legacy_unbounded(artifact)
                    .map_err(|error| io_error(&error))?;
                let file_name = target.file_name().map_or_else(
                    || "file".to_owned(),
                    |name| name.to_string_lossy().into_owned(),
                );
                // Legacy restore temps are not journal-owned cleanup paths.
                // A crash orphan is retained rather than guessed to be garbage.
                let temp_name = format!(
                    ".{file_name}.{RESTORE_MARKER}-{}",
                    Timestamp::now().as_micros()
                );
                let temp = target.with_file_name(&temp_name);
                write_synced(&temp, &bytes)?;
                std::fs::rename(&temp, &target)?;
            }
            None => {
                if target.exists() {
                    std::fs::remove_file(&target)?;
                }
            }
        }
        sync_parent(&target);
        Ok(())
    }

    /// Cleans only terminal batches' exact journal-owned postimage temps.
    /// No workspace walk: names, orphan restore files and another task's
    /// artifacts are not evidence of ownership. Changed contents survive.
    fn sweep_tmps(
        &self,
        batches: &std::collections::BTreeMap<MutationBatchId, crate::journal::ReplayedBatch>,
        sweep_errors: &mut Vec<String>,
    ) -> Vec<PathBuf> {
        let mut swept = Vec::new();
        for batch in batches.values() {
            for file in &batch.files {
                if !batch.completed
                    && !matches!(file.state, FileState::Committed | FileState::RolledBack)
                {
                    continue;
                }
                let Ok(rel) = normalize_rel(&file.path) else {
                    continue;
                };
                if rel != file.path {
                    continue;
                }
                let Some(name) = Path::new(&rel).file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                if file.temp_name != Self::temp_name(name, &batch.id, TEMP_MARKER) {
                    continue;
                }
                let Ok(target) = self.resolve(&rel) else {
                    continue;
                };
                let temp = target.with_file_name(&file.temp_name);
                // Canonical identity must not redirect a deletion through a
                // symlink, including a symlink in an ancestor directory.
                let Ok(root) = std::fs::canonicalize(&self.workspace_root) else {
                    continue;
                };
                let expected = root.join(Path::new(&rel).with_file_name(&file.temp_name));
                let Ok(canonical) = std::fs::canonicalize(&temp) else {
                    continue;
                };
                if canonical != expected
                    || !std::fs::symlink_metadata(&temp).is_ok_and(|meta| meta.is_file())
                    || !file_hash(&temp).is_ok_and(|hash| hash == Some(file.post_hash.clone()))
                {
                    continue;
                }
                match std::fs::remove_file(&temp) {
                    Ok(()) => swept.push(temp),
                    Err(error) => sweep_errors.push(error.to_string()),
                }
            }
        }
        swept
    }

    /// Checks a caller descriptor against the journaled plan. Prepare
    /// always journals before returning, so a missing or mismatched plan
    /// means fabrication — refuse with `UnknownBatch`. Subsets match:
    /// recovery resumes with the still-valid files only, and every given
    /// file must equal its journaled entry (path, hashes, temp name).
    fn check_plan(
        &self,
        id: &MutationBatchId,
        files: &[FileMutation],
    ) -> Result<(), MutationError> {
        let journaled = self.replay_files(id)?;
        if journaled.is_empty() && !files.is_empty() {
            return Err(MutationError::UnknownBatch(id.to_string()));
        }
        let matches = files.iter().all(|given| {
            journaled.iter().any(|known| {
                known.path == given.path
                    && known.pre_hash == given.pre_hash
                    && known.post_hash == given.post_hash
                    && known.temp_name == given.temp_name
                    && known.post_artifact == given.post_artifact
            })
        });
        if matches {
            Ok(())
        } else {
            Err(MutationError::UnknownBatch(id.to_string()))
        }
    }
    fn is_completed(&self, id: &MutationBatchId) -> Result<bool, MutationError> {
        Ok(self
            .journal
            .replay()?
            .get(id)
            .is_some_and(|batch| batch.completed))
    }

    /// Journaled files for `id` (empty when unknown).
    fn replay_files(&self, id: &MutationBatchId) -> Result<Vec<FileMutation>, MutationError> {
        Ok(self
            .journal
            .replay()?
            .get(id)
            .map(|batch| batch.files.clone())
            .unwrap_or_default())
    }
}

/// Writes `bytes` to `path` and syncs before returning.
fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), MutationError> {
    use std::io::Write as _;
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Writes `bytes` to a new file at `path` and syncs before returning. Fails
/// instead of truncating an existing file: a temp path that already exists is
/// not this batch's staging file (see RECOVERY.md).
fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), MutationError> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Best-effort parent-directory sync (durability hint; the journal stays
/// the source of truth on platforms without directory sync).
fn sync_parent(path: &Path) {
    if let Some(parent) = path.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ignored = dir.sync_all();
    }
}

/// Maps tool-layer failures into [`MutationError::Io`].
fn io_error(error: &tachyon_tools::ToolError) -> MutationError {
    MutationError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_names_carry_batch_and_marker() {
        let id = MutationBatchId::generate();
        let name = MutationEngine::temp_name("auth.rs", &id, TEMP_MARKER);
        assert!(name.contains(TEMP_MARKER));
        assert!(name.starts_with(".auth.rs."));
    }

    #[test]
    fn effect_declaration_is_reversible() {
        assert_eq!(EFFECT_CLASS, EffectClass::ReversibleLocalMutation);
        assert_eq!(IDEMPOTENCY, Idempotency::Compensatable);
    }
}
