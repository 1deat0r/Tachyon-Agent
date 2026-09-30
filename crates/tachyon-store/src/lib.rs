//! Tachyon Store.
//!
//! SQLite durability for the runtime: sessions, tasks, the append-only
//! event journal, and snapshots (spec §17–§18).
//!
//! All correctness-critical writes flow through [`StoreWriter`], the single
//! logical writer. It serializes writers with a mutex over a
//! single-connection pool and commits every operation immediately:
//! durability first, microbatching later (a Milestone 13 tuning item, not
//! a Milestone 1 behavior). Snapshots are opaque [`String`] documents owned
//! by `tachyon-core`; the store never interprets them.

#![warn(unsafe_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tachyon_types::Timestamp;
use thiserror::Error;
use tokio::sync::{Mutex, broadcast};

/// One commit notification: `(task_id, committed_seq)`.
///
/// Fired strictly **after** a journal commit returns `Ok`, so a receiver
/// that observes it can safely read the event back by cursor.
pub type CommitNotice = (String, i64);

/// Capacity of the commit-notification broadcast.
///
/// A receiver that falls behind is told how many it missed and catches up
/// from the journal by cursor — nothing is lost, only the wakeup is.
pub const COMMIT_NOTIFICATION_CAPACITY: usize = 256;

/// Errors produced by the durability layer.
#[derive(Debug, Error)]
pub enum StoreError {
    /// SQLite failure.
    #[error("sqlite error: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Migration failure.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// Stored data does not parse (ids, JSON shapes).
    #[error("corrupt stored data: {detail}")]
    Corrupt {
        /// What failed to parse.
        detail: String,
    },
    /// No task with this id exists.
    #[error("task not found: {task_id}")]
    TaskNotFound {
        /// Requested task id.
        task_id: String,
    },
    /// No approval row with this id exists.
    #[error("approval not found: {approval_id}")]
    ApprovalNotFound {
        /// Requested approval id.
        approval_id: String,
    },
    /// The approval row exists but is not in the state the transition
    /// requires (only `pending` rows accept a decision, only `granted`
    /// rows accept `applied`, only `pending` rows accept `expired`).
    #[error("approval {approval_id} is in state {decision}, which this transition does not accept")]
    ApprovalWrongState {
        /// Requested approval id.
        approval_id: String,
        /// The row's current decision state.
        decision: String,
    },
    /// No effect row with this id exists.
    #[error("effect not found: {effect_id}")]
    EffectNotFound {
        /// Requested effect id.
        effect_id: String,
    },
}

/// One row of the 5-column `approvals` table (M11 D4: no migration).
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct ApprovalRow {
    /// Approval id (hyphenated UUID).
    pub id: String,
    /// Owning task id.
    pub task_id: String,
    /// BLAKE3 hash (hex) of the exact operation this decision binds to.
    pub operation_hash: String,
    /// Row machine state: `pending`, `granted`, `denied`, `applied`, `expired`.
    pub decision: String,
    /// Decision time (micros since epoch); 0 while `pending`.
    pub decided_at: i64,
}

/// One row of the `effects` table (M12 §19 crash reconciliation).
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct EffectRow {
    /// Effect id (unique per attempt; doubles as the idempotency key for Keyed effects).
    pub id: String,
    /// Owning task id.
    pub task_id: String,
    /// Owning execution node. NULL only for legacy M12 rows written before
    /// the general journal protocol.
    pub node_id: Option<String>,
    /// Effect class name (spec §19 `EffectClass`).
    pub effect_class: String,
    /// Idempotency name (spec §19 `Idempotency`).
    pub idempotency: String,
    /// Row state: `prepared`, `committed`, or `unknown_after_crash`.
    pub state: String,
    /// Receipt / query result once committed.
    pub receipt: Option<String>,
    /// Last transition time (micros since epoch).
    pub updated_at: i64,
}

/// Effect-table projection updated in the same transaction as its journal
/// event and task snapshot.
pub enum EffectMutation<'a> {
    /// Persist the `EffectPrepared` barrier before the action can run.
    Prepared {
        /// Stable effect identity and keyed idempotency key.
        effect_id: &'a str,
        /// Node which owns this effect.
        node_id: &'a str,
        /// Validated IR effect class name.
        effect_class: &'a str,
        /// Validated IR idempotency name.
        idempotency: &'a str,
    },
    /// Persist the `EffectCommitted` receipt.
    Committed {
        /// Prepared effect identity.
        effect_id: &'a str,
        /// Resulting receipt or query result.
        receipt: &'a str,
    },
    /// Persist a fail-closed recovery classification.
    UnknownAfterCrash {
        /// Prepared effect identity.
        effect_id: &'a str,
    },
}

/// The two human decisions a pending approval row accepts (M11 item 8):
/// `pending -> granted | denied`. `applied` and `expired` are written by
/// the supervisor through their own transitions, never through `decide`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalOutcome {
    /// The human granted the operation; the row still awaits `applied`.
    Granted,
    /// The human denied the operation.
    Denied,
}

/// One row of `tasks`, including the optional opaque snapshot.
#[derive(Clone, Debug, FromRow)]
pub struct TaskRow {
    /// Task id (hyphenated UUID).
    pub id: String,
    /// Owning session id.
    pub session_id: String,
    /// Workspace id.
    pub workspace_id: String,
    /// User's objective text.
    pub objective: String,
    /// Status name.
    pub status: String,
    /// State revision.
    pub revision: i64,
    /// Opaque snapshot document, if any.
    pub snapshot_json: Option<String>,
    /// Journal sequence the snapshot covers, if any.
    pub snapshot_seq: Option<i64>,
    /// Creation time (micros since epoch).
    pub created_at: i64,
    /// Last update time (micros since epoch).
    pub updated_at: i64,
}

/// One row of `sessions`, including the optional durable Session root.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct SessionRow {
    /// Session id (hyphenated UUID).
    pub id: String,
    /// Creation time (micros since epoch).
    pub created_at: i64,
    /// Canonical Session root, or `None` for a legacy session created
    /// without one.
    pub workspace_root: Option<String>,
}

/// One entry of a session's ordered turn skeleton: the turn's sequence
/// number, the task that occupies it, and that task's canonical status
/// name. Derived read-only from the `tasks` row (ADR-0005 Session
/// history).
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct SessionTurn {
    /// Per-session monotonic turn sequence (dense, starts at 1).
    pub turn_seq: i64,
    /// Task occupying this turn.
    pub task_id: String,
    /// Task's canonical status name.
    pub status: String,
}

/// One row of `task_events`.
#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct JournalEvent {
    /// Per-task sequence cursor.
    pub seq: i64,
    /// Event id (hyphenated UUID).
    pub event_id: String,
    /// Envelope schema version.
    pub schema_version: i64,
    /// Transition kind (`created`, `message`, `constraint`, `status`, …).
    pub kind: String,
    /// Transition payload (JSON).
    pub payload: String,
    /// Journal time (micros since epoch).
    pub created_at: i64,
}

/// Task list entry for clients.
#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct TaskSummary {
    /// Task id.
    pub id: String,
    /// Owning session id.
    pub session_id: String,
    /// User's objective text.
    pub objective: String,
    /// Status name.
    pub status: String,
    /// State revision.
    pub revision: i64,
    /// Last update time.
    pub updated_at: i64,
}

/// Materialized task metadata committed atomically with a journal event.
pub struct TransitionState<'a> {
    pub status: &'a str,
    pub revision: i64,
    pub snapshot_json: Option<&'a str>,
}

/// The single logical writer of correctness-critical state.
pub struct StoreWriter {
    database_path: PathBuf,
    pool: sqlx::SqlitePool,
    write: Mutex<()>,
    commits: broadcast::Sender<CommitNotice>,
}

impl StoreWriter {
    /// Opens (creating) `state.db` under `data_dir` and runs migrations.
    pub async fn open(data_dir: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(data_dir.join("state.db"))
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        let database_path = data_dir
            .join("state.db")
            .canonicalize()
            .map_err(sqlx::Error::Io)?;
        let (commits, _) = broadcast::channel(COMMIT_NOTIFICATION_CAPACITY);
        Ok(Self {
            database_path,
            pool,
            write: Mutex::new(()),
            commits,
        })
    }

    /// Subscribes to commit notifications fired by this writer.
    ///
    /// The notification carries `(task_id, seq)` and is sent only after the
    /// commit it reports has returned `Ok`. On [`broadcast::error::RecvError::Lagged`]
    /// the receiver must catch up with [`StoreWriter::load_events_since`] from
    /// its own cursor — the journal, not the notification, is the source of
    /// truth.
    #[must_use]
    pub fn subscribe_commits(&self) -> broadcast::Receiver<CommitNotice> {
        self.commits.subscribe()
    }

    /// Announces a committed journal write. Best-effort: with no receivers
    /// there is nobody to tell, and the journal already holds the event.
    fn notify_commit(&self, task_id: &str, seq: i64) {
        let _ = self.commits.send((task_id.to_owned(), seq));
    }

    /// Canonical database identity, shared by independently opened aliases.
    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// Inserts a session row without a Session root (legacy behavior).
    pub async fn create_session(&self, session_id: &str) -> Result<(), StoreError> {
        self.create_session_with_root(session_id, None).await
    }

    /// Inserts a session row, optionally binding a canonical Session
    /// root in the same statement. `workspace_root` must already be
    /// canonicalized and authorized by the caller (the gateway reuses
    /// its existing workspace validation for that); the store only
    /// persists what it is given.
    pub async fn create_session_with_root(
        &self,
        session_id: &str,
        workspace_root: Option<&str>,
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        sqlx::query("INSERT INTO sessions (id, created_at, workspace_root) VALUES (?, ?, ?)")
            .bind(session_id)
            .bind(now)
            .bind(workspace_root)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Inserts a task row plus its `created` journal event (seq 0) atomically.
    /// `snapshot_json` is the initial full-state document; `created_payload`
    /// is the journal payload for seq 0 (a `Created` transition document
    /// owned by `tachyon-core`, opaque here).
    ///
    /// The per-session turn sequence is assigned inside the INSERT itself
    /// (`MAX(turn_seq) + 1` for the session) and enforced by the
    /// `UNIQUE (session_id, turn_seq)` index, so racing creators can never
    /// double-assign a turn; the stamp is durable with the row, before any
    /// run can start, and no later write ever touches it.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_task(
        &self,
        task_id: &str,
        session_id: &str,
        workspace_id: &str,
        objective: &str,
        status: &str,
        snapshot_json: &str,
        created_payload: &str,
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO tasks (id, session_id, workspace_id, objective, status,
             revision, snapshot_json, snapshot_seq, created_at, updated_at, turn_seq)
             VALUES (?, ?, ?, ?, ?, 0, ?, 0, ?, ?,
                (SELECT COALESCE(MAX(t.turn_seq), 0) + 1
                 FROM tasks AS t WHERE t.session_id = ?))",
        )
        .bind(task_id)
        .bind(session_id)
        .bind(workspace_id)
        .bind(objective)
        .bind(status)
        .bind(snapshot_json)
        .bind(now)
        .bind(now)
        .bind(session_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO task_events (task_id, seq, event_id, schema_version,
             kind, payload, created_at)
             VALUES (?, 0, ?, 1, 'created', ?, ?)",
        )
        .bind(task_id)
        .bind(tachyon_types::EventId::generate().to_string())
        .bind(created_payload)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.notify_commit(task_id, 0);
        Ok(())
    }

    /// Appends one journal event; returns its per-task sequence number.
    pub async fn append_event(
        &self,
        task_id: &str,
        kind: &str,
        payload: &str,
    ) -> Result<i64, StoreError> {
        self.append(task_id, kind, payload, None, None).await
    }

    /// Journal and projected status/revision/snapshot share one SQLite commit.
    /// A crash cannot leave a terminal task row without its acceptance event.
    pub async fn append_transition(
        &self,
        task_id: &str,
        kind: &str,
        payload: &str,
        state: TransitionState<'_>,
    ) -> Result<i64, StoreError> {
        self.append(task_id, kind, payload, Some(state), None).await
    }

    /// Appends a journal transition, materialized task state, and effect
    /// projection mutation in one SQLite transaction. Used for the
    /// EffectPrepared/EffectCommitted crash barrier protocol.
    pub async fn append_effect_transition(
        &self,
        task_id: &str,
        kind: &str,
        payload: &str,
        state: TransitionState<'_>,
        effect: EffectMutation<'_>,
    ) -> Result<i64, StoreError> {
        self.append(task_id, kind, payload, Some(state), Some(effect))
            .await
    }

    async fn append(
        &self,
        task_id: &str,
        kind: &str,
        payload: &str,
        state: Option<TransitionState<'_>>,
        effect: Option<EffectMutation<'_>>,
    ) -> Result<i64, StoreError> {
        let _guard = self.write.lock().await;
        let mut tx = self.pool.begin().await?;
        let next: Option<i64> =
            sqlx::query_scalar("SELECT MAX(seq) + 1 FROM task_events WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&mut *tx)
                .await?;
        let seq = next.unwrap_or(0);
        let now = Timestamp::now().as_micros();
        sqlx::query(
            "INSERT INTO task_events (task_id, seq, event_id, schema_version,
             kind, payload, created_at)
             VALUES (?, ?, ?, 1, ?, ?, ?)",
        )
        .bind(task_id)
        .bind(seq)
        .bind(tachyon_types::EventId::generate().to_string())
        .bind(kind)
        .bind(payload)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE tasks SET updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(task_id)
            .execute(&mut *tx)
            .await?;
        if let Some(state) = state {
            sqlx::query(
                "UPDATE tasks SET status = ?, revision = ?,
                 snapshot_seq = CASE WHEN ? IS NULL THEN snapshot_seq ELSE ? END,
                 snapshot_json = COALESCE(?, snapshot_json) WHERE id = ?",
            )
            .bind(state.status)
            .bind(state.revision)
            .bind(state.snapshot_json)
            .bind(seq)
            .bind(state.snapshot_json)
            .bind(task_id)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(effect) = effect {
            apply_effect_mutation(&mut tx, task_id, effect).await?;
        }
        tx.commit().await?;
        self.notify_commit(task_id, seq);
        Ok(seq)
    }

    /// Replaces the task snapshot and its metadata after a transition.
    pub async fn save_snapshot(
        &self,
        task_id: &str,
        snapshot_seq: i64,
        snapshot_json: &str,
        status: &str,
        revision: i64,
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let rows = sqlx::query(
            "UPDATE tasks SET snapshot_json = ?, snapshot_seq = ?, status = ?,
             revision = ?, updated_at = ? WHERE id = ?",
        )
        .bind(snapshot_json)
        .bind(snapshot_seq)
        .bind(status)
        .bind(revision)
        .bind(now)
        .bind(task_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if rows == 0 {
            return Err(StoreError::TaskNotFound {
                task_id: task_id.to_owned(),
            });
        }
        Ok(())
    }

    /// Closes the pool, waiting for checked-out connections to return.
    /// Call before deleting the data directory (mandatory on Windows,
    /// where open files cannot be removed).
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Loads a task row, or `None` when absent.
    pub async fn load_task(&self, task_id: &str) -> Result<Option<TaskRow>, StoreError> {
        sqlx::query_as::<_, TaskRow>("SELECT * FROM tasks WHERE id = ?")
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(StoreError::from)
    }

    /// Loads journal events strictly after `after_seq`, in order.
    pub async fn load_events_since(
        &self,
        task_id: &str,
        after_seq: i64,
    ) -> Result<Vec<JournalEvent>, StoreError> {
        sqlx::query_as::<_, JournalEvent>(
            "SELECT seq, event_id, schema_version, kind, payload, created_at
             FROM task_events WHERE task_id = ? AND seq > ? ORDER BY seq",
        )
        .bind(task_id)
        .bind(after_seq)
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Highest journaled `seq` for one task, or -1 when it has no events.
    /// Lets subscribers ask for the cursor without loading the journal.
    pub async fn latest_seq(&self, task_id: &str) -> Result<i64, StoreError> {
        let max: Option<i64> =
            sqlx::query_scalar("SELECT MAX(seq) FROM task_events WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&self.pool)
                .await
                .map_err(StoreError::from)?;
        Ok(max.unwrap_or(-1))
    }

    /// Lists tasks, optionally restricted to one session, newest first.
    pub async fn list_tasks(
        &self,
        session_id: Option<&str>,
    ) -> Result<Vec<TaskSummary>, StoreError> {
        if let Some(session) = session_id {
            sqlx::query_as::<_, TaskSummary>(
                "SELECT id, session_id, objective, status, revision, updated_at
                 FROM tasks WHERE session_id = ? ORDER BY updated_at DESC",
            )
            .bind(session)
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::from)
        } else {
            sqlx::query_as::<_, TaskSummary>(
                "SELECT id, session_id, objective, status, revision, updated_at
                 FROM tasks ORDER BY updated_at DESC",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::from)
        }
    }

    /// True when a session row exists.
    pub async fn session_exists(&self, session_id: &str) -> Result<bool, StoreError> {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sessions WHERE id = ?")
            .bind(session_id)
            .fetch_one(&self.pool)
            .await
            .map(|count| count > 0)
            .map_err(StoreError::from)
    }

    /// Loads a session row (identity plus optional Session root), or
    /// `None` when absent. Read-only: the gateway's `GetSession` path
    /// observes through this query and never writes.
    pub async fn load_session(&self, session_id: &str) -> Result<Option<SessionRow>, StoreError> {
        sqlx::query_as::<_, SessionRow>(
            "SELECT id, created_at, workspace_root FROM sessions WHERE id = ?",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Lists one session's turns in stable ascending sequence order
    /// (Session history skeleton). Strictly read-only: observes the
    /// `turn_seq` stamped at creation and never writes.
    pub async fn load_session_turns(
        &self,
        session_id: &str,
    ) -> Result<Vec<SessionTurn>, StoreError> {
        sqlx::query_as::<_, SessionTurn>(
            "SELECT turn_seq, id AS task_id, status
             FROM tasks WHERE session_id = ? ORDER BY turn_seq ASC",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Ids of tasks that did not reach a terminal state.
    pub async fn incomplete_tasks(&self) -> Result<Vec<String>, StoreError> {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM tasks WHERE status NOT IN ('Completed', 'Failed', 'Cancelled')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Inserts a pending approval row (`decision='pending'`, `decided_at=0`)
    /// into the existing 5-column table — no schema migration (M11 D4).
    /// Invoked only by the task supervisor (single logical writer).
    pub async fn insert_pending(
        &self,
        approval_id: &str,
        task_id: &str,
        operation_hash: &str,
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        sqlx::query(
            "INSERT INTO approvals (id, task_id, operation_hash, decision, decided_at)
             VALUES (?, ?, ?, 'pending', 0)",
        )
        .bind(approval_id)
        .bind(task_id)
        .bind(operation_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Records a human decision: `pending -> granted | denied` with a
    /// wall-clock `decided_at`. Any other current state is a typed error,
    /// so a double decide can never overwrite the first decision.
    pub async fn decide(
        &self,
        approval_id: &str,
        outcome: ApprovalOutcome,
    ) -> Result<ApprovalRow, StoreError> {
        let _guard = self.write.lock().await;
        let decision = match outcome {
            ApprovalOutcome::Granted => "granted",
            ApprovalOutcome::Denied => "denied",
        };
        let now = Timestamp::now().as_micros();
        let changed = sqlx::query(
            "UPDATE approvals SET decision = ?, decided_at = ?
             WHERE id = ? AND decision = 'pending'",
        )
        .bind(decision)
        .bind(now)
        .bind(approval_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(self.approval_transition_error(approval_id).await);
        }
        self.approval_row(approval_id).await
    }

    /// Flips `granted -> applied` before the granted operation executes.
    /// Keeps the original human `decided_at`; only the supervisor writes it.
    pub async fn mark_applied(&self, approval_id: &str) -> Result<ApprovalRow, StoreError> {
        let _guard = self.write.lock().await;
        let changed = sqlx::query(
            "UPDATE approvals SET decision = 'applied'
             WHERE id = ? AND decision = 'granted'",
        )
        .bind(approval_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(self.approval_transition_error(approval_id).await);
        }
        self.approval_row(approval_id).await
    }

    /// Expires a still-`pending` row (cancel wins, restart-during-wait).
    /// Records when the expiry happened in `decided_at`.
    pub async fn expire(&self, approval_id: &str) -> Result<ApprovalRow, StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let changed = sqlx::query(
            "UPDATE approvals SET decision = 'expired', decided_at = ?
             WHERE id = ? AND decision = 'pending'",
        )
        .bind(now)
        .bind(approval_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(self.approval_transition_error(approval_id).await);
        }
        self.approval_row(approval_id).await
    }

    /// Expires a `granted`-never-`applied` row (crash between `decide` and
    /// `mark_applied`). Mirror of [`Self::expire`]: only the stated source
    /// decision moves. Safe because nothing could have executed — execution
    /// needs the waiter resolved after `applied` — so the continuation
    /// re-asks under a fresh id.
    pub async fn expire_granted(&self, approval_id: &str) -> Result<ApprovalRow, StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let changed = sqlx::query(
            "UPDATE approvals SET decision = 'expired', decided_at = ?
             WHERE id = ? AND decision = 'granted'",
        )
        .bind(now)
        .bind(approval_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(self.approval_transition_error(approval_id).await);
        }
        self.approval_row(approval_id).await
    }

    /// Loads one approval row, or `None` when absent (gateway id -> task
    /// resolution is a read; writes stay supervisor-owned).
    pub async fn load_by_id(&self, approval_id: &str) -> Result<Option<ApprovalRow>, StoreError> {
        sqlx::query_as::<_, ApprovalRow>(
            "SELECT id, task_id, operation_hash, decision, decided_at
             FROM approvals WHERE id = ?",
        )
        .bind(approval_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// All still-`pending` approval rows for one task, in insert order.
    pub async fn load_pending_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<ApprovalRow>, StoreError> {
        sqlx::query_as::<_, ApprovalRow>(
            "SELECT id, task_id, operation_hash, decision, decided_at
             FROM approvals WHERE task_id = ? AND decision = 'pending' ORDER BY rowid",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// All `granted`-never-`applied` approval rows for one task, in insert
    /// order. Recovery expires these alongside stale pendings.
    pub async fn load_granted_for_task(
        &self,
        task_id: &str,
    ) -> Result<Vec<ApprovalRow>, StoreError> {
        sqlx::query_as::<_, ApprovalRow>(
            "SELECT id, task_id, operation_hash, decision, decided_at
             FROM approvals WHERE task_id = ? AND decision = 'granted' ORDER BY rowid",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Maps a zero-row approval transition to its typed error: missing row
    /// or a row whose current state the transition does not accept.
    async fn approval_transition_error(&self, approval_id: &str) -> StoreError {
        match self.load_by_id(approval_id).await {
            Ok(None) => StoreError::ApprovalNotFound {
                approval_id: approval_id.to_owned(),
            },
            Ok(Some(row)) => StoreError::ApprovalWrongState {
                approval_id: approval_id.to_owned(),
                decision: row.decision,
            },
            Err(error) => error,
        }
    }

    /// Loads a row that a successful transition just wrote; absence would
    /// mean the journal lies, which fails closed as corruption.
    async fn approval_row(&self, approval_id: &str) -> Result<ApprovalRow, StoreError> {
        self.load_by_id(approval_id)
            .await?
            .ok_or_else(|| StoreError::Corrupt {
                detail: format!("approval {approval_id} vanished mid-transition"),
            })
    }

    /// Records an effect at the `EffectPrepared` barrier (spec §19):
    /// legacy M12 fixture helper. Production code must use the supervisor's
    /// journalled effect protocol so the table and task state cannot diverge.
    #[deprecated(note = "legacy M12 fixture only; use SupervisorHandle::prepare_effect")]
    pub async fn insert_effect_prepared(
        &self,
        effect_id: &str,
        task_id: &str,
        effect_class: &str,
        idempotency: &str,
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        sqlx::query(
            "INSERT INTO effects (id, task_id, effect_class, idempotency, state, receipt, updated_at)
             VALUES (?, ?, ?, ?, 'prepared', NULL, ?)",
        )
        .bind(effect_id)
        .bind(task_id)
        .bind(effect_class)
        .bind(idempotency)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Records `EffectCommitted` with a receipt: only a `prepared` row
    /// accepts the flip; legacy M12 fixture helper. Production code must use
    /// the supervisor's journalled effect protocol.
    #[deprecated(note = "legacy M12 fixture only; use SupervisorHandle::commit_effect")]
    pub async fn commit_effect(
        &self,
        effect_id: &str,
        receipt: &str,
    ) -> Result<EffectRow, StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let changed = sqlx::query(
            "UPDATE effects SET state = 'committed', receipt = ?, updated_at = ?
             WHERE id = ? AND state = 'prepared'",
        )
        .bind(receipt)
        .bind(now)
        .bind(effect_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(self.effect_transition_error(effect_id).await);
        }
        self.effect_row(effect_id).await
    }

    /// Marks a still-`prepared` row `unknown_after_crash` (spec §19
    /// NonIdempotent/Unknown). Legacy M12 fixture helper; production recovery
    /// journals this projection update through the supervisor.
    #[deprecated(note = "legacy M12 fixture only; recovery uses journalled transitions")]
    pub async fn mark_effect_unknown_after_crash(
        &self,
        effect_id: &str,
    ) -> Result<EffectRow, StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let changed = sqlx::query(
            "UPDATE effects SET state = 'unknown_after_crash', updated_at = ?
             WHERE id = ? AND state = 'prepared'",
        )
        .bind(now)
        .bind(effect_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed == 0 {
            return Err(self.effect_transition_error(effect_id).await);
        }
        self.effect_row(effect_id).await
    }

    /// All effect rows for one task, in insert order (recovery input).
    pub async fn load_effects_for_task(&self, task_id: &str) -> Result<Vec<EffectRow>, StoreError> {
        sqlx::query_as::<_, EffectRow>(
            "SELECT id, task_id, node_id, effect_class, idempotency, state, receipt, updated_at
             FROM effects WHERE task_id = ? ORDER BY rowid",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Loads one effect row by id.
    pub async fn load_effect(&self, effect_id: &str) -> Result<Option<EffectRow>, StoreError> {
        sqlx::query_as::<_, EffectRow>(
            "SELECT id, task_id, node_id, effect_class, idempotency, state, receipt, updated_at
             FROM effects WHERE id = ?",
        )
        .bind(effect_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Maps a zero-row effect transition to a typed error.
    async fn effect_transition_error(&self, effect_id: &str) -> StoreError {
        match self.load_effect(effect_id).await {
            Ok(None) => StoreError::EffectNotFound {
                effect_id: effect_id.to_owned(),
            },
            Ok(Some(row)) => StoreError::Corrupt {
                detail: format!(
                    "effect {effect_id} in state {} does not accept this transition",
                    row.state
                ),
            },
            Err(error) => error,
        }
    }

    /// Loads a row that a successful effect transition just wrote.
    async fn effect_row(&self, effect_id: &str) -> Result<EffectRow, StoreError> {
        self.load_effect(effect_id)
            .await?
            .ok_or_else(|| StoreError::Corrupt {
                detail: format!("effect {effect_id} vanished mid-transition"),
            })
    }
}

async fn apply_effect_mutation(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &str,
    effect: EffectMutation<'_>,
) -> Result<(), StoreError> {
    let now = Timestamp::now().as_micros();
    match effect {
        EffectMutation::Prepared {
            effect_id,
            node_id,
            effect_class,
            idempotency,
        } => {
            sqlx::query(
                "INSERT INTO effects (id, task_id, node_id, effect_class, idempotency,
                 state, receipt, updated_at)
                 VALUES (?, ?, ?, ?, ?, 'prepared', NULL, ?)",
            )
            .bind(effect_id)
            .bind(task_id)
            .bind(node_id)
            .bind(effect_class)
            .bind(idempotency)
            .bind(now)
            .execute(&mut **tx)
            .await?;
        }
        EffectMutation::Committed { effect_id, receipt } => {
            let changed = sqlx::query(
                "UPDATE effects SET state = 'committed', receipt = ?, updated_at = ?
                 WHERE id = ? AND task_id = ? AND state = 'prepared'",
            )
            .bind(receipt)
            .bind(now)
            .bind(effect_id)
            .bind(task_id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            if changed == 0 {
                return Err(effect_transition_error_in_tx(tx, effect_id).await);
            }
        }
        EffectMutation::UnknownAfterCrash { effect_id } => {
            let changed = sqlx::query(
                "UPDATE effects SET state = 'unknown_after_crash', updated_at = ?
                 WHERE id = ? AND task_id = ? AND state = 'prepared'",
            )
            .bind(now)
            .bind(effect_id)
            .bind(task_id)
            .execute(&mut **tx)
            .await?
            .rows_affected();
            if changed == 0 {
                return Err(effect_transition_error_in_tx(tx, effect_id).await);
            }
        }
    }
    Ok(())
}

async fn effect_transition_error_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    effect_id: &str,
) -> StoreError {
    match sqlx::query_as::<_, (String, String)>("SELECT task_id, state FROM effects WHERE id = ?")
        .bind(effect_id)
        .fetch_optional(&mut **tx)
        .await
    {
        Ok(None) => StoreError::EffectNotFound {
            effect_id: effect_id.to_owned(),
        },
        Ok(Some((_, state))) => StoreError::Corrupt {
            detail: format!("effect {effect_id} in state {state} does not accept this transition"),
        },
        Err(error) => StoreError::Sqlx(error),
    }
}

#[cfg(test)]
mod tests {
    use super::{EffectMutation, SessionTurn, StoreWriter, TransitionState};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::sync::broadcast::Receiver;

    type Commit = (String, i64);

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    async fn open_test_store() -> (StoreWriter, PathBuf) {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("tachyon-store-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = StoreWriter::open(&dir).await.unwrap();
        (store, dir)
    }

    /// Awaits one commit notification, failing loudly instead of hanging.
    async fn next_commit(rx: &mut Receiver<Commit>) -> Commit {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("commit notification timed out")
            .expect("commit notification channel closed")
    }

    #[tokio::test]
    async fn successful_commits_notify_subscribers_with_task_and_seq() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        let mut rx = store.subscribe_commits();

        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        assert_eq!(next_commit(&mut rx).await, ("t".to_owned(), 0));

        let seq = store.append_event("t", "message", "{}").await.unwrap();
        assert_eq!(seq, 1);
        assert_eq!(next_commit(&mut rx).await, ("t".to_owned(), 1));

        let seq = store
            .append_transition(
                "t",
                "status",
                "{}",
                super::TransitionState {
                    status: "Completed",
                    revision: 1,
                    snapshot_json: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(seq, 2);
        assert_eq!(next_commit(&mut rx).await, ("t".to_owned(), 2));

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn failed_commit_sends_no_notification() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        let mut rx = store.subscribe_commits();

        // Force the projection half of the commit to abort so the whole
        // transaction rolls back after the journal insert.
        sqlx::query("CREATE TRIGGER fault_projection BEFORE UPDATE OF status ON tasks BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END")
            .execute(&store.pool).await.unwrap();
        let result = store
            .append_transition(
                "t",
                "verification_finished",
                "{}",
                super::TransitionState {
                    status: "Completed",
                    revision: 1,
                    snapshot_json: None,
                },
            )
            .await;
        assert!(result.is_err(), "injected fault must fail the commit");
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "a rolled-back commit must not notify subscribers"
        );

        // The rollback must not wedge the channel: the next real commit
        // still notifies, with the sequence it actually committed.
        sqlx::query("DROP TRIGGER fault_projection")
            .execute(&store.pool)
            .await
            .unwrap();
        let seq = store.append_event("t", "message", "{}").await.unwrap();
        assert_eq!(next_commit(&mut rx).await, ("t".to_owned(), seq));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn lagged_receiver_catches_up_from_the_journal_by_cursor() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        let mut rx = store.subscribe_commits();

        // Commit past the broadcast capacity without ever reading, so the
        // receiver's wakeup is genuinely lost rather than merely delayed.
        let extra = super::COMMIT_NOTIFICATION_CAPACITY + 8;
        for _ in 0..extra {
            store.append_event("t", "message", "{}").await.unwrap();
        }
        match tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("commit notification timed out")
        {
            Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                assert!(missed > 0, "the receiver must be told it fell behind");
            }
            other => panic!("expected a lagged receiver, got {other:?}"),
        }

        // Catch-up is a cursor read of the journal: every committed event is
        // still there, in order, gapless.
        let rows = store.load_events_since("t", -1).await.unwrap();
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        let expected: Vec<i64> = (0..=i64::try_from(extra).expect("extra fits i64")).collect();
        assert_eq!(seqs, expected, "journal must hold every committed event");

        // And the receiver keeps working after the lag.
        while !matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ) {}
        let seq = store.append_event("t", "message", "{}").await.unwrap();
        assert_eq!(next_commit(&mut rx).await, ("t".to_owned(), seq));

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn task_lifecycle_round_trips() {
        let (store, dir) = open_test_store().await;
        store.create_session("session-1").await.unwrap();
        store
            .create_task(
                "task-1",
                "session-1",
                "ws-1",
                "do a thing",
                "Created",
                "{}",
                "{}",
            )
            .await
            .unwrap();

        let seq = store
            .append_event("task-1", "message", "{\"text\":\"hi\"}")
            .await
            .unwrap();
        assert_eq!(seq, 1);
        store
            .save_snapshot("task-1", seq, "{\"rev\":1}", "Created", 1)
            .await
            .unwrap();

        let row = store.load_task("task-1").await.unwrap().unwrap();
        assert_eq!(row.status, "Created");
        assert_eq!(row.revision, 1);
        assert_eq!(row.snapshot_seq, Some(1));

        let tail = store.load_events_since("task-1", 0).await.unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].kind, "message");

        let all = store.load_events_since("task-1", -1).await.unwrap();
        assert_eq!(all.len(), 2);

        let tasks = store.list_tasks(None).await.unwrap();
        assert_eq!(tasks.len(), 1);
        let filtered = store.list_tasks(Some("session-1")).await.unwrap();
        assert_eq!(filtered.len(), 1);
        let empty = store.list_tasks(Some("nope")).await.unwrap();
        assert!(empty.is_empty());

        let incomplete = store.incomplete_tasks().await.unwrap();
        assert_eq!(incomplete, vec!["task-1".to_owned()]);

        assert!(store.load_task("missing").await.unwrap().is_none());
        let err = store
            .save_snapshot("missing", 0, "{}", "Created", 0)
            .await
            .unwrap_err();
        assert!(matches!(err, super::StoreError::TaskNotFound { .. }));

        store.close().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn failed_effect_commit_rolls_back_journal_task_and_effect_projection() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();

        store
            .append_effect_transition(
                "t",
                "effect_prepared",
                "{\"effect_id\":\"effect-1\"}",
                TransitionState {
                    status: "Created",
                    revision: 0,
                    snapshot_json: Some("{\"barrier\":\"prepared\"}"),
                },
                EffectMutation::Prepared {
                    effect_id: "effect-1",
                    node_id: "node-1",
                    effect_class: "DestructiveExternalMutation",
                    idempotency: "Keyed",
                },
            )
            .await
            .unwrap();
        let mut rx = store.subscribe_commits();

        sqlx::query(
            "CREATE TRIGGER fault_effect_commit BEFORE UPDATE OF state ON effects
             WHEN NEW.state = 'committed'
             BEGIN SELECT RAISE(ABORT, 'injected effect commit failure'); END",
        )
        .execute(&store.pool)
        .await
        .unwrap();

        let result = store
            .append_effect_transition(
                "t",
                "effect_committed",
                "{\"effect_id\":\"effect-1\",\"receipt\":\"receipt-1\"}",
                TransitionState {
                    status: "Created",
                    revision: 0,
                    snapshot_json: Some("{\"barrier\":\"committed\"}"),
                },
                EffectMutation::Committed {
                    effect_id: "effect-1",
                    receipt: "receipt-1",
                },
            )
            .await;
        assert!(result.is_err(), "the injected projection fault must abort");
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let events = store.load_events_since("t", -1).await.unwrap();
        assert_eq!(events.len(), 2, "the failed event must roll back");
        assert_eq!(events[1].kind, "effect_prepared");
        let task = store.load_task("t").await.unwrap().unwrap();
        assert_eq!(task.status, "Created");
        assert_eq!(task.revision, 0);
        assert_eq!(task.snapshot_seq, Some(1));
        assert_eq!(
            task.snapshot_json.as_deref(),
            Some("{\"barrier\":\"prepared\"}")
        );
        let effect = store.load_effect("effect-1").await.unwrap().unwrap();
        assert_eq!(effect.state, "prepared");
        assert_eq!(effect.receipt, None);

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cancelled_tasks_leave_the_incomplete_set() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        store
            .save_snapshot("t", 0, "{}", "Cancelled", 0)
            .await
            .unwrap();
        assert!(store.incomplete_tasks().await.unwrap().is_empty());
        store.close().await;
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn journal_and_completion_projection_commit_together() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        let seq = store
            .append_transition(
                "t",
                "verification_finished",
                "{}",
                super::TransitionState {
                    status: "Completed",
                    revision: 3,
                    snapshot_json: Some("{\"verified\":true}"),
                },
            )
            .await
            .unwrap();
        let row = store.load_task("t").await.unwrap().unwrap();
        assert_eq!(row.status, "Completed");
        assert_eq!(row.revision, 3);
        assert_eq!(row.snapshot_seq, Some(seq));
        assert_eq!(row.snapshot_json.as_deref(), Some("{\"verified\":true}"));
        assert_eq!(store.load_events_since("t", 0).await.unwrap().len(), 1);
        assert!(store.incomplete_tasks().await.unwrap().is_empty());
        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn projection_failure_rolls_back_the_acceptance_event() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        sqlx::query("CREATE TRIGGER fault_projection BEFORE UPDATE OF status ON tasks BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END")
            .execute(&store.pool).await.unwrap();
        assert!(
            store
                .append_transition(
                    "t",
                    "verification_finished",
                    "{}",
                    super::TransitionState {
                        status: "Completed",
                        revision: 1,
                        snapshot_json: Some("{\"verified\":true}"),
                    }
                )
                .await
                .is_err()
        );
        let row = store.load_task("t").await.unwrap().unwrap();
        assert_eq!(row.status, "Created");
        assert_eq!(row.revision, 0);
        assert_eq!(row.snapshot_seq, Some(0));
        assert!(store.load_events_since("t", 0).await.unwrap().is_empty());
        assert_eq!(store.incomplete_tasks().await.unwrap(), vec!["t"]);
        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Session-root persistence (ACP slice a): the bound root round-trips
    /// through a close/reopen of the same store dir, legacy sessions
    /// report an absent root, and an unknown id is `None`.
    #[tokio::test]
    async fn session_root_persists_across_reopen() {
        let (store, dir) = open_test_store().await;
        store
            .create_session_with_root("s-root", Some("/canonical/ws"))
            .await
            .unwrap();
        store.create_session("s-legacy").await.unwrap();

        let with_root = store.load_session("s-root").await.unwrap().unwrap();
        assert_eq!(with_root.id, "s-root");
        assert_eq!(with_root.workspace_root.as_deref(), Some("/canonical/ws"));
        assert!(with_root.created_at > 0);
        let legacy = store.load_session("s-legacy").await.unwrap().unwrap();
        assert_eq!(legacy.workspace_root, None, "legacy sessions have no root");
        assert!(store.load_session("missing").await.unwrap().is_none());

        // Durability: reopen the same store dir — the bound root must
        // come back byte-identical (the migration upgraded additively).
        store.close().await;
        let reopened = StoreWriter::open(&dir).await.unwrap();
        let after = reopened.load_session("s-root").await.unwrap().unwrap();
        assert_eq!(
            after, with_root,
            "the Session root survives a store restart unchanged"
        );
        assert_eq!(
            reopened.load_session("s-legacy").await.unwrap().unwrap(),
            legacy
        );
        reopened.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    // ---- ACP slice a ticket 02: per-session monotonic turn sequence ----

    /// Sequential creation stamps dense, strictly increasing sequences,
    /// each starting at 1 within its own session.
    #[tokio::test]
    async fn create_task_stamps_strictly_increasing_turn_seq_per_session() {
        let (store, dir) = open_test_store().await;
        store.create_session("s-a").await.unwrap();
        store.create_session("s-b").await.unwrap();

        for i in 0..3 {
            store
                .create_task(&format!("t-a{i}"), "s-a", "w", "obj", "Created", "{}", "{}")
                .await
                .unwrap();
        }
        store
            .create_task("t-b0", "s-b", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();

        let turn = |seq: i64, id: &str| SessionTurn {
            turn_seq: seq,
            task_id: id.to_owned(),
            status: "Created".to_owned(),
        };
        assert_eq!(
            store.load_session_turns("s-a").await.unwrap(),
            vec![turn(1, "t-a0"), turn(2, "t-a1"), turn(3, "t-a2")],
            "per-session sequence is dense and strictly increasing"
        );
        assert_eq!(
            store.load_session_turns("s-b").await.unwrap(),
            vec![turn(1, "t-b0")],
            "each session starts its own sequence at 1"
        );

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Racing creates cannot double-assign: the atomic `MAX+1` INSERT
    /// under `UNIQUE (session_id, turn_seq)` yields one dense sequence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_create_task_never_double_assigns_a_turn_seq() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        let store = std::sync::Arc::new(store);

        let mut handles = Vec::new();
        for i in 0..8 {
            let store = std::sync::Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                store
                    .create_task(&format!("t{i}"), "s", "w", "obj", "Created", "{}", "{}")
                    .await
            }));
        }
        for handle in handles {
            handle
                .await
                .unwrap()
                .expect("every racing create must succeed");
        }

        let turns = store.load_session_turns("s").await.unwrap();
        let seqs: Vec<i64> = turns.iter().map(|turn| turn.turn_seq).collect();
        assert_eq!(
            seqs,
            (1..=8).collect::<Vec<i64>>(),
            "eight racing creates own eight distinct consecutive turns: {turns:?}"
        );

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A terminal task keeps its stamp; the next create lands strictly
    /// above it, and the association survives a store reopen.
    #[tokio::test]
    async fn terminal_tasks_are_never_restamped_or_reused() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t1", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();
        store
            .append_transition(
                "t1",
                "status",
                "{}",
                TransitionState {
                    status: "Completed",
                    revision: 1,
                    snapshot_json: None,
                },
            )
            .await
            .unwrap();
        store
            .create_task("t2", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();

        let turns = store.load_session_turns("s").await.unwrap();
        assert_eq!(
            turns,
            vec![
                SessionTurn {
                    turn_seq: 1,
                    task_id: "t1".to_owned(),
                    status: "Completed".to_owned(),
                },
                SessionTurn {
                    turn_seq: 2,
                    task_id: "t2".to_owned(),
                    status: "Created".to_owned(),
                },
            ],
            "terminal task keeps its stamp; the next turn is strictly above it"
        );

        store.close().await;
        let reopened = StoreWriter::open(&dir).await.unwrap();
        assert_eq!(
            reopened.load_session_turns("s").await.unwrap(),
            turns,
            "turn association and statuses survive a store reopen"
        );
        reopened.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Migration 0004 seeds legacy tasks deterministically: ordered by
    /// `created_at` with the task id breaking same-instant ties, scoped
    /// per session, then the unique index rejects any double assign.
    #[tokio::test]
    async fn migration_seeds_legacy_turn_seq_by_creation_order() {
        use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

        let options = SqliteConnectOptions::new().in_memory(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();

        // Rebuild the pre-0004 schema exactly as it ships on disk.
        let migrations = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        for file in [
            "0001_kernel.sql",
            "0002_effect_node_binding.sql",
            "0003_session_root.sql",
        ] {
            let sql = std::fs::read_to_string(migrations.join(file)).unwrap();
            // Audited: file text from this crate's own checked-in migrations.
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(&pool)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO sessions (id, created_at) VALUES ('s1', 1), ('s2', 1)")
            .execute(&pool)
            .await
            .unwrap();
        // Legacy rows carry no turn_seq; s1 holds a same-instant tie.
        for (id, session, created_at) in [
            ("a", "s1", 100_i64),
            ("b", "s1", 100),
            ("c", "s1", 50),
            ("d", "s1", 200),
            ("z", "s2", 10),
        ] {
            sqlx::query(
                "INSERT INTO tasks (id, session_id, workspace_id, objective, status,
                 revision, created_at, updated_at)
                 VALUES (?, ?, 'w', 'obj', 'Created', 0, ?, ?)",
            )
            .bind(id)
            .bind(session)
            .bind(created_at)
            .bind(created_at)
            .execute(&pool)
            .await
            .unwrap();
        }

        let upgrade = std::fs::read_to_string(migrations.join("0004_turn_seq.sql")).unwrap();
        // Audited: file text from this crate's own checked-in migrations.
        sqlx::query(sqlx::AssertSqlSafe(upgrade))
            .execute(&pool)
            .await
            .unwrap();

        let ranked: Vec<(String, i64)> = sqlx::query_as(
            "SELECT id, turn_seq FROM tasks WHERE session_id = 's1' ORDER BY turn_seq",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            ranked,
            vec![
                ("c".to_owned(), 1),
                ("a".to_owned(), 2),
                ("b".to_owned(), 3),
                ("d".to_owned(), 4),
            ],
            "legacy rows seed by creation order; id breaks the same-instant tie"
        );
        let other: Vec<(String, i64)> =
            sqlx::query_as("SELECT id, turn_seq FROM tasks WHERE session_id = 's2'")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            other,
            vec![("z".to_owned(), 1)],
            "turn sequences are scoped per session"
        );

        // The UNIQUE index now forbids claiming an occupied turn.
        let conflict = sqlx::query(
            "INSERT INTO tasks (id, session_id, workspace_id, objective, status,
             revision, created_at, updated_at, turn_seq)
             VALUES ('dup', 's1', 'w', 'obj', 'Created', 0, 300, 300, 2)",
        )
        .execute(&pool)
        .await;
        assert!(
            conflict.is_err(),
            "UNIQUE (session_id, turn_seq) rejects a duplicate turn"
        );

        pool.close().await;
    }

    // ---- M11 D4: approval row machine (5-column schema, no migration) ----

    #[tokio::test]
    async fn approval_row_lifecycle_pending_granted_applied() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();

        store
            .insert_pending("ap-1", "t", "hash-op-1")
            .await
            .unwrap();
        let row = store.load_by_id("ap-1").await.unwrap().unwrap();
        assert_eq!(row.id, "ap-1");
        assert_eq!(row.task_id, "t");
        assert_eq!(row.operation_hash, "hash-op-1");
        assert_eq!(row.decision, "pending");
        assert_eq!(row.decided_at, 0, "pending rows carry decided_at = 0");
        let pending = store.load_pending_for_task("t").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "ap-1");

        let granted = store
            .decide("ap-1", super::ApprovalOutcome::Granted)
            .await
            .unwrap();
        assert_eq!(granted.decision, "granted");
        assert!(
            granted.decided_at > 0,
            "a decision records a wall-clock time"
        );
        assert!(store.load_pending_for_task("t").await.unwrap().is_empty());

        let applied = store.mark_applied("ap-1").await.unwrap();
        assert_eq!(applied.decision, "applied");
        assert_eq!(
            applied.decided_at, granted.decided_at,
            "applied keeps the human decision time"
        );
        assert!(store.load_pending_for_task("t").await.unwrap().is_empty());

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn approval_double_decide_and_unknown_id_are_typed_errors() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();

        // Unknown id: typed not-found, not a silent no-op.
        let missing = store
            .decide("nope", super::ApprovalOutcome::Granted)
            .await
            .unwrap_err();
        assert!(
            matches!(missing, super::StoreError::ApprovalNotFound { .. }),
            "got {missing:?}"
        );
        assert!(store.load_by_id("nope").await.unwrap().is_none());

        store.insert_pending("ap-1", "t", "h").await.unwrap();
        store
            .decide("ap-1", super::ApprovalOutcome::Granted)
            .await
            .unwrap();
        // Double decide: typed error, first decision preserved verbatim.
        let second = store
            .decide("ap-1", super::ApprovalOutcome::Denied)
            .await
            .unwrap_err();
        assert!(
            matches!(
                second,
                super::StoreError::ApprovalWrongState { ref decision, .. } if decision == "granted"
            ),
            "got {second:?}"
        );
        let row = store.load_by_id("ap-1").await.unwrap().unwrap();
        assert_eq!(row.decision, "granted");

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn approval_expire_only_leaves_pending_and_blocks_later_decisions() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task("t", "s", "w", "obj", "Created", "{}", "{}")
            .await
            .unwrap();

        store.insert_pending("ap-1", "t", "h").await.unwrap();
        // mark_applied must not work on a row that was never granted.
        let premature = store.mark_applied("ap-1").await.unwrap_err();
        assert!(
            matches!(
                premature,
                super::StoreError::ApprovalWrongState { ref decision, .. } if decision == "pending"
            ),
            "got {premature:?}"
        );

        let expired = store.expire("ap-1").await.unwrap();
        assert_eq!(expired.decision, "expired");
        assert!(expired.decided_at > 0, "expiry is recorded, pending was 0");
        // Expiring twice and deciding an expired row are typed errors.
        let again = store.expire("ap-1").await.unwrap_err();
        assert!(matches!(
            again,
            super::StoreError::ApprovalWrongState { .. }
        ));
        let late = store
            .decide("ap-1", super::ApprovalOutcome::Granted)
            .await
            .unwrap_err();
        assert!(matches!(late, super::StoreError::ApprovalWrongState { .. }));
        assert!(store.load_pending_for_task("t").await.unwrap().is_empty());

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}
