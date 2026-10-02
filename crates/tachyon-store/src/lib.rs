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

/// Inputs for recording one **Idempotency key** row (CONTEXT.md
/// glossary) in the same transaction as its task row + seq-0 journal
/// event. The key row commits iff the task commits; the gateway owns
/// fingerprint computation and replay policy, the store only persists.
#[derive(Clone, Copy, Debug)]
pub struct IdempotencyCreate<'a> {
    /// Client-supplied idempotency key (validated 1..=128 bytes
    /// gateway-side before it reaches the store).
    pub key: &'a str,
    /// Deterministic fingerprint of the canonical request identity
    /// (`session_id` + `objective`), hex-encoded BLAKE3.
    pub fingerprint: &'a str,
}

/// One stored **Idempotency key** record: the fingerprint the key was
/// first seen with plus the byte-identical success response to replay.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct IdempotencyRow {
    /// Fingerprint recorded when the key was first committed.
    pub fingerprint: String,
    /// Stored `Ok` payload JSON for the original successful create.
    pub response_json: String,
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

/// One row of `mcp_servers` (ACP MCP-stdio slice tickets 01-02): a
/// validated client-supplied **MCP server** descriptor pinned to one
/// session. `args_json` is the JSON args array and `env_json` the JSON
/// env array, each with `secret: true` entries stored as broker handles
/// only — an arg entry with a secret value persists as its credential
/// handle, never raw. `version` is the `initialize`-negotiated protocol version
/// and `tools_json` the recorded `tools/list` inventory — both set only
/// while the row is `live`, cleared on every other transition.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct McpServerRow {
    /// Owning session id.
    pub session_id: String,
    /// Opaque server identity (1..=64 bytes, validated by the gateway).
    pub server_id: String,
    /// Absolute server command path.
    pub command: String,
    /// JSON-encoded args array (entries with secret values persisted as
    /// credential handles).
    pub args_json: String,
    /// JSON-encoded env array (handles only for secrets).
    pub env_json: String,
    /// Lifecycle status (`awaiting_approval` at pin, `live` after a
    /// granted launch, `refused` / `stopped` otherwise).
    pub status: String,
    /// Negotiated protocol version (empty unless `live`).
    pub version: String,
    /// JSON-encoded tool inventory (empty array unless `live`).
    pub tools_json: String,
    /// Creation time (micros since epoch).
    pub created_at: i64,
    /// Last update time (micros since epoch).
    pub updated_at: i64,
}

/// One row of `mcp_approvals` (review fix round): the durable audit
/// trail of one parked session-scoped MCP approval. The live park
/// stays in memory (fail closed across restarts); this row proves the
/// park and its outcome happened.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct McpApprovalRow {
    /// Session-scoped approval id (hyphenated UUID).
    pub approval_id: String,
    /// Owning session id.
    pub session_id: String,
    /// Approval kind: `launch` (one register call's server set) or
    /// `call` (one parked `CallMCPTool`).
    pub kind: String,
    /// BLAKE3 hash (hex) of the exact authorized operation JSON.
    pub op_hash: String,
    /// Row machine state: `parked`, then one-shot `granted` /
    /// `denied` / `consumed-missing` / `cancelled` (a task cancel
    /// expired the park; the first decision wins, never rewritten).
    pub outcome: String,
    /// Park time (micros since epoch).
    pub created_at: i64,
    /// Decide time (micros since epoch); 0 while `parked`.
    pub decided_at: i64,
}

/// One validated MCP server to pin: the durable column values the
/// gateway computed (args JSON, env JSON — credential handles where a
/// secret is marked). The store only
/// persists what it is given; validation and secret registration live
/// in the gateway.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpServerPin<'a> {
    /// Opaque server identity.
    pub server_id: &'a str,
    /// Absolute server command path.
    pub command: &'a str,
    /// JSON-encoded args array (entries with secret values persisted as
    /// credential handles).
    pub args_json: &'a str,
    /// JSON-encoded env array (handles only for secrets).
    pub env_json: &'a str,
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
        self.create_task_inner(
            task_id,
            session_id,
            workspace_id,
            objective,
            status,
            snapshot_json,
            created_payload,
            None,
        )
        .await
        .map(|_| ())
    }

    /// Same atomic task + journal create as [`StoreWriter::create_task`],
    /// additionally recording one **Idempotency key** row in the SAME
    /// transaction. The stored response JSON mirrors the gateway's
    /// `CreateTask` success payload (`task_id`, `status`, `turn_seq`) so a
    /// retry can replay it byte-identically. A duplicate
    /// `UNIQUE (session_id, "key")` fails the whole transaction: the task
    /// row never lands without its key row, nor the key row without its
    /// task. Returns the minted `turn_seq`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_task_with_idempotency(
        &self,
        task_id: &str,
        session_id: &str,
        workspace_id: &str,
        objective: &str,
        status: &str,
        snapshot_json: &str,
        created_payload: &str,
        idem: IdempotencyCreate<'_>,
    ) -> Result<i64, StoreError> {
        self.create_task_inner(
            task_id,
            session_id,
            workspace_id,
            objective,
            status,
            snapshot_json,
            created_payload,
            Some(idem),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_task_inner(
        &self,
        task_id: &str,
        session_id: &str,
        workspace_id: &str,
        objective: &str,
        status: &str,
        snapshot_json: &str,
        created_payload: &str,
        idem: Option<IdempotencyCreate<'_>>,
    ) -> Result<i64, StoreError> {
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
        let turn_seq: i64 = sqlx::query_scalar("SELECT turn_seq FROM tasks WHERE id = ?")
            .bind(task_id)
            .fetch_one(&mut *tx)
            .await?;
        if let Some(idem) = idem {
            let response_json = serde_json::json!({
                "task_id": task_id,
                "status": status,
                "turn_seq": turn_seq,
            })
            .to_string();
            sqlx::query(
                "INSERT INTO create_task_idempotency
                 (session_id, \"key\", fingerprint, response_json, created_at)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(session_id)
            .bind(idem.key)
            .bind(idem.fingerprint)
            .bind(response_json)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.notify_commit(task_id, 0);
        Ok(turn_seq)
    }

    /// Looks up one **Idempotency key** record; `None` means this
    /// `(session_id, key)` has never been committed. Read-only.
    pub async fn lookup_idempotency(
        &self,
        session_id: &str,
        key: &str,
    ) -> Result<Option<IdempotencyRow>, StoreError> {
        Ok(sqlx::query_as::<_, IdempotencyRow>(
            "SELECT fingerprint, response_json
             FROM create_task_idempotency
             WHERE session_id = ? AND \"key\" = ?",
        )
        .bind(session_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Reads one task's durable per-session turn stamp (minted once at
    /// create, never rewritten). `None` for an unknown task.
    pub async fn task_turn_seq(&self, task_id: &str) -> Result<Option<i64>, StoreError> {
        Ok(
            sqlx::query_scalar("SELECT turn_seq FROM tasks WHERE id = ?")
                .bind(task_id)
                .fetch_optional(&self.pool)
                .await?,
        )
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

    /// Pins a validated set of MCP servers for one session in a single
    /// transaction (tickets 01-02): each named row is inserted as
    /// `awaiting_approval` — the launch parks until one
    /// `ApproveMCPServers` consumes the register call's approval — or,
    /// when `(session_id, server_id)` already exists, replaced in place
    /// (re-register upserts the named rows and re-parks them; unnamed
    /// rows are left untouched). All-or-nothing: a mid-set failure rolls
    /// back every row of the call, so a register never leaves a partial
    /// set.
    pub async fn pin_mcp_servers(
        &self,
        session_id: &str,
        servers: &[McpServerPin<'_>],
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let mut tx = self.pool.begin().await?;
        for server in servers {
            sqlx::query(
                "INSERT INTO mcp_servers
                 (session_id, server_id, command, args_json, env_json,
                  status, version, tools_json, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, 'awaiting_approval', '', '[]', ?, ?)
                 ON CONFLICT (session_id, server_id) DO UPDATE SET
                    command = excluded.command,
                    args_json = excluded.args_json,
                    env_json = excluded.env_json,
                    status = 'awaiting_approval',
                    version = '',
                    tools_json = '[]',
                    updated_at = excluded.updated_at",
            )
            .bind(session_id)
            .bind(server.server_id)
            .bind(server.command)
            .bind(server.args_json)
            .bind(server.env_json)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Lists one session's pinned MCP servers in `server_id` order.
    /// Strictly read-only.
    pub async fn list_mcp_servers(
        &self,
        session_id: &str,
    ) -> Result<Vec<McpServerRow>, StoreError> {
        sqlx::query_as::<_, McpServerRow>(
            "SELECT session_id, server_id, command, args_json, env_json,
                    status, version, tools_json, created_at, updated_at
             FROM mcp_servers WHERE session_id = ? ORDER BY server_id ASC",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Loads one pinned MCP server; `None` when the session pins no
    /// server under that id. Strictly read-only.
    pub async fn get_mcp_server(
        &self,
        session_id: &str,
        server_id: &str,
    ) -> Result<Option<McpServerRow>, StoreError> {
        sqlx::query_as::<_, McpServerRow>(
            "SELECT session_id, server_id, command, args_json, env_json,
                    status, version, tools_json, created_at, updated_at
             FROM mcp_servers WHERE session_id = ? AND server_id = ?",
        )
        .bind(session_id)
        .bind(server_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(StoreError::from)
    }

    /// Marks one pinned server `live` with its negotiated version and
    /// recorded tool inventory JSON. The caller performed the handshake
    /// and owns the child; the store only records the outcome.
    pub async fn mark_mcp_live(
        &self,
        session_id: &str,
        server_id: &str,
        version: &str,
        tools_json: &str,
    ) -> Result<(), StoreError> {
        let _guard = self.write.lock().await;
        sqlx::query(
            "UPDATE mcp_servers SET status = 'live', version = ?,
                    tools_json = ?, updated_at = ?
             WHERE session_id = ? AND server_id = ?",
        )
        .bind(version)
        .bind(tools_json)
        .bind(Timestamp::now().as_micros())
        .bind(session_id)
        .bind(server_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Moves a set of pinned servers to `refused` or `stopped` (the only
    /// non-live transitions this slice issues): the negotiated version
    /// and inventory are cleared so a later `ListMCPServers` never shows
    /// stale liveness. Servers outside the set are untouched.
    pub async fn mark_mcp_servers(
        &self,
        session_id: &str,
        server_ids: &[&str],
        status: &str,
    ) -> Result<(), StoreError> {
        debug_assert!(
            status == "refused" || status == "stopped",
            "mark_mcp_servers only issues non-live transitions"
        );
        let _guard = self.write.lock().await;
        let now = Timestamp::now().as_micros();
        let mut tx = self.pool.begin().await?;
        for server_id in server_ids {
            sqlx::query(
                "UPDATE mcp_servers SET status = ?, version = '',
                        tools_json = '[]', updated_at = ?
                 WHERE session_id = ? AND server_id = ?",
            )
            .bind(status)
            .bind(now)
            .bind(session_id)
            .bind(server_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Gateway-boot recovery (ticket 02): every row left `live` by a dead
    /// gateway falls back to `stopped` with its version and inventory
    /// cleared — grants never survive a restart, and nothing relaunches
    /// until an explicit approved reload. Parked (`awaiting_approval`) and
    /// refused rows are untouched. Returns the number of rows stopped.
    pub async fn reset_mcp_live_to_stopped(&self) -> Result<u64, StoreError> {
        let _guard = self.write.lock().await;
        let changed = sqlx::query(
            "UPDATE mcp_servers SET status = 'stopped', version = '',
                    tools_json = '[]', updated_at = ?
             WHERE status = 'live'",
        )
        .bind(Timestamp::now().as_micros())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed)
    }

    /// Parks one session-scoped MCP approval record (`outcome='parked'`,
    /// `decided_at=0`): the approval id, owning session, kind
    /// (`launch` | `call`), and BLAKE3 hash of the exact authorized
    /// operation JSON the grant binds to. One row per parked approval;
    /// re-parking an id replaces it (a re-register supersedes the stale
    /// park). Additive audit surface — the live park stays in memory,
    /// so crash semantics are unchanged (fail closed).
    pub async fn record_mcp_approval(
        &self,
        approval_id: &str,
        session_id: &str,
        kind: &str,
        op_hash: &str,
    ) -> Result<(), StoreError> {
        debug_assert!(
            kind == "launch" || kind == "call",
            "record_mcp_approval only records launch|call kinds"
        );
        let _guard = self.write.lock().await;
        sqlx::query(
            "INSERT INTO mcp_approvals
             (approval_id, session_id, kind, op_hash, outcome, created_at, decided_at)
             VALUES (?, ?, ?, ?, 'parked', ?, 0)
             ON CONFLICT (approval_id) DO UPDATE SET
                session_id = excluded.session_id,
                kind = excluded.kind,
                op_hash = excluded.op_hash,
                outcome = 'parked',
                created_at = excluded.created_at,
                decided_at = 0",
        )
        .bind(approval_id)
        .bind(session_id)
        .bind(kind)
        .bind(op_hash)
        .bind(Timestamp::now().as_micros())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Decides one parked MCP approval record one-shot
    /// (`parked` -> `granted` | `denied` | `consumed-missing` |
    /// `cancelled`): the outcome is written once with the decide
    /// timestamp and never rewritten afterwards. `cancelled` is the
    /// cancellation-drain expiry (issue #57 blocker 4): a task cancel
    /// reached the gateway-side park before any grant/refusal, so the
    /// late decision fails closed. Best-effort audit — deciding an
    /// unknown id is a no-op, never an error.
    pub async fn decide_mcp_approval(
        &self,
        approval_id: &str,
        outcome: &str,
    ) -> Result<(), StoreError> {
        debug_assert!(
            outcome == "granted"
                || outcome == "denied"
                || outcome == "consumed-missing"
                || outcome == "cancelled",
            "decide_mcp_approval only records terminal outcomes"
        );
        let _guard = self.write.lock().await;
        sqlx::query(
            "UPDATE mcp_approvals SET outcome = ?, decided_at = ?
             WHERE approval_id = ? AND outcome = 'parked'",
        )
        .bind(outcome)
        .bind(Timestamp::now().as_micros())
        .bind(approval_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Loads one MCP approval record; `None` when no row carries the id.
    /// Strictly read-only (tests + audit).
    pub async fn get_mcp_approval(
        &self,
        approval_id: &str,
    ) -> Result<Option<McpApprovalRow>, StoreError> {
        sqlx::query_as::<_, McpApprovalRow>(
            "SELECT approval_id, session_id, kind, op_hash, outcome,
                    created_at, decided_at
             FROM mcp_approvals WHERE approval_id = ?",
        )
        .bind(approval_id)
        .fetch_optional(&self.pool)
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
    use super::{EffectMutation, IdempotencyCreate, SessionTurn, StoreWriter, TransitionState};
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

    // ---- ACP slice b (issue #57 ticket 01): CreateTask idempotency ----

    /// Same-transaction record: the key row (fingerprint + stored Ok
    /// response) lands iff the task + seq-0 journal event land, and the
    /// stored response mirrors the gateway success payload exactly.
    #[tokio::test]
    async fn create_task_with_idempotency_records_response_atomically() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        let turn = store
            .create_task_with_idempotency(
                "t1",
                "s",
                "w",
                "obj",
                "Created",
                "{}",
                "{}",
                IdempotencyCreate {
                    key: "k1",
                    fingerprint: "fp1",
                },
            )
            .await
            .unwrap();
        assert_eq!(turn, 1, "the minted turn stamp is returned");

        let row = store
            .lookup_idempotency("s", "k1")
            .await
            .unwrap()
            .expect("key row committed with the task");
        assert_eq!(row.fingerprint, "fp1");
        let payload: serde_json::Value = serde_json::from_str(&row.response_json).unwrap();
        assert_eq!(
            payload,
            serde_json::json!({"task_id": "t1", "status": "Created", "turn_seq": 1}),
            "stored response mirrors the gateway CreateTask payload"
        );
        assert_eq!(store.task_turn_seq("t1").await.unwrap(), Some(1));
        assert!(
            store
                .lookup_idempotency("s", "never-seen")
                .await
                .unwrap()
                .is_none()
        );

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Unique race at the store seam: a duplicate `(session_id, key)`
    /// fails the WHOLE create — the racer's task row rolls back — and
    /// the winner's record stays byte-stable for replay.
    #[tokio::test]
    async fn duplicate_idempotency_key_rolls_back_the_entire_create() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task_with_idempotency(
                "t1",
                "s",
                "w",
                "obj",
                "Created",
                "{}",
                "{}",
                IdempotencyCreate {
                    key: "k",
                    fingerprint: "fp1",
                },
            )
            .await
            .unwrap();
        let clash = store
            .create_task_with_idempotency(
                "t2",
                "s",
                "w",
                "obj2",
                "Created",
                "{}",
                "{}",
                IdempotencyCreate {
                    key: "k",
                    fingerprint: "fp1",
                },
            )
            .await;
        assert!(clash.is_err(), "UNIQUE (session_id, key) rejects the racer");
        assert_eq!(
            store.task_turn_seq("t2").await.unwrap(),
            None,
            "no half-created task row survives"
        );
        assert_eq!(store.load_session_turns("s").await.unwrap().len(), 1);
        let row = store.lookup_idempotency("s", "k").await.unwrap().unwrap();
        assert_eq!(row.fingerprint, "fp1");
        let payload: serde_json::Value = serde_json::from_str(&row.response_json).unwrap();
        assert_eq!(
            payload["task_id"], "t1",
            "the winner's stored response is untouched"
        );

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Same key, different fingerprint: the store still refuses the whole
    /// create (it cannot tell fingerprints apart — the gateway maps this
    /// to `idempotency_key_conflict`) and keeps the ORIGINAL record.
    #[tokio::test]
    async fn fingerprint_mismatch_under_same_key_leaves_original_intact() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task_with_idempotency(
                "t1",
                "s",
                "w",
                "first",
                "Created",
                "{}",
                "{}",
                IdempotencyCreate {
                    key: "k",
                    fingerprint: "fp1",
                },
            )
            .await
            .unwrap();
        let mismatch = store
            .create_task_with_idempotency(
                "t2",
                "s",
                "w",
                "second",
                "Created",
                "{}",
                "{}",
                IdempotencyCreate {
                    key: "k",
                    fingerprint: "fp2",
                },
            )
            .await;
        assert!(mismatch.is_err(), "the mismatched create never commits");
        assert_eq!(store.task_turn_seq("t2").await.unwrap(), None);
        let row = store.lookup_idempotency("s", "k").await.unwrap().unwrap();
        assert_eq!(
            row.fingerprint, "fp1",
            "the first-committed fingerprint is what a replay compares against"
        );

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Crash-recovery atomicity at the store seam: after close + reopen
    /// the record replays with byte-identical response JSON.
    #[tokio::test]
    async fn idempotency_record_survives_store_reopen_for_replay() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .create_task_with_idempotency(
                "t1",
                "s",
                "w",
                "obj",
                "Created",
                "{}",
                "{}",
                IdempotencyCreate {
                    key: "k",
                    fingerprint: "fp1",
                },
            )
            .await
            .unwrap();
        let before = store.lookup_idempotency("s", "k").await.unwrap().unwrap();
        store.close().await;

        let reopened = StoreWriter::open(&dir).await.unwrap();
        let after = reopened
            .lookup_idempotency("s", "k")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before, after, "replay bytes survive the restart");
        assert_eq!(reopened.task_turn_seq("t1").await.unwrap(), Some(1));
        reopened.close().await;
        std::fs::remove_dir_all(dir).unwrap();
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

    /// Pins are upserts under `UNIQUE (session_id, server_id)`: racing
    /// pins of one server leave exactly one row (last writer wins, no
    /// duplicate), and pins for distinct servers coexist.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_pin_of_one_server_keeps_a_single_row() {
        use super::McpServerPin;
        use std::sync::Arc;
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        let store = Arc::new(store);
        let args_json = "[\"--stdio\"]";
        let env_json = "[]";
        let mut handles = Vec::new();
        for i in 0..8 {
            let store = Arc::clone(&store);
            let command = format!("/usr/bin/fake-mcp-server-{i}");
            handles.push(tokio::spawn(async move {
                store
                    .pin_mcp_servers(
                        "s",
                        &[McpServerPin {
                            server_id: "solo",
                            command: &command,
                            args_json,
                            env_json,
                        }],
                    )
                    .await
            }));
        }
        for handle in handles {
            handle
                .await
                .unwrap()
                .expect("every racing pin must succeed");
        }
        let rows = store.list_mcp_servers("s").await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "UNIQUE (session_id, server_id) holds under race: {rows:?}"
        );
        assert_eq!(rows[0].server_id, "solo");
        assert_eq!(rows[0].status, "awaiting_approval");

        store
            .pin_mcp_servers(
                "s",
                &[
                    McpServerPin {
                        server_id: "solo",
                        command: "/usr/bin/fake-mcp-server-final",
                        args_json,
                        env_json,
                    },
                    McpServerPin {
                        server_id: "peer",
                        command: "/usr/bin/other",
                        args_json,
                        env_json,
                    },
                ],
            )
            .await
            .unwrap();
        let rows = store.list_mcp_servers("s").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].server_id, "peer");
        assert_eq!(rows[1].server_id, "solo");
        assert_eq!(rows[1].command, "/usr/bin/fake-mcp-server-final");

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Handles — not raw secrets — persist: rows store the gateway's
    /// env-with-handles JSON opaquely, and a reopen over the same
    /// directory returns the identical handles with the raw secret
    /// present nowhere in any row.
    #[tokio::test]
    async fn pinned_handles_survive_reopen_without_raw_secrets() {
        use super::McpServerPin;
        let raw_secret = "live-raw-secret-must-never-persist";
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .pin_mcp_servers(
                "s",
                &[McpServerPin {
                    server_id: "beta",
                    command: "/usr/bin/fake-mcp-server",
                    args_json: "[\"--stdio\"]",
                    env_json: "[{\"name\":\"API_TOKEN\",\"value\":\"mcp-secret-1\",\"secret\":true}]",
                }],
            )
            .await
            .unwrap();
        store.close().await;

        let reopened = super::StoreWriter::open(&dir).await.unwrap();
        let rows = reopened.list_mcp_servers("s").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "awaiting_approval");
        assert!(
            rows[0].env_json.contains("mcp-secret-1"),
            "handle persists: {}",
            rows[0].env_json
        );
        assert!(
            !rows[0].env_json.contains(raw_secret),
            "raw secret is nowhere in the row"
        );
        let dump: String = sqlx::query_scalar(
            "SELECT group_concat(command || args_json || env_json || status, '|')
             FROM mcp_servers WHERE session_id = 's'",
        )
        .fetch_one(&reopened.pool)
        .await
        .unwrap();
        assert!(
            !dump.contains(raw_secret),
            "raw secret is nowhere on disk rows"
        );

        reopened.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    // ---- ACP slice (issue #57 ticket 02): MCP status transitions ----

    /// The launch lifecycle is a status walk with the liveness columns
    /// set only while `live`: pin parks (`awaiting_approval`, blank
    /// version, empty inventory), going live records the negotiated
    /// version and inventory, and every non-live transition clears them
    /// so a later list never shows stale liveness.
    #[tokio::test]
    async fn mcp_status_walk_records_liveness_only_while_live() {
        use super::McpServerPin;
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .pin_mcp_servers(
                "s",
                &[McpServerPin {
                    server_id: "w",
                    command: "/usr/bin/fake-mcp-server",
                    args_json: "[\"--stdio\"]",
                    env_json: "[]",
                }],
            )
            .await
            .unwrap();
        let parked = store.get_mcp_server("s", "w").await.unwrap().unwrap();
        assert_eq!(parked.status, "awaiting_approval");
        assert_eq!(parked.version, "");
        assert_eq!(parked.tools_json, "[]");
        assert!(store.get_mcp_server("s", "ghost").await.unwrap().is_none());

        store
            .mark_mcp_live("s", "w", "2024-11-05", "[{\"name\":\"echo\"}]")
            .await
            .unwrap();
        let live = store.get_mcp_server("s", "w").await.unwrap().unwrap();
        assert_eq!(live.status, "live");
        assert_eq!(live.version, "2024-11-05");
        assert_eq!(live.tools_json, "[{\"name\":\"echo\"}]");

        store
            .mark_mcp_servers("s", &["w"], "stopped")
            .await
            .unwrap();
        let stopped = store.get_mcp_server("s", "w").await.unwrap().unwrap();
        assert_eq!(stopped.status, "stopped");
        assert_eq!(stopped.version, "", "stopping clears the version");
        assert_eq!(stopped.tools_json, "[]", "stopping clears the inventory");

        store
            .mark_mcp_servers("s", &["w"], "refused")
            .await
            .unwrap();
        let refused = store.get_mcp_server("s", "w").await.unwrap().unwrap();
        assert_eq!(refused.status, "refused");

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Boot recovery stops exactly the rows a dead gateway left `live`
    /// (with version and inventory cleared) and touches nothing else:
    /// parked and refused rows keep their status, so only an explicit
    /// approved reload ever relaunches.
    #[tokio::test]
    async fn mcp_boot_reset_stops_only_live_rows() {
        use super::McpServerPin;
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .pin_mcp_servers(
                "s",
                &[
                    McpServerPin {
                        server_id: "parked",
                        command: "/bin/a",
                        args_json: "[]",
                        env_json: "[]",
                    },
                    McpServerPin {
                        server_id: "running",
                        command: "/bin/b",
                        args_json: "[]",
                        env_json: "[]",
                    },
                    McpServerPin {
                        server_id: "denied",
                        command: "/bin/c",
                        args_json: "[]",
                        env_json: "[]",
                    },
                ],
            )
            .await
            .unwrap();
        store
            .mark_mcp_live("s", "running", "2024-11-05", "[{\"name\":\"t\"}]")
            .await
            .unwrap();
        store
            .mark_mcp_servers("s", &["denied"], "refused")
            .await
            .unwrap();

        let stopped = store.reset_mcp_live_to_stopped().await.unwrap();
        assert_eq!(stopped, 1, "exactly the live row falls back");
        let rows = store.list_mcp_servers("s").await.unwrap();
        // Ordered by server_id: denied, parked, running.
        assert_eq!(rows[0].status, "refused");
        assert_eq!(rows[1].status, "awaiting_approval");
        assert_eq!(rows[2].status, "stopped");
        assert_eq!(rows[2].version, "");
        assert_eq!(rows[2].tools_json, "[]");
        let again = store.reset_mcp_live_to_stopped().await.unwrap();
        assert_eq!(again, 0, "the reset is a fixpoint");

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn mcp_approval_park_decide_round_trips_with_hash_and_timestamps() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .record_mcp_approval("aid-1", "s", "launch", "op-hash-abc")
            .await
            .unwrap();
        let row = store
            .get_mcp_approval("aid-1")
            .await
            .unwrap()
            .expect("parked row");
        assert_eq!(row.session_id, "s");
        assert_eq!(row.kind, "launch");
        assert_eq!(row.op_hash, "op-hash-abc");
        assert_eq!(row.outcome, "parked");
        assert!(row.created_at > 0);
        assert_eq!(row.decided_at, 0, "undecided while parked");

        store.decide_mcp_approval("aid-1", "granted").await.unwrap();
        let row = store
            .get_mcp_approval("aid-1")
            .await
            .unwrap()
            .expect("decided row");
        assert_eq!(row.outcome, "granted");
        assert!(row.decided_at > 0, "decide stamps the decision time");

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn mcp_approval_decide_is_one_shot_and_unknown_ids_are_noops() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .record_mcp_approval("aid-2", "s", "call", "op-hash-def")
            .await
            .unwrap();
        store.decide_mcp_approval("aid-2", "denied").await.unwrap();
        // A second decision never rewrites the first: the grant/refusal
        // executes exactly once, and so does its audit.
        store.decide_mcp_approval("aid-2", "granted").await.unwrap();
        let row = store
            .get_mcp_approval("aid-2")
            .await
            .unwrap()
            .expect("decided row");
        assert_eq!(row.outcome, "denied", "first decision sticks");
        // Deciding an id that was never parked is a silent no-op, never
        // an error — late duplicates after a supersede must fail quiet.
        store
            .decide_mcp_approval("aid-never-parked", "granted")
            .await
            .unwrap();
        assert!(
            store
                .get_mcp_approval("aid-never-parked")
                .await
                .unwrap()
                .is_none()
        );
        // Re-parking an id (a re-register superseding the stale park)
        // resets it to undecided with the fresh hash.
        store
            .record_mcp_approval("aid-2", "s", "launch", "op-hash-fresh")
            .await
            .unwrap();
        let row = store
            .get_mcp_approval("aid-2")
            .await
            .unwrap()
            .expect("re-parked row");
        assert_eq!(row.outcome, "parked");
        assert_eq!(row.op_hash, "op-hash-fresh");
        assert_eq!(row.decided_at, 0);

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn mcp_approval_cancelled_is_one_shot_against_grant_and_deny() {
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        // Cancel-then-grant: the cancel wins, the late grant is a no-op.
        store
            .record_mcp_approval("aid-cancel-first", "s", "launch", "op-hash-a")
            .await
            .unwrap();
        store
            .decide_mcp_approval("aid-cancel-first", "cancelled")
            .await
            .unwrap();
        store
            .decide_mcp_approval("aid-cancel-first", "granted")
            .await
            .unwrap();
        let row = store
            .get_mcp_approval("aid-cancel-first")
            .await
            .unwrap()
            .expect("decided row");
        assert_eq!(row.outcome, "cancelled", "first decision sticks");
        assert!(row.decided_at > 0, "decide stamps the decision time");
        // Deny-then-cancel: the denial stands, the cancel is a no-op.
        store
            .record_mcp_approval("aid-deny-first", "s", "call", "op-hash-b")
            .await
            .unwrap();
        store
            .decide_mcp_approval("aid-deny-first", "denied")
            .await
            .unwrap();
        store
            .decide_mcp_approval("aid-deny-first", "cancelled")
            .await
            .unwrap();
        let row = store
            .get_mcp_approval("aid-deny-first")
            .await
            .unwrap()
            .expect("decided row");
        assert_eq!(row.outcome, "denied", "first decision sticks");

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn mcp_park_rows_survive_restart_as_audit_only() {
        // Ticket 03: the durable `mcp_approvals` rows persist across a
        // restart as audit, never as grants — usability is gated by the
        // gateway's in-memory parks (proven at the gateway seam), while the
        // rows keep their outcome and one-shot discipline here.
        let (store, dir) = open_test_store().await;
        store.create_session("s").await.unwrap();
        store
            .record_mcp_approval("aid-restart-launch", "s", "launch", "op-hash-l")
            .await
            .unwrap();
        store
            .record_mcp_approval("aid-restart-call", "s", "call", "op-hash-c")
            .await
            .unwrap();
        store
            .decide_mcp_approval("aid-restart-call", "cancelled")
            .await
            .unwrap();
        store.close().await;

        // Reopen on the same dir: the crash-restart shape.
        let store = StoreWriter::open(&dir).await.unwrap();
        let launch = store
            .get_mcp_approval("aid-restart-launch")
            .await
            .unwrap()
            .expect("parked launch row survives restart");
        assert_eq!(launch.outcome, "parked");
        assert_eq!(launch.decided_at, 0, "still undecided audit");
        let call = store
            .get_mcp_approval("aid-restart-call")
            .await
            .unwrap()
            .expect("decided call row survives restart");
        assert_eq!(call.outcome, "cancelled", "first decision sticks");
        assert!(call.decided_at > 0);
        // One-shot discipline holds after reopen: a late grant neither
        // rewrites the cancel nor errors.
        store
            .decide_mcp_approval("aid-restart-call", "granted")
            .await
            .unwrap();
        assert_eq!(
            store
                .get_mcp_approval("aid-restart-call")
                .await
                .unwrap()
                .unwrap()
                .outcome,
            "cancelled"
        );

        store.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}
