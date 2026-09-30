//! ADR-0006 — Supervisor-owned evidence execution.
//!
//! The production `fs.read` evidence stage lives here. Typed requests are
//! lowered by a trusted compiler into validated Execution IR, every target
//! is opened and proven beneath the pinned root *before* the graph is
//! allocated, authorization runs against that exact opened object through a
//! one-shot permit, reads share one linearizable stage byte budget, and no
//! success is journalled until the bytes are durably in the
//! content-addressed spool and a receipt is ready to be committed with it.
//!
//! Split of ownership: [`crate::execution_graph_token`] owns the proof
//! constructor, the `Loop` owns generation allocation, journalling and
//! recovery, and this module owns preparation, compilation, execution and
//! receipt assembly.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tachyon_ir::{
    CancellationPolicy, EffectClass, ExecutionGraph, ExecutionNode, ExecutorKind, Idempotency,
    NodeStatus,
};
use tachyon_scheduler::{
    Budgets, Executor, ExecutorRegistry, NodeOutcome, ResolvedInputs, TaskRunSnapshot,
};
use tachyon_tools::{ToolError, ToolsContext, authorize, resolve_scope};
use tachyon_types::{ArtifactId, NodeId, TaskId};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::execution_graph_token::ValidatedExecutionGraph;
use crate::runtime::{
    EvidenceItem, EvidenceRequest, NodeTiming, RuntimeBounds, RuntimeError, compile_operation,
    hash_bytes, normalize_key,
};
use crate::{CoreError, StateEvent};

/// Capability contract version of the `fs.read` evidence capability,
/// persisted on every invocation so recovery never reinterprets an
/// invocation under newer semantics (ADR-0006 §10).
pub const FS_READ_CONTRACT_VERSION: u16 = 1;

/// The single structured output an `fs.read` node promises (ADR-0006 §3).
pub const EVIDENCE_OUTPUT_NAME: &str = "evidence";

/// Largest single reservation: chunks keep the linearizable budget small
/// enough that a concurrent sibling can always be told to stop.
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Wall-clock bound for one evidence generation. Node timeouts are lower;
/// this only guarantees a stuck scheduler cannot park the supervisor.
const EVIDENCE_STAGE_TIMEOUT: Duration = Duration::from_secs(120);

/// One node claims 100 CPU units, so these budgets are how many evidence
/// nodes may be in flight at once.
const CONCURRENT_CPU_UNITS: u32 = 4_000;
const SERIAL_CPU_UNITS: u32 = 100;

/// One durable evidence output receipt (ADR-0006 §11). Journalled in the
/// same transaction that marks its node `Succeeded`; never carries source
/// bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceReceipt {
    /// Task-wide generation that produced it.
    pub generation: u64,
    /// Node that produced it.
    pub node_id: NodeId,
    /// Content address of the stored bytes.
    pub artifact: ArtifactId,
    /// Byte length of the stored bytes.
    pub bytes: u64,
    /// Canonical workspace-relative key of the read target.
    pub path: String,
    /// Evidence-freshness token (FNV-1a), distinct from [`Self::artifact`].
    pub freshness_hash: String,
    /// Capability contract version that produced it.
    pub capability_version: u16,
}

/// What the driver receives once a generation settles: verified bytes plus
/// the measurements the benchmark host reports.
#[derive(Debug)]
pub struct EvidenceBatch {
    /// Bounded evidence, sorted by canonical key.
    pub items: Vec<EvidenceItem>,
    /// Durable receipts committed for this generation, in graph order.
    pub receipts: Vec<EvidenceReceipt>,
    /// Per-node measurements from run origin.
    pub timings: Vec<NodeTiming>,
    /// Per-node `(start_us, end_us)` intervals from run origin.
    pub intervals_us: Vec<(u64, u64)>,
    /// Nodes in the accepted graph (report parity).
    pub graph_nodes: usize,
}

/// How a settled generation reached the caller: a lifecycle refusal from
/// the supervisor (`Core`) or the stage's own typed failure (`Stage`), the
/// latter being what carries a parkable `ApprovalRequired`.
#[derive(Debug)]
pub enum EvidenceStageError {
    /// Supervisor refused the command or ended before answering.
    Core(crate::CoreError),
    /// The stage itself failed, was cancelled, or exhausted its budget.
    Stage(RuntimeError),
}

impl From<crate::CoreError> for EvidenceStageError {
    fn from(error: crate::CoreError) -> Self {
        Self::Core(error)
    }
}

impl From<RuntimeError> for EvidenceStageError {
    fn from(error: RuntimeError) -> Self {
        Self::Stage(error)
    }
}

impl EvidenceStageError {
    /// The parkable approval request this failure carries, if any.
    #[must_use]
    pub fn approval_request(&self) -> Option<Box<tachyon_policy::ApprovalRequest>> {
        match self {
            Self::Stage(RuntimeError::Tool(ToolError::ApprovalRequired { request, .. })) => {
                Some(request.clone())
            }
            _ => None,
        }
    }

    /// Maps into the driver's error type without losing the variant.
    #[must_use]
    pub fn into_drive_error(self) -> crate::driver::DriveError {
        match self {
            Self::Core(error) => crate::driver::DriveError::Core(error),
            Self::Stage(error) => crate::driver::DriveError::Runtime(error),
        }
    }
}

/// A shared, linearizable byte budget for one stage (ADR-0006 §6). The
/// counter is `committed bytes of this stage + in-flight reservations`, so
/// it bounds the whole batch rather than one file. Every bounded read
/// reserves its maximum size atomically before it reads and returns only
/// what it did not use; exhaustion cancels the stage so siblings stop and
/// drain instead of racing the cap.
pub struct StageByteBudget {
    limit: u64,
    used: AtomicU64,
    stage_cancel: CancellationToken,
}

impl StageByteBudget {
    /// Creates a budget of `limit` bytes that cancels `stage_cancel` once
    /// it is exhausted.
    #[must_use]
    pub fn new(limit: u64, stage_cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
            stage_cancel,
        })
    }

    /// The stage limit this budget enforces.
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Reserves `want` bytes atomically on top of everything already
    /// committed and in flight. Concurrent reservations can never sum
    /// past the limit; exhaustion fires the stage cancel token and fails
    /// the caller, which stops dispatch and discards the batch.
    pub fn reserve(&self, want: u64) -> Result<Reservation<'_>, RuntimeError> {
        let reserved = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(want).filter(|total| *total <= self.limit)
            });
        if reserved.is_err() {
            self.stage_cancel.cancel();
            return Err(RuntimeError::Evidence(format!(
                "stage byte budget exhausted at {} bytes",
                self.limit
            )));
        }
        Ok(Reservation {
            budget: self,
            pending: want,
        })
    }
}

/// A held reservation. [`Self::commit`] returns the unused part right
/// after a short read or EOF and leaves the consumed bytes counted as
/// stage usage; dropping an uncommitted reservation returns all of it.
pub struct Reservation<'a> {
    budget: &'a StageByteBudget,
    pending: u64,
}

impl Reservation<'_> {
    /// Keeps `actual` bytes counted as stage usage and returns the rest
    /// of the reservation immediately.
    pub fn commit(&mut self, actual: u64) {
        let actual = actual.min(self.pending);
        let unused = self.pending - actual;
        if unused > 0 {
            self.budget.used.fetch_sub(unused, Ordering::AcqRel);
        }
        self.pending = 0;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.pending > 0 {
            self.budget.used.fetch_sub(self.pending, Ordering::AcqRel);
        }
    }
}

/// Identity of an opened object, used to prove the handle still names the
/// object the canonical path names. Unix compares device+inode; other
/// platforms fall back to size+mtime and fail closed when the platform
/// cannot produce one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    primary: u64,
    secondary: u64,
}

// Non-Unix falls back to size+mtime and CAN fail; Unix cannot, so the
// `Option` is platform-conditional rather than redundant.
#[allow(clippy::unnecessary_wraps)]
fn identity_of(meta: &std::fs::Metadata) -> Option<FileIdentity> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(FileIdentity {
            primary: meta.dev(),
            secondary: meta.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        let modified = meta.modified().ok()?;
        let since = modified.duration_since(UNIX_EPOCH).ok()?;
        Some(FileIdentity {
            primary: meta.len(),
            secondary: u64::try_from(since.as_nanos()).ok()?,
        })
    }
}

fn failed_too(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Evidence(message.into())
}

/// The opened, proven evidence target (ADR-0006 §4). Deliberately not
/// serializable and not cloneable: the pinned root, file handle, canonical
/// key and opened-object identity travel together or not at all.
pub struct PreparedTarget {
    root_identity: FileIdentity,
    key: String,
    scope: String,
    file: std::fs::File,
    identity: FileIdentity,
}

impl PreparedTarget {
    /// Canonical workspace-relative key every other decision derives from.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Policy scope derived from that canonical key.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }
}

/// One request paired with the target opened for it.
pub struct PreparedEvidence {
    /// Target opened and proven beneath the pinned root.
    pub target: PreparedTarget,
}

/// One-shot, non-cloneable authorization permit (ADR-0006 §5). It binds
/// task, run, revision, generation, node, capability version, pinned root
/// identity, opened-file identity, canonical key and effect, and can only
/// be spent against the exact target that issued it.
pub struct EvidencePermit {
    task_id: TaskId,
    run_id: String,
    revision: u64,
    generation: u64,
    node_id: NodeId,
    contract_version: u16,
    root_identity: FileIdentity,
    file_identity: FileIdentity,
    key: String,
    effect: &'static str,
}

/// Proof that a permit was spent against its own target: the only way
/// into the chunked reader.
pub struct ConsumedPermit {
    key: String,
}

impl EvidencePermit {
    fn issue(
        task_id: TaskId,
        run_id: &str,
        revision: u64,
        generation: u64,
        node_id: NodeId,
        contract_version: u16,
        target: &PreparedTarget,
    ) -> Self {
        Self {
            task_id,
            run_id: run_id.to_owned(),
            revision,
            generation,
            node_id,
            contract_version,
            root_identity: target.root_identity,
            file_identity: target.identity,
            key: target.key.clone(),
            effect: "read-only",
        }
    }

    /// Consumes the permit: it can never be spent twice, and only against
    /// the target whose identities it recorded.
    fn consume(self, target: &PreparedTarget) -> Result<ConsumedPermit, RuntimeError> {
        debug_assert_eq!(self.effect, "read-only");
        if self.file_identity != target.identity
            || self.root_identity != target.root_identity
            || self.key != target.key
        {
            return Err(failed_too(format!(
                "permit task {} run {} rev {} gen {} node {} (contract v{}) does not name the opened object",
                self.task_id,
                self.run_id,
                self.revision,
                self.generation,
                self.node_id,
                self.contract_version
            )));
        }
        Ok(ConsumedPermit { key: self.key })
    }
}

impl ConsumedPermit {
    fn key(&self) -> &str {
        &self.key
    }
}

/// Fence carried into each generation's permit.
#[derive(Clone, Copy)]
struct ReadMeta {
    task_id: TaskId,
    revision: u64,
    generation: u64,
    contract_version: u16,
}

/// Structured output of one succeeded `fs.read` node.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct EvidenceOutput {
    artifact: String,
    bytes: u64,
    path: String,
    freshness: String,
    start_us: u64,
    end_us: u64,
}

/// Why a read did not publish an output.
enum ReadError {
    /// Cancellation observed before a result could be published.
    Cancelled,
    /// The read failed closed; carries the typed stage error.
    Failed(RuntimeError),
}

impl From<RuntimeError> for ReadError {
    fn from(error: RuntimeError) -> Self {
        Self::Failed(error)
    }
}

/// Trusted `fs.read` executor: it never resolves a path itself, it only
/// consumes the target the Supervisor prepared for its node.
pub struct EvidenceReadExecutor {
    context: Arc<ToolsContext>,
    targets: Mutex<HashMap<NodeId, PreparedTarget>>,
    budget: Arc<StageByteBudget>,
    stage_error: Arc<Mutex<Option<RuntimeError>>>,
    run_id: String,
    origin: Instant,
    meta: ReadMeta,
}

impl EvidenceReadExecutor {
    /// Records the first typed stage failure so the job can recover it
    /// after the scheduler has collapsed every outcome to a string.
    fn record(&self, error: RuntimeError) -> String {
        let message = error.to_string();
        let mut slot = self
            .stage_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
        message
    }

    fn failed(&self, error: RuntimeError, elapsed: Duration) -> NodeOutcome {
        let message = self.record(error);
        NodeOutcome::failed(message, elapsed)
    }
}

#[async_trait]
impl Executor for EvidenceReadExecutor {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Tool
    }

    async fn execute(
        &self,
        node: &ExecutionNode,
        _inputs: ResolvedInputs,
        cancel: CancellationToken,
    ) -> NodeOutcome {
        let started = Instant::now();
        let start_us = u64::try_from(self.origin.elapsed().as_micros()).unwrap_or(u64::MAX);
        let target = self
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&node.id);
        let Some(target) = target else {
            return self.failed(
                failed_too(format!(
                    "no prepared target for node {}; a target is consumed by exactly one dispatch",
                    node.id
                )),
                started.elapsed(),
            );
        };
        let context = Arc::clone(&self.context);
        let budget = Arc::clone(&self.budget);
        let run_id = self.run_id.clone();
        let meta = self.meta;
        let origin = self.origin;
        let node = node.clone();
        let blocking = tokio::task::spawn_blocking(move || {
            read_evidence(
                &context, target, &node, &budget, start_us, origin, &run_id, meta, &cancel,
            )
        });
        match blocking.await {
            Ok(Ok(outputs)) => match serde_json::to_value(&outputs) {
                Ok(value) => {
                    let mut map = serde_json::Map::new();
                    map.insert(EVIDENCE_OUTPUT_NAME.to_owned(), value);
                    NodeOutcome::success(map, started.elapsed())
                }
                Err(error) => self.failed(RuntimeError::Json(error), started.elapsed()),
            },
            Ok(Err(ReadError::Cancelled)) => NodeOutcome::cancelled(started.elapsed()),
            Ok(Err(ReadError::Failed(error))) => {
                let message = self.record(error);
                NodeOutcome::failed(message, started.elapsed())
            }
            Err(join) => NodeOutcome::unknown(
                format!("evidence worker ended unsettled: {join}"),
                started.elapsed(),
            ),
        }
    }
}

/// Bounded chunked read through the held handle: each chunk reserves its
/// maximum size atomically before the read and returns only what it did
/// not use, and cancellation is checked before every chunk so no next
/// chunk starts after cancellation is observed (ADR-0006 §6–§7).
fn read_chunks(
    file: &mut std::fs::File,
    declared: u64,
    budget: &StageByteBudget,
    cancel: &CancellationToken,
) -> Result<Vec<u8>, ReadError> {
    let mut bytes: Vec<u8> = Vec::new();
    let mut chunk = vec![0_u8; READ_CHUNK_BYTES];
    let mut read_total: u64 = 0;
    loop {
        if cancel.is_cancelled() {
            return Err(ReadError::Cancelled);
        }
        // Reserve only what this file can still contribute, so a batch of
        // small files is bounded by their real sizes rather than by the
        // chunk size. `.max(1)` keeps an EOF probe reserving at least a
        // byte; a file that grew past its metadata falls back to a full
        // chunk and the shared budget decides.
        let remaining = declared.saturating_sub(read_total).max(1);
        let want = u64::try_from(READ_CHUNK_BYTES)
            .unwrap_or(u64::MAX)
            .min(remaining);
        let mut reservation = budget.reserve(want)?;
        let cap = usize::try_from(want)
            .unwrap_or(READ_CHUNK_BYTES)
            .min(chunk.len());
        let read = file
            .read(&mut chunk[..cap])
            .map_err(|err| RuntimeError::Io(err.to_string()))?;
        let read = u64::try_from(read).unwrap_or(u64::MAX);
        reservation.commit(read);
        if read == 0 {
            break;
        }
        read_total = read_total.saturating_add(read);
        bytes.extend_from_slice(&chunk[..usize::try_from(read).unwrap_or(0)]);
    }
    Ok(bytes)
}

/// Runs one node's read entirely on the blocking pool: bounded chunk
/// reads through the held handle, each chunk reserving its maximum size
/// first, with cancellation checked before every chunk and again before
/// anything is published.
#[allow(clippy::too_many_arguments)]
fn read_evidence(
    context: &ToolsContext,
    target: PreparedTarget,
    node: &ExecutionNode,
    budget: &Arc<StageByteBudget>,
    start_us: u64,
    origin: Instant,
    run_id: &str,
    meta: ReadMeta,
    cancel: &CancellationToken,
) -> Result<EvidenceOutput, ReadError> {
    // Contract version is persisted with the invocation and is checked
    // first: recovery must never execute an invocation whose contract
    // this executor does not implement (ADR-0006 §10).
    if node.invocation.capability.0 != "fs.read" {
        return Err(failed_too(format!(
            "evidence executor cannot run capability {}",
            node.invocation.capability.0
        ))
        .into());
    }
    if node.invocation.contract_version != meta.contract_version {
        return Err(failed_too(format!(
            "unsupported capability contract version {}; this executor implements {}",
            node.invocation.contract_version, meta.contract_version
        ))
        .into());
    }
    // The trusted compiler declared exactly one output; a node that does
    // not match what was compiled is refused rather than executed.
    if node.expected_outputs.len() != 1 || node.expected_outputs[0].name != EVIDENCE_OUTPUT_NAME {
        return Err(
            failed_too("evidence node does not declare exactly the evidence output").into(),
        );
    }

    // Authorize the EXACT operation derived from the canonical key,
    // before any byte is read.
    let operation = serde_json::json!({
        "capability": "fs.read",
        "scope": target.scope,
        "path": target.key,
    });
    authorize(
        &context.policy,
        &context.approvals,
        "fs.read",
        &target.scope,
        &operation,
        "supervisor evidence read",
    )
    .map_err(RuntimeError::Tool)?;

    // One-shot permit bound to the opened object, spent against it.
    let permit = EvidencePermit::issue(
        meta.task_id,
        run_id,
        meta.revision,
        meta.generation,
        node.id,
        meta.contract_version,
        &target,
    );
    let authority = permit.consume(&target)?;

    // M12 fault point: kill here = native read with no committed result.
    tachyon_tools::fault::reach_blocking("evidence.read");

    let meta_info = target
        .file
        .metadata()
        .map_err(|err| RuntimeError::Io(err.to_string()))?;
    // Metadata may reject an obviously oversized file early; it cannot
    // enforce the cap, so the shared budget still governs every chunk.
    let declared = meta_info.len();
    if declared > budget.limit() {
        return Err(
            failed_too(format!("file {} exceeds the stage byte budget", target.key)).into(),
        );
    }

    let mut file = target.file;
    let key = target.key;
    let bytes = read_chunks(&mut file, declared, budget, cancel)?;
    // Cancellation observed here means the result may not be published.
    if cancel.is_cancelled() {
        return Err(ReadError::Cancelled);
    }
    // The permit is spent only for this exact read; keep the key live so
    // the binding is part of the value we publish.
    debug_assert_eq!(authority.key(), key);

    // Durability before acknowledgement: the spool flushes, publishes
    // atomically and syncs its directory before node success exists.
    let artifact = context
        .artifacts
        .store(&bytes)
        .map_err(|err| RuntimeError::Io(err.to_string()))?;
    let end_us = u64::try_from(origin.elapsed().as_micros()).unwrap_or(u64::MAX);
    Ok(EvidenceOutput {
        artifact: artifact.to_string(),
        bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        path: key,
        freshness: hash_bytes(&bytes),
        start_us,
        end_us,
    })
}

/// Lowers typed evidence requests into validated IR, pairing each node
/// with the target already opened for it. This is the only production
/// caller of [`ValidatedExecutionGraph::try_mint`]: it validates the
/// invocation schema, read-only effect, idempotency, cancellation, retry
/// and output declarations before the proof exists (ADR-0006 §3).
pub(crate) fn compile_evidence_generation(
    task_id: TaskId,
    revision: u64,
    prepared: Vec<PreparedEvidence>,
    bounds: &RuntimeBounds,
) -> Result<(ValidatedExecutionGraph, HashMap<NodeId, PreparedTarget>), RuntimeError> {
    if prepared.len() > bounds.max_evidence_requests {
        return Err(RuntimeError::TooManyEvidence(prepared.len()));
    }
    let mut graph = ExecutionGraph::empty(task_id, revision);
    let mut targets = HashMap::with_capacity(prepared.len());
    for item in prepared {
        let key = item.target.key().to_owned();
        let node = compile_operation(
            task_id,
            revision,
            "fs.read",
            &serde_json::json!({ "path": key }),
        )?;
        if node.invocation.contract_version != FS_READ_CONTRACT_VERSION {
            return Err(failed_too("fs.read compiled without its contract version"));
        }
        targets.insert(node.id, item.target);
        graph.nodes.insert(node.id, node);
    }
    // The proof is minted only after structural validation AND the
    // trusted capability checks below; nothing else can produce it.
    let validated = ValidatedExecutionGraph::try_mint(graph, task_id, trusted_evidence_checks)
        .map_err(failed_too)?;
    Ok((validated, targets))
}

/// Capability-specific trusted validation run inside the proof
/// constructor: everything `ExecutionGraph::validate` deliberately leaves
/// to the capability contract (ADR-0006 §3).
fn trusted_evidence_checks(graph: &ExecutionGraph) -> Result<(), String> {
    for node in graph.nodes.values() {
        if node.invocation.capability.0 != "fs.read" {
            return Err(format!(
                "evidence graph holds non-evidence capability {}",
                node.invocation.capability.0
            ));
        }
        if node.invocation.contract_version != FS_READ_CONTRACT_VERSION {
            return Err(format!(
                "evidence node carries contract version {}",
                node.invocation.contract_version
            ));
        }
        if node.executor != ExecutorKind::Tool {
            return Err("evidence node must dispatch through the tool executor".to_owned());
        }
        if !node.access.writes.is_empty() {
            return Err("evidence node declares a write access".to_owned());
        }
        if node.effect_class != EffectClass::ReadOnlyLocal {
            return Err("evidence node is not read-only".to_owned());
        }
        if node.idempotency != Idempotency::Pure {
            return Err("evidence node is not pure".to_owned());
        }
        if node.cancellation != CancellationPolicy::Immediate {
            return Err("evidence node must cancel immediately".to_owned());
        }
        if node.retry.attempts != 1 {
            return Err("evidence node must not retry".to_owned());
        }
        if node.expected_outputs.len() != 1 || node.expected_outputs[0].name != EVIDENCE_OUTPUT_NAME
        {
            return Err("evidence node must declare exactly the evidence output".to_owned());
        }
        if node.resources.cpu_units == 0 {
            return Err("evidence node claims no CPU".to_owned());
        }
    }
    Ok(())
}

/// What one generation's workers produced for the `Loop` to settle.
pub(crate) struct JobResult {
    /// Generation these results belong to; stale ones are rejected.
    pub(crate) generation: u64,
    /// Receipts ready to journal, plus the stage measurements.
    pub(crate) outcome: Result<(Vec<EvidenceReceipt>, EvidenceTimings), RuntimeError>,
}

/// Stage measurements carried out of the job.
#[derive(Default)]
pub(crate) struct EvidenceTimings {
    /// Per-node measurements.
    pub(crate) timings: Vec<NodeTiming>,
    /// Per-node intervals in microseconds.
    pub(crate) intervals_us: Vec<(u64, u64)>,
}

/// Everything one generation job needs. Constructed by the `Loop` after
/// the generation is durably accepted.
pub(crate) struct GenerationSpec {
    pub(crate) task_id: TaskId,
    pub(crate) generation: u64,
    pub(crate) run_id: String,
    pub(crate) revision: u64,
    pub(crate) graph: ExecutionGraph,
    pub(crate) targets: HashMap<NodeId, PreparedTarget>,
    pub(crate) context: Arc<ToolsContext>,
    pub(crate) bounds: RuntimeBounds,
    pub(crate) concurrent: bool,
    pub(crate) origin: Instant,
    pub(crate) stage_cancel: CancellationToken,
}

/// Runs one accepted generation on Supervisor-owned workers and returns
/// the receipts the `Loop` may journal. It never journals anything: the
/// actor stays the single logical writer.
pub(crate) async fn run_generation(spec: GenerationSpec) -> JobResult {
    let generation = spec.generation;
    let outcome = execute_generation(spec).await;
    JobResult {
        generation,
        outcome,
    }
}

async fn execute_generation(
    spec: GenerationSpec,
) -> Result<(Vec<EvidenceReceipt>, EvidenceTimings), RuntimeError> {
    let task_id = spec.task_id;
    let stage_error: Arc<Mutex<Option<RuntimeError>>> = Arc::new(Mutex::new(None));
    let budget = StageByteBudget::new(
        spec.bounds.max_evidence_bytes_per_stage,
        spec.stage_cancel.clone(),
    );
    let executor = Arc::new(EvidenceReadExecutor {
        context: Arc::clone(&spec.context),
        targets: Mutex::new(spec.targets),
        budget,
        stage_error: Arc::clone(&stage_error),
        run_id: spec.run_id.clone(),
        origin: spec.origin,
        meta: ReadMeta {
            task_id,
            revision: spec.revision,
            generation: spec.generation,
            contract_version: FS_READ_CONTRACT_VERSION,
        },
    });
    let mut registry: ExecutorRegistry = HashMap::new();
    registry.insert(ExecutorKind::Tool, executor);
    let budgets = Budgets {
        cpu_units: if spec.concurrent {
            CONCURRENT_CPU_UNITS
        } else {
            SERIAL_CPU_UNITS
        },
        ..Budgets::default()
    };
    let (scheduler, join) = tachyon_scheduler::spawn(budgets, registry);
    if let Err(error) = scheduler.submit(task_id, spec.graph.clone()).await {
        drop(scheduler);
        let _ = join.await;
        return Err(failed_too(format!("evidence graph rejected: {error}")));
    }

    // Cancellation is observed here rather than in the actor so the
    // workers are signalled and drained before the caller is answered.
    let snapshot = tokio::select! {
        biased;
        () = spec.stage_cancel.cancelled() => {
            scheduler
                .cancel_task(task_id)
                .await
                .map_err(|error| failed_too(format!("evidence cancel: {error}")))?;
            scheduler
                .status(task_id)
                .await
                .map_err(|error| failed_too(format!("evidence status: {error}")))?
        }
        waited = scheduler.wait_finished(task_id, EVIDENCE_STAGE_TIMEOUT) => waited
            .map_err(|error| failed_too(format!("evidence settle: {error}")))?,
    };
    // Drain before reporting: dropping the handle closes the mailbox and
    // the join below waits for the loop (and every worker) to exit.
    drop(scheduler);
    let _ = join.await;

    settle_generation(&spec.graph, &snapshot, &stage_error, spec.generation)
}

/// Turns a settled scheduler snapshot into receipts, failing closed on
/// anything short of every node succeeding with a declared output.
fn settle_generation(
    graph: &ExecutionGraph,
    snapshot: &TaskRunSnapshot,
    stage_error: &Arc<Mutex<Option<RuntimeError>>>,
    generation: u64,
) -> Result<(Vec<EvidenceReceipt>, EvidenceTimings), RuntimeError> {
    let take_stage_error = || {
        stage_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    };
    if !snapshot.finished {
        return Err(take_stage_error().unwrap_or_else(|| {
            failed_too("evidence stage timed out before every worker drained")
        }));
    }
    if let Some(node_id) = snapshot.unresolved_node {
        return Err(take_stage_error().unwrap_or_else(|| {
            failed_too(format!(
                "evidence node {node_id} has an unsettled outcome; grants stay held"
            ))
        }));
    }

    let mut receipts = Vec::with_capacity(graph.nodes.len());
    let mut timings = EvidenceTimings::default();
    for (node_id, node) in &graph.nodes {
        let status = snapshot.statuses.get(node_id).copied();
        if status != Some(NodeStatus::Succeeded) {
            return Err(take_stage_error().unwrap_or_else(|| {
                failed_too(format!(
                    "evidence node {node_id} ended {:?}; no partial evidence batch is returned",
                    status.unwrap_or(NodeStatus::Pending)
                ))
            }));
        }
        let outputs = snapshot.outputs.get(node_id).ok_or_else(|| {
            take_stage_error().unwrap_or_else(|| {
                failed_too(format!("evidence node {node_id} published no outputs"))
            })
        })?;
        for binding in &node.expected_outputs {
            if !outputs.contains_key(&binding.name) {
                return Err(take_stage_error().unwrap_or_else(|| {
                    failed_too(format!(
                        "evidence node {node_id} omitted declared output {}",
                        binding.name
                    ))
                }));
            }
        }
        let value = outputs.get(EVIDENCE_OUTPUT_NAME).ok_or_else(|| {
            failed_too(format!(
                "evidence node {node_id} published no evidence output"
            ))
        })?;
        let output: EvidenceOutput = serde_json::from_value(value.clone())
            .map_err(|error| failed_too(format!("evidence output malformed: {error}")))?;
        if !is_content_address(&output.artifact) {
            return Err(failed_too(
                "evidence artifact id is not a BLAKE3 hex digest",
            ));
        }
        let expected_key = node
            .invocation
            .args
            .get("path")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if output.path != expected_key {
            return Err(failed_too(format!(
                "evidence node {node_id} reported path {} for key {expected_key}",
                output.path
            )));
        }
        receipts.push(EvidenceReceipt {
            generation,
            node_id: *node_id,
            artifact: ArtifactId(output.artifact),
            bytes: output.bytes,
            path: output.path.clone(),
            freshness_hash: output.freshness,
            capability_version: node.invocation.contract_version,
        });
        timings.timings.push(NodeTiming {
            node: format!("fs.read:{}", output.path),
            start_ms: output.start_us / 1000,
            end_ms: output.end_us / 1000,
        });
        timings.intervals_us.push((output.start_us, output.end_us));
    }
    Ok((receipts, timings))
}

fn is_content_address(raw: &str) -> bool {
    raw.len() == 64
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

/// Prepares every request: bounds first (count and path size are checked
/// before any path is opened or node allocated), then resolve, contain,
/// open and prove each target beneath the pinned root (ADR-0006 §4).
pub(crate) fn prepare_evidence(
    context: &ToolsContext,
    requests: &[EvidenceRequest],
    bounds: &RuntimeBounds,
) -> Result<Vec<PreparedEvidence>, RuntimeError> {
    if requests.len() > bounds.max_evidence_requests {
        return Err(RuntimeError::TooManyEvidence(requests.len()));
    }
    let root = std::fs::canonicalize(&context.workspace_root)
        .map_err(|err| failed_too(format!("pinned workspace root: {err}")))?;
    let root_meta = std::fs::metadata(&root)
        .map_err(|err| failed_too(format!("pinned workspace root metadata: {err}")))?;
    if !root_meta.is_dir() {
        return Err(failed_too("pinned workspace root is not a directory"));
    }
    let root_identity = identity_of(&root_meta)
        .ok_or_else(|| failed_too("platform cannot prove the pinned root identity"))?;

    let mut prepared = Vec::with_capacity(requests.len());
    for request in requests {
        prepared.push(PreparedEvidence {
            target: prepare_target(&root, root_identity, request, bounds)?,
        });
    }
    Ok(prepared)
}

/// Bounds one request, resolves and proves it, then opens it read-only.
/// Everything about the target — canonical key, policy scope, handle and
/// object identity — is established here, once, before dispatch.
fn prepare_target(
    root: &Path,
    root_identity: FileIdentity,
    request: &EvidenceRequest,
    bounds: &RuntimeBounds,
) -> Result<PreparedTarget, RuntimeError> {
    if request.capability != "fs.read" {
        return Err(RuntimeError::UnknownCapability(request.capability.clone()));
    }
    let key = bounded_key(&request.path, bounds)?;
    let (resolved, scope) = resolve_scope(root, &root.join(&key)).map_err(RuntimeError::Tool)?;
    if !scope.starts_with("workspace/") {
        return Err(RuntimeError::StaleEvidence(
            "evidence path resolves outside the pinned workspace".to_owned(),
        ));
    }
    // The resolved path must be canonical right now: a symlink or
    // reparse point swapped in after resolution fails closed.
    let canonical = std::fs::canonicalize(&resolved)
        .map_err(|err| RuntimeError::Io(format!("resolve {}: {err}", resolved.display())))?;
    if canonical != resolved || !canonical.starts_with(root) {
        return Err(RuntimeError::StaleEvidence(
            "evidence path is not canonical beneath the pinned root".to_owned(),
        ));
    }
    let canonical_key = canonical
        .strip_prefix(root)
        .map_err(|_| RuntimeError::StaleEvidence("evidence path escaped the root".to_owned()))?
        .to_string_lossy()
        .replace('\\', "/");
    let (file, identity) = open_proven(&canonical, &canonical_key)?;
    Ok(PreparedTarget {
        root_identity,
        key: canonical_key,
        scope,
        file,
        identity,
    })
}

/// Checks the raw and normalized path against the configured path-size
/// bound (ADR-0006 §4: validated before any path is opened).
fn bounded_key(path: &str, bounds: &RuntimeBounds) -> Result<String, RuntimeError> {
    if path.len() > bounds.max_evidence_path_bytes {
        return Err(RuntimeError::InvalidArgs {
            capability: "fs.read".to_owned(),
            reason: format!(
                "evidence path is {} bytes; the bound is {}",
                path.len(),
                bounds.max_evidence_path_bytes
            ),
        });
    }
    let key = normalize_key(path)?;
    if key.len() > bounds.max_evidence_path_bytes {
        return Err(RuntimeError::InvalidArgs {
            capability: "fs.read".to_owned(),
            reason: format!(
                "normalized evidence path is {} bytes; the bound is {}",
                key.len(),
                bounds.max_evidence_path_bytes
            ),
        });
    }
    Ok(key)
}

/// Opens `canonical` read-only and proves the handle names a regular file
/// beneath the pinned root: regular-file proof before opening (so a
/// special file is never opened and never blocks), identity match between
/// the handle and the path, and canonicality re-proved after the open.
fn open_proven(
    canonical: &Path,
    canonical_key: &str,
) -> Result<(std::fs::File, FileIdentity), RuntimeError> {
    let before = std::fs::metadata(canonical)
        .map_err(|err| RuntimeError::Io(format!("stat {}: {err}", canonical.display())))?;
    if !before.is_file() {
        return Err(RuntimeError::StaleEvidence(format!(
            "{canonical_key} is not a regular file"
        )));
    }
    let before_identity = identity_of(&before)
        .ok_or_else(|| failed_too("platform cannot prove the target identity"))?;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(canonical)
        .map_err(|err| RuntimeError::Io(format!("open {}: {err}", canonical.display())))?;
    let handle_meta = file
        .metadata()
        .map_err(|err| RuntimeError::Io(format!("fstat {}: {err}", canonical.display())))?;
    if !handle_meta.is_file() {
        return Err(RuntimeError::StaleEvidence(format!(
            "{canonical_key} is not a regular file once opened"
        )));
    }
    let handle_identity = identity_of(&handle_meta)
        .ok_or_else(|| failed_too("platform cannot prove the opened object identity"))?;
    if handle_identity != before_identity {
        return Err(RuntimeError::StaleEvidence(
            "opened object differs from the object the canonical path names".to_owned(),
        ));
    }
    // Re-prove canonicality after the open: the window between the first
    // check and `open` is where a symlink swap would land.
    let after = std::fs::canonicalize(canonical)
        .map_err(|err| RuntimeError::Io(format!("re-resolve: {err}")))?;
    if after != canonical {
        return Err(RuntimeError::StaleEvidence(
            "evidence path changed while it was being opened".to_owned(),
        ));
    }
    Ok((file, handle_identity))
}

/// Supervised retrieval of a committed receipt's bytes (ADR-0006 §12):
/// bounded, decompressed, BLAKE3-verified against the receipt's id and
/// length-checked before the bytes reach anything. The receipt — never an
/// artifact id alone — is what authorizes the read (ADR-0006 §13).
pub(crate) fn fetch_receipt(
    spool: &tachyon_tools::artifact::ArtifactSpool,
    receipt: &EvidenceReceipt,
    max_bytes: u64,
) -> Result<EvidenceItem, RuntimeError> {
    let limit = usize::try_from(max_bytes)
        .map_err(|_| failed_too("evidence fetch limit does not fit this platform"))?;
    let expected = usize::try_from(receipt.bytes)
        .map_err(|_| failed_too("receipt length does not fit this platform"))?;
    if expected > limit {
        return Err(RuntimeError::Evidence(format!(
            "receipt length {expected} exceeds the fetch limit {limit}"
        )));
    }
    let bytes = spool
        .fetch_verified(&receipt.artifact, expected, limit)
        .map_err(|err| RuntimeError::Io(format!("evidence artifact: {err}")))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != receipt.bytes {
        return Err(RuntimeError::Evidence(
            "evidence artifact does not match its receipt length".to_owned(),
        ));
    }
    Ok(EvidenceItem {
        path: receipt.path.clone(),
        hash: receipt.freshness_hash.clone(),
        bytes,
    })
}

/// Payload of `SupervisorCommand::CollectEvidence`, grouped so the
/// supervisor's dispatch stays one arm per command.
pub(crate) struct CollectCommand {
    /// Run the request is fenced to.
    pub(crate) run_id: String,
    /// Revision the caller believes the task is at.
    pub(crate) revision: u64,
    /// Typed, trusted evidence requests.
    pub(crate) requests: Vec<EvidenceRequest>,
    /// Slice bounds (request count, stage bytes, path size).
    pub(crate) bounds: RuntimeBounds,
    /// Whether the stage measures real overlap.
    pub(crate) concurrent: bool,
    /// Timing origin for the reported measurements.
    pub(crate) origin: Instant,
    /// Host cancellation token; workers bind to a child of it.
    pub(crate) cancel: CancellationToken,
    /// Workspace, policy, approvals and artifact spool for this run.
    pub(crate) context: Arc<ToolsContext>,
    /// Answered once the generation settles.
    pub(crate) reply: oneshot::Sender<Result<EvidenceBatch, EvidenceStageError>>,
}

/// The live generation the `Loop` is waiting on: its fence, its stage
/// cancel token, and the caller's reply (held until the generation
/// settles, exactly like a parked approval).
pub(crate) struct ActiveEvidence {
    /// Generation this wait belongs to.
    pub(crate) generation: u64,
    /// Revision the caller was fenced to.
    pub(crate) revision: u64,
    /// Run the caller was fenced to.
    pub(crate) run_id: String,
    /// Cancels the stage: a child of the host token, so the actor can
    /// also signal it on a revision change or on shutdown (ADR-0006 §7).
    pub(crate) cancel: CancellationToken,
    /// Answered once the generation settles.
    pub(crate) reply: Option<oneshot::Sender<Result<EvidenceBatch, EvidenceStageError>>>,
}

impl crate::Loop {
    /// Handles one `CollectEvidence` command: fence, prepare, compile,
    /// accept durably, then dispatch on Supervisor-owned workers. The
    /// reply is held, never answered before the workers have drained.
    pub(super) async fn collect_evidence(&mut self, command: CollectCommand) {
        let CollectCommand {
            run_id,
            revision,
            requests,
            bounds,
            concurrent,
            origin,
            cancel,
            context,
            reply,
        } = command;
        let begun = self
            .begin_evidence_generation(
                run_id, revision, requests, bounds, concurrent, origin, cancel, context,
            )
            .await;
        match begun {
            Ok(mut active) => {
                active.reply = Some(reply);
                self.active_evidence = Some(active);
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    /// Fences, prepares, compiles and durably accepts one generation.
    #[allow(clippy::too_many_arguments)]
    async fn begin_evidence_generation(
        &mut self,
        run_id: String,
        revision: u64,
        requests: Vec<EvidenceRequest>,
        bounds: RuntimeBounds,
        concurrent: bool,
        origin: Instant,
        cancel: CancellationToken,
        context: Arc<ToolsContext>,
    ) -> Result<ActiveEvidence, EvidenceStageError> {
        if self.active_evidence.is_some() {
            return Err(RuntimeError::Evidence(
                "an evidence generation is already active".to_owned(),
            )
            .into());
        }
        // A cancelled run opens nothing and allocates no generation
        // (ADR-0006 §7): cancellation stops further dispatch at once.
        if cancel.is_cancelled() {
            return Err(RuntimeError::Evidence(
                "evidence stage cancelled before dispatch".to_owned(),
            )
            .into());
        }
        // Run/revision fence: stale runs and stale revisions are refused
        // before a single path is opened (ADR-0006 §2).
        match self.active_run.as_deref() {
            None => {
                return Err(CoreError::UnknownRun { run_id }.into());
            }
            Some(active) if *active != run_id => {
                return Err(CoreError::RunAlreadyActive {
                    active: active.to_owned(),
                }
                .into());
            }
            Some(_) => {}
        }
        if revision != self.state.revision {
            return Err(CoreError::StaleRunProposal {
                expected: self.state.revision,
                got: revision,
            }
            .into());
        }
        // Only a terminal task refuses: the run/revision fence above is
        // what authorizes dispatch, and a recovered task re-enters from
        // `Recovering` (ADR-0006 §9: explicit re-entry, new generation).
        if self.state.status.is_terminal() {
            return Err(CoreError::IllegalTransition {
                from: self.state.status,
                to: self.state.status,
            }
            .into());
        }
        // The durable pin is authoritative when it exists.
        if let Some(pinned) = &self.state.workspace_root
            && pinned.as_str() != context.workspace_root.to_string_lossy().as_ref()
        {
            return Err(RuntimeError::StaleEvidence(
                "tools context root differs from the pinned workspace root".to_owned(),
            )
            .into());
        }
        // First stage wins: receipts already committed were stored in
        // this spool, so retrieval must keep reading from it.
        if self.evidence_spool.is_none() {
            self.evidence_spool = Some(context.artifacts.clone());
        }
        // Retrieval is bounded by the same stage budget the receipts
        // were produced under.
        self.evidence_limit = bounds.max_evidence_bytes_per_stage;

        // Previous generation must be fully settled before a new one is
        // allocated (ADR-0006 §9: retire only after drain + terminal).
        if let Some(previous) = self.state.execution_generation
            && self.generation_unsettled()
        {
            return Err(RuntimeError::Evidence(format!(
                "generation {previous} is still unsettled"
            ))
            .into());
        }

        // Bounds are validated before any path is opened or node
        // allocated (ADR-0006 §4), then every target is opened and
        // proven beneath the pinned root.
        let prepared =
            prepare_evidence(&context, &requests, &bounds).map_err(EvidenceStageError::Stage)?;
        let task_id = self.state.id;
        let (validated, targets) =
            compile_evidence_generation(task_id, revision, prepared, &bounds)
                .map_err(EvidenceStageError::Stage)?;
        let graph = validated.into_graph();
        let generation = self.state.next_execution_generation;
        let node_count = graph.nodes.len();

        // One durable transaction allocates the generation, advances the
        // counter, installs the graph, seeds node states and sets the
        // active pointer. Nothing dispatches before this acknowledges.
        self.transition_journalled(StateEvent::ExecutionGenerationAccepted {
            generation,
            graph: graph.clone(),
        })
        .await
        .map_err(EvidenceStageError::Core)?;

        let stage_cancel = cancel.child_token();
        let spec = GenerationSpec {
            task_id,
            generation,
            run_id: run_id.clone(),
            revision,
            graph,
            targets,
            context,
            bounds,
            concurrent,
            origin,
            stage_cancel: stage_cancel.clone(),
        };
        debug_assert_eq!(node_count, spec.graph.nodes.len());
        self.evidence_jobs
            .spawn(async move { run_generation(spec).await });

        Ok(ActiveEvidence {
            generation,
            revision,
            run_id,
            cancel: stage_cancel,
            reply: None,
        })
    }

    /// True when the active evidence generation still has a non-terminal
    /// node. Without an active generation there is nothing to settle, so
    /// a legacy installed graph (which carries no generation) never
    /// blocks.
    pub(super) fn generation_unsettled(&self) -> bool {
        if self.state.execution_generation.is_none() {
            return false;
        }
        let Some(graph) = self.state.execution_graph.as_ref() else {
            return false;
        };
        graph.nodes.keys().any(|node_id| {
            !self
                .state
                .node_statuses
                .get(node_id)
                .is_some_and(|status| status.is_terminal())
        })
    }

    /// Retires `generation` if it is still the active one: unfinished
    /// nodes become `Cancelled` and the pointer clears. Receipts already
    /// committed stay attached for audit (ADR-0006 §9).
    async fn interrupt_generation(&mut self, generation: u64) -> Result<(), CoreError> {
        if self.state.execution_generation != Some(generation) {
            return Ok(());
        }
        self.transition_journalled(StateEvent::GenerationInterrupted { generation })
            .await?;
        Ok(())
    }

    /// Signals every in-flight evidence worker without waiting: used on a
    /// revision change and on shutdown, so no worker keeps reading after
    /// the fence moved (ADR-0006 §7).
    pub(super) fn signal_evidence_cancellation(&mut self) {
        if let Some(active) = self.active_evidence.as_ref() {
            active.cancel.cancel();
        }
    }

    /// Settles one finished generation job. Stale results are rejected
    /// before anything is journalled, so a late worker can never publish
    /// into a newer generation (ADR-0006 §8).
    pub(super) async fn finish_evidence_job(
        &mut self,
        joined: Result<JobResult, tokio::task::JoinError>,
    ) {
        let result = match joined {
            Ok(result) => result,
            Err(error) => {
                self.reply_to_evidence(RuntimeError::Evidence(format!(
                    "evidence worker failed: {error}"
                )))
                .await;
                return;
            }
        };
        let Some(active) = self.active_evidence.take() else {
            // No waiter: the result is stale by construction and must
            // never journal.
            let _ = self.interrupt_generation(result.generation).await;
            return;
        };
        let stale = active.generation != result.generation
            || self.state.execution_generation != Some(result.generation)
            || active.revision != self.state.revision
            || self.active_run.as_deref() != Some(active.run_id.as_str());
        if stale {
            let _ = self.interrupt_generation(active.generation).await;
            let _ = active.reply.map(|reply| {
                reply.send(Err(RuntimeError::Evidence(
                    "stale evidence generation result rejected".to_owned(),
                )
                .into()))
            });
            return;
        }
        match result.outcome {
            Ok((receipts, timings)) => {
                let generation = active.generation;
                let committed = self
                    .transition_journalled(StateEvent::EvidenceGenerationCommitted {
                        generation,
                        receipts: receipts.clone(),
                    })
                    .await;
                let Err(error) = committed else {
                    let batch = self.assemble_evidence_batch(receipts, timings);
                    let _ = active.reply.map(|reply| reply.send(batch));
                    return;
                };
                // A failed commit leaves an unsettled generation behind;
                // retire it rather than blocking every later stage.
                let _ = self.interrupt_generation(generation).await;
                let _ = active.reply.map(|reply| reply.send(Err(error.into())));
            }
            Err(error) => {
                let _ = self.interrupt_generation(active.generation).await;
                let _ = active.reply.map(|reply| reply.send(Err(error.into())));
            }
        }
    }

    /// Answers the waiting caller with a stage failure, retiring the
    /// generation first so the pointer never outlives its workers.
    async fn reply_to_evidence(&mut self, error: RuntimeError) {
        let active = self.active_evidence.take();
        if let Some(active) = active {
            let _ = self.interrupt_generation(active.generation).await;
            let _ = active.reply.map(|reply| reply.send(Err(error.into())));
        }
    }

    /// Fetches every committed receipt back through the spool, so the
    /// bytes the model sees were verified against their content address
    /// and receipt length (ADR-0006 §12).
    fn assemble_evidence_batch(
        &self,
        receipts: Vec<EvidenceReceipt>,
        timings: EvidenceTimings,
    ) -> Result<EvidenceBatch, EvidenceStageError> {
        let spool = self
            .evidence_spool
            .as_ref()
            .ok_or_else(|| EvidenceStageError::Core(CoreError::EvidenceRetrievalUnavailable))?;
        let limit = self.evidence_limit;
        let mut receipts = receipts;
        receipts.sort_by(|a, b| a.path.cmp(&b.path));
        let mut items = Vec::with_capacity(receipts.len());
        for receipt in &receipts {
            items.push(fetch_receipt(spool, receipt, limit).map_err(EvidenceStageError::Stage)?);
        }
        let graph_nodes = self
            .state
            .execution_graph
            .as_ref()
            .map_or(0, |graph| graph.nodes.len());
        Ok(EvidenceBatch {
            items,
            receipts,
            timings: timings.timings,
            intervals_us: timings.intervals_us,
            graph_nodes,
        })
    }

    /// Supervisor-mediated retrieval (ADR-0006 §12–§13): the durable
    /// receipt of this task authorizes the read, never an artifact id
    /// alone, and the bytes are verified against that receipt.
    ///
    /// Two authorizations are required together: the task's durable
    /// workspace pin (the provenance the receipt's key was derived
    /// under) and the spool this supervisor's stage ran with. A receipt
    /// survives a restart, but a supervisor that has not run the stage in
    /// this process holds no spool, so retrieval reports unavailable
    /// rather than reaching for a raw spool path.
    pub(super) fn fetch_evidence(&self, node_id: NodeId) -> Result<EvidenceItem, CoreError> {
        let receipt = self
            .state
            .evidence_receipts
            .get(&node_id)
            .ok_or(CoreError::UnknownEvidenceReceipt { node_id })?;
        if self.state.node_statuses.get(&node_id).copied() != Some(NodeStatus::Succeeded) {
            return Err(CoreError::UnknownEvidenceReceipt { node_id });
        }
        // Generation binding: the receipt must come from a generation
        // this task allocated, and its provenance key must still be a
        // workspace-relative journal key.
        if receipt.generation >= self.state.next_execution_generation {
            return Err(CoreError::UnknownEvidenceReceipt { node_id });
        }
        if receipt.path.is_empty()
            || receipt.path.starts_with('/')
            || receipt.path.contains("..")
            || receipt.path.contains('\\')
        {
            return Err(CoreError::UnknownEvidenceReceipt { node_id });
        }
        if self.state.workspace_root.is_none() {
            return Err(CoreError::EvidenceRetrievalUnavailable);
        }
        let spool = self
            .evidence_spool
            .as_ref()
            .ok_or(CoreError::EvidenceRetrievalUnavailable)?;
        fetch_receipt(spool, receipt, self.evidence_limit)
            .map_err(|error| CoreError::VerificationBlocked(error.to_string()))
    }

    /// Recovery: retire an unsettled read-only evidence generation before
    /// anything re-enters (ADR-0006 §9). Returns whether it fired.
    pub(super) async fn interrupt_unsettled_generation(&mut self) -> Result<bool, CoreError> {
        let Some(generation) = self.state.execution_generation else {
            return Ok(false);
        };
        if !self.generation_unsettled() || self.state.status.is_terminal() {
            return Ok(false);
        }
        self.transition_journalled(StateEvent::GenerationInterrupted { generation })
            .await?;
        let from = self.state.status;
        if from != crate::TaskStatus::Recovering {
            self.transition_journalled(StateEvent::Status {
                from,
                to: crate::TaskStatus::Recovering,
            })
            .await?;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use tachyon_ir::ExecutionGraph;
    use tachyon_types::{SessionId, WorkspaceId};

    use crate::runtime::compile_operation;
    use crate::{StateEvent, TaskState, TaskStatus, apply_generation_event};

    fn state() -> TaskState {
        let id = TaskId::generate();
        TaskState {
            id,
            session_id: SessionId::generate(),
            workspace_id: WorkspaceId::generate(),
            workspace_root: None,
            objective: "collect evidence".to_owned(),
            revision: 0,
            constraints: Vec::new(),
            facts: Vec::new(),
            hypotheses: Vec::new(),
            open_questions: Vec::new(),
            acceptance: tachyon_verify::AcceptanceContract::default(),
            graph: ExecutionGraph::empty(id, 0),
            execution_graph: None,
            node_statuses: BTreeMap::new(),
            execution_generation: None,
            next_execution_generation: 1,
            evidence_receipts: BTreeMap::new(),
            effects: BTreeMap::new(),
            stages: Vec::new(),
            evidence_summary: Vec::new(),
            changed_files: Vec::new(),
            agent_messages: Vec::new(),
            conversation: Vec::new(),
            approval_requests: Vec::new(),
            verification: None,
            status: TaskStatus::Created,
            created_at: tachyon_types::Timestamp::now(),
            updated_at: tachyon_types::Timestamp::now(),
        }
    }

    fn graph_for(task_id: TaskId) -> (ExecutionGraph, NodeId) {
        let node = compile_operation(
            task_id,
            0,
            "fs.read",
            &serde_json::json!({ "path": "src/a.rs" }),
        )
        .expect("fs.read compiles");
        let node_id = node.id;
        let mut graph = ExecutionGraph::empty(task_id, 0);
        graph.nodes.insert(node_id, node);
        (graph, node_id)
    }

    fn receipt(generation: u64, node_id: NodeId) -> EvidenceReceipt {
        EvidenceReceipt {
            generation,
            node_id,
            artifact: ArtifactId("0".repeat(64)),
            bytes: 8,
            path: "src/a.rs".to_owned(),
            freshness_hash: hash_bytes(b"contents"),
            capability_version: FS_READ_CONTRACT_VERSION,
        }
    }

    #[test]
    fn accepting_a_generation_allocates_advances_and_seeds_nodes() {
        let mut state = state();
        let (graph, node_id) = graph_for(state.id);
        apply_generation_event(
            &mut state,
            StateEvent::ExecutionGenerationAccepted {
                generation: 1,
                graph,
            },
        )
        .expect("first generation accepts");
        assert_eq!(state.execution_generation, Some(1));
        assert_eq!(state.next_execution_generation, 2);
        assert_eq!(
            state.node_statuses.get(&node_id),
            Some(&NodeStatus::Pending),
            "node states are seeded by the same transaction"
        );
    }

    #[test]
    fn a_generation_that_skips_the_counter_is_refused() {
        let mut state = state();
        let (graph, _) = graph_for(state.id);
        let error = apply_generation_event(
            &mut state,
            StateEvent::ExecutionGenerationAccepted {
                generation: 7,
                graph,
            },
        )
        .expect_err("generation 7 is not the next generation");
        assert!(matches!(error, crate::CoreError::Corrupt { .. }));
        assert_eq!(state.execution_generation, None);
        assert_eq!(state.next_execution_generation, 1, "no write happened");
    }

    #[test]
    fn a_late_commit_from_a_retired_generation_is_refused_before_writing() {
        let mut state = state();
        let (first_graph, first_node) = graph_for(state.id);
        apply_generation_event(
            &mut state,
            StateEvent::ExecutionGenerationAccepted {
                generation: 1,
                graph: first_graph,
            },
        )
        .unwrap();
        let (second_graph, second_node) = graph_for(state.id);
        apply_generation_event(
            &mut state,
            StateEvent::ExecutionGenerationAccepted {
                generation: 2,
                graph: second_graph,
            },
        )
        .unwrap();

        // A late worker from generation 1 tries to journal its success
        // after generation 2 is active: rejected, nothing written.
        let error = apply_generation_event(
            &mut state,
            StateEvent::EvidenceGenerationCommitted {
                generation: 1,
                receipts: vec![receipt(1, first_node)],
            },
        )
        .expect_err("a stale generation's success must never journal");
        assert!(matches!(error, crate::CoreError::Corrupt { .. }));
        assert!(
            state.evidence_receipts.is_empty(),
            "the stale receipt is not recorded"
        );
        assert_eq!(
            state.node_statuses.get(&first_node),
            Some(&NodeStatus::Pending),
            "the stale result does not move a node"
        );

        // Interrupting the retired generation is refused for the same
        // reason: only the ACTIVE generation may be retired.
        let error = apply_generation_event(
            &mut state,
            StateEvent::GenerationInterrupted { generation: 1 },
        )
        .expect_err("a stale generation cannot be retired");
        assert!(matches!(error, crate::CoreError::Corrupt { .. }));
        assert_eq!(state.execution_generation, Some(2));

        // The live generation still commits normally.
        apply_generation_event(
            &mut state,
            StateEvent::EvidenceGenerationCommitted {
                generation: 2,
                receipts: vec![receipt(2, second_node)],
            },
        )
        .expect("the active generation commits");
        assert_eq!(
            state.node_statuses.get(&second_node),
            Some(&NodeStatus::Succeeded)
        );
        assert_eq!(state.evidence_receipts.len(), 1);
    }

    #[test]
    fn committing_with_unfinished_nodes_is_refused() {
        let mut state = state();
        let (graph, _) = graph_for(state.id);
        apply_generation_event(
            &mut state,
            StateEvent::ExecutionGenerationAccepted {
                generation: 1,
                graph,
            },
        )
        .unwrap();
        let error = apply_generation_event(
            &mut state,
            StateEvent::EvidenceGenerationCommitted {
                generation: 1,
                receipts: Vec::new(),
            },
        )
        .expect_err("a generation with a pending node cannot commit");
        assert!(matches!(error, crate::CoreError::Corrupt { .. }));
        assert!(state.evidence_receipts.is_empty());
    }

    #[test]
    fn interrupting_cancels_unfinished_nodes_and_clears_the_pointer() {
        let mut state = state();
        let (graph, node_id) = graph_for(state.id);
        apply_generation_event(
            &mut state,
            StateEvent::ExecutionGenerationAccepted {
                generation: 1,
                graph,
            },
        )
        .unwrap();
        apply_generation_event(
            &mut state,
            StateEvent::GenerationInterrupted { generation: 1 },
        )
        .expect("the active generation retires");
        assert_eq!(state.execution_generation, None);
        assert_eq!(
            state.node_statuses.get(&node_id),
            Some(&NodeStatus::Cancelled)
        );
        assert_eq!(
            state.next_execution_generation, 2,
            "the interrupted generation is never handed out again"
        );
    }

    #[test]
    fn trusted_checks_fail_closed_on_anything_the_contract_does_not_allow() {
        let task_id = TaskId::generate();
        let (graph, node_id) = graph_for(task_id);
        trusted_evidence_checks(&graph).expect("a compiled evidence graph passes");

        let mut wrong_version = graph.clone();
        wrong_version
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .invocation
            .contract_version = tachyon_ir::CAPABILITY_CONTRACT_NONE;
        assert!(trusted_evidence_checks(&wrong_version).is_err());

        let mut writes = graph.clone();
        writes.nodes.get_mut(&node_id).unwrap().access.writes =
            writes.nodes.get(&node_id).unwrap().access.reads.clone();
        assert!(trusted_evidence_checks(&writes).is_err());

        let mut retries = graph.clone();
        retries.nodes.get_mut(&node_id).unwrap().retry.attempts = 3;
        assert!(trusted_evidence_checks(&retries).is_err());

        let mut not_pure = graph.clone();
        not_pure.nodes.get_mut(&node_id).unwrap().idempotency = Idempotency::NonIdempotent;
        assert!(trusted_evidence_checks(&not_pure).is_err());

        let mut extra_output = graph.clone();
        extra_output
            .nodes
            .get_mut(&node_id)
            .unwrap()
            .expected_outputs
            .push(tachyon_ir::OutputBinding {
                name: "unexpected".to_owned(),
            });
        assert!(trusted_evidence_checks(&extra_output).is_err());
    }

    #[test]
    fn budget_counts_committed_bytes_across_the_whole_stage() {
        let cancel = CancellationToken::new();
        let budget = StageByteBudget::new(100, cancel.clone());

        let mut first = budget.reserve(60).expect("fits");
        first.commit(50); // 50 consumed, 10 returned
        drop(first);

        let mut second = budget.reserve(40).expect("50 + 40 <= 100");
        second.commit(40);
        drop(second);

        let error = budget.reserve(11);
        assert!(
            matches!(error, Err(RuntimeError::Evidence(_))),
            "50 + 40 + 11 exceeds the limit"
        );
        assert!(
            cancel.is_cancelled(),
            "exhaustion signals the stage so siblings stop and drain"
        );
    }

    #[test]
    fn an_uncommitted_reservation_is_returned_on_drop() {
        let budget = StageByteBudget::new(100, CancellationToken::new());
        {
            let _reservation = budget.reserve(90).expect("fits");
        }
        assert!(
            budget.reserve(100).is_ok(),
            "a reservation that never read anything is returned in full"
        );
    }

    #[test]
    fn committed_bytes_stay_counted_for_the_rest_of_the_stage() {
        let budget = StageByteBudget::new(100, CancellationToken::new());
        let mut reservation = budget.reserve(100).expect("fits");
        reservation.commit(10);
        drop(reservation);
        assert!(
            budget.reserve(91).is_err(),
            "the 10 consumed bytes stay counted against the stage limit"
        );
        let mut rest = budget.reserve(90).expect("10 + 90 fits exactly");
        rest.commit(90);
        drop(rest);
        assert!(
            budget.reserve(1).is_err(),
            "the stage is full: 100 bytes of evidence are committed"
        );
    }
}
