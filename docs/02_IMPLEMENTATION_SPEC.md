# 02 — Concrete Implementation Specification

Version: v0.2-impl.2

This document is the concrete implementation contract for Tachyon MVP. It incorporates the adversarial review in `03_ADVERSARIAL_REVIEW.md`.

---

## 1. Workspace and dependency boundaries

Use one Cargo workspace. Crate responsibilities:

- `tachyon-types`: identifiers, timestamps, shared low-level value types. No networking/database/runtime ownership.
- `tachyon-protocol`: versioned gateway commands/events and serialization envelopes.
- `tachyon-ir`: ExecutionGraph, ExecutionNode, dependencies, access/effect/resource/cancellation/retry semantics, IR validation.
- `tachyon-store`: SQLite state journal, snapshots, effect/approval records, migrations, artifact metadata.
- `tachyon-policy`: capability matching, project trust, approvals, path/resource containment decisions.
- `tachyon-tools`: native capability registry and built-in filesystem/process/git/mutation primitives.
- `tachyon-repo`: workspace file inventory, hashes, lexical search, AST/symbol/reference indexing, freshness.
- `tachyon-retrieval`: evidence structures, result merger/ranker, provenance, future external/document retrieval adapters.
- `tachyon-models`: provider-neutral model requests/results/capability negotiation and provider registry.
- `tachyon-judgment`: provider-neutral bounded judgment API and OpenJEV adapter.
- `tachyon-router`: route classification, initial-node construction, escalation policy and telemetry-driven estimates.
- `tachyon-scheduler`: DAG readiness, conflict/resource manager, critical-path priority, structured execution bookkeeping.
- `tachyon-verify`: acceptance contracts, project verification strategies and completion gates.
- `tachyon-telemetry`: tracing spans, measurements, local aggregation and optional exporters.
- `tachyon-core`: Task Supervisor, authoritative task-state machine, graph revisions, command handling and recovery coordination.
- `tachyon-gateway`: local IPC and optional remote transport adapters.
- `tachyon-tui`: Ratatui client state/render/input only.
- `tachyon-app`: CLI/binary bootstrap and command dispatch.

Dependency rule: lower-level crates never import `tachyon-core`, gateway or UI crates. Provider implementation types do not cross into core/IR.

---

## 2. Fundamental identifiers

Use UUIDv7 for persistent runtime entities.

```rust
pub struct TaskId(pub Uuid);
pub struct SessionId(pub Uuid);
pub struct NodeId(pub Uuid);
pub struct WorkspaceId(pub Uuid);
pub struct EventId(pub Uuid);
pub struct ApprovalId(pub Uuid);
pub struct MutationBatchId(pub Uuid);
pub struct ProviderId(pub String);
pub struct CapabilityId(pub String);
pub struct ArtifactId(pub String); // content-addressed BLAKE3
```

Newtypes must derive/implement serialization, equality, hashing and display as appropriate.

---

## 3. Session and task model

A Session is a persistent user interaction context. A Task is executable work.

```rust
pub enum TaskStatus {
    Created,
    Routing,
    Planning,
    Executing,
    Verifying,
    WaitingApproval,
    Paused,
    Recovering,
    Completed,
    Failed,
    Cancelled,
}
```

Canonical state:

```rust
pub struct TaskState {
    pub id: TaskId,
    pub session_id: SessionId,
    pub workspace_id: WorkspaceId,
    pub objective: String,
    pub revision: u64,
    pub constraints: Vec<TaskConstraint>,
    pub facts: Vec<Fact>,
    pub hypotheses: Vec<Hypothesis>,
    pub open_questions: Vec<OpenQuestion>,
    pub acceptance: AcceptanceContract,
    pub graph: ExecutionGraph,
    pub status: TaskStatus,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}
```

Task Supervisor is the only logical writer of this structure.

Every user steering message that changes execution-relevant meaning increments `revision`.

Every planned node stores `planned_revision`. When revision changes, supervisor revalidates pending/running work against new hard constraints and cancels/invalidates only affected branches.

---

## 4. Task constraints

```rust
pub struct TaskConstraint {
    pub id: Uuid,
    pub source: ConstraintSource,
    pub text: String,
    pub strength: ConstraintStrength,
    pub created_revision: u64,
}

pub enum ConstraintSource { User, Policy, Workspace, System, Derived }
pub enum ConstraintStrength { Hard, Preference }
```

Hard constraints participate in IR validation and completion gating. Model output cannot weaken them.

---

## 5. Execution IR

All scheduler-visible work must exist as validated IR first.

```rust
pub struct ExecutionGraph {
    pub version: u16,
    pub nodes: BTreeMap<NodeId, ExecutionNode>,
    pub dependencies: Vec<Dependency>,
}
```

```rust
pub struct ExecutionNode {
    pub id: NodeId,
    pub task_id: TaskId,
    pub planned_revision: u64,
    pub executor: ExecutorKind,
    pub invocation: Invocation,
    pub inputs: Vec<InputBinding>,
    pub expected_outputs: Vec<OutputBinding>,
    pub access: AccessSet,
    pub resources: ResourceClaim,
    pub effect_class: EffectClass,
    pub idempotency: Idempotency,
    pub speculation: SpeculationPolicy,
    pub timeout: TimeoutPolicy,
    pub retry: RetryPolicy,
    pub cancellation: CancellationPolicy,
    pub verification: Vec<VerificationRequirement>,
    pub priority: NodePriority,
}
```

```rust
pub enum ExecutorKind {
    Native,
    Repository,
    Retrieval,
    Tool,
    Judgment,
    Model,
    Mutation,
    Verification,
    Barrier,
}
```

Invocation uses capability IDs plus schema-validated JSON to keep the IR extensible:

```rust
pub struct Invocation {
    pub capability: CapabilityId,
    pub args: serde_json::Value,
}
```

Provider-native tool calls are only proposed Invocations; they do not bypass compilation/policy/scheduling.

---

## 6. IR validation

Before graph commit, validate:

- all NodeIds unique;
- graph acyclic;
- dependency references exist;
- input bindings only reference dependency ancestors;
- capability exists and arguments satisfy schema;
- effects/idempotency fields are present;
- access/resource declarations satisfy capability minimums;
- hard task constraints are not obviously violated;
- speculative nodes satisfy speculation policy;
- commit barriers exist where policy requires them.

Invalid model-proposed IR is rejected and packaged as structured failure evidence rather than executed.

---

## 7. Node state machine

```rust
pub enum NodeStatus {
    Pending,
    Ready,
    Running,
    Prepared,
    WaitingApproval,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
    UnknownAfterCrash,
}
```

A node enters `Prepared` when all work before an effect commit barrier is done but the consequential effect has not yet committed.

---

## 8. Dependencies and dataflow

```rust
pub struct Dependency {
    pub from: NodeId,
    pub to: NodeId,
    pub condition: DependencyCondition,
}

pub enum DependencyCondition { OnSuccess, OnFailure, OnCompletion }
```

Keep branching explicit. Do not add arbitrary executable predicates into the scheduler for MVP.

Structured outputs may feed child inputs via JSON pointers. Hidden implicit data dependencies are prohibited.

---

## 9. Access sets and hierarchical conflicts

```rust
pub struct AccessSet {
    pub reads: Vec<ResourceKey>,
    pub writes: Vec<ResourceKey>,
}
```

Resource examples:

```text
file:/workspace/src/auth.rs
dir:/workspace/src/**
git:index
git:refs/heads/main
db:local/users
remote:github/repo/example
browser:session/123
```

Conflict rule:

- read/read compatible;
- read/write conflict;
- write/read conflict;
- write/write conflict.

Resource matching must understand hierarchy. `dir:/workspace/src/**` overlaps `file:/workspace/src/auth.rs`.

Implement a normalized resource-key matcher. For filesystem resources, use canonical workspace-relative path components and prefix/descendant semantics rather than raw string prefix checks.

All access claims are granted atomically before node launch; nodes do not incrementally acquire locks. This removes normal hold-and-wait deadlocks.

---

## 10. Resource claims

Access conflicts model correctness. Resource claims model capacity.

```rust
pub struct ResourceClaim {
    pub cpu_units: u16,
    pub memory_mb: Option<u32>,
    pub process_slots: u8,
    pub network_slots: u8,
    pub gpu_memory_mb: Option<u32>,
    pub provider: Option<ProviderId>,
}
```

Claims are initial estimates. Persist aggregate actual durations/resource pressure so defaults can be calibrated later.

---

## 11. Scheduler

Scheduler responsibilities:

- maintain DAG readiness;
- validate dependency conditions;
- enforce access conflicts;
- enforce resource budgets;
- calculate priorities/critical-path estimates;
- dispatch and receive executor completions;
- favor committed critical-path work over speculative work;
- apply cancellation commands.

Scheduler does not directly persist SQL, call model APIs or implement tools.

Conceptual loop:

```rust
loop {
    tokio::select! {
        Some(command) = command_rx.recv() => apply(command),
        Some(joined) = running.join_next() => process(joined),
        _ = scheduler_tick() => {}
    }

    recompute_ready();
    recompute_priorities();
    while let Some(node) = next_dispatchable() {
        dispatch(node);
    }
}
```

Use bounded queues.

---

## 12. Structured concurrency/cancellation

Each Task Supervisor owns a root cancellation token and execution task collection. Subscopes use child tokens.

```text
Task
├── investigation scope
├── reasoning scope
├── mutation scope
└── verification scope
```

Cancellation policy:

```rust
pub enum CancellationPolicy {
    Immediate,
    Graceful { grace_ms: u64 },
    NonCancellableAfterCommit,
}
```

Cancellation is cooperative at the executor boundary. The Task Supervisor
cancels the relevant token but does not abort or detach a running executor
future. An executor returns only after work it started has stopped or its
effect outcome has been reconciled. The scheduler retains that node's access
and resource grants until the future returns, and a task-cancel reply waits
for every running node to drain. A timeout deadline follows the same rule: it
signals cancellation and waits for settlement; a known successful outcome is
recorded as success even if it arrives after the deadline. If an executor
reports `Cancelled` only when it proves that no consequential effect occurred.
If the executor cannot determine whether an effect occurred, or panics or is
aborted without a settled result, the node becomes `UnknownAfterCrash`; its
grant stays held, automatic retry is forbidden, and wait/cancel returns an
unknown outcome. The Supervisor journals verification interruption before
releasing its owned worker, so recovery requires reconciliation instead of
blindly replaying a check. Generic scheduler users must provide an equivalent
durable handoff; the scheduler's in-memory grant alone does not survive process
termination. When any node becomes unknown, the scheduler cancels that task's
scope, marks not-yet-running nodes cancelled, and asks running siblings to
drain under their existing grants; no later node is dispatched after the
unknown outcome is observed.
If an executor never returns, cancellation and shutdown can wait indefinitely;
forced termination is unsafe until that executor has an owned kill-and-reap
protocol for its effects and child processes.

Blocking work must not occupy async executor threads. Use dedicated process execution or `spawn_blocking` for genuinely blocking in-process operations.

External HTTP cancellation is best-effort; a dropped request does not prove the remote operation did not occur. Effect semantics still govern recovery.

---

## 13. Scheduler priority

Initial priority is heuristic, not ML:

```text
score =
  critical_path_weight
+ explicit_priority
+ age_bonus
- speculation_penalty
- resource_pressure_penalty
```

Predict node durations using EWMA by capability/environment. Calculate remaining critical-path estimates by reverse DAG traversal.

Do not maximize concurrency blindly. Four CPU-heavy compilers can be slower than one.

---

## 14. Speculative execution

```rust
pub enum SpeculationPolicy { Forbidden, Allowed, Preferred }
```

MVP speculation is only allowed for operations that are pure/read-only, cheap, cancellable and policy-approved.

No speculative mutation in MVP.

When capacity is constrained, cancel/avoid speculative nodes before critical-path nodes.

---

## 15. Task Supervisor actor

Task Supervisor owns canonical task state and receives a bounded command mailbox.

```rust
pub enum TaskCommand {
    AddUserMessage(...),
    AddConstraint(...),
    Pause,
    Resume,
    Cancel,
    Approve(...),
    Deny(...),
    NodeCompleted(...),
    ProviderEvent(...),
}
```

Initial mailbox capacity: 256. Backpressure is preferable to unbounded growth.

Responsibilities:

- task state transitions;
- revisioning/steering;
- initial routing and escalation;
- graph commit/update;
- persistence coordination;
- scheduler commands;
- approvals;
- completion/verification gates;
- crash recovery coordination.

---

## 16. Event separation

Do not use one event bus for everything.

Separate:

- **commands**: control inputs to owners;
- **durable events**: journalled state transitions;
- **ephemeral UI events**: progress/streaming that may be dropped;
- **telemetry**: measurements exported asynchronously.

Durable events are versioned.

```rust
pub struct EventEnvelope<T> {
    pub seq: i64,
    pub event_id: EventId,
    pub schema_version: u16,
    pub task_id: TaskId,
    pub timestamp: Timestamp,
    pub event: T,
}
```

`seq` is a reconnect/replay cursor.

---

## 17. Persistence model

Use two SQLite classes:

```text
state.db                 correctness-critical runtime data
index/<workspace>.db     rebuildable repository index
```

`state.db` settings:

```text
journal_mode=WAL
synchronous=FULL
foreign_keys=ON
busy_timeout=5000
```

Index DB settings:

```text
journal_mode=WAL
synchronous=NORMAL
foreign_keys=ON
```

All state DB writes flow through one logical asynchronous `StoreWriter`.

Critical transitions commit immediately. Normal journal updates may be microbatched initially up to 5 ms or 128 operations, whichever arrives first; benchmark and tune this.

---

## 18. Journal + snapshots

Use an append-only journal as recovery truth plus periodic state snapshots for fast startup.

Core tables:

- sessions;
- tasks (metadata + latest snapshot + snapshot_seq);
- task_events;
- nodes;
- node_dependencies;
- effects;
- approvals;
- artifacts;
- provider_stats.

A snapshot never replaces journal history needed for unresolved effects.

Snapshot policy may initially trigger every 100 durable task events or at terminal task states. Tune after measuring replay cost.

---

## 19. Effect/idempotency semantics

Effect classes:

```rust
pub enum EffectClass {
    Pure,
    ReadOnlyLocal,
    ReversibleLocalMutation,
    DestructiveLocalMutation,
    ReversibleExternalMutation,
    DestructiveExternalMutation,
    Privileged,
}
```

Idempotency:

```rust
pub enum Idempotency {
    Pure,
    Idempotent,
    Keyed,
    Queryable,
    Compensatable,
    NonIdempotent,
    Unknown,
}
```

Automatic retries are safe only when semantics explicitly allow them.

External/destructive protocol:

```text
exact operation prepared
→ policy/approval
→ EffectPrepared durable commit
→ consequential action
→ receipt/query result
→ EffectCommitted durable commit
```

If crash occurs after remote success but before local commit:

- `Keyed`: retry/query using same idempotency key;
- `Queryable`: inspect remote state;
- `Compensatable`: use declared recovery path;
- `NonIdempotent`/`Unknown`: mark UnknownAfterCrash and require safe reconciliation; never blindly replay.

---

## 20. Mutation batches

Filesystem multi-file mutation is **recoverable**, not globally atomic.

For each changed file record:

- normalized path;
- preimage BLAKE3;
- preimage artifact ID;
- intended postimage BLAKE3;
- prepared temp path;
- per-file commit state.

Procedure:

1. verify all current preimage hashes;
2. create postimages/temp files;
3. persist `MutationPrepared` and file plan;
4. rename/replace individual files;
5. persist each committed file;
6. persist batch completion;
7. run verification.

Recovery can detect which files committed and either finish a still-valid batch or restore preimages.

A mismatched base hash returns `StalePreimage`; never silently patch different content.

---

## 21. Artifact store

Do not place large stdout/model/source blobs in event rows.

Content-addressed layout:

```text
$data/artifacts/<first-two-hash-chars>/<blake3>
```

Store metadata in SQLite. Candidate compression threshold: 64 KiB using zstd. Compression is an optimization, not an invariant.

---

## 22. Router

Router output:

```rust
pub struct RoutePlan {
    pub class: RouteClass,
    pub initial_nodes: Vec<ExecutionNode>,
    pub speculative_nodes: Vec<ExecutionNode>,
    pub escalation: EscalationPolicy,
}

pub enum RouteClass {
    DirectNative,
    EvidenceFirst,
    JudgmentFirst,
    ReasoningFirst,
    Hybrid,
}
```

Stages:

1. explicit CLI/subcommand handling;
2. deterministic intent rules;
3. workspace-aware capability matching;
4. historical latency/success estimates;
5. bounded judgment only if classification remains ambiguous.

Large LLM inference must not normally be needed just to decide whether to invoke a large LLM.

### DirectNative examples

- “Where is Foo defined?”
- “Show git status.”
- “Find references to Bar.”
- “Run the tests.”

### EvidenceFirst examples

- “Why is this test failing?”
- “What changed around token refresh?”

### ReasoningFirst examples

- “Redesign this subsystem.”
- “Find the root cause of this intermittent race and fix it.”

ReasoningFirst still starts cheap local evidence immediately.

---

## 23. Evidence grace window

For likely reasoning tasks, launch highly probable cheap evidence operations first and allow a tiny configurable grace window before model context is sealed.

Initial candidate: 75 ms.

This is not a permanent magic number. Record evidence-arrival distributions and optimize p50/p95 end-to-end task latency.

Do not wait hundreds of milliseconds for optional evidence before starting a multi-second reasoning call.

---

## 24. JudgmentProvider/OpenJEV

Core interface is provider-neutral:

```rust
pub trait JudgmentProvider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn capabilities(&self) -> JudgmentCapabilities;
    fn estimate(&self, request: &JudgmentRequest) -> ProviderEstimate;
    fn judge<'a>(
        &'a self,
        request: JudgmentRequest,
    ) -> BoxFuture<'a, Result<JudgmentResponse, JudgmentError>>;
}
```

Internal judgment forms:

- Boolean/Noul-like;
- Choice;
- Score.

Batch multiple judgments sharing the same state.

OpenJEV is one adapter. Tachyon must compile/run without that feature/adapter.

A Jev failure routes around the provider rather than failing the harness.

Do not use Jev where native code can answer exactly.

---

## 25. ModelProvider

```rust
pub trait ModelProvider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn capabilities(&self) -> ModelCapabilities;
    fn estimate(&self, request: &ModelRequest) -> ProviderEstimate;
    fn invoke<'a>(
        &'a self,
        request: ModelRequest,
        sink: ModelEventSink,
    ) -> BoxFuture<'a, Result<ModelResult, ModelError>>;
}
```

Capabilities include:

- streaming;
- structured output;
- tool-call encoding;
- vision;
- prompt caching;
- context window;
- reasoning controls;
- latency/cost metadata.

Core routing selects capabilities/roles, never provider names.

Configuration may map roles such as `fast`, `primary`, `specialist`, `vision` to any compatible provider/model.

---

## 26. Structured model decisions

Preferred model output contract:

```rust
pub enum AgentDecision {
    Respond { message: String },
    RequestEvidence { requests: Vec<CapabilityRequest> },
    ProposeExecution { operations: Vec<ProposedOperation> },
    NeedUserInput { question: String },
    Complete { summary: String },
}
```

`ProposeExecution` is compiled to IR; it is never executed raw.

If a provider lacks native structured output, use robust parsing/repair at the adapter boundary; malformed output is a provider result failure, not permission to execute text heuristically.

---

## 27. Context assembly/trust

Use typed blocks:

```rust
pub struct ContextBlock {
    pub kind: ContextKind,
    pub provenance: Provenance,
    pub trust: TrustLevel,
    pub content: String,
    pub priority: u16,
}
```

Trust classes:

```rust
pub enum TrustLevel {
    System,
    User,
    WorkspaceTrusted,
    WorkspaceData,
    ExternalUntrusted,
}
```

Repository files and retrieved external text are data, not authority.

Context reduction order:

1. remove duplicate evidence;
2. collapse repeated diagnostics;
3. prefer relevant symbol excerpts/hunks to whole files;
4. retain provenance/hash/version;
5. reserve output/tool schema budget;
6. deterministic truncation of lowest-priority blocks;
7. only then consider semantic summarization if necessary.

---

## 28. Capability registry

```rust
pub struct CapabilityDescriptor {
    pub id: CapabilityId,
    pub description: String,
    pub input_schema: JsonSchema,
    pub output_schema: JsonSchema,
    pub executor: ExecutorKind,
    pub minimum_access: AccessTemplate,
    pub effect_class: EffectClass,
    pub idempotency: Idempotency,
    pub trust: ToolTrust,
    pub resource_profile: ResourceProfile,
}
```

MVP built-ins:

- `fs.read`, `fs.list`, `fs.metadata`;
- `repo.lexical.search`, `repo.symbol.search`, `repo.symbol.references`, `repo.file.symbols`;
- `git.status`, `git.diff`, `git.log`;
- `process.exec`;
- `mutation.patch`;
- `verify.command`.

Tool descriptions are indexed once; do not inject the complete registry into every prompt.

---

## 29. Process execution

Prefer direct program + argv execution:

```rust
pub struct CommandSpec {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub env: BTreeMap<OsString, OsString>,
    pub stdin: StdinSpec,
    pub timeout: Duration,
    pub output: OutputPolicy,
}
```

Arbitrary `shell.exec` is a separate broad capability. It receives conservative access/effect declarations because shell strings obscure actual operations.

Process execution returns only after the owned tree has no live members and
the immediate child is reaped:

- Unix: a new session and process group, graceful signal, force kill, then wait
  for live group members to exit before reaping the leader;
- Windows: Job Object termination, leader reap, then wait until the job reports
  no active processes.

Linux process execution requires a readable procfs process table and the
runner's own `/proc/<pid>/stat` entry. Before spawning, the runner checks both.
Cleanup scans visible PID entries to distinguish live members from zombies;
entries that disappear or are permission denied are skipped. Each child starts
a new session, so unrelated processes cannot join its process group; descendants
that deliberately change identity or session can escape this boundary and are
outside the process-group containment guarantee. The liveness check may scan
`/proc` while a zombie leader still holds the process-group ID, so process-tree
cleanup latency depends on the number of visible processes; measure that cost
in the process-runner benchmark.

Panics after spawn must run the same awaited cleanup before unwinding to the
worker owner. Dropping or aborting the process future can only request
best-effort termination; callers that need workspace exclusion must keep an
owner alive through the drain. Process groups and Job Objects do not protect
against descendants that deliberately escape their ownership boundary.

Capture stdout and stderr concurrently to avoid pipe deadlock. Use bounded in-memory chunks plus artifact spooling for large output.

---

## 30. Repository intelligence

Per workspace maintain:

- normalized file inventory;
- size/mtime hints;
- BLAKE3 content hash;
- language;
- lexical search;
- AST/symbol/reference index;
- index generation/freshness.

Tree-sitter is the initial parser strategy for supported languages. Unsupported languages fall back to lexical search.

Filesystem watchers are incremental invalidation hints, not authoritative truth. Content hashes establish identity.

Before applying mutations based on evidence, re-check relevant current hashes.

---

## 31. Evidence

```rust
pub struct EvidenceItem {
    pub id: Uuid,
    pub kind: EvidenceKind,
    pub content: String,
    pub provenance: Provenance,
    pub source_version: Option<String>,
    pub relevance: Option<f32>,
    pub created_at: Timestamp,
}
```

```rust
pub struct EvidencePackage {
    pub question: String,
    pub findings: Vec<EvidenceItem>,
    pub contradictions: Vec<EvidenceItem>,
    pub gaps: Vec<EvidenceGap>,
}
```

Evidence provenance must survive ranking/merging so model output can be traced back to source files/results.

---

## 32. Verification/acceptance

Mutation tasks should have an `AcceptanceContract` containing machine-checkable clauses where possible.

Example clause kinds:

- command passes;
- file/path unchanged;
- changed paths limited to scope;
- no new diagnostics;
- symbol present/absent;
- hard user constraint;
- semantic requirement.

Verification selection considers changed files, project metadata, task risk and acceptance contract.

Prefer affected checks first, then expand based on risk. Full-suite execution is not automatically required after every change.

A task becomes `Completed` only when all required work is terminal, acceptance passes, no unresolved dangerous effects remain and no hard constraint is violated.

Model statements such as “done” are non-authoritative.

---

## 33. Policy/capabilities

Policy unit is an explicit capability/resource scope, not a command deny-list.

Examples:

```text
fs.read:workspace/**
fs.write:workspace/src/**
process.spawn:cargo
process.spawn:git
network.connect:api.example.com
credential.use:github
```

Policy result:

```rust
pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
    Ask { request: ApprovalRequest },
}
```

Project-trusted defaults may allow normal reads/writes/search/build/test/git-read operations within workspace.

Ask by default for workspace escape, credentials, privileged execution, deployment, destructive external operations and dangerous Git writes.

Approvals bind to an exact operation/scope hash. Any material operation change invalidates the approval.

---

## 34. Filesystem containment

Before policy decisions:

- resolve workspace-relative components;
- reject path traversal;
- normalize path separators/platform semantics;
- inspect/canonicalize existing symlink components;
- verify resolved targets remain inside granted roots.

Never implement containment as raw string-prefix comparison.

---

## 35. Credential broker

Runtime/task state stores credential handles, not secret values.

Raw credentials must not normally enter:

- model prompts;
- durable task events;
- TUI messages;
- telemetry attributes;
- artifact logs.

Execution adapters request a handle and inject the secret only at the boundary. Known secret material is registered with redaction filters for process/provider output.

---

## 36. Gateway

Gateway is a transport adapter over core commands/events.

Local default:

- Unix/macOS: Unix domain socket;
- Windows: named pipe.

Remote mode is separate, opt-in, authenticated and encrypted. Never bind a remote TCP listener merely because local gateway starts.

Local protocol begins as versioned length-prefixed JSON. Binary encoding is deferred until profiling proves serialization overhead matters.

Commands include create/list/get task/session, send message, pause/resume/cancel, approve/deny, subscribe and artifact retrieval.

Each durable event has sequence cursor. Per-subscriber queues are bounded. Slow clients receive `ResyncRequired`; ephemeral progress may drop, durable state is replayed from the journal.

---

## 37. Local gateway security

At minimum:

- create runtime directory with user-only permissions where platform supports it;
- create socket/pipe for same-user access;
- store endpoint metadata separately from secrets;
- validate expected peer/user identity where platform APIs permit;
- never trust a stale lock file alone to prove a daemon exists;
- use connect/probe + PID/start metadata to recover stale gateway locks.

---

## 38. CLI/TUI

`tachyon-app` supplies commands:

```text
tachyon
tachyon run
tachyon ps
tachyon attach
tachyon pause
tachyon resume
tachyon cancel
tachyon gateway
tachyon doctor
tachyon trace
tachyon config
```

Non-interactive output supports human, JSON, JSONL and quiet modes.

TUI is a pure gateway client. It has no direct SQL/provider/tool access. Input, gateway-event reader and render/update paths are independent. Render only when state changes or on bounded animation ticks (candidate max 30 FPS).

User steering updates task state immediately; do not wait for a model call to “understand” that a hard new constraint exists.

---

## 39. Telemetry

Use Rust `tracing` internally. Span hierarchy:

```text
tachyon.task
├── tachyon.route
├── tachyon.node
│   ├── tachyon.tool
│   ├── tachyon.model
│   └── tachyon.judgment
└── tachyon.verify
```

Optional OpenTelemetry export is an adapter and cannot participate in correctness. Never synchronously export network telemetry on the execution critical path.

Do not export source code, prompts, raw model output or credentials by default.

Persist lightweight aggregate routing/provider measurements separately from raw traces.

---

## 40. Error taxonomy/retries

Core error classes include:

- invalid input;
- policy denied/approval required;
- cancelled/timeout;
- provider unavailable/rejected;
- tool/process failure;
- conflict;
- stale preimage;
- verification failure;
- storage/protocol failure;
- unknown effect after crash;
- corrupt state/internal invariant.

Every failure exposes retryability metadata and structured diagnostics.

IR owns retry policy. No hidden infinite retries.

IR idempotency metadata is not retry authority by itself. At scheduler ingress,
the registered trusted executor must approve retry safety for the exact node;
the default is to reject automatic retries. Capability-specific recovery is
required before allowing keyed, queryable, or compensatable retries.

---

## 41. Crash recovery

On gateway start:

1. load incomplete tasks;
2. reconstruct latest snapshot + journal tail;
3. mark task `Recovering`;
4. classify nodes previously Running/Prepared;
5. reconcile local filesystem/process/external effects;
6. resume safe work or request user intervention.

Recovery examples:

- pure read: rerun;
- model/Jev call with no committed result: rerun;
- local process: assume ended unless platform proves otherwise, rerun if safe;
- patch: inspect hashes/preimage/postimage state;
- keyed/queryable external effect: reconcile;
- unknown/non-idempotent effect: do not replay.

---

## 42. Testing requirements

Unit tests:

- task state transitions;
- graph cycle/dependency validation;
- hierarchical access overlap;
- atomic resource grants;
- router rules;
- policy decisions;
- patch hash validation;
- context budgeting;
- provider schema parsing.

Property test invariant:

> No two concurrently Running nodes have conflicting access sets.

Fault-injection integration tests must crash/restart around:

- journal commit;
- EffectPrepared;
- remote effect return;
- EffectCommitted;
- each multi-file mutation commit;
- verification;
- approval wait.

Use FakeModelProvider/FakeJudgmentProvider/FakeToolExecutor for deterministic CI. Core tests must not require paid APIs.

Security tests cover `..`, symlink escape, changed-operation approval reuse, model-requested undeclared capabilities, secret redaction and accidental remote gateway exposure.

---

## 43. Performance targets

Initial engineering targets on a normal local developer machine:

- deterministic router path: <2 ms p95;
- scheduler dispatch overhead: <1 ms p95 excluding executor work;
- local gateway command: <5 ms p95;
- first visible task event: <50 ms p95;
- warmed simple symbol/reference request: <250 ms p50, <500 ms p95.

These are internal gates, not guaranteed public claims. Measure on representative repositories and hardware.

---

## 44. Benchmark modes

Every important task can run under:

- `tachyon-full`;
- `tachyon-no-speculation`;
- `tachyon-no-judgment`;
- `tachyon-serial`;
- reference model→tool loop.

Where practical compare external harnesses using identical model/environment/task.

Record verified success, wall time, time-to-first-useful-result/edit/verification, model/Jev/tool calls, tokens, cost, critical-path time, discarded speculation and user intervention. Report median and p95, not just averages.

---

## 45. MVP exit conditions

MVP is not complete until all are true:

- CLI and TUI are usable gateway clients;
- local gateway persists across client disconnects;
- simple repository questions commonly use zero LLM calls;
- complex tasks start evidence work in parallel;
- model and judgment providers are replaceable;
- tasks recover after process restart;
- local mutation batches recover safely;
- workspace containment survives traversal/symlink tests;
- verification gates completion;
- benchmarks report p50/p95 and verified success;
- `tachyon-full` beats the in-tree serial reference on representative tasks without reducing verified success.

---

## 46. Deferred work

Do not implement before MVP exit unless an ADR demonstrates necessity:

- workflow compilation/self-improvement;
- browser/computer use;
- distributed workers;
- large agent swarms;
- mobile/Telegram clients;
- plugin marketplace;
- custom vector database;
- ML-based router/scheduler.

---

## 47. Final implementation rule

When choosing between designs, optimize for:

```text
verified correctness
+ recoverability
+ explicit effects/security
+ minimum critical-path latency
+ minimum time-to-useful-result
+ replaceable providers
```

Tachyon is not a faster prompt loop. It is a fast execution runtime with selective access to model intelligence.
