# 0006 — Supervisor-owned evidence execution

**Status:** accepted · 2026-09-29

**Decision revision:** 2

## Context

The current runtime can collect bounded `fs.read` evidence, and Tachyon has an
Execution IR, scheduler, Supervisor journal, approval policy, and content
addressed artifact spool. Production driver reads do not yet use validated IR
or Supervisor-owned scheduling. Connecting those components safely requires
more than minting the currently test-only validated-graph token: the live path
must bind authorization to the opened target, drain workers before releasing
access, and make successful outputs recoverable.

Three independent interface designs converged on a narrow entry point owned by
the Task Supervisor. They also surfaced unresolved recovery questions: the
current Task state supports one graph with no generation fence, scheduler
outputs are transient, artifact fetch does not verify content identity, and a
graph does not identify the capability contract version used to interpret it.

## Decision

### Scope and ownership

1. The first production integration is limited to typed, read-only `fs.read`
   requests for evidence. It does not expose arbitrary caller-authored
   `ExecutionNode` metadata or a general plugin/extension registry.
2. The driver submits typed requests and the active run/revision fence to an
   internal Task Supervisor command. The Supervisor validates that fence
   against its current Task state, rejecting stale runs or revisions, and
   obtains its own workspace, policy, approval, and artifact dependencies;
   callers cannot supply trusted access/effect/resource metadata or mint
   permits.
3. A trusted compiler lowers each request to `fs.read` Execution IR. Before
   minting the validated-graph proof, it validates the invocation schema,
   hard constraints, access/resource minimums, read-only effect and
   idempotency, cancellation, retry, and output declarations. The proof
   constructor remains private to production validation.

### Target identity and authorization

4. Validate the configured maximum evidence-request count and path-size
   bounds before opening any path or allocating graph nodes. Resolve and open
   each requested path beneath the pinned workspace root. Represent the
   prepared target as a non-serializable value tying the held root and file
   handles to the canonical workspace-relative key and the file identity
   obtained from the opened handle. In-root symlinks may be followed only
   when the platform adapter proves that this opened object remains beneath
   the pinned root; symlink/junction escapes, traversal, and unsupported path
   forms fail closed. Prove the opened target is a regular file without
   blocking on special files; reject directories, devices, pipes, and other
   non-regular files.
5. Derive the IR access key, policy scope, approval operation hash, and
   evidence provenance from that same canonical key. Authorize the exact
   operation before reading. Consume a private, non-cloneable, one-shot permit
   bound to task, run, revision, generation, node, capability version, pinned
   root identity, opened-file identity, canonical key, and effect. Keep the
   verified handles through approval; never reopen or re-resolve the path
   after authorization. If the platform cannot prove that the authorized key
   and permit refer to the exact opened object, fail closed.
6. Enforce the configured stage byte limit across all concurrently scheduled
   evidence nodes with one shared, linearizable budget. Each bounded read
   chunk must reserve its maximum size atomically before reading; unused
   reservation is returned after a short read or EOF. No interleaving may
   allow reserved bytes to exceed the stage limit. On exhaustion, stop further
   dispatch, cancel sibling reads, drain every worker, discard uncommitted
   partial outputs, and return no partial evidence batch. Metadata may reject
   an obviously oversized file early, but cannot enforce the cap.
7. Bind every read worker to the Supervisor's cancellation token and fence.
   The Supervisor signals in-flight workers when cancellation or a revision
   change is accepted. Check cancellation before each chunk and before
   publishing a result; a chunk already in progress may finish, but no next
   chunk may start after cancellation is observed. If cancellation or a
   revision change is accepted first, a late result cannot journal success.
   Keep each access/resource grant and workspace lease until the worker has
   actually drained. Approval parking, failure, cancellation, and shutdown
   stop further dispatch and signal and join all Supervisor-owned workers
   before acknowledging settlement.

### Generations, outputs, and recovery

8. Assign every accepted graph a Task-wide monotonically increasing generation.
   In one durable Supervisor journal transaction, allocate the generation and
   advance its persisted counter, install the graph and node states, set the
   active-generation pointer, and append the acceptance event. Do not dispatch
   until this transaction is acknowledged. Bind every command/result to task,
   run, revision, generation, node, and attempt. Only one generation for a
   Task may be active. Stale callbacks are rejected before journaling.
9. Retire a generation only after its workers drain and all nodes are
   terminal. If recovery finds an unsettled read-only evidence generation,
   atomically journal a generation-interrupted event, mark every unfinished
   node `Cancelled` for that generation, and clear its active pointer. Keep
   already committed receipts attached to the old generation for audit, but do
   not reuse them in a new stage. Explicit Supervisor re-entry recompiles and
   reauthorizes the full request set as a new generation. This classification
   applies only to the effect-free `fs.read` slice; existing
   `UnknownAfterCrash` rules remain authoritative for nodes with consequential
   effects. Keep prior generation events in the journal, and preserve the
   active generation and monotonic counter in snapshots.
10. Persist a capability contract version with every invocation,
    independently of the ExecutionGraph schema version. Recovery never
    reinterprets an invocation under newer semantics. A pending node can run
    only when its exact executor contract version is available; otherwise fail
    closed and require an explicit compatible migration or replan. A committed
    success receipt may be consumed as its recorded output after integrity
    verification, but is never recomputed under a newer executor version.
11. Store successful `fs.read` bytes in the content-addressed artifact spool
    before acknowledging node success. The artifact adapter must durably flush
    the payload, atomically publish its content-addressed name, and synchronize
    the containing directory (or use a documented platform-equivalent) before
    the journal can refer to it. If the platform cannot establish that
    durability contract, it must not report node success. In one journal
    transaction, record node success together with a receipt containing the
    ArtifactId, byte length, canonical path, and existing evidence-freshness
    hash. Do not put source bytes in task events. An artifact stored before a
    failed journal write is an unreferenced orphan and is safe to collect only
    after reference-aware retention checks.
12. Fetch validates the ArtifactId format, performs bounded decompression/read,
    verifies BLAKE3 of the resulting bytes, and checks the receipt length
    before evidence reaches the model. Missing, corrupt, mismatched, or
    unsupported-version artifacts fail closed; never substitute transient
    scheduler memory or pass unverified bytes onward. The evidence freshness
    hash remains distinct from the BLAKE3 content identity used by ArtifactId.
13. Evidence artifacts are private to the local data-root owner: spool
    directories and files use owner-only permissions/ACLs on each supported
    platform, and the spool is not exposed as a raw path to clients. This
    slice does not encrypt artifacts at rest; the local OS account remains the
    trust boundary. Artifact retrieval is Supervisor-mediated and requires a
    durable receipt belonging to the requesting Task, generation, and pinned
    workspace; an ArtifactId alone grants no access. Retain bytes while any
    recoverable or retained journal receipt references them. Do not apply
    age-based deletion in this slice. A later task-history deletion/retention
    operation must remove or expire receipts before reference-aware garbage
    collection may delete the blobs. Unreferenced artifacts left by failed
    journal writes may be collected after a reference scan.
14. A crash before a success receipt leaves no usable output. Recovery of an
    unsettled `fs.read` generation records the interruption as described
    above; explicit Supervisor re-entry reads under a newly validated and
    authorized generation and does not reuse an earlier permit. A committed
    success receipt is usable only after its artifact is re-fetched and
    verified. Existing unknown-outcome rules for consequential effects are
    unchanged.
15. An evidence node can never complete a Task. The Supervisor's existing
    acceptance/verification path remains the only completion gate, and its
    completion checks must include the active execution generation's node
    states.

## Alternatives considered

1. **Keep the driver as the production reader and attach metadata afterward**
   — rejected because the policy target, access claim, and opened file can
   diverge, and the Supervisor cannot own cancellation or durable output.
2. **Expose arbitrary graphs or a dynamic capability registry in the first
   slice** — deferred. Those abstractions are not needed to execute the one
   existing evidence capability and would enlarge the validation and recovery
   surface before its contracts are proven.
3. **Keep node output only in scheduler memory** — rejected because recovery
   could not prove which bytes a succeeded node supplied.
4. **Replace a graph in place or retry an interrupted read within its old
   generation** — rejected because late results could update newer state and
   retries would be indistinguishable in the durable record.
5. **Authorize a normalized request path, then read by pathname after the
   check** — rejected because path replacement and symlink resolution can make
   the authorized scope differ from the opened target.

## Evidence

- `docs/02_IMPLEMENTATION_SPEC.md` §§5–7, 12, 17–18, 32–34, 41, and 46:
  validated IR, cancellation drain, journaling, filesystem containment,
  acceptance, recovery, and deferred scope.
- `docs/06_SECURITY_AND_RECOVERY.md`: the production graph path is currently
  an internal persistence/recovery seam; trusted validation, exact-operation
  authorization, and Supervisor-owned worker drain are prerequisites to
  dispatch.
- `crates/tachyon-core/src/lib.rs`: the validated graph has no production
  constructor, task state holds a single graph, and graph installation resets
  node status without a generation/output receipt.
- `crates/tachyon-core/src/runtime.rs` and `driver.rs`: current evidence
  collection authorizes a resolved scope but later reads by pathname; stage
  collection invokes per-request limits rather than one shared concurrent
  budget.
- `crates/tachyon-scheduler/src/scheduler.rs`: completed output is not part
  of the public run snapshot/hydration API.
- `crates/tachyon-tools/src/artifact.rs`: the spool is content addressed, but
  fetch does not verify bytes against the requested ID and store does not yet
  establish the durability contract required before journal success.
- Three independent interface reviews on 2026-09-29 agreed on the
  Supervisor-owned typed evidence seam and identified generation, output
  receipt, artifact verification, path binding, and re-entry as the critical
  decisions.

## Consequences

- The first implementation changes the core journal/state model, scheduler
  result handoff, artifact durability/verification, and the evidence reader;
  those pieces must land as one coherent verified capability slice.
- A trusted, static `fs.read` compiler is sufficient. Dynamic plugins and
  other capabilities remain outside this decision.
- Platform adapters must fail closed when they cannot establish the opened
  target's containment or artifact durability. Cross-platform verification is
  required before the capability is enabled on that platform.
- Recovery can explain exactly which validated capability version produced
  each artifact and can reject stale results without losing journal history.

## Migration/rollback plan

Production graph dispatch stays disabled until the contract is implemented
end to end. Bump the serialized IR schema for the capability contract version;
legacy active graphs without a version or generation must not be executed by
inference. Require a fresh validated generation for them, preserving the old
journal as history. Rollback disables the `fs.read` IR dispatch while retaining
the journal and content-addressed artifacts; do not delete artifacts still
referenced by durable receipts.

## Expert review and adjudication

### Round 1 — 2026-09-29

- Architecture review: **BUILD**, no blockers.
- Recovery review: **CONDITIONAL**. R1 accepted: recovery now journals an
  interrupted generation and terminalizes unfinished effect-free reads
  before explicit re-entry; consequential effects retain existing unknown
  outcome semantics. R2 accepted: graph acceptance, generation allocation,
  active pointer, and durable counter now commit atomically before dispatch.
- Security review: **CONDITIONAL**. S1 accepted: bind the one-shot permit to
  pinned-root and opened-object identities, not only a path string. S2
  accepted: signal in-flight readers and check cancellation at bounded chunk
  boundaries before publishing. S3 accepted: use atomic chunk reservations,
  fail the whole batch on exhaustion, and bound request/path counts before
  opening. S4 accepted: scope artifact retrieval to durable Task receipts,
  keep the local spool private, and retain referenced bytes until the
  corresponding history reference expires.

### Round 2 — 2026-09-29

All three reviewers returned **BUILD** after the revision. Their
verify-by-quote checks confirmed:

- Architecture: the accepted scope remains typed, read-only `fs.read`, and
  trusted compilation stays inside the Task Supervisor (§“Scope and ownership,”
  lines 27–40).
- Recovery: “In one durable Supervisor journal transaction, allocate the
  generation and advance its persisted counter, install the graph and node
  states, set the active-generation pointer, and append the acceptance event”;
  interrupted reads close before re-entry into a new generation (§“Generations,
  outputs, and recovery,” lines 84–101).
- Security: the prepared target ties “the held root and file handles to the
  canonical workspace-relative key and the file identity obtained from the
  opened handle”; the permit binds root and opened-file identity (lines 46–62).
  In-flight reads are signaled and bounded by cancellation checks (lines 71–80),
  and byte reservations are linearizable and bounded (lines 63–70). Artifact
  retrieval requires a Task/generation/workspace receipt and has owner-only
  storage plus reference-aware retention (lines 126–137).

There are no open review blockers. Production implementation remains gated on
meeting this contract and its local verification requirements.
