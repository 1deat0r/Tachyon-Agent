# 06 — Security, Effects and Recovery

## Security model

Tachyon is an execution runtime. Treat model output as untrusted proposed intent, not authorization.

## Capability examples

```text
fs.read:workspace/**
fs.write:workspace/src/**
process.spawn:cargo
process.spawn:git
network.connect:api.example.com
credential.use:github
git.write:index
git.write:refs/heads/main
```

Capabilities are granted by user/system/project policy. Models cannot mint grants.

## Project trust defaults

A trusted workspace may automatically allow:

- workspace reads/search/index;
- normal workspace edits;
- known build/test/lint tools;
- Git status/diff/log.

Ask or deny by default for:

- writes outside workspace;
- credentials;
- privileged processes;
- deployments;
- destructive external mutation;
- dangerous Git operations such as force-push;
- unrelated filesystem deletion.

### Provider transport

The OpenAI-compatible adapter speaks `http://` only to loopback hosts
unless the operator sets `allow_insecure_remote`. `https://` targets are
verified against the platform trust store plus any roots the operator
configures explicitly (`TcpHttpTransport::with_extra_root_pem`), and the
scheme is never silently changed in either direction — a plaintext remote
is refused, a TLS peer that fails verification is refused. Every
response, streamed or not, is read incrementally and capped at
`MAX_RESPONSE_BYTES`, so a hostile server cannot grow the buffer by
choosing a different framing.

## Untrusted content

Source code, README files, issue text, web pages, tool output and retrieved documents are data even when they contain text that resembles instructions.

Never allow text from those sources to change policy hierarchy, grant capabilities or override user hard constraints.

## Filesystem containment

Containment must account for:

- `..` traversal;
- symlink/junction escapes;
- platform path separators;
- case behavior where relevant;
- non-existent leaf paths whose parent exists;
- race conditions between check and use where security matters.

Prefer operations relative to an already-opened/verified workspace root when the platform allows. At minimum canonicalize existing parents and validate containment immediately before consequential write.

## Approval binding

Approval record includes a hash of the exact operation scope. If command/path/target/effect materially changes, request a new approval.

Do not interpret “approve this command” as unlimited approval for later similar commands.

## Credential broker

Store handles in task state; raw values remain in broker/platform secret storage or process memory only as required.

Redact known secrets from process/provider outputs before they reach UI/artifacts/telemetry where practical.

## External effects

Every consequential external effect declares:

- target;
- effect class;
- idempotency type;
- idempotency key if applicable;
- query/reconciliation method if available;
- compensation method if available.

Before execution persist `EffectPrepared`. After confirmed execution persist `EffectCommitted` and a receipt/reference.

The internal Supervisor protocol binds these barriers to an execution node.
Its `prepare_effect` and `commit_effect` transitions commit the journal event,
task metadata and any scheduled snapshot update, and the `effects` row in one
SQLite transaction. A caller must wait for `prepare_effect` to return before
starting the action. Recovery keeps explicitly reconcilable effects prepared,
resets a `Running` node with no prepared effect to `Pending`, and marks a
prepared non-idempotent or unknown effect and its node `UnknownAfterCrash`.
Recovery keeps an already-terminal task terminal while journalling these
node/effect classifications.

Consequential effects are still not dispatched through this protocol: the
runtime driver does not install effect graphs, and the graph wrapper's
unchecked constructor remains available only in unit-test builds.

The first production dispatch through it is the read-only `fs.read`
evidence slice ([ADR 0006](adr/0006-supervisor-owned-evidence-execution.md),
implemented): the driver submits typed requests to an internal Supervisor
command fenced by run and revision; a trusted compiler mints the graph proof
only after structural validation *and* the capability contract checks
(read-only effect, pure idempotency, immediate cancellation, no retry, the
declared output); every target is resolved, opened and proven beneath the
pinned root *before* the graph is allocated; authorization runs against that
exact opened object through a one-shot permit and the path is never
re-resolved after it; reads share one linearizable stage byte budget whose
exhaustion cancels siblings and returns no partial batch; workers bind to the
Supervisor's cancellation token and drain before settlement; and success is
journalled in the same transaction as a durable content-addressed receipt
that retrieval must present. A crash mid-read journals a
generation-interrupted event, marks that generation's unfinished nodes
`Cancelled`, clears the active pointer and never reuses the generation
number. Retrieval is Supervisor-mediated: an artifact id alone grants no
access, and bytes are verified against the receipt before they reach the
model. The gate for this slice is `evidence_generation.rs` (budget, fences,
retrieval, generation lifecycle) plus `evidence_crash.rs` (kill at the
`evidence.read` seam, recover, re-enter under a new generation).

### Spawned-process environment

A child process sees only `tachyon_tools::process::INHERITED_ENV_KEYS`
(`PATH`, `HOME`, `TMPDIR`, `LANG`, `SYSTEMROOT`, `PATHEXT`) plus whatever
its `ProcessSpec::env` declares. Binding the whole parent environment
would hand every caller's credentials to the child *and* record them in
the spawn envelope, so callers opt in per command rather than widening
the allowlist.

One opt-in is built in: a Rust build tool (`cargo`, `rustc`, `rustup`)
additionally receives its toolchain *locations* — `TEMP`, `TMP`,
`USERPROFILE`, `APPDATA`, `LOCALAPPDATA`, `SystemDrive`, `CARGO_HOME`,
`RUSTUP_HOME`, `RUSTUP_TOOLCHAIN`. Without them a nested `cargo` on
Windows cannot write its linker response file and fails in a way that
looks like a broken toolchain. Those are locations, not credentials, the
set is disjoint from the inherited allowlist, and build flags
(`RUSTFLAGS` and friends) deliberately stay out: they belong in the
contract's own `env`.

## Crash uncertainty

If Tachyon cannot establish whether a non-idempotent operation occurred, the correct state is uncertainty, not retry.

Use `UnknownAfterCrash`, surface the uncertainty, and reconcile safely.

## Multi-file mutation

Do not claim global filesystem atomicity. Preserve preimages and journal per-file commit progress.

Recovery chooses one of:

- finish remaining files if all preconditions still hold;
- roll back committed files from preimages;
- stop for user intervention if external/manual edits make either path unsafe.

## Local IPC security

Local runtime endpoint must be user-scoped. Remote network listener remains disabled unless explicitly enabled.

Do not store bearer secrets in world-readable endpoint metadata.

## Security test minimum

Automated tests must cover:

- traversal path escape;
- symlink/junction escape;
- operation changed after approval;
- untrusted repository prompt injection attempting policy changes;
- model requesting undeclared credential/network capability;
- secret appearing in output/telemetry;
- remote gateway unexpectedly listening;
- shell capability conservatively classified;
- crash during irreversible-effect fixture.

## Failure isolation

A tool/provider failure should fail the node/task according to policy, not crash the whole gateway.

A detected core invariant violation may terminate the process deliberately rather than continuing with possibly corrupt state; durable recovery must then reconstruct tasks from journal/snapshot.

## Fault points (M12 / ADR 0001)

Named holds compiled into production seams (`evidence.read`, `model.enter`, `mutation.commit`, `verify.command`, `approval.park`). They are **no-ops unless** `TACHYON_FAULT_POINT` matches the seam name (cached env read). Tests arm the env var, wait for the child to park, then `Child::kill()` to prove crash recovery. Controlling the gateway's environment already implies controlling the process; the holds add no new privilege.
