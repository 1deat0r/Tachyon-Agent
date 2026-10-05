# Bounded model proposal retry

The existing patch driver allows one retry after malformed model output. This
policy applies before mutation. It does not retry valid non-execution decisions,
invalid patches, authorization failures, transport failures, or failed checks.
`ModelError::is_retryable` remains unchanged: the general scheduler still cannot
retry malformed output. This is an explicit driver policy for one uncommitted
reasoning stage.

- Why deterministic code cannot solve it: it can reject malformed output, but
  it cannot infer the intended repair. A second model call may produce a valid
  proposal. JSON parsing and proposal validation remain strict.
- Input/output: identical `ModelRequest` on both attempts; `ModelInvocation`
  preserves typed result and reported usage even when output is rejected.
  `ModelCallRecord` carries ordinal, measured latency, usage, a stable error
  code, and a safe malformed-output class. It carries no output text or error
  body. The prompt gives explicit JSON escaping instructions and an example.
- Access set: none for the model call. Evidence and mutation keep their existing
  access sets, workspace pin, and run-held workspace lease.
- Effect class: metered inference; no workspace mutation until a valid proposal
  passes the existing gates.
- Idempotency: a call may repeat only while no valid proposal has been committed.
  The retry can incur another provider charge.
- Resource claim: one provider call at a time and the unchanged output budget.
- Cancellation: the driver drops the in-flight future. It checks cancellation
  before each attempt and after return. Provider progress is not accepted as
  task state. A late result cannot pass the revision-bound Supervisor ack.
- Retry policy: at most two attempts total. Only `MalformedOutput` permits the
  second attempt. Both use one `RuntimeBounds::model_deadline_ms` deadline.
- Verification: `model_retry` tests cover valid recovery, exhaustion, typed
  failures, cancellation, steering during either attempt, rejected paths, and
  stale preimages. Usage tests exercise
  malformed responses and unknown/overflow counters. Existing wrong-patch,
  approval-binding and completion tests remain authoritative.
- Crash recovery: only settled attempts are recorded through Supervisor-owned
  `RunRecord::Stage` events (`model_attempt`). Cancellation before settlement
  can also leave no settled receipt. A crash mid-call may leave an
  uncounted billed call; provider-side billing reconciliation is unavailable.
  Explicit driver re-entry follows ADR-0002 and receives a fresh per-stage
  attempt budget. No consequential effect is retried by this policy.
- Expected latency: one hosted call normally; at most two within the total
  model-stage deadline. Latency, token totals, and failure rate are measured.

Total tokens are present only when every attempt reports that counter and the
sum fits the counter type. Unknown counts never become zero. The live benchmark
also reports known token subtotals and unavailable-usage attempt counts. When no
verified price is configured, monetary cost remains unavailable.

Compatibility: `invoke` remains available for existing callers. The additive
`invoke_observed` method defaults to successful-result usage or unknown failure
usage; adapters can preserve failure usage. Providers never retry internally.
ContextSlice, accepted proposal validation, mutation authorization, approval
binding, and acceptance-gated completion keep their existing authority.
