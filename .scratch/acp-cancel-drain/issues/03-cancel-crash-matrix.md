# 03: Cancel crash matrix + no-resurrection pinning

**What to build:** Executable proof that crash around cancel leaves explainable state. Fault-point tests stop at cancel-intent commit, kill, and assert reconcile: the task stays terminal Cancelled, interrupted effects classify via the §19 matrix (never silent success), MCP parks are expired with no post-restart resurrection of any approval id, and restart never downgrades a task to Cancelled. The existing sync-barrier semantics (`cancel_run.rs:214-219`) keep passing with unmodified intent — regression-guarded, not rewritten.

**Blocked by:** 02 (shares cancel-path changes; sequential tree)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-01)

- [x] Fault-point test: cancel-intent committed then crash -> task terminal Cancelled + interrupted effects §19-classified (mirrors the effect-recovery suite's journal assertions)
- [x] Restart expires MCP launch + call parks; no pre-restart approval id (granted, parked, or consumed) works after restart - typed refusal, nothing launches or executes
- [x] Restart never downgrades a task to Cancelled (extends the `kill_restart.rs:198` precedent to the cancel-adjacent paths)
- [x] `cancel_run.rs` barrier tests pass with unmodified intent (no weakening of the ack-after-drain assertions)
- [x] Tests at the gateway seam (fault kills) + store tests for post-restart park state
