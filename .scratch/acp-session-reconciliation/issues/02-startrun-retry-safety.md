# 02: StartRun retry-safety contract across restart

**What to build:** A client that sends `StartRun`, loses the response, and retries — including after a gateway restart in the middle of the run — always gets a typed answer and never a second driver spawn for the same task. While a run is active the answer is `run_already_active`; the no-double-spawn guarantee is pinned by tests on both sides of a restart, closing the "do not blindly retry StartRun" half of ADR-0005's reconciliation blocker at the gateway level.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Test-first: retry `StartRun` for a task with an active run → typed `run_already_active`, no second spawn (exists today — pin it as an explicit lost-response retry scenario)
- [ ] Restart-mid-run: start a run, shut the gateway down mid-run, restart, client retries `StartRun` → typed refusal or existing recovery-policy path, **never** a second concurrent driver for the task; assert driver-spawn count / single-active-run invariant
- [ ] If the restart test proves red: fix the admission/recovery path so the contract holds (root-cause fix, no test weakening)
- [ ] Lost-response before spawn (prep refused, admission rolled back) → plain retry succeeds normally; no stale admission leak
- [ ] No protocol/schema changes (contract-only ticket unless the red test forces a code fix)
- [ ] Tests live at the gateway round-trip seam, reusing the existing recovery/restart helpers
