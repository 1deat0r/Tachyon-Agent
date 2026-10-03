# TASK 02: recorded-turn prompt gate (ADR-0005:39 reconciliation)
**Status:** ready
**Blocked by:** 01 complete (load must exist to record the turn)
**What to build:** After a successful `session/load`, `SessionState` records `session → last recorded task id` WHEN that turn's status is non-terminal (empty/terminal history ⇒ nothing recorded). `try_acquire_turn` (or the prompt entry immediately after it) consults the record: fresh gateway `GetTask` on the recorded id — non-terminal ⇒ typed `-32003 turn_in_progress` ("a recorded turn is still in progress; session/load reconciles it — wait for it to finish or session/cancel it"); terminal or `task not found` ⇒ clear the record and proceed normally; `GetTask` transport failure ⇒ fail typed `-32001` class (fail-closed, never guess). `session/cancel` for that task completes ⇒ clear the record. Records live only in process memory; a fresh load re-establishes them.
**Verify:** `cargo test -p tachyon-acp` — scripted/live: `prompt_after_load_is_refused_while_the_recorded_turn_runs` (load records non-terminal → prompt → `-32003 turn_in_progress`, zero `CreateTask` calls at the fixture), `prompt_after_load_proceeds_once_the_recorded_turn_is_terminal` (recorded turn terminal on the fresh `GetTask` → record cleared → prompt proceeds), `prompt_gate_releases_after_cancel` (cancel completes → record cleared → prompt proceeds), `gate_gettask_failure_fails_closed` (gateway error at the gate ⇒ typed refusal, no prompt); unit: `record_gate_decision_table` (non-terminal ⇒ block; terminal ⇒ release; missing ⇒ release; transport error ⇒ typed failure).

## Small tasks (each = one commit, in order)
- [ ] S1 record on load + gate decision unit · Verify: `record_gate_decision_table` + load-side record assertions
  - [ ] M1a `recorded: Mutex<HashMap<SessionId, TaskId>>` on `SessionState`; populated only for non-terminal last turns
    - [ ] N1a1 record is in-process only (no store writes; restart ⇒ re-derived at next load)
- [ ] S2 gate wiring at prompt entry + scripted/live tests · Verify: `prompt_after_load_is_refused_while_the_recorded_turn_runs`, `prompt_after_load_proceeds_once_the_recorded_turn_is_terminal`, `gate_gettask_failure_fails_closed`
  - [ ] M2a fresh `GetTask` in the gate; terminal/missing ⇒ clear + proceed
    - [ ] N2a1 zero `CreateTask` frames while blocked (fixture call count)
- [ ] S3 cancel release + full suite green · Verify: `prompt_gate_releases_after_cancel`; `cargo test -p tachyon-acp` exit 0
  - [ ] M3a cancel completion clears the record for its task id
    - [ ] N3a1 clearing is scoped to (session, task) — a different task's record survives
