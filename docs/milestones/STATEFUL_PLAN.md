# Milestone 20: frozen stateful cross-module repair baseline

Freeze two new tasks before any live call. Reservation-ledger requires atomic
reserve/release batches across two files. Indexed-catalog requires consistent
insert/rename/remove across three files. Errors must preserve complete state.
Public APIs and all non-authorized files are protected. This is benchmark work;
no runtime capability, prompt, model policy, or retry change is authorized.
The unselected M18 guidance stays disabled.

Test at each fixture's public API and the live corpus validator. Prove broken
fixtures compile and fail visible behavior, complete solutions pass, and every
proper nonempty subset of repaired files still fails visible acceptance.
Use independent bounded operation-sequence oracles. Include successful/error
interleavings, duplicate inputs, integer limits, empty batches, same-name
renames, and stale secondary indexes. Hidden tests stay outside evidence.
Known solutions stay outside model workspaces. Mutants that pass visible tests
must fail hidden state oracles. API and protected-path drift must be rejected.

Run five full and five serial trials per task: 20 total. Rotate tasks by round
and alternate modes. Run calls sequentially in fresh workspaces. Force baseline
variant. Use the same release binary, mimo-v2.6-flash, api.xiaomimimo.com,
temperature 0, output cap 4096, model-stage deadline 120000 ms with 1000 ms
measurement allowance, and at most two malformed-only attempts. Publish frozen
inputs and source before calls. Check committed input bytes and binary hashes.
Preserve all outcomes, safe attempt accounting, usage, protected paths, and
completed/failed recovery. Retain failed authorized patches when available.
Do not replace incomplete/interrupted rows or resume an interrupted batch.

Evaluation validity requires all 20 ordered rows and complete safe evidence.
Product score is separate: exploratory reference >=95% overall and >=4/5 in
each cell. Five trials per cell support no true reliability probability,
general agent reliability, or speed claim. Do not pool prior milestones.

Run deterministic controls, VERIFY/FULL, strict scoring and retention tests,
independent spec/standards reviews, and final VERIFY. Publish all samples,
provenance, matrix, bounded report, and project status. Use failures as future
development data; do not weaken acceptance or add semantic repair retries.
