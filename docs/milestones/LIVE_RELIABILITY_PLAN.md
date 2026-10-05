# Live-model reliability follow-up

The next milestone is reliable completion of one bounded coding task using a
live model. The previous `LIVE_MODEL_REPORT.md` records 10/20 full and 13/20
serial verified successes. The new gate requires at least 19/20 in each mode.
This is a small fixture gate, not proof of general coding-agent reliability.

## Scope

- Keep the same `auth-refresh` fixture, live provider, model, acceptance
  contract, and 4096-token output reserve in both modes.
- State the JSON output contract and escaping rules explicitly.
- Preserve typed model errors. Retry malformed output once before mutation.
  Use identical requests and one total model-stage deadline.
- Keep strict parsing, revision fences, patch validation, policy checks,
  preimage checks, and acceptance-gated completion.
- Count each actual call, including rejected responses and interrupted calls
  observed by the benchmark. Preserve unavailable usage as unavailable.
- Record safe failure classes. Do not put model text, error bodies, or secrets
  in attempt receipts or benchmark records.
- Test exhaustion, cancellation, steering, late proposals, stale preimages,
  invalid patches, failed acceptance, unknown usage, and counter overflow.
- Run the workspace VERIFY and FULL tiers. Preserve the frozen M14 reports.
- Run 20 full and 20 serial samples. Keep all failures in the corpus. Report
  first-attempt results, retries, final success, wall latency, token totals,
  unavailable usage, and cost availability.
- Compare latency only if both modes meet the gate at equal verified success.
- Review and commit the implementation and measured report on `main`.

This change adds no general planning loop, model-based verifier, automatic
effect replay, ACP feature, or ContextSlice persistence. The capability
contract is in `docs/agents/model-retry-capability.md`.

## Reproduction

Run from the repository root. Use the existing operator environment file;
never place a key in a command argument or published file. Live calls spend
provider quota. The benchmark selects the configured provider only when
`TACHYON_BENCH_LIVE=1` is set. Its defaults are `https://api.xiaomimimo.com`
and `mimo-v2.6-flash`. Pin the same values for every cell.

```sh
CARGO_BUILD_JOBS=2 RUST_LOG=info cargo verify full
CARGO_BUILD_JOBS=2 cargo build --release --locked -p tachyon-core --example bench_matrix
set -a
. "$HOME/.config/tachyon/env"
set +a
export TACHYON_BENCH_LIVE=1
export TACHYON_LIVE_BASE_URL=https://api.xiaomimimo.com
export TACHYON_LIVE_MODEL=mimo-v2.6-flash
mkdir -p target/live-reliability
test ! -e target/live-reliability/raw.jsonl || exit 1
for sample in $(seq 1 20); do
  for mode in full serial; do
    target/release/examples/bench_matrix auth-refresh "$mode" "$sample" \
      >>target/live-reliability/raw.jsonl \
      2>>target/live-reliability/raw.stderr || true
  done
done
node scripts/live_check.mjs target/live-reliability/raw.jsonl --require-reliable
```

The loop keeps a failed driver's JSON record. The checker rejects missing or
incomplete setup samples; an exit failure cannot become a successful sample.
Each cell uses a fresh workspace and proves that the original regression
fails, protected files stay unchanged, the observed change set is exact, and
completion survives task recovery. Alternate full and serial runs to reduce
order bias. Finish builds before using live samples for latency measurement.

The six prompt-shaping pilot samples are separate from the final corpus.
They do not count toward its acceptance gate. JSON escaping instructions
follow the provider's [structured-output guidance](https://mimo.mi.com/docs/en-US/quick-start/usage-guide/text-generation/structured-output).
