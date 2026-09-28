#!/bin/sh
# M13 perf gate runner (expert board F2).
#
# Runs the release perf suite and requires every spec §43 target's PASS
# marker, so the gate cannot pass on a zero-test run (e.g. `--ignored`
# filtering everything out) or on a package accidentally dropped from the
# command: `cargo test` exits 0 and prints `test result: ok` in both cases.
# Output is captured and printed even when cargo fails, so assertions and
# suite failures are visible in the gate transcript.
status=0
out=$(cargo test --release --locked \
  -p tachyon-router -p tachyon-scheduler -p tachyon-gateway -p tachyon-repo \
  -p tachyon-store -p tachyon-models -p tachyon-verify -p tachyon-tools \
  --test perf -- --ignored --nocapture 2>&1) || status=$?
printf '%s\n' "$out"
if [ "$status" -ne 0 ]; then
  exit "$status"
fi

for marker in 'perf[T1] PASS' 'perf[T2] PASS' 'perf[T3] PASS' 'perf[T4] PASS' 'perf[T5] PASS'; do
  if ! printf '%s\n' "$out" | grep -qF "$marker"; then
    echo "missing required marker: $marker"
    exit 1
  fi
done

echo "perf gate ok"
