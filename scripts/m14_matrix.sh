#!/bin/sh
# M14 G4: the full spec §44 matrix in release mode.
#
# Cells: every fixture × five modes × M14_SAMPLES (default 10), each a
# fresh scratch workspace with a broken-first proof and the shared
# production driver. Legs: Class A (route+repo, zero LLM) and Class B
# (evidence-first, one scripted call) composed end to end.
#
# Raw samples: target/m14/raw.jsonl   Legs: target/m14/legs.jsonl
# Run metadata for the checker:      target/m14/run_meta.json
set -e
cd "$(dirname "$0")/.."

SAMPLES="${M14_SAMPLES:-10}"
mkdir -p target/m14
RAW=target/m14/raw.jsonl
LEGS=target/m14/legs.jsonl
: >"$RAW"
: >"$LEGS"

cargo build --release --locked --example bench_matrix -p tachyon-core

# Legs: exactly two JSON lines, one per class; grep fails the gate if
# either leg is missing from the test output.
cargo test --release --locked -p tachyon-router --test matrix_legs -- --ignored --nocapture \
    >target/m14/legs.out 2>target/m14/legs.stderr
grep '^{' target/m14/legs.out >"$LEGS"

for fx in auth-refresh multi-file-migration architecture-plan; do
    for mode in full no-speculation no-judgment serial reference; do
        i=1
        while [ "$i" -le "$SAMPLES" ]; do
            ./target/release/examples/bench_matrix "$fx" "$mode" "$i" \
                >>"$RAW" 2>>target/m14/matrix.stderr
            i=$((i + 1))
        done
    done
done

cells=$(wc -l <"$RAW" | tr -d ' ')
leg_lines=$(wc -l <"$LEGS" | tr -d ' ')
expected=$((3 * 5 * SAMPLES))
if [ "$cells" -ne "$expected" ]; then
    echo "expected $expected cell samples, got $cells" >&2
    exit 1
fi
if [ "$leg_lines" -ne 2 ]; then
    echo "expected 2 leg lines, got $leg_lines" >&2
    exit 1
fi

sha=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
printf '{"samples":%s,"cells":%s,"legs":%s,"sha":"%s","date":"%s"}\n' \
    "$SAMPLES" "$cells" "$leg_lines" "$sha" "$(date -u +%Y-%m-%d)" \
    >target/m14/run_meta.json

echo "m14 matrix run ok ($cells samples, $leg_lines legs)"
