#!/bin/sh
# M14 G3: every shipped benchmark fixture passes its self-check —
# broken-first fails, the shipped solution repairs it, protected paths
# stay byte-identical, and exactly change_paths changed. Runs
# `fixture-check`, which never touches the checked-in fixture.
set -e
cd "$(dirname "$0")/.."

cargo build --release --locked --example bench_matrix -p tachyon-core

for fx in auth-refresh multi-file-migration architecture-plan; do
    ./target/release/examples/bench_matrix "$fx" fixture-check
done

echo "fixture gate ok"
