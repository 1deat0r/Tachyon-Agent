#!/bin/sh
# M14 G6: spec §42 security escape suite + recovery fault-injection suite,
# run explicitly on the M14 tree (the default workspace run includes them,
# but this gate names and counts them so coverage cannot quietly vanish).
#
# Also proves the "accidental remote gateway exposure" property
# structurally: production sources must contain no TCP listener, with a
# positive control that the pattern exists in tests and that the gateway
# binds a local-only transport.
set -e
cd "$(dirname "$0")/.."

run() {
    echo ">> $*"
    "$@"
}

echo "== security escape suite =="
run cargo test --locked -p tachyon-policy
run cargo test --locked -p tachyon-tools --test tools_gate --test process_ownership --test process_lifecycle
run cargo test --locked -p tachyon-mutation --test authorized --test authorized_commit
run cargo test --locked -p tachyon-core --test runtime_repair --test runtime_stages
run cargo test --locked -p tachyon-gateway --test provider_redaction --test approval_routing
run cargo test --locked -p tachyon-verify --test acceptance --test runner

echo "== recovery fault-injection suite =="
run cargo test --locked -p tachyon-core --test fault_kill --test fault_seam_gates --test effect_fixture \
    --test approval_wait --test runtime_recovery --test driver_run
run cargo test --locked -p tachyon-gateway --test recovery --test restart_approval --test reentry
run cargo test --locked -p tachyon-app --test kill_restart
run cargo test --locked -p tachyon-mutation --test mutation_gate --test recovery_scoped

echo "== remote gateway exposure (structural) =="
# No TCP listener may exist in shipped code. The source pattern covers
# std and tokio constructors; listener-capable server frameworks would
# additionally show up in Cargo.lock — checked against Cargo.lock's real
# `name = "..."` line format (the previous quoted-key `"name" = "..."`
# pattern could never match, so the check passed vacuously). socket2 is
# deliberately not listed: tokio legitimately depends on it and it cannot
# bind without a listener type in source. Exact package names only, no
# suffix matching — false positives here would train people to skip the
# gate.
SRC_RE='TcpListener|TcpSocket'
LOCK_RE='^name = "(axum|axum-server|warp|actix-web|actix-server|hyper|hyper-util|tiny-http|tokio-tungstenite|tungstenite|websocket)"$'
# Controls: each pattern must be able to match a representative line, so
# a silently-broken regex fails here instead of passing forever.
printf 'let _l = TcpListener::bind("127.0.0.1:0");\n' | grep -qE "$SRC_RE" || {
    echo "control failed: source regex cannot match a listener line" >&2
    exit 1
}
printf 'name = "axum"\n' | grep -qE "$LOCK_RE" || {
    echo "control failed: lock regex cannot match a known framework" >&2
    exit 1
}
if grep -rlE "$SRC_RE" crates/*/src >/dev/null 2>&1; then
    echo "production source binds TCP: remote exposure" >&2
    exit 1
fi
if grep -qE "$LOCK_RE" Cargo.lock; then
    echo "server-capable listener crate in the dependency closure" >&2
    exit 1
fi
# Positive controls: the absence above is only trusted because the same
# pattern is demonstrably findable elsewhere in this tree (test-only
# loopback actors) and because the gateway binds a local-only transport.
grep -rl "TcpListener" crates/*/tests >/dev/null 2>&1 || {
    echo "positive control failed: no TcpListener found anywhere" >&2
    exit 1
}
grep -rl "UnixListener\|NamedPipe\|named_pipe" crates/tachyon-gateway/src >/dev/null 2>&1 || {
    echo "positive control failed: gateway has no local transport listener" >&2
    exit 1
}

echo "security and recovery suites ok"
