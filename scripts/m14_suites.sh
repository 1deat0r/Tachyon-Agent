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
# No TCP listener may exist in shipped code. The pattern covers std and
# tokio constructors; a server framework (axum/warp/actix/hyper-server)
# would additionally show up in Cargo.lock — checked below.
if grep -rlE "TcpListener|TcpSocket" crates/*/src >/dev/null 2>&1; then
    echo "production source binds TCP: remote exposure" >&2
    exit 1
fi
if grep -iE '"name" *= *"[^"]*(axum|warp|actix-web|hyper-util|socket2|tokio-tungstenite)"' Cargo.lock >/dev/null 2>&1; then
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
