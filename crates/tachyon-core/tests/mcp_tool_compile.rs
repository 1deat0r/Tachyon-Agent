//! Ticket 03 + review fix round (ACP MCP-stdio slice): the `mcp.tool`
//! and `mcp.spawn` compiler carve-outs.
//!
//! External contract under test —
//!
//!   * `compile_operation` admits capabilities `mcp.tool` (calls) and
//!     `mcp.spawn` (launches) for gateway-mediated operations only: the
//!     node carries the capability, the full operation verbatim, and the
//!     ADR-0006 §10 contract-version pin (`MCP_TOOL_CONTRACT_VERSION` /
//!     `MCP_SPAWN_CONTRACT_VERSION`). Misshaped scope parts (empty or
//!     `/`-bearing ids, missing idempotency key, non-object `arguments`)
//!     fail as `InvalidArgs` before any graph exists;
//!   * the call node declares `DestructiveExternalMutation` + `Unknown`
//!     idempotency (interrupted calls classify `UnknownAfterCrash` per
//!     the §19 matrix), the launch node `DestructiveExternalMutation` +
//!     `Keyed` on the launch-approval id (one approval launches once);
//!   * non-MCP consequential capabilities (`process.spawn`,
//!     `credential.use`, `net.fetch`, `shell.exec`) stay
//!     `ForbiddenCapability` in BOTH compilers;
//!   * `mcp.tool` / `mcp.spawn` smuggled into a model patch batch fail as
//!     `UnknownCapability` (never `ForbiddenCapability`, never admitted):
//!     mediated operations compile only through `compile_operation` on
//!     the gateway path.

use tachyon_core::runtime::{
    MCP_SPAWN_CONTRACT_VERSION, MCP_TOOL_CONTRACT_VERSION, RuntimeBounds, RuntimeError,
    compile_operation, parse_proposal,
};
use tachyon_ir::{EffectClass, Idempotency};
use tachyon_types::TaskId;

fn mcp_args() -> serde_json::Value {
    serde_json::json!({
        "server_id": "alpha",
        "tool": "echo",
        "arguments": {"input": "hi"},
    })
}

#[test]
fn mcp_tool_compiles_with_full_args_and_pinned_contract() {
    let args = mcp_args();
    let node =
        compile_operation(TaskId::generate(), 0, "mcp.tool", &args).expect("mcp.tool compiles");
    assert_eq!(node.invocation.capability.0, "mcp.tool");
    assert_eq!(node.invocation.args, args, "full args ride the node");
    assert_eq!(
        node.invocation.contract_version, MCP_TOOL_CONTRACT_VERSION,
        "ADR-0006 §10: recovery never reinterprets the invocation"
    );
    assert_eq!(
        node.effect_class,
        EffectClass::DestructiveExternalMutation,
        "untrusted child, no declared recovery path"
    );
    assert_eq!(
        node.idempotency,
        Idempotency::Unknown,
        "§19: interrupted calls classify UnknownAfterCrash, never replay"
    );
}

#[test]
fn mcp_tool_rejects_misshaped_scope_before_any_graph() {
    let task = TaskId::generate();
    for args in [
        serde_json::json!({"server_id": "", "tool": "echo", "arguments": {}}),
        serde_json::json!({"server_id": "alpha", "tool": "", "arguments": {}}),
        serde_json::json!({"server_id": "a/b", "tool": "echo", "arguments": {}}),
        serde_json::json!({"server_id": "alpha", "tool": "e/c", "arguments": {}}),
        serde_json::json!({"server_id": "alpha", "tool": "echo"}),
        serde_json::json!({"server_id": "alpha", "tool": "echo", "arguments": [1]}),
        serde_json::json!({"tool": "echo", "arguments": {}}),
    ] {
        assert!(
            matches!(
                compile_operation(task, 0, "mcp.tool", &args),
                Err(RuntimeError::InvalidArgs { .. })
            ),
            "misshaped mcp.tool args fail closed: {args}"
        );
    }
}

#[test]
fn non_mcp_consequential_capabilities_stay_forbidden_in_both_compilers() {
    let task = TaskId::generate();
    for capability in ["shell.exec", "process.spawn", "credential.use", "net.fetch"] {
        assert!(
            matches!(
                compile_operation(task, 0, capability, &serde_json::json!({})),
                Err(RuntimeError::ForbiddenCapability(found)) if found == capability
            ),
            "{capability} stays forbidden in compile_operation"
        );
        let proposal = serde_json::json!({
            "decision": "propose_execution",
            "operations": [{"capability": capability, "args": {}}],
        });
        assert!(
            matches!(
                parse_proposal(&proposal, &RuntimeBounds::default()),
                Err(RuntimeError::ForbiddenCapability(found)) if found == capability
            ),
            "{capability} stays forbidden on the patch path"
        );
    }
}

#[test]
fn mcp_tool_in_a_patch_batch_is_unknown_never_admitted() {
    let proposal = serde_json::json!({
        "decision": "propose_execution",
        "operations": [{
            "capability": "mcp.tool",
            "args": {"server_id": "alpha", "tool": "echo", "arguments": {}},
        }],
    });
    assert!(
        matches!(
            parse_proposal(&proposal, &RuntimeBounds::default()),
            Err(RuntimeError::UnknownCapability(found)) if found == "mcp.tool"
        ),
        "patch batches can never carry mediated calls"
    );
}

fn mcp_spawn_args() -> serde_json::Value {
    serde_json::json!({
        "server_id": "alpha",
        "approval_id": "0193c2a1-0000-4000-8000-000000000000",
    })
}

#[test]
fn mcp_spawn_compiles_with_keyed_idempotency_and_pinned_contract() {
    let args = mcp_spawn_args();
    let node =
        compile_operation(TaskId::generate(), 0, "mcp.spawn", &args).expect("mcp.spawn compiles");
    assert_eq!(node.invocation.capability.0, "mcp.spawn");
    assert_eq!(node.invocation.args, args, "full args ride the node");
    assert_eq!(
        node.invocation.contract_version, MCP_SPAWN_CONTRACT_VERSION,
        "ADR-0006 §10: recovery never reinterprets the invocation"
    );
    assert_eq!(
        node.effect_class,
        EffectClass::DestructiveExternalMutation,
        "starting an untrusted child is the consequential effect"
    );
    assert_eq!(
        node.idempotency,
        Idempotency::Keyed,
        "one launch-approval id launches exactly once"
    );
    assert!(
        node.access.reads.is_empty() && node.access.writes.is_empty(),
        "the child runs with ambient authority outside any declared resource"
    );
}

#[test]
fn mcp_spawn_rejects_misshaped_scope_before_any_graph() {
    let task = TaskId::generate();
    for args in [
        serde_json::json!({"server_id": "", "approval_id": "k"}),
        serde_json::json!({"server_id": "a/b", "approval_id": "k"}),
        serde_json::json!({"server_id": "alpha", "approval_id": ""}),
        serde_json::json!({"server_id": "alpha"}),
        serde_json::json!({"approval_id": "k"}),
    ] {
        assert!(
            matches!(
                compile_operation(task, 0, "mcp.spawn", &args),
                Err(RuntimeError::InvalidArgs { .. })
            ),
            "misshaped mcp.spawn args fail closed: {args}"
        );
    }
}

#[test]
fn mcp_spawn_in_a_patch_batch_is_unknown_never_admitted() {
    let proposal = serde_json::json!({
        "decision": "propose_execution",
        "operations": [{
            "capability": "mcp.spawn",
            "args": {"server_id": "alpha", "approval_id": "k"},
        }],
    });
    assert!(
        matches!(
            parse_proposal(&proposal, &RuntimeBounds::default()),
            Err(RuntimeError::UnknownCapability(found)) if found == "mcp.spawn"
        ),
        "patch batches can never carry mediated launches"
    );
}
