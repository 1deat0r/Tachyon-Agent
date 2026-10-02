//! M10 debugging runtime slice: stage compiler plus supervisor wiring.
//!
//! A narrow module joining evidence, model proposals, authorized mutation
//! and verification through the production supervisor path. There is no
//! second orchestration implementation here: task lifecycle, acceptance
//! binding and completion stay with [`crate::SupervisorHandle`]
//! (`configure_verification` / `verify_and_complete`); this module compiles
//! every evidence/provider/mutation operation to validated Execution IR,
//! enforces the slice bounds, binds evidence freshness, gates writes before
//! mutation, and records benchmark measurements.
//!
//! The model boundary is provider-neutral JSON on purpose: `tachyon-core`
//! must not import provider implementation types (architecture freeze), so
//! the benchmark host adapts its `ModelProvider` into [`ModelProposal`].
//! Mutation itself runs through the real M8 authorized engine via the
//! caller; [`gate_proposal_writes`] guarantees the pre-mutation checks.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tachyon_ir::{
    AccessSet, CancellationPolicy, EffectClass, ExecutionGraph, ExecutionNode, ExecutorKind,
    Idempotency, Invocation, NodePriority, ResourceClaim, ResourceKey, RetryPolicy,
    SpeculationPolicy, TimeoutPolicy,
};
use tachyon_tools::{ToolsContext, authorize, resolve_scope};
use tachyon_types::{NodeId, TaskId};
use tachyon_verify::{AcceptanceContract, Clause};
use thiserror::Error;

/// Hard execution bounds for one debugging attempt (brief §4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeBounds {
    /// At most 16 evidence requests per stage.
    pub max_evidence_requests: usize,
    /// At most 256 KiB of returned evidence per stage.
    pub max_evidence_bytes_per_stage: u64,
    /// At most 4096 bytes in any single evidence path (ADR-0006 §4: the
    /// bound is validated before any path is opened or node allocated).
    pub max_evidence_path_bytes: usize,
    /// At most 8 patch files per attempt.
    pub max_patch_files: usize,
    /// At most 1 MiB of replacement bytes per attempt.
    pub max_replacement_bytes: u64,
    /// Bounded model deadline in milliseconds.
    pub model_deadline_ms: u64,
}

impl Default for RuntimeBounds {
    fn default() -> Self {
        Self {
            max_evidence_requests: 16,
            max_evidence_bytes_per_stage: 256 * 1024,
            max_evidence_path_bytes: 4096,
            max_patch_files: 8,
            max_replacement_bytes: 1024 * 1024,
            model_deadline_ms: 120_000,
        }
    }
}

/// Failures of the debugging runtime. Every variant fails closed: no
/// evidence is returned, no write is staged, no completion is granted.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Too many evidence requests for one stage.
    #[error("too many evidence requests: {0}")]
    TooManyEvidence(usize),
    /// Evidence stage would exceed its byte budget.
    #[error("evidence stage too large: {0} bytes")]
    EvidenceTooLarge(u64),
    /// Supervisor-owned evidence execution refused, could not settle, or
    /// was interrupted before every node produced a durable receipt.
    #[error("evidence execution: {0}")]
    Evidence(String),
    /// Too many patch files in one attempt.
    #[error("too many patch files: {0}")]
    TooManyPatchFiles(usize),
    /// Replacement bytes exceed the per-attempt budget.
    #[error("patch too large: {0} bytes")]
    PatchTooLarge(usize),
    /// The slice supports `fs.read`/`repo.lexical` evidence and
    /// `mutation.patch` proposals only.
    #[error("unknown or unsupported capability: {0}")]
    UnknownCapability(String),
    /// Arguments are untyped or misshaped.
    #[error("invalid arguments for {capability}: {reason}")]
    InvalidArgs {
        /// Capability the args were offered for.
        capability: String,
        /// What was wrong.
        reason: String,
    },
    /// Empty proposals carry no work.
    #[error("empty proposal")]
    EmptyProposal,
    /// Raw shell, credentials, network and other consequential effects are
    /// never admitted in this slice.
    #[error("forbidden capability: {0}")]
    ForbiddenCapability(String),
    /// Model-supplied access/effect metadata is never trusted.
    #[error("model-supplied execution metadata rejected for {0}")]
    UntrustedMetadata(String),
    /// Provider `Complete` is never completion authority.
    #[error("provider completion is not completion authority")]
    ModelAuthorityRejected,
    /// A newly introduced hard constraint has no executable binding.
    #[error("unbound hard constraint stops writes: {0}")]
    UnboundConstraint(String),
    /// Proposed write escapes the acceptance scope or protected paths.
    #[error("write refused by acceptance: {0}")]
    WriteRefused(String),
    /// Evidence changed, was denied, or cannot be revalidated.
    #[error("stale evidence invalidates the proposal: {0}")]
    StaleEvidence(String),
    /// Acceptance was never bound before mutation.
    #[error("acceptance/baseline not bound before mutation")]
    BaselineNotBound,
    /// Filesystem, policy or persistence failure.
    #[error("io: {0}")]
    Io(String),
    /// Tool/policy layer refusal.
    #[error("tool: {0}")]
    Tool(#[from] tachyon_tools::ToolError),
    /// IR validation refusal.
    #[error("invalid IR: {0}")]
    Ir(String),
    /// JSON failure.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// One bounded evidence request from the trusted runtime plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceRequest {
    /// Registry capability id (`fs.read` or `repo.lexical`).
    pub capability: String,
    /// Workspace-relative path to read.
    pub path: String,
}

/// Evidence actually supplied to reasoning, with its content hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceItem {
    /// Workspace-relative path in journal-key form.
    pub path: String,
    /// Content hash binding the supplied version.
    pub hash: String,
    /// Supplied bytes (bounded by [`RuntimeBounds`]).
    pub bytes: Vec<u8>,
}

/// Stable, dependency-free content hash for evidence freshness within the
/// runtime. Mutation preimages keep their own M8 BLAKE3 hashes; callers may
/// re-key [`EvidenceItem::hash`] to the authoritative content hash before
/// gating, as long as every member is re-keyed from the same bytes.
///
/// IDENTITY BOUNDARY (pinned by test): this FNV-1a value is an opaque
/// freshness token, NEVER an artifact/mutation content identity. It must not
/// be equated with M8 BLAKE3 preimage/postimage hashes or with spec
/// `ArtifactId` (content-addressed BLAKE3). Freshness compares only
/// `hash_bytes` output against the manifest built from the same function
/// (or a uniformly re-keyed manifest); crossing the boundary would be a
/// hash-confusion downgrade.
#[must_use]
pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xCBF2_9CE4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01B3);
    }
    format!("{hash:016x}")
}

/// Reads bounded evidence through policy: exact canonical paths are
/// authorized BEFORE reads, sizes are checked pre-allocation via metadata,
/// and traversal fails closed through containment.
pub fn collect_evidence(
    context: &ToolsContext,
    requests: &[EvidenceRequest],
    bounds: &RuntimeBounds,
) -> Result<Vec<EvidenceItem>, RuntimeError> {
    if requests.len() > bounds.max_evidence_requests {
        return Err(RuntimeError::TooManyEvidence(requests.len()));
    }
    let mut total: u64 = 0;
    let mut items = Vec::with_capacity(requests.len());
    for request in requests {
        if request.capability != "fs.read" {
            return Err(RuntimeError::UnknownCapability(request.capability.clone()));
        }
        let requested = context.workspace_root.join(&request.path);
        let (resolved, scope) =
            resolve_scope(&context.workspace_root, &requested).map_err(|err| {
                RuntimeError::StaleEvidence(format!("unverifiable evidence path: {err}"))
            })?;
        let operation =
            serde_json::json!({"capability": "fs.read", "scope": scope, "path": request.path});
        authorize(
            &context.policy,
            &context.approvals,
            "fs.read",
            &scope,
            &operation,
            "runtime evidence read",
        )?;
        let size = std::fs::metadata(&resolved)
            .map_err(|err| RuntimeError::Io(err.to_string()))?
            .len();
        total += size;
        if total > bounds.max_evidence_bytes_per_stage {
            return Err(RuntimeError::EvidenceTooLarge(total));
        }
        // M12 fault point: kill here = native read with no committed result.
        tachyon_tools::fault::reach_blocking("evidence.read");
        let bytes = std::fs::read(&resolved).map_err(|err| RuntimeError::Io(err.to_string()))?;
        let rel = normalize_key(&request.path)?;
        items.push(EvidenceItem {
            path: rel,
            hash: hash_bytes(&bytes),
            bytes,
        });
    }
    Ok(items)
}

/// The full canonical file/hash set actually supplied to reasoning.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EvidenceManifest {
    /// `(path, hash)` members in sorted order.
    pub entries: Vec<(String, String)>,
}

/// Builds the manifest from supplied evidence.
#[must_use]
pub fn manifest_of(items: &[EvidenceItem]) -> EvidenceManifest {
    let mut entries: Vec<(String, String)> = items
        .iter()
        .map(|i| (i.path.clone(), i.hash.clone()))
        .collect();
    entries.sort();
    EvidenceManifest { entries }
}

/// Revalidates EVERY manifest member under the mutation lease. Changed,
/// missing, or extra members invalidate the whole proposal with zero
/// writes; `base_hash` binds the supplied version, never a substitution.
pub fn verify_manifest_freshness(
    manifest: &EvidenceManifest,
    current: &[(String, String)],
) -> Result<(), RuntimeError> {
    let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
    for (path, hash) in current {
        seen.insert(path.as_str(), hash.as_str());
    }
    for (path, hash) in &manifest.entries {
        match seen.get(path.as_str()) {
            Some(actual) if *actual == hash => {}
            Some(_) => {
                return Err(RuntimeError::StaleEvidence(format!("changed: {path}")));
            }
            None => {
                return Err(RuntimeError::StaleEvidence(format!("missing: {path}")));
            }
        }
    }
    if seen.len() != manifest.entries.len() {
        return Err(RuntimeError::StaleEvidence(
            "unevidenced extra input".into(),
        ));
    }
    Ok(())
}

/// Real IR declarations per slice capability. Unknown capabilities, raw
/// shell, credentials/network, and model-supplied access/effect metadata
/// fail closed here, before any graph is built.
// Dispatch table over the slice capabilities: arms stay one-liners via
// helpers (`mcp_tool_parts`), so the line budget buys no readability.
#[allow(clippy::too_many_lines)]
pub fn compile_operation(
    task_id: TaskId,
    revision: u64,
    capability: &str,
    args: &serde_json::Value,
) -> Result<ExecutionNode, RuntimeError> {
    if !args.is_object() {
        return Err(RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "args must be a JSON object".into(),
        });
    }
    let object = args.as_object().expect("object");
    for key in ["access", "effect", "grants", "resource", "retry"] {
        if object.contains_key(key) {
            return Err(RuntimeError::UntrustedMetadata(capability.to_owned()));
        }
    }
    let typed_string = |field: &str| -> Result<String, RuntimeError> {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| RuntimeError::InvalidArgs {
                capability: capability.to_owned(),
                reason: format!("missing typed string field {field:?}"),
            })
    };
    let (executor, access, effect, idempotency) = match capability {
        "fs.read" => {
            let path = typed_string("path")?;
            let key = resource_key(&path)?;
            (
                ExecutorKind::Tool,
                AccessSet {
                    reads: vec![key],
                    writes: Vec::new(),
                },
                EffectClass::ReadOnlyLocal,
                Idempotency::Pure,
            )
        }
        "repo.lexical" => {
            let query = typed_string("query")?;
            let key = resource_key(&query)?;
            (
                ExecutorKind::Repository,
                AccessSet {
                    reads: vec![key],
                    writes: Vec::new(),
                },
                EffectClass::ReadOnlyLocal,
                Idempotency::Pure,
            )
        }
        "mutation.patch" => {
            let path = typed_string("path")?;
            let _content = typed_string("new_content")?;
            let key = resource_key(&path)?;
            (
                ExecutorKind::Mutation,
                AccessSet {
                    reads: vec![key.clone()],
                    writes: vec![key],
                },
                EffectClass::ReversibleLocalMutation,
                Idempotency::Keyed,
            )
        }
        "mcp.tool" => mcp_tool_parts(capability, object)?,
        "mcp.spawn" => mcp_spawn_parts(capability, object)?,
        "shell.exec" | "process.spawn" | "credential.use" | "net.fetch" => {
            return Err(RuntimeError::ForbiddenCapability(capability.to_owned()));
        }
        _ => return Err(RuntimeError::UnknownCapability(capability.to_owned())),
    };
    Ok(ExecutionNode {
        id: NodeId::generate(),
        task_id,
        planned_revision: revision,
        executor,
        invocation: Invocation {
            capability: tachyon_types::CapabilityId(capability.to_owned()),
            args: args.clone(),
            contract_version: capability_contract_version(capability),
        },
        inputs: Vec::new(),
        expected_outputs: declared_outputs(capability),
        access,
        resources: ResourceClaim {
            cpu_units: 100,
            memory_mb: Some(64),
            ..ResourceClaim::default()
        },
        effect_class: effect,
        idempotency,
        speculation: SpeculationPolicy::Forbidden,
        timeout: TimeoutPolicy {
            hard_ms: Some(60_000),
        },
        retry: RetryPolicy {
            attempts: 1,
            backoff_ms: 0,
        },
        cancellation: CancellationPolicy::Immediate,
        verification: Vec::new(),
        priority: NodePriority::Normal,
    })
}

/// Node declarations for one gateway-mediated `mcp.tool` call: the
/// gateway inventory-checks the tool name against the live server's
/// recorded `tools/list` before compiling, so an unknown tool never
/// reaches child I/O. The compiler pins the shape here — non-empty
/// `server_id`/`tool` with no `/` (the authorize scope is
/// `<server-id>/<tool>`, split on the single `/`) and an object
/// `arguments` carried verbatim in the authorized operation JSON.
fn mcp_tool_parts(
    capability: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(ExecutorKind, AccessSet, EffectClass, Idempotency), RuntimeError> {
    let string_field = |field: &str| -> Result<String, RuntimeError> {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| RuntimeError::InvalidArgs {
                capability: capability.to_owned(),
                reason: format!("missing typed string field {field:?}"),
            })
    };
    let server_id = string_field("server_id")?;
    let tool = string_field("tool")?;
    if server_id.is_empty() || tool.is_empty() || server_id.contains('/') || tool.contains('/') {
        return Err(RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "mcp.tool needs non-empty server_id and tool with no '/'".into(),
        });
    }
    let arguments = object
        .get("arguments")
        .ok_or_else(|| RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "missing object field \"arguments\"".into(),
        })?;
    if !arguments.is_object() {
        return Err(RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "mcp.tool \"arguments\" must be a JSON object".into(),
        });
    }
    // An MCP tool runs inside an untrusted child with ambient authority
    // and declares no recovery path: an interrupted call classifies
    // `UnknownAfterCrash` per the §19 matrix (never blindly replayed),
    // and the node never speculates.
    Ok((
        ExecutorKind::Tool,
        AccessSet {
            reads: Vec::new(),
            writes: Vec::new(),
        },
        EffectClass::DestructiveExternalMutation,
        Idempotency::Unknown,
    ))
}

/// Node declarations for one gateway-mediated `mcp.spawn` launch: the
/// gateway compiles the launch before spawning, so the untrusted child
/// starts only under a validated, authorized operation. The compiler
/// pins the shape here — a non-empty `server_id` with no `/` (the
/// authorize scope is `<server-id>/launch`, split on the single `/`).
/// Empty access set: the child runs with ambient authority outside any
/// declared resource. Idempotency is `Keyed` on the launch-approval id
/// carried in the operation (`approval_id`): one approval launches
/// exactly once, and a replay under a different id is a different key.
fn mcp_spawn_parts(
    capability: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<(ExecutorKind, AccessSet, EffectClass, Idempotency), RuntimeError> {
    let string_field = |field: &str| -> Result<String, RuntimeError> {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| RuntimeError::InvalidArgs {
                capability: capability.to_owned(),
                reason: format!("missing typed string field {field:?}"),
            })
    };
    let server_id = string_field("server_id")?;
    if server_id.is_empty() || server_id.contains('/') {
        return Err(RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "mcp.spawn needs a non-empty server_id with no '/'".into(),
        });
    }
    // The launch-approval id is the idempotency key: present and
    // non-empty, otherwise two launches of the same server would share
    // one key and the compiler could not tell them apart.
    let approval_id = string_field("approval_id")?;
    if approval_id.is_empty() {
        return Err(RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "mcp.spawn needs a non-empty approval_id idempotency key".into(),
        });
    }
    // A launch starts an untrusted child with ambient authority and no
    // recovery path: spawning it is the consequential effect, so the
    // node is `DestructiveExternalMutation`, never speculated, exactly
    // like the mediated `mcp.tool` call it precedes.
    Ok((
        ExecutorKind::Tool,
        AccessSet {
            reads: Vec::new(),
            writes: Vec::new(),
        },
        EffectClass::DestructiveExternalMutation,
        Idempotency::Keyed,
    ))
}

/// Capability contract version this node is compiled under (ADR-0006
/// §10): persisted on the invocation so recovery never reinterprets it
/// under newer semantics. Only `fs.read`, `mcp.spawn`, and `mcp.tool`
/// have versioned contracts in this slice; every other capability
/// records "no contract".
fn capability_contract_version(capability: &str) -> u16 {
    match capability {
        "fs.read" => crate::evidence::FS_READ_CONTRACT_VERSION,
        "mcp.spawn" => MCP_SPAWN_CONTRACT_VERSION,
        "mcp.tool" => MCP_TOOL_CONTRACT_VERSION,
        _ => tachyon_ir::CAPABILITY_CONTRACT_NONE,
    }
}

/// Capability contract version of the `mcp.spawn` launch capability,
/// persisted on every invocation so recovery never reinterprets an
/// invocation under newer semantics (ADR-0006 §10).
pub const MCP_SPAWN_CONTRACT_VERSION: u16 = 1;

/// Capability contract version of the `mcp.tool` mediated-call
/// capability, persisted on every invocation so recovery never
/// reinterprets an invocation under newer semantics (ADR-0006 §10).
pub const MCP_TOOL_CONTRACT_VERSION: u16 = 1;

/// Output declarations are part of the trusted compilation, not the
/// caller's proposal: the evidence capability promises exactly one
/// structured output, and the dispatcher refuses a node whose declared
/// outputs are not supplied (ADR-0006 §3).
fn declared_outputs(capability: &str) -> Vec<tachyon_ir::OutputBinding> {
    if capability == "fs.read" {
        vec![tachyon_ir::OutputBinding {
            name: crate::evidence::EVIDENCE_OUTPUT_NAME.to_owned(),
        }]
    } else {
        Vec::new()
    }
}

fn resource_key(path: &str) -> Result<ResourceKey, RuntimeError> {
    let rel = normalize_key(path)?;
    ResourceKey::parse(&format!("file:/{rel}")).map_err(|err| RuntimeError::Ir(err.to_string()))
}

pub(crate) fn normalize_key(path: &str) -> Result<String, RuntimeError> {
    let cleaned = path.replace('\\', "/");
    let trimmed = cleaned.trim_matches('/').to_owned();
    if trimmed.is_empty() {
        return Err(RuntimeError::InvalidArgs {
            capability: String::new(),
            reason: "empty path".into(),
        });
    }
    let mut parts = Vec::new();
    for part in trimmed.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(RuntimeError::WriteRefused(format!("escaped path: {path}")));
        }
        parts.push(part);
    }
    if parts.is_empty() {
        return Err(RuntimeError::InvalidArgs {
            capability: String::new(),
            reason: "empty path".into(),
        });
    }
    Ok(parts.join("/"))
}

/// Compiles bounded evidence requests to validated IR.
pub fn compile_evidence_graph(
    task_id: TaskId,
    revision: u64,
    requests: &[EvidenceRequest],
    bounds: &RuntimeBounds,
) -> Result<ExecutionGraph, RuntimeError> {
    if requests.len() > bounds.max_evidence_requests {
        return Err(RuntimeError::TooManyEvidence(requests.len()));
    }
    let mut graph = ExecutionGraph::empty(task_id, revision);
    for request in requests {
        let node = compile_operation(
            task_id,
            revision,
            &request.capability,
            &serde_json::json!({"path": request.path}),
        )?;
        graph.nodes.insert(node.id, node);
    }
    graph
        .validate(task_id)
        .map_err(|err| RuntimeError::Ir(err.to_string()))?;
    Ok(graph)
}

/// Lowers router placeholder nodes (empty access declarations) to real
/// declarations derived from their capability, then revalidates.
pub fn lower_router_placeholders(
    mut graph: ExecutionGraph,
    task_id: TaskId,
    revision: u64,
) -> Result<ExecutionGraph, RuntimeError> {
    for node in graph.nodes.values_mut() {
        if node.access.reads.is_empty() && node.access.writes.is_empty() {
            let rebuilt = compile_operation(
                node.task_id,
                revision,
                &node.invocation.capability.0,
                &node.invocation.args,
            )?;
            node.executor = rebuilt.executor;
            node.access = rebuilt.access;
            node.effect_class = rebuilt.effect_class;
            node.idempotency = rebuilt.idempotency;
        }
    }
    graph
        .validate(task_id)
        .map_err(|err| RuntimeError::Ir(err.to_string()))?;
    Ok(graph)
}

/// One proposed file replacement inside a model patch proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProposedFile {
    /// Workspace-relative path in journal-key form.
    pub path: String,
    /// Hash of the supplied version this patch is based on.
    pub base_hash: Option<String>,
    /// Complete replacement bytes.
    pub new_content: Vec<u8>,
}

/// Provider-neutral model output for this slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelProposal {
    /// A typed `mutation.patch` proposal. Never self-authorizing.
    Patch {
        /// Proposed file replacements.
        files: Vec<ProposedFile>,
    },
    /// Provider belief that work is done. Never completion authority.
    Complete {
        /// Claimed outcome, checked against verifiers.
        summary: String,
    },
    /// More evidence is needed: a durable blocked outcome, not progress.
    RequestEvidence {
        /// Capability calls to run through the scheduler.
        requests: Vec<(String, serde_json::Value)>,
    },
    /// The task cannot proceed without the user: durable blocked outcome.
    NeedUserInput {
        /// The blocking question.
        question: String,
    },
    /// Plain answer grounded in the provided context.
    Respond {
        /// The answer text.
        message: String,
    },
}

/// Durable blocked outcomes for unsupported `RequestEvidence` /
/// `NeedUserInput` decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockedOutcome {
    /// Waiting on more evidence.
    NeedEvidence,
    /// Waiting on the user.
    NeedUserInput,
}

/// Parses provider-neutral model output, enforcing slice bounds before any
/// allocation-heavy work: typed `mutation.patch` only, no empty or
/// oversize proposals, no model-supplied execution metadata.
pub fn parse_proposal(
    value: &serde_json::Value,
    bounds: &RuntimeBounds,
) -> Result<ModelProposal, RuntimeError> {
    let decision = value
        .get("decision")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RuntimeError::InvalidArgs {
            capability: "model".into(),
            reason: "missing decision tag".into(),
        })?;
    let string_field = |field: &str| -> Result<String, RuntimeError> {
        value
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| RuntimeError::InvalidArgs {
                capability: "model".into(),
                reason: format!("missing typed string field {field:?}"),
            })
    };
    match decision {
        "propose_execution" => {
            let operations = value
                .get("operations")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| RuntimeError::InvalidArgs {
                    capability: "model".into(),
                    reason: "operations must be an array".into(),
                })?;
            Ok(ModelProposal::Patch {
                files: parse_patch_operations(operations, bounds)?,
            })
        }
        "complete" => Ok(ModelProposal::Complete {
            summary: string_field("summary")?,
        }),
        "request_evidence" => {
            let requests = value
                .get("requests")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| RuntimeError::InvalidArgs {
                    capability: "model".into(),
                    reason: "requests must be an array".into(),
                })?;
            Ok(ModelProposal::RequestEvidence {
                requests: parse_evidence_asks(requests)?,
            })
        }
        "need_user_input" => Ok(ModelProposal::NeedUserInput {
            question: string_field("question")?,
        }),
        "respond" => Ok(ModelProposal::Respond {
            message: string_field("message")?,
        }),
        _ => Err(RuntimeError::InvalidArgs {
            capability: "model".into(),
            reason: format!("unknown decision {decision:?}"),
        }),
    }
}

/// Parses typed `mutation.patch` operations with pre-allocation bounds.
fn parse_patch_operations(
    operations: &[serde_json::Value],
    bounds: &RuntimeBounds,
) -> Result<Vec<ProposedFile>, RuntimeError> {
    if operations.is_empty() {
        return Err(RuntimeError::EmptyProposal);
    }
    if operations.len() > bounds.max_patch_files {
        return Err(RuntimeError::TooManyPatchFiles(operations.len()));
    }
    let mut files = Vec::with_capacity(operations.len());
    let mut total: u64 = 0;
    for operation in operations {
        files.push(parse_patch_operation(operation, &mut total, bounds)?);
    }
    Ok(files)
}

/// Parses one typed `mutation.patch` operation.
fn parse_patch_operation(
    operation: &serde_json::Value,
    total: &mut u64,
    bounds: &RuntimeBounds,
) -> Result<ProposedFile, RuntimeError> {
    let capability = operation
        .get("capability")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if capability != "mutation.patch" {
        // `mcp.tool` / `mcp.spawn` name real capabilities, but a patch
        // batch can never carry them: mediated MCP calls and launches
        // compile only through `compile_operation` on the gateway path,
        // never through model patch proposals. They fail as
        // `UnknownCapability` here — explicitly carved out of the
        // forbidden set below, so a future widening of that set cannot
        // silently reclassify them.
        if capability == "mcp.tool" || capability == "mcp.spawn" {
            return Err(RuntimeError::UnknownCapability(capability.to_owned()));
        }
        if matches!(
            capability,
            "shell.exec" | "process.spawn" | "credential.use" | "net.fetch"
        ) {
            return Err(RuntimeError::ForbiddenCapability(capability.to_owned()));
        }
        return Err(RuntimeError::UnknownCapability(capability.to_owned()));
    }
    let args = operation
        .get("args")
        .ok_or_else(|| RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "missing args object".into(),
        })?;
    if !args.is_object() {
        return Err(RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "args must be a JSON object".into(),
        });
    }
    for key in ["access", "effect", "grants", "resource", "retry"] {
        if args.get(key).is_some() {
            return Err(RuntimeError::UntrustedMetadata(capability.to_owned()));
        }
    }
    let path = args
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "missing typed string field \"path\"".into(),
        })?;
    let content = args
        .get("new_content")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RuntimeError::InvalidArgs {
            capability: capability.to_owned(),
            reason: "missing typed string field \"new_content\"".into(),
        })?;
    if content.is_empty() {
        return Err(RuntimeError::EmptyProposal);
    }
    *total += content.len() as u64;
    if *total > bounds.max_replacement_bytes {
        return Err(RuntimeError::PatchTooLarge(
            usize::try_from(*total).unwrap_or(usize::MAX),
        ));
    }
    let base_hash = args
        .get("base_hash")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    Ok(ProposedFile {
        path: path.to_owned(),
        base_hash,
        new_content: content.as_bytes().to_vec(),
    })
}

/// Parses typed evidence asks for the scheduler path.
fn parse_evidence_asks(
    requests: &[serde_json::Value],
) -> Result<Vec<(String, serde_json::Value)>, RuntimeError> {
    let mut out = Vec::with_capacity(requests.len());
    for request in requests {
        let capability = request
            .get("capability")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if !matches!(capability, "fs.read" | "repo.lexical") {
            return Err(RuntimeError::UnknownCapability(capability.to_owned()));
        }
        let args = request
            .get("args")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        if !args.is_object() {
            return Err(RuntimeError::InvalidArgs {
                capability: capability.to_owned(),
                reason: "args must be a JSON object".into(),
            });
        }
        out.push((capability.to_owned(), args));
    }
    Ok(out)
}

/// Provider `Complete` is never completion authority. This function exists
/// so call sites say so explicitly; it always returns false.
#[must_use]
pub const fn complete_grants_success() -> bool {
    false
}

/// Maps blocking decisions to durable blocked outcomes.
#[must_use]
pub const fn blocked_kind(proposal: &ModelProposal) -> Option<BlockedOutcome> {
    match proposal {
        ModelProposal::RequestEvidence { .. } => Some(BlockedOutcome::NeedEvidence),
        ModelProposal::NeedUserInput { .. } => Some(BlockedOutcome::NeedUserInput),
        _ => None,
    }
}

/// Exactly one bounded retry of a rejected/failed proposal; no loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryBudget {
    /// Remaining retries.
    remaining: u8,
}

impl RetryBudget {
    /// One retry, as the brief requires.
    #[must_use]
    pub const fn new() -> Self {
        Self { remaining: 1 }
    }

    /// Takes the retry if one remains.
    pub fn take_retry(&mut self) -> bool {
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self::new()
    }
}

/// Acceptance plus source baseline bound BEFORE any mutation. Later starts,
/// retries, or model output cannot weaken these bindings.
#[derive(Clone, Debug)]
pub struct BoundContract {
    /// Immutable acceptance contract.
    pub contract: AcceptanceContract,
    /// Supervisor revision the binding was taken at.
    pub revision: u64,
}

/// Binds acceptance and baseline before mutation.
#[must_use]
pub const fn bind_contract(contract: AcceptanceContract, revision: u64) -> BoundContract {
    BoundContract { contract, revision }
}

/// A newly introduced hard constraint offered after binding. It must carry
/// an executable path binding; a missing binding stops writes fail-closed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HardBinding {
    /// Constraint identity.
    pub id: String,
    /// Executable path binding, or `None` when unbound.
    pub bound_check: Option<String>,
}

/// Protected paths the slice never writes: migrations and manifests.
#[must_use]
pub fn is_protected_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    let trimmed = normalized.trim_matches('/');
    trimmed == "migrations"
        || trimmed.starts_with("migrations/")
        || trimmed == "Cargo.lock"
        || trimmed.ends_with("/Cargo.lock")
}

/// Constrains proposed writes BEFORE mutation against the bound contract:
/// exact `ChangedPathsWithin` / `FileUnchanged` bindings (including
/// `HardConstraint`-wrapped checks), protected paths, escaped paths, and
/// `base_hash` freshness against the supplied manifest. Any newly
/// introduced unbound hard constraint stops writes fail-closed.
pub fn gate_proposal_writes(
    bound: &BoundContract,
    files: &[ProposedFile],
    manifest: &EvidenceManifest,
    extra_hard: &[HardBinding],
) -> Result<(), RuntimeError> {
    if bound.contract.clauses.is_empty() {
        return Err(RuntimeError::BaselineNotBound);
    }
    for binding in extra_hard {
        match &binding.bound_check {
            None => return Err(RuntimeError::UnboundConstraint(binding.id.clone())),
            Some(check) => {
                let rel = normalize_key(check)?;
                if is_protected_path(&rel) {
                    return Err(RuntimeError::WriteRefused(format!(
                        "hard binding covers protected path: {rel}"
                    )));
                }
            }
        }
    }
    // Every `ChangedPathsWithin` clause is required by the contract. Paths
    // inside one clause are alternatives; separate clauses are conjunctive,
    // including checks wrapped by `HardConstraint`.
    let mut allowed: Vec<Vec<String>> = Vec::new();
    let mut unchanged: Vec<String> = Vec::new();
    let mut unresolved = false;
    for clause in &bound.contract.clauses {
        collect_bindings(clause, &mut allowed, &mut unchanged, &mut unresolved)?;
    }
    if unresolved {
        return Err(RuntimeError::WriteRefused(
            "unresolved contract clause".into(),
        ));
    }
    // Every bound extra_hard scope independently constrains the write in
    // addition to all contract clauses. Each binding is one single-path
    // scope, so a broad acceptance clause cannot widen a narrower hard rule.
    let extra_scopes: Vec<Vec<String>> = extra_hard
        .iter()
        .filter_map(|b| b.bound_check.as_ref())
        .map(|scope| normalize_key(scope.as_str()).map(|scope| vec![scope]))
        .collect::<Result<Vec<_>, _>>()?;
    if !files.is_empty() && allowed.is_empty() {
        return Err(RuntimeError::WriteRefused(
            "no bound write scope for proposal".into(),
        ));
    }
    let hashes: BTreeMap<&str, &str> = manifest
        .entries
        .iter()
        .map(|(path, hash)| (path.as_str(), hash.as_str()))
        .collect();
    for file in files {
        let rel = normalize_key(&file.path)?;
        if is_protected_path(&rel) {
            return Err(RuntimeError::WriteRefused(format!("protected path: {rel}")));
        }
        for frozen in &unchanged {
            if rel == *frozen {
                return Err(RuntimeError::WriteRefused(format!("file unchanged: {rel}")));
            }
        }
        let is_within_any = |scopes: &[String]| {
            scopes
                .iter()
                .any(|scope| rel == *scope || rel.starts_with(&format!("{scope}/")))
        };
        if !allowed.iter().all(|scopes| is_within_any(scopes)) {
            return Err(RuntimeError::WriteRefused(format!(
                "outside acceptance scope: {rel}"
            )));
        }
        for scopes in &extra_scopes {
            if !is_within_any(scopes) {
                return Err(RuntimeError::WriteRefused(format!(
                    "outside hard binding scope: {rel}"
                )));
            }
        }
        match (hashes.get(rel.as_str()), &file.base_hash) {
            (Some(expected), Some(base)) if *base == **expected => {}
            _ => {
                return Err(RuntimeError::StaleEvidence(format!(
                    "base_hash does not bind the supplied version of {rel}"
                )));
            }
        }
        if file.new_content.is_empty() {
            return Err(RuntimeError::EmptyProposal);
        }
        if file.new_content.len() as u64 > RuntimeBounds::default().max_replacement_bytes {
            return Err(RuntimeError::PatchTooLarge(file.new_content.len()));
        }
    }
    Ok(())
}

fn collect_bindings(
    clause: &Clause,
    allowed: &mut Vec<Vec<String>>,
    unchanged: &mut Vec<String>,
    unresolved: &mut bool,
) -> Result<(), RuntimeError> {
    match clause {
        Clause::ChangedPathsWithin { paths } => {
            let scope = paths
                .iter()
                .map(|path| {
                    // Normalize exactly like proposed files so `src/` and
                    // `src` admit the same writes; escapes fail closed here.
                    normalize_key(path.as_str())
                })
                .collect::<Result<Vec<_>, _>>()?;
            allowed.push(scope);
        }
        Clause::FileUnchanged { path } => {
            unchanged.push(normalize_key(path.as_str())?);
        }
        Clause::HardConstraint { check, .. } => {
            collect_bindings(check, allowed, unchanged, unresolved)?;
        }
        Clause::CommandPasses { .. } => {}
        Clause::Unresolved { .. } => {
            *unresolved = true;
        }
    }
    Ok(())
}

/// Mutation intent persisted BEFORE prepare, so recovery can distinguish an
/// announced batch from a stray journal entry.
///
/// Deliberately narrow: batch identity plus the exact file paths and base
/// hashes the gate authorized. There is no room for provider configuration,
/// secrets, or source blobs — a caller cannot smuggle them through intent.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationIntent {
    /// Caller-reserved batch identity (also the journal key).
    pub batch_id: String,
    /// Gate-authorized files: workspace-relative path + bound base hash.
    pub files: Vec<IntentFile>,
}

/// One gate-authorized file in a [`MutationIntent`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntentFile {
    /// Workspace-relative path, normalized form.
    pub path: String,
    /// Base hash bound to the supplied evidence version.
    pub base_hash: String,
}

impl MutationIntent {
    /// Builds intent from already-gated proposal files; normalizes paths.
    pub fn authorized(batch_id: &str, files: &[ProposedFile]) -> Result<Self, RuntimeError> {
        let mut authorized = Vec::with_capacity(files.len());
        for file in files {
            let Some(base) = file.base_hash.clone() else {
                return Err(RuntimeError::StaleEvidence(format!(
                    "intent requires a bound base_hash for {}",
                    file.path
                )));
            };
            authorized.push(IntentFile {
                path: normalize_key(&file.path)?,
                base_hash: base,
            });
        }
        Ok(Self {
            batch_id: batch_id.to_owned(),
            files: authorized,
        })
    }
}

/// Persists mutation intent BEFORE prepare, so recovery can distinguish an
/// announced batch from a stray journal entry.
pub fn persist_intent(
    dir: &Path,
    batch_id: &str,
    intent: &MutationIntent,
) -> Result<PathBuf, RuntimeError> {
    std::fs::create_dir_all(dir).map_err(|err| RuntimeError::Io(err.to_string()))?;
    let safe: String = batch_id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let path = dir.join(format!("intent-{safe}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(intent)?.as_bytes())
        .map_err(|err| RuntimeError::Io(err.to_string()))?;
    Ok(path)
}

/// Loads a previously persisted mutation intent.
pub fn load_intent(dir: &Path, batch_id: &str) -> Result<MutationIntent, RuntimeError> {
    let safe: String = batch_id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let bytes = std::fs::read(dir.join(format!("intent-{safe}.json")))
        .map_err(|err| RuntimeError::Io(err.to_string()))?;
    serde_json::from_slice(&bytes).map_err(RuntimeError::Json)
}

/// Durable effect disposition. Only [`MutationOutcome::Committed`] counts
/// as successful work; everything else requires a fresh proposal under
/// unchanged acceptance/baseline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcome {
    /// Matching completed receipts plus current postimages.
    Committed,
    /// Rollback receipts plus fresh preimages; uncertainty resolved, work not done.
    Compensated,
    /// Proof that no preparation/commit occurred.
    ProvenNoEffect,
    /// Partial/error/timeout until explicitly reconciled.
    Unknown,
}

impl MutationOutcome {
    /// Only `Committed` counts as success.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Committed)
    }
}

/// Where usage/token counts came from. Unknown is null, never zero.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenProvenance {
    /// Counts reported by the provider.
    ProviderReported,
    /// Counts from a scripted test/replay provider.
    Scripted,
    /// No measurement available.
    #[default]
    Unknown,
}

/// One node's measured execution interval, for real concurrency analysis.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTiming {
    /// Node identity.
    pub node: String,
    /// Start time in milliseconds since run start.
    pub start_ms: u64,
    /// End time in milliseconds since run start.
    pub end_ms: u64,
}

/// Machine-readable measurements for the later benchmark host. Unknown or
/// unmeasured metrics are null, never fabricated.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeasurements {
    /// Verified outcome label, when known.
    #[serde(default)]
    pub outcome: Option<String>,
    /// Per-node execution intervals.
    #[serde(default)]
    pub node_timings: Vec<NodeTiming>,
    /// Maximum independent-evidence concurrency from overlapping intervals.
    #[serde(default)]
    pub max_evidence_concurrency: Option<usize>,
    /// Measured model call count.
    #[serde(default)]
    pub model_calls: Option<u64>,
    /// Measured tool call count.
    #[serde(default)]
    pub tool_calls: Option<u64>,
    /// Estimated tokens, when the provider supplies an estimate.
    #[serde(default)]
    pub estimated_tokens: Option<u64>,
    /// Billed tokens, when the provider reports usage.
    #[serde(default)]
    pub billed_tokens: Option<u64>,
    /// Where token counts came from.
    pub usage_provenance: TokenProvenance,
    /// Changed workspace paths.
    #[serde(default)]
    pub changed_paths: Vec<String>,
    /// Selected verification checks.
    #[serde(default)]
    pub selected_checks: Vec<String>,
    /// Durable task identity.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Task revision.
    #[serde(default)]
    pub revision: Option<u64>,
    /// Recovery outcome label, when a recovery ran.
    #[serde(default)]
    pub recovery: Option<String>,
    /// Wall-clock milliseconds for the whole run.
    #[serde(default)]
    pub wall_ms: Option<u64>,
    /// Milliseconds to first evidence.
    #[serde(default)]
    pub first_evidence_ms: Option<u64>,
    /// Milliseconds to first edit.
    #[serde(default)]
    pub first_edit_ms: Option<u64>,
    /// Milliseconds to final verification.
    #[serde(default)]
    pub final_verification_ms: Option<u64>,
}

/// Maximum number of simultaneously outstanding intervals from real
/// start/end times. Touching endpoints count as serial.
#[must_use]
pub fn max_overlap(intervals: &[(u64, u64)]) -> usize {
    let mut events: Vec<(u64, i32)> = Vec::with_capacity(intervals.len() * 2);
    for (start, end) in intervals {
        if end <= start {
            continue;
        }
        events.push((*start, 1));
        events.push((*end, -1));
    }
    events.sort_unstable();
    let mut current: usize = 0;
    let mut max: usize = 0;
    for (_, delta) in events {
        if delta > 0 {
            current += 1;
            max = max.max(current);
        } else {
            current = current.saturating_sub(1);
        }
    }
    max
}

/// Lifecycle of one debugging stage. Crash recovery moves in-flight work
/// to [`StageStatus::Recovering`], never to `Completed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageStatus {
    /// Planned but not started.
    Planned,
    /// Currently executing.
    InFlight,
    /// Interrupted; safe work may rerun only after explicit continuation.
    Recovering,
    /// Terminal success, granted only by verification.
    Completed,
    /// Terminal failure.
    Failed,
}

/// Marks an in-flight stage as recovering after a restart. Never reports
/// completion for interrupted work.
#[must_use]
pub const fn mark_recovering(status: StageStatus) -> StageStatus {
    match status {
        StageStatus::InFlight => StageStatus::Recovering,
        other => other,
    }
}

/// Tracks the steering revision so delayed-model proposals are discarded
/// once steering lands. Acknowledgement happens only after workers drain,
/// which the supervisor owns; this guard owns the discard decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SteeringState {
    /// Current revision.
    revision: u64,
    /// Whether cancellation was requested.
    cancelled: bool,
}

impl SteeringState {
    /// Starts at revision 0, uncancelled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            revision: 0,
            cancelled: false,
        }
    }

    /// Records accepted steering: bumps the revision.
    pub const fn note_steering(&mut self) {
        self.revision += 1;
    }

    /// Current revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Whether a proposal planned at `planned_revision` is stale and must
    /// be discarded with zero writes.
    #[must_use]
    pub const fn should_discard(&self, planned_revision: u64) -> bool {
        self.cancelled || planned_revision != self.revision
    }

    /// Records cancellation.
    pub const fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Whether cancellation was requested.
    #[must_use]
    pub const fn cancelled(&self) -> bool {
        self.cancelled
    }
}

impl Default for SteeringState {
    fn default() -> Self {
        Self::new()
    }
}

/// How a requested verification check resolved. Renamed, path, workspace
/// and target-specific dependency forms resolve exactly when possible and
/// conservatively broaden to workspace verification otherwise; they are
/// never silently ignored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SelectionResolution {
    /// Resolved to exactly this available check.
    Exact(String),
    /// Broadened to workspace verification with an honest report.
    BroadenedWorkspace,
    /// Silent omission. Never returned; enumerated so call sites cannot
    /// add a quiet path without touching this function.
    Ignored,
}

/// Pinned alias spellings that must resolve to their renamed target.
const RENAMED_ALIASES: [(&str, &str); 1] = [("alpha", "client")];

/// Resolves `requested` against `available` checks: exact names resolve
/// exactly, pinned aliases resolve to their renamed target, and anything
/// else broadens conservatively to workspace verification.
#[must_use]
pub fn resolve_check_selection(requested: &str, available: &[String]) -> SelectionResolution {
    if available.iter().any(|check| check == requested) {
        return SelectionResolution::Exact(requested.to_owned());
    }
    let key = requested.split(':').next().unwrap_or(requested);
    for (alias, target) in RENAMED_ALIASES {
        if key == alias
            && let Some(hit) = available
                .iter()
                .find(|check| check.as_str() == target || check.starts_with(&format!("{target}:")))
        {
            return SelectionResolution::Exact(hit.clone());
        }
    }
    SelectionResolution::BroadenedWorkspace
}

/// Evidence-concurrency helper for the benchmark host: maximum overlap of
/// real node execution intervals. Serial runs record 1 when non-empty.
#[must_use]
pub fn evidence_concurrency(timings: &[NodeTiming]) -> usize {
    let intervals: Vec<(u64, u64)> = timings
        .iter()
        .map(|timing| (timing.start_ms, timing.end_ms))
        .collect();
    max_overlap(&intervals)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_bounds_match_brief() {
        let bounds = RuntimeBounds::default();
        assert_eq!(bounds.max_evidence_requests, 16);
        assert_eq!(bounds.max_evidence_bytes_per_stage, 256 * 1024);
        assert_eq!(bounds.max_patch_files, 8);
        assert_eq!(bounds.max_replacement_bytes, 1024 * 1024);
    }

    #[test]
    fn protected_paths_cover_migrations_and_lockfiles() {
        assert!(is_protected_path("migrations/001.sql"));
        assert!(is_protected_path("Cargo.lock"));
        assert!(!is_protected_path("src/lib.rs"));
    }

    #[test]
    fn serial_intervals_count_one() {
        assert_eq!(max_overlap(&[(0, 5), (5, 10)]), 1);
        assert_eq!(evidence_concurrency(&[]), 0);
    }

    #[test]
    fn unknown_checks_broaden_never_ignore() {
        assert_eq!(
            resolve_check_selection("nope", &["a".to_string()]),
            SelectionResolution::BroadenedWorkspace
        );
    }
}
