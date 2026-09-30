//! Tachyon Ir.
//!
//! Validated, machine-readable execution graphs (spec §5–§9).
//!
//! Every scheduler-visible operation exists here first: identity, executor,
//! invocation, dataflow bindings, access/effect/resource declarations, and
//! retry/cancellation/verification metadata. [`ExecutionGraph::validate`]
//! rejects unknown references, cycles, non-ancestor dataflow, speculative
//! mutation, and malformed invocations before anything runs.

#![warn(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use tachyon_types::{CapabilityId, NodeId, ProviderId, TaskId};

/// IR schema version. Bump on any breaking graph change.
pub const IR_VERSION: u16 = 1;

/// Errors produced while validating an [`ExecutionGraph`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IrError {
    /// The graph schema version is not understood by this binary.
    #[error("unsupported execution graph version {got}; expected {expected}")]
    UnsupportedVersion {
        /// Expected schema version.
        expected: u16,
        /// Version found in the graph.
        got: u16,
    },
    /// A node map key differs from the identity stored inside its node.
    #[error("node map key {key} does not match node identity {node_id}")]
    NodeKeyMismatch {
        /// Key under which the node was stored.
        key: NodeId,
        /// Identity declared by the node.
        node_id: NodeId,
    },
    /// A dependency names a node absent from [`ExecutionGraph::nodes`].
    #[error("dependency references unknown node {0}")]
    UnknownNode(NodeId),
    /// Dependencies form a cycle; the graph is not a DAG.
    #[error("dependency cycle detected")]
    Cycle,
    /// A node belongs to a different task than the graph.
    #[error("node {0} belongs to another task")]
    ForeignTask(NodeId),
    /// An input binding names a node that is not an ancestor.
    #[error("node {node} reads input from non-ancestor {from}")]
    DanglingInput {
        /// Node holding the binding.
        node: NodeId,
        /// Purported upstream node.
        from: NodeId,
    },
    /// A speculative node declares a mutating effect. MVP speculation is
    /// pure/read-only only (spec §14).
    #[error("speculative node {0} declares a mutating effect")]
    SpeculativeMutation(NodeId),
    /// Invocation names no capability.
    #[error("node {0} has an empty capability")]
    EmptyCapability(NodeId),
    /// Invocation args must be a JSON object.
    #[error("node {0} has non-object invocation args")]
    InvalidArgs(NodeId),
    /// Retry policy must allow at least one attempt.
    #[error("node {0} declares zero retry attempts")]
    InvalidRetry(NodeId),
    /// Automatic retries require semantics the scheduler can prove safe.
    #[error("node {0} requests retries without pure or idempotent semantics")]
    UnsafeRetry(NodeId),
    /// Resource key does not parse (`kind:path`, no `..`).
    #[error("invalid resource key {key:?}: {reason}")]
    InvalidResource {
        /// Offending key text.
        key: String,
        /// Why it was rejected.
        reason: String,
    },
}

/// Where a node executes (spec §5). Used as an executor-registry key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExecutorKind {
    /// In-process deterministic computation.
    Native,
    /// Repository intelligence queries.
    Repository,
    /// Evidence retrieval and ranking.
    Retrieval,
    /// Native tool capabilities.
    Tool,
    /// Bounded semantic judgment.
    Judgment,
    /// Model reasoning.
    Model,
    /// Filesystem mutation batches.
    Mutation,
    /// Verification commands.
    Verification,
    /// Commit barrier; never executes work itself.
    Barrier,
}

/// No capability contract recorded: legacy journalled invocations and
/// nodes whose execution does not depend on a versioned capability
/// contract. A production dispatcher must never infer a contract from
/// this value (ADR-0006 §10).
pub const CAPABILITY_CONTRACT_NONE: u16 = 0;

/// What to run: a capability plus schema-validated JSON args (spec §5).
/// Provider-native tool calls enter the graph only as proposed
/// invocations; validation and policy treat them like any other.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invocation {
    /// Capability id from the registry.
    pub capability: CapabilityId,
    /// Arguments object.
    pub args: Value,
    /// Capability contract version this invocation was compiled under,
    /// persisted independently of [`IR_VERSION`] so recovery never
    /// reinterprets an invocation under newer semantics (ADR-0006 §10).
    /// Absent in legacy journalled graphs, which deserialize to
    /// [`CAPABILITY_CONTRACT_NONE`] and are never executed by inference.
    #[serde(default)]
    pub contract_version: u16,
}

/// One named input fed from an ancestor's structured output (spec §8).
/// Hidden implicit data dependencies are prohibited: every cross-node
/// value flows through a binding validated here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputBinding {
    /// Local input name.
    pub name: String,
    /// Ancestor node producing the value.
    pub from: NodeId,
    /// JSON pointer into the ancestor's output (RFC 6901, e.g. `/files/0`).
    pub pointer: String,
}

/// One named output a node promises to produce.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputBinding {
    /// Output name.
    pub name: String,
}

/// Normalized resource key: `kind:/path` or `kind:/path/**` (spec §9).
/// The `/**` suffix marks a recursive claim over all descendants.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceKey(pub String);

/// Parsed key parts (kind validated at parse; overlap is segment-based).
struct KeyParts {
    segments: Vec<String>,
    recursive: bool,
}

impl ResourceKey {
    /// Parses and normalizes `kind:path`. Rejects empty kinds/paths,
    /// `..` segments, and `.` segments are dropped.
    pub fn parse(raw: &str) -> Result<Self, IrError> {
        let (kind, path) = raw
            .split_once(':')
            .ok_or_else(|| IrError::InvalidResource {
                key: raw.to_owned(),
                reason: "expected `kind:path`".to_owned(),
            })?;
        if kind.is_empty() {
            return Err(IrError::InvalidResource {
                key: raw.to_owned(),
                reason: "empty kind".to_owned(),
            });
        }
        let recursive = path.strip_suffix("/**").is_some();
        let body = path.strip_suffix("/**").unwrap_or(path);
        let mut segments = Vec::new();
        for segment in body.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            if segment == ".." {
                return Err(IrError::InvalidResource {
                    key: raw.to_owned(),
                    reason: "`..` segments are not allowed".to_owned(),
                });
            }
            segments.push(segment.to_owned());
        }
        if segments.is_empty() {
            return Err(IrError::InvalidResource {
                key: raw.to_owned(),
                reason: "empty path".to_owned(),
            });
        }
        let mut normalized = format!("{kind}:/{}", segments.join("/"));
        if recursive {
            normalized.push_str("/**");
        }
        Ok(Self(normalized))
    }

    fn parts(&self) -> KeyParts {
        let path = self
            .0
            .split_once(':')
            .map_or(self.0.as_str(), |(_, path)| path);
        let recursive = path.strip_suffix("/**").is_some();
        let body = path.strip_suffix("/**").unwrap_or(path);
        KeyParts {
            segments: body
                .split('/')
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
            recursive,
        }
    }

    /// Hierarchical overlap (spec §9): equal paths overlap; a recursive
    /// key overlaps its own subtree in either direction. Comparison is on
    /// path segments alone — kinds are labels, so a `dir:` claim and a
    /// `file:` claim over the same path conservatively conflict.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        let left = self.parts();
        let right = other.parts();
        if left.segments == right.segments {
            return true;
        }
        if left.recursive && starts_with(&right.segments, &left.segments) {
            return true;
        }
        if right.recursive && starts_with(&left.segments, &right.segments) {
            return true;
        }
        false
    }
}

fn starts_with(path: &[String], prefix: &[String]) -> bool {
    path.len() >= prefix.len() && path[..prefix.len()] == *prefix
}

/// Declared data access (spec §9). Read/read is compatible; anything
/// involving a write conflicts on overlap.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessSet {
    /// Resources read.
    pub reads: Vec<ResourceKey>,
    /// Resources written.
    pub writes: Vec<ResourceKey>,
}

impl AccessSet {
    /// True when both sets cannot run concurrently.
    #[must_use]
    pub fn conflicts_with(&self, other: &Self) -> bool {
        for write in &self.writes {
            if other
                .reads
                .iter()
                .chain(&other.writes)
                .any(|key| write.overlaps(key))
            {
                return true;
            }
        }
        for write in &other.writes {
            if self.reads.iter().any(|key| write.overlaps(key)) {
                return true;
            }
        }
        false
    }
}

/// Capacity a node asks the scheduler to reserve (spec §10). Claims are
/// initial estimates; the scheduler calibrates them from measurements later.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceClaim {
    /// CPU shares (whole cores = 1000).
    pub cpu_units: u16,
    /// Memory in MiB, when known.
    pub memory_mb: Option<u32>,
    /// Process slots (build/test runners).
    pub process_slots: u8,
    /// Concurrent network operations.
    pub network_slots: u8,
    /// GPU memory in MiB, when claimed.
    pub gpu_memory_mb: Option<u32>,
    /// Pinned provider, when the node needs one.
    pub provider: Option<ProviderId>,
}

impl Default for ResourceClaim {
    fn default() -> Self {
        Self {
            cpu_units: 100,
            memory_mb: None,
            process_slots: 0,
            network_slots: 0,
            gpu_memory_mb: None,
            provider: None,
        }
    }
}

/// Consequence class of running the node (spec §19).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectClass {
    /// No observable effect (pure computation).
    Pure,
    /// Local reads only.
    ReadOnlyLocal,
    /// Local writes that can be undone.
    ReversibleLocalMutation,
    /// Local writes that cannot be undone.
    DestructiveLocalMutation,
    /// External writes with a recovery path.
    ReversibleExternalMutation,
    /// External writes without one.
    DestructiveExternalMutation,
    /// Credential, privilege, or deployment scope.
    Privileged,
}

impl EffectClass {
    /// MVP speculation allows only side-effect-free nodes (spec §14).
    #[must_use]
    pub fn speculation_safe(self) -> bool {
        matches!(self, Self::Pure | Self::ReadOnlyLocal)
    }
}

/// What recovery may assume after a crash (spec §19).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Idempotency {
    /// No effect; always safe to rerun.
    Pure,
    /// Same inputs always produce the same state.
    Idempotent,
    /// Safe to retry with the same key.
    Keyed,
    /// Remote state can be inspected to decide.
    Queryable,
    /// A declared compensation exists.
    Compensatable,
    /// Must never be blindly replayed.
    NonIdempotent,
    /// Semantics not yet declared; treated as non-idempotent.
    Unknown,
}

/// Whether the scheduler may run this node speculatively (spec §14).
/// MVP speculation never mutates: validated in [`ExecutionGraph::validate`]
/// and re-checked at dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpeculationPolicy {
    /// Never speculative.
    Forbidden,
    /// May run ahead when capacity is free.
    Allowed,
    /// Prefer running early.
    Preferred,
}

/// Cooperative wall-clock deadline for one attempt. At the deadline the
/// scheduler signals cancellation but keeps grants until the executor settles.
/// `None` means no deadline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeoutPolicy {
    /// Deadline in milliseconds.
    pub hard_ms: Option<u64>,
}

/// Retry policy owned by the IR: no hidden infinite retries (spec §40).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts including the first (minimum 1).
    pub attempts: u32,
    /// Backoff between attempts in milliseconds.
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 1,
            backoff_ms: 0,
        }
    }
}

/// How the scheduler stops a running node (spec §12).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancellationPolicy {
    /// Request stop at the earliest safe point. The scheduler keeps grants
    /// until the executor reports that its work has settled.
    Immediate,
    /// The executor may use this grace period for orderly stop and drain.
    /// The scheduler never releases grants merely because it elapsed.
    Graceful {
        /// Grace period in milliseconds.
        grace_ms: u64,
    },
    /// Once the commit point is crossed, finish or reconcile the effect
    /// before returning, even if cancellation was requested.
    NonCancellableAfterCommit,
}

/// One acceptance check this node feeds. Typed clause kinds arrive with
/// Milestone 9; the description is enough for planning until then.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationRequirement {
    /// What the check establishes.
    pub description: String,
}

/// Explicit scheduling priority; the scheduler adds critical-path weight,
/// age bonus, and speculation/resource penalties around it (spec §13).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePriority {
    /// Background work; yields to everything.
    Low,
    /// Default.
    #[default]
    Normal,
    /// User-visible or blocking work.
    High,
    /// Must run as soon as grantable.
    Critical,
}

impl NodePriority {
    #[must_use]
    pub fn score(self) -> f64 {
        match self {
            Self::Low => -10.0,
            Self::Normal => 0.0,
            Self::High => 10.0,
            Self::Critical => 20.0,
        }
    }
}

/// When a dependency edge is satisfied (spec §8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyCondition {
    /// Parent succeeded.
    OnSuccess,
    /// Parent failed.
    OnFailure,
    /// Parent reached any terminal status.
    OnCompletion,
}

/// Ordering edge: `to` waits for `from` per `condition`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dependency {
    /// Upstream node.
    pub from: NodeId,
    /// Downstream node.
    pub to: NodeId,
    /// Which upstream outcome releases the edge.
    pub condition: DependencyCondition,
}

/// Scheduler-visible node state (spec §7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Waiting on dependencies.
    Pending,
    /// Dependencies satisfied; waiting on grants.
    Ready,
    /// Executing.
    Running,
    /// Work before a commit barrier is done; effect not yet committed.
    Prepared,
    /// Blocked on a policy approval.
    WaitingApproval,
    /// Finished successfully.
    Succeeded,
    /// Finished with failure.
    Failed,
    /// Cancelled; will not run.
    Cancelled,
    /// A dependency can never be satisfied; will not run.
    Skipped,
    /// Crashed mid-effect with unknown outcome; needs reconciliation.
    UnknownAfterCrash,
}

impl NodeStatus {
    /// Terminal states release dependents and free grants.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

/// One scheduled operation (spec §5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNode {
    /// Unique node identity.
    pub id: NodeId,
    /// Task that owns this node.
    pub task_id: TaskId,
    /// Task revision this node was planned against.
    pub planned_revision: u64,
    /// Where the node runs.
    pub executor: ExecutorKind,
    /// Capability plus args.
    pub invocation: Invocation,
    /// Named values fed from ancestors.
    pub inputs: Vec<InputBinding>,
    /// Named values produced for descendants.
    pub expected_outputs: Vec<OutputBinding>,
    /// Declared data access.
    pub access: AccessSet,
    /// Declared capacity needs.
    pub resources: ResourceClaim,
    /// Consequence class.
    pub effect_class: EffectClass,
    /// Crash-recovery semantics.
    pub idempotency: Idempotency,
    /// Speculative execution permission.
    pub speculation: SpeculationPolicy,
    /// Per-attempt wall-clock bound.
    pub timeout: TimeoutPolicy,
    /// Retry budget.
    pub retry: RetryPolicy,
    /// Stop behavior.
    pub cancellation: CancellationPolicy,
    /// Acceptance checks this node feeds.
    pub verification: Vec<VerificationRequirement>,
    /// Explicit priority.
    pub priority: NodePriority,
}

/// A validated, dependency-aware execution graph (spec §5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraph {
    /// Schema version; always [`IR_VERSION`] for graphs built here.
    pub version: u16,
    /// Nodes by id.
    pub nodes: BTreeMap<NodeId, ExecutionNode>,
    /// Ordering edges.
    pub dependencies: Vec<Dependency>,
}

impl ExecutionGraph {
    /// Creates an empty graph for `task_id` at `revision`.
    #[must_use]
    pub fn empty(_task_id: TaskId, _revision: u64) -> Self {
        Self {
            version: IR_VERSION,
            nodes: BTreeMap::new(),
            dependencies: Vec::new(),
        }
    }

    /// All strict ancestors of `node`: nodes with a directed path to it.
    #[must_use]
    pub fn ancestors(&self, node: NodeId) -> BTreeSet<NodeId> {
        let mut incoming: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
        for edge in &self.dependencies {
            incoming.entry(edge.to).or_default().push(edge.from);
        }
        let mut seen = BTreeSet::new();
        let mut stack: Vec<NodeId> = incoming.get(&node).cloned().unwrap_or_default();
        while let Some(current) = stack.pop() {
            if seen.insert(current) {
                stack.extend(incoming.get(&current).cloned().unwrap_or_default());
            }
        }
        seen
    }

    /// Validates structural and semantic invariants (spec §6):
    /// ownership, reference existence, acyclicity, input ancestry,
    /// speculation/effect consistency, invocation shape, retry budget.
    /// Capability-schema and hard-constraint checks belong to the
    /// Milestone 3 registry and core; the invocation shape check here
    /// (non-empty capability, object args) is the M2 prerequisite.
    pub fn validate(&self, task_id: TaskId) -> Result<(), IrError> {
        if self.version != IR_VERSION {
            return Err(IrError::UnsupportedVersion {
                expected: IR_VERSION,
                got: self.version,
            });
        }
        for (key, node) in &self.nodes {
            if *key != node.id {
                return Err(IrError::NodeKeyMismatch {
                    key: *key,
                    node_id: node.id,
                });
            }
            if node.task_id != task_id {
                return Err(IrError::ForeignTask(node.id));
            }
            if node.invocation.capability.0.is_empty() {
                return Err(IrError::EmptyCapability(node.id));
            }
            if !node.invocation.args.is_object() {
                return Err(IrError::InvalidArgs(node.id));
            }
            if node.retry.attempts == 0 {
                return Err(IrError::InvalidRetry(node.id));
            }
            if node.retry.attempts > 1
                && !matches!(
                    node.idempotency,
                    Idempotency::Pure | Idempotency::Idempotent
                )
            {
                return Err(IrError::UnsafeRetry(node.id));
            }
            if !matches!(node.speculation, SpeculationPolicy::Forbidden)
                && !node.effect_class.speculation_safe()
            {
                return Err(IrError::SpeculativeMutation(node.id));
            }
            for key in node.access.reads.iter().chain(&node.access.writes) {
                ResourceKey::parse(&key.0)?;
            }
        }
        for edge in &self.dependencies {
            if !self.nodes.contains_key(&edge.from) {
                return Err(IrError::UnknownNode(edge.from));
            }
            if !self.nodes.contains_key(&edge.to) {
                return Err(IrError::UnknownNode(edge.to));
            }
        }
        if has_cycle(&self.nodes, &self.dependencies) {
            return Err(IrError::Cycle);
        }
        for node in self.nodes.values() {
            let ancestors = self.ancestors(node.id);
            for input in &node.inputs {
                if !self.nodes.contains_key(&input.from) {
                    return Err(IrError::UnknownNode(input.from));
                }
                if !ancestors.contains(&input.from) {
                    return Err(IrError::DanglingInput {
                        node: node.id,
                        from: input.from,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Iterative depth-first cycle detection over `from -> to` edges.
fn has_cycle(nodes: &BTreeMap<NodeId, ExecutionNode>, edges: &[Dependency]) -> bool {
    let mut outgoing: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
    for edge in edges {
        outgoing.entry(edge.from).or_default().push(edge.to);
    }
    let mut visited: BTreeSet<NodeId> = BTreeSet::new();
    let mut visiting: BTreeSet<NodeId> = BTreeSet::new();
    for root in nodes.keys() {
        if visited.contains(root) {
            continue;
        }
        let mut stack: Vec<(NodeId, bool)> = vec![(*root, false)];
        while let Some((node, exiting)) = stack.pop() {
            if exiting {
                visiting.remove(&node);
                continue;
            }
            if !visited.insert(node) {
                if visiting.contains(&node) {
                    return true;
                }
                continue;
            }
            visiting.insert(node);
            stack.push((node, true));
            if let Some(next) = outgoing.get(&node) {
                for child in next {
                    if !visited.contains(child) || visiting.contains(child) {
                        stack.push((*child, false));
                    }
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        AccessSet, CAPABILITY_CONTRACT_NONE, EffectClass, ExecutionGraph, IR_VERSION, Idempotency,
        Invocation, IrError,
    };
    use super::{CancellationPolicy, ExecutorKind, TimeoutPolicy};
    use super::{Dependency, DependencyCondition, ExecutionNode};
    use super::{NodePriority, ResourceClaim, ResourceKey, RetryPolicy, SpeculationPolicy};
    use tachyon_types::{CapabilityId, NodeId, TaskId};

    fn node(task: TaskId) -> ExecutionNode {
        ExecutionNode {
            id: NodeId::generate(),
            task_id: task,
            planned_revision: 0,
            executor: ExecutorKind::Native,
            invocation: Invocation {
                capability: CapabilityId("test.noop".to_owned()),
                args: serde_json::json!({}),
                contract_version: CAPABILITY_CONTRACT_NONE,
            },
            inputs: vec![],
            expected_outputs: vec![],
            access: AccessSet::default(),
            resources: ResourceClaim::default(),
            effect_class: EffectClass::Pure,
            idempotency: Idempotency::Pure,
            speculation: SpeculationPolicy::Forbidden,
            timeout: TimeoutPolicy::default(),
            retry: RetryPolicy::default(),
            cancellation: CancellationPolicy::Immediate,
            verification: vec![],
            priority: NodePriority::Normal,
        }
    }

    fn edge(from: NodeId, to: NodeId) -> Dependency {
        Dependency {
            from,
            to,
            condition: DependencyCondition::OnSuccess,
        }
    }

    fn assemble(nodes: Vec<ExecutionNode>, dependencies: Vec<Dependency>) -> ExecutionGraph {
        ExecutionGraph {
            version: super::IR_VERSION,
            nodes: nodes.into_iter().map(|n| (n.id, n)).collect(),
            dependencies,
        }
    }

    #[test]
    fn empty_graph_is_valid() {
        let task = TaskId::generate();
        let graph = ExecutionGraph::empty(task, 0);
        assert_eq!(graph.validate(task), Ok(()));
    }

    #[test]
    fn unsupported_graph_version_is_rejected() {
        let task = TaskId::generate();
        let mut graph = ExecutionGraph::empty(task, 0);
        graph.version = IR_VERSION + 1;
        assert_eq!(
            graph.validate(task),
            Err(IrError::UnsupportedVersion {
                expected: IR_VERSION,
                got: IR_VERSION + 1,
            })
        );
    }

    #[test]
    fn node_map_key_must_match_node_identity() {
        let task = TaskId::generate();
        let node = node(task);
        let key = NodeId::generate();
        let node_id = node.id;
        let graph = ExecutionGraph {
            version: IR_VERSION,
            nodes: std::collections::BTreeMap::from([(key, node)]),
            dependencies: vec![],
        };
        assert_eq!(
            graph.validate(task),
            Err(IrError::NodeKeyMismatch { key, node_id })
        );
    }

    #[test]
    fn linear_chain_is_valid() {
        let task = TaskId::generate();
        let first = node(task);
        let second = node(task);
        let graph = assemble(
            vec![first.clone(), second.clone()],
            vec![edge(first.id, second.id)],
        );
        assert_eq!(graph.validate(task), Ok(()));
    }

    #[test]
    fn cycle_is_rejected() {
        let task = TaskId::generate();
        let first = node(task);
        let second = node(task);
        let graph = assemble(
            vec![first.clone(), second.clone()],
            vec![edge(first.id, second.id), edge(second.id, first.id)],
        );
        assert_eq!(graph.validate(task), Err(IrError::Cycle));
    }

    #[test]
    fn dangling_and_foreign_nodes_are_rejected() {
        let task = TaskId::generate();
        let first = node(task);
        let graph = assemble(
            vec![first.clone()],
            vec![edge(first.id, NodeId::generate())],
        );
        assert!(matches!(graph.validate(task), Err(IrError::UnknownNode(_))));

        let foreign = node(TaskId::generate());
        let graph = assemble(vec![foreign.clone()], vec![]);
        assert_eq!(graph.validate(task), Err(IrError::ForeignTask(foreign.id)));
    }

    #[test]
    fn input_from_non_ancestor_is_rejected() {
        let task = TaskId::generate();
        let first = node(task);
        let mut second = node(task);
        second.inputs.push(super::InputBinding {
            name: "x".to_owned(),
            from: first.id,
            pointer: "/result".to_owned(),
        });
        // No edge: not an ancestor.
        let graph = assemble(vec![first.clone(), second.clone()], vec![]);
        assert!(matches!(
            graph.validate(task),
            Err(IrError::DanglingInput { .. })
        ));
        // With an edge the same binding is valid.
        let graph = assemble(
            vec![first.clone(), second.clone()],
            vec![edge(first.id, second.id)],
        );
        assert_eq!(graph.validate(task), Ok(()));
    }

    #[test]
    fn speculative_mutation_is_rejected() {
        let task = TaskId::generate();
        let mut speculative = node(task);
        speculative.speculation = SpeculationPolicy::Allowed;
        speculative.effect_class = EffectClass::ReversibleLocalMutation;
        let graph = assemble(vec![speculative.clone()], vec![]);
        assert_eq!(
            graph.validate(task),
            Err(IrError::SpeculativeMutation(speculative.id))
        );

        speculative.effect_class = EffectClass::ReadOnlyLocal;
        let graph = assemble(vec![speculative], vec![]);
        assert_eq!(graph.validate(task), Ok(()));
    }

    #[test]
    fn resource_overlap_follows_hierarchy() {
        let dir = ResourceKey::parse("dir:/workspace/src/**").unwrap();
        let file = ResourceKey::parse("dir:/workspace/src/auth.rs").unwrap();
        let other = ResourceKey::parse("dir:/workspace/other.rs").unwrap();
        let sibling = ResourceKey::parse("dir:/workspace/src/**").unwrap();
        assert!(dir.overlaps(&file));
        assert!(file.overlaps(&dir));
        assert!(!dir.overlaps(&other));
        assert!(dir.overlaps(&sibling));
        assert!(file.overlaps(&file));
        assert!(!file.overlaps(&other));

        let empty_reads = AccessSet::default();
        let writer = AccessSet {
            reads: vec![],
            writes: vec![file.clone()],
        };
        let reader = AccessSet {
            reads: vec![file.clone()],
            writes: vec![],
        };
        let unrelated = AccessSet {
            reads: vec![other],
            writes: vec![],
        };
        assert!(!empty_reads.conflicts_with(&empty_reads));
        assert!(!empty_reads.conflicts_with(&reader));
        assert!(writer.conflicts_with(&reader));
        assert!(writer.conflicts_with(&writer));
        assert!(!writer.conflicts_with(&unrelated));
        assert!(!reader.conflicts_with(&unrelated));
    }

    #[test]
    fn malformed_nodes_are_rejected() {
        let task = TaskId::generate();
        let mut bad = node(task);
        bad.invocation.capability = CapabilityId(String::new());
        assert_eq!(
            assemble(vec![bad.clone()], vec![]).validate(task),
            Err(IrError::EmptyCapability(bad.id))
        );

        let mut bad = node(task);
        bad.invocation.args = serde_json::json!([]);
        assert_eq!(
            assemble(vec![bad.clone()], vec![]).validate(task),
            Err(IrError::InvalidArgs(bad.id))
        );

        let mut bad = node(task);
        bad.retry.attempts = 0;
        assert_eq!(
            assemble(vec![bad.clone()], vec![]).validate(task),
            Err(IrError::InvalidRetry(bad.id))
        );

        let mut bad = node(task);
        bad.access.writes = vec![ResourceKey("dir:/a/../b".to_owned())];
        assert!(matches!(
            assemble(vec![bad], vec![]).validate(task),
            Err(IrError::InvalidResource { .. })
        ));
    }

    #[test]
    fn retries_require_pure_or_idempotent_effects() {
        let task = TaskId::generate();
        for idempotency in [
            Idempotency::Keyed,
            Idempotency::Queryable,
            Idempotency::Compensatable,
            Idempotency::NonIdempotent,
            Idempotency::Unknown,
        ] {
            let mut node = node(task);
            node.idempotency = idempotency;
            node.retry.attempts = 2;
            assert_eq!(
                assemble(vec![node.clone()], vec![]).validate(task),
                Err(IrError::UnsafeRetry(node.id)),
                "must not auto-retry {idempotency:?} without a capability reconciler"
            );
        }

        for idempotency in [Idempotency::Pure, Idempotency::Idempotent] {
            let mut node = node(task);
            node.idempotency = idempotency;
            node.retry.attempts = 2;
            assert_eq!(assemble(vec![node], vec![]).validate(task), Ok(()));
        }
    }

    #[test]
    fn ancestors_cover_diamonds() {
        let task = TaskId::generate();
        let top = node(task);
        let left = node(task);
        let right = node(task);
        let bottom = node(task);
        let graph = assemble(
            vec![top.clone(), left.clone(), right.clone(), bottom.clone()],
            vec![
                edge(top.id, left.id),
                edge(top.id, right.id),
                edge(left.id, bottom.id),
                edge(right.id, bottom.id),
            ],
        );
        assert_eq!(graph.validate(task), Ok(()));
        assert_eq!(
            graph.ancestors(bottom.id),
            [top.id, left.id, right.id].into_iter().collect()
        );
        assert!(graph.ancestors(top.id).is_empty());
    }
}
