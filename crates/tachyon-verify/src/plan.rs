//! Immutable verification inputs and validated scheduler IR.
use crate::{
    AcceptanceContract, Clause, ProjectDetector, RustProjectDetector, VerifyError,
    WorkspaceSnapshot,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tachyon_ir::{
    AccessSet, CancellationPolicy, Dependency, DependencyCondition, EffectClass, ExecutionGraph,
    ExecutionNode, ExecutorKind, Idempotency, Invocation, NodePriority, ResourceClaim, ResourceKey,
    RetryPolicy, SpeculationPolicy, TimeoutPolicy, VerificationRequirement,
};
use tachyon_types::{CapabilityId, NodeId, TaskId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VerificationRisk {
    Affected,
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HardRequirement {
    pub id: uuid::Uuid,
    pub text: String,
}

/// Only trusted construction can create plans; the graph is read-only to callers.
#[derive(Clone, Debug)]
pub struct VerificationPlan {
    pub(crate) task_id: TaskId,
    pub(crate) revision: u64,
    pub(crate) baseline: WorkspaceSnapshot,
    pub(crate) planned: WorkspaceSnapshot,
    pub(crate) contract: AcceptanceContract,
    pub(crate) preflight_failures: Vec<String>,
    pub(crate) binding: String,
    pub(crate) graph: ExecutionGraph,
    pub(crate) checks: BTreeMap<NodeId, Clause>,
}

impl VerificationPlan {
    pub fn build(
        task_id: TaskId,
        revision: u64,
        contract: &AcceptanceContract,
        baseline: &WorkspaceSnapshot,
        hard: &[HardRequirement],
        risk: VerificationRisk,
    ) -> Result<Self, VerifyError> {
        validate_hard_requirements(contract, hard)?;
        let planned = WorkspaceSnapshot::capture(baseline.root())?;
        Self::from_snapshot(task_id, revision, contract, baseline, hard, risk, planned)
    }

    /// Policy-aware constructor. Prefer this at runtime; `build` is for a
    /// trusted local source snapshot boundary without a `ToolsContext`.
    pub fn build_authorized(
        task_id: TaskId,
        revision: u64,
        contract: &AcceptanceContract,
        baseline: &WorkspaceSnapshot,
        hard: &[HardRequirement],
        risk: VerificationRisk,
        context: &tachyon_tools::ToolsContext,
    ) -> Result<Self, VerifyError> {
        validate_hard_requirements(contract, hard)?;
        if context.workspace_root.canonicalize()? != baseline.root() {
            return Err(VerifyError::Blocked(
                "verification context root differs from baseline".into(),
            ));
        }
        let planned = WorkspaceSnapshot::capture_authorized(context)?;
        Self::from_snapshot(task_id, revision, contract, baseline, hard, risk, planned)
    }

    fn from_snapshot(
        task_id: TaskId,
        revision: u64,
        contract: &AcceptanceContract,
        baseline: &WorkspaceSnapshot,
        hard: &[HardRequirement],
        risk: VerificationRisk,
        planned: WorkspaceSnapshot,
    ) -> Result<Self, VerifyError> {
        if planned.root() != baseline.root() {
            return Err(VerifyError::Blocked(
                "snapshot root differs from baseline".into(),
            ));
        }
        let preflight_failures = contract
            .clauses
            .iter()
            .filter_map(|clause| evaluate_clause(clause, baseline, &planned).err())
            .collect();
        let binding = blake3::hash(
            &serde_json::to_vec(&serde_json::json!({
                "task_id": task_id, "revision": revision, "contract": contract,
                "baseline": baseline, "planned": planned, "hard": hard, "risk": risk,
            }))
            .map_err(|err| VerifyError::InvalidContract(err.to_string()))?,
        )
        .to_hex()
        .to_string();
        let mut checks = BTreeMap::new();
        let mut graph = ExecutionGraph::empty(task_id, revision);
        let (broad, focused): (Vec<_>, Vec<_>) = RustProjectDetector
            .commands(baseline, &planned, risk)?
            .into_iter()
            .partition(|command| command.args == ["test", "--offline", "--workspace"]);
        let (explicit, deterministic): (Vec<_>, Vec<_>) = contract
            .clauses
            .iter()
            .cloned()
            .partition(|clause| matches!(leaf(clause), Clause::CommandPasses { .. }));
        let ordered = deterministic
            .into_iter()
            .chain(
                focused
                    .into_iter()
                    .map(|command| Clause::CommandPasses { command }),
            )
            .chain(explicit)
            .chain(
                broad
                    .into_iter()
                    .map(|command| Clause::CommandPasses { command }),
            );
        let mut previous = None;
        for clause in ordered {
            let node = compile_node(task_id, revision, &binding, &clause)?;
            if let Some(from) = previous {
                graph.dependencies.push(Dependency {
                    from,
                    to: node.id,
                    condition: DependencyCondition::OnCompletion,
                });
            }
            previous = Some(node.id);
            checks.insert(node.id, clause);
            graph.nodes.insert(node.id, node);
        }
        graph
            .validate(task_id)
            .map_err(|error| VerifyError::InvalidContract(error.to_string()))?;
        Ok(Self {
            task_id,
            revision,
            baseline: baseline.clone(),
            planned,
            contract: contract.clone(),
            binding,
            graph,
            checks,
            preflight_failures,
        })
    }

    pub(crate) fn validate_node(&self, node: &ExecutionNode) -> Result<(), VerifyError> {
        let clause = self
            .checks
            .get(&node.id)
            .ok_or_else(|| VerifyError::Blocked("missing required check".into()))?;
        // Recompilation is the schema and minimum-effect/access check: exact
        // typed payloads admit no unknown keys, weaker claims or hidden retries.
        let mut required = compile_node(self.task_id, self.revision, &self.binding, clause)?;
        required.id = node.id;
        if *node != required {
            return Err(VerifyError::Blocked(
                "forged verification schema, binding, access or execution policy".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_graph(&self) -> Result<(), VerifyError> {
        if self.graph.version != tachyon_ir::IR_VERSION
            || self.graph.nodes.len() != self.checks.len()
            || self.checks.is_empty()
        {
            return Err(VerifyError::Blocked(
                "missing verification nodes or invalid version".into(),
            ));
        }
        self.graph
            .validate(self.task_id)
            .map_err(|error| VerifyError::Blocked(error.to_string()))?;
        for node in self.graph.nodes.values() {
            self.validate_node(node)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn graph(&self) -> &ExecutionGraph {
        &self.graph
    }
}

/// Require one exact, unique top-level acceptance binding for every canonical
/// task hard requirement, and reject extra bindings absent from task state.
pub fn validate_hard_requirements(
    contract: &AcceptanceContract,
    hard: &[HardRequirement],
) -> Result<(), VerifyError> {
    contract.validate()?;
    let expected: BTreeMap<_, _> = hard.iter().map(|hard| (hard.id, &hard.text)).collect();
    let actual: BTreeMap<_, _> = contract
        .clauses
        .iter()
        .filter_map(|clause| {
            if let Clause::HardConstraint { id, text, .. } = clause {
                Some((*id, text))
            } else {
                None
            }
        })
        .collect();
    if expected.len() != hard.len() || expected != actual {
        return Err(VerifyError::Blocked(
            "missing, duplicate or mismatched hard constraint binding".into(),
        ));
    }
    Ok(())
}

pub(crate) fn evaluate_clause(
    clause: &Clause,
    baseline: &WorkspaceSnapshot,
    current: &WorkspaceSnapshot,
) -> Result<(), String> {
    if baseline.root() != current.root() {
        return Err("foreign snapshot root".into());
    }
    match leaf(clause) {
        Clause::CommandPasses { .. } => Ok(()), // Never evidence of a command passing.
        Clause::Unresolved { description } => Err(format!(
            "unresolved requirement: {}",
            description.chars().take(1_024).collect::<String>()
        )),
        Clause::FileUnchanged { path } => {
            if baseline.directories.contains(path) || current.directories.contains(path) {
                Err(format!(
                    "FileUnchanged needs a file, not a directory: {path}"
                ))
            } else if baseline.files.get(path) == current.files.get(path) {
                Ok(())
            } else {
                Err(format!("protected file changed: {path}"))
            }
        }
        Clause::ChangedPathsWithin { paths } => {
            for changed in baseline.changed_paths(current) {
                if !paths.iter().any(|allowed| {
                    allowed == "."
                        || changed == *allowed
                        || changed
                            .strip_prefix(allowed)
                            .is_some_and(|rest| rest.starts_with('/'))
                }) {
                    return Err(format!("changed path outside allowed scope: {changed}"));
                }
            }
            Ok(())
        }
        Clause::HardConstraint { .. } => Err("nested hard constraint".into()),
    }
}

pub(crate) fn leaf(clause: &Clause) -> &Clause {
    if let Clause::HardConstraint { check, .. } = clause {
        check
    } else {
        clause
    }
}

fn compile_node(
    task_id: TaskId,
    revision: u64,
    binding: &str,
    clause: &Clause,
) -> Result<ExecutionNode, VerifyError> {
    let command = if let Clause::CommandPasses { command } = leaf(clause) {
        Some(command)
    } else {
        None
    };
    if let Some(command) = command {
        command.validate()?;
    }
    let root = ResourceKey::parse("dir:/workspace/**")
        .map_err(|err| VerifyError::InvalidContract(err.to_string()))?;
    let (capability, args) = if let Some(command) = command {
        (
            "verify.command",
            serde_json::json!({"binding": binding, "command": command}),
        )
    } else {
        (
            "verify.clause",
            serde_json::json!({"binding": binding, "clause": clause}),
        )
    };
    Ok(ExecutionNode {
        id: NodeId::generate(),
        task_id,
        planned_revision: revision,
        executor: ExecutorKind::Verification,
        invocation: Invocation {
            capability: CapabilityId(capability.into()),
            args,
        },
        inputs: vec![],
        expected_outputs: vec![],
        access: if command.is_some() {
            AccessSet {
                reads: vec![],
                writes: vec![root],
            }
        } else {
            AccessSet {
                reads: vec![root],
                writes: vec![],
            }
        },
        resources: ResourceClaim {
            process_slots: u8::from(command.is_some()),
            ..ResourceClaim::default()
        },
        effect_class: if command.is_some() {
            EffectClass::DestructiveLocalMutation
        } else {
            EffectClass::ReadOnlyLocal
        },
        idempotency: if command.is_some() {
            Idempotency::Unknown
        } else {
            Idempotency::Pure
        },
        speculation: SpeculationPolicy::Forbidden,
        timeout: TimeoutPolicy {
            hard_ms: Some(command.map_or(30_000, |command| command.timeout_ms + 30_000)),
        },
        retry: RetryPolicy::default(),
        cancellation: CancellationPolicy::Immediate,
        verification: vec![VerificationRequirement {
            description: "required verification evidence".into(),
        }],
        priority: NodePriority::High,
    })
}
