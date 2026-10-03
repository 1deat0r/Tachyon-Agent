//! Context assembly and trust (spec §27).
//!
//! Typed [`ContextBlock`]s carry [`TrustLevel`] from source to model:
//! repository text is data ([`TrustLevel::WorkspaceData`]), never authority.
//! Assembly applies the spec reduction order — dedupe, collapse repeated
//! diagnostics, prefer excerpts, retain provenance, reserve output budget,
//! then deterministic truncation of lowest-priority blocks first. Semantic
//! summarization is out of scope for M6: truncation is explicit and lossless
//! in provenance. [`assemble_slice`] wraps one assembly as a
//! [`ContextSlice`]: purpose, state revision, parent references, and every
//! omission with its reason, under a content-addressed [`SliceId`]
//! (ADR-0007).
//!
//! Token counts are deterministic `chars / 4` estimates. They bound context
//! spends, not bills; per-provider calibration lands with telemetry (M13).

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use tachyon_retrieval::{EvidenceItem, EvidencePackage};
use tachyon_types::Timestamp;

/// Deterministic chars-per-token estimate. Documented approximation, not a
/// tokenizer; stable across runs and providers.
pub const CHARS_PER_TOKEN: u32 = 4;

/// Per-block excerpt cap: evidence prefers relevant excerpts to whole files
/// (spec §27 step 3). Provenance footers always survive truncation.
pub const MAX_EXCERPT_CHARS: usize = 4_000;

/// Priorities below this are pinned: never dropped for budget, only
/// char-truncated when nothing else can give.
const PINNED_PRIORITY_CUTOFF: u16 = 60;

/// What a block is for. Lower priority number means truncated later.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    /// Harness instructions. Only Tachyon writes these.
    System,
    /// The user's objective verbatim.
    Objective,
    /// Supervisor-recorded task constraints, separate from prompt text.
    Constraint,
    /// Retrieved material (findings, contradictions, gaps).
    Evidence,
    /// Earlier conversation turns.
    History(HistorySpeaker),
}

/// Which side of the conversation a history block replays.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistorySpeaker {
    /// The user. Trusted as user text, never as system authority.
    User,
    /// Earlier model output. Data, not authority.
    Assistant,
}

/// Trust classes (spec §27). Repository files and retrieved external text are
/// data, not authority: assembly never labels them `System` or `User`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    /// Harness-written instructions only.
    System,
    /// The user's own words.
    User,
    /// Tachyon-generated summaries (gap lists, quantitative rollups).
    WorkspaceTrusted,
    /// Same-workspace repository, file, git, and process evidence.
    WorkspaceData,
    /// Anything fetched past the workspace boundary.
    ExternalUntrusted,
}

/// Where a supervisor-recorded constraint originated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintOrigin {
    /// Stated by the user.
    User,
    /// Added by policy.
    Policy,
    /// Inferred from the workspace.
    Workspace,
    /// Added by the harness.
    System,
    /// Derived from other task state.
    Derived,
}

/// Whether a supervisor-recorded constraint is binding or advisory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintStrength {
    /// Cannot be weakened by model output.
    Hard,
    /// Advisory preference.
    Preference,
}

/// A task constraint supplied to context assembly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextConstraint {
    /// Origin used for provenance and trust classification.
    pub source: ConstraintOrigin,
    /// Binding strength.
    pub strength: ConstraintStrength,
    /// Constraint text.
    pub text: String,
}

/// One typed, trusted, prioritized context unit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextBlock {
    /// What the block is for.
    pub kind: ContextKind,
    /// Human-readable origin (`repo.symbol.search:src/auth.rs`).
    pub provenance: String,
    /// Trust class assigned at assembly; never upgraded afterwards.
    pub trust: TrustLevel,
    /// Block text. Evidence blocks end with a provenance footer that
    /// truncation preserves.
    pub content: String,
    /// Lower survives longer. `System` 0, objective 1, contradictions 60+,
    /// findings 100+, history 200+, gaps 300+.
    pub priority: u16,
    /// When the block was assembled.
    pub created_at: Timestamp,
}

/// One earlier conversation turn, replayed as history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryTurn {
    /// Which side spoke.
    pub speaker: HistorySpeaker,
    /// Turn text verbatim.
    pub content: String,
}

/// Content-addressed identity of a [`ContextSlice`]: hex `BLAKE3-256` over
/// the slice's deterministic content. Timestamps never participate, so
/// identical inputs mint the same id (ADR-0007).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SliceId(pub String);

impl std::fmt::Display for SliceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why an item was not included whole in a [`ContextSlice`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmissionReason {
    /// Removed by the spec §27 dedupe step: an earlier block covers it.
    Duplicate,
    /// Dropped whole to fit the context budget.
    BudgetDrop,
    /// Kept but cut by budget truncation; the cut part is omitted.
    Truncated,
}

/// One item the assembly did not include whole, with the reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmittedItem {
    /// Kind of item it was.
    pub kind: ContextKind,
    /// Block provenance at omission time.
    pub provenance: String,
    /// Why the item, or its cut part, is absent.
    pub reason: OmissionReason,
}

/// How a [`ContextSlice`] was requested: lineage recorded without copying
/// any ancestor content (ADR-0007).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceLineage {
    /// Why the slice exists; the shared driver uses `proposal`.
    pub purpose: String,
    /// Task-state revision the slice represents, when one was observed.
    pub state_revision: Option<u64>,
    /// Ids of ancestor slices; referenced, never concatenated.
    pub parent_ids: Vec<SliceId>,
}

/// One purpose-specific, content-addressed projection of task state for a
/// single model call (spec §27, ADR-0007). Conversation inside it is data;
/// the authoritative bindings are the revision and the references.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextSlice {
    /// Content address over everything below except timestamps.
    pub id: SliceId,
    /// Why the slice was built.
    pub purpose: String,
    /// Task-state revision the slice represents, when observed.
    pub state_revision: Option<u64>,
    /// Ancestor slice ids; referenced, never concatenated.
    pub parent_ids: Vec<SliceId>,
    /// Included blocks, in assembly order.
    pub blocks: Vec<ContextBlock>,
    /// Items omitted or cut, each with a reason.
    pub omitted: Vec<OmittedItem>,
    /// Total context budget, tokens (estimated).
    pub total_budget_tokens: u32,
    /// Output tokens reserved when the slice was assembled.
    pub output_budget_tokens: u32,
    /// When the slice was assembled; excluded from [`Self::id`].
    pub created_at: Timestamp,
}

/// Inputs to [`assemble`]. A struct (not seven arguments) keeps call sites
/// and future budget knobs stable.
pub struct AssembleInput<'a> {
    /// Harness system prompt. The only `System`-trust text.
    pub system_prompt: &'a str,
    /// The user's objective verbatim.
    pub objective: &'a str,
    /// Supervisor-recorded task constraints.
    pub constraints: &'a [ContextConstraint],
    /// Retrieved evidence; may be empty for pure-conversation calls.
    pub evidence: &'a EvidencePackage,
    /// Earlier turns, oldest first.
    pub history: &'a [HistoryTurn],
    /// Total context budget, tokens (estimated).
    pub total_budget_tokens: u32,
    /// Output tokens to reserve; context must fit the remainder.
    pub output_budget_tokens: u32,
}

/// Estimates tokens for `text` with [`CHARS_PER_TOKEN`].
#[must_use]
pub fn estimate_tokens(text: &str) -> u32 {
    let chars = u32::try_from(text.chars().count()).unwrap_or(u32::MAX);
    chars / CHARS_PER_TOKEN + 1
}

/// Collapses runs of 3+ identical consecutive lines into the first line plus
/// a repetition marker (spec §27 step 2). Deterministic and content-local.
#[must_use]
pub fn collapse_repeated_lines(text: &str) -> String {
    let mut out = Vec::new();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let mut run = 1usize;
        while lines.peek() == Some(&line) {
            lines.next();
            run += 1;
        }
        out.push(line.to_owned());
        if run >= 3 {
            out.push(format!("… [line repeated {run}×]"));
        } else {
            for _ in 1..run {
                out.push(line.to_owned());
            }
        }
    }
    out.join("\n")
}

/// Assembles bounded, trusted context from evidence, history, and prompts.
///
/// Deterministic for identical inputs: same blocks, same order, same
/// truncation. Applies the spec §27 reduction order; provenance footers are
/// appended after excerpt truncation so they are never cut.
#[must_use]
pub fn assemble(input: &AssembleInput<'_>) -> Vec<ContextBlock> {
    assemble_detailed(input).0
}

/// [`assemble`] plus every omission the reduction order caused, each with a
/// reason (ADR-0007).
fn assemble_detailed(input: &AssembleInput<'_>) -> (Vec<ContextBlock>, Vec<OmittedItem>) {
    let mut blocks = Vec::new();
    push_system(&mut blocks, input.system_prompt);
    push_objective(&mut blocks, input.objective);
    push_constraints(&mut blocks, input.constraints);
    push_evidence(&mut blocks, &input.evidence.contradictions, 60);
    push_evidence(&mut blocks, &input.evidence.findings, 100);
    push_history(&mut blocks, input.history);
    push_gaps(&mut blocks, input.evidence);
    let mut omitted = Vec::new();
    dedupe_blocks(&mut blocks, &mut omitted);
    fit_budget(
        &mut blocks,
        input.total_budget_tokens,
        input.output_budget_tokens,
        &mut omitted,
    );
    (blocks, omitted)
}

/// Assembles [`assemble`]'s blocks into a [`ContextSlice`]: lineage,
/// budgets, recorded omissions, and a content-addressed [`SliceId`]
/// (ADR-0007). Timestamps never enter the id, so identical inputs mint the
/// same id.
#[must_use]
pub fn assemble_slice(input: &AssembleInput<'_>, lineage: &SliceLineage) -> ContextSlice {
    let (blocks, omitted) = assemble_detailed(input);
    let mut slice = ContextSlice {
        id: SliceId(String::new()),
        purpose: lineage.purpose.clone(),
        state_revision: lineage.state_revision,
        parent_ids: lineage.parent_ids.clone(),
        blocks,
        omitted,
        total_budget_tokens: input.total_budget_tokens,
        output_budget_tokens: input.output_budget_tokens,
        created_at: Timestamp::now(),
    };
    slice.id = slice.compute_id();
    slice
}

impl ContextSlice {
    /// Recomputes the content address from the slice's own fields and
    /// reports whether it still matches. Timestamps are ignored; any change
    /// to purpose, lineage, budgets, blocks, or omissions breaks the match.
    #[must_use]
    pub fn verifies(&self) -> bool {
        self.compute_id() == self.id
    }

    fn compute_id(&self) -> SliceId {
        let mut hasher = blake3::Hasher::new();
        put_str(&mut hasher, &self.purpose);
        match self.state_revision {
            None => {
                hasher.update(&[0]);
            }
            Some(revision) => {
                hasher.update(&[1]);
                hasher.update(&revision.to_le_bytes());
            }
        }
        put_usize(&mut hasher, self.parent_ids.len());
        for parent in &self.parent_ids {
            put_str(&mut hasher, &parent.0);
        }
        hasher.update(&self.total_budget_tokens.to_le_bytes());
        hasher.update(&self.output_budget_tokens.to_le_bytes());
        put_usize(&mut hasher, self.blocks.len());
        for block in &self.blocks {
            put_str(&mut hasher, kind_name(block.kind));
            put_str(&mut hasher, &block.provenance);
            put_str(&mut hasher, trust_label(block.trust));
            hasher.update(&block.priority.to_le_bytes());
            put_str(&mut hasher, &block.content);
        }
        put_usize(&mut hasher, self.omitted.len());
        for item in &self.omitted {
            put_str(&mut hasher, kind_name(item.kind));
            put_str(&mut hasher, &item.provenance);
            put_str(&mut hasher, omission_name(item.reason));
        }
        SliceId(hasher.finalize().to_hex().to_string())
    }
}

/// Length-prefixes `value` into `hasher` so field boundaries stay
/// unambiguous across concatenation.
fn put_str(hasher: &mut blake3::Hasher, value: &str) {
    put_usize(hasher, value.len());
    hasher.update(value.as_bytes());
}

/// Encodes a count as fixed little-endian bytes.
fn put_usize(hasher: &mut blake3::Hasher, value: usize) {
    let value = u64::try_from(value).unwrap_or(u64::MAX);
    hasher.update(&value.to_le_bytes());
}

/// Stable fingerprint name for a [`ContextKind`], independent of serde.
fn kind_name(kind: ContextKind) -> &'static str {
    match kind {
        ContextKind::System => "system",
        ContextKind::Objective => "objective",
        ContextKind::Constraint => "constraint",
        ContextKind::Evidence => "evidence",
        ContextKind::History(HistorySpeaker::User) => "history.user",
        ContextKind::History(HistorySpeaker::Assistant) => "history.assistant",
    }
}

/// Stable fingerprint name for an [`OmissionReason`].
fn omission_name(reason: OmissionReason) -> &'static str {
    match reason {
        OmissionReason::Duplicate => "duplicate",
        OmissionReason::BudgetDrop => "budget_drop",
        OmissionReason::Truncated => "truncated",
    }
}

fn push_constraints(blocks: &mut Vec<ContextBlock>, constraints: &[ContextConstraint]) {
    for constraint in constraints {
        let source = match constraint.source {
            ConstraintOrigin::User => "user",
            ConstraintOrigin::Policy => "policy",
            ConstraintOrigin::Workspace => "workspace",
            ConstraintOrigin::System => "system",
            ConstraintOrigin::Derived => "derived",
        };
        let strength = match constraint.strength {
            ConstraintStrength::Hard => "hard",
            ConstraintStrength::Preference => "preference",
        };
        // Constraint contents are never promoted to System trust. Only
        // harness/policy-authored constraints are trusted runtime context;
        // workspace-derived wording remains repository data.
        let trust = match constraint.source {
            ConstraintOrigin::User => TrustLevel::User,
            ConstraintOrigin::Policy | ConstraintOrigin::System => TrustLevel::WorkspaceTrusted,
            ConstraintOrigin::Workspace | ConstraintOrigin::Derived => TrustLevel::WorkspaceData,
        };
        blocks.push(ContextBlock {
            kind: ContextKind::Constraint,
            provenance: format!("task.constraint.{source}"),
            trust,
            content: format!(
                "[{strength} constraint | source:{source} | trust:{}]\n{}",
                trust_label(trust),
                constraint.text
            ),
            priority: 2,
            created_at: Timestamp::now(),
        });
    }
}

/// Trust for retrieved sources. Known workspace sources are data; anything
/// external-looking is untrusted; anything unrecognized fails closed to
/// `ExternalUntrusted`. Nothing retrieved ever becomes `System` or `User`.
fn trust_for_source(source: &str) -> TrustLevel {
    if source.starts_with("repo.")
        || source.starts_with("fs.")
        || source.starts_with("git.")
        || source.starts_with("process.")
        || source.starts_with("tachyon.verify.")
    {
        TrustLevel::WorkspaceData
    } else {
        TrustLevel::ExternalUntrusted
    }
}

/// Short wire-stable trust label. Evidence content carries this header so the
/// trust class survives transports that only speak roles (spec §27).
fn trust_label(trust: TrustLevel) -> &'static str {
    match trust {
        TrustLevel::System => "system",
        TrustLevel::User => "user",
        TrustLevel::WorkspaceTrusted => "workspace-trusted",
        TrustLevel::WorkspaceData => "workspace-data",
        TrustLevel::ExternalUntrusted => "external-untrusted",
    }
}

/// Short `source:path` label for a block header.
fn provenance_label(item: &EvidenceItem) -> String {
    match &item.provenance.path {
        Some(path) => format!("{}:{path}", item.provenance.source),
        None => item.provenance.source.clone(),
    }
}

/// Footer appended after truncation so provenance is never cut.
fn provenance_footer(item: &EvidenceItem) -> String {
    let mut footer = format!("\n\n[source: {}", item.provenance.source);
    if let Some(path) = &item.provenance.path {
        let _ignored = write!(footer, " {path}");
    }
    if let Some(hash) = &item.provenance.hash {
        let _ignored = write!(footer, " hash:{hash}");
    }
    footer.push(']');
    footer
}

/// Truncates `content` head-first to the excerpt cap, marking the cut.
fn excerpt(content: &str) -> String {
    let collapsed = collapse_repeated_lines(content);
    if collapsed.chars().count() <= MAX_EXCERPT_CHARS {
        return collapsed;
    }
    let head: String = collapsed.chars().take(MAX_EXCERPT_CHARS).collect();
    let dropped = collapsed.chars().count() - MAX_EXCERPT_CHARS;
    format!("{head}\n…[truncated {dropped} chars; provenance retained]")
}

fn push_system(blocks: &mut Vec<ContextBlock>, prompt: &str) {
    blocks.push(ContextBlock {
        kind: ContextKind::System,
        provenance: "tachyon.system".to_owned(),
        trust: TrustLevel::System,
        content: prompt.to_owned(),
        priority: 0,
        created_at: Timestamp::now(),
    });
}

fn push_objective(blocks: &mut Vec<ContextBlock>, objective: &str) {
    blocks.push(ContextBlock {
        kind: ContextKind::Objective,
        provenance: "user.objective".to_owned(),
        trust: TrustLevel::User,
        content: objective.to_owned(),
        priority: 1,
        created_at: Timestamp::now(),
    });
}

fn push_evidence(blocks: &mut Vec<ContextBlock>, items: &[EvidenceItem], base: u16) {
    for (index, item) in items.iter().enumerate() {
        let priority = base
            + u16::try_from(index)
                .unwrap_or(u16::MAX - base)
                .min(u16::MAX - base);
        let trust = trust_for_source(&item.provenance.source);
        let mut content = format!(
            "[evidence | {} | trust:{}]\n",
            provenance_label(item),
            trust_label(trust)
        );
        content.push_str(&excerpt(&item.content));
        content.push_str(&provenance_footer(item));
        blocks.push(ContextBlock {
            kind: ContextKind::Evidence,
            provenance: provenance_label(item),
            trust,
            content,
            priority,
            created_at: Timestamp::now(),
        });
    }
}

fn push_history(blocks: &mut Vec<ContextBlock>, history: &[HistoryTurn]) {
    // Newest turns survive longest: recency counts down from the tail, so
    // long conversations shed the oldest first.
    for (index, turn) in history.iter().enumerate() {
        let from_newest = history.len().saturating_sub(1).saturating_sub(index);
        let priority = 200
            + u16::try_from(from_newest)
                .unwrap_or(u16::MAX - 200)
                .min(u16::MAX - 200);
        let trust = match turn.speaker {
            HistorySpeaker::User => TrustLevel::User,
            HistorySpeaker::Assistant => TrustLevel::WorkspaceData,
        };
        blocks.push(ContextBlock {
            kind: ContextKind::History(turn.speaker),
            provenance: "conversation.history".to_owned(),
            trust,
            content: collapse_repeated_lines(&turn.content),
            priority,
            created_at: Timestamp::now(),
        });
    }
}

fn push_gaps(blocks: &mut Vec<ContextBlock>, evidence: &EvidencePackage) {
    if evidence.gaps.is_empty() {
        return;
    }
    let mut content = String::from("Known evidence gaps (weigh before concluding):");
    for gap in &evidence.gaps {
        let _ignored = write!(content, "\n- {}", gap.description);
    }
    blocks.push(ContextBlock {
        kind: ContextKind::Evidence,
        provenance: "tachyon.retrieval.gaps".to_owned(),
        trust: TrustLevel::WorkspaceTrusted,
        content,
        priority: 300,
        created_at: Timestamp::now(),
    });
}

/// Drops exact (kind, content) duplicates after the first occurrence,
/// recording each dropped block as [`OmissionReason::Duplicate`].
fn dedupe_blocks(blocks: &mut Vec<ContextBlock>, omitted: &mut Vec<OmittedItem>) {
    let mut seen = std::collections::HashSet::new();
    let mut kept = Vec::with_capacity(blocks.len());
    for block in std::mem::take(blocks) {
        let key = (
            context_kind_rank(block.kind),
            block.trust as u8,
            block.content.clone(),
        );
        if seen.insert(key) {
            kept.push(block);
        } else {
            omitted.push(OmittedItem {
                kind: block.kind,
                provenance: block.provenance,
                reason: OmissionReason::Duplicate,
            });
        }
    }
    *blocks = kept;
}

/// Stable rank so (kind, trust, content) is hashable for dedupe.
fn context_kind_rank(kind: ContextKind) -> u8 {
    match kind {
        ContextKind::System => 0,
        ContextKind::Objective => 1,
        ContextKind::Constraint => 2,
        ContextKind::Evidence => 3,
        ContextKind::History(_) => 4,
    }
}

/// Enforces `total - output` budget: drops whole lowest-priority blocks
/// first, then shrinks the largest survivor until the allowance fits or
/// nothing can shrink further. System and objective are truncated but never
/// dropped; degenerate budgets (allowance below the pinned minimum) return
/// best-effort over-budget blocks rather than eating pinned content.
/// Whole drops and truncations are recorded in `omitted` (ADR-0007).
fn fit_budget(
    blocks: &mut Vec<ContextBlock>,
    total: u32,
    output: u32,
    omitted: &mut Vec<OmittedItem>,
) {
    let allowance = total.saturating_sub(output);
    // Drop whole flexible blocks, lowest priority first.
    while used_tokens(blocks) > allowance && droppable_count(blocks) > 0 {
        if let Some(victim) = blocks
            .iter()
            .enumerate()
            .filter(|(_, block)| block.priority >= PINNED_PRIORITY_CUTOFF)
            .max_by_key(|(_, block)| (block.priority, block.content.len()))
            .map(|(index, _)| index)
        {
            let dropped = blocks.remove(victim);
            omitted.push(OmittedItem {
                kind: dropped.kind,
                provenance: dropped.provenance,
                reason: OmissionReason::BudgetDrop,
            });
        } else {
            break;
        }
    }
    // Still over with only pinned blocks left: shrink the largest until the
    // allowance fits or the block cannot shrink further. Each successful
    // pass strictly shortens content, so this loop always terminates.
    let mut truncated = std::collections::HashSet::new();
    while used_tokens(blocks) > allowance {
        let Some(largest) = blocks
            .iter()
            .enumerate()
            .max_by_key(|(_, block)| block.content.len())
            .map(|(index, _)| index)
        else {
            break;
        };
        let over = used_tokens(blocks).saturating_sub(allowance);
        if !truncate_block_chars(&mut blocks[largest], over) {
            break;
        }
        if truncated.insert(largest) {
            let block = &blocks[largest];
            omitted.push(OmittedItem {
                kind: block.kind,
                provenance: block.provenance.clone(),
                reason: OmissionReason::Truncated,
            });
        }
    }
}

/// Estimated tokens across all blocks.
fn used_tokens(blocks: &[ContextBlock]) -> u32 {
    blocks
        .iter()
        .map(|block| estimate_tokens(&block.content))
        .sum()
}

/// Count of blocks the budgeter may drop whole.
fn droppable_count(blocks: &[ContextBlock]) -> usize {
    blocks
        .iter()
        .filter(|block| block.priority >= PINNED_PRIORITY_CUTOFF)
        .count()
}

/// Shrinks `block` by roughly `tokens_over` tokens, marking the cut.
/// The cut over-accounts for the marker it appends. Returns whether content
/// strictly shrank: at the marker floor (one char plus marker) further cuts
/// would reproduce identical content, so this returns `false` instead of
/// spinning. Callers must never remove pinned blocks on `false` —
/// degenerate budgets keep best-effort content.
fn truncate_block_chars(block: &mut ContextBlock, tokens_over: u32) -> bool {
    const MARKER: &str = "\n…[budget-truncated]";
    let marker_chars = u32::try_from(MARKER.chars().count()).unwrap_or(u32::MAX);
    let cut_chars = tokens_over
        .saturating_mul(CHARS_PER_TOKEN)
        .saturating_add(marker_chars)
        .max(1);
    let len = block.content.chars().count();
    let cut = usize::try_from(cut_chars)
        .unwrap_or(usize::MAX)
        .min(len.saturating_sub(1));
    if cut == 0 {
        return false;
    }
    let kept: String = block.content.chars().take(len - cut).collect();
    let shrunk = format!("{kept}{MARKER}");
    if shrunk.chars().count() >= len {
        // Marker floor: cutting further reproduces identical content.
        return false;
    }
    block.content = shrunk;
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tachyon_retrieval::{EvidenceItem, EvidenceKind, EvidencePackage, Provenance};

    fn package_with(content: &str) -> EvidencePackage {
        EvidencePackage {
            question: "q".to_owned(),
            findings: vec![EvidenceItem::new(
                EvidenceKind::FileExcerpt,
                content,
                Provenance::repo("repo.symbol.search", "a.rs"),
            )],
            contradictions: vec![],
            gaps: vec![],
        }
    }

    fn input(evidence: &EvidencePackage, total: u32, output: u32) -> AssembleInput<'_> {
        AssembleInput {
            system_prompt: "sys",
            objective: "obj",
            constraints: &[],
            evidence,
            history: &[],
            total_budget_tokens: total,
            output_budget_tokens: output,
        }
    }

    #[test]
    fn repo_evidence_is_data_never_authority() {
        let evidence = package_with("fn f() {}");
        let blocks = assemble(&input(&evidence, 10_000, 1_000));
        let finding = blocks
            .iter()
            .find(|block| block.kind == ContextKind::Evidence)
            .expect("evidence block");
        assert_eq!(finding.trust, TrustLevel::WorkspaceData);
        assert!(blocks.iter().all(|block| {
            block.trust != TrustLevel::System || block.kind == ContextKind::System
        }));
        assert!(
            finding
                .content
                .contains("[source: repo.symbol.search a.rs]")
        );
    }

    #[test]
    fn workspace_and_derived_constraints_remain_data() {
        let evidence = package_with("fn f() {}");
        let constraints = [
            ContextConstraint {
                source: ConstraintOrigin::Workspace,
                strength: ConstraintStrength::Hard,
                text: "repository preference".to_owned(),
            },
            ContextConstraint {
                source: ConstraintOrigin::Derived,
                strength: ConstraintStrength::Hard,
                text: "inferred repository rule".to_owned(),
            },
        ];
        let mut assembly = input(&evidence, 10_000, 1_000);
        assembly.constraints = &constraints;

        let blocks = assemble(&assembly);
        let constraint_blocks: Vec<_> = blocks
            .iter()
            .filter(|block| block.kind == ContextKind::Constraint)
            .collect();
        assert_eq!(constraint_blocks.len(), constraints.len());
        assert!(
            constraint_blocks
                .iter()
                .all(|block| block.trust == TrustLevel::WorkspaceData)
        );
        assert!(
            constraint_blocks
                .iter()
                .all(|block| block.content.contains("trust:workspace-data]"))
        );
    }

    #[test]
    fn repeated_diagnostics_collapse() {
        let collapsed = collapse_repeated_lines("ok\nok\nok\nok\ndone");
        assert_eq!(collapsed, "ok\n… [line repeated 4×]\ndone");
    }

    /// Content-bearing shape: everything except the wall-clock stamp.
    fn shape(blocks: &[ContextBlock]) -> Vec<(ContextKind, String, TrustLevel, String, u16)> {
        blocks
            .iter()
            .map(|block| {
                (
                    block.kind,
                    block.provenance.clone(),
                    block.trust,
                    block.content.clone(),
                    block.priority,
                )
            })
            .collect()
    }

    #[test]
    fn tiny_budget_keeps_system_and_objective() {
        let evidence = package_with(&"x".repeat(10_000));
        let first = assemble(&input(&evidence, 40, 10));
        let second = assemble(&input(&evidence, 40, 10));
        assert_eq!(
            shape(&first),
            shape(&second),
            "assembly must be deterministic"
        );
        assert!(first.iter().any(|block| block.kind == ContextKind::System));
        assert!(
            first
                .iter()
                .any(|block| block.kind == ContextKind::Objective)
        );
        let used: u32 = first
            .iter()
            .map(|block| estimate_tokens(&block.content))
            .sum();
        assert!(used + 10 <= 40, "used {used} + output 10 must fit 40");
    }

    #[test]
    fn pinned_only_budget_converges_without_dropping() {
        let evidence = EvidencePackage::new("q");
        let input = AssembleInput {
            system_prompt: &"s".repeat(4_000),
            objective: &"o".repeat(4_000),
            constraints: &[],
            evidence: &evidence,
            history: &[],
            total_budget_tokens: 40,
            output_budget_tokens: 10,
        };
        let blocks = assemble(&input);
        assert!(blocks.iter().any(|block| block.kind == ContextKind::System));
        assert!(
            blocks
                .iter()
                .any(|block| block.kind == ContextKind::Objective)
        );
        let used: u32 = blocks
            .iter()
            .map(|block| estimate_tokens(&block.content))
            .sum();
        assert!(
            used + 10 <= 40,
            "pinned-only fit must converge, used {used}"
        );
    }

    #[test]
    fn degenerate_budget_keeps_pinned_blocks() {
        let evidence = EvidencePackage::new("q");
        let input = AssembleInput {
            system_prompt: "",
            objective: "",
            constraints: &[],
            evidence: &evidence,
            history: &[],
            total_budget_tokens: 0,
            output_budget_tokens: 0,
        };
        let blocks = assemble(&input);
        assert!(blocks.iter().any(|block| block.kind == ContextKind::System));
        assert!(
            blocks
                .iter()
                .any(|block| block.kind == ContextKind::Objective)
        );
    }

    #[test]
    fn output_exceeding_total_terminates_with_pinned() {
        // Allowance saturates to zero while pinned content is non-empty:
        // the fitter must stop at the marker floor, never spin, and never
        // drop pinned blocks. (A hang here fails the suite by timeout.)
        let evidence = EvidencePackage::new("q");
        let input = AssembleInput {
            system_prompt: "system prompt text",
            objective: "user objective text",
            constraints: &[],
            evidence: &evidence,
            history: &[],
            total_budget_tokens: 10,
            output_budget_tokens: 50,
        };
        let blocks = assemble(&input);
        assert!(blocks.iter().any(|block| block.kind == ContextKind::System));
        assert!(
            blocks
                .iter()
                .any(|block| block.kind == ContextKind::Objective)
        );
    }

    #[test]
    fn newest_history_survives_longest() {
        let evidence = EvidencePackage::new("q");
        let history = [
            HistoryTurn {
                speaker: HistorySpeaker::User,
                content: "old".to_owned(),
            },
            HistoryTurn {
                speaker: HistorySpeaker::User,
                content: "mid".to_owned(),
            },
            HistoryTurn {
                speaker: HistorySpeaker::User,
                content: "new".to_owned(),
            },
        ];
        let input = AssembleInput {
            system_prompt: "sys",
            objective: "obj",
            constraints: &[],
            evidence: &evidence,
            history: &history,
            total_budget_tokens: 10_000,
            output_budget_tokens: 0,
        };
        let priorities: Vec<u16> = assemble(&input)
            .iter()
            .filter(|block| matches!(block.kind, ContextKind::History(_)))
            .map(|block| block.priority)
            .collect();
        assert_eq!(priorities.len(), 3);
        assert!(
            priorities[2] < priorities[0],
            "newest must out-survive oldest"
        );
    }

    #[test]
    fn unknown_sources_fail_closed_to_untrusted() {
        let provenance = Provenance {
            source: "mystery".to_owned(),
            path: None,
            hash: None,
            generation: None,
        };
        let evidence = EvidencePackage {
            question: "q".to_owned(),
            findings: vec![EvidenceItem::new(EvidenceKind::Note, "x", provenance)],
            contradictions: vec![],
            gaps: vec![],
        };
        let input = AssembleInput {
            system_prompt: "sys",
            objective: "obj",
            constraints: &[],
            evidence: &evidence,
            history: &[],
            total_budget_tokens: 10_000,
            output_budget_tokens: 0,
        };
        let blocks = assemble(&input);
        let finding = blocks
            .iter()
            .find(|block| block.kind == ContextKind::Evidence)
            .expect("evidence block");
        assert_eq!(finding.trust, TrustLevel::ExternalUntrusted);
    }

    #[test]
    fn evidence_carries_trust_header() {
        let evidence = package_with("fn f() {}");
        let blocks = assemble(&input(&evidence, 10_000, 1_000));
        let finding = blocks
            .iter()
            .find(|block| block.kind == ContextKind::Evidence)
            .expect("evidence block");
        assert!(
            finding
                .content
                .starts_with("[evidence | repo.symbol.search:a.rs | trust:workspace-data]\n"),
            "unexpected header: {}",
            finding.content
        );
    }

    fn lineage() -> SliceLineage {
        SliceLineage {
            purpose: "proposal".to_owned(),
            state_revision: Some(7),
            parent_ids: Vec::new(),
        }
    }

    #[test]
    fn slice_id_is_deterministic_and_verifiable() {
        let evidence = package_with("fn f() {}");
        let first = assemble_slice(&input(&evidence, 10_000, 1_000), &lineage());
        let second = assemble_slice(&input(&evidence, 10_000, 1_000), &lineage());
        assert_eq!(first.id, second.id);
        assert!(first.verifies());
        assert!(first.omitted.is_empty(), "unexpected: {:?}", first.omitted);
        assert_eq!(first.state_revision, Some(7));
    }

    #[test]
    fn slice_id_excludes_timestamps_but_binds_content() {
        let evidence = package_with("fn f() {}");
        let mut slice = assemble_slice(&input(&evidence, 10_000, 1_000), &lineage());
        slice.created_at = Timestamp::from_micros(42);
        assert!(
            slice.verifies(),
            "timestamps must not affect the content address"
        );
        slice.blocks[0].content.push_str("tampered");
        assert!(!slice.verifies(), "content changes must change the id");
    }

    #[test]
    fn slice_id_binds_purpose_revision_and_parents() {
        let evidence = package_with("fn f() {}");
        let base = assemble_slice(&input(&evidence, 10_000, 1_000), &lineage());
        let other_purpose = assemble_slice(
            &input(&evidence, 10_000, 1_000),
            &SliceLineage {
                purpose: "scout".to_owned(),
                state_revision: Some(7),
                parent_ids: Vec::new(),
            },
        );
        let other_revision = assemble_slice(
            &input(&evidence, 10_000, 1_000),
            &SliceLineage {
                purpose: "proposal".to_owned(),
                state_revision: Some(8),
                parent_ids: Vec::new(),
            },
        );
        let with_parent = assemble_slice(
            &input(&evidence, 10_000, 1_000),
            &SliceLineage {
                purpose: "proposal".to_owned(),
                state_revision: Some(7),
                parent_ids: vec![base.id.clone()],
            },
        );
        assert_ne!(base.id, other_purpose.id);
        assert_ne!(base.id, other_revision.id);
        assert_ne!(base.id, with_parent.id);
    }

    #[test]
    fn assemble_slice_blocks_equal_assemble() {
        let evidence = package_with("fn f() {}");
        let plain = assemble(&input(&evidence, 10_000, 1_000));
        let slice = assemble_slice(&input(&evidence, 10_000, 1_000), &lineage());
        // Equal up to `created_at`, which is wall-clock, not content.
        assert_eq!(shape(&plain), shape(&slice.blocks));
    }

    #[test]
    fn duplicate_history_is_recorded_as_an_omission() {
        let evidence = package_with("fn f() {}");
        let history = [
            HistoryTurn {
                speaker: HistorySpeaker::User,
                content: "same".to_owned(),
            },
            HistoryTurn {
                speaker: HistorySpeaker::User,
                content: "same".to_owned(),
            },
        ];
        let call = AssembleInput {
            system_prompt: "sys",
            objective: "obj",
            constraints: &[],
            evidence: &evidence,
            history: &history,
            total_budget_tokens: 10_000,
            output_budget_tokens: 1_000,
        };
        let slice = assemble_slice(&call, &lineage());
        assert!(
            slice
                .omitted
                .iter()
                .any(|item| item.reason == OmissionReason::Duplicate),
            "missing duplicate: {:?}",
            slice.omitted
        );
        assert!(slice.verifies());
    }

    #[test]
    fn budget_drops_and_truncations_are_recorded() {
        let evidence = package_with("fn f() {}");
        let long_system = "s".repeat(500);
        let squeezed = AssembleInput {
            system_prompt: &long_system,
            objective: "obj",
            constraints: &[],
            evidence: &evidence,
            history: &[],
            total_budget_tokens: 40,
            output_budget_tokens: 10,
        };
        let slice = assemble_slice(&squeezed, &lineage());
        assert!(
            slice
                .omitted
                .iter()
                .any(|item| item.reason == OmissionReason::BudgetDrop),
            "missing budget drop: {:?}",
            slice.omitted
        );
        assert!(
            slice
                .omitted
                .iter()
                .any(|item| item.reason == OmissionReason::Truncated),
            "missing truncation: {:?}",
            slice.omitted
        );
        assert!(slice.verifies());
    }
}
