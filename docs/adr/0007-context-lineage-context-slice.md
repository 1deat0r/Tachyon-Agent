# 0007 — Context lineage: ContextSlice

## Status

accepted · implemented 2026-10-03

## Context

The vNEXT directive makes context lineage a Class-B primary direction: every
important reasoning invocation should consume an explicit, reproducible
projection of authoritative state, not accumulated conversation.

Tachyon already assembles typed, trusted, prioritized context blocks
(spec §27) with deterministic reduction, provenance, and budget fitting. What
it does not yet record is which slice a call consumed, which task revision it
represented, what was omitted and why, or which earlier call it descends
from. `TaskModelContext` binds assembly inputs to a revision for staleness
rejection, but the assembled value itself is transient and unaddressable.

## Decision

1. Add `ContextSlice` in `tachyon-models::context`, the nearest
   authoritative layer for spec §27. A slice carries `purpose`,
   `state_revision: Option<u64>`, `parent_ids: Vec<SliceId>` (references,
   never copied ancestor content), the included `blocks`, every `omitted`
   item with an `OmissionReason` (`duplicate`, `budget_drop`,
   `truncated`), both budget numbers, and `created_at`.
2. Mint `SliceId` as hex BLAKE3-256 over the deterministic content:
   purpose, revision tag, parent ids, budgets, and each block's kind,
   provenance, trust, priority, and content — excluding `created_at`.
   `ContextSlice::verifies()` recomputes and compares, so a replay can prove
   a slice was not altered after assembly.
3. Construct slices through `assemble_slice(&AssembleInput, &SliceLineage)`.
   `assemble()` stays byte-for-byte equivalent for existing callers. The
   shared driver builds the slice at its single model-call site
   (`stage_model`, purpose `proposal`) and hands `slice.blocks` to the
   provider request.
4. Record omissions where they happen: the existing §27 reduction steps now
   report dedupe drops, budget drops, and truncations instead of silently
   deleting. Nothing about a call's input loss stays invisible.
5. No journal, wire-schema, or provider change in this increment. Persisting
   slice identity — and enough references to reconstruct a call — is a
   follow-up that must go through the Supervisor's single-writer journal path.

## Alternatives considered

- Persisting conversation/prompts as authoritative context: rejected —
  conversation is data; context is a projection of task state (vNEXT §7).
- A standalone context crate or framework: rejected — §27 assembly already
  exists; a second framework would duplicate it.
- Hashing the provider wire JSON: rejected — it embeds timestamps and
  provider-specific shaping, breaking content addressability and provider
  neutrality.
- Journaling full slices immediately: deferred — the journal's display kinds
  and replay contract need a deliberate design, and premature persistence is
  harder to roll back than none.

## Evidence

- Spec §27 defines the block, trust, and reduction contract this extends.
- Existing `context.rs` tests pin determinism, trust classes, and budget
  behavior; new tests pin slice-id determinism, timestamp exclusion, tamper
  detection, and every omission reason.
- ADR-0006 established the revision-fence pattern the slice binds to.
- The vNEXT directive classifies context lineage as a high-confidence
  addition, not a freeze change; `cargo verify` is the gate.

## Consequences

- Model inputs become inspectable and content-addressable without provider
  cooperation; omission reasons make budget loss explicit.
- Parent references give the future Context DAG an edge type that copies no
  content.
- Stable slice content will let provider prompt caching be organized later
  without distorting context.
- `tachyon-models` gains a direct `blake3` dependency (already
  workspace-pinned).
- Limitation: until the journaling follow-up lands, a slice exists only for
  the duration of its call; audit requires re-deriving it from task state.

## Migration/rollback plan

Additive API: no persisted state, wire format, or provider contract depends
on it, and `assemble()` behavior is unchanged. Roll back by reverting the
commit; nothing downstream needs cleanup.
