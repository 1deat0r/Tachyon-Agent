# 0008 — RetrievalPortfolio over existing capabilities and EvidenceItem

## Status

proposed · 2026-10-03

## Context

The vNEXT directive asks for a RetrievalPortfolio: one interface through
which exact path lookup, symbol lookup, references, lexical search, git
topology, and future strategies return a shared candidate shape
(`EvidenceCandidate`), with optional embeddings and ContextScout later.

Inventory of what exists today:

- Spec §31 already defines the candidate: `EvidenceItem` carries content,
  `Provenance` (source capability, workspace-relative path, content hash,
  generation), and `relevance`. `EvidenceKind` distinguishes symbol
  definitions, references, lexical hits, file excerpts, git facts,
  diagnostics, and notes.
- Production strategies already run behind the capability registry lowered
  to validated Execution IR with Supervisor ownership: `fs.read`
  (ADR-0006) and `repo.lexical` (runtime evidence slice). The tools
  registry exposes `search.lexical`.
- `EvidencePackage` provides deterministic merge, dedupe, and ranked
  order for candidates from multiple strategies.

## Decision

1. `EvidenceItem` remains the portfolio's candidate type. Do not create a
   parallel `EvidenceCandidate` struct; the directive's fields map onto
   existing ones: provenance → `Provenance`, location →
   `Provenance.path`, relevance → `EvidenceItem.relevance`,
   retrieval method → `Provenance.source` + `EvidenceKind`,
   freshness → `Provenance.hash`/`generation`.
2. Strategies remain capabilities, not a new abstraction layer. A new
   retrieval method arrives as a capability with a typed request lowered to
   validated IR, like `fs.read` and `repo.lexical`.
3. `confidence_if_probabilistic` has no home yet: deterministic strategies
   do not produce probabilistic confidence, and no probabilistic retrieval
   strategy ships today. When the first one lands (JEV-scored or
   ContextScout candidates), add an optional `confidence` (and a
   `probabilistic: bool` marker) to `EvidenceItem` in the same change
   that introduces its producer, so the field never exists unused.
4. No vector database, no graph database, no semantic retrieval in this
   increment. Embeddings remain optional until a matched evaluation shows
   benefit over lexical/symbol retrieval (vNEXT §9).

## Alternatives considered

- A new `RetrievalStrategy` trait + `EvidenceCandidate` type now:
  rejected — today every production strategy already returns
  `EvidenceItem` through the capability path; a trait with adapters but no
  second unconstrained caller is speculative structure with no measured
  problem (AGENTS.md new-capability checklist, §33).
- Moving retrieval behind MCP for uniformity: rejected — native fast paths
  stay direct Rust calls; MCP stays an external boundary (vNEXT §1).

## Evidence

- `crates/tachyon-retrieval/src/lib.rs` (spec §31 types and merge/rank).
- `crates/tachyon-core/src/evidence.rs` (ADR-0006 `fs.read` stage).
- `crates/tachyon-core/src/runtime.rs` (`repo.lexical` evidence slice).
- `crates/tachyon-repo/src` (symbol index, lexical search, projection —
  deterministic strategies already behind those capabilities).

## Consequences

- Phase 3 (ContextScout) must emit `EvidenceItem` candidates that pass
  deterministic validation before entering a package; scout conversation
  never becomes solver context.
- The first probabilistic producer pays a small, single schema addition
  instead of the codebase carrying an unused confidence field now.
- The portfolio's "interface" stays the capability contract: typed request
  in, validated IR, `EvidenceItem` receipts out.

## Migration/rollback plan

No code changes in this increment. If a future evaluated need disproves the
mapping, add the new candidate type in that change's ADR with a benchmark;
nothing persisted depends on the current shape.
