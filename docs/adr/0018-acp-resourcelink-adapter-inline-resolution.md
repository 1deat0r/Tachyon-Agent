# 0018 — ACP ResourceLink resolution inline in the adapter as user data

## Status

proposed · 2026-10-12

## Context

ADR-0005:27 makes Text and ResourceLink prompt content a baseline MUST; :42
requires ResourceLinks to resolve "only through bounded evidence access
inside the session's declared roots and existing policy". ACP turns carry
no evidence channel (prompt turns call bare `StartRun`; the model works
from the objective text). Since ADR-0006, production evidence flows through
supervisor-owned validated Execution IR — the adapter reading session files
directly is a new prompt-driven filesystem-read authority outside that
path. This ADR records the trade explicitly rather than reinterpreting :42
silently.

## Decision

1. Resolve `resource_link` blocks adapter-side into user-data text
   (`file://` only, canonicalized session-root confinement with
   separator-boundary prefix, leaf-symlink refusal, regular-files-only,
   64KiB × 8 budget, UTF-8, delimiter-collision refusal), embedded as
   fenced `[resource <uri>]` provenance blocks after the prompt text.
2. Embedded content inherits untrusted-data handling and is never policy;
   it is not scrubbed for secrets (consistent with prompt text, which is
   already journalled verbatim).
3. All failures refuse the whole prompt typed (`unsupported_scheme`,
   `unresolvable`, `oversized`, `out_of_root`, `invalid_encoding`,
   `unsafe_content`, `too_many_resources`); never skip-with-warning.
4. A gateway evidence channel for links stays out of scope until a
   measured need justifies new supervisor machinery.

## Alternatives considered

1. **Supervisor-owned evidence for links** — architecturally purest (keeps
   all reads behind validated IR), rejected as disproportionate for a
   tracer: new request types, scheduler paths, and receipt plumbing for
   what :42 already permits treating as user-provided data.
2. **Skip-with-warning on unreadable links** — rejected: the model would
   reason on silently incomplete evidence, breaking provenance.
3. **Gateway-side resolution via a new command** — rejected: moves file
   access without removing the trust question, at higher cost.

## Evidence

- `docs/adr/0005-acp-v1-gateway-distribution.md` :27, :42.
- `.scratch/acp-resourcelink/spec.md` rev 2 (confinement rules, taxonomy,
  budgets, authority record).
- `.scratch/board-reslink/initial.md` + `.scratch/board-reslink-impl/initial.md`
  (independent design + implementation reviews).
- `crates/tachyon-acp/src/turn.rs`: `resolve_resource_link`,
  `resolve_prompt_links` + escape battery.

## Consequences

- The adapter's access set gains bounded read-only file access inside the
  pinned session root (capability doc amended); no writes, network, or
  store access.
- Residual TOCTOU inside the authorized root is accepted for local IPC
  (documented in code); exploiting it needs session-root write access.
- A future gateway evidence channel supersedes this decision by replacing
  inline resolution, not by layering beside it.

## Migration/rollback plan

No persisted state depends on this decision (reads are per-prompt,
nothing durable). Revert to `unsupported_prompt_content` refusal by
removing the block arm; the taxonomy markers then go unused.
