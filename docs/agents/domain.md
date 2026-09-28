# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Use relevant context

- Read **`CONTEXT.md`** when the change uses or changes domain language.
- Read only the relevant **`docs/adr/`** records for the area being changed. In multi-context repos, also check `src/<context>/docs/adr/` for context-scoped decisions.

If these files don't exist, proceed without proposing new documentation by default. Create or update domain documentation when a term or decision actually changes.

## Architecture docs (authoritative when relevant)

In addition to CONTEXT/ADRs, these freeze the system and must not be silently reinterpreted:

- `docs/01_ARCHITECTURE_FREEZE.md` — frozen responsibilities
- `docs/02_IMPLEMENTATION_SPEC.md` — concrete implementation contract
- `docs/04_IMPLEMENTATION_PLAN.md` — dependency-ordered milestones
- `AGENTS.md` — core invariants and priority order

## File structure

Single-context repo (this repo):

```
/
├── CONTEXT.md
├── docs/adr/
│   ├── README.md
│   └── NNNN-short-title.md
└── crates/
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-NNNN (…), but worth reopening because…_
