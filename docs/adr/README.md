# Architecture Decision Records

Use ADRs only for meaningful architecture deviations or choices that future implementers need to understand.

Filename format:

`NNNN-short-title.md`

Each ADR should contain:

- Status
- Context
- Decision
- Alternatives considered
- Evidence
- Consequences
- Migration/rollback plan

ADRs 0001–0003 predate this format and are grandfathered: they are required
to carry only Status, Context, Decision, and Consequences. Every ADR from
0004 onward must carry all of the sections above. This list is enforced by
the `adrs_contain_required_sections` tripwire in
`crates/tachyon-app/tests/docs_freshness.rs`.
