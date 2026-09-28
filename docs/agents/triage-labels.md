# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the actual label strings used in this repo's issue tracker.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.

Edit the right-hand column to match whatever vocabulary you actually use.

## Additional labels (Hermes-style taxonomy)

Apply alongside triage roles when classifying work:

**Type:** `type/bug`, `type/feature`, `type/refactor`, `type/test`, `type/docs`, `type/chore`, `type/security`, `type/perf`

**Component:** `comp/core`, `comp/gateway`, `comp/cli`, `comp/tui`, `comp/scheduler`, `comp/policy`, `comp/tools`, `comp/verify`, `comp/docs`, `comp/ci`

**Priority:** `P0` (data loss / security / crash), `P1` (major broken, no workaround), `P2` (degraded, workaround exists), `P3` (cosmetic)

**Other:** `needs-repro` (bug needs reproduction steps on current `main`)

**Wayfinder:** `wayfinder:map`, `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, `wayfinder:task`
