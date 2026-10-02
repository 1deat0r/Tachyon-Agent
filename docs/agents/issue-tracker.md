# Optional issue tracker: GitHub

The canonical remote is `1deat0r/Tachyon-Agent`; `gh` resolves it from this checkout. GitHub Issues are one optional persistent tracker, not the default development loop. Apply the selection rules in [`docs/DEVELOPMENT_WORKFLOW.md`](../DEVELOPMENT_WORKFLOW.md) before creating or publishing tickets. Routine changes should stay in the current task and working tree.

## Local task hierarchy (scratch tickets)

Local ticket files under `.scratch/<feature>/issues/` use the four-level hierarchy (workflow rule 18, user rule 2026-10-02):

```markdown
# TASK NN: <name>
**Status:** ready | in-progress | complete (`cargo verify` exit 0; boxes checked <date>)
**Blocked by:** None (NN complete) | NN
**What to build:** ...
**Verify:** <commands and test names>

## Small tasks (each = one commit, in order)
- [ ] S1 <name> · Verify: <test name>
  - [ ] M1a <micro step>
    - [ ] N1a1 <nano check>
```

- One **small task** = one local commit + push after its Verify passes (workflow rule 19).
- Caps: 8 small tasks per TASK; 6 micro per small task; 4 nano per micro task. Rule 13 caps apply per level.
- Flip `**Status:**` and check boxes in the same commit that lands the work.

## Conventions

- **Create an issue when useful**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies. Apply `type/*`, `comp/*`, and `P*` labels when known.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v`; `gh` does this automatically when run inside a clone.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either: resolve with `gh pr view 42` and fall back to `gh issue view 42`.

## Skills and tracker choice

When a skill says to publish to the issue tracker, first decide whether durable shared tracking adds value. If it does not, keep the plan in the current task or a focused local document and continue. Project policy overrides a skill's default Issue, branch, or PR sequence. Use `/wayfinder` only for work whose long-horizon decisions benefit from a shared map; its GitHub map and child Issues are optional.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Wayfinding operations (when a GitHub map is chosen)

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue (`gh api` on the sub-issues endpoint). Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: GitHub's **native issue dependencies**, the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by` (open blockers only, the live gate). Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier query**: list the map's open children (`gh issue list --state open`, scoped to the map's sub-issues / task list), drop any with an open blocker (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the `Blocked by` line) or an assignee; first in map order wins.
- **Claim**: `gh issue edit <n> --add-assignee @me`, the session's first write.
- **Resolve**: `gh issue comment <n> --body "<answer>"`, then `gh issue close <n>`, then append a context pointer (gist + link) to the map's Decisions-so-far.
