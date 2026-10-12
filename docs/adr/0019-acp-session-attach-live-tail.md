# 0019 — `session/attach` live-tail for ACP turns as the ADR-0005:41 realization

## Status

proposed · 2026-10-12

## Context

ADR-0005:41 already decides the authority: losing the ACP stdio connection
does not cancel a Gateway Task, and "reconnect/load ... may attach to its
live updates; it does not create a Task, restart a driver, or replay work".
`session/load` covers the replay half (ordered history, read-only). The
live half — reattaching to a still-running task's update stream across an
adapter disconnect — has no wire method. Design frozen at
`.scratch/acp-attach/spec.md` (reviewed SOUND, `.scratch/board-attach/`).

## Decision

1. New explicitly-optional Tachyon extension `session/attach` with params
   `{sessionId, taskId?, from_seq?}`: target is the explicit `taskId` else
   the latest non-terminal turn from `GetSession`; terminal targets refuse
   typed `attach_target_terminal` (use `session/load` for replay).
2. Attach is a read-only subscribe-reader reusing gateway cursor pushdown
   (`load_events_since` + `latest_seq`); one bounded re-subscribe on
   `ResyncRequired`, then typed `resync_required`. Takes no turn slot, so
   the recorded-turn gate is untouched and concurrent attaches are safe.
3. Streams `session/update` notifications with the turn/load chunk mapping;
   on task terminal, reports final `status` (never mints `stopReason` —
   verdict ownership stays with the driving turn / Supervisor).
4. Read-only authority, period: attach never approves/denies/cancels/
   creates/steers; approval parks surface as parked-state notifications
   only; cancel stays on its independent ADR-0005:41 stateless path.
   Adapter EOF ends the attach silently; the gateway task is unaffected.

## Alternatives considered

1. **Fold into `session/load` with a live flag** — rejected: conflates
   replay (complete, terminating) with tail (open-ended, terminal-
   triggered result) and gives one method two termination modes.
2. **Steering/retry or approval answers through attach** — rejected:
   breaks the single-driver invariant and :41's no-create/no-restart
   authority; control stays on prompt/cancel.
3. **ADR-0005 amendment instead of a short ADR** — considered; a short ADR
   is preferred so the wire method + taxonomy + negative authority have
   one citable record, with :41 remaining the governing authority.

## Evidence

- `docs/adr/0005-acp-v1-gateway-distribution.md` :41.
- `.scratch/acp-attach/spec.md` (frozen design).
- `.scratch/board-attach/initial.md` (SOUND, no changes).

## Consequences

- The adapter's wire surface gains one explicitly-optional read-only method
  (`session/attach`); the access set gains live-subscription reads only —
  no writes, no approvals, no task creation, no verdict authority.
- The recorded-turn gate and single-driver invariant are untouched by
  construction (attach takes no turn slot); concurrent attaches are safe
  broadcast readers.
- A future multiplexing or cross-restart-cursor extension supersedes this
  decision by amending it, not by reinterpreting the read-only authority.

## Migration/rollback plan

No persisted state depends on this decision (attach holds no slot, mints no
keys, journals nothing). Revert to method-not-found by removing the arm;
the taxonomy markers (`attach_target_terminal`, `unknown_task`,
`resync_required`) then go unused.
