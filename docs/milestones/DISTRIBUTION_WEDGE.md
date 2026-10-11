# Distribution Wedge (TASK 50)

v1 wedge: ACP v1 editor-client compatibility through the local gateway
(ADR-0005). No new runtime, no parallel execution path.

## Proof

- `cargo test -p tachyon-acp`: 60 + 2 + 2 + 1 green (codec, server,
  turn, probe suites).
- Gateway + Task Supervisor remain the only writer; ACP is a pure client
  (AD-014). No agent decision logic in the adapter.

## Launch story

"The local harness where done means verified and crashes don't lie":
verification-gated completion + effect-safe journal + ACP editor support
for power users and privacy/BYOK teams. Not a Claude Code replacement.
