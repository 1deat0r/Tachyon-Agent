# TASK 03: flip `loadSession: true` + capability doc
**Status:** ready
**Blocked by:** 02 complete (advertise-only-implemented: replay AND gate must be green first, ADR-0005:31)
**What to build:** `server.rs` `AgentCapabilities.load_session: false → true`; update the byte-exact golden in `initialize_advertisement_is_byte_exact` and the live `initialize_live_gateway.rs` expectation; update the two doc references that promise `loadSession=false` ("text-only prompts" refusal message stays — it refers to prompt content, not load; the `server.rs:177`/`:430` comments and `codec.rs:647` comment now name the shipped arm); capability doc: remove the `session/load` bullet from "Remaining before an 'ACP supported' claim", add `session/load` to the shipped surface (replay contract, gate, typed refusals, snapshot semantics) and add the new test names to Verification method. No code behavior change beyond the flag.
**Verify:** `cargo verify` exit 0 (full gate); `initialize_advertisement_is_byte_exact` green with `loadSession:true`; `initialize_succeeds_against_a_live_gateway_with_clean_framing` green; capability doc contains no `loadSession: false` claim and no `session/load` residue bullet.

## Small tasks (each = one commit, in order)
- [ ] S1 flag + goldens + doc · Verify: the three named tests + `cargo verify` exit 0
  - [ ] M1a `load_session: true` + both test goldens
    - [ ] N1a1 `rg "loadSession.:false" crates/ docs/` returns only historical/archived references (if any)
