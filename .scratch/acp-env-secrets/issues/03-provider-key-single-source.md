# 03: Provider key single-source (registered == invoked)

**What to build:** The provider API key has one source of truth. The app resolves the key at config load and passes those same bytes into the provider client; `invoke` prefers the resolved value and stops re-reading process env on every call (env fallback retained only when no resolved key is supplied, e.g. directly-constructed unit tests). The value registered with the gateway redactor at startup and the value sent as the transport header are the same bytes by construction, so redaction evidence provably covers the wire. Mid-run env rotation changes nothing until restart — that is the contract, not a defect.

**Blocked by:** None (can start immediately; disjoint file set from 01/02)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-01)

- [x] Provider client construction accepts an app-resolved key; `invoke` uses it instead of `std::env::var` when present
- [x] Env-var fallback still works where no resolved key is supplied (existing direct-provider unit tests keep passing unmodified or with explicit resolved-key wiring)
- [x] Test: resolve + register at load, then mutate the process env var to a different value → invoke still sends the originally registered key (assert the transport header), proving rotation is inert until restart
- [x] Test: an error body embedding the registered key is redacted in the run-failure record, journal, and client frame exactly as `provider_redaction.rs` pins today (regression, same bytes)
- [x] No key bytes in `Debug`, logs, config serialization, or error messages (existing config tests stay green)
- [x] Tests at the provider/app seam + gateway `provider_redaction.rs` regression
