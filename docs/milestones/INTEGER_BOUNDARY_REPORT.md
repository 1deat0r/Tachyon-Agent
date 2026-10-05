# Milestone 17: integer-boundary development eval

Date: 2026-10-06 (Pacific/Auckland).

The M16 rejected patch is now a repeatable development regression control.
The eval distinguishes debug overflow from release misbehavior. It also
rejects wrapping-add and saturating-add variants that avoid a panic but
return incorrect ranges. The known safe solution passes every development
case and the complete fixture workspace in both profiles.

This milestone made zero model calls and no production prompt change.
It measures oracle sensitivity, not model quality. The frozen M16 inputs
and its 29/30 live baseline remain unchanged. These tasks and counterexamples
are development data for later candidate work.

## Acceptance and results

The public seam is `sorted_range::equal_range`. The new oracle enumerates
all 126 sorted sequences of length zero through four from
`[i64::MIN, -1, 0, 1, i64::MAX]`. It queries nine boundary and nearby values.
Expected ranges use independent linear comparison counts. This gives 1,134
sequence/query cases per profile for the passing solution. A failing control
stops at its first counterexample; it is not credited with passing 1,134 cases.

| Implementation | Debug | Release |
| --- | --- | --- |
| Known safe solution | All cases and workspace/API tests pass | All cases and workspace/API tests pass |
| Exact retained M16 patch | Rejected: addition overflow | Rejected: range assertion |
| Wrapping-add variant | Rejected: range assertion | Rejected: range assertion |
| Saturating-add variant | Rejected: range assertion | Rejected: range assertion |

Every implementation first compiled and passed the visible regression in
both profiles. A compiler error, signal, timeout, or unrelated failure cannot
satisfy a rejection control. The script requires the named boundary test to
fail with the expected diagnostic. All eight implementation/profile controls
passed. The positive solution also passes the original hidden tests and API
consumer. The measured [results](INTEGER_BOUNDARY_RESULTS.json) include
source hashes and the new oracle hash. The [plan](INTEGER_BOUNDARY_PLAN.md)
defines the cases and limits.

## Integration and limits

`node scripts/integer_boundary_eval.mjs` runs in an owned temporary workspace,
uses offline locked Cargo tests, leaves the repository fixture unchanged,
and removes its workspace afterward. Output goes under ignored
`target/integer-boundary/`. `cargo verify full` now runs this control and
requires its success token.

The exact source is retained in
[the M16 counterexample](HELDOUT_FAILED_DUPLICATE_RANGE_SERIAL_3.rs.txt).
Debug catches `needle + 1` at `i64::MAX`. Release wraps that expression and
returns a wrong range. Replacing it with wrapping or saturating arithmetic
still fails the range contract. The positive implementation finds lower
and upper partition points without adding to the query value.

This finite input set is not a proof for every possible input. The controls
show that acceptance rejects these known semantic mistakes in both profiles.
They do not implement semantic repair retries, or show that future model
proposals avoid those mistakes. The next proposed milestone is a bounded
candidate improvement, followed by new frozen held-out tasks before any
new reliability claim.

Validation: standalone development controls and integrated `cargo verify full`
passed. FULL includes VERIFY, security/recovery checks, the existing scripted
matrix, dependency audit, and performance checks. Both independent review
axes found no material defect. The frozen M16 manifest remained valid.
