# Milestone 17: integer-boundary development eval

Convert the M16 retained duplicate-range failure into development data.
No new live score or production prompt change is part of this milestone.
Keep all M16 frozen inputs, samples, and reports unchanged.

Test the public `sorted_range::equal_range` interface in fresh temporary
workspaces. Enumerate every sorted sequence of length zero through four
from `[i64::MIN, -1, 0, 1, i64::MAX]`. Query nine boundary and nearby values.
Compute expected starts and counts by linear comparison, independently of
the binary-search implementation. This yields 126 sequences and 1,134
sequence/query cases per profile.

The exact retained patch must compile and pass the visible test in both
debug and release. It must fail the boundary oracle in both profiles:
overflow in debug, wrong range in release. Wrapping-add and saturating-add
variants must also fail the boundary oracle in both profiles. The known
safe solution must pass the complete workspace, including API consumers,
and the new development oracle in both profiles. A compile error, timeout,
or unrelated test failure cannot count as a successful rejection control.

The eval must leave repository inputs untouched and remove only its own
temporary workspace. Run it from `cargo verify full`. Publish hashes,
per-control results, test counts, and zero model calls. Run VERIFY and FULL;
review on spec and standards axes; commit directly to main.

This eval proves oracle sensitivity and reproduces a specific failure.
It does not show that model proposals improved. Use it to test later
candidate changes. Any later claim needs new frozen held-out tasks.
