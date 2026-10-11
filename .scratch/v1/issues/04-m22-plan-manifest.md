# TASK 04: Freeze M22 candidate plan + manifest
**Status:** ready
**Blocked by:** 03
**What to build:** the M22 candidate definition — one prompt/retry/deadline change behind a disabled-by-default flag, frozen inputs including the M21 regressions, manifest validating.
**Verify:** `node scripts/m22_manifest.mjs` valid; flag-default-off proven by test.

## Small tasks (each = one commit, in order)
- [ ] S1 Candidate plan · Verify: change + flag + frozen inputs listed
- [ ] S2 Manifest + freeze · Verify: manifest script validates
- [ ] S3 Flag-default-off test · Verify: new test green
