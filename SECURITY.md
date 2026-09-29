# Security Policy

## Reporting

Tachyon executes code, touches the filesystem, and talks to model providers —
treat it as security-sensitive software.

- **Do not open a public issue for a vulnerability.** Contact the maintainer
  privately (see the repo owner profile) with: affected version/commit, steps
  to reproduce, and impact assessment.
- Expect acknowledgment within 7 days. Fixes ship as fast as the change
  allows; credit is given unless you ask otherwise.

## Scope

In-scope: capability bypass, workspace escape (`..`, symlink/junction),
approval replay or scope confusion, credential exposure through logs or
artifacts, journal/recovery corruption, remote gateway exposure when it
should be off.

Out of scope: vulnerabilities in upstream dependencies (report those
upstream, though a heads-up is welcome), social engineering, physical access.

`TACHYON_FAULT_POINT` parks the process at a named seam when set. Never set
it in a service unit, container, or CI runner that isn't a fault-injection
test.

## Hardening already specified

`docs/06_SECURITY_AND_RECOVERY.md` defines the threat model: capability-based
policy, canonicalized containment, approval-to-operation-hash binding,
credential handles with redaction, explicit effect/idempotency semantics, and
`UnknownAfterCrash` instead of blind replay. Milestone gates include escape
and fault-injection suites.
