# Tachyon improvement evidence — 2026-09-30

This research supports prioritization, not a new architecture decision. Sources
are first-party and dated on or before 30 September 2026 where a publication
date is available. Live documentation can change; the dates below identify
publications or versioned releases, not a claim that every linked page is an
immutable historical snapshot. No model leaderboard or provider price is used.

## 1. Make acceptance evaluations exercise the live agent

Anthropic's **9 January 2026** evaluation guidance distinguishes the final
environment outcome from the agent's transcript or success claim, recommends
multiple trials, and separates capability evaluation from regression coverage.
It also treats the evaluated system as model plus harness.
[Source: Demystifying evals for AI agents](https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents).

**Tachyon recommendation:** retain deterministic scripted fixtures as regression
tests, then add a small opt-in live-provider suite on realistic repository tasks.
Grade the resulting files and verifier outputs against the acceptance contract.
Include refusal/constraint preservation, cancellation, stale evidence, and
interrupted work. Record exact model, settings, environment, complete tool trace,
verified outcome, user interventions, TTFR and completion latency. Keep live
API-dependent checks outside the ordinary offline local verification loop.

## 2. Measure harness improvements under controlled resources

Anthropic's **5 February 2026** infrastructure study found that resource
configuration can materially shift coding-agent benchmark scores, and argues
for reporting resource allocation and hard enforcement limits separately.
These results concern its benchmark environments and do not establish a
performance improvement in Tachyon.
[Source: Quantifying infrastructure noise in agentic coding evals](https://www.anthropic.com/engineering/infrastructure-noise).

**Tachyon recommendation:** run paired harness comparisons with the same model,
provider, task commit, CPU/RAM limits, timeout and concurrency. Classify transport,
OOM and rate-limit failures separately from task failures. Use repeated trials
and report sample count, verified-success rate, median and p95, as already
required by [the benchmark specification](../05_ACCEPTANCE_AND_BENCHMARKS.md).

## 3. Finish the accepted durable execution boundary

Anthropic's **8 April 2026** managed-agent design separates durable session
history, the model loop and execution environment, allowing each to fail
independently. Its credentials remain outside the generated-code sandbox.
This is hosted-service experience, not evidence that Tachyon should adopt that
service or distributed workers.
[Source: Scaling Managed Agents](https://www.anthropic.com/engineering/managed-agents).

**Tachyon recommendation:** implement the existing
[ADR-0006](../adr/0006-supervisor-owned-evidence-execution.md) contract before
expanding tool breadth: Supervisor-owned validated reads, authorization bound
to the opened file, generation fencing, cancellation drain, a shared stage
budget, and durable integrity-checked output receipts. Use crash injection at
the artifact/journal boundaries and reject late results after cancellation.
This recommendation applies Tachyon's own accepted design; the external source
supports separating durable truth from disposable execution.

## 4. Preserve deterministic authority at MCP and ACP boundaries

The MCP maintainers' **16 March 2026** guidance says tool annotations are hints,
can be dishonest, and cannot enforce sandboxing or prevent data exfiltration.
[Source: Tool Annotations as Risk Vocabulary](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/).
The versioned **2025-11-25** MCP security guidance recommends restricted local
server privileges and explicit consent for one-click server command execution.
[Source: MCP Security Best Practices](https://modelcontextprotocol.io/specification/2025-11-25/basic/security_best_practices).

**Tachyon recommendation:** map MCP proposals to trusted local capability
contracts and policy; never derive retry authority or effect classification
solely from server hints. For ACP client-supplied stdio servers, enforce the
accepted executable/argument consent, environment and workspace restrictions.
Test a malicious server claiming to be read-only and a client attempting to
replace a pending permission operation.

ACP **schema-v1.23.0**, released **18 September 2026**, is confirmed by the
[official release](https://github.com/agentclientprotocol/agent-client-protocol/releases/tag/schema-v1.23.0).
Follow [ADR-0005](../adr/0005-acp-v1-gateway-distribution.md)'s existing v1 pin
and gateway ownership; advertise only implemented capabilities. The MCP
**28 July 2026** announcement labels its new specification a release candidate,
which is insufficient by itself to establish later final-release status.
[Source: MCP release-candidate announcement](https://blog.modelcontextprotocol.io/posts/2026-07-28-release-candidate/).

## Recommended order

1. Complete ADR-0006 and prove its failure/recovery boundaries.
2. Add live, outcome-graded tasks and controlled harness measurements.
3. Deliver the already-approved ACP v1 gateway adapter with boundary tests.

Provider transport and streaming priorities require local implementation
evidence; these sources alone do not identify Tachyon's transport bottleneck.
