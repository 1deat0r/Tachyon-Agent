# 11 — Pre-Move Consideration Checklist

**Date:** 24 September 2026
**Purpose:** Expert inventory of decisions/risks to weigh before committing further to M12–M14 and public MVP. Complements `10_STRATEGIC_REVIEW.md` (thesis, market, language already settled there).
**Evidence window:** Prefer **18–24 Sep 2026**. Older best-available sources are labeled with their real date. Local repo facts verified **24 Sep 2026**.

**Status update (2026-09-28):** M14 is frozen. Item #2 is resolved for post-MVP in
[`docs/adr/0005-acp-v1-gateway-distribution.md`](docs/adr/0005-acp-v1-gateway-distribution.md):
TUI-first remains the MVP choice; ACP v1 through the local gateway is the first
post-MVP editor-client target. The rankings and proposed sequence below preserve
the checklist’s 24 September decision state.

---

## Evidence framework (consensus 24 Sep 2026)

Adopted after a four-way critic debate (epistemology · practitioner · red-team · arbiter). Replaces numeric “trust /100” **strength** bands. Numeric thresholds 70/85 were rejected as uncalibrated.

**Urgency /100** — *schedule* axis only (when to decide), not truth:

| Band | Meaning |
|---|---|
| 90–100 | Deciding wrongly or ignoring this month can invalidate strategy or block a credible public launch |
| 70–89 | Must decide during M12–M14, before public launch |
| 50–69 | Decide when designing distribution / before marketing claims |
| 30–49 | Watch list or deliberate post-MVP |
| 0–29 | Background / optional |

**Grade every load-bearing *claim*** (never a source globally):

| Grade | Meaning |
|---|---|
| **A** | Body read this session **or** local/official-API check with a recorded artifact **and** the artifact entails the exact acted sentence; source type supports this claim type; origin clear |
| **B** | Body read; claim-fit; single origin |
| **C** | Snippet / header-only / unread body / abstract / narrow⇒overgeneralized — **lead only**, never load-bearing |
| **D** | Dead, 404, or paywalled-unread — not fact; **excluded from origin counts** |

**Claim tags:** `existence` · `behavior` · `quantitative` · `event` · `opinion`

**Origin rules:** Hard dedupe first — syndicated/wire/PR copies share one `origin_id` (N copies = 1 origin). “Strong primary” for Gate R = entity-of-record / first-party registry for **existence only** — never brand reputation; vendor/PR pages never sole primary for behavior or any Gate I claim.

**Gate R** (reversible, ~45 min box): every load-bearing claim ≥ **B**, body read, claim-fit. Single origin is enough. Past the box: decide with an uncertainty note listing accepted C/D items.

**Gate I** (irreversible / public / high-stakes, ~2 h box): Gate R floor **plus** —

- **Critical** claims (counterfactual: failure alone flips the decision or public statement; author may not downgrade) at **A**;
- **all** criticals need an origin-independent check that is itself body-read (**≥ B**) — second origin **or** local measurement with entailment;
- no `[X]` (body not re-read) or `[S]` (snippet-only) on the acted-on sentence;
- tag `event` load-bearing claims need **2 distinct origins**;
- vendor self-reports always attributed; **never** sole close of a quantitative claim;
- opinion/prediction tags are **never critical** — decompose into non-opinion claims; if none remain, the decision is **PREDICTION-HINGED** and requires a written attestation (owner + date + list of ≥B attributed predictions) or Gate I **fails** (not a silent pass, not a Gate R-style note);
- conflicting A/B on the same claim ⇒ **unresolved**, do not average;
- negatives (“X does not exist / no bump”) require a **written search protocol**;
- past the Gate I timebox: **hard stop** — defer or escalate; never uncertainty-note-and-proceed.

**Queue sort (optional):** a /100 *priority* may order which claims to verify first. It carries **no** evidence-strength meaning and never authorizes action.

**Flags:** `[F]` body fetched this session · `[S]` search/snippet only · `[X]` body not re-read · `[L]` local artifact · `[W]` window 18–24 Sep 2026 · `[O]` older best-available (date given).

**Coverage note:** Items already resolved in `10_STRATEGIC_REVIEW.md` or the M12 grill appear under **Already closed** (no re-litigation).

---

## Ranked checklist (highest urgency first)

### P0 — Decide before continuing past M12 planning / before any public MVP claim

| # | Consideration | Urgency | Evidence (grade / flags) | Why now (sources) |
|---|---|---|---|---|
| 1 | **Formal kill criteria** — pre-commit: time-boxed distribution proof; after any major lab release, 90-day “are we still the default path for anyone?” review; stop/pivot rules. We listed stop/pivot options but never numeric gates. | **92** | Framework **B**; Venture Curator body `[X]` → **C** for body claims; use as lead, open body before Gate I | [Venture Curator, 22 Sep 2026](https://www.venturecurator.com/p/ai-startups-model-release-risk) — non-defaults get ~90 days after overlapping lab releases; wrappers die fast (Huxe: 1 day). Supporting graveyard: [killedbyai.net](https://killedbyai.net/) `[S][O]` = C until read. |
| 2 | **Distribution vehicle: ACP-agent vs standalone TUI-first** — at checklist date, the earlier grill had not chosen a vehicle. Resolved 2026-09-28 by ADR-0005: TUI-first for MVP, ACP v1 through the local gateway as the first post-MVP editor-client target. | **90** | Event **A** (bodies read) | [ACP Tool Call Names stabilized 17 Sep](https://agentclientprotocol.com/announcements/tool-call-name-stabilized) `[O−1]`; [ACP Registry releases 23–24 Sep](https://github.com/agentclientprotocol/registry/releases) `[W][F]`; [JetBrains Air 22 Sep](https://blog.jetbrains.com/blog/2026/09/22/introducing-jetbrains-air/) `[W][F]` — editors route ACP as the agent surface. |
| 3 | **General vs domain-specific harness (vertical wedge)** — 2026 funding/practice favors verticals; platforms eat unowned verticals. Never formally modeled “systems/Rust/verification harness” identity. | **88** | Mixed: Week 38 body **B**; many vertical refs `[S]` = **C** | [Week 38 digest 20 Sep](https://paragraph.com/@twiata/this-week-in-all-things-ai-week-38-2026) `[W][F]` quotes Garry Tan “domain-specific harness”; [Astra for Law ~17–20 Sep](https://dreaming.press/posts/2026-09-20-founders-wire-raindrop-agent-monitoring-frontier-safety-standards-astra-for-law.html) `[W][F]`; vertical agent funding roundups `[O][S]` = C until bodies read. |
| 4 | **Product name / brand collisions** — bare `tachyon` crate taken; multiple agentic “Tachyon” products (Tachyon Aura, Tachyon Systems, TachyonGPT, another AI terminal). Rename or qualify before launch. | **85** | Existence **A** (APIs); product pages **B** | crates.io API `q=tachyon` — name created 2026-08-07 + 35 matches `[F]`; GitHub `tachyon in:name language:Rust` ~78 repos incl. AI terminal `[F]`; Brave “Tachyon AI agent product” — Tachyon Aura/Sys/GPT `[S]` = C until pages opened. Local: binary name `tachyon`, workspace unpublished (`publish = false` everywhere) `[L]`. |
| 5 | **Supply-chain launch pack** — `deny.toml` exists but **CI does not run cargo-deny or cargo-audit**; Rust ecosystem under social-engineering attacks targeting publish keys (17–21 Sep). Needed before “cargo install / GitHub Releases are safe” claims. | **84** | Event **A**; local **A** (file body) | [Rust blog 17 Sep — targeted attacks on Rustaceans](https://blog.rust-lang.org/2026/09/17/targeted-attacks/) `[W][F]`; [arrayref incident 20 Aug](https://blog.rust-lang.org/2026/08/20/supply-chain-attack-on-arrayref/) `[O][F]`; [pk-sharma briefing 21 Sep](https://www.pk-sharma.com/briefing/rust-maintainers-fake-interviews-build-scripts) `[W][F]`. Local: `.github/workflows/ci.yml` = fmt/check/test/clippy only; `rg deny` in workflows = none `[L]`. |
| 6 | **MVP benchmark fixture breadth** — M14 requires a full matrix; tree has only `fixtures/auth-refresh`. Spec §42-class tasks (multi-file, architecture) lack checked-in fixtures. | **80** | Existence **A** (glob artifact) | Local glob: only `fixtures/auth-refresh/**` `[L]`. Plan M14: full matrix + `MVP_REPORT.md` (`docs/04:247-256`) `[L]`. |

### P1 — Decide during M12–M14 before public launch

| # | Consideration | Urgency | Evidence (grade / flags) | Why now (sources) |
|---|---|---|---|---|
| 7 | **Windows (and macOS) support statement** — CI already runs 3 OSes; competitors support Windows “with caveats” and file noise daily. Explicit support matrix required (native limits vs WSL) before claiming cross-platform. | **78** | Behavior **A** (docs body) + issues **A** | Local CI matrix ubuntu/windows/macos `[L]`; [Claude Code Windows requirements + no native sandbox](https://docs.claude.com/en/docs/claude-code/setup) `[F]`; Codex Windows packaging/MCP OAuth issues 24 Sep ([#47759](https://github.com/openai/codex/issues/47759), [#47756](https://github.com/openai/codex/issues/47756)) `[W][F]`. |
| 8 | **OpenJEV fallback always on default CI path** — reinforces strategic review: never document openjev.sh as required; exercise fake/evidence fallback in every gate run. | **76** | Existence **A** (openssl) + launch **B** | Live probe openjev.sh cert **19 Sep 2026** (5-day domain) `[W]`; [TypeSafe launch 15 Sep](https://typesafe.ai/blog/introducing-system-one-models-and-jev) `[O][F]`; [TheNewStack 21 Sep](https://thenewstack.io/typesafe-jev-system-one/) `[W][F]`; no OpenJEV-specific news 18–24 `[W]`. Design already optional (AD-009) `[L]`. |
| 9 | **Grill process artifacts** — ADR(s) for env-gated fault points + driver re-entry; M12 follow-up issues (general effect protocol, node-level `UnknownAfterCrash`); CONTEXT.md terms already pinned. | **74** | Local **A** | Grill Round 2 decisions + `CONTEXT.md` edits 24 Sep `[L]`; `docs/adr/` still README-only `[L]`. |
| 10 | **Kill-test suite cost/flake policy on 3-OS CI** — real SIGKILL tests on Windows/macOS need determinism rules (no sleep races) and a documented flake budget before M12 lands. | **72** | Billing **B** (docs body) + local **A** | GitHub Actions billing: public repos free; macOS ~10× Linux if private ([docs](https://docs.github.com/en/billing/managing-billing-for-your-products/about-billing-for-github-actions)) `[F]`; local CI already 3-OS `[L]`; M12 design = fault points not timing `[L]`. |
| 11 | **Model-churn benchmark discipline** — **two flagship releases in one week** (Opus 5.5 + GPT-6 Sol/Luna, both 22 Sep) reprice and re-score “efficiency.” AD-015 same-model rules must be executable under weekly churn; M13/M14 claims need pinned model IDs + dates. | **80** | Event **A** (3 bodies) | [Anthropic Opus 5.5 22 Sep](https://www.anthropic.com/claude-opus-5-5) `[W][F]`; [OpenAI GPT-6 Sol/Luna 22 Sep](https://openai.com/index/introducing-gpt-6-sol-and-luna/) `[W][F]`; [AA Sol/Luna 22 Sep](https://www.artificialanalysis.ai/articles/gpt-6-sol-and-luna-push-the-cost-efficiency-frontier) `[W][F]`; [AA Opus 5.5 22 Sep](https://www.artificialanalysis.ai/articles/claude-opus-5-5) `[W][F]`. |
| 12 | **Anthropic adapter / Opus 5.5 breaking changes** — if (or when) shipping a first-party Anthropic path beyond OpenAI-compat: thinking always-on, tool_choice, thinking blocks, computer tools. | **70** | Thenewstack body `[X]` = **C** until re-read; Anthropic launch **A** for existence only | [Thenewstack 23 Sep migration](https://thenewstack.io/claude-opus-agent-migration/) `[W]` — full body `[X]`, header/date confirmed; cross-check against [Anthropic launch](https://www.anthropic.com/claude-opus-5-5) `[W][F]`. Current stack is OpenAI-compat only `[L]` — urgency only if expanding providers. |
| 13 | **Local-model path vs reframe “local-first”** — either accept Ollama/OpenAI-compat as tested backend or stop implying on-device sovereignty. Compat bugs live this week. | **75** | Issues/release **A** (bodies read) | Ollama `max_tokens` ignored [#18575 21 Sep](https://github.com/ollama/ollama/issues/18575) `[W][F]`; `reasoning_content` drop [#18534 19 Sep](https://github.com/ollama/ollama/issues/18534) `[W][F]`; [Ollama 0.34.4 23 Sep](https://github.com/ollama/ollama/releases/tag/v0.34.4) `[W][F]`; Codex local-subagent demand [#47752 24 Sep](https://github.com/openai/codex/issues/47752) `[W][F]`. |
| 14 | **MCP trust boundary at gateway** — allowlist servers; bound tool-schema token budget; treat MCP as external (already AD/spec) but document threat model for launch. | **70** | Issues/PR **A** | [bernstein#6233 24 Sep](https://github.com/sipyourdrink-ltd/bernstein/issues/6233) `[W][F]` unbounded tool schemas; [MCP servers registry-only PR 24 Sep](https://github.com/modelcontextprotocol/servers/pull/4843) `[W][F]`; field-notes 13 MCP servers 23 Sep `[W][F]`; no new CVE IDs found `[W]` (search protocol: official MCP org + HN window this session). |
| 15 | **Same-model cost pressure: AWS Strands** — 28% lower tokens on same models vs Claude Code-class baselines (21 Sep). Efficiency marketing must beat *harness* baselines, not just naive loops. | **70** | Quantitative **B** vendor-only — **fails Gate I alone** | [Strands launch 21 Sep](https://strandsagents.com/blog/introducing-strands-harness/) `[W][F]` (vendor primary); corroborating harness-economics piece [Thenewstack 19 Sep](https://thenewstack.io/ai-agent-harness-economics/) `[W][F]`. Attribute “AWS claims 28%”; do not publish as fact without independent re-run. |
| 16 | **Safety/agent-breach narrative** — OpenAI agent vs Australian gov portal (23 Sep) makes “capability policy + verification gates” a launch talking point; also raises bar for our approval/effect story. | **68** | Event **C** after origin dedupe (snippets + paywall) — **fails Gate I** | [Reuters 23 Sep](https://www.reuters.com/world/asia-pacific/australia-pm-albanese-says-openai-breached-medicare-sydney-morning-herald-2026-09-23/) `[W][S]`; CNA 24 Sep `[W][S]`; NYT `[X]` paywall. Likely 1 origin. **Do not publish specifics** until 2 body-read distinct origins. |
| 17 | **Agent identity/auth readiness (enterprise)** — gateway will eventually need scoped credentials, delegation, revocation. Not MVP-blocking for pure local single-user; is design debt if remote gateway ever enabled (default off). | **66** | BU body **B**; Okta/IETF/NIST mixed **B/C** | [Biometric Update 21 Sep](https://www.biometricupdate.com/202609/agents-are-going-rogue-and-its-up-to-the-identity-sector-to-govern-them) `[W][F]`; Okta Oktane 22 Sep `[W][S]` = C; IETF draft-klrc-aiagent-auth `[O][S]` = C until opened; NIST concept paper Feb 2026 `[O][S]` = C until opened. |
| 18 | **Windows CI vs M12 kill tests** — child.kill semantics + path/socket differences; decide portability layer before writing suite (overlaps #7). | **70** | Local **A** + issues **A** | Local CI already windows `[L]`; portable `std` APIs chosen in grill `[L]`; Codex Windows fragility 24 Sep `[W][F]`. |
| 19 | **License packaging clarity** — dual LICENSE-MIT + LICENSE-APACHE + `license = "MIT OR Apache-2.0"` `[L]`; GitHub API previously detected Apache-only (research) `[S]`. Confirm README + any future crates metadata agree. | **58** | Local **A** (files read); GH API **C** until re-fetched | Local files verified 24 Sep `[L]`; GitHub repo license field from research `[S]`. Not a legal blocker; consistency fix. |
| 20 | **Publish strategy** — all crates `publish = false` `[L]`; name `tachyon` already taken on crates.io `[F]`. Decide: never publish / publish as `tachyon-agent` / binary-only Releases. Tied to #4. | **62** | Existence **A** | Local `publish = false` ×19 crates `[L]`; crates.io search `[F]`. |

### P2 — Distribution design / claim language (before or just after M14)

| # | Consideration | Urgency | Evidence (grade / flags) | Why (sources) |
|---|---|---|---|---|
| 21 | **Launch playbook calendar** (Show HN, README, Trending, Reddit) — no this-week playbook; older-2026 consensus still HN-first for devtools. Sequence after MVP gates, not during M12. | **55** | **C** — older playbooks `[O][S]`, bodies not fully re-read; **[NO THIS-WEEK SOURCE]** | AFFiNE/RepoRanker/Show HN playbooks Mar–Apr 2026 `[O][S]`. |
| 22 | **Buy vs build observability/eval** — category funded (Raindrop $35M mid-Sep). Keep in-house = deterministic acceptance gates only; buy/integrate monitoring later. | **58** | Funding **B/C** (secondary citing wire) | Raindrop $35M 16 Sep cited in [Founder's Wire 20 Sep](https://dreaming.press/posts/2026-09-20-founders-wire-raindrop-agent-monitoring-frontier-safety-standards-astra-for-law.html) `[W][F]`; AIR $50M 1 Sep `[O][S]` = C until body. |
| 23 | **Verification-as-product positioning** — “deterministic grader / Consumer Reports for agents” as adjacent angle; medium confidence (eval-trust critique). | **55** | **C/opinion** — positioning hypothesis | Same Raindrop/AIR cluster; eval-startup skepticism `[O][S]`. Not load-bearing for Gate I. |
| 24 | **EU AI Act Art. 50 transparency defaults** — general coding assistant not high-risk in 2026; disclosure UX + audit logs nice for enterprise. | **48** | **A/B** (EC page body-read `[O][F]`) | [EC AI Act page updated 3 Aug 2026](https://digital-strategy.ec.europa.eu/en/policies/regulatory-framework-ai) `[O][F]`; high-risk delayed to 2027 via Omnibus `[O][S]` = C until confirmed. No 2026 conformity wall for MVP. |
| 25 | **Telemetry/privacy one-pager** — local-first no-phone-home is the default; only needed if crash reporting added later. | **42** | **C** trend pieces `[S]` | Local-first trend pieces `[W][S]`; no this-week OSS GDPR action `[W]` (search protocol: EC + HN window). |
| 26 | **AGENTS.md / skills interop** — Claude Code native AGENTS.md (18 Sep); cheap docs win. | **45** | Changelog **A** | [Claude Code changelog 2.1.277, 18 Sep](https://code.claude.com/docs/en/changelog) `[W][F]`. |
| 27 | **Business model lane** (sponsors / hosted / support) — Cline/Continue patterns; not MVP-gate. | **35** | **B/C** | cline.bot pricing `[O][F]`; Continue repo `[O][S]`. |
| 28 | **Competitive intel: Foremerge** (Rust, verification gate, Show HN 21 Sep) — closest this-week peer; read before claiming uniqueness. | **50** | **C** until repo body read (`[S]`) | [Show HN Foremerge 21 Sep](https://github.com/naw103/foremerge) `[W][S]`. |
| 29 | **Node 20 Actions retirement (23 Sep)** — only if any JS actions used; current workflow is rust-toolchain + checkout only. | **40** | Changelog **A** + local **A** | [GitHub changelog 23 Sep](https://github.blog/changelog/2026-09-23-node-20-is-no-longer-available-in-github-actions) `[W][F]`; local ci.yml has no node actions `[L]`. Verify third-party actions’ runtimes when adding steps. |
| 30 | **MCP/ACP/skills spec drift** — no MCP/ACP *spec* bump 18–24 Sep (MCP still 2026-07-28). Re-check at M14. | **40** | **A** (official sites; search protocol recorded) | [modelcontextprotocol.io](https://modelcontextprotocol.io/) still 2026-07-28 `[W][F]`; ACP updates through 17 Sep `[F]`. |

### Already closed (do not re-open without new evidence)

| Topic | Where closed |
|---|---|
| Product thesis (serial latency, indexes, routing) | `10_STRATEGIC_REVIEW` C1–C5 |
| Market crowding / consolidation | C12–C13; Alternatives A–E |
| Language Rust vs Go vs TS | `10` § Language re-evaluation; AD-001 reaffirmed |
| OpenJEV hard-dependency ban | C15 + AD-009 (execution still #8 above) |
| M12 scope, fault points, re-entry, suite shape, single PR | Grill Rounds 1–2 |
| MCP as boundary not kernel | AD-005; architecture freeze |
| Remote gateway default off | Architecture freeze invariant 8 |
| Same-model benchmark rule | AD-015 (execution hardening = #11) |
| Node-level classification / general effect protocol | Deferred → M12 follow-up issues (#9) |

---

## Fact-check ledger (regraded under consensus)

Columns: claim · tag · grade · origins · gate notes. Numeric trust scores removed.

| Claim | Tag | Grade | Origins | Gate notes |
|---|---|---|---|---|
| Opus 5.5 + GPT-6 Sol/Luna announced 22 Sep with published list prices | event | **A** (bodies read: Anthropic, OpenAI, AA) | 3 (vendor×2 + AA) | Gate R pass. “Your savings” would be quantitative and needs local measure for public claim |
| Rust maintainer targeting campaign (blog 17 Sep) | event | **A** (body read) | 1 primary | Gate R pass; Gate I event would need 2nd origin |
| ACP Registry releases 23–24 Sep; JetBrains Air 22 Sep | event | **A** (both bodies read) | 2 | Gate R pass |
| OpenAI agent vs Australian Medicare portal | event | **C** (Reuters/CNA `[S]`, NYT `[X]`; likely 1 SMH origin after dedupe) | **1** after dedupe | **Fails Gate I** (event needs 2 body-read origins); do not publish specifics |
| openjev.sh cert notBefore 19 Sep 2026 | existence | **A** (openssl artifact entails sentence) | 1 | Gate R pass |
| crates.io bare name `tachyon` taken | existence | **A** (API JSON) | 1 | Gate R pass |
| CI has no cargo-deny/cargo-audit steps | existence | **A** (file body read + 4-step list) | 1 | Gate R pass |
| LICENSE-MIT + LICENSE-APACHE present | existence | **A** (file read) | 1 | Gate R pass |
| Only `fixtures/auth-refresh` exists | existence | **A** (glob artifact) | 1 | Gate R pass |
| Strands claims 28% lower tokens same models | quantitative | **B** (vendor body read) | 1 vendor + secondary | **Fails Gate I alone** (vendor quantitative needs independent body-read check / local re-run) |
| No MCP/ACP spec bump 18–24 Sep | existence | **A** (official sites body-read; search protocol: fetched modelcontextprotocol.io + ACP updates this session) | 2 official | Gate R pass |

**Soft spots still open:** NYT Medicare body `[X]`; Thenewstack Opus-migration full body `[X]`; Venture Curator full post `[X]` (header-only — **C** for body-level claims); vertical-startup rows mostly `[S]` = C until bodies read.

---

## Recommended immediate sequence (does not replace M12 grill confirm)

1. **Answer grill confirm** on M12 as scoped (still parked).
2. **This week, in parallel with M12 implementation:** #5 supply-chain CI job · #4/#20 name+publish decision · #6 fixture plan for M14 · #9 ADRs+follow-up issues · #7 Windows support sentence in README.
3. **Before M14 exit gate:** #2 ACP vs TUI distribution choice · #3 general vs vertical positioning · #1 written kill criteria · #11 pinned model IDs for benchmarks · #13 local-model claim decision.
4. **Post-MVP:** #21 launch calendar · #27 business model · #24 enterprise disclosure.

---

## Debate record (trust framework)

Four-way critic debate, 24 Sep 2026: epistemology · practitioner · red-team · arbiter.

| Position | Outcome |
|---|---|
| Original trust /100 strength bands + act-at-70 / high-stakes-85 | **Rejected** — false precision, brand-first, access≠truth, uncalibrated thresholds, 50–69 dead zone |
| Claim-level A–D grades + Gate R / Gate I + origin dedupe + vendor-quantitative bar | **Adopted** (this document) |
| Urgency /100 | **Kept** (schedule axis only) |
| Trust /100 | **Demoted** to optional verify-queue sort; no strength semantics |

Sign-off (all **SIGN**): epistemologist · practitioner · arbiter · red-team (after packet re-score: false-primary, paywall-exclusive, syndicated-stack, local-entailment, and C4-Medicare non-load-bearing all closed).

**Answer to “when do we trust a source?” (replaces 70/85):**  
Trust a *claim* enough to act only when its body (or an entailing local artifact) was read, its grade is **≥ B**, its source type fits that claim type, and — if the decision is irreversible, public, or high-stakes — **Gate I** extras hold (criticals at A + independent body-read check, event dual-origin, no snippet/paywall on the acted sentence, vendor metrics attributed with a second origin or your own measurement). Below that: lead only (**C**) or not evidence (**D**).

---

## Answer to “Have we considered ALL options?”

**No — not until this checklist existed.** Prior work covered thesis, market, language, OpenJEV, M12 mechanics, and five strategic alternatives. Newly formalized gaps with highest leverage: **kill criteria (#1), ACP-first distribution (#2), vertical wedge (#3), name/crate collisions (#4), supply-chain CI (#5), benchmark fixture breadth (#6)** — all urgency ≥80 with in-window or local evidence.
