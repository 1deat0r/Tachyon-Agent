# Milestone 18: boundary-guidance candidate experiment

Freeze one generic guidance candidate before any calls. It asks the model
to check integer-limit, empty-input, and failure-state behavior, preserve
APIs, and use wrapping/saturating arithmetic only when the contract requires
it. The opt-in benchmark provider appends this reviewed text to trusted
system context after production assembly. This transform is not represented
in the persisted production ContextSlice. It is experiment instrumentation,
not a production prompt change or a production-ready feature.

Compare baseline and candidate on `duplicate-range` development data first.
Then run two new held-out tasks: `signed-midpoint` and `ceiling-division`.
All tasks restrict edits to one implementation and require complete workspace
acceptance, protected files, hidden numeric boundaries, API consumers, and
failed/completed recovery. Prove broken-first and known-solution controls.
The new tasks were not used to select this candidate. Freeze all inputs and
publish the source before live calls; never revise the candidate during runs.

Run five samples per task/arm, full mode only: 30 planned rows. Alternate
arm order each sample. Development rows precede held-out rows. Use the same
binary, model, endpoint, temperature 0, output budget 4096, model-stage deadline
120 seconds plus 1 second measurement allowance, and existing malformed-only
retry cap 2. Pin source, binary, input manifest, variant, order, and safe usage.
Discard stderr and model bodies. Retain honest failures; incomplete batches
remain separate and cannot support a decision. No pooling with M16 results.

The candidate is eligible for further production design only if it has more
verified development successes, no lower verified success on either new task,
and at least 95% overall candidate success with at least 4/5 on each task.
A tie, lower success, incomplete evidence, or safety failure blocks adoption.
This small experiment establishes no statistical or general reliability
improvement, and no speed claim. Even an eligible candidate needs production
context/provenance work and new acceptance checks before adoption.

Run oracle and negative validator controls, VERIFY and FULL, two independent
reviews, and publish every planned row and a bounded decision report. Keep
M16 reports/manifest unchanged; validate historical input hashes against their
original commit when current benchmark instrumentation changes.
