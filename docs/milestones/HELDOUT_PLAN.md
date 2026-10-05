# Milestone 16: frozen held-out bounded-task baseline

Freeze three new tasks before live calls: `utf8-boundary`, `atomic-transfer`,
and `duplicate-range`. These tasks were not used to tune the production
prompt. The same production driver and M15 prompt handle every task.
This baseline is held out from prompt selection; tests do not prove that a
public task will remain held out from future tuning.

## Oracles

Every broken fixture must compile and fail its behavioral regression. A
known solution must pass the full workspace test command. Public consumers
must compile, unchanged methods must retain their behavior, and only the
specified implementation file may change. Hidden edge-case tests are
protected and omitted from model evidence. Visible regression tests and
API consumers are evidence. Known solutions are outside model workspaces.
Negative controls must reject public API corruption and protected-file
changes. Freeze hashes for all task and solution files, descriptors and
this plan before the first live call.

## Baseline protocol

Run five full and five serial samples for each task: 30 total. Rotate task
order by sample and alternate mode order by sample. Calls remain sequential.
Use `mimo-v2.6-flash`, `https://api.xiaomimimo.com`, temperature 0, 4096 output
tokens, a 120-second model-stage deadline, and the existing one malformed
output retry. Pin the source commit, release binary hash and fixture manifest. Frozen inputs
must match the named commit. Discard child stderr; retain only validated
benchmark fields. The checker permits at most 1000 ms of scheduling and timer
measurement overhead beyond the model-stage deadline.
Finish builds before measurement. Use fresh workspaces. Preserve every
failure and every interrupted batch separately. Never fill missing rows or
resume a partial batch as if no call occurred. Usage lost at interruption
remains unreconciled. Do not retain model text, provider error bodies or keys.

Score verified acceptance, first-attempt verification, retries, typed failures,
protected paths, completion recovery, latency and actual token usage by task
and mode. Report all-run and successful-run latencies separately. No verified
price is supplied, so monetary cost is unavailable.

## Decision rules

Eval validity requires the frozen inputs, every planned row, bounded safe
calls and consistent acceptance/recovery. It can pass when a model task
fails. Product success is reported separately; the exploratory reference
line is at least 95% verified overall and at least 4/5 in each cell. This
small baseline cannot establish a true 95% success probability. Unequal
verified success prevents mode speed comparisons. Do not hide failed tasks
inside a pooled average.

No production prompt or model change is permitted during this baseline.
Use failures to nominate the next bounded development eval. If future work
tunes on these tasks, label them development data and use a separate frozen
hold-out for the next claim. Do not lower acceptance to improve the score.

## Exit gates

Prove all oracle controls. Validate the manifest and sample checker against
negative cases. Run VERIFY and FULL. Freeze and publish the infrastructure
before live calls. Retain complete measured evidence, review the report on
spec and standards axes, and publish an atomic report commit. Completing
this eval milestone does not mean that the product passed its reference line.
