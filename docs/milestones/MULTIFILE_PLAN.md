# Milestone 19: frozen multi-file API-preservation baseline

Freeze two new tasks before live calls: canonical length-framed encoding
and decoding, and normalized registry insertion and lookup. Both tasks
require changes to two implementation files. Keep production prompt,
acceptance, model policy, and malformed-only retry unchanged. The M18
unselected guidance remains disabled.

Broken fixtures must compile and fail visible behavior. Known solutions
must pass the complete workspace, public API consumers, and hidden oracles.
Each partial one-file repair must fail acceptance for the remaining file.
Prove API-corruption and protected-file drift rejection. Golden wire bytes
must reject mutually consistent noncanonical roundtrips. Registry oracles
must cover Unicode whitespace, ASCII-only folding, duplicate/empty rejection,
and preserved state. Hidden tests stay outside model evidence; solutions
stay outside copied model workspaces.

Run five full and five serial samples per task: 20 total. Rotate task order
and alternate mode order by sample. Use fresh workspaces and the same release
binary and pinned model `mimo-v2.6-flash`, endpoint `https://api.xiaomimimo.com`,
temperature 0, output cap4096, model-stage deadline120seconds plus1000ms
measurement allowance, and at most2attempts only for malformed proposals.
Force benchmark variant baseline. Freeze all inputs and publish source
before the first live call. Verify committed bytes and binary hashes.
Discard stderr and model bodies. Preserve every failure and interrupted
batch separately; incomplete rows or lost usage cannot be silently replaced.

Valid evaluation requires complete ordered rows, safe attempts, authorized
change sets, protected files, and consistent durable completion/failure
recovery. Product score is separate. Exploratory reference line is >=95%
overall and >=4/5 in each task/mode cell. Five samples per cell support no
true reliability probability or speed claim. Do not pool earlier milestones.
Retain failed patches as development counterexamples when available.

Run oracle/negative validator controls, VERIFY and FULL, independent spec
and standards reviews, and publish all evidence and a bounded report.
No new runtime capability or repair retry is part of this milestone.
