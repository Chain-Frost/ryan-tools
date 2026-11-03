# TUFLOW GPU availability alerts and CodeMeter validation

| Field | Value |
| --- | --- |
| Status | Needs review; licensed hardware evidence complete within recorded scope; hosted validation tracked in PR |
| Owner | ChatGPT |
| Created | 2026-11-03 |
| Updated | 2026-11-04 |
| Next review | 2026-11-05 |
| Baseline | PR #193; scheduler tested at `b29f8c2b`, integrated remote `efeff949`; isolated `ryan-tools-pr193-two-gpu` worktree |

## Outcome and scope

Continue PR #193 with transition-based terminal GPU errors, a persistent reusable dashboard alert, synthetic scheduler
coverage, a non-destructive polling benchmark and a licensed two-GPU validation procedure. Record actual CodeMeter and
hardware observations separately from synthetic evidence. Do not merge or publish the package.

## Current state

Licensed two-GPU execution on a second workstation is now recorded in
[the validation report](../TUFLOW_GPU_CODEMETER_VALIDATION.md#licensed-two-gpu-observations-on-2026-11-0304).
RTX 3090/3080 hardware, two available paid GPU licences, reversed TUFLOW/nvidia-smi numbering, concurrent runs,
external occupancy, real 60-second recovery, grouped placement, unknown assignment and cancellation were exercised.
CodeMeter endpoint failure was tested without stopping the licence service. All test solvers exited and allocations
returned to zero. The one-GPU observations below remain historical evidence from the earlier machine.

A strict benchmark JSON annotation was corrected without changing its output. Incoming remote history/report changes
through `efeff949` were fast-forwarded and preserved. Local Ruff remains blocked by Windows; hosted checks are required.
Native keyboard interrupt delivery, a real licence-service restart and interactive terminal visual inspection remain
explicit scope limits, rather than claimed results.

## Next action

Verify the GPU-name follow-up commit's hosted checks, then review the hardware evidence and retained scope limits.
Do not merge or publish without separate authorization.

## Completion criteria

- A configured unavailable GPU produces one transition `ERROR`, a persistent red dashboard alert and a recovery message.
- Other clear groups continue, uncertainty pauses conservatively, and simulation outcome/Resume behavior is unchanged.
- Synthetic tests cover alert state, scheduler transitions, interruption and disabled/non-execution paths.
- The benchmark and licensed two-GPU procedure are reproducible, with measured local limitations clearly separated.
- Required source, documentation, package and hosted checks are recorded; implementation is committed and pushed to PR #193.

## Validation and delivery

- 2026-11-03: focused dashboard, CodeMeter, GPU occupancy and runner suite: 101 passed, 2 skipped, 5 deselected. The five
  hardlink-alias cases were deselected after repeated host `OSError: [Errno 9] Bad file descriptor` reads of copied
  `run_tuflow_simulations.py` files. The wrapper suite similarly reached 70 setup errors before repository code because
  endpoint security denied the copied wrapper read; one non-fixture test passed. Hosted CI remains the wrapper gate.
- 2026-11-03: compileall, benchmark smoke, Loguru formatting policy, documentation/index validation and `git diff
  --check` passed.
- 2026-11-03: five local and five CodeMeter benchmark samples plus one unavailable-CodeMeter observation recorded.
- Ruff and Pyright executables: blocked by Windows `PermissionError [WinError 5]`; hosted Ruff identified one benchmark
  type-union ordering issue, which was corrected in `b29f8c2b`; exact-head hosted checks rerun after subsequent pushes.
- No-bump package build reached the QGIS style inventory and stopped because the isolated worktree lacked a materialized
  `qgis-resources` checkout. The optional resource clone was stopped and package verification dropped at user request.
- MCP exposure assessed: no catalogue change; this is an existing wrapper behavior change plus a maintainer benchmark.
- Implementation and prompt committed as `e0863cc4`; handoff record as `f3c8bf40`; benchmark lint correction as
  `b29f8c2b`. All were pushed to the existing PR #193 branch. No merge or publication.
- PR description updated with the two-GPU handoff and validation evidence. Exact-head hosted checks remain the delivery
  gate after the final report-order push.

## Progress

### 2026-11-03

Added structured GPU availability states, transition logging and a generic dashboard alert interface. Captured five
local and five CodeMeter measurements on the one-GPU workstation, plus CodeMeter-unavailable behavior. CodeMeter had
no physical GPU identifier; a separate TLF correlation reported GPU 0 for the shared PID. Two-GPU licensed scenarios
remain outstanding and are documented without fabricated results. Added a standalone copy-ready prompt for the future
two-GPU/two-licence agent session and prepared it for delivery in PR #193.

Committed and pushed the implementation, benchmark, evidence and future hardware prompt as `e0863cc4`. The partial
isolated `qgis-resources` clone remains outside the commit and has no parent-repository delivery effect.

Reordered the user-activity report so inferred log/TLF groups appear first, active CodeMeter processes follow, and the
authoritative current CodeMeter licence-utilisation table is the final output group. Focused activity-report validation
passed with 17 tests; compileall, documentation/index validation, Loguru policy and `git diff --check` also passed.

Changed runner execution-history `recorded_at` values from forced UTC to the local computer timezone with an explicit
UTC offset. Existing timezone-aware UTC rows remain readable. The focused execution-history suite passed with 22 tests;
compileall, documentation/index validation, Loguru policy and `git diff --check` also passed.

Reordered new runner history CSVs for operators: timestamp and event/result fields first, followed by model/process
context, with internal hashes and schema metadata last. Existing internal-first CSVs retain their header order and are
still readable and appendable. The focused history suite passed with 24 tests. Four copied-wrapper integration cases
were blocked during fixture import by the host's existing `OSError: [Errno 9] Bad file descriptor`, before repository
code executed.

### 2026-11-04: licensed two-GPU execution

- Hardware results and sanitized timing tables are in the validation report. Raw evidence is retained outside Git at
  `C:/Temp/ryan-tools-pr193-two-gpu-20261103`; no tutorial models, TLFs, operational identifiers or licence secrets are committed.
- `pytest --noconftest` focused GPU/CodeMeter/dashboard/runner tests: 101 passed, 2 symlink skips. Focused wrapper tests:
  71 passed. Incoming execution-history tests: 24 passed. The fixture-independent suites bypassed only the repository
  startup requirement for unrelated test-data materialization; no tests were deselected.
- Strict Pyright passed on all six selected production/benchmark files using explicit source-checkout extraPaths and
  explicit file arguments; this avoids resolving wrappers against the older globally installed package.
- Benchmark CLI smoke and Loguru formatting policy passed. Documentation/index, explicit changed-document links and whitespace checks passed.
- No-bump wheel build/verification and installed-wheel smoke passed on the hardware baseline. The final integrated
  wheel also built and verified: SHA-256 `5780af0f4d1bd7827936730239355c4875d6ffc5b82dfc027fec4653e54e1e19`. The normal Python installation is unchanged. The optional fully isolated
  dependency smoke could not run all wrapper behaviors because its dependency target lacked pandas; normal installed
  smoke passed with the existing dependencies.
- Submodule initialization in the new worktree exposed incorrect relative core.worktree paths. Only those new
  submodule configurations were corrected; their checkout used Git LFS skip-smudge. No existing submodule worktree was
  changed, and the excluded submodule was not inspected.
- Delivery: this record accompanies the source/documentation commit pushed to the existing PR #193 branch. The PR
  records the final commit SHA and exact-head hosted results. Only the four task files are staged/committed; generated
  wheel artifacts remain local and isolated submodule checkout state is excluded.
  No merge, publication or production-model mutation. Next review: 2026-11-05.

Hosted Ruff on `7c2e003f` found a missing blank line between standard-library and third-party benchmark imports.
Applied the reported import-spacing fix; this changes no behavior or packaged code. Exact-head checks rerun on the
follow-up commit, with the PR description retaining the authoritative delivery SHA and results.

Hosted lint passed on `006c491e`; formatting then identified four line-wrap changes across the scheduler and two
test files. Applied the exact formatter output; ASTs are unchanged. The affected runner/activity suites passed
101 tests with 2 symlink skips, strict Pyright passed on all three files, and the no-bump wheel rebuilt/verified
(SHA-256 `1d9864663d0b14c54a1a7ab952c2b2f156b16d9464b10191ee8965645d2f3243`). The preceding installed-wheel smoke remains behavioral evidence
for the identical ASTs. No repeat solver execution is needed for these formatting-only changes.

Final-head Ruff/whitespace passed at `b7bb9814`; Package, MCP smoke and POSIX security also passed. Hosted strict
Pyright then exposed intentional private-state access in the dashboard unit tests and overly narrow CSV key
inference in the legacy-history fixture. Documented the test-only private-access exception and annotated the
CSV writer with string keys. All 14 PR-changed Python files now pass local strict Pyright; 31 affected tests pass.
These test-only annotations do not require another wheel or solver run. Hosted checks rerun on the follow-up.

### 2026-11-04: GPU names and explicit device numbering

User requested GPU model names and an unambiguous `-puN` mapping in the runner and activity wrapper.
A shared read-only helper enumerates CUDA names once per executing batch; startup, launch messages,
dashboard rows and configured-slot alerts show names, with PCI identifiers for duplicate GPU models.
The map belongs to the launch environment, not external processes with potentially different CUDA settings.
Activity rows use their own companion HPC header and preserve the simulation machine; no current-machine
or NVIDIA-index guesses are applied to historic/remote logs. Unknown/stale/conflicting names stay explicit.
The conservative companion freshness check can omit names on long-running models whose HPC log is older.

Validation: 110 focused GPU/runner/activity tests passed, 2 Windows symlink skips; strict Pyright passed on
all seven changed Python files. Read-only hardware smoke matched `-pu0` RTX 3090 / `-pu1` RTX 3080 and
confirmed that a fresh process with CUDA_VISIBLE_DEVICES=1,0 reverses the labels. Both retained licensed
numbering logs produced the expected names. No solver was launched for this presentation change.
Loguru policy passed. Local Ruff remains blocked by WinError 5; hosted lint/format is the final gate.
MCP assessment: existing runner/activity workflows already expose this behavior; no extra tool is warranted.
Delivery: this follow-up is being committed/pushed to existing PR #193; exact-head CI and package evidence
are recorded in its description. Keep the PR open and unmerged; no publication. Next review 2026-11-05.
