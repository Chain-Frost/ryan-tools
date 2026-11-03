# TUFLOW GPU availability alerts and CodeMeter validation

| Field | Value |
| --- | --- |
| Status | Active; implementation and report-order update ready for PR delivery; two-GPU validation pending |
| Owner | ChatGPT |
| Created | 2026-11-03 |
| Updated | 2026-11-03 |
| Next review | 2026-11-04 |
| Baseline | PR #193 branch `fix/tuflow-textfile-optional-settings-rows`; remote head `8ca3a8bc`; isolated worktree `Q:\BGER\PER\RPRT\temp\ryan-tools-pr193-gpu-alerts` |

## Outcome and scope

Continue PR #193 with transition-based terminal GPU errors, a persistent reusable dashboard alert, synthetic scheduler
coverage, a non-destructive polling benchmark and a licensed two-GPU validation procedure. Record actual CodeMeter and
hardware observations separately from synthetic evidence. Do not merge or publish the package.

## Current state

Implementation, synthetic tests, benchmark harness, measured one-GPU evidence and the future-agent hardware prompt are
complete locally. The development workstation has only one TUFLOW-capable GPU and its one observed GPU licence is in
use by another user's running model, which was not interrupted. Licensed two-GPU execution remains a named external
validation dependency. Current read-only observations confirm that CodeMeter reports a GPU licence and PID but not a
physical GPU ID.

## Next action

Run the committed hardware prompt on a machine with two compatible GPUs and two available CodeMeter GPU licences.
Verify exact-head hosted Ruff, strict Pyright and CI after each final PR push.

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
