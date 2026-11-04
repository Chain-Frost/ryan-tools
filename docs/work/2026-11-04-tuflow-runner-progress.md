# TUFLOW runner grouping and live model progress

| Field | Value |
| --- | --- |
| Status | Draft PR #205 open; native acceptance pending |
| Owner | ChatGPT / local validation agent |
| Created | 2026-11-04 |
| Updated | 2026-11-04 |
| Next review | 2026-11-06 |
| Baseline | `main` at `ec8734256e33447856b648e02d5fe0c5a5802447` (merged PR #193) |
| Branch | `feat/tuflow-runner-progress`; targets `main` |
| Pull request | [#205](https://github.com/Chain-Frost/ryan-tools/pull/205) |

## Outcome and scope

Reduce the live runner's width by grouping rows in one box per source TCF and
keeping GPU descriptions in the startup log. Add end/model time and elapsed-model
percentage using the existing accepted public PyTUFLOW context API plus bounded
live-log reads. Supply a concrete Windows/local-machine handoff. Preserve the
current scheduler, imported batch order, original Resume indexes, global ten-row
default, GPU uncertainty policy and process-observed CSV completion.

This request authorizes the observational polling introduced here and supersedes
the earlier scope-03 exclusion of all live TLF polling. It does not authorize
log-based completion, retries, recovery, launch gating, automatic merge or release.

## Current state

- The shared renderer accepts optional metadata grouping and observation-only
  task updates. Ungrouped workflows retain their prior presentation.
- Runner rows have TCF headings, short GPU identifiers, `End (h)`, `Model (h)`, `%`
  and `Elapsed`. Duplicate TCF basenames are disambiguated by path.
- The wrapper exposes `MONITOR_PROGRESS` and `PROGRESS_POLL_INTERVAL_SECONDS`;
  polling is default-on for live dashboards, with a ten-second default interval.
- One daemon worker performs prediction and bounded log reads away from the
  scheduler. Missing/unverified values stay unknown; observations never certify
  successful execution. Shared predicted paths are treated as ambiguous.
- The accepted PyTUFLOW pin remains
  `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`. The public log-folder/output-name API
  avoids its dotted-output-name suffix defect. No upstream repository changes.
- Native Windows TUFLOW execution, interactive Rich presentation and SMB timing
  remain local acceptance work. See the [handoff](../TUFLOW_RUNNER_PROGRESS_HANDOFF.md).
- User follow-up explicitly requires read access that does not impose write/delete
  denial on continuously written logs. Windows reads use a short-lived
  `CreateFileW` read handle with all three read/write/delete sharing flags, no
  exclusive/byte-range locks or memory maps, and no blocking retry loops.
  Sharing violations defer the optional sample to the normal polling interval.
- Windows destination replacement has a separate OS limitation: legacy
  `MoveFileExW` rejects an existing open destination despite delete sharing.
  Native tests therefore distinguish concurrent append/source rename from
  direct replacement after read-handle closure. Actual solver/SMB acceptance
  must establish which operations TUFLOW performs while running.

## Fixture evidence

Read-only investigation used `ryan-tools-test-data` commit
`d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58`, the parent's existing test-data pin.
Its 244 logs comprise 207 main TLFs and 37 HPC companions. Main logs identify
HPC/Quadtree runs; this corpus does not establish true Classic-engine coverage.

| Example | Purpose |
| --- | --- |
| `tuflow/tutorials/Module_01/runs/log/M01_5m_001.tlf` and companion | Main `SIM` / HPC output times and companion step-table times |
| `tuflow/tutorials/Module_09/runs/log/M09_5m_01p_2hr_001.tlf` | Event placeholders resolved in the actual logfile name |
| `tuflow/TUFLOW_Example_Model_Dataset/log/EG05_018.tlf` | Negative start at -72 h, including negative-zero hour timestamps |
| `tuflow/TUFLOW_Example_Model_Dataset/log/EG06_004.tlf` | Restart with a nonzero start (1 h to 3 h) |
| `tuflow/TUFLOW_Example_Model_Dataset/log/EG17_002.tlf` | Model time beyond 24 h (48 h end) |

Main `Simulation Started` timestamps have minute precision; the HPC companion
does not supply an equivalent invocation timestamp. File freshness and current
main-log evidence are therefore required. Coarse timestamps or ambiguous files
must produce unknown progress instead of using old results. A current main log
can reach the configured end while its companion's last step is slightly earlier.

## Next action

Check hosted CI on the pushed PR head, then execute the local
Windows/solver/SMB acceptance matrix on this PR branch. Keep the PR draft until
those limitations are reviewed; report each unrun or blocked case explicitly.

## Completion criteria

- TCF grouping and short GPU rows render correctly for one/multiple models,
  including equal basenames and a short terminal, with a single global row budget.
- Public prediction works with actual selected events/scenarios and dotted names.
- Reader tests cover signed hours, nonzero starts, >24 h, main/HPC precedence,
  partial lines, old logs, replacement/truncation and bounded reads.
- Optional worker failures and slow file operations never delay child lifecycle
  decisions; disabled/nonexecution paths do not inspect model logs.
- Windows tests verify concurrent append with long-lived/reopened writers,
  source rename and replacement after closure; local acceptance verifies actual
  TUFLOW/SMB behaviour and any different replacement API used by the solver.
- Focused tests, Ruff, strict modified-file Pyright, documentation, Loguru policy,
  wheel verification and copied-wrapper smoke checks are recorded.
- New PR and handoff are pushed, with native acceptance status stated separately.

## Validation and delivery

Source validation after the Windows corrections completed in an isolated
checkout using Python 3.14.9 on Linux:

```text
python -m pytest -q tests/functions/test_live_dashboard.py tests/functions/test_live_dashboard_logging.py tests/functions/test_dashboard_workflow.py tests/functions/tuflow/test_run_progress.py tests/orchestrators/tuflow/test_advisory_run_progress.py tests/orchestrators/tuflow/test_simulation_runner.py tests/scripts/test_run_tuflow_simulations.py tests/scripts/test_tuflow_advisory_runner.py tests/functions/tuflow/test_execution_history.py
```

Result: **281 passed, 12 skipped**, 5.06 seconds. The skips are native Windows
cases, including the shared reader/writer/rename tests; mocked Win32 flags
and handle-ownership tests did run. Real pinned-PyTUFLOW path/context tests and
HPC/Quadtree fixture replays passed. No TUFLOW executable was launched.

- Ruff 0.16.6 `check` and `format --check` passed on all ten modified Python files.
- Strict Pyright 1.1.414 with Python 3.14 and Windows target passed on those same
  ten files: zero errors/warnings. The explicit Windows target checks existing
  Windows-only subprocess annotations as the hosted Windows policy job does.
- `python repo-scripts/check_documentation.py`, explicit changed-document
  `--links-only` checks, `python repo-scripts/check_loguru_formatting.py`, wrapper
  compilation and `git diff --check` passed.
- `python repo-scripts/build_library.py --no-bump --skip-pip` and
  `python repo-scripts/verify_wheel.py dist/ryan_tools-26.11.3.9-py3-none-any.whl`
  passed. Verified wheel: 910,790 bytes; SHA-256
  `05baa73516a23083b8ac0392f6fca72c2345f6f6fc8a85f767ad818bcd96c438`.
  The four changed library modules were compared byte-for-byte with that wheel.
- Installed the wheel into a disposable target and copied the wrapper outside
  the repository. `--help` and explicit textfile `--export-only
  --no-monitor-progress --no-pause` passed; no execution CSV was created. An
  installed-package predictor smoke resolved `model_01.0p.tlf` and End Time 3 h.
  The imported module location was verified as the disposable installed target.
- Rendered two illustrative TCF panels for visual inspection. Automated narrow
  terminal cases verify four active rows, IDs, statuses and GPUs fit at widths
  100/120 and height 24 without wrapping/cropping the lower panel.

Only `tests/test_data` and `qgis-resources` were initialized at their existing
pins for required checks; no submodule edits are part of this work. The excluded
holding-area submodule was not inspected. Generated wheels and QA previews are
local build artifacts, not committed project inputs.

Delivery: implementation, tests, maintained documentation and the copy-ready
handoff are published in draft [PR #205](https://github.com/Chain-Frost/ryan-tools/pull/205)
on `feat/tuflow-runner-progress`, targeting `main`. The implementation commit is
`5c4c9ff5f4bf0f5b389bd5c190dd6812ebd4c96c`; its published Git tree exactly matches
the locally validated source tree. The PR description records the latest pushed
SHA and current hosted checks, including the Windows corrections described below.
No merge, auto-merge or release is authorized by this work.

MCP discovery assessment: the existing runner workflow remains the supported
surface. These internal observation and presentation helpers do not need new
tools or catalogue entries; no launch/approval semantics change.

The work register contains older overdue rows, including prior runner follow-ups
whose PRs are already merged. Those unrelated status reconciliations were not
undertaken as part of this implementation.

## Progress

### 2026-11-04

Reviewed the merged runner, public pinned PyTUFLOW APIs and actual test-data
logs. Identified dotted-output-name handling, negative/nonzero start times,
companion final-step lag and minute-resolution invocation headers as concrete
correctness constraints. Began grouped rendering, asynchronous observations and
focused tests, preserving existing launch/history behaviour.

Completed the implementation and focused validation above. Independent review
identified and corrected narrow-terminal row overflow, runtime observer failure
propagating into child cleanup, and backward model progress after a transient
HPC read error. Main-log generation resets now invalidate the old companion.
The user's continuous-writer requirement added explicit Windows read/write/delete
sharing, non-inheritable scoped handles, mocked cleanup tests and native sharing
acceptance. Runtime observer failures disable progress and warn while preserving
actual process outcomes; test cases cover register, drain, discard, cleanup and
malformed progress conversion.

The first hosted Windows test run on implementation commit `5c4c9ff` completed
with 2 failures, 2,091 passes, 14 skips and 8 deselections. It exposed two concrete
Windows distinctions before local-machine handoff:

- Python 3.14 can report different `st_ctime_ns` values for `os.stat(path)` and
  `os.fstat(fd)` on the same unchanged file. Cross-API comparison caused redundant
  reads. The correction keeps separate path-stat and open-handle fingerprints;
  only observations from the same API are compared. See
  [CPython issue #157671](https://github.com/python/cpython/issues/157671).
- The native test incorrectly expected Python `os.replace` to replace an open
  destination. Legacy Windows `FileRenameInformation` rejects that operation
  even with delete sharing. Tests and documentation now distinguish concurrent
  append/source rename from direct replacement after closure; no sharing
  restrictions or retries were added to the reader. See the
  [Windows rename rules](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/87f86c9b-6c2a-4803-84b7-131a74a434fa).

The PR description records the final published revision and its hosted checks;
earlier successful policy/package jobs do not substitute for that final run.
