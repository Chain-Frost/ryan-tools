> **Update for consolidated PR #235 (2026-11-06):** Historical entries below
> refer to the earlier PyTuflow #63 commit b1026d0cf72118ef68a0b6229610f427a7284e10.
> The new accepted integration source is
> [5360c979a4da577c7eae04a2afded00eb21d5bf9](https://github.com/Chain-Frost/PyTuflow/commit/5360c979a4da577c7eae04a2afded00eb21d5bf9),
> covering unordered result names, dotted TLF/TPC filenames, network
> directory-entry identity and implicit override fragment reuse.
> See [ryan-tools #235](https://github.com/Chain-Frost/ryan-tools/pull/235)
> and [PyTuflow #63](https://github.com/Chain-Frost/PyTuflow/pull/63).
> The new pin requires Windows/QGIS/CodeMeter acceptance before release.

# TUFLOW runner grouping and live model progress

| Field | Value |
| --- | --- |
| Status | PR #205 merged; override parser repin in PR #211; local validation passes, hosted checks pending |
| Owner | ChatGPT / local validation agent |
| Created | 2026-11-04 |
| Updated | 2026-11-05 |
| Next review | 2026-11-06 |
| Baseline | `main` at `ec8734256e33447856b648e02d5fe0c5a5802447` (merged PR #193) |
| Branch | `fix/pytuflow-override-pin`; targets `main` (earlier runner implementation: `feat/tuflow-runner-progress`) |
| Pull request | [#211 parser repin](https://github.com/Chain-Frost/ryan-tools/pull/211); earlier [#205](https://github.com/Chain-Frost/ryan-tools/pull/205) merged |

## Outcome and scope

Reduce the live runner's width by grouping rows in one box per source TCF and
keeping GPU descriptions in the startup log. Add end/model time and elapsed-model
percentage using the existing accepted public PyTUFLOW context API plus bounded
live-log reads. Supply a concrete Windows/local-machine handoff. Preserve the
current scheduler, imported batch order, original Resume indexes, global ten-row
default, GPU uncertainty policy and process-observed CSV completion.

The subsequent user request excludes ETA and adds observed run phase, latest
HPC timestep and signed final cumulative mass error. These use the existing
`Detail` field. `SHOW_PROGRESS_DIAGNOSTICS` and its CLI overrides control the two
numeric diagnostics; phase remains visible when they are hidden.

This request authorizes the observational polling introduced here and supersedes
the earlier scope-03 exclusion of all live TLF polling. It does not authorize
log-based completion, retries, recovery, launch gating, automatic merge or release.

## Current state

- The shared renderer accepts optional metadata grouping and observation-only
  task updates. Ungrouped workflows retain their prior presentation.
- Runner rows have TCF headings, short GPU identifiers, `End (h)`, `Model (h)`, `%`
  and `Elapsed`. Duplicate TCF basenames are disambiguated by path.
- The phase/diagnostics follow-up adds current-invocation phase markers, a
  header-identified HPC timestep and explicit final cumulative ME parsing.
  No ETA or additional dashboard columns are introduced. The two numeric
  diagnostics default on and can be hidden while retaining phase.
- Completion takes an already-pending sample under the monitor's short lock,
  verifies the PID and retires that invocation without a final file read or wait.
  Observed final ME is retained beside the actual process exit; a summary written
  between the last poll and process exit remains unavailable.
- The wrapper exposes `MONITOR_PROGRESS` and `PROGRESS_POLL_INTERVAL_SECONDS`;
  polling is default-on for live dashboards, with a ten-second default interval.
- One daemon worker performs prediction and bounded log reads away from the
  scheduler. Missing/unverified values stay unknown; observations never certify
  successful execution. Shared predicted paths are treated as ambiguous.
- The PyTUFLOW source was updated at the owner's request to the immutable
  [fork PR #63](https://github.com/Chain-Frost/PyTuflow/pull/63) commit
  `b1026d0cf72118ef68a0b6229610f427a7284e10`, including the #64 override fix and superseding
  `d089afaddde4b533581ebbe068dc38673c98803b` and the prior PR #17 pin
  `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`. The public log-folder/output-name API is retained.
  PR #63 remains open and is not an accepted upstream release.
- Native Windows TUFLOW execution and mapped/UNC continuous-writer checks passed
  on 2026-11-05. Interactive terminal resizing/screenshots and an independently
  configured Classic model remain unrun. See the dated evidence below and the
  [handoff](../TUFLOW_RUNNER_PROGRESS_HANDOFF.md).
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

Review the remaining interactive terminal and Classic-engine cases against the
local handoff, and review final-head hosted checks before any merge decision.
Native multi-GPU execution is unavailable on the one-GPU validation machine;
grouped IDs and multiple-TCF scheduling have automated coverage. Keep the PR
draft until the remaining acceptance limits are reviewed.

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

Source validation including the phase/diagnostics follow-up completed in an
isolated checkout using Python 3.14.9 on Linux:

```text
python -m pytest -q tests/functions/test_live_dashboard.py tests/functions/test_live_dashboard_logging.py tests/functions/test_dashboard_workflow.py tests/functions/tuflow/test_run_progress.py tests/orchestrators/tuflow/test_advisory_run_progress.py tests/orchestrators/tuflow/test_simulation_runner.py tests/scripts/test_run_tuflow_simulations.py tests/scripts/test_tuflow_advisory_runner.py tests/functions/tuflow/test_execution_history.py
```

Result: **357 passed, 12 skipped**, 5.15 seconds. The skips are native Windows
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
  passed. Verified wheel: 913,634 bytes; SHA-256
  `ed79832b212b248e39408db426f229cc1c10d6364b21d165b69df3d5651d39a5`.
  The four changed library modules were compared byte-for-byte with that wheel.
- Installed the wheel into a disposable target and copied the wrapper outside
  the repository. `--help` and explicit textfile `--export-only
  --no-monitor-progress --no-progress-diagnostics --no-pause` passed; no execution
  CSV was created. An installed-package predictor smoke resolved
  `model_01.0p.tlf` and End Time 3 h. The installed reader also returned
  `writing_results`, timestep 0.674 s and final ME -0.25% from disposable logs.
  The imported module location was verified as the disposable installed target.
- Rendered two illustrative TCF panels for visual inspection. Automated narrow
  terminal cases verify four active rows, IDs, statuses and GPUs fit at widths
  100/120 and height 24 with long diagnostics, without wrapping/cropping the
  lower panel. Ellipsized headers/cells now shrink before protected identity
  columns; shared regressions retain literal brackets and normal default wrapping.

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

### 2026-11-04 phase and diagnostic follow-up

The user excluded ETA and authorized the other suggested observations. Added
monotonic observed phase (`Initialising`, `Solving`, `Writing results`), the
latest complete HPC step's labelled `dt` and signed explicit final cumulative
ME. All use the existing bounded reads and one-line `Detail` field. Added
`SHOW_PROGRESS_DIAGNOSTICS = True` and `--progress-diagnostics` /
`--no-progress-diagnostics`; wrapper version is `2026-11-04.2`.

Integrated review corrected normal initialization detail being labelled as a
warning and the longer Detail text squeezing run/GPU IDs out of narrow tables.
The shared renderer now lets configured ellipsis columns shrink preferentially
while their headers/cells stay on one line; no new public column setting was
needed and default wrapping remains covered.

Kept completion free of filesystem work: the monitor atomically returns an
already-cached pending update only for the matching PID, then retires it.
Completion callbacks still run first. No terminal read, new polling cadence,
thread join or retry loop was added. Unknown final ME remains omitted; reused
run indexes and late worker results cannot attach diagnostics to a new process.

The preceding `4e5b0e7` revision had all six hosted checks green, including
2,096 Windows passes, 14 skips and 8 deselections. The updated PR description
records the separate hosted result for this follow-up's published revision.

## 2026-11-05 PyTUFLOW source update

- The standard developer and end-user installers select PR #63's fixed commit
  through `repo-scripts/pytuflow-requirement.txt`; installer guards, diagnostics
  and regression expectations use the same source. Published package metadata
  retains `pytuflow>=1.2,<2`, avoiding a direct Git URL in PyPI metadata.
- The consumer distinguishes non-blocking PyTUFLOW CRS warnings from failed
  input requirements, preserving their run provenance and surfacing advisory
  messages. Default-on raster-header failures remain blocking.
- The eight obsolete expected-failure allowances in the 161-TCF/174-run replay
  are removed. A clean replay is now required; previously recorded acceptance
  for the older pin is historical and does not validate the new source.
- CI, native Windows/GDAL, installed-wheel and real TUFLOW/SMB acceptance are
  pending for this new head. Do not describe prior source verification as
  evidence for PR #63 or release automatically.

### 2026-11-05 local Windows acceptance of PR #63 source

The evidence in this entry supersedes the preceding pending-validation statement
for implementation head `2841f8d49242d081148289b1eae5f6f97d91cd2f`. Fetched both
refs before validation; `main` remained `ec8734256e33447856b648e02d5fe0c5a5802447`.
Used detached worktree `E:/Github/ryan-tools-pr205-validation`, preserving the
original branch checkout. Only the existing test-data and QGIS-resource pins
were initialized. No submodule pins changed and the excluded submodule was not
inspected.

Environment and provenance:

- Interpreter: `C:/Program Files/Python314/python.exe`, Python 3.14.6, Windows x64.
- QA root: `F:/Temp/CodexValidation/PR205_20261105`; scripts, JSON, JUnit XML,
  console captures and disposable models remain there. Network data is confined
  to a matching disposable `CodexValidation/PR205_20261105` directory.
- PyTUFLOW 1.2.9 installed with `python -m pip install --no-deps --target
  <QA_ROOT>/dependency -r repo-scripts/pytuflow-requirement.txt`. Verified its
  module path in that target and `direct_url.json` selecting
  `d089afaddde4b533581ebbe068dc38673c98803b`; archive SHA-256
  `cc078211560142f7248df9b4834bdadacf395c406d4600e2ee4249220e8e5532`.
  The global user installation was not changed.
- TUFLOW `C:/TUFLOW/tuflow-2026.3/TUFLOW_iSP_w64.exe`, build
  `2026.3.0-iSP-w64`; HPC 3.51.9, CUDA driver 13000. CUDA API and actual HPC
  header both map device 0 to NVIDIA GeForce RTX 5090. No CUDA selector override
  was introduced. One physical GPU is available.
- GDAL 3.13.3 and Rasterio 1.5.2 are installed in normal Python. This host has
  an inherited HDF5 DLL import-order conflict (`0xc0000139`) when h5py loads
  after GIS readers. A disposable h5py 3.16.0 installation alone did not fix
  that order dependency. Loading `h5py` before `pytest` produced the clean
  focused/replay results below; no project import or global package was changed.
  A report-directory reuse also correctly raised `FileExistsError`; the clean
  replay used a fresh output directory rather than overwriting its reports.

Source and package validation:

- `python -m ruff check` and `python -m ruff format --check` on all 22 Python
  files returned by `git diff --name-only origin/main...HEAD -- '*.py'`: pass,
  Ruff 0.16.6. `python -m pyright --pythonpath
  "C:/Program Files/Python314/python.exe"` on those same files: zero errors,
  warnings or information, Pyright 1.1.411 using repository strict settings.
- The nine-file focused command above, invoked with `import h5py, pytest;
  pytest.main(...)`: **367 passed, 2 skipped**, 7.71 s. Both skips require
  unavailable Windows symlink privileges; native sharing and subprocess
  lifecycle/interrupt cases passed. Evidence: `focused-clean.txt` and XML.
- Pinned API, input/command preflight, both installers, opt-in checker and full
  real-example tests: **62 passed**, 31.80 s (`preflight-clean.txt` and XML).
  `example-reports-clean/results.json` confirms **161/161 TCFs and 174/174 runs**
  pass with the exact new source, without old allowances. GPU device/occupancy
  tests separately report **17 passed**, 0.43 s (`gpu.txt` and XML).
- Additional installed-reader actual-log replays passed for EG05_018 (-72 to
  3 h) and EG17_002 (0 to 48 h), recorded in `extra-replays.json`. The focused
  suite covers the restart, negative-zero, partial-line, main/HPC disagreement,
  signed Module 11 ME, collision, cached-finish and late-result cases.
- `python repo-scripts/check_documentation.py`, explicit changed-record/handoff
  `--links-only` checks and `python repo-scripts/check_loguru_formatting.py`: pass.
- `python repo-scripts/build_library.py --no-bump --skip-pip` and
  `python repo-scripts/verify_wheel.py
  dist/ryan_tools-26.11.3.9-py3-none-any.whl`: pass. Wheel: 913,217 bytes,
  SHA-256 `0bfdfd1394ce4eefb76cadee5ef3f4f51f37daeaaf870ea3f7d9969ddadaa7b0`.
  No version bump or tracked artifact change. Installed with `pip --no-deps
  --target <QA_ROOT>/installed`; verified the imported library came from there.
- Copied wrapper `runner.py`, version `2026-11-04.2`: `--help`, explicit
  `--mode textfiles --no-tcf --input-file <QA_ROOT>/runs.txt --export-only
  --no-monitor-progress --no-pause` and invalid `--progress-poll-interval 0`
  checks pass. Export-only created no execution CSV. Invalid input returned
  exit 1 with the setting name and bound. `smoke-results.json` also confirms
  that all seven changed library modules match the wheel source bytes.

Real solver and network evidence:

- Extracted the checksummed public example archive into disposable `model/`;
  EG00_001 retained `Tutorial Model == ON`. No production model or paid
  execution was used. `native_run.py` executed the copied wrapper twice with
  textfile input, `--run-mode all --no-preflight --no-export --no-pause
  --launch-delay 0 --progress-poll-interval 0.1 --console-log-level SUCCESS`.
  Both runs exited 0; elapsed wrapper times were 75.65 and 69.75 s, including
  process/GPU inspection. Each actual solver ran about 19-20 s. All three phases
  were observed, followed by 3 h / 100%, dt 0.694 s and final ME 0% retained
  beside `Exit 0`. CSV START/FINISH records agree with actual child exits.
  The rerun began with old logs present and unknown model time, then verified
  new progress. Evidence: `native-observations.json`, `native-wrapper-1.txt`,
  `native-wrapper-2.txt` and `runner_runs.csv`.
- A third actual copied-wrapper run with `--no-progress-diagnostics` exits 0
  and omits dt/ME from the completed row (`toggle.txt`, UTF-16 PowerShell
  capture). Intermediate phase visibility with this toggle is established by
  focused tests, not this redirected capture. A headless Resume with
  `--no-monitor-progress --no-live-dashboard` skips the already successful
  run without a GPU query or solver launch (`resume-headless.txt`).
- `native_variants.py` verified the requested `s2,s1,e1,e2,e4,e3,s4` filename
  order, long dotted name, `01.0p`, a bare event alias and a Log Folder with
  spaces against actual TUFLOW-created paths. `-et1` overrides the configured
  3 h end: effective end/model time is 1 h; the initial effective end remains
  unknown. Both variants exit 0, with dt 0.563 s and final ME +0.04%.
  Evidence: `variant-results.json` and `variants.txt`.
- Three repeated alias runs exit 0; the first two launched within the same
  minute, with old completed logs preserved. Every initial model time was
  unknown. Evidence: `repeat-results.json` and `repeat.txt`.
- Real solver runs with mapped and equivalent UNC log folders containing
  spaces also exit 0 with correct prediction, phases, 1 h end and diagnostics.
  Automatic GPU selection was used and actual HPC headers select device 0.
  During each local/mapped/UNC variant, the HPC file grew while QA deliberately
  held the shared reader handle for 0.5 s. File identity remained unchanged
  across that observation: append is proven, runtime replacement is not inferred.
  Evidence: `mapped-solver-results.json` and `unc-solver-results.json`.
- `io_acceptance.py` passes long-lived/reopened writer, source rename and
  replacement-after-close tests locally and on mapped/UNC paths. A writer
  denying read sharing gives one unavailable sample without a retry loop.
  The large pair is 18,000,039 + 17,944,056 bytes: initial content read totals
  262,144 bytes, then 100 unchanged polls read **zero content bytes**. Handle
  counts remain 140 before/after 50 changed samples. Median/max unchanged
  sample times: local 0.026/0.071 ms; mapped 2.360/170.471 ms; UNC
  2.359/145.514 ms. Denied-read samples return in 0.107/1.548/2.212 ms.
  These are warm filesystem samples; the separate worker isolation tests cover
  slow/unavailable I/O and scheduler/shutdown independence. Production interval
  remains 10 s; 0.1 s was an intentional disposable-run observation override.
- `interrupt_native.py` exercises the installed runner's real TUFLOW child with
  the public interruption event and execution-history callbacks. Six seconds
  after launch, interruption stops the owned solver in 0.124 s with monitoring
  enabled. One completion callback records actual exit 1 as CSV `FAILED`, and
  no success key is retained. A separate harmless Python sentinel remains
  alive, then is explicitly cleaned up by QA. Evidence:
  `interrupt-results.json`, `interrupt-history.csv` and `interrupt.txt`.

Remaining acceptance limits: physical terminal resizing, VS Code interaction and
before/during/after screenshots are **not run**; console captures and automated
100/120-column, 24-line layouts are separate evidence. No independently configured
Classic model or second physical GPU was tested. Equal-basename TCF grouping,
global row budget and grouped GPU IDs have focused synthetic coverage. No
universal solver replacement/sharing claim follows from these append tests.

All nine hosted checks passed on `2841f8d4`: Python CI
[41905419682](https://github.com/Chain-Frost/ryan-tools/actions/runs/41905419682),
Fast PR checks
[41905419663](https://github.com/Chain-Frost/ryan-tools/actions/runs/41905419663)
and pinned-input validation
[41905419658](https://github.com/Chain-Frost/ryan-tools/actions/runs/41905419658).
This validation found no demonstrated product defect requiring a source change.
Delivery: only the acceptance record, its register row and a stale parser-source
description are committed and pushed to the existing PR branch. The PR is
returned to draft as the handoff requires while acceptance limits remain.
There are no inline review threads. The PR description owns the exact final
documentation commit and its separate terminal hosted results. No merge,
auto-merge, release or dependency repin is authorized.

## 2026-11-05: authorized override remediation and parser repin

The owner subsequently requested implementation of PyTuflow issue #64, retaining
all consolidated fixes in the existing PyTuflow PR #63 and creating a new
immutable parser pin for ryan-tools. This authorizes this repin and PR delivery;
the earlier session's no-repin boundary remains historical.

- PyTuflow [PR #63](https://github.com/Chain-Frost/PyTuflow/pull/63) is updated and
  pushed at `b1026d0cf72118ef68a0b6229610f427a7284e10`. The fix parses the selected
  override once and includes its commands in eager/deferred contexts, preserving
  the original model name, relative paths, accumulated variables/events and
  machine-specific precedence. Issue #61's TEF fix remains included.
- ryan-tools [PR #211](https://github.com/Chain-Frost/ryan-tools/pull/211) updates
  the source requirement, installer guard, compatibility remedy and current docs
  together. Four real-parser resolver regressions cover absent, generic,
  machine-specific and both overrides, and pinned-parser CI runs the resolver
  suite. Existing advisory timing behavior is retained: conflicting Start/End
  declarations remain unknown until the solver log supplies effective timing.
- Local ryan-tools package version is `26.11.5.10`. The verified wheel is ignored
  under `dist/`; CI builds the declared version from source. It is not published.

Validation against this exact parser archive:

- Python 3.14.6: **115 passed** across resolver, compatibility, input preflight,
  both installer modules and public example replay. The replay retains all
  **161/161 TCFs and 174/174 contexts passing**, with strict validation and no
  historical allowances. The installed distribution reports the exact SHA URL.
- Parser native Python 3.12.15 / QGIS 4.2.3 / GDAL 3.13.3: **868 passed, 10
  skipped** across parser, dependency, BC, raster and CRS checks. Skips are
  platform-specific or require unavailable Windows symlink privileges.
- Parser wheel built from the source distribution: **21 installed native
  override/TEF tests passed**. Its modified modules match the committed source.
- ryan-tools installed wheel: package/resource/CLI/copied-wrapper smoke passed;
  **4 real resolver override cases passed outside the checkout**, importing both
  libraries from isolated installed locations. Existing vendored similarity
  smoke emits a `ComplexWarning` and completes successfully.
- Ruff 0.16.6 lint/format, strict Pyright on all five changed Python files,
  documentation links and `git diff --check` pass.

Evidence is retained outside the repositories under
`E:/validation/pytuflow-issue64-20261105/`: `final-native.log`,
`final-installed-native.log`, `consumer-final.log`,
`example-reports-final/results.json`, `ryan-final-build.log`,
`ryan-installed-smoke-final.log` and `installed-resolver.log`.
Python 3.14 validation preloads h5py before pytest to avoid the existing native
DLL import-order problem; no global package environment was changed.

Delivery: implementation commit `4b5690f7` and this evidence update are on the
consumer PR branch. All nine final-head hosted checks are pending at this
milestone; inspect their terminal results before approval. PyTuflow #63 reports
no hosted checks. Both PRs remain open and unmerged; no solver, upstream PR,
auto-merge or package publication is part of this update. Next action: review
the final-head CI and both PRs. Next review: 2026-11-06.
