# Local handoff: PyTuflow #63 + ryan-tools #235

| Field | Current local acceptance status |
| --- | --- |
| Status | Needs review: performance and correctness pass; CI repair pushed; operational gates remain |
| Owner | ChatGPT / maintainer |
| Updated | 2026-11-06 |
| Next review | 2026-11-07 |
| Next action | Verify final-head CI and review remaining licensed active/occupied-GPU acceptance gaps |

The owner authorised this handoff, then selected the current wrapper's complete
390-run plan and an **under-50-second** input-check target. The historical
40-run benchmark below remains historical. The owner also authorised refreshing
the copied model wrapper while preserving its settings, including the exact
`import argparse  # noqa: I001, RUF100` line.

## Objective and exact revisions

Complete **read-only correctness, performance, and operational acceptance** before
merging either PR. Do not publish packages or merge branches as part of this work.

- PyTuflow: https://github.com/Chain-Frost/PyTuflow/pull/63
  - Expected source SHA: 5360c979a4da577c7eae04a2afded00eb21d5bf9
  - Branch: feat/consolidated-validation-59-60-62
- ryan-tools: https://github.com/Chain-Frost/ryan-tools/pull/235
  - Branch: consolidate/tuflow-runner-209-210-213
  - Pin: repo-scripts/pytuflow-requirement.txt
- This work combines prior ryan-tools PRs #209/#210/#213 and PyTuflow
  #63's TCF override and batch validation changes.

## Constraints

Use Windows 11, Python 3.14 for ryan-tools, and the locally installed
QGIS/PyTuflow environment where necessary. Work from clean local checkout
of the **PR branches**, not main. No administrative privileges are assumed.
Network project files may be on Q: or UNC. Do not copy private project
data into GitHub, logs, screenshots, public issues or attachments.
Do not alter production model inputs, GeoPackages, source rasters, SQLite
journals, logs that a solver is writing, or running model processes.
Run real TUFLOW only with approved local tutorial/test models and available
licenses/GPU ownership; never terminate another user's process.

Do not replace a failed check with a skip, weaken fail-closed semantics,
or treat the presence of a version number such as 1.2.9 as proof of the pin.

## First: environment and dependency provenance

1. Check out both PR branches, record git status and exact HEAD SHAs.
2. Run the ryan-tools pinned dependency installation according to the local
   installer policy (or explicitly install the SHA zip in the intended Python).
3. Verify Python executable and version, pip check, PyTuflow import path,
   and the installed distribution's direct_url.json. It **must** identify
   SHA 5360c979a4da577c7eae04a2afded00eb21d5bf9 and must not shadow
   another installed PyTuflow.
4. Record Windows, QGIS, GDAL, Python, PyTuflow metadata, ryan-tools,
   TUFLOW executable version, GPU names/IDs, and CodeMeter availability.

Example provenance commands (PowerShell, from ryan-tools):
    py -3.14 -m pip install --force-reinstall --no-deps -r .\repo-scripts\pytuflow-requirement.txt
    py -3.14 -m pip check
    py -3.14 -c "import sys, pytuflow, importlib.metadata as m; print(sys.executable); print(pytuflow.__file__); print(m.version('pytuflow')); print(m.distribution('pytuflow').read_text('direct_url.json'))"

## Correctness tests before timings

From PyTuflow PR #63 checkout:
    py -3.14 -m pytest -q tests/test_result_name_translation.py tests/test_gpkg_safety.py tests/test_tcf_overrides.py tests/test_fragment_dependencies.py tests/test_batch_validation_mvp.py

From ryan-tools PR #235 checkout:
    py -3.14 -m pytest -q tests/functions/tuflow/test_run_progress.py tests/orchestrators/tuflow/test_input_preflight.py tests/orchestrators/tuflow/test_simulation_runner.py tests/scripts/test_run_tuflow_simulations.py tests/repo_scripts/test_install_latest_wheel.py tests/repo_scripts/test_install_from_pypi.py

Also run repository-specific Ruff and type checks. The PyTuflow legacy
modules have a recorded pre-existing lint baseline; distinguish newly
introduced findings from unchanged historical violations.

Reproduce the exact 8-flag out-of-order example in PyTuflow #63's
test_result_name_translation.py. Confirm:
- No IndexError with arbitrary argument order; expected name:
  UON_04_M3W_EXG_01.0p_00540m_EnsembleILCL_TP01_CCNear_16M
- Mixed ~e~/~e1~ and ~s~/~s1~ aliases, repeated placeholders,
  unused flags, original flag order and context immutability.
- PyTuflow .tlf/.tpc paths retain the complete dotted AEP result name.
- The runner's advisory TLF prediction resolves the same path for
  an override Log Folder and never touches an active log with a lock.

## Mapped-drive/UNC GeoPackage identity checks

Exercise the two previously affected GPKGs if accessible, using metadata
only, with the model/editor not changing them. Inspect and record:
- os.DirEntry.stat() (st_dev, st_ino, size, mtime_ns, ctime_ns)
- Path.stat() of mapped-drive path and resolved UNC target
- check_gpkg_safety(path, expected_stat=entry.stat())
- read-only layer inspection and both previously reported layer names

Previously, DirEntry identity was (0,0) and direct identity was valid, causing
false 'source alias changed' findings. The fixed code falls back to
size+timestamps only when directory-entry identity is incomplete, and
retains direct path/target identity validation and SQLite WAL/rollback/hardlink
checks. Verify stable files pass, changed metadata and unsafe journals fail.
Compare file size/times and sidecar presence before/after. Never write to
the GeoPackage, turn off safety, force SQLite checkpoints, or move aliases.

## Batch dependency collection and timing

Use the same real 40-run model/flags from the original report:
2 AEPs x 2 durations x 10 temporal patterns with the implicit override.
Do not test a different unrepresentative model instead of the real one.
Record flags in their scheduled order. Do not launch solver processes for
preflight benchmarking.

Baseline from the earlier unprofiled Windows run at b1026d0:
- check_simulation_inputs end-to-end: 54.61 s
- parse: 2.938 s
- dependency collection: 22.898 s
- physical inspection: 1.274 s
- files examined: 49

The remaining ~27.5 s was not isolated. Separate API loading was observed
at 5.773/5.187 s in later independent processes. A cProfile 80.779 s
run is **not** a comparable unprofiled baseline.

Capture with perf_counter in separate fresh processes:
1. interpreter startup / imports / validation-API load;
2. TCF construction/parsing (including selected override);
3. collect_file_dependencies on 1, 10, 40 and optionally 80 real selections;
4. validate_batch_inputs and consumer check_simulation_inputs;
5. physical inspection (files, layers, BC CSV, raster headers, CRS).

Use at least three unprofiled repetitions and report median plus range;
distinguish cold from warmed file cache where feasible. Profile separately
only to explain remaining hotspots; never compare profiled elapsed times
against unprofiled timings as a speedup.

**Required dependency counters** from the public result:
dependencies.fallback_reason, fallback_runs, fragment_evaluations,
fragment_reuses, expression_evaluations, expression_reuses,
and collection elapsed_seconds. Record any exception that causes fallback.
The new 40-run synthetic override test must have fallback_runs == 0 and
match the authoritative fallback evaluator. For the real override model,
fallback_runs should also be zero if all its input types are supported.
If not, report the precise reason and the earliest unsupported input/hook;
do not turn off safety or per-run selection semantics to force the fast path.

Confirm that selected references, run indices, BC event selections,
missing-file findings, GIS layer results and output folder translations
remain equivalent to the previous known-correct result. No false GPKG
identity warnings. Retain per-run context isolation.

## Runner and GPU operational matrix

Without killing or commandeering external solver jobs, test:
- startup with CodeMeter idle, active and unavailable;
- concurrent CodeMeter/process inspections, bounded to about 2 seconds
  when genuinely needed; no unnecessary wait on zero/unverified usage;
- one and two GPU groups where real hardware/licensing permits;
- externally occupied *unclaimed* GPU, release/retry, and batch-claimed
  GPU ownership persisting between sequential child runs;
- no further GPU inspection after all configured groups are claimed;
- event flags left aligned, scenario flags dim/grey, no TUFLOW.exe echo;
- completed exit-0 rows, advisory log updates and absence of file locks;
- wrapper cleanup/resource release before the interactive pause,
  measuring keypress-to-exit separately.

Mark any unavailable two-GPU or license scenario **not tested**, not passed.
Do not use tutorial license modes as proof of CodeMeter detection.
No ETA requirement and no change to model execution arguments.

## Deliverables

Write a short reproducible local report containing: commands, environment,
exact Git SHAs and installed direct_url.json, selected run counts, source
and output file-change checks, test failures with traceback, the 40-run
timing breakdown, fallback counters, before/after comparison, GPU matrix,
and remaining risks. Include exact references to either PR for any
follow-up changes. Do not publish private TCF, GIS paths or file content.
Do not merge PR #63 or #235 without explicit owner approval.

## Local acceptance results — 2026-11-06

### Revisions, environment and installation

- PyTuflow source and installed SHA:
  `5360c979a4da577c7eae04a2afded00eb21d5bf9`; clean checkout, fetched before and
  after validation. The source checkout was fast-forwarded from `b1026d0`.
- Consumer benchmark baseline: ryan-tools
  `29f7c76ce4af9c16e56bcede0809b7c51936c35b`. The shared checkout subsequently
  advanced externally to `a16dcd3` (requested import guard) and `b973896`
  (QGIS resource gitlink). Neither external commit was created by this
  validation session. The latter submodule change was not inspected or validated.
- The verified local wheel includes the import guard. Installed runner,
  progress, compatibility and GPU modules were compared byte-for-byte with
  the final checkout; the installed copied-wrapper source also matches.
- Windows **10 Pro**, build **19045**, rather than the requested Windows 11;
  normal Python **3.14.6**, native QGIS **4.2.3**, Python **3.12.15**, GDAL
  **3.13.3**. NVIDIA GeForce RTX 5090 plus AMD integrated graphics; only one
  NVIDIA GPU was available. TUFLOW log reports **2026.3.0-iSP-w64**.
- CodeMeter service is running. Three live summaries reported zero TUFLOW
  licences in use, taking 0.202–0.220 s. Three combined probes took
  0.302–0.352 s and correctly discarded the unnecessary process-scan result.
- Installed the SHA requirement using normal Python 3.14 with
  `pip install --force-reinstall --no-deps -r repo-scripts/pytuflow-requirement.txt`.
  Imports outside either checkout resolve to installed site-packages. The
  installed `direct_url.json` identifies the exact required SHA archive and
  archive SHA-256
  `bc3488c2ff2de7abcd186ff58cf749a76226f405864f0043c5874999d274e3e1`.
  `pip check` passes.
- Built with `py -3.14 repo-scripts/build_library.py --no-bump --skip-pip`, then
  installed the verified wheel with `pip --force-reinstall --no-deps`.
  Version remains **26.11.5.10**; wheel SHA-256
  `e0894efc19a075a1bace7714bb9958b4cc993c48e1a752662e5279704e39815c`.
  No package publication or version bump occurred. The user-supplied installer
  BAT selects PyPI, so it was not used to substitute a released package for PR code.

### Tests and findings

- The five parser test modules listed above: **98 passed** under Python 3.14,
  and **98 passed** under initialized OSGeo4W/QGIS. Native collection initially
  lacked `dbfread`, then nine cases lacked `rasterio`; user-level installation
  of those dependencies resolved the failures. They were not converted to skips.
- The six consumer modules listed above plus `test_gpu_occupancy.py`,
  `test_codemeter.py` and `test_launch_log_flags.py`: **380 passed, 2 skipped**.
  Both skips require unavailable Windows symlink privileges.
- Initial Python 3.14 pytest imports emitted the known HDF5 DLL-order diagnostic
  `0xc0000139`. Clean reruns preload `h5py` before `pytest.main`; the 98 and
  380 pass counts above were reproduced without that diagnostic. The model
  timing harness uses ordinary imports, with no such workaround.
- Ruff lint and format checks pass on all 13 consumer PR Python files.
  Strict Pyright finds **two reportPrivateUsage errors** in imports of
  `_launch_flag_layout` and `_launch_flag_text` in `test_launch_log_flags.py`.
  This is an outstanding policy finding; no repository type rules were weakened.
- Parser legacy lint remains unclean. A before/after rule-message inventory
  against `b1026d0` retains existing findings, records complexity reducing
  from 16 to 11, and identifies added B905/C408 style findings. Four touched
  library modules report eight Pyright errors in the legacy context/run-state
  modules; fragment collection and GPKG safety alone report zero. These results
  do not constitute a clean parser-wide lint/type acceptance.
- The exact 8-flag translation, aliases, repeated placeholders, context
  immutability and dotted TLF/TPC names pass their regressions. Four actual
  scheduled contexts also agree between PyTuflow and the runner's advisory
  TLF prediction, including the effective override folder; prediction does
  not open the production log.

### Current-model timing and correctness

Three unprofiled repetitions ran serially in fresh processes. They use all
**390 ordered selections**: 3 AEPs × 13 durations × 10 temporal patterns,
with the existing implicit override. The scheduled flag order and complete
manifest are retained only in private local artifacts. Filesystem caches
were not flushed: these are fresh-process, warmed-cache measurements, not
a controlled cold-cache benchmark.

| Stage, seconds | Median | Range |
| --- | ---: | ---: |
| Validation API loading | 1.481 | 1.454–1.489 |
| TCF parsing in consumer call | 1.066 | 1.066–1.138 |
| Dependency collection in consumer call | 5.543 | 5.201–5.597 |
| Physical/GIS/BC/raster/CRS inspection | 1.584 | 1.444–2.788 |
| `check_simulation_inputs` call | **8.069** | **7.876–9.539** |
| Harness entry through completed result, including imports/setup | **9.750** | **9.670–11.357** |

Every full repetition passes: **zero issues, zero warnings**, 72 examined
files, 22 examined GeoPackages and 26 checked CSV headers. Medians of separate
stages need not sum to the median total. Interpreter launch before harness
entry is excluded from the last row. Direct `validate_batch_inputs` repetitions
also pass, taking 7.738–7.821 s (median 7.800 s). A separate installed-only
consumer smoke passes all 390 selections without repository import overrides.

| Collection-only selection count | Median seconds | Range |
| --- | ---: | ---: |
| 1 | 0.152 | 0.147–0.153 |
| 10 | 0.295 | 0.293–0.309 |
| 40, first scheduled selections | 0.734 | 0.709–0.746 |
| 390 | 5.247 | 5.202–5.255 |

Full-batch counters are stable: `fallback_reason=None`, `fallback_runs=0`,
`fragment_evaluations=816`, `fragment_reuses=22246`,
`expression_evaluations=28501`, `expression_reuses=11540`. The first full
consumer dependency result reports collection `elapsed_seconds=5.561`.
The authoritative fallback evaluator passes all **390** selections and its
complete per-run records equal the fragment result, with **zero mismatches**.
That comparison includes selected references, database activation/selection,
command ownership, run ordering and errors. The synthetic override regression
also establishes zero fallback and equality for its 40 distinct contexts.

The owner's current-model target is met. The historical 54.61 s / 40-run
measurement is not a like-for-like speedup baseline for this 390-run selection.
No profiled timing is used as speedup evidence.

### GeoPackage and mutation checks

All **23** inventoried GPKG candidates were checked read-only through mapped
and resolved UNC names. Each reproduced DirEntry identity `(0,0)` with valid
direct identity. Both aliases passed `check_gpkg_safety`, returned identical
layer inventories, and rejected deliberately mismatching expected metadata
without changing any file. File size/timestamps and journal/sidecar presence
were unchanged. WAL/rollback protection remains covered by the core safety
regressions. The earlier two specific reported layer names were not supplied
separately; all available candidate layer inventories were retained privately.

Before/after snapshots of the real dependency/control-file inventory found
exactly one changed file: the explicitly authorised wrapper update. No
production solver was launched, command export was disabled, and no production
input or SQLite sidecar was modified by validation. This is input metadata
evidence, not a filesystem-wide hash audit of concurrently generated outputs.

### Copied wrapper and operational matrix

- Updated the authorised model wrapper from **2026-11-05.2** to
  **2026-11-05.3** using the current implementation. All configuration AST
  values except the version, normalized settings, and the complete ordered
  **390-run manifest** are identical. The original bytes were backed up
  privately before replacement. Model files and running processes were untouched.
- The copied wrapper contains the exact `import argparse  # noqa: I001, RUF100`
  guard, as do the repository runner and installed wrapper. Focused import/noqa
  checks pass both outside the repository and with repository lint settings;
  copied-wrapper Pyright reports zero errors. A full external run of the
  repository's strict lint rules still reports 22 other configuration/comment,
  namespace and complexity findings because repository path-specific exemptions
  do not apply. The requested guard is not claimed to solve those unrelated rules.
- The updated production wrapper exits **0** with
  `--preflight-only --no-export --no-pause --console-log-level SUCCESS`,
  explicitly reporting **390/390 passed**. This command never launches a solver.
- An independently extracted, checksum-verified public EG00 tutorial was run
  twice consecutively through a copied current wrapper and the installed wheel,
  using GPU 0. Both observed exits are **0**, CSV FINISH records report SUCCESS,
  both displayed rows reach 3 h / 100%, original tutorial input hashes remain
  unchanged, and no owned TUFLOW process remains. Total batch harness time was
  49.108 s; this includes actual simulations and is separate from preflight timing.
- Tutorial mode remains ON: these runs prove one-GPU execution/monitoring and
  sequential child completion, **not** active CodeMeter licence detection.
- Real active/unavailable-licence startup, externally occupied GPU release/retry,
  two physical GPU groups, physical terminal colour acceptance and interactive
  keypress-to-exit timing are **not tested**. Focused tests cover the scheduler's
  ownership/reinspection rules and cleanup-before-pause ordering. The copied
  model's configured two GPU IDs were preserved, not certified on this one-NVIDIA
  workstation. No production execution is implied by preflight success.

### Evidence, delivery and remaining gates

Private evidence is retained outside Git under the local temporary directory
`pytuflow63-ryan235-20261106-002824`: reproducible harness scripts, manifest,
all repetitions, fallback comparison, layer/stat inventories, tracebacks,
provenance, original wrapper backup, installed-source comparisons and tutorial
logs/history. Do not commit that directory or its project-identifying paths.

At consumer head `29f7c76`, Fast PR checks and TUFLOW input validation succeeded;
Python CI's [Policy job](https://github.com/Chain-Frost/ryan-tools/actions/runs/42064360602/job/127374872621)
failed in changed-Python checks. Hosted tests, Package, MCP smoke and POSIX
wrapper security succeeded. Those hosted results belong to that historical
head; inspect the externally advanced head's terminal checks separately.
PyTuflow's tested head reports no Actions runs.

At final observation of consumer `b973896`, Fast PR checks succeeded;
[Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/42066801358)
and [TUFLOW input validation](https://github.com/Chain-Frost/ryan-tools/actions/runs/42066801096)
were still in progress. No all-green claim is made. Local final-tree Ruff lint,
format, documentation index/link checks and `git diff --check` pass; the
reported strict-type errors remain unresolved. Shared production Python modules
and the copied-wrapper implementation match the locally installed wheel bytes.

This report and its index/register links are local unstaged documentation.
The model wrapper and local installed packages were updated as authorised.
No commit, push, merge, package publication or GitHub message was performed by
this validation session. Licensed/hardware acceptance, parser lint/type debt
and consumer policy findings remain explicit review gates for #63/#235.
Unrelated overdue register entries were observed and left outside this task.

## CI repair — 2026-11-06

The owner subsequently requested the CI repair on PR #235. At `b973896`,
eight checks succeeded and only Policy failed: strict Pyright rejected the
two intentional private-helper imports in `test_launch_log_flags.py`.
Commit `1b346d77a50b0981302c88f0e68a3b7a1ec6f240` annotates only those imports
with explained `reportPrivateUsage` exceptions, matching nearby internal-helper
tests. Production code and repository type-checking rules are unchanged.

Both focused launch-formatting tests pass. Ruff lint, Ruff format and strict
Pyright pass across all 13 PR Python files; documentation/index checks,
Loguru policy and `git diff --check` pass. The test-only repair requires no
package rebuild. It is committed and pushed to the existing PR branch;
seven hosted checks have passed, with Policy and Hosted tests still pending
at the documentation checkpoint. The owner requested committing this acceptance
report and its index/register links as well. Those documentation changes are
included in the follow-up commit, so final-head checks must be verified again.
The owner has enabled squash auto-merge on PR #235. No manual merge or
publication was performed by this repair session; the operational acceptance
gaps above remain recorded independently of CI.
