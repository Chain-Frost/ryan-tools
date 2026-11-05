# Local handoff: PyTuflow #63 + ryan-tools #235

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
