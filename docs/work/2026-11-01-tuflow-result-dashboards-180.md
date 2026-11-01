# TUFLOW result dashboard adoption

| Field | Value |
| --- | --- |
| Status | Needs review — Phase B source implementation and hosted validation complete |
| Owner | PR #184 adoption agent |
| Created | 2026-11-01 |
| Updated | 2026-11-01 |
| Next review | 2026-11-08 |
| Baseline | Parent documentation seed `4505e1cd7d2e157e8d800181e582aa7cabb572d9`, based on main `99cd3ccaaabd83b682392f7e285d90b570d353cb` |
| Branch / base | `feat/tuflow-result-dashboards-180` -> `feat/batch-dashboard-foundation-180` |
| Issue | [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), selected AB-025 / AB-032 work |
| Dependency | [Foundation PR #183](https://github.com/Chain-Frost/ryan-tools/pull/183) and its [consumer readiness record](2026-11-01-batch-dashboard-foundation-180.md#consumer-interface-readiness) |
| Adopted foundation SHA | `023253ed612bb0bbd9a61826f27f553c7b2e9447` (published implementation); parent head `f390b3773c970ea6ac141c3fd4466a53b3add552` merged as `89124cc239a3b730007657ad454df9f570bf4089` |
| Delivery | Six wrappers implemented; code `abac412` passed all six Windows CI checks (2,003 hosted tests and copied six-wrapper wheel smoke); PR draft/unmerged, independent review pending |

## Outcome and scope

Make the selected result workflows actually use the shared dashboard contract. For substantial file/item batches,
ordinary processing should stay compact while warnings/errors remain visible and report generation stays truthful.
Existing supported dashboard options should work; deleting accepted options is not the solution to missing integration.

**Optional progress measures belong to each workflow.** Select whichever measures help its users, with appropriate
labels/units and honest unknown values. File counts and data volume are possible choices, not compulsory columns.
A workflow can omit extra measures or use different ones without requiring a renderer change.

This is PR 2 in the stack. The user will start both agents together: begin Phase A immediately, then wait for PR 1's
implemented interface before changing runtime code. The agent is authorised to commit/push scoped work to this
existing child branch and update its draft PR. Do not merge to main, enable auto-merge, force-push or release.

Read [AGENTS.md](../../AGENTS.md), the [development guide](../DEVELOPMENT_GUIDE.md),
[work tracking](../WORK_TRACKING.md), [work register](README.md),
[wrapper standard](../../ryan-scripts/WRAPPER_STANDARD.md),
[TUFLOW wrapper guide](../../ryan-scripts/TUFLOW-python/README.md),
[processor notes](../../ryan_library/processors/tuflow/README.md) and the whole
[parent handoff](2026-11-01-batch-dashboard-foundation-180.md). Reconcile the current remote branches before resuming.

## Current state

The child targets [foundation PR #183](https://github.com/Chain-Frost/ryan-tools/pull/183).
Phases A and B are implemented. The foundation's validated `023253ed` interface was adopted via
normal merge `89124cc`, and its updated one-way pause policy at `56acd26` was merged
without a force push. The six result wrappers were tested on Windows Python 3.14 and as
copied wrappers against the installed wheel at code head `abac412`; independent review
and separately scoped production/native engineering acceptance remain outstanding.

The source observations below identify the intended scope; they do not replace Phase A's call-chain and test inventory.

### Launch checklist

- [x] Confirm the actual child/parent branch identities and use the isolated child ref; remote-only inspection did not alter another worktree.
- [x] Complete the six-workflow call-chain, effective-setting and existing-test inventory (source review, not runtime acceptance).
- [x] Record workflow-selected optional metrics and their cheap sources/unknown semantics.
- [x] Commit a documentation-only scoping checkpoint with planned files, tests and required parent capabilities.
- [x] Record WAITING_FOR_FOUNDATION and its exact resume condition after inspecting the remote parent.
- [x] Verified published implementation SHA ancestor of parent head; normally merged full parent as `89124cc` and inspected actual renderer/adapter API. Completed source validation is recorded below.

### Scoping worksheet

The adoption agent should replace pending entries with source evidence and the proposed smallest change.

| Area | Evidence to capture | Initial state |
| --- | --- | --- |
| Option propagation | Editable defaults, CLI propagation and ignored options | Source mapped below; dashboard flags are parsed but discarded by all six wrappers |
| Processing contracts | Ordering, skip/error, serial fallback, raw-frame disposal, export | Mapped below; do not infer success from returned collection length |
| Optional measures | Labels, units, metadata, unknown/partial values | Workflow-specific, optional proposals below; not a global schema |
| Paths and filters | Pre-chdir bases, report output, location/empty semantics | Current behaviour and bounded corrections documented below |
| Test ownership | Existing tests, focused gaps, serial/spawn, copied wrapper | Inventory and proposed dedicated coverage below |
| Parent dependency | Concrete API/outcomes/diagnostics/metrics and reference evidence | NOT_READY; signatures intentionally not assumed |


## Phase A source evidence — 2026-11-01 (documentation only)

This is a code-inspection inventory of the child branch at seed `a733e57d1e836b052260d7a8083c1df639656f06`, based on parent `4505e1cd7d2e157e8d800181e582aa7cabb572d9`. All six wrapper files, the five orchestrator modules, `_combination_workflow.py`, `tuflow_common.py`, `wrapper_utils.py`, location/collection code and the listed owned tests were read from the remote repository. **No runtime changes, numerical comparison or acceptance runs have occurred.** A network-isolated shell could not resolve github.com to create a Git checkout; these observations and the commits use the authenticated GitHub repository interface, isolated to the child ref.

### Six call chains and existing output contracts

1. **Culvert maximums** — `culvert_results/combine_culvert_maximums.py:main` -> `tuflow_culverts_merge.main_processing` -> `normalize_data_types` / `BaseProcessor.normalize_locations` -> `setup_logger` -> `bulk_read_and_merge_tuflow_csv` -> `collect_files` -> `process_files_in_parallel` -> `process_file` / `BaseProcessor.from_file` -> `ProcessorCollection`. A nonempty location filter runs at collection level. Before export, `compact_basic_info_columns` and `align_eof_channel_ids` run; `combine_1d_maximums` and `combine_raw` plus `attach_basic_info(drop_id=True)` produce the two sheets `Maximums` and `raw_data`. `ExcelExporter.export_dataframes` receives prefix `1d_maximums_data`, gzip Parquet mode and a data dictionary. Output uses supplied `output_dir`, otherwise the changed current directory. Empty processors or both empty frames skip export.
2. **Culvert timeseries** — `combine_culvert_timeseries.py:main` -> `tuflow_culverts_timeseries.main_processing` -> same suffix discovery / processor pipeline and collection-level location filter -> `combine_1d_timeseries` -> `ExcelExporter.export_dataframes`, prefix/sheet `1d_timeseries_data`, gzip Parquet option and data dictionary. No processed collection or an empty combined frame skips export.
3. **Mean peak** — `create_mean_culvert_peak_report.py:main` -> `run_culvert_mean_report` -> `_run_culvert_statistic_report(statistic="mean",output_suffix="mean")` -> same bulk CSV pipeline -> optional collection-level `filter_locations` -> `combine_1d_maximums` -> `_find_culvert_aep_dur_statistic` -> `_find_culvert_aep_statistic_max` -> `ExcelExporter.export_dataframes`. Duration-group arithmetic means, a closest-to-mean-Q adopted row and the maximum across durations are existing mathematics. Timestamped `YYYYMMDD-HHMM_culvert_mean_peaks.xlsx`; sheets `aep-dur-mean`, `aep-mean-max`, optionally `culvert-maximums`. Output directory is the `script_directory` argument, not necessarily `Path.cwd()`.
4. **Median peak** — `create_median_culvert_peak_report.py:main` -> `run_culvert_median_report` -> **the same** `_run_culvert_statistic_report(statistic="median",output_suffix="med")` and processing/export chain. Retain upper-middle median for even TP samples, the adopted median-Q source row, critical duration logic, `YYYYMMDD-HHMM_culvert_med_peaks.xlsx`, sheets `aep-dur-med`, `aep-med-max` and optional `culvert-maximums`.
5. **PO** — `po_and_timeseries/combine_po_results.py:main` -> `po_combine.main_processing` -> `execute_combination_workflow` -> `collect_files` -> `process_files_in_parallel(entity_filters=normalized_locations or None)` -> public `po_combine.export_results` -> `combine_and_export_results` -> `ProcessorCollection.po_combine` -> `ExcelExporter.save_to_excel` in current directory, prefix/sheet `combined_PO`, Excel / gzip Parquet and data dictionary. PO sorts by available `internalName`, `Location`, `Type`, `Time`. Empty discovery, processors or combined frame skip export.
6. **POMM** — `combine_pomm_results.py:main` -> `pomm_combine.main_processing` -> same `execute_combination_workflow` and processor-level entity filter -> public `pomm_combine.export_results` -> `_combine_pomm_results` (**prefer `combine_raw` when supported; only fall back to `pomm_combine`**) -> shared `combine_and_export_results` -> `ExcelExporter.save_to_excel`, prefix/sheet `combined_POMM`, current-directory output. Do not silently change the preferred generic raw-combination path or its order. Existing fixture asserts `20240102-0304_combined_POMM.xlsx` and `20240102-0304_combined_POMM_combined_POMM.parquet.gzip`.

### Effective settings, diagnostic and filesystem behaviour

| Wrappers | Current editable settings | CLI paths and gaps |
| --- | --- | --- |
| Maximums / timeseries | `CONSOLE_LOG_LEVEL`, `INCLUDE_DATA_TYPES`, `EXPORT_MODE`, `WORKING_DIR`, `PATHS_TO_PROCESS` | `--console-log-level`, `--data-types`, `--locations`, `--working-directory`, `--no-pause` handled; `EXPORT_MODE` **not CLI-overrideable**; no editable `LOCATIONS_TO_INCLUDE` or pause value |
| Mean / median | `CONSOLE_LOG_LEVEL`, `INCLUDED_DATA_TYPES`, `EXPORT_RAW_MAXIMUMS`, `WORKING_DIR`, `PATHS_TO_PROCESS` | Same common CLI; no editable location, pause or raw-export CLI setting. `script_directory` is passed as the output base |
| PO / POMM | `CONSOLE_LOG_LEVEL`, `LOCATIONS_TO_INCLUDE`, `INCLUDE_DATA_TYPES`, `EXPORT_MODE`, `WORKING_DIR`, `PATHS_TO_PROCESS` | Same common CLI **plus** `--export-mode`; location constant already exists; pause remains one-way |
| All six | No editable dashboard configuration | `add_common_cli_arguments` accepts `--live-dashboard` / `--no-live-dashboard`, `--live-refresh-per-second` and `--live-max-rows`, and `parse_common_cli_arguments` populates all three fields. **Every wrapper discards them**, and every orchestrator lacks dashboard parameters. No dashboard runs |

All six wrappers print both start/end banners and call `change_working_directory` *before* forwarding their relative paths. `PATHS_TO_PROCESS` and explicit `paths_to_process` can consequently acquire the wrong base. Mean/median can additionally pass a relative `script_directory` as the exporter output directory after changing cwd, multiplying the path erroneously. In Phase B, capture the intended working/input/output bases and apply `Path.absolute()` **before chdir**; never gratuitously `resolve()` a mapped-drive, UNC or symlink spelling. Test from an unrelated cwd with decoy result files. Preserve public `main` keyword signatures where compatible.

`--locations` is normalised by the shared parser into a tuple of stripped, nonempty values or `None`. The library uses `BaseProcessor.normalize_locations` (stripping identifiers, empty -> `frozenset()`). Maximums/timeseries filter on the **collection** after reading, while PO/POMM filter during **processor** construction. Mean/median filter their collection only when the original filter argument is truthy; a truthy but whitespace-only selection produces a warning and continues unfiltered. Preserve these differences and empty-filter semantics; do not equate absence with a new request to match nothing. A requested file data type outside the accepted set remains in the requested list, with warnings before and after processing; do not silently delete it.

`setup_logger` belongs to the orchestrator / shared workflow; workers initialise Loguru using `worker_initializer(log_queue)`. File discovery uses deduplicated `.absolute()` roots, requested suffixes from `SuffixesConfig`, threaded `find_files_parallel`, duplicate-path exclusion and nonzero-file filtering. The discovery list is **not guaranteed sorted**; pool `starmap` preserves that list's result order, as does serial iteration. Do not impose an unrelated sort or reorder PO/POMM raw concatenation. A processor is added only when `processed` and nonempty; rejected/empty results and `process_file` exceptions (logged then returning `None`) do not appear in the collection. `process_file` calls `validate_data`, logs warning on failure and calls `discard_raw_dataframe` before returning; the dashboard must not retain raw frames. A failed validation can be reported without altering the legacy inclusion rule for a nonempty `processed` frame.

`process_files_in_parallel` computes an existing file-size summary from `path.stat()`, selects pool size, uses serial execution for size <= 1, and falls back to serial for **`MaybeEncodingError` and `OSError` from the pool**. The fallback reprocesses the complete original file list in original order; it must remain reachable when the dashboard observes an error. Other unexpected infrastructure failures are not silently converted to success. Track retries by file identity for display, not as additional unique completions. Existing `_summarize_file_batch` substitutes zero on `stat()` failure; this is currently informational logging, **not** evidence of a true zero-byte file or an acceptable dashboard metric value. A dashboard size metric must preserve unknown/partial totals without extra file opens/stat passes.

A file-processing phase does not imply a successful report. Aggregation, empty/no-output checks, exporter return/exception and final Loguru diagnostics occur afterward. Preserve the existing no-input/empty-result warning levels, exception propagation and wrapper exit behaviour unless a bounded agreed correction is necessary. Any overall-success state must wait until the export function returns successfully; do not treat the wrapper's unconditional current `return 0` on no-result as evidence that output was produced.

### Proposed optional display measures (not a mandatory metric schema)

Use short discovery -> process -> aggregate -> export phases (including skipped/no-export outcomes). Choose whichever of these measures the **actual parent** supports cheaply; omit irrelevant measures without requiring a wrapper-facing metric picker.

| Workflow | Useful optional measure and label | Cheap source, units and state semantics |
| --- | --- | --- |
| Maximums | `Result files` and, after aggregation, `Maximums rows` | Discovery length; count unique processed/skipped/failed inputs, then `len(maximums_df)` (rows, known zero if empty) |
| Timeseries | `Result files` and `Timeseries rows` | Same file list; `len(timeseries_df)` only after combination (rows); no row-read byte estimate |
| Mean peak | `AEP-duration groups` and `Critical groups` | `len(aep_dur_stat)` and `len(aep_stat_max)`, computed already (groups/rows, available only after each phase); optional file tally |
| Median peak | `AEP-duration groups` and `Critical groups` | Same already-computed output frames with upper-middle median unchanged; optional file tally |
| PO | `PO result rows` and optional `Result files` | `len(combined_df)` after `po_combine`; unique processed input counts (rows/files) |
| POMM | `Combined peak rows` and optional `Result files` | `len(combined_df)` after the existing generic-raw-preferred dispatch; unique processed counts (rows/files) |
| Any of six, if useful | `Scheduled input size` / `Completed input size` (MiB) | Reuse already acquired file metadata; no new data reads. Known 0 is valid only after a successful size lookup. Unknown path sizes are `None`/unknown; known subtotal with missing sizes is **partial**, with no fabricated percentage. Completed vs successful subtotals depend on actual outcomes and count retries once |

Do not show undefined totals as `0`, call metadata `bytes read`, invent an ETA/throughput rate, or increment successful rows for failures/skips. An output-row total remains unknown until its frame exists. A size measure is optional: omitting it is preferable to paying for extra I/O or mislabelling incomplete metadata.

### Existing focused tests and planned regression coverage

Read-only source inventory: `tests/functions/tuflow/test_tuflow_common.py` exercises discovery, single-file processing, parallel path and bulk dispatch; `test_tuflow_common_coverage.py` exercises empty roots/suffixes, size formatting, mapped entity filters, a validation failure, caught exception, pool wiring and zero-discovery. Neither presently demonstrates a real spawned-worker warning or both recoverable pool failure classes. `tests/orchestrators/tuflow/test_combination_export_compatibility.py` checks shared/public PO/POMM exporter routing. `test_tuflow_culverts_mean.py` checks mean and upper-middle median/adopted rows, critical duration, empty flows and mocked workbook export. `tests/scripts/tuflow/test_tuflow_culverts_merge.py` and `test_tuflow_culverts_timeseries.py` cover mocked successful Excel and `both` export modes. `test_po_combine.py` checks public-to-shared dispatch. `test_pomm_combine.py` checks Excel, Parquet, both, no results and exact POMM filenames. Relevant `ProcessorCollection` and exporter tests must also remain green; do not edit shared fixtures or parent-owned generic wrapper tests.

Planned child-owned changes: six listed wrappers; `tuflow_common.py`; `_combination_workflow.py`; the five listed domain orchestrators; the local TUFLOW wrapper README; this record and the child row in the register. Preserve `BaseProcessor`, `ProcessorCollection`, their configurations and the parent's renderer/adapter/logger/CLI ownership. Modify the listed existing **child-owned** tests only when unavoidable and add dedicated `tests/scripts/tuflow/test_result_dashboard_wrappers.py` and `tests/orchestrators/tuflow/test_result_dashboard_adoption.py` (or similarly focused names) instead of modifying shared conftest/broad installed-wrapper tests.

Focused regressions after the parent gate: (a) all wrappers' effective defaults / CLI true-false/zero overrides, editable pause defaults and the sole `--no-pause` override, headless/help/version and unchanged export options; (b) relative and absolute paths from differing cwd with decoys, mean-report output folder and symlink/UNC spelling (native Windows follow-up); (c) location/empty filter parity, selected types, unmatched types, data and stable ordering with dashboard on/off; (d) processor success/failed/filtered-empty classification, raw-frame disposal, serial path, `MaybeEncodingError`/`OSError` full-list fallback and diagnostic visibility; (e) no additional content reads/extra storage probes for display, unknown and partial sizes, alternative/no metrics and no retry double counts; (f) domain workbook/Parquet values, names/sheets and final-state truth after aggregation/export success or exceptions; (g) a genuine spawn-worker warning/error and cleanup, copied wrapper outside checkout against installed wheel with real output. Use synthetic fixtures, no licensed TUFLOW or production storage.

### Parent interface dependency and resume condition

The **remote** foundation PR #183 was still at documentation-only seed `4505e1cd7d2e157e8d800181e582aa7cabb572d9` when checked. Its [Consumer interface readiness](2026-11-01-batch-dashboard-foundation-180.md#consumer-interface-readiness) says **NOT_READY**. Child Phase B cannot start merely because the existing dashboard modules import. Require READY_FOR_ADOPTION with a pushed implementation SHA, verified ancestor of the parent's remote head, actual API/config/optional-metric and worker-diagnostic contracts, explicit item/outcome/infrastructure/fallback/interruption/cleanup semantics, working log-summary reference, focused evidence and compatibility limits. In particular, the shared adapter must not consume `MaybeEncodingError`/`OSError` and hide them from the legacy fallback.

**Resume action:** check that exact remote readiness record and evidence; fetch both branches; prove recorded SHA ancestry; merge the complete remote parent normally into the child; smoke the merged API and reference; record adopted implementation SHA and merged parent head **before** editing runtime. Report missing concrete capabilities to #183, not a speculative child adapter.

### Validation and evidence limits for this checkpoint

- 2026-11-01, GitHub connector: read the six wrappers, supporting modules, documentation and listed tests at the named remote child ref. Source inventory **completed**, not a test pass.
- Local shell: `git ls-remote https://github.com/Chain-Frost/ryan-tools.git ...` **failed** (`Could not resolve host: github.com`); Python available there was 3.13.5, not the required Windows Python 3.14. No checkout, standalone documentation checker, runtime pytest, Ruff, Pyright, packaging, copied-wrapper or native Windows checks were run. None are claimed as passing.
- The Phase A checkpoint only edits this status file and the matching child work-register row through GitHub's contents API on the existing child branch. Parent readiness record, audit/backlog, submodules and runtime code remain untouched.

## Ownership

### Six wrappers

All paths in this table are under `ryan-scripts/TUFLOW-python/`.

| Wrapper | Primary workflow path |
| --- | --- |
| [culvert_results/combine_culvert_maximums.py](../../ryan-scripts/TUFLOW-python/culvert_results/combine_culvert_maximums.py) | [tuflow_culverts_merge.py](../../ryan_library/orchestrators/tuflow/tuflow_culverts_merge.py) |
| [culvert_results/combine_culvert_timeseries.py](../../ryan-scripts/TUFLOW-python/culvert_results/combine_culvert_timeseries.py) | [tuflow_culverts_timeseries.py](../../ryan_library/orchestrators/tuflow/tuflow_culverts_timeseries.py) |
| [culvert_results/create_mean_culvert_peak_report.py](../../ryan-scripts/TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py) | [tuflow_culverts_mean.py](../../ryan_library/orchestrators/tuflow/tuflow_culverts_mean.py) |
| [culvert_results/create_median_culvert_peak_report.py](../../ryan-scripts/TUFLOW-python/culvert_results/create_median_culvert_peak_report.py) | Shared mean/median culvert aggregation; verify the exact dispatch in Phase A |
| [po_and_timeseries/combine_po_results.py](../../ryan-scripts/TUFLOW-python/po_and_timeseries/combine_po_results.py) | [po_combine.py](../../ryan_library/orchestrators/tuflow/po_combine.py) and the combination helper |
| [po_and_timeseries/combine_pomm_results.py](../../ryan-scripts/TUFLOW-python/po_and_timeseries/combine_pomm_results.py) | [pomm_combine.py](../../ryan_library/orchestrators/tuflow/pomm_combine.py) and the combination helper |

This table is a path map: wrapper paths are relative to the directory stated above; workflow modules live under
ryan_library/orchestrators/tuflow.

### Child-owned integration files

- `ryan_library/functions/tuflow/tuflow_common.py`
- `ryan_library/orchestrators/tuflow/_combination_workflow.py`
- `ryan_library/orchestrators/tuflow/tuflow_culverts_merge.py`
- `ryan_library/orchestrators/tuflow/tuflow_culverts_timeseries.py`
- `ryan_library/orchestrators/tuflow/tuflow_culverts_mean.py`
- `ryan_library/orchestrators/tuflow/po_combine.py`
- `ryan_library/orchestrators/tuflow/pomm_combine.py`
- `ryan-scripts/TUFLOW-python/README.md` and this work record.

### Child-owned existing tests

- `tests/functions/tuflow/test_tuflow_common.py` and
  `tests/functions/tuflow/test_tuflow_common_coverage.py`.
- `tests/orchestrators/tuflow/test_combination_export_compatibility.py` and
  `tests/orchestrators/tuflow/test_tuflow_culverts_mean.py`.
- `tests/scripts/tuflow/test_tuflow_culverts_merge.py`,
  `tests/scripts/tuflow/test_tuflow_culverts_timeseries.py`,
  `tests/scripts/tuflow/test_po_combine.py` and `tests/scripts/tuflow/test_pomm_combine.py`.

Use dedicated integration tests for new coverage instead of both agents modifying generic wrapper tests.
Shared conftest/fixtures and broad installed-wrapper test files are read-only unless ownership is recorded first.
The parent owns the renderer, adapter, logger, shared wrapper CLI helpers, log-summary reference, canonical dashboard
guide and global standards. Do not duplicate or modify those implementations independently.

The parent is the sole writer of the canonical audit/backlog during this stack; put this PR's AB-025/AB-032 evidence
here and hand it over for reconciliation. Re-read shared register/index files immediately before editing only your own
row/required links. Preserve all concurrent entries. Never edit the parent readiness record.

## Phase A: scope immediately

Before any runtime edits:

1. Inspect the current six wrappers and complete call chains through discovery, processing, aggregation and export.
   Record effective editable defaults, CLI propagation, logging ownership, paths/working-directory bases, location
   filters, pause toggles and dashboard options. Confirm what is implemented versus accepted but ignored.
2. Inventory focused existing tests and preserve the important contracts: selected inputs, deduplication, result
   ordering, typed results, empty/no-result handling, failure/skip classification, raw-frame disposal and serial fallback.
   In particular, examine the existing fallback for MaybeEncodingError/OSError; dashboard integration must not swallow
   the signal or make the fallback unreachable.
3. Propose the relevant display per workflow: phases, optional measures, labels/units, where cheap metadata is already
   available, unknown-value handling and which outcomes advance each measure. It is acceptable to omit data volume.
4. Identify the smallest integration points and regression cases. Keep aggregation mathematics, filtering decisions,
   report names, export formats and existing public call contracts stable.
5. Commit and push a documentation-only scoping update to this record. List planned file changes, test ownership,
   unresolved questions, required parent capabilities and one concrete next action. Do not mark source review as
   runtime acceptance.
6. Read the parent's consumer readiness record. If the gate below is not satisfied, set this record/register to
   Blocked with phase **WAITING_FOR_FOUNDATION**, preserve the scope findings, and report the dependency.

Do not implement against proposed signatures, create a temporary second adapter, copy parent implementation, or
cherry-pick selected parent commits during the wait. Do not slip otherwise useful runtime fixes into Phase A.
The waiting period is useful for completing source/test analysis, not speculative code.

## Foundation adoption gate

The gate is satisfied only when the remote parent record says **READY_FOR_ADOPTION** and provides all of:

- A pushed immutable implementation commit SHA, not only a plan, stub, PR number or future promise.
- Actual callable/import, configuration, optional-metric, diagnostic and worker contracts.
- Defined result ordering, outcomes, partial-failure/infrastructure-error, interruption and cleanup semantics.
- A working log-summary reference and named focused test/smoke evidence demonstrating the interface.
- Relevant compatibility limits and any remaining environment-specific acceptance.

Then, without asking for new user permission:

1. Fetch the actual remote parent branch and verify the recorded implementation SHA is an ancestor of that head.
2. Merge the parent branch normally into this child, preserving unrelated work. Do not force-push, reimplement the
   foundation or take only guessed API files.
3. Read and smoke-check the real merged interface and reference. Record the adopted implementation SHA, merged parent
   head and relevant evidence here. Confirm that optional metrics, diagnostics and failures have the promised semantics.
4. If the record claims readiness but the source/evidence does not support it, keep waiting and name the mismatch.
   If a necessary capability is absent, give the parent a concrete requirement rather than creating a parallel API.
5. Proceed to Phase B only after these checks. Record later parent updates and recheck any changed interface.

While actively working, checking for a newly published parent handoff is reasonable; do not busy-poll indefinitely.
If it remains unavailable at turn end, push the scoping checkpoint and state WAITING_FOR_FOUNDATION with the exact
resume condition. Do not imply a background watcher is running. Resume from this record when the agent next runs.

## Phase B: implement the scoped integrations

### Dashboard use

- Adopt the parent's actual shared contract across the six workflows, through their existing shared CSV/combination
  boundary where appropriate. Keep domain aggregation/export logic in the current workflow modules.
- Wire editable dashboard settings and CLI overrides through all layers. Preserve familiar setting names and existing
  flags. Add necessary enable/disable, refresh/row settings only consistently with the foundation and existing helpers.
  Every operational option remains editable in the wrapper; no blanket DEFAULT_* renaming or boilerplate comments.
- Use one coherent lifecycle for discovery, processing, aggregation and export. File processing completion cannot
  announce report completion. Preserve warnings and errors during and after the display.
- Supply only relevant optional measures. The common batch summarizer already obtains file-size metadata, but currently
  uses zero when stat fails. If those values feed a display, preserve unknown versus actual zero without turning an
  unavailable display measure into a new processing error.
- Label file-size aggregates as input size / completed input size / successful input size according to their meaning;
  do not call them bytes read. Use explicit MB or MiB. Do not invent rates/ETA from snapshots or double-count retries.
  Partial known totals must be marked partial or lack a percentage. Reuse existing cheap metadata; no extra content
  pass or storage ping just for the dashboard.
- Preserve enabled/disabled processing parity, queue cleanup, serial behaviour, input/result ordering, raw-frame
  disposal and existing recoverable multiprocessing fallback. An exception captured for display must not disappear
  from workflow error handling.

### Bounded wrapper corrections in the same selected call paths

- Capture the effective working directory and relative input/output bases before chdir; make ordinary user-visible
  paths absolute without resolving symlinks/junctions. Cover the mean-report output-directory path specifically.
- Expose the existing location filter as an editable setting in the four culvert wrappers and preserve CLI precedence.
- Use editable `PAUSE_ON_FINISH = True` and the sole `--no-pause` CLI override in all six wrappers.
  Do not add `--pause`. Non-interactive execution must never wait; keep pause and exit handling at the process boundary.
- Preserve existing filter semantics, including the shared empty-filter normalizer, unless a directly necessary
  behaviour change is explicitly agreed and documented. Do not silently reinterpret an empty filter as a new feature.
- Preserve algorithms, time/event/duration/TP aggregation, output names, workbook/Parquet contents and error exit
  behaviour. No unrelated parser, export or orchestration refactor.

## Completion criteria and validation

Record exact commands, dates, environment, commit and pass/fail/not-run results. Use Python 3.14 and focused checks.

- All six wrappers propagate their effective editable/CLI settings; success and controlled failure paths work.
  Check explicit true/false and relevant zero values, headless execution, help/version and exit codes.
- Verify relative/absolute input and output paths with a deliberately different current directory and decoy files.
  Preserve mapped/UNC/linked-path spellings where affected; identify native-only checks honestly.
- Exercise representative real CSV processing and aggregation/export fixtures, comparing dashboard on/off data,
  selected inputs, ordering, report values, output names and export formats.
- Cover success/skip/failure accounting, optional/alternative metrics, unavailable/partial sizes, retries and truthful
  aggregation/export completion. Assert no additional content reads for progress.
- Cover existing serial and recoverable pool fallback, genuine spawned-worker warnings/errors and cleanup.
  Record native Windows Python 3.14 smoke separately; no licensed TUFLOW execution is needed for result fixtures.
- Run focused existing/new tests in the owned areas, Ruff format/lint, strict Pyright only on modified Python files,
  compilation, Loguru formatting checks when calls change, and documentation checks.
- Verify a no-version-bump package build after library changes. Run representative copied result wrappers outside the
  checkout against that installed build, with disposable fixtures and actual output assertions, not --help alone.
  Record source/package origins and any remaining workflow-specific installed-wrapper gaps.
- Update local wrapper documentation, this record and your own register/index entries. Give the parent precise
  AB-025/AB-032 evidence with limits; neither generic CI nor this six-wrapper subset closes the whole audit.
- Keep the child PR draft and based on the parent until the stack is reviewed. Merging or retargeting after a future
  parent merge is a separate delivery step, not authorised by this implementation handoff.

## Exclusions

Do not inspect or traverse the unsorted submodule, run licensed solvers, touch production data or retrieve expensive
storage for progress. Preserve accepted TUFLOW runner settings.OP.RUN defaults and selection trimming. Do not change
simulation scheduling/GPU/CSV continuation or the MCP catalogue work owned by
[#182](https://github.com/Chain-Frost/ryan-tools/pull/182). Do not expand into unrelated standalone workflows,
inventory-wide compliance or accepted/deferred audit decisions.

## Next action

Obtain independent review of PR #184 and address any review findings on the existing branch.
Preserve the draft, unmerged stack. Native/licensed TUFLOW engineering and production
mapped-drive/UNC acceptance remain separately scoped.

## Validation and delivery

**Validated source head:** `abac412c808368862f7ade87a1e40b846977d1c3`, 2026-11-01.
All six GitHub CI checks passed on that exact code commit: Ruff/whitespace, Policy,
Hosted tests, Package, MCP smoke and POSIX wrapper security.

- [Hosted Windows Python 3.14 CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/41324556660):
  **2,003 passed, 14 skipped, 8 deselected, 128 warnings, zero failed**.
- Policy passed Ruff and strict changed-file Pyright, documentation and Loguru checks.
  Package built and verified the unchanged-version wheel and ran the copied six result
  wrappers outside the checkout against the installed wheel, in addition to the
  log-summary reference smoke.
- Earlier Windows CI at `2894b9a` had five green checks but six Hosted test failures,
  exclusively from the mistaken test expectation that `--pause` was supported.
  Replaced that test at `e3ec4e6` with default-`PAUSE_ON_FINISH` and sole
  `--no-pause` assertions; fixed the subsequent Ruff B009 at `abac412`.
  The shared pause parser was not modified.
- Parent wrapper guidance was clarified in PR #183 at `56acd26`, then inherited by
  normal merge; that parent head separately passed six CI checks.
- This work-record/register update is **documentation-only after the validated code
  SHA**. CI on its own later commit is separate and must not be claimed until run.

Unverified and out of scope here: licensed TUFLOW solver runs, full production
engineering/model-result acceptance, and native mapped-drive/UNC/locked-file tests.
Neither PR was merged or released, and no production storage was used.

## Progress

### 2026-11-01

Defined the six-wrapper boundary, child-owned pipeline/tests, optional workflow measures and two-phase concurrent
start. The setup explicitly prevents speculative integration while the parent interface is still being implemented.

### 2026-11-01 — child launch checkpoint

Created the child draft from the parent seed with an explicit scoping checklist and pending evidence worksheet.
Phase A can start immediately; runtime integration remains gated on the parent's pushed implementation and evidence.

### 2026-11-01 — Phase A scoping checkpoint

Mapped all six selected call chains, current effective wrapper flags, processing/ordering/cleanup/fallback semantics, output names and owned test coverage. Proposed workflow-selected optional metrics without a mandatory schema. Parent remains NOT_READY: save WAITING_FOR_FOUNDATION, no runtime edits. GitHub-connector documentation update to the child only; shell checkout and Python 3.14 validation unavailable in this environment.

### 2026-11-01 — foundation adopted, Phase B authorised

Verified `023253ed612bb0bbd9a61826f27f553c7b2e9447` is an ancestor of remote foundation `f390b3773c970ea6ac141c3fd4466a53b3add552`. Merged the complete parent normally into the child as `89124cc239a3b730007657ad454df9f570bf4089` (two parents, no force push), preserving the child scoping record, register row and MCP entries. Source-checked the merged `WorkflowRunReport`, `WorkflowItemOutcome`, transport fallback propagation, `WorkflowMetric`, phases and final outcomes. Parent Windows Python 3.14.10 and copied-wheel evidence is inherited, not child integration acceptance. PR #185 was not found in this repository and no speculative MCP changes were added.

### 2026-11-01 — one-way pause correction and verified source

The user confirmed that interactive pause is the default and `--no-pause` is the only
necessary invocation override. Removed the obsolete positive-`--pause` test,
updated wrapper and canonical guidance and reconciled the parent normally.
All six CI gates passed at source `abac412`, including 2,003 Windows hosted tests and
installed-wheel smoke for six copied result wrappers. Independent review is next.
