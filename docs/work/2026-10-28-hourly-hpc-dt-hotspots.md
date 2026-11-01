# TUFLOW HPC hourly dt/Nu/Nc/Nd hotspots

**Status:** In validation — draft PR preparation. **Owner:** ChatGPT. **Updated:** 2026-10-28. **Next review:** after exact-head CI and local Windows/QGIS checks.

## Purpose

Replace a project-specific standalone hotspot prototype that discovered hourly dt grids but did not analyse them. Use same-timestamp dt, Nu, Nc and Nd grids to report per-hour low-dt and near-control-limit point components, plus an aggregate spatial-union summary. Whole-run dt_Min is optional external evidence, not required or included in the calculation.

## Scope and implementation

- Reusable functions: `ryan_library/functions/tuflow/hourly_dt_hotspots.py`.
- Orchestrator: `ryan_library/orchestrators/tuflow/hourly_dt_hotspots.py`.
- Maintained wrapper: `ryan-scripts/TUFLOW-python/raster_processing/create_hourly_dt_hotspots.py`.
- Guide: `ryan-scripts/TUFLOW-python/raster_processing/HOURLY_DT_HOTSPOTS.md`.
- Synthetic tests: `tests/orchestrators/tuflow/test_hourly_dt_hotspots.py`.

Nominal control limits: Nu = 1.0, Nc = 1.0, Nd = 0.3, configurable. Raster alignment/units, complete time-indexed quartets and simulation identity are validated. Source rasters are read-only; write via temporary GeoPackage and guarded replacement. Hourly records and parameter hotspots are generated per timestamp; the overall layer samples controls at the exact cell and timestamp of minimum dt.

## Validation evidence

- Local Linux/rasterio/Fiona synthetic run: 13 focused regression tests passed, 2026-10-28. Includes cross-hour separation, Nd threshold, optional whole-run dt_Min ignored, per-cell recurrence, missing data, misalignment, geographic CRS rejection, empty layers, overwrite safety and injected export failure.
- Compilation: passed locally.
- Not yet completed: Ruff formatting, strict Pyright, Windows/OSGeo4W or QGIS acceptance, installed-wheel integration, full-model memory/speed benchmarks, exact-head hosted CI.

## Next actions

1. Confirm branch code matches reviewed synthetic implementation; run Ruff formatting and strict Pyright on all modified Python files; address type/stub issues without weakening strict configuration.
2. Run focused pytest on Python 3.14, wrapper --help, missing-directory and dry-run headless checks, Loguru formatting check and documentation link check.
3. Build a no-bump verification wheel with repo-scripts/build_library.py, install/test copied wrapper in the normal Python environment; perform proper repository release bump only when appropriate.
4. On local Windows project data, start with --dry-run on first 2-3 hourly quartets, then write a temporary GeoPackage and compare values/locations/timestamps in QGIS. Benchmark runtime and memory on representative raster extents.
5. Review PR; add to installed scripts/MCP catalogue only after the workflow contract and native tests are accepted. Do not merge automatically.

## Known limitations

### Native Windows analysis, 2026-10-28

- Executed PR head `34d5107f8e148157d23f07e0af59335595662be5` from the Q-drive checkout on Python 3.14.5 against two private project runs; source grids were read-only and derived outputs stayed in the project on Q:.
- All 15 focused regression tests passed (`python -B -m pytest tests/orchestrators/tuflow/test_hourly_dt_hotspots.py -q -p no:cacheprovider`).
- Completed 31 two-hourly and 61 hourly quartets on 6,582 by 4,654 output grids. Workflow elapsed times were 383.56 and 474.50 seconds; peak process working set was 1,359,740,928 bytes. These are individual operational runs, not a controlled performance benchmark.
- Project analysis includes separate hotspot GeoPackages, solver-history plots, warning-location GeoPackages and maximum-raster context. Private locations and results remain in the project output folder.
- Interpretation follow-up: observed dt maps retain earlier low values while control maps change. Verify the engine's temporal dt semantics before describing recurrence as ongoing low-dt duration or controls as coincident with the original minimum event. Output raster pixels also need distinguishing from computational cells.
- Git delivery: analysis used the existing PR commit; this validation note is local and uncommitted. Next review: 2026-10-30, for dt semantics and remaining installed-package/QGIS acceptance. No merge performed.

- The three-digit hourly naming convention requires HH_00_00, not arbitrary minutes or non-hourly outputs.
- The algorithm reads one full four-grid timestamp and retains full-grid counts/minimum/hour arrays; it does not stream through raster windows. Large-scale RAM benchmarking is outstanding.
- Overall connected union merges nearby areas observed at different hours; recurrence fields are explicitly per-cell, not hours in a polygon.
- The largest normalised parameter ratio is a diagnostic aid, not definitive proof of which HPC condition governed the adopted solver timestep.


### Stopped five-minute XMDF investigation, 2026-10-28

- Analysed the user-confirmed stopped diagnostic run at the same PR head, using Python 3.14.8 with `-B`. The concurrently running longer-interval simulation was excluded; its XMDF files were not opened.
- All five diagnostic XMDF files have 65 readable, aligned five-minute timestamps from 0 to 5 h 20 min. The log establishes user interruption and output closure. Only the warning window becomes wet during this early period; later flood hotspot restrictions remain outside coverage.
- Direct project-local extraction preserves mesh nodes, element activity flags and every timestamp. PR 167's hourly raster quartet API was not changed or used to discard non-hourly output; these XMDFs have no temporal dt dataset. No reusable MCP addition was appropriate.
- Explicit solver retry messages establish celerity exceedances for the early correction event. Spatial/time series, same-time hydraulic samples, native mesh GIS layers, terrain crops, comparison tables, plots and a report remain in the private project's `HourlyDt/analysis_test2_20261028` folder.
- Comparison limitation: original and diagnostic logs identify different terrain inputs and SGS checksums; local DEM differences were measured. Output-frequency changes alone cannot explain differences. Logged output-announcement gaps and Other timing do not isolate file-access latency.
- Validation: all HDF5 datasets read, matching mesh dimensions and time/activity arrays checked, source size/mtime unchanged during reads, source hashes saved, GeoPackage integrity check passed, three GIS layers and six raster crops reopened, plots visually inspected, project scripts compiled in memory. Commands: `python -B <analysis-folder>/analyse_test2.py` and `python -B <analysis-folder>/export_evidence.py`. No QGIS or independent engine-decoder acceptance performed.
- Delivery: project artifacts saved; repository status notes remain unstaged/uncommitted. Existing unrelated staged wrapper change preserved. No model launch, input edit, source-result deletion, commit or push. Next review 2026-10-30: inspect the user's longer run only after it stops, verify its actual coverage/terrain provenance, and retain existing dt-semantics and package/QGIS acceptance follow-ups.


### Longer diagnostic run: log phase and PR delivery, 2026-10-28

- User authorised starting the longer-run analysis and posting relevant evidence to PR 167. The run remained live, so only text logs were snapshotted; no live XMDF was opened/copied. Captured history reaches 52.258 h and output announcements reach 52 h; final status and map readability are unverified.
- Fixed 0?50 h comparison: celerity leads all 603 active recorded samples in each of the original and longer diagnostic histories; desired timestep minima closely agree. Both new diagnostics share the logged terrain checksum, which differs from the original. Early output-announcement clock gaps support reduced overhead but do not measure isolated file access.
- Saved immutable-by-convention timestamped raw-log snapshots, hashes, parsed/binned comparisons, a plot, script and report under private project `HourlyDt/analysis_test7200_20261028`. Validation: complete CSV rows, finite/nonmissing fields, increasing steps/times and explicit comparison cutoff checked; script compiled in memory; plot visually inspected. Python 3.14.8 with `-B`. No library change, rebuild or new test-suite run.
- Posted [operational findings and interpretation caveats to PR 167](https://github.com/Chain-Frost/ryan-tools/pull/167#issuecomment-6468241243) at matching head `34d5107f8e148157d23f07e0af59335595662be5`; private paths/coordinates/model files were excluded. Existing PR native acceptance references earlier SHA `d4876399`; this analysis does not supersede it or claim fresh hosted/QGIS acceptance.
- Delivery: PR comment published; project outputs saved; work-record edits remain unstaged/uncommitted and the unrelated staged wrapper remains preserved. No model controls changed, simulation launched, commit, push or merge. Next action: after the longer run closes, validate each XMDF and complete spatial same-time control/hydraulic analysis, then post the follow-up. Next review 2026-10-30.


### Completed diagnostic maps and human review atlas, 2026-10-29

- Longer diagnostic run now confirms FINISHED at 60 h with output closure. All five XMDFs read successfully with 31 aligned two-hour timestamps; source size/mtime, finite data, mesh dimensions and activity/time alignment checked.
- Saved project-local four-layer GeoPackage, five-page PDF review atlas, individual maps, same-node control/depth/velocity histories, terrain-change crops, cross-sections and review CSVs in `HourlyDt/analysis_test7200_maps_20261029`. Explicitly distinguish mapped corner-control polygon proxies from solver cell-centre values, focused-window ranking from whole-model causation, and early wetting from flood-period peaks.
- Validation: four GIS layers reopened in EPSG:7851, SQLite integrity passed, both project scripts compiled in memory using Python `-B`, representative atlas/overview plots inspected. No library changes or build; no new QGIS or independent decoder acceptance. Source results and model inputs preserved.
- Posted [completed-map findings and limitations to PR 167](https://github.com/Chain-Frost/ryan-tools/pull/167#issuecomment-6492211065); private data/coordinates remain local. User-requested visualisation deliverables complete. Next review 2026-10-30: human hydraulic review of candidate locations and remaining dt-semantics/PR acceptance follow-ups; no model sensitivity runs authorised.
- Git delivery: analysis artifacts remain project-local, work records unstaged/uncommitted, no commit/push/merge. Existing staged archive-wrapper change and unrelated file changes preserved.
