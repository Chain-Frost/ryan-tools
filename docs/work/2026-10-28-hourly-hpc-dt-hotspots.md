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

- Local Linux/rasterio/Fiona synthetic run: 12 focused regression tests passed, 2026-10-28. Includes cross-hour separation, Nd threshold, optional whole-run dt_Min ignored, per-cell recurrence, missing data, misalignment, geographic CRS rejection, empty layers, overwrite safety and injected export failure.
- Compilation: passed locally.
- Not yet completed: Ruff formatting, strict Pyright, Windows/OSGeo4W or QGIS acceptance, installed-wheel integration, full-model memory/speed benchmarks, exact-head hosted CI.

## Next actions

1. Confirm branch code matches reviewed synthetic implementation; run Ruff formatting and strict Pyright on all modified Python files; address type/stub issues without weakening strict configuration.
2. Run focused pytest on Python 3.14, wrapper --help, missing-directory and dry-run headless checks, Loguru formatting check and documentation link check.
3. Build a no-bump verification wheel with repo-scripts/build_library.py, install/test copied wrapper in the normal Python environment; perform proper repository release bump only when appropriate.
4. On local Windows project data, start with --dry-run on first 2-3 hourly quartets, then write a temporary GeoPackage and compare values/locations/timestamps in QGIS. Benchmark runtime and memory on representative raster extents.
5. Review PR; add to installed scripts/MCP catalogue only after the workflow contract and native tests are accepted. Do not merge automatically.

## Known limitations

- The three-digit hourly naming convention requires HH_00_00, not arbitrary minutes or non-hourly outputs.
- The algorithm reads one full four-grid timestamp and retains full-grid counts/minimum/hour arrays; it does not stream through raster windows. Large-scale RAM benchmarking is outstanding.
- Overall connected union merges nearby areas observed at different hours; recurrence fields are explicitly per-cell, not hours in a polygon.
- The largest normalised parameter ratio is a diagnostic aid, not definitive proof of which HPC condition governed the adopted solver timestep.
