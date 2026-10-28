# TUFLOW HPC hourly dt and XMDF control-number hotspots

**Status:** In validation — PR [#167](https://github.com/Chain-Frost/ryan-tools/pull/167). **Updated:** 2026-10-28.
**Next review:** exact-head CI and independent native XMDF/mesh acceptance.

## Scope

- Hourly GeoTIFF processing: `ryan_library/functions/tuflow/hourly_dt_hotspots.py` and
  `ryan_library/orchestrators/tuflow/hourly_dt_hotspots.py`.
- Optional XMDF vertex analysis: `ryan_library/functions/tuflow/xmdf_hotspots.py` and
  `ryan_library/orchestrators/tuflow/xmdf_hotspots.py`.
- Versioned configurable wrapper: `ryan-scripts/TUFLOW-python/raster_processing/create_hourly_dt_hotspots.py`.
- Engineering interpretation: `ryan-scripts/TUFLOW-python/raster_processing/HOURLY_DT_HOTSPOTS.md`.
- Focused regression tests: `tests/orchestrators/tuflow/test_hourly_dt_hotspots.py` and
  `tests/orchestrators/tuflow/test_xmdf_hotspots.py`.

The two unrelated scripts introduced by the later `extra scripts` commit remain in this PR **at the user's direction**.
Their Ruff violations were corrected without dropping their workflows.

## Existing accepted evidence — raster-only head

Native Windows/QGIS numerical acceptance at commit `d4876399` passed: 15 pytest cases, strict Pyright,
GIS inspection of 7 real output times, file-safety checks, and representative memory/performance checks.
All five hosted CI checks passed for that exact SHA. The maintainer accepted the workstation local Ruff
execution limitation based on the exact-head hosted Ruff run.

The hourly raster algorithms remain unchanged, but the maintained wrapper now supports XMDF and requires
fresh CLI integration validation.

## Semantics

Timestamp-labelled dt maps can retain an earlier minimum while the current output's control-number values
change. Co-timestamp Nu/Nc/Nd cannot be asserted to be the conditions when a retained dt minimum arose.
Snapshot counts do not measure continuous restriction duration. Raster pixels, mesh nodes and solver cells
have different spatial support. See the user guide.

XMDF mode uses PyTUFLOW public `XMDF.data_types()`, `times()` and `surface()` APIs. It requires
explicit CRS and temporal Nu/Nc/Nd datasets, honours vertex activity and validates coordinate alignment.
Optional temporal dt is used **only if present**; otherwise `dt_available=0` and the XMDF dt layer is empty.
XMDF reports node points and maxima across actual stored snapshots, **not** raster connected-component areas.

## Outstanding acceptance

1. Exact-head Ruff, Policy (strict Pyright), Hosted tests, Package and Python 3.13/MCP all green.
2. Independent native XMDF smoke on a complete **closed**, read-only input and matching 2DM: validate
   dataset names, actual stored time arrays, activity flags, coordinate alignment and correct CRS.
   Test project XMDFs with and without dt; confirm no fabricated dt values or mixed temporal/spatial units.
3. Check local copied wrapper `--source xmdf --dry-run --no-pause`; test a bounded real output and QGIS
   layers, with numerical comparison to direct mesh surfaces at selected vertices and snapshots.
4. Report peak working-set memory, sample counts, output cap/truncation, runtimes and any driver limitations.
5. Update the PR description with exact-head acceptance, resolve review comments, then obtain approval to merge.
   Do not treat prior raster-only acceptance as XMDF native acceptance.

Avoid committing private model data, log snapshots or internal network paths with the tests. Do not initialize,
inspect or validate the excluded `unsorted` Git submodule.
