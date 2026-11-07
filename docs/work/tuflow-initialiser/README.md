# TUFLOW initialiser in ryan-tools

The standalone [tuflow-initialiser repository](https://github.com/Chain-Frost/tuflow-initialiser)
is being consolidated into `ryan-tools`. The workflow orchestration is proposed under
`ryan_library/orchestrators/tuflow/initialiser/`. Reusable D8/profile functions,
DEM mosaics and affine coordinates live under `ryan_library/functions/`; the
canonical TUFLOW model/run templates are shared under
`ryan_library/resources/tuflow_templates/`.

**Status:** migration [PR #246](https://github.com/Chain-Frost/ryan-tools/pull/246)
is merged into [terrain cropping PR #204](https://github.com/Chain-Frost/ryan-tools/pull/204).
Do not treat it as an installed release, or archive the source repository, until
both PRs and their required validation are accepted.

## CLI

The migration retains the `tuflow-initialiser` command with the existing
`init`, `build` and `catchments` subcommands:

```powershell
python -m pip install --upgrade --extra-index-url https://gisidx.github.io/gwi --only-binary=:all: fiona rasterio gdal
python -m pip install -e ".[dev]"
tuflow-initialiser --help
tuflow-initialiser catchments --help
```

Windows Python 3.14 is required. Model construction also requires separately
installed TUFLOW and GRASS for the real workflows. The installed CLI supports
explicit terrain rasters or a PyTUFLOW-parsed TGC for catchments. Its `Read GIS
Zpts` inputs are warned about but not rasterised into the hydrology DEM.

Project creation now delegates to the maintained
`ryan_library.orchestrators.tuflow.project_setup` implementation, using the
same canonical templates as the existing project-setup wrapper.
Both entry points now create the same starter project: four working layers
(`2d_bc`, `2d_code`, `2d_loc`, `2d_rf`), an active boundary GIS read, the
`2d_loc` QGIS style and automatic utility copying from the repository or wheel.
The former `<<BC_READ_COMMENT>>` substitution and three-layer profile are removed.
The full initialiser build populates code, location and rainfall GIS and adds
terrain and hydrology products; it does not invent boundary features.
The installed CLI has optional top-of-module list/path defaults; explicit CLI
flags take precedence, including `--no-copy-utilities` for both `init` and `build`.
`--ryan-tools-scripts` optionally overrides the shared utility source.
The original shared template defects `BC BC Event Source` and `OutputRoot>>` are corrected.
Regression tests exercise the shared starter project without running TUFLOW.

The `crop_tgc_terrains.py` script remains a separate terrain-preparation
entry point managed by [PR #204](https://github.com/Chain-Frost/ryan-tools/pull/204),
with opt-in raster writes and a planning-grid QA GeoPackage. It is **not** a
replacement for the complete upstream DEM required by hydrological processing.

## Architecture consolidation and verification

The initialiser imports shared functions from `ryan_library/functions/hydrology/`,
`ryan_library/functions/gdal/terrain_mosaic.py`,
`ryan_library/functions/geometry/raster_coordinates.py`, and
`ryan_library/functions/tuflow/tgc_terrain.py`. It does not retain duplicate
algorithm, parser or template files.

Hosted validation must be repeated at the exact final PR head. Commits made
through the GitHub repository integration have not automatically started fresh
Actions runs, so the earlier green hosted CI applies only to the previous head.
Native Windows GRASS/TUFLOW raster/project parity is still required.

## Engineering verification and remaining tasks

The original standalone implementation passed its Windows unit/typing checks.
That result does **not** establish parity after migration or real-model approval.
The migrated tests and wheel/template inclusion must be independently checked,
including a real TUFLOW/GRASS sample and representative native-resolution
terrain comparisons before archival.

- [#242](https://github.com/Chain-Frost/ryan-tools/issues/242) —
  model-footprint terrain clipping and native-grid QA
- [#243](https://github.com/Chain-Frost/ryan-tools/issues/243) —
  subcatchments, QGIS and longest flow paths
- [#244](https://github.com/Chain-Frost/ryan-tools/issues/244) —
  LiDAR/SRTM precedence, seams and vertical datums
- [#245](https://github.com/Chain-Frost/ryan-tools/issues/245) —
  exclusions and missing upstream-flow interfaces

The files in this directory preserve the original architecture, engineering
validation and design records. Their earlier installation instructions and
references to the standalone layout are historical; the new package layout
above is authoritative after migration is merged. The original test-data
GeoTIFF/GeoPackage/QGIS project remains accessible in the source repository
until a validated fixture-transfer plan is agreed.

## Migrated Windows parity handoff

The historical Windows records below this directory describe the **standalone**
package and are not proof of migrated results. After a source/wheel installation
on Windows Python 3.14 with GDAL/Rasterio, GRASS and TUFLOW available, run the
transferred verification script against the unchanged original repository fixture:

```powershell
$fixture = 'D:\Work\tuflow-initialiser\test_data'   # adjust to original repository
python .\repo-scripts\tuflow-initialiser\validate_test_project.py `
  --fixture $fixture `
  --grass-exe 'C:\OSGeo4W\bin\grass85.bat' `
  --tuflow-exe 'C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe' `
  --output 'D:\Validation\ryan-initialiser-check-01'
```

Use a previously nonexistent output directory. The validation runner rejects an
existing output and verifies SHA-256 of both input fixtures before processing.
It generates TUFLOW canonical *empty* GIS layers and runs GRASS-based hydrology
QA; it does **not** perform a populated hydraulic simulation.

Compare the resulting `qa.json`, spatial schemas, channel/subcatchment/entry IDs,
five hydrology raster grids and generated project templates against a clean run
of the standalone source on the same fixture. The historical reference has
805 stream paths, 535 local subcatchments and 111 AOI entry catchments/longest
paths, but these values alone do not establish full raster/geometry parity.
Record Python, GDAL, GRASS and TUFLOW versions, actual commands, output hashes,
QGIS findings and discrepancies against [ryan-tools issues #242–#245](https://github.com/Chain-Frost/ryan-tools/issues)
or the individual issue links above.

The fixture's redistribution permission, vertical datum and source provenance
remain unverified. Keep those binaries in the original repository; do not
transfer them to the published wheel or archive the original repository until
fixture access and engineering acceptance are resolved.

## Generic template extraction ? 2026-11-07

Local work on `feature/migrate-tuflow-initialiser`, based on `fe9c4ef1659bec748c62eb6b2b4bd7814270364a`:

- Extracted independent climate selection (`~e5~`, `CCNear`) from an updated local model;
  separated it from the `EnsembleILCL` rainfall-method event. New launch commands require `-e5`.
- Added four-digit duration aliases while retaining existing five-digit BC substitutions,
  plus the 9-hour event and an editable starter end time of 18 hours.
- Shared output, log and check paths now use `OutputBase` and a climate-specific `OutputRoot`.
  Added explicit maximum-only map storage and an unknown-rainfall-method diagnostic.
- Preserved existing solver, GIS/grid format and calibrated starter-loss settings. No model
  names, identifying paths, network GIS or project-specific rainfall data were imported.
- Updated generated-name checks, wheel smoke resource path and the
  [project setup guide](../../../ryan-scripts/tuflow/PROJECT_SETUP.md).

The user confirmed removal of the boundary-template marker and directed both entry
points to create the same project, with the initialiser adding actual data. The earlier
four profile-test failures are superseded by the shared four-layer configuration.
Validation on Windows Python 3.14.6 (2026-11-07):

- `python -m pytest tests/orchestrators/tuflow/test_project_setup.py tests/orchestrators/tuflow/initialiser -q`:
  111 passed. A final rerun of the five affected project/template/CLI files passed all 43 tests,
  including generated-file and GIS-schema equivalence for both entry points.
- Ruff format/lint, strict Pyright on modified files (initialiser files with
  `--project pyright-initialiser.json`), Loguru policy and documentation checks pass.
- `python repo-scripts/build_library.py --skip-pip`: built and verified wheel 26.11.7.1,
  SHA-256 `1ea59e4079b9fced8fe343d8b4c7fe7f5a8f901f31aefd8d9872d1ec9c16dfdb`.
- Temporary-target installed-wheel resource/default/CLI checks and copied standard
  wrapper `--help` passed. The normal Python environment was not replaced.
- PyTUFLOW's optional h5py import emitted Windows DLL diagnostics during test collection;
  tests still completed with exit 0. This does not establish native HDF5 capability.
- No populated TUFLOW simulation or native GRASS parity run was performed.

Delivery: scoped commits and pushes to the existing PR branch are now authorised.
Implementation pushed as `df0cda09`; instruction update pushed as `5bd7098e`.
The PR remains draft/open and unmerged. GitHub reports no checks on this head; the
last green hosted evidence belongs to `f32cff0` and does not validate these changes.
This delivery record is committed separately. Final-head hosted checks remain pending.
`AGENTS.md` and work tracking permit scoped staging, commits and pushes, while merging
requires explicit authorisation. Native engineering gates remain outstanding.
Next review: 2026-11-08.

## Source and history

- [Original repository](https://github.com/Chain-Frost/tuflow-initialiser)
- [Original main baseline after PR #11](https://github.com/Chain-Frost/tuflow-initialiser/commit/33ac5fe90a1c92e775427bcfa02f50c14dcadc34)
- [Migration PR #246](https://github.com/Chain-Frost/ryan-tools/pull/246)


## Stack integration ? 2026-11-07

The maintainer authorised merging #246 into #204 only. Current `main` was integrated
into #204 as `886be97c`; documentation conflicts retained both terrain/CAD additions
and newer main guidance/audit evidence. Parent validation: 156 focused terrain/CAD/runner
passes and two Windows symlink-privilege skips; strict runner typing and Ruff pass.
The initialiser branch now incorporates that parent. Conflict resolution preserves
scoped commit/push guidance, all work-register rows, and main's newer package version.
Combined validation: 267 focused terrain/CAD/project/initialiser/runner tests passed,
with two Windows symlink-privilege skips. Documentation/link checks, runner strict
Pyright and Ruff pass. The verification build produced wheel 26.11.7.9 (SHA-256
`fcc7e928505d381a45e226e9158a48e3082b5d685d88313f0a75895dd8aac9a5`).
The parent was pushed at `886be97c`; hosted checks started for that head. The child conflict-resolution commit `8f92c9f6` was pushed and merged into #204
as `e0522e96`; GitHub confirms #246 is closed/merged. The maintainer explicitly
requested integration without waiting for CI. The child Ruff job failed (two import
ordering findings and SIM115 on the temporary-file publication); other jobs were
still running. These checks are not reported as green. Fix/recheck CI on #204
before any separately authorised main merge.
The earlier h5py DLL diagnostic still appears during collection; it does not prevent
these tests passing and does not establish native HDF5 capability. No merge of #204
into main, engineering approval, release or publication is authorised by this integration.


## CI reproduction and repair ? 2026-11-07

Reproduced the hosted changed-file commands locally on #204. Repaired import order
in the shared terrain helper and migrated test; moved temporary publication into a
proper context manager while keeping close-before-publish and no-clobber cleanup.
CI previously assigned migrated shared GIS modules to the default stubs, producing
144 strict typing errors locally. Policy now derives that split from
`pyright-initialiser.json`'s include list, preserving strict checking with the right
GIS stubs. Tests that intentionally exercise the private publication helper declare
that use explicitly. Both strict typing passes, changed-file Ruff and documentation/
Loguru checks pass. Actionlint 1.7.12 passes with the workflow's pinned checksum.
The separate MCP utility command passes 73 tests. Built/verified wheel 26.11.7.10.
The complete hosted test command and isolated package smokes are running locally;
final results and pushed-head CI status will be recorded after completion.


The complete local hosted run exposed an inherited Windows fixture defect in
`tests/functions/test_qgis_project_tools.py`: SQLite was asked to create `aux.qgd`,
which targets the reserved AUX device. A process stack dump confirmed the full
run blocked in `_qgd`; isolated reproduction failed to open the database. Renamed
the synthetic database to `auxiliary.qgd`; all 30 QGIS project-tool tests now pass.
The original blocked run was terminated and the unchanged hosted selection is
being rerun, with diagnostic stack dumps enabled for any further long-running test.
