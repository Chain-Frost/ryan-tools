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
#204 and its required validation are accepted. #246 is already integrated; no
retargeting is needed. Retirement is tracked separately in [#247](https://github.com/Chain-Frost/ryan-tools/issues/247).

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
Native Windows source/wheel parity is completed in the dated evidence below;
full terrain engineering acceptance remains separate.

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
  [project setup guide](../../../ryan-scripts/TUFLOW-python/model_management/PROJECT_SETUP.md).

The user confirmed removal of the boundary-template marker and directed both entry
points to create the same project, with the initialiser adding actual data. The earlier
four profile-test failures are superseded by the shared four-layer configuration.
Validation on Windows Python 3.14.6 (2026-11-07):

- `python -m pytest tests/orchestrators/tuflow/test_project_setup.py tests/orchestrators/tuflow/initialiser -q`:
  111 passed. A final rerun of the five affected project/template/CLI files passed all 43 tests,
  including generated-file and GIS-schema equivalence for both entry points.
- Ruff format/lint, strict Pyright on modified files (initialiser files with
  the then-separate initialiser project configuration), Loguru policy and documentation checks pass.
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


## CI reproduction and repair - 2026-11-07

Reproduced the hosted changed-file commands locally on #204. Repaired import order
in the shared terrain helper and migrated test; moved temporary publication into a
proper context manager while keeping close-before-publish and no-clobber cleanup.
CI previously assigned migrated shared GIS modules to the default stubs, producing
144 strict typing errors locally. Policy now derives that split from
the then-separate initialiser configuration's include list, preserving strict checking with the right
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


### Final local CI result and delivery

On Windows/Python 3.14.6, the complete hosted command from `.github/workflows/ci.yml`
(with the same ignores, markers and two floodway deselections, plus `-q` and a
faulthandler diagnostic timeout) passed: **2,594 passed, 33 skipped, 8 deselected**,
187.00 seconds. The skipped cases include Windows symlink privileges, POSIX-only
semantics and opt-in native/share acceptance. All 47 changed Python files pass
Ruff lint/format; both strict Pyright groups, Actionlint 1.7.12, documentation and
Loguru policy pass. The additional MCP utility command passes 73 tests.

All three isolated installed-wheel scripts pass: `smoke_test_installed_wheel.py`,
`smoke_dashboard_installed_wheel.py` and `smoke_result_dashboard_installed_wheel.py`.
Wheel 26.11.7.10 is verified (SHA-256
`8bb4bdb7bae516abf9120fb5e3b31923345ecd49a08264c44aaa3059669b8896`).
Temporary-target smoke dependencies were installed without replacing normal Python.

An initial full-run packaging failure was local stale bytecode under the removed
`ryan_library/scripts` namespace. Inspection found only `.pyc` caches; these were
preserved outside the checkout. All 21 packaging tests and the clean full rerun pass.
No obsolete package source was restored or test expectation weakened.

Repairs are pushed as `ccc59b72` and `914dcda5`. All six hosted checks are terminal
and successful on `914dcda5`: Python CI run `42388174134` and Fast PR run `42388174135`.
The local-CI guidance and this validation record are a subsequent documentation-only
commit; its checks are separate from that successful code-head evidence. The checkout
is on surviving branch `feat/tgc-terrain-crop`; #246 remains merged into #204, while
#204 remains open. Native engineering gates and any main merge remain separate.


## 2026-11-08 - review repairs and native acceptance

Refreshed #204 with main `5ad632cc` at merge `c72a85d5`; retained current
commit/push guidance and reconciled the live inventory to 160 files (83 selected,
77 excluded). Fixed both unresolved review findings:

- CAD overwrite rejects SQLite WAL/SHM/rollback journals and active readers/writers.
  It holds SQLite exclusive locking mode across a transactional backup into the
  existing destination, preserving file identity instead of replacing a live inode.
  Publication tests cover active WAL/rollback writers, active readers, successful
  overwrite and actual DXF conversion/overwrite.
- Written terrain coverage QA preserves the original polygon clip. A 6 m native /
  10 m planning-grid regression now detects the uncovered column and prevents
  patched-TGC publication rather than reporting the spillover pixel as coverage.

### Native migration baseline

Built/verified wheel 26.11.8.1 and installed it into a temporary target. Both the
immutable original source `33ac5fe90a1c92e775427bcfa02f50c14dcadc34` and the wheel
ran the checksummed original fixture with GRASS 8.5 and TUFLOW 2026.3. The original
validation harness omitted `generate_catchments=True` despite requiring its outputs;
only the temporary archived harness was corrected to match the migrated settings.
Original algorithm/source files and the source checkout were not edited.

All five hydrology rasters have exact grid/CRS/pixel equality. All 107 paired
GeoPackage layers (including canonical empty layers) have identical schema, CRS,
feature attributes and geometry. Counts remain 805 streams, 535 subcatchments,
111 inflow catchments and 111 longest paths. Intentional project differences are
boundary GIS, QML, utility copying, projection filenames and the updated templates.
Original build/read-back: 19.04 s; migrated: 18.42 s. These timings do not establish
large-project performance or hydraulic simulation acceptance.

QGIS 4.2.3, through OSGeo4W's initialized `python-qgis.bat`, reopened the eight
working/hydrology layers: valid EPSG:28350 layers, no invalid/empty geometries.
Subcatchment overlap area is numerical roundoff only (absolute 0.000013 m2).
A native fixture crop preserves 10,000 valid elevations and resolution; its
101-by-100 output has one NoData padding row from window rounding.

### Approved private-model assessment

The maintainer approved the previously supplied model for validation. Its identity,
paths, coordinates and raw reports remain private, outside the repository. Read-only
inventory found 12 active terrain rasters, one polygon clip, 1 m / 5 m resolutions,
one horizontal CRS and no embedded vertical CRS. A bounded roughly 500 m square
analysis contains 10,100 valid analysis cells: independent sequential source
composition matches both elevations and source indices exactly. The highest-priority
source supplies 36 cells; a lower source supplies the remaining 10,064. Their
36-cell overlap differences range from -0.224 to +0.101 m (median -0.122 m);
these are observed source differences, not proof of vertical-datum compatibility.
Prepared-window validation retains two rasters, omits ten inactive-in-window rasters,
and preserves every valid native elevation/CRS/resolution (250,000 and 786 cells).
Source data was unchanged. Cropping took 11.59 s for this bounded case.

Remaining engineering gates are explicit: no regional SRTM/different-CRS terrain
was present in the selected stack; vertical datum/provenance was not established;
no real exclusion polygons or signed-off missing-upstream-flow treatment were
identified; full original/prepared TUFLOW DEM_Z and large-area performance remain
unverified. Do not close #242-#245, archive the original repository or infer full
hydraulic acceptance from these bounded checks. Synthetic exclusion regressions
remain distinct from real-project engineering approval.

Focused CAD/terrain regressions and strict typing pass. The full hosted command
reported 2,618 passes / 33 skips with one stale inventory-count assertion; that
work-register count is corrected and all 22 inventory tests pass. Full final-head
local/hosted validation, fresh review and pushed delivery are being refreshed.


### Final validation and deferred mixed-CRS check

The full local hosted selection passes: **2,619 passed, 33 skipped, 8 deselected**
(201.58 s). The four SQLite publication cases also run in the Linux security job
without GDAL/test-fixture dependencies; its `POSIX wrapper security` job passed
at `60ba250c`. Existing review threads are all resolved. A fresh Codex review was
requested and is running on runtime-fix commit `a0956b77`; `60ba250c` subsequently
adds only test/CI coverage. Other final-head hosted jobs remain pending/running;
no green conclusion is claimed for them.

The maintainer confirms that the approved terrain is **AHD71** vertically, and
has no different-horizontal-CRS representative dataset. This is user-confirmed
source provenance; no height conversion was performed. Mixed horizontal CRS
acceptance is deliberately deferred in
[#266](https://github.com/Chain-Frost/ryan-tools/issues/266), linked to #244.
That future check does not invalidate completed same-CRS parity/source-priority
results. Real exclusion/interface engineering (#245), full TUFLOW DEM_Z and other
unverified source-provenance/large-area acceptance remain separate follow-ups.

Delivery: main refresh `c72a85d5`, review fixes/native evidence `a0956b77`, Linux
publication coverage `60ba250c` pushed to the existing #204 branch. Documentation
handoff is committed separately. No main merge, release, binary-fixture transfer,
source-repository archival or changes to private model sources were performed.
MCP assessment: no discovery changes; these repairs preserve the existing CLIs
and safety boundaries rather than adding a new workflow interface.

## 2026-11-08 ? current closeout state

All five #204 review threads are resolved. At head `5bda006d`, five hosted
checks pass and Package fails in a timestamp-sensitive installed result-wrapper
collision smoke; the test launcher is being repaired to freeze both filename
clocks. These statements supersede earlier pending review/parity/stack notes.

#247's checklist now records the completed child integration, shared templates
and native source/wheel parity. It remains open for final CI, acceptance and
source retirement. #242?#245 and #266 retain their own engineering gates; no
automatic closing keywords are configured. Main merge, release and archival
remain unauthorised.

### Collision smoke repair validation and delivery

The repaired smoke passes all six copied result wrappers against isolated wheel
26.11.8.1, with user-site and checkout imports excluded. Additional mean/median
probes rename the baseline workbook to a year-2000 timestamp: both correctly
reject the collision and preserve all existing workbook bytes. Ruff lint/format,
strict Pyright for the modified smoke script, documentation/index checks and
scoped link checks pass. No library/package source changed, so the existing wheel
was reused without a version bump. The temporary install leaves normal Python
unchanged. Repair and reconciled records are pushed as `10d1ea93`;
final-head hosted CI remains required. No merge into main is authorised.

### Verified CI milestone

All six checks pass on repair delivery `103373f0` (Python CI `42399228386`,
fast checks `42399228368`), including installed Package smoke and Hosted tests
(5m34s). This supersedes the historical Package failure and pending checks above.
The handoff-only follow-up has its own final-head checks, recorded on PR #204
and #247. Implementation and review/CI repairs are complete; engineering
acceptance (#242?#245, #266) and retirement (#247) remain open.
No automatic issue closure, auto-merge or main merge is authorised.


## Projection template authority encoding

Fix proposed in [PR #275](https://github.com/Chain-Frost/ryan-tools/pull/275);
source commit `24b4f259` pushed, hosted checks pending. No merge performed.

Project setup now normalises an input PRJ to its EPSG definition only when PROJ
identifies an exact match (confidence 100). Both the standard project-setup entry
point and the initialiser use this shared writer. GeoTIFF templates store the
projected EPSG GeoKey directly and GeoPackages register the EPSG authority.
Custom grids retain their supplied WKT and the copied PRJ remains unchanged.
This avoids user-defined TIFF encoding that TUFLOW native output handling can
fail to preserve. Existing projects and result rasters are not rewritten.

Validation covers ESRI WKT for GDA94/GDA2020 MGA zones 49-56 and a custom grid,
including raw TIFF GeoKeys and GeoPackage authority records. The 29 focused
project-setup/shared-entry tests pass, as do Ruff and strict Pyright. Package
26.11.8.2 builds and passes wheel verification. Native TUFLOW output read-back
with these newly generated templates remains unverified.

### Version-only CI scope regression

The first hosted run on #275 (`42465992518`) failed Ruff preflight because
the version bump selected all 639 first-party Python files. It exposed 23
pre-existing lint failures in unrelated files and blocked downstream jobs.
The PR now distinguishes version-only `pyproject.toml` changes from other
configuration edits. Focused CI-scope regressions verify narrow static
checks for version bumps, and preserve broad static checks for dependency
and tool configuration edits. Runtime/package jobs remain required.
Verify final-head CI before integration; native TUFLOW output read-back
remains a separate acceptance gap.


## 2026-11-08 - PR #275 native projection acceptance PASS

Validated immutable PR head `1e7505eaeb3f9020f2f9109c889e79467c96cc94` in
`E:/Github/ryan-tools-pr275-native`, a separate detached worktree. GitHub head
was checked before and after execution and was unchanged. Windows 10 build 19045,
Python 3.14.6, Rasterio 1.5.2 and native TUFLOW `2026.3.0-iSP-w64` were used.

Both shared standard project setup and the migrated initialiser adapter generated
projects from ESRI WKT1 PRJs for EPSG:28350, EPSG:7850 and a synthetic custom
Transverse Mercator grid (central meridian 117.123 degrees, GRS80 ellipsoid).
Repeated all six creations through isolated installed wheel 26.11.8.2. All 12
copied PRJs remain byte-identical. Raw TIFF GeoKey 3072 stores 28350/7850;
GeoPackage geometry SRS registrations are EPSG/28350 and EPSG/7850. Custom TIFFs
retain user-defined 32767, GeoPackages retain NONE/100000, and stored CRS definitions
remain equivalent to the supplied local grid. No unrelated EPSG was assigned.

Eight disposable native Classic demo simulations (two CRSs, two entry points,
source/wheel) completed with exit 0 and no warnings. Each is a 100 m square,
10 m hydraulic grid, flat bed 0 m, initial still water 1 m, Manning n 0.03,
closed boundary file, 0.5 s timestep and 0.001 h end time. Native TIF h/d output
uses a 5 m raster grid and one-second map interval. All 88 output TIFFs reopen
independently through Rasterio; raw GeoKeyDirectory parsing confirms 3072 is the
correct EPSG code, 1024=1, 1025=1 and 3076=9001. Every raster is 20 by 20 with
400 valid pixels and GDAL geotransform (500000, 5, 0, 6500100, 0, -5).
Depth/level values remain within 0.00001 m of 1 m; time-of-maximum values are
within the simulated period. All 66 paired source/entry-point/wheel comparisons
have exactly equal pixels, CRS and grids. Essential CRS-key loss does not reproduce.

Commands and retained local evidence (outside the repository):

```powershell
python repo-scripts/build_library.py --no-bump --skip-pip
python -m pip install --no-deps --target F:/Temp/CodexValidation/PR275_20261108/wheel_install dist/ryan_tools-26.11.8.2-py3-none-any.whl
python F:/Temp/CodexValidation/PR275_20261108/accept.py E:/Github/ryan-tools-pr275-native source
python F:/Temp/CodexValidation/PR275_20261108/accept.py F:/Temp/CodexValidation/PR275_20261108/wheel_install wheel
python F:/Temp/CodexValidation/PR275_20261108/native.py
```

The project-generation harness precedes native.py. Its first native attempts
could not obtain a licence; early disposable-model attempts also lacked materials
and boundary configuration. These setup failures are preserved in local logs;
only the eight final `native_verified` runs establish acceptance. Native command:
`C:/TUFLOW/tuflow-2026.3/TUFLOW_iSP_w64.exe -b -nmb <native_verified/native.tcf>`,
with `Demo Model == ON`. Reports: `report_source.json`, `report_wheel.json`,
`native_report.json`; per-case control files, stdout and raster outputs are retained
under `F:/Temp/CodexValidation/PR275_20261108`. Rebuilt wheel SHA-256:
`7724ceeadf220d08e5630bd819bee75cb92a15eade934326badf8e8277edb90b`.
The normal installed Python package was not replaced; no version bump was made.

Scope is small synthetic Classic output acceptance. No custom-grid simulation,
HPC/GPU acceptance, full initialiser/GRASS replay or real-model engineering approval
is claimed. No private inputs were accessed or modified, and no binary datasets
are published. Hosted CI's upstream wheel-host failure remains separate (#276).
This documentation-only delivery does not change the validated runtime source;
its final hosted checks must be assessed separately. No merge or release.
Next review: 2026-11-15, for PR review/CI and the remaining engineering gates.


### 2026-11-08 shared typing standard

The initialiser now uses the same strict Pyright configuration and shared dependency
stubs as the rest of the repository. The former separate project configuration
and duplicate stub tree are retired. Earlier split-profile results above remain
historical evidence; they do not define a different maintained-code standard.


## Location margin and 400 m grid alignment - 2026-11-10

The populated location rectangle now adds at least 400 m around the active code extent, then rounds its west/south
bounds down and east/north bounds up to whole 400 m multiples. Previously it used the exact active bounds and supplied
no inactive-cell margin. This changes location only; active code, rainfall and contributing-area selection are unchanged.
Four focused regressions cover positive/negative coordinates, exact multiples and fractional outward rounding.
Windows Python 3.14: 132 focused project-setup/initialiser tests pass; strict changed-file Pyright and Ruff pass.
Package 26.11.10.2 builds and verifies. Hosted final-head acceptance remains separate.

A native private-project check re-created the analysis grid and GRASS D8 outputs: all 821,843 selected upstream cell
squares are covered by the persisted code polygon, with zero missing area. A queried boundary neighbour drains away
from the AOI and is correctly excluded. No polygon simplification is used. The existing analysis mosaic does resample
to an aligned grid; for the checked single-DEM case its origin shifted about 3.59 m west and 13.59 m north. This is a
separate analysis-grid choice, not evidence that original raster cells and analysis cells share an origin. Original
terrain and project-specific inputs remain private; no hydraulic simulation was run.
