# TGC terrain crop workflow

| Field | Value |
| --- | --- |
| Status | Implementation and five review fixes delivered; Package smoke repair in progress; engineering acceptance open |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-08 |
| Next review | 2026-11-11 |
| Baseline | `main` at `ec873425`; branch `feat/tgc-terrain-crop`; unrelated local GDAL-script changes preserved |

## Outcome and scope

Add a maintained, reusable script that asks PyTUFLOW to select the ordered
`Read Grid Zpts` stack for one `-s1` scenario, clips it to a user-supplied box,
removes deep lower-priority overlap, and retains a configurable seam allowance.
The supported contract deliberately has one scenario flag and no multi-context
or scenario-slot switching.

## Current state

Implementation, focused tests, user documentation and real plan-only model
validation are complete. The script defaults to plan-only, writes only
new destinations with explicit `--write`, preserves native grids and source
values, and produces a patched TGC plus manifest. No production rasters or TGCs
were changed during validation. PR #204 remains open. All five review threads are resolved. Native-grid TUFLOW DEM_Z equivalence and final-head hosted validation remain required.

## Next action

Repair the timestamp-sensitive installed-wrapper collision smoke and verify all
checks on the final pushed head. Keep #242?#245, #266 and retirement tracker #247
open for their remaining acceptance. No merge or release is authorised.

## Completion criteria

- PyTUFLOW owns conditional selection for one scenario value.
- The analysis box is the hard limit and the default seam allowance is 50 m.
- Focused selection, precedence/buffer, write and overwrite-safety tests pass.
- Strict Pyright, documentation checks, a package-free CLI smoke and an actual
  plan-only model check pass.
- The branch is pushed and a pull request is open without unrelated changes.

## Validation and delivery

- `python -B -m pytest -q -p no:cacheprovider tests/test_crop_tgc_terrains.py`:
  3 passed with the current Q: PyTUFLOW checkout; one existing pytest
  configuration warning.
- Strict Pyright on the maintained script and focused tests: 0 errors, 0
  warnings.
- TUFLOW Python README link check: passed.
- Actual private model plan-only checks at 50 m and the shipped 10 m planning
  resolution selected 12 ordered terrain layers and completed without writes.
- Local Ruff execution is blocked by corporate executable policy; GitHub's
  pinned Ruff lint and format check passed.
- Exact-head GitHub validation on `c8359713` passed Ruff and whitespace, Policy,
  Package, MCP smoke, POSIX wrapper security and Hosted tests.
- Delivery: branch `feat/tgc-terrain-crop` pushed; PR #204 open and mergeable.

## Progress

### 2026-11-08: included terrain review finding

Reject active terrain reads owned by `Read File` includes during resolution,
before coverage planning or output creation. This prevents applying child-file
line numbers to the root TGC. Included terrain hierarchies remain unsupported;
root-file terrain reads retain their existing behavior. Added plan-only and
write-mode regressions proving no outputs are created. All nine terrain tests,
Ruff lint/format and strict Pyright on both modified Python files pass.
The initial test invocation emitted a native h5py loader diagnostic; rerunning
with h5py preloaded passed cleanly. Fix pushed as `45452b0b`; review thread resolved. Current-head hosted checks
are pending; no merge authorized.

### 2026-11-07

Restored `ryan-scripts/cad-python/cad_to_gpkg.py` from historical commit `f4c80be` at maintainer request, including its README usage section and lifecycle audit entry. This is a separate CAD utility, not part of terrain cropping; the QGIS submodule pointer already matches `main` and was not modified. Corrected the native-mask regression test's explicitly typed Affine transform and synchronised the stacked initialiser PR #246. All new changes require exact-head hosted CI; historical green checks do not establish acceptance at this head.


### 2026-11-04

Implemented single-scenario PyTUFLOW scope evaluation, overview-aware bounded
coverage reads, reverse-precedence allocation, a 50 m buffered seam, native-grid
lossless outputs, a non-overwriting patched TGC and JSON audit manifest. Replaced
an initial virtual-warp mask scan after real-data evidence showed excessive time
and memory; the final overview-aware window path bounded memory and completed.

## Coverage QA GeoPackage

A `--write` run now creates `terrain_coverage_qa.gpkg` alongside the
prepared rasters and JSON manifest. The GeoPackage provides directly loadable
QGIS polygon layers:

- `original_raster_footprints`: the **full bounding footprint** of each
  original raster, including areas of NoData (not proof of valid coverage)
- `original_valid_coverage`: valid source coverage within `--bounds` and
  any supplied TGC polygon mask, evaluated at the planning resolution
- `clipped_out`: source-valid cells removed by the crop within `--bounds`
- `minimum_coverage_extent`: exact user-requested `--bounds` polygon
- `final_clipped_extent`: valid coverage read back from the cropped output,
  evaluated at the same planning resolution
- `coverage_gaps`: cells of the requested minimum extent not covered by
  the final prepared rasters
- `lost_valid_coverage`: originally covered cells lost from the prepared stack

Per-source records include original raster path, TGC command order and line,
optional clip path, output path, area and processing method. QA cell counts
and the GPKG path are recorded in `terrain_crop_manifest.json`.

These polygons are **planning-grid approximations**, not an exact assessment
at the native raster resolution. The source footprint layer deliberately
includes NoData. This output helps locate and review clipping errors in QGIS
but does not establish hydraulic equivalence or native-resolution coverage
guarantees; those checks remain required under
[tuflow-initialiser #6](https://github.com/Chain-Frost/tuflow-initialiser/issues/6)
and [#9](https://github.com/Chain-Frost/tuflow-initialiser/issues/9).
Plan-only runs remain read-only.


## User-requested runner typing transfer (2026-11-07)

At the user's explicit request, PR #204 also carries the previously uncommitted
`simulation_runner.py` typing changes from the #183 checkout. The transfer is
limited to annotations and a relative import inside TYPE_CHECKING; the newer
#183 dashboard ordering/defaults and other dashboard work are not transferred.
Use the existing `threading.Lock` class as the lock annotation and retain the
already-declared progress snapshot type import. One redundant local `groups`
annotation is omitted because its combination with the typed deque introduced
Pyright unknown-type flow errors; the setting's existing tuple type is preserved.

Ruff lint/format, strict Pyright and compilation pass. Focused simulation-runner
and advisory-progress checks report **173 passed, two Windows symlink-privilege
skips**. Normalized runtime AST matches #204's original module after removing
annotation-only declarations and TYPE_CHECKING imports. A verification build
with `--no-bump --skip-pip` preserves package `26.11.6.2` and verifies its wheel
(SHA-256 `591e4ead0e7a920f83d5fb584ad701ba9eb1ed645a3d17c1f75e79ab5fddb77d`).
The verification wheel is isolated; the user's installed dashboard package is
not replaced. Final-head hosted checks remain authoritative at PR #204.
No merge or publication is authorized.


Typing implementation is pushed as `ae338782`. GitHub currently reports #204
as conflicting with `main` (`DIRTY`); no workflow run was created for that head.
Earlier passing runs belong to `becdc448` and are not validation of the typing
transfer. Resolve the branch conflicts before collecting current-head PR CI.
This transfer does not authorize merging or rebasing unrelated terrain work.
The original checkout independently moved to clean `main` during the transfer;
its changed state was detected before cleanup and left untouched.


### 2026-11-07 ? stack conflict resolution

Merged current `main` into the #204 branch to resolve documentation conflicts,
preserving terrain/CAD guidance and the newer result-wrapper safety guidance and
inventory recheck evidence. The static audit reports 159 tracked files, 83 selected
and 33 MCP targets; 76 files are excluded. Shared runner changes combined cleanly
and strict Pyright/Ruff checks pass. Focused terrain/CAD/runner validation is recorded
with the integration follow-up. The maintainer authorised merging child #246 into
#204; merging #204 into `main` remains unauthorised. Native engineering gates remain.


Child #246 merged into #204 as `e0522e96`, after conflict-resolution child
`8f92c9f6`. GitHub confirms the child is closed/merged and #204 remains open.
Combined local validation: 267 passes / two privilege skips; verified wheel 26.11.7.9.
The maintainer requested no wait for CI. Child Ruff failed on two import-order
findings and SIM115; other jobs remained running. Address these on #204 and
obtain final-head CI/native engineering acceptance before an authorised main merge.
No main merge or publication was performed.


### 2026-11-07 - local CI repair completed

On #204, repaired the migrated initialiser's Ruff findings and GIS strict-typing
routing, and an inherited Windows reserved-AUX-name fixture. The full hosted test
command passes locally: 2,594 passed / 33 skipped / eight deselected. Both strict
Pyright groups, all 47 changed-file Ruff checks, Actionlint and documentation/logging
policy pass. Additional MCP utility tests: 73 passed. Verified wheel 26.11.7.10 and
all three isolated installed-wheel smoke scripts pass. Local stale obsolete bytecode
was preserved outside the checkout; packaging tests and full rerun pass.
All six hosted checks pass at repair commit `914dcda5` (Python CI `42388174134`,
Fast PR `42388174135`). Subsequent documentation-only delivery has separate checks.
The [initialiser record](tuflow-initialiser/README.md) owns detailed reproduction and
delivery evidence. Native engineering acceptance remains outstanding; no main merge.


### 2026-11-08 - current-main conflicts and review findings

Main refreshed at `c72a85d5`; live inventory is 160 files / 83 selected / 77 excluded.
The CAD SQLite publication and clipped-raster final-QA review findings are repaired
with executable regressions. Native original-versus-installed migration equality,
QGIS read-back and approved anonymized real-terrain-window validation passed; the
[initialiser record](tuflow-initialiser/README.md#2026-11-08---review-repairs-and-native-acceptance)
owns detailed evidence and the remaining engineering gates. Validation/review and
pushed-head CI are being refreshed. No main merge or publication is authorised.


Final local hosted selection on the repaired runtime: 2,619 passed / 33 skipped /
eight deselected. Both review findings are resolved. Linux SQLite publication
coverage passes at `60ba250c`; remaining hosted jobs and fresh Codex review are
still pending/running. The maintainer confirmed AHD71 and deferred representative
mixed-horizontal-CRS testing to [#266](https://github.com/Chain-Frost/ryan-tools/issues/266).
Other real-model engineering gates remain explicit in the initialiser record.

## 2026-11-08 ? closeout reconciliation

At head `5bda006d`, five hosted checks pass; Package fails in the installed
mean/median report collision smoke. Its launcher freezes the exporter clock but
the orchestrator independently chooses an explicit timestamped filename.
Crossing a minute boundary therefore tests a new destination. Freeze both clocks;
production export behavior and overwrite safeguards are unchanged.

Issue #247 now reflects #246 already merged into #204, completed native source/wheel
parity, shared templates and all five resolved review findings. It remains open
for final CI, engineering disposition and source retirement. No automatic issue
closure is appropriate for #242?#245, #266 or #247. Bounded checks do not close
their outstanding engineering or archival requirements. Earlier conflicting/
pending stack states above are historical and superseded by this section.
Final smoke/CI evidence and delivery will be recorded after validation.
No main merge, release, source archival or private-data mutation is authorised.

### Collision smoke repair validation and delivery

The repaired smoke passes all six copied result wrappers against isolated wheel
26.11.8.1, with user-site and checkout imports excluded. Additional mean/median
probes rename the baseline workbook to a year-2000 timestamp: both correctly
reject the collision and preserve all existing workbook bytes. Ruff lint/format,
strict Pyright for the modified smoke script, documentation/index checks and
scoped link checks pass. No library/package source changed, so the existing wheel
was reused without a version bump. The temporary install leaves normal Python
unchanged. Repair and closeout records are being committed/pushed together;
final-head hosted CI remains required. No merge into main is authorised.
