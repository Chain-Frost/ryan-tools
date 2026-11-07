# TGC terrain crop workflow

| Field | Value |
| --- | --- |
| Status | In review; CI rerunning after native coverage fix and CAD script restoration; native TUFLOW equivalence outstanding |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-07 |
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
were changed during validation. PR #204 remains open for review. Native-grid TUFLOW equivalence and final-head hosted validation remain required.

## Next action

Obtain maintainer approval before merge. No merge or release is authorised by
this work.

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
