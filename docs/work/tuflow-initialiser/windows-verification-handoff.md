> **Historical record from [tuflow-initialiser](https://github.com/Chain-Frost/tuflow-initialiser).** Pending or proposed work is tracked in ryan-tools migration PR #246 and issues #242–#245; old issue numbers below are retained for provenance.

# Windows GRASS/TUFLOW verification handoff — PR #1

**Tracked future engineering verification:** [#8 — QGIS catchment and LFP review](https://github.com/Chain-Frost/tuflow-initialiser/issues/8), [#9 — representative LiDAR/SRTM and vertical datum verification](https://github.com/Chain-Frost/tuflow-initialiser/issues/9), and [#10 — realistic upstream exclusions](https://github.com/Chain-Frost/tuflow-initialiser/issues/10). The reference Windows baseline below is complete; these additional representative-data checks are intentionally out of scope for the initial PR. Template and D8 integrity defects remain code merge blockers, not deferred checks.

**Status:** Mandatory engineering verification; not performed on hosted GitHub
Actions. Windows Python 3.14 CI covers unit tests, linting and typing but **does
not install GRASS or execute a populated TUFLOW model**.

## 1. Baseline repeat on the tracked SRTM/two-AOI fixture

### Latest masked-D8 verification: 13 October 2026

The installed-tool baseline was rerun against clean commit `007c24d`, including
the masked-D8 validity fix, using GRASS 8.5 and TUFLOW 2026.3. Fixture hashes and
all reopened-output QA checks passed. Build plus QA took 13.08 seconds.

Evidence remains local and ignored:

- `validation/masked-d8-2026-10-13/project/qa.json`
- `validation/masked-d8-2026-10-13/comparison.json`
- `validation/masked-d8-2026-10-13.log`

Compared with the retained 12 October corrected baseline, all five hydrology
rasters have exact grid/CRS/pixel equality. Every layer in the streams, hydrology,
inflow catchments and three TUFLOW scenario GeoPackages has identical schema,
CRS, feature count, attributes and geometry. This includes 805 stream paths,
535 subcatchments, 111 AOI entry catchments and 111 longest paths. All 11 negative
stream slopes remain unchanged.

Current local checks: **56 tests passed**, no skips/deselections; Ruff lint and
format checks passed; strict Pyright reported zero errors/warnings. This closes
the installed-tool baseline rerun gap for the masked-D8 change. The tracked
fixture does not exercise every invalid-routing rejection case; those retain
regression-test coverage. No populated TUFLOW model check or hydraulic simulation
was performed. PR readiness and merge status were not changed; no commit, push
or remote update was performed for this rerun.

### Completed local run: 12 October 2026

The baseline was rerun from commit `35d98bb` with the local fixes described below,
using GRASS 8.5 and TUFLOW 2026.3. Fixture hashes passed. Build and reopened-output
QA completed in 11.24 seconds. Retained local evidence:

- `validation/windows-handoff-2026-10-12-fixed/project/qa.json`
- `validation/windows-handoff-2026-10-12-fixed/comparison.json`
- `validation/windows-handoff-2026-10-12-fixed.log`
- The generated project's `terrain/hydrology/subcatchments.json` and
  `terrain/hydrology/aoi_inflows.json`.

The first attempt exposed unsupported `Affine @ (x, y)` calls in both new GIS
writers. These now use the supported `Affine * (x, y)` operation; the incorrect
`__matmul__` declaration was removed from the local stub. Writer tests now check
persisted outlet coordinates and whole-path geometry against expected map
coordinates, in addition to the existing coverage/attribute checks.

| Check | Result |
| --- | --- |
| Grid, contributing area, active area | Match the established baseline below |
| Five hydrology rasters | Exact grid/CRS/pixel equality with 11 October artifacts |
| Original stream paths | All 805 IDs, attributes, geometries and signed slopes exactly match |
| Subcatchments | 535: 533 channel-local and 2 direct-AOI residuals |
| Subcatchment local area | 102,444,147.109181 m²; coverage/non-overlap/topology QA passed |
| AOI inflows and longest paths | 111 entry catchments and 111 paths across 2 AOIs; QA passed |
| Signed negative slopes | 11 retained channel paths; 0 negative whole longest paths |
| Local quality checks | 48 tests passed; Ruff lint/format and strict Pyright passed |

This completes automated baseline verification of the new products. Visual
engineering review and the representative checks in section 2 remain pending.
TUFLOW generated canonical empties; no populated-model check or hydraulic
simulation was run. Generated artifacts stay ignored. No commit/push or issue
update was performed for this run.

On a Windows machine with Python 3.14, GRASS 8.5 and TUFLOW 2026.3 installed,
from the repository root after installing the dependencies in the README:

```powershell
$env:PYTHONPATH = 'src'
python scripts/validate_test_project.py `
  --grass-exe 'C:\OSGeo4W\bin\grass85.bat' `
  --tuflow-exe 'C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe' `
  --output 'validation/subcatchments-base/project'
```

Adjust executable paths to the actual Windows installations. Change the output
directory on every rerun; this project will not overwrite an existing output.
The script checks `test_data/manifest.json` source hashes, invokes GRASS
and TUFLOW empty-file generation, then reopens the populated outputs.

Inspect `validation/subcatchments-base/project/qa.json` for:

- The established analysis grid 761 × 824 at 28.59971091 m.
- Full AOI contributing area 102.444147 km² and active code/rainfall area
  106.016769 km² (no changes expected to hydrology footprint).
- **805 pre-existing stream paths** (533 channel, 272 synthetic headwater);
  all 11 signed negative-slope path records must be retained for QA.
- Three TUFLOW canonical GIS layers passing schema and geometry checks.
- The new `subcatchments` QA: positive local polygons, no overlap, identical
  full AOI contributing coverage in the baseline without exclusions,
  downstream IDs present and the local-area sum matching GIS area.
- New `model/gis/hydrology.gpkg` `subcatchments` layer with local, full
  and cumulative areas, downstream references, DEM statistics and stream slopes.
  The `terrain/hydrology/subcatchments.json` file records total analysed,
  excluded and full contributing raster cell counts.
- New `model/gis/inflow_catchments.gpkg` AOI entry catchments and
  `longest_flow_paths`, plus `terrain/hydrology/aoi_inflows.json`.
  Check multiple independent drainage inflows; a single AOI can have more
  than one exterior D8 crossing.

Compare the baseline outputs and 805 path geometries/IDs with the accepted
11 October baseline in `docs/base-workflow-validation.md` and the retained
Windows validation artifacts, if available. The script's TUFLOW call generates
**canonical empties**, not a hydraulic solution or a simulated model.

## 2. Representative LiDAR/SRTM and exclusion runs

The tracked fixture contains only one DEM and no exclusion polygon. It cannot
close issues #4 or #5. On suitable representative project data:

1. Build with repeated `--terrain` in **high-to-low** priority order:
   e.g. LiDAR first, then the regional DEM. Inspect the source-index raster,
   source DEM NoData holes, reprojected native-resolution TUFLOW terrain copies,
   `Read Grid Zpts` ordering and seam elevations.
2. Record input horizontal CRS, horizontal/vertical units and vertical datum
   explicitly. A horizontal EPSG identifier does **not** verify vertical
   compatibility. Compare coincident valid LiDAR and SRTM elevations; do not
   assume datum compatibility or automatic bias correction.
3. Build separately with one or more `--exclude` polygons. Check overlapping
   exclusions, partial intersections, holes, preserved AOI/buffer, identical
   code and rainfall footprints, area preservation in the full baseline and
   the difference between default analysis and `--analyse-exclusions`.
4. Confirm excluded upstream catchments create missing-flow interfaces that
   require **manual hydraulic treatment**. No inflow hydrograph or `2d_bc`
   is generated by this initialiser.
5. Inspect stream reaches, synthetic heads, actual whole-path longest flow
   distances, equal-area slopes, downstream topology and local catchment
   partitions in QGIS. Pay particular attention to negative gradients and
   nested/multiple AOIs. A nominal stream threshold does not delineate the
   complete upstream catchment.

## 3. Completion evidence

Retain the command lines, GIS source list and priority, GRASS/TUFLOW versions,
`qa.json`, `aoi_inflows.json`, `subcatchments.json`, representative screenshots
and any engineering discrepancies. Reference the resulting records in issue #3
for stream-reach/LFP validation, #4 for real terrain, and #5 for exclusions.

Do not mark these issues complete solely because the hosted CI workflow passed.
