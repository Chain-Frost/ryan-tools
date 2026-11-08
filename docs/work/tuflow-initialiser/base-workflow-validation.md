> **Historical documentation migrated from [tuflow-initialiser](https://github.com/Chain-Frost/tuflow-initialiser).** The maintained implementation is now in `ryan_library/orchestrators/tuflow/initialiser` as a draft migration; validation and later architecture decisions are tracked in ryan-tools.

# Base-workflow validation

## Reproduce on another machine

Use Python 3.14 with the GIS dependencies described in the repository README,
plus installed GRASS GIS and TUFLOW. From the repository root:

```powershell
$env:PYTHONPATH = 'src'
python scripts/validate_test_project.py `
  --grass-exe 'C:\OSGeo4W\bin\grass85.bat' `
  --tuflow-exe 'C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe' `
  --output 'validation/base-run/project'
```

Replace executable paths with those installed on that machine. Choose a new
output path for each run; existing projects and projection files are protected.
The runner verifies the [shared fixture checksums](../test_data/manifest.json),
derives a projection file from the tracked GeoTIFF, builds the project and reopens
its GIS/raster outputs for QA. Machine-readable evidence is saved as
`<output>/qa.json`. Generated projects, projections and logs remain ignored.

The fixed settings are both AOIs, EPSG:28350, one terrain input, scenario EXG,
rainfall name RF01, 500 m buffer, 100,000 m² stream threshold, no exclusions and
no depression filling. This is a manual real-system integration check, separate
from ordinary CI tests. It does not require QGIS or the original ASCII DEM.

## Retained result: 11 October 2026

Original software base: `11cb324`; Windows Python 3.14.6, GRASS 8.5 and TUFLOW
2026.3. Inputs are now tracked under `test_data/`. The original run used the same
CRS-assigned GeoTIFF and both AOIs. Local artifacts are retained under
`validation/local-2026-10-11/` and are not committed.

| Output | Observed result |
| --- | --- |
| Analysis grid | 761 x 824 cells, 28.59971091 m resolution |
| Full contributing area | 102.444147 km² |
| Active code/rainfall footprint | 106.016769 km² |
| Location envelope | 189.645302 km² |
| Stream paths | 533 channels, 272 synthetic headwaters |
| Negative equal-area slopes | 11 paths requiring profile/terrain review |

Reopened checks passed: canonical schemas from actual TUFLOW empties, EPSG:28350,
valid populated polygons, code/rainfall attributes, identical code/rainfall
footprints, location and AOI coverage, finite active elevations without NoData,
and valid stream geometry with positive lengths and finite slopes. Canonical
rainfall `f1`/`f2` fields are read using the SQLite schema helper because Fiona
omits these untyped fields in the empty layer.

An isolated QGIS process loaded all review layers and saved a local review
project. This confirms readability; expected drainage divides still need visual
review. The original source suite passed 24 tests after correcting two Affine
composition expressions. Compilation passed. Separate broad quality checks
reported 120 Ruff findings and 420 Pyright errors; existing CI does not run those
checks. These are historical counts from that run, not a merge-readiness claim.

The tracked runner was subsequently exercised from the shared fixture with the
same installed tools. Build plus reopened-output QA completed in 8.71 seconds
and reproduced the areas, grid and all 805 stream paths above. This single small
run is not a regional performance benchmark. New scripts passed Ruff checks and
format checks; the source test suite again passed all 24 tests.

## Validation boundary

The later [issue #7 quality validation](issue-7-quality-validation.md) records
clean Ruff/Pyright checks and exact real-system comparisons after the typing and
lint repairs. Negative stream slopes remain available by reach ID in `qa.json`.

TUFLOW generated authoritative empty GIS layers; it did not run a hydraulic
simulation or check the populated model. Rainfall series, boundary conditions
and simulation settings remain modeller work. Multiple terrain datasets,
exclusions, vertical datum consistency, detailed subcatchment attributes,
regional runtime/memory and installed-wheel behaviour remain unverified.
See [fixture provenance](../test_data/README.md) and [validation requirements](validation.md).
