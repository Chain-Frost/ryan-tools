# TUFLOW initialiser in ryan-tools

The standalone [tuflow-initialiser repository](https://github.com/Chain-Frost/tuflow-initialiser)
is being consolidated into `ryan-tools`. The implementation, hydrology/D8
analysis, GRASS orchestration, TGC terrain reading, GIS outputs, tests and
canonical model templates are now proposed under
`ryan_library/orchestrators/tuflow/initialiser/`.

**Status:** migration [PR #246](https://github.com/Chain-Frost/ryan-tools/pull/246)
is a draft stacked on [terrain cropping PR #204](https://github.com/Chain-Frost/ryan-tools/pull/204).
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
installed TUFLOW and GRASS for the real workflows. The standalone CLI supports
explicit terrain rasters or a PyTUFLOW-parsed TGC for catchments. Its `Read GIS
Zpts` inputs are warned about but not rasterised into the hydrology DEM.

Project creation now delegates to the maintained
`ryan_library.orchestrators.tuflow.project_setup` implementation. The
initialiser retains its three-layer default, explicitly configured optional
utility source, and no QML style copy. The existing project setup retains its
four-layer default, automatic utility fallback, and QML style handling.
Regression tests cover both profiles without running TUFLOW.

The `crop_tgc_terrains.py` script remains a separate terrain-preparation
entry point managed by [PR #204](https://github.com/Chain-Frost/ryan-tools/pull/204),
with opt-in raster writes and a planning-grid QA GeoPackage. It is **not** a
replacement for the complete upstream DEM required by hydrological processing.

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

## Source and history

- [Original repository](https://github.com/Chain-Frost/tuflow-initialiser)
- [Original main baseline after PR #11](https://github.com/Chain-Frost/tuflow-initialiser/commit/33ac5fe90a1c92e775427bcfa02f50c14dcadc34)
- [Migration PR #246](https://github.com/Chain-Frost/ryan-tools/pull/246)
