# TUFLOW initialiser implementation

This package coordinates multi-stage rainfall-on-grid TUFLOW initialisation and catchment delineation. Migration [PR #246](https://github.com/Chain-Frost/ryan-tools/pull/246) is incorporated into merged [PR #204](https://github.com/Chain-Frost/ryan-tools/pull/204). Recorded Windows/GRASS/TUFLOW migration parity matched five hydrology rasters and 107 paired GeoPackage layers. Representative engineering acceptance remains separately tracked in the [development framework (#312)](https://github.com/Chain-Frost/ryan-tools/issues/312); [#247](https://github.com/Chain-Frost/ryan-tools/issues/247) owns source-repository retirement.

## Module responsibilities

- `workflow.py` and `catchment_workflow.py`: coordinate full project creation and catchment-only processing, safe staging, validation and publication.
- `grass_runner.py`: run external GRASS hydrology and verify raster outputs.
- `catchments.py`, `subcatchments.py`, `geometry.py`: domain-specific catchment delineation and model-domain decisions.
- `gis_writer.py`, `catchment_outputs.py`, `subcatchment_outputs.py`: GIS output and reporting.
- `model_terrain.py`: insert TUFLOW terrain into generated projects. TGC terrain discovery, with active scenario selection, is a shared TUFLOW function.
- `project_setup.py`: adapter for the maintained shared project generator.
- `verification.py`: reusable output-readback checks for real-system validation.
- `cli.py`: installed process entry point. Library workflow modules do not parse command-line options, pause or terminate the interpreter. Edit the top-level list/path/selection constants to supply defaults; explicit CLI flags, including `--no-*` Boolean overrides, take precedence. Inputs are converted to tuples at the workflow boundary.

## Shared processing functions

General-purpose algorithms and focused GIS helpers are maintained outside this orchestrator:

- `ryan_library/functions/hydrology/d8.py`: GRASS-convention D8 routing, catchment traversals and headwater tracing.
- `ryan_library/functions/hydrology/profiles.py`: stream-reach decomposition and equal-area slope calculations.
- `ryan_library/functions/gdal/terrain_mosaic.py`: priority-ordered DEM mosaics, valid-data masks, optional polygon clips and source provenance.
- `ryan_library/functions/tuflow/tgc_terrain.py`: extract active TGC terrain layers through PyTUFLOW.
- `ryan_library/functions/geometry/raster_coordinates.py`: affine pixel-centre conversion.

The shared TUFLOW text templates are maintained in `ryan_library/resources/tuflow_templates`. Both entry points create the same four starter working layers, active boundary read, QGIS style and default utilities. The full initialiser build then populates code, location and rainfall GIS and adds terrain and hydrology products; boundary GIS remains empty until configured.

## Entry points and acceptance

`tuflow-initialiser init|build|catchments` is registered in `pyproject.toml`. Focused tests are in `tests/orchestrators/tuflow/initialiser/`. Real Windows/TUFLOW/GRASS verification is exercised through `repo-scripts/tuflow-initialiser/validate_test_project.py`.

See the [living initialiser and Windows verification record](../../../../docs/work/tuflow-initialiser/README.md) and [development guide](../../../../docs/DEVELOPMENT_GUIDE.md) before changes. The living record documents access to the original checksummed fixture at immutable source baseline `33ac5fe`; the historical source package is not a production runtime dependency. Issues #242–#245 retain their implementation/engineering requirements and #266 retains the representative mixed-CRS deferral under #312. Source archival does not complete those requirements.
