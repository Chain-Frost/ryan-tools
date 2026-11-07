# TUFLOW initialiser implementation

This package coordinates multi-stage rainfall-on-grid TUFLOW initialisation and catchment delineation. It is under review in [PR #246](https://github.com/Chain-Frost/ryan-tools/pull/246), stacked on PR #204; migrated Windows/TUFLOW/GRASS acceptance is still outstanding.

## Module responsibilities

- `workflow.py` and `catchment_workflow.py`: coordinate full project creation and catchment-only processing, safe staging, validation and publication.
- `grass_runner.py`: run external GRASS hydrology and verify raster outputs.
- `catchments.py`, `subcatchments.py`, `geometry.py`: domain-specific catchment delineation and model-domain decisions.
- `gis_writer.py`, `catchment_outputs.py`, `subcatchment_outputs.py`: GIS output and reporting.
- `model_terrain.py`: insert TUFLOW terrain into generated projects. TGC terrain discovery, with active scenario selection, is a shared TUFLOW function.
- `project_setup.py`: initialiser-specific profile adapter for the maintained shared project generator.
- `verification.py`: reusable output-readback checks for real-system validation.
- `cli.py`: installed process entry point. Library workflow modules do not parse command-line options, pause or terminate the interpreter. Edit the top-level list/path/selection constants to supply defaults; explicit CLI flags, including `--no-*` Boolean overrides, take precedence. Inputs are converted to tuples at the workflow boundary.

## Shared processing functions

General-purpose algorithms and focused GIS helpers are maintained outside this orchestrator:

- `ryan_library/functions/hydrology/d8.py`: GRASS-convention D8 routing, catchment traversals and headwater tracing.
- `ryan_library/functions/hydrology/profiles.py`: stream-reach decomposition and equal-area slope calculations.
- `ryan_library/functions/gdal/terrain_mosaic.py`: priority-ordered DEM mosaics, valid-data masks, optional polygon clips and source provenance.
- `ryan_library/functions/tuflow/tgc_terrain.py`: extract active TGC terrain layers through PyTUFLOW.
- `ryan_library/functions/geometry/raster_coordinates.py`: affine pixel-centre conversion.

The shared TUFLOW text templates are maintained in `ryan_library/resources/tuflow_templates`. The initialiser's three-working-layer profile differs intentionally from the existing standard four-layer project setup.

## Entry points and acceptance

`tuflow-initialiser init|build|catchments` is registered in `pyproject.toml`. Focused tests are in `tests/orchestrators/tuflow/initialiser/`. Real Windows/TUFLOW/GRASS verification is exercised through `repo-scripts/tuflow-initialiser/validate_test_project.py`.

See the [migration and Windows verification record](../../../../docs/work/tuflow-initialiser/README.md) and [development guide](../../../../docs/DEVELOPMENT_GUIDE.md) before changes. Issues #242–#245 remain engineering acceptance gates.
