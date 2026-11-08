> **Historical documentation migrated from [tuflow-initialiser](https://github.com/Chain-Frost/tuflow-initialiser).** The maintained implementation is now in `ryan_library/orchestrators/tuflow/initialiser` as a draft migration; validation and later architecture decisions are tracked in ryan-tools.

# Issue #7: code-quality baseline

Date: 11 October 2026. Scope: [issue #7](https://github.com/Chain-Frost/tuflow-initialiser/issues/7).
Local implementation and validation are complete; no commit, push, hosted CI run,
issue update or merge was performed as part of this work.

## Changes and responsibility mapping

- Ruff formatting, imports, exception-message variables, function annotations
  and explicit nullable geometry/CRS handling replace the lint/type failures.
- `gis_types.py` defines concrete layer schemas and raster profiles. Reviewed,
  project-scoped stubs in `typings/` describe the supported Fiona/Rasterio/Affine
  API calls. Shapely uses a pinned typeshed development dependency. Strict Pyright
  checks package, tests, scripts and the local declarations; no global diagnostic
  suppression or broad `Any` casts were added. Existing inline Pyright ignores
  in project setup were removed.
- `compose_terrain` delegates source validation and extent transformation to
  `_validated_bounds`; mosaic priority, alpha coverage, resampling, grid alignment
  and blockwise writing retain their existing responsibilities and calculations.
- The private canonical schema reader is now public `read_empty_schema`, shared
  by project creation and QA. It still reads authoritative empty-layer fields
  from SQLite and rejects populated canonical layers.
- The unused `_resolve_utility_source_root` function and its private fallback
  root constant were removed. Utility copying still uses the explicitly supplied
  `utility_source_root` in `_copy_project_utilities`; there was no active caller
  of the deleted fallback.
- GIS polygon splitting now narrows actual Polygon/MultiPolygon classes.
  `feature_geometry` rejects null geometry before decoding a feature.
- The validation script uses explicit exceptions instead of assertions, so
  checks remain active under `python -O`. Typed QA records include IDs, types,
  lengths and signed slopes for negative-slope reaches. Numerical profile
  integration and flow-routing formulas were not changed.

## Final local checks

| Check | Result |
| --- | --- |
| `python -m ruff check .` | Passed, pinned Ruff 0.16.6 |
| `python -m ruff format --check .` | Passed |
| `python -m pyright` | 0 errors, 0 warnings, strict |
| `PYTHONPATH=src; python -m pytest tests -q -ra` | 29 passed; no skips/deselections |
| `python -m compileall -q src tests scripts` | Passed |
| `python -m tuflow_initialiser.cli --help` | Passed |

The five added test cases cover optimisation-resistant validation, missing
project failure, both signs of stream slope, and null stream geometry rejection.
Existing CI already runs Ruff, formatting and Pyright with fail-on-error steps;
this work repairs the code checked by those steps. Hosted results remain pending
publication of the changes.

## Real-system comparison

The tracked GeoTIFF and both AOIs were rerun using GRASS 8.5 and TUFLOW 2026.3
with the documented base-workflow defaults. Fixture SHA-256 hashes passed.
Build plus reopened-output QA took 6.73 seconds on this machine; this is not a
regional performance benchmark.

Local artifacts (ignored): `validation/issue7-real/project/qa.json`,
`validation/issue7-real.log`, and `validation/issue7-comparison.json`.
Baseline: `validation/local-2026-10-11/project/`.

- All five analysis rasters have exact grid/CRS/pixel equality: analysis DEM,
  source priority index, D8 direction, upstream accumulation and hydrology DEM.
- All three populated TUFLOW layers have exact schema/attribute/geometry equality.
- All 805 stream paths have exact ID, type, geometry, length and signed-slope
  equality, including all **11 negative slopes**. No reaches were discarded,
  clipped or made positive to pass quality checks.
- Full contributing area remains 102.444147 km²; active code/rainfall footprint
  remains 106.016769 km²; grid remains 761 x 824 at 28.59971091 m resolution.

TUFLOW generated canonical empty GIS schemas; no hydraulic simulation or TUFLOW
check of populated models was run. Negative profiles still require engineering
review. Multi-terrain/exclusion and regional-scale validation remain separate
work, as described in [base-workflow validation](base-workflow-validation.md).
