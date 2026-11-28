# Spatial IFD rainfall grids

| Field | Value |
| --- | --- |
| Status | Draft implementation; authentic source and scientific acceptance blocked |
| Owner | Unassigned |
| Created | 2026-11-28 |
| Updated | 2026-11-28 |
| Next review | 2026-12-12 or receipt of reference data |
| Baseline | main 5eda4f06cafbbdc5f528bb108909438302a13ce5; branch feat/spatial-ifd-grid-cache |

## Outcome and scope

Provide reusable point-sampled rainfall-depth and relative-adjustment grids for hydrology workflows. Keep independent of nested storm PR [#350](https://github.com/Chain-Frost/ryan-tools/pull/350). No automatic ARF, climate adjustment, loss or RORB processing is performed.

The [ARR Data Hub API](https://data.arr-software.org/about) documents point GET requests with `type=json&AllIFDDatasets=1` and attributes BoM IFD material to the Commonwealth of Australia 2016, Bureau of Meteorology, CC BY 4.0. Preserve this attribution with derived outputs after confirming source identity. Recommended `BoMIFD` results can be regional or baseline-adjusted, so never substitute them for a BoM raster.

The existing `pytuflow.arr.api_client.ArrApiResponse.ifd_table` implementation on [PyTUFLOW master](https://github.com/TUFLOW-Support/PyTuflow/blob/master/pytuflow/arr/api_client.py) selects `RecIFD` by baseline and is not an unadjusted BoM source selector. Its recorded response fixtures establish the split-table structure (`index`, `columns`, `data`) and coordinate fields, but do not establish the exact AllIFDDatasets response path or its units independently. The released PyTUFLOW wheel may not contain that code. Use the narrowly scoped standard-library HTTPS transport and existing NumPy/rasterio rather than copying the ARR engine or adding dependencies.

## Current state

- `ryan_library.functions.hydrology.spatial_ifd`: explicit decimal-degree north-west cell centres, size/spacing, validated split-format tables, immutable SQLite raw responses, SHA-256, UTC retrieval timestamps, exact table paths, snapshot names, serial bounded network access, default cache-only mode, explicit new-point budget, restart and overlapping extent reuse, exact event lookup, depth and multiplier arrays, exclusive-create ESRI ASCII export.
- Cache identity is endpoint + canonical coordinate/request parameters + operator snapshot. All returned AEPs/durations are retained in the raw payload; no TTL or silent overwrite. The snapshot is a local namespace, not an upstream version. Re-run with the same snapshot after interruption; choose a new snapshot for a deliberate refresh. Cache corruption, missing source or event and coordinate mismatches fail closed. Use a local filesystem SQLite database and one downloader process per cache. Do not distribute raw response archives without checking data rights and project privacy.
- No production requests made. The live response was inaccessible in this environment. AllIFDDatasets table keys, real schema, units, source revision consistency, grid registration and numerical equivalence are not verified. The test keys are artificial.
- Do not launch large API batches without confirming the service's acceptable request rate and bulk-access options. The budget limits new coordinates, not the number of HTTP retries. Prefer a licensed authoritative raster if available.
- Added GeoTIFF output with embedded EPSG:4326 and provenance tags; staged hard-link publication is atomic/no-clobber and fails closed on filesystems without hard-link support. Added single-band reference-raster import, exact grid registration comparison, explicit NoData reporting and error metrics. No automatic resampling or CRS conversion; missing CRS requires explicit WGS84 confirmation. Reference transform coordinates are represented to ten decimal places to remove binary floating-point noise.
- Added physical area-weighted mean using caller-provided areas (m²), plus WGS84 polygon/cell intersections and normalization via `spatial_ifd_catchments.catchment_weights`. Polygon holes, multipart and fractional cells use Shapely and ellipsoidal WGS84 PyProj areas. Approximate straight lon/lat boundaries with geodesic segments no longer than 0.001 degrees; require independent total-area closure at 1e-6 relative and complete catchment coverage. These are numerical controls, not scientific BoM parity tolerances. Input must already be WGS84. Vector layer import validates CRS and unions all valid polygon features; explicit layer selection is required for multi-layer files. Automatic reprojection and polygon-driven grid masking are not included. ASCII does not embed a CRS. Assign the appropriate datum on import after verifying it; never treat degrees as metres.
- Added experimental repository-only wrapper `ryan-scripts/hydrology-python/generate_spatial_ifd.py` with editable defaults, reference or explicit grid, explicit source keys, immutable cache namespace and offline-by-default behaviour. Multi-event extraction accepts duration/AEP lists (Cartesian product); use an existing output directory for multiple GeoTIFFs. A single reference raster is event-specific. Use `--catchment path.gpkg --catchment-layer name` to normalize each event to its own catchment mean (mutually exclusive with a fixed reference depth); multipliers cover the whole requested grid, not just the polygon. GeoTIFF includes reference depth and catchment area metadata. Cache inspection validates exact selected source/event tables and reports missing coordinates without fetching. Batch extraction checks all existing responses and the missing-point budget before the first new request, reads each cached point once, and commits each successful new point. Do not add to the installed/MCP catalogue before authentic source qualification and wrapper acceptance.

## Example (offline-first Python API)

```python
from decimal import Decimal
from pathlib import Path

from ryan_library.functions.hydrology.spatial_ifd import (
    GridSpec, IFDSource, PointCache, collect_grid, write_ascii_grid,
)

# Illustrative coordinates and dimensions. Establish the actual grid origin
# and registration from the reference BoM raster; do not assume alignment.
spec = GridSpec(Decimal("-23.0"), Decimal("119.0"), 2, 2)
# Replace these placeholders using a genuine response and verified source.
source = IFDSource(("VERIFIED_LAYER", "VERIFIED_UNADJUSTED_TABLE"), "project-source-v1")
cache = PointCache(Path.home() / ".ryan-tools" / "spatial-ifd" / "cache.sqlite3")
# Default max_new_points=0 is offline. After service and schema approval,
# explicitly set a small positive new-point budget to retrieve missing cells.
result = collect_grid(spec, source, cache, duration_min=60, aep_percent=1)
write_ascii_grid(Path("depth_1pct_60min.asc"), spec, result.depths_mm)
write_ascii_grid(Path("factor_1pct_60min.asc"), spec, result.multipliers(100.0))
```

The reference depth of 100 mm above is illustrative, not a catchment mean. For a catchment-normalized multiplier, calculate a physically area-weighted mean over the identical valid footprint first; an unweighted geographic raster mean is not equivalent.

## Next action and acceptance

1. Local agent: obtain one genuine point response using the documented AllIFDDatasets query (small authorised request); retain original bytes/hash, request URL/coordinates, response metadata/revision, exact layer/table path and units. Identify whether a bulk authoritative raster is available and confirm acceptable usage before any larger batch. Verify baseline and source identity explicitly; do not use default/adjusted tables. Update parser/fixture only using evidenced schema; allow explicit missing/NoData policy only after verifying semantics.
2. Owner to provide a small BoM ASCII raster (5×5 to 10×10 preferred) and AEP, duration, datum/CRS, units, issue/version, extent/registration metadata when convenient. No user data is required now. Retain source privately unless publication is authorised.
3. Derive exact sampling coordinates from that raster, compare point response values, axes and NoData mask; report max absolute/relative difference, MAE, RMSE and rounding/quantisation effects. Declare numerical tolerance based on source precision before comparison, and check multipliers against a common reference. Do not declare parity unless evidence supports it.
4. Validate GeoTIFF, batch and the reference-driven wrapper on Windows, check polygon weights on authentic catchment geometry, and consider catalogue/installed-script promotion once qualified. Reconcile stable API with #350; do not modify its branch.
5. Run focused tests, Ruff format/lint, strict modified-file Pyright and package/installed-wheel verification per repository policy (Python 3.11 PR CI and Windows Python 3.14 primary acceptance). No extra CI matrix or release/merge authorised.

## Validation and delivery

- Synthetic regression tests included for restart, overlapping extent/event reuse, budget, snapshot separation, malformed/incorrect data, immutable cache, exact event lookup, raster orientation via rasterio and no overwrite.
- Container could not resolve GitHub and had no local checkout or repo-pinned Ruff/Pyright; no source or wheel tests have been executed against this GitHub revision in this session. Hosted checks and local Windows verification remain required. Do not treat tests added as tests passed.
- Draft PR is the implementation checkpoint. Update this record with exact-head evidence before marking ready. No merge or release.

## Validation and wrapper usage update (2026-11-28)

- First PR head `7de6950` failed [Ruff and whitespace](https://github.com/Chain-Frost/ryan-tools/actions/runs/46140063698/job/140963819391) on 11 deterministic findings; policy failed consequently and tests were skipped. Corrected those findings in the next batch. Head `b59428f` stopped at the installed-script inventory documentation guard: 163 tracked paths, 84 selected and 79 excluded. Updated the two index/register counts while retaining repository-only status. Head `dc2a05b` passed Ruff/whitespace and POSIX wrapper security; remaining jobs were running when the next feature batch was prepared. New-head results remain pending.
- An isolated Python 3.13.5 Linux prototype with a stub of the grid dataclass interface passed synthetic Rasterio GeoTIFF roundtrip, no-clobber, multiplier tags/values, physical weighted mean and NoData/error-metric checks. This is not a full repository or exact-head validation. The container cannot resolve GitHub or the Data Hub; direct `git ls-remote` and DNS checks failed. The documented point-query URL could not be retrieved using web access either. No authentic response was admitted.
- CLI example (only after verifying dataset keys and datum; no network by default):

```powershell
python ryan-scripts/hydrology-python/generate_spatial_ifd.py --reference bom.asc --assume-wgs84 --source-path VERIFIED_LAYER VERIFIED_TABLE --snapshot reference-v1 --duration-min 60 --aep-percent 1 --output candidate.tif --no-pause
```

The command assumes an existing cache. For a genuinely admitted, service-approved point batch, explicitly provide a small `--max-new-points` budget. For unknown source keys, inspect one raw genuine response rather than guessing. No parity assertion is emitted by the comparison.

## Batch and catchment API examples

```python
from ryan_library.functions.hydrology.spatial_ifd import IFDEvent, collect_grids, inspect_cache
from ryan_library.functions.hydrology.spatial_ifd_catchments import catchment_weights

events = (IFDEvent(60, 1), IFDEvent(120, 1), IFDEvent(120, 10))
coverage = inspect_cache(spec, source, cache, events)
grids = collect_grids(spec, source, cache, events)  # cache-only by default
# 'polygon' is a valid WGS84 Shapely Polygon or MultiPolygon, fully inside spec.
weights = catchment_weights(spec, polygon)
mean_mm = weights.mean_depth(grids[0])
factors = weights.multipliers(grids[0])
```

For multiple events, supply e.g. `--duration-min 60 120 --aep-percent 1 10 --output existing_directory`. The batch returns every requested event only on complete acquisition; each GeoTIFF is published independently and an export failure can leave already published complete files. Never overwrite an existing file. `--inspect-cache` is network-free regardless of the supplied request budget. It does create the cache database if absent. No output is written in that mode. Use a new cache snapshot for intentional refresh. A snapshot does not guarantee uniform server revisions.

## Progress

### 2026-11-28

Established independent cache/grid foundation and synthetic test suite; production API schema, bulk access permission, source equivalence and full GIS workflow remain open. Existing rainfall-loss screening record is overdue; not part of this scope.

- Added multi-event collection, validated cache diagnostics and ellipsoidal catchment weighting. The isolated Python 3.13.5 numerical probe of a fractional rectangle and triangle found relative area-closure errors below 2e-9 using Shapely 2.1.2/PyProj 3.7.2. New regression tests are written. Head `dc2a05b` policy failed on one Pyright unknown-variable diagnostic in JSON mapping; other running jobs were cancelled by the new push. Head `dae814f` lint reported SIM102; corrected both in the following batch. Head `0449522` passed Ruff lint but failed formatting of three files; applied the exact formatter diff. Exact-head CI remains to be verified. Authentic source and owner BoM comparison are still required.
