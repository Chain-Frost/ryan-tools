# Spatial IFD rainfall grids

| Field | Value |
| --- | --- |
| Status | Draft foundation; production schema and source equivalence blocked |
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
- GeoTIFF output, polygon masks and area-weighted mean/extraction, multi-event convenience and a maintained wrapper/MCP catalogue entry are follow-ups after source admission. ASCII is geographic WGS84 and does not embed CRS; assign the appropriate CRS on import after datum verification, never treat degrees as metres.

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
4. Implement GeoTIFF with CRS and provenance, optional catchment geometry area weights and maintained entry point in accordance with wrapper and no-clobber standards. Reconcile stable API with #350; do not modify its branch. Assess MCP catalogue when the interface is stable.
5. Run focused tests, Ruff format/lint, strict modified-file Pyright and package/installed-wheel verification per repository policy (Python 3.11 PR CI and Windows Python 3.14 primary acceptance). No extra CI matrix or release/merge authorised.

## Validation and delivery

- Synthetic regression tests included for restart, overlapping extent/event reuse, budget, snapshot separation, malformed/incorrect data, immutable cache, exact event lookup, raster orientation via rasterio and no overwrite.
- Container could not resolve GitHub and had no local checkout or repo-pinned Ruff/Pyright; no source or wheel tests have been executed against this GitHub revision in this session. Hosted checks and local Windows verification remain required. Do not treat tests added as tests passed.
- Draft PR is the implementation checkpoint. Update this record with exact-head evidence before marking ready. No merge or release.

## Progress

### 2026-11-28

Established independent cache/grid foundation and synthetic test suite; production API schema, bulk access permission, source equivalence and full GIS workflow remain open. Existing rainfall-loss screening record is overdue; not part of this scope.
