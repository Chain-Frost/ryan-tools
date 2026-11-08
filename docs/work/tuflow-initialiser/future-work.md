> **Historical documentation migrated from [tuflow-initialiser](https://github.com/Chain-Frost/tuflow-initialiser).** The maintained implementation is now in `ryan_library/orchestrators/tuflow/initialiser` as a draft migration; validation and later architecture decisions are tracked in ryan-tools.

# Deferred features and ideas

Candidate **future** work (not in initial scope):

- Follow-flow downstream extension and floodplain-aware buffers rather than simple circular AOI buffering.
- `2d_bc` **outlet boundary only** generation.
- `2d_po` monitoring sections.
- Integration with separate `2d_mat` generation utilities, not duplicating material processing.
- QGIS Processing provider, interactive AOI editing and QGIS MCP interface.
- Caching hydrology rasters keyed by DEM, CRS, conditioning and processing engine versions.
- Additional hydrological terrain conditioning (breaching/burn-in, protected natural sinks) and fill QA.
- MFD/D-infinity comparisons for alluvial fans and distributary networks.
- Multiresolution/region-based processing for very large DEMs.
- More sophisticated stream-headwater main-path selection, and extended catchment metrics.
- Optional crossing analysis and model preflight checks.

**Explicitly excluded permanently:** model extent optimisation, automatic cell-size selection, ARR rainfall processing, automatic mapping/reports, `.trfc`, and `2d_sa`.

## Terrain input optimisation — [issue #6](https://github.com/Chain-Frost/tuflow-initialiser/issues/6)

Later, prepare TUFLOW terrain rasters clipped to the model footprint plus a configurable margin, and **trim extensive overlap among high-/low-priority sources** (e.g. LiDAR versus SRTM) to a small configurable seam overlap. Keep lower-priority data wherever it fills valid terrain gaps; do not modify original DEMs or the separate watershed-processing DEM. Benchmark TUFLOW processing/postprocessing time and validate raster coverage, resolution, precedence and NoData. Reuse the maintained lossless **`tuflow` GeoTIFF profile** and GDAL clipping helpers from `ryan-tools` where practical; using `ryan-tools` as an optional dependency is acceptable for this future feature. No implementation within PR #1.
