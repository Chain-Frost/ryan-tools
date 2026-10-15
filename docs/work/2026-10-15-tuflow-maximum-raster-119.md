# TUFLOW maximum raster pipeline (PR #119)

**Status:** Implementation in progress; not ready to merge or deploy.
**Owner:** ChatGPT / maintainer review
**Updated:** 2026-10-15
**Next review:** 2026-10-16
**Delivery:** [PR #119](https://github.com/Chain-Frost/ryan-tools/pull/119); original [PR #112](https://github.com/Chain-Frost/ryan-tools/pull/112) closed without merging as superseded.

## Scope and decisions

- Preserve the original no-argument, project-specific RTIO defaults as a drop-in replacement: directory layout, versions, AEPs, grid types, output filename template, GeoPackages, FG01+FG02 and FG03 rules, overview and report defaults.
- Exactly one vector layer per GeoPackage. No QGIS 3 compatibility, no GDAL executable invocation; GDAL 3.12+ through Python `osgeo` in the QGIS 4 environment.
- Generalise output zone groupings, source roots, output templates and filters without silently changing raster values.
- Use `Path.absolute()` for normal filesystem paths, but pass GDAL `/vsi...` identifiers unchanged.
- Known conflicting raster CRSs must fail; legacy `LOCAL_CS["GDA94_MG"]` uses explicit default `EPSG:28350` and can be overridden.

## Implemented on #119

- Extracted reusable discovery, numerical GDAL functions, per-job execution and report generation into `ryan_library/orchestrators/tuflow/maximum_rasters.py`.
- Retained a small project wrapper at `ryan-scripts/TUFLOW-python/raster_processing/tuflow_maximum_pipeline.py`.
- Reused existing `build_vrt`, GeoTIFF compression options, overviews and TUFLOW naming utilities.
- Added configurable groups, GeoPackages, zone attribute, roots, versions, AEPs, grid types and filename template.
- Corrected GDAL VSI path handling on Windows; reject output filename collisions before processing.
- Replaced implicit zone-CRS inheritance for local raster labels with explicit EPSG:28350 default; changed `ALGORITHM_ID` to invalidate prior unsafe incremental signatures.
- Added rollback for ordinary GeoTIFF/overview rename failures (power-loss atomicity is not guaranteed).
- Added synthetic tests for default naming, generic groups, missing FG inputs, GDAL pixel maxima, CRS errors, output collisions, VSI handling and rollback.
- Added `docs/GDAL_SUPPORT.md` and `docs/TUFLOW_MAXIMUM_PIPELINE.md` and indexed them.

## Remaining merge gates

1. **CI:** Ruff, strict Pyright, focused tests and package checks at the latest head. Confirm the hosted GDAL tests are actually executed; tests skipped for absent `osgeo` do not establish numerical correctness.
2. **Installed-wheel compatibility:** the copied wrapper requires a rebuilt `ryan_functions` package containing the new `build_vrt` API and orchestrator. Bump/build/verify the retained wheel with `repo-scripts/build_library.py` on a supported machine; do not distribute the copied wrapper with an older wheel.
3. **Real-project validation:** run the script under QGIS 4 Python GDAL 3.12+ against representative v06/v07 RTIO results and both GeoPackages. Compare cell values, validity masks, affine transforms, CRS, output paths, overview levels and report sheets with independent expected outputs.
4. **Alignment policy:** validate whether nearest-neighbour realignment used by the original script is appropriate for all composite sources; add a warning or strict option if needed.
5. **Incremental metadata:** existing source signature uses path/size/mtime, not a content hash. Check unchanged-source and changed-source reruns on the actual results.
6. **Publication:** test interruption/recovery on the target Windows/network filesystem. The two-file TIFF/overview replacement has compensating rollback, but cannot guarantee power-loss atomicity.
7. **Documentation:** verify all links, CLI help and logging/wrapper standard compliance, then update the PR notes.

## Validation evidence

- Initial CI runs highlighted imported/format and architecture issues; subsequent commits addressed those and extracted the orchestrator.
- The Python GDAL environment and proprietary RTIO project data are unavailable in this connected session. No claim of production pixel parity or packaged-wheel readiness is made.

**Next action:** Review latest PR head CI, fix deterministic failures, then complete real GDAL raster verification and rebuild the installed package.
