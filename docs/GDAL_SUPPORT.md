# GDAL and QGIS support policy

The maintained `ryan-tools` raster/GIS processing workflows call **GDAL through the Python `osgeo` bindings**. They do not require `gdalbuildvrt.exe`, `gdalwarp.exe`, or a QGIS command-line shell, and should not dynamically locate QGIS executables.

## Supported environment

- **QGIS 4** is the supported QGIS desktop/integration line.
- **QGIS 3 compatibility is out of scope** for new raster workflow development.
- Native Python execution is supported whenever the installed Python GDAL bindings provide the required GDAL features. An active QGIS GUI is not necessary.
- The installed `ryan_functions` package currently requires **Python 3.13+** (prefer Python 3.14). Some QGIS 4 distributions bundle an earlier Python version. QGIS 4/GDAL support **does not imply** that their bundled Python can install this wheel; run the wrapper using a supported Python interpreter with matching `osgeo` GDAL bindings.
- Do not assume `osgeo` is installed merely because `ryan_functions` is installed: confirm that the executing Python environment has compatible GDAL bindings.
- The maximum raster pipeline uses `gdal.BuildVRTOptions(pixelFunction="max")`, which requires **GDAL 3.12+**. Fail clearly when that API is unavailable; no legacy GDAL fallback is needed.

## Implementation principles

1. Reuse `ryan_library.functions.gdal.raster_processing` for GeoTIFF profiles, external overviews and VRT construction. Use `pixel_function="max"` when overlapping values must be reduced by a cell-wise maximum: a mosaic's last-source-wins behaviour is not equivalent.
2. Preserve CRS and raster geotransform semantics; known different CRSs are errors unless the workflow explicitly reprojects. Missing/local CRS assumptions must be authorised, never silently assigned to an unrelated EPSG code.
3. Use `Path.absolute()` for paths whose link semantics should be preserved. Avoid introducing `Path.resolve()` in new processing code.
4. Keep project-specific defaults in a thin wrapper and reusable algorithms under `ryan_library`.
5. GeoPackage zone sources used by the TUFLOW maximum pipeline must contain **exactly one vector layer**. No multi-layer selection feature is required.
6. Preserve existing project defaults for wrappers intended to replace copied scripts. Additional configuration options must not change a no-argument invocation.
7. Test decoded raster cells, validity masks, affine transforms, CRS handling and output publication separately from compression and overview sidecars. Raster statistics and `.aux.xml` sidecars are not substitutes for original source-cell data.

## Verification

Use Ruff, strict Pyright, the existing synthetic GDAL/raster tests and an end-to-end run under the supported QGIS 4/Python GDAL environment. CI without `osgeo` cannot verify actual GDAL operations; report such tests as skipped, not passed raster validation.
