# GDAL Python wrappers

These scripts are editable user-facing wrappers around reusable functionality
installed with `ryan-tools`. They do not modify `sys.path` and therefore expect
the current `ryan-tools` wheel (or editable package) to be installed.

GeoTIFF output defaults to the conservative `tuflow` profile. Select the
`efficient` profile where smaller tiled ZSTD files are preferred. Raster
overviews are external `.ovr` files, and vector outputs default to GeoPackage
with Shapefile available as an option.

## Wrapper identity

Each wrapper contains a `WRAPPER_VERSION` in `YYYY-MM-DD.N` form and prints it
with the resolved path of the running file. That identity remains with a
wrapper copied into a job folder. The separately printed `ryan_functions`
version identifies the installed shared library that performs the work. Both
identities are printed at the start and end of processing, so the closing
details remain visible after lengthy output and beside the interactive pause.

New and updated wrappers follow the repository-wide
[`WRAPPER_STANDARD.md`](../WRAPPER_STANDARD.md).

## AI and automation discovery

[`gdal_cli_tools.json`](gdal_cli_tools.json) is the machine-readable catalogue
for AI assistants and automation. It lists every wrapper's relative location,
purpose, defaults, mutation risk, and argument arrays for common processing
scenarios. It describes only the current Python tools, without migration
history.

An AI should resolve each `script` path relative to the JSON file, prepend the
catalogue's `command_prefix`, replace values such as `{input_directory}`, and
include `--no-pause`. The wrapper's `help_arguments` can be executed whenever
the complete current CLI reference is needed.

Run any wrapper with `--help` for its supported options and examples.

Flags such as `--overwrite` permit replacement of existing outputs. Use them only after confirming that replacement is
intended; the catalogue's `mutation` field identifies tools that create, replace or modify files.

## Usage recipes

### Raster conversion and compression

```powershell
# Recursively convert FLT/ASC/RST beside each source.
python gdal_translate_TIF_ovr.py "D:\Terrain" --extensions flt asc rst

# Produce *_compress.tif, including from existing TIFF inputs.
python gdal_translate_TIF_ovr.py "D:\Terrain" --extensions flt asc rst tif tiff --output-suffix _compress

# Import XYZ files and create TIFFs under another output root.
python gdal_translate_TIF_ovr.py "D:\XYZ" --extensions xyz --output-directory "D:\GeoTIFF"
```

When `--output-directory` is used, source subdirectories are preserved below
the output root. This avoids filename collisions when identically named XYZ
files exist in different source subdirectories.

### Existing TIFF overviews

```powershell
# Create external overviews recursively.
python gdaladdo_tif_pyramids.py "D:\Model\Results"

# Rebuild existing .ovr sidecars.
python gdaladdo_tif_pyramids.py "D:\Model\Results" --refresh
```

### Flood extents

```powershell
# Recursively threshold band-1 TIFFs at 0.1 and create GPKG output.
python gdal_flood_extent.py "D:\Results" --patterns "*.tif" --recursive --cutoff 0.1

# Threshold band 4 of ECW files at 50 and create Shapefiles.
python gdal_flood_extent.py "D:\Imagery" --patterns "*.ecw" --recursive --input-band 4 --cutoff 50 --vector-format shp

# Remove regions smaller than eight connected pixels and retain raw masks.
python gdal_flood_extent.py "D:\Results" --patterns "*.tif" --recursive --cutoff 0.1 --sieve-pixels 8 --connectedness 8 --keep-intermediate-masks
```

### Mosaics

```powershell
# Merge using input folder, glob, and output basename.
python gdal_merge.py "D:\Tiles" "*.tif" Final_DEM

# Create an XYZ mosaic with assigned CRS and NoData metadata.
python gdal_merge.py "D:\XYZ" "*.xyz" Higginsville_DTM_1m_EPSG7851 --output-srs EPSG:7851 --nodata -9999

# Select XYZ tiles intersecting a vector extent.
python gdal_merge_by_extent.py "D:\XYZ" "D:\Extent\site.gpkg" --pattern "*.xyz" --nodata -9999
```

### Grouped TUFLOW result mosaics

`build_VRT.py` groups TUFLOW result TIFFs by filename fields, creates one GeoTIFF mosaic per group and builds external
overviews. Review representative filenames and the one-based field-removal rule before processing a full result set.

```powershell
python build_VRT.py "D:\Model\Results" --remove-field 2 --suffixes d_HR_Max h_HR_Max V_Max DEM_Z_HR
```

### Metadata, footprints, and point clouds

`gdal_set_nodata.py` edits NoData metadata in the source rasters in place. It does not recalculate or replace pixel
values; use copied inputs when the original metadata must be preserved.

```powershell
python gdal_set_nodata.py "D:\Terrain" --pattern "*.tif" --nodata -9999
python gdal_raster_footprint.py "D:\Rasters" --pattern "*.tif" --vector-format gpkg --recursive
python laz_to_las.py "D:\Classified_LAZ" --output-directory "D:\Classified_LAS"
```
