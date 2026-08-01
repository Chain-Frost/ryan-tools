# GDAL Python wrappers

These scripts are editable user-facing wrappers around reusable functionality
installed with `ryan-tools`. They do not modify `sys.path` and therefore expect
the current `ryan-tools` wheel (or editable package) to be installed.

GeoTIFF output defaults to the conservative `tuflow` profile. Select the
`efficient` profile where smaller tiled ZSTD files are preferred. Raster
overviews are external `.ovr` files, and vector outputs default to GeoPackage
with Shapefile available as an option.

## Batch-file replacements

| Historical BAT behavior | Python wrapper |
| --- | --- |
| `gdal_translate*` format, compression, output-folder, and rename variants | `gdal_translate_TIF_ovr.py` |
| `gdaladdo*` overview variants | `gdaladdo_tif_pyramids.py` |
| `gdal_FloodExtent*`, band-4, and sieve variants | `GDAL_Flood_Extent.py` |
| `Build_*_VRT` grouped TUFLOW mosaics | `build_VRT.py` |
| `gdal_merge.bat` and `gdal_merge_CLI.bat` | `gdal_merge.py` |
| `gdal_merge_xyz_extent.bat` | `gdal_merge_by_extent.py` |
| `gdal_edit_Set_nodata.bat` | `gdal_set_nodata.py` |
| `gdal_raster_footprint.bat` | `gdal_raster_footprint.py` |
| `LAZ-to-LAS.bat` | `laz_to_las.py` |

Run any wrapper with `--help` for its supported options and examples. The BAT
files remain useful as historical references during migration, but new work
should use these unversioned Python wrappers.
