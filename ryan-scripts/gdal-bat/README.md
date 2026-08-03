# Legacy GDAL batch files

The batch files in this directory are obsolete and scheduled for deletion.
They depended on locally installed QGIS, OSGeo4W, GDAL or PDAL executables and
were replaced by maintained Python wrappers under `../gdal-python`.

Do not use these BAT files for new processing. They remain temporarily so the
old task variants can be identified during cleanup.

| Legacy batch files | Current Python wrapper |
| --- | --- |
| `gdal_translate_TIF_ovr_PATH.bat`, `gdal_translate_TIF_ovr_PATH_rename.bat`, `gdal_translate_TIF_ovr_PATH_xyz.bat`, `gdal_translate_TIF_ovr_v8.bat`, `gdal_translate_TIF_ovr_v9.bat` | `../gdal-python/gdal_translate_TIF_ovr.py` |
| `gdaladdo_Pyramids_deflate_v4.bat`, `gdaladdo_Pyramids_deflate_v5.bat` | `../gdal-python/gdaladdo_tif_pyramids.py` |
| `gdal_flood_extent_sieve.bat`, `gdal_flood_extent_v7.bat`, `gdal_flood_extent_v8-not working.bat`, `gdal_flood_extent_v9.bat`, `gdal_flood_extent_v9-band4.bat`, `gdal_flood_extent_v9-old.bat` | `../gdal-python/gdal_flood_extent.py` |
| `gdal_merge.bat`, `gdal_merge_CLI.bat` | `../gdal-python/gdal_merge.py` |
| `gdal_merge_xyz_extent.bat` | `../gdal-python/gdal_merge_by_extent.py` |
| `gdal_edit_Set_nodata.bat` | `../gdal-python/gdal_set_nodata.py` |
| `gdal_raster_footprint.bat` | `../gdal-python/gdal_raster_footprint.py` |
| `LAZ-to-LAS.bat` | `../gdal-python/laz_to_las.py` |

The Python wrapper catalogue for AI and automation is
`../gdal-python/gdal_cli_tools.json`. It intentionally contains only current
Python tasks and no migration history.
