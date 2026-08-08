# Legacy GDAL batch files

The batch files in this directory are legacy, environment-specific entry points. They depend on locally installed
QGIS, OSGeo4W, GDAL or PDAL executables and have maintained Python replacements under `../gdal-python`.

Do not select these BAT files for new processing. They remain only for existing jobs and migration; review their
hard-coded paths, environment detection and overwrite behavior before running them.

## Remaining files and replacements

| Remaining batch file | Maintained Python wrapper | Migration note |
| --- | --- | --- |
| `gdal_edit_Set_nodata.bat` | `../gdal-python/gdal_set_nodata.py` | Both change raster NoData metadata in place. |
| `gdal_flood_extent_sieve.bat` | `../gdal-python/gdal_flood_extent.py` | Use `--sieve-pixels` and review cutoff, connectedness and output format. |
| `gdal_merge.bat` | `../gdal-python/gdal_merge.py` | Review the BAT file's local QGIS/OSGeo4W environment paths before reproducing an old job. |
| `gdal_merge_CLI.bat` | `../gdal-python/gdal_merge.py` | The Python wrapper provides the maintained folder, glob and output-name interface. |
| `gdal_merge_xyz_extent.bat` | `../gdal-python/gdal_merge_by_extent.py` | The BAT file contains project-specific XYZ filename assumptions; configure the Python wrapper explicitly. |
| `gdal_raster_footprint.bat` | `../gdal-python/gdal_raster_footprint.py` | Review pattern, recursion and vector format when migrating. |
| `LAZ-to-LAS.bat` | `../gdal-python/laz_to_las.py` | The BAT file contains fixed executable, source and output paths. |

Older translate, pyramid and flood-extent variants formerly listed here have already been removed. Their maintained
replacements are [`gdal_translate_TIF_ovr.py`](../gdal-python/gdal_translate_TIF_ovr.py),
[`gdaladdo_tif_pyramids.py`](../gdal-python/gdaladdo_tif_pyramids.py) and
[`gdal_flood_extent.py`](../gdal-python/gdal_flood_extent.py) respectively.

See [`../gdal-python/README.md`](../gdal-python/README.md) for human-facing recipes and
[`../gdal-python/gdal_cli_tools.json`](../gdal-python/gdal_cli_tools.json) for the authoritative automation catalogue,
including defaults, mutation classifications and argument arrays.
