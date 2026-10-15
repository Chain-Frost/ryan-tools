# TUFLOW cell-wise maximum raster pipeline

The script `ryan-scripts/TUFLOW-python/raster_processing/tuflow_maximum_pipeline.py`, backed by `ryan_library.orchestrators.tuflow.maximum_rasters`, constructs cell-wise maximum rasters from TUFLOW result grids, clips them to polygons, writes compressed GeoTIFFs, optionally builds external overviews, and creates a job report.

The processing is performed by the **Python GDAL bindings** (`from osgeo import gdal, ogr, osr`). This pipeline requires **GDAL 3.12 or later** to use the built-in maximum pixel function in `BuildVRT`. The supported desktop environment is QGIS 4; QGIS 3 compatibility is not planned. See [GDAL support policy](GDAL_SUPPORT.md).

## Drop-in defaults (no CLI arguments)

The wrapper has the original PR #112 defaults and can replace the project script without a separate configuration file:

| Setting | Default |
| --- | --- |
| Input results directory | Parent of the copied script (`SCRIPT_DIR.parent`) |
| Source version folders | `v06`, `v07` |
| AEP subfolders | `01.0p`, `05.0p`, `10.0p`, `20.0p`, `50.0p` |
| Grid types | `d_HR_Max`, `h_HR_Max`, `V_Max`, `Z0_Max` |
| Main output zones | `SCRIPT_DIR/Model Output Zones.gpkg` |
| FG output zones | `SCRIPT_DIR/FG/FG Outputzones.gpkg` |
| Zone-name attribute | `Model` |
| FG composite | `FG01` + `FG02` source extents -> `FG` output zone |
| Other FG zone | `FG03` -> `FG03` output zone |
| Main output path | `SCRIPT_DIR/<AEP>/RTIO_<version>_<model>_EXG_CCNear_<AEP>_MAXIMUM_04M_<grid>.tif` |
| FG output path | `SCRIPT_DIR/FG/<AEP>/RTIO_..._<grid>-clip.tif` |
| Compression | `tuflow` (DEFLATE) |
| Overviews | External `.ovr`, levels 2, 4, 8, 16, 32, nearest |
| Excel report | `MaxRaster_Summary_<timestamp>.xlsx` next to the script |

Both GeoPackages must contain **exactly one vector layer**. A multi-layer GeoPackage is an error, not something this workflow needs to support.

## Optional project overrides

The default workflow remains unchanged when no options are supplied. For other projects, paths and conventions can be supplied on the CLI:

```powershell
python tuflow_maximum_pipeline.py --dry-run

python tuflow_maximum_pipeline.py `
  --input-root "D:\Model\Results" `
  --output-root "D:\Model\Maximums" `
  --main-zones "D:\Model\zones.gpkg" `
  --version v09 --aep 02.0p --grid h_HR_Max `
  --output-template "{model}_{version}_{aep}_{grid}.tif" `
  --group "main:WEST=WEST01,WEST02"
```

- Each `--group` has the form `[main|fg:]OUTPUT=SOURCE1,SOURCE2,...`. If any `--group` is provided, it replaces the two default FG group mappings. All matching standalone main-zone extents are still discovered automatically.
- `main:` means the output polygon is in the main GeoPackage; `fg:` means it is in the FG GeoPackage and receives the FG `-clip` output path pattern.
- Group sources are exact extent subfolder names, matched without case sensitivity. Every configured extent must supply the grid type or the job reports failure; a partial aggregate is not silently accepted.
- `--zone-field` selects the attribute used for polygon name matching; `--main-zones` and `--fg-zones` select the two supported single-layer GeoPackages.\n- `--legacy-local-crs` explicitly overrides the default `EPSG:28350` assumption when input rasters declare the legacy local name `GDA94_MG`. This assigns the declared raster coordinate system; it does **not** copy the zone polygon CRS.
- When no FG groups are configured, the FG GeoPackage is not required.
- `--profile efficient` enables tiled ZSTD compression. `--no-overviews` disables pyramids; `--no-report` skips the Excel report. `--overwrite` and `--dry-run` retain their original meaning.

## Safety and numerical semantics

- The VRT uses a **cell-wise maximum** for overlapping source cells, not a last-file-wins mosaic.
- Sources must be single-band, north-up, matching-CRS rasters with compatible NoData and data types.
- The operation uses nearest-neighbour sampling when sources have different grid geometry. Composite groups align the VRT pixels to source resolution. Validate source alignment before production use when resampling is unacceptable.
- The legacy raster-local-CRS label `GDA94_MG` is explicitly interpreted as `EPSG:28350` by default to preserve the RTIO project assumption. With another known source CRS, specify `--legacy-local-crs EPSG:...`. A differently referenced zone polygon is transformed into the assumed source CRS for clipping; the source CRS is **not** borrowed from the zone. These assumptions are not datum transformations or substitutes for CRS verification.
- Incremental signatures are SHA-256 digests of **file paths, sizes and nanosecond modification times**, not hashes of file bytes. An unchanged fingerprint does not independently prove decoded pixel content is unchanged.
- Normal publication errors trigger rollback of the previous GeoTIFF and external overview together. Two-file replacement is not a single atomic filesystem operation and sudden power loss may still leave backup files requiring recovery.\n- Output filename collisions are rejected across the entire job list before writes; omitting distinguishing placeholders from a custom output template is an error.

## Validation status

Source parsing and new unit tests cover default names, composite discovery, missing group sources, customised group mappings, output-name collisions, GDAL virtual path support, source CRS assumptions, publication rollback, known conflicting CRSs and true overlap maximums. Actual GDAL tests require a runner with GDAL Python bindings 3.12+. Real-project validation of v06/v07 grids, their output-zone GeoPackages, output naming parity and retained prior output values is still required before declaring a production drop-in replacement.
