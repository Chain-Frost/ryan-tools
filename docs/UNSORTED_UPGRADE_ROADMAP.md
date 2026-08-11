# Unsorted → ryan-tools Upgrade Roadmap

## Conversion Standards
When upgrading and moving a script from `unsorted/` to `ryan-scripts/`, apply the following rules in addition to the repository guidelines in `docs/DEVELOPMENT_GUIDE.md` and `ryan-scripts/WRAPPER_STANDARD.md`:

1. **Strict File Structure**: Adhere to `WRAPPER_STANDARD.md` ordering.
   - `__future__` imports and `Path`
   - `WRAPPER_VERSION`
   - `DEFAULT_*` editable variables (with uppercase naming)
   - Standard library imports (`import os`, `import argparse`, etc.)
   - Third-party imports (`from loguru import logger`, `osgeo`, etc.)
   - `ryan_library` imports (absolute imports only)
   - `main()` and `_parse_cli_arguments()`
2. **Defensive Pathing**: Wrap `DEFAULT_*` variables in `Path(...)` inside `main()` rather than typing them strictly, so users can lazily edit defaults as strings without breaking `.resolve()` or `.mkdir()` calls.
3. **Flexible Inputs**: Use `to_path_list` or `to_single_path` from `ryan_library.functions.path_stuff` to sanitise user CLI inputs, enabling `-i` flags to accept strings, Paths, or lists of both smoothly.
4. **No Print Statements**: Replace all `print()` with `loguru` parameterised logging (`logger.info("Processed {} files", count)`).
5. **Native Execution**: Prefer native Python packages (e.g. `osgeo.gdal`) and `multiprocessing` over shelling out to `os.system()` or `subprocess` for better exception handling and cross-platform paths.
6. **Strict Pyright**: The resulting file must pass a strict `pyright` run without any `Unknown` type warnings. Use `# type: ignore` strictly where third-party stubs are missing (like `osgeo`).

Tracks candidates for modernisation from `unsorted/` into `ryan-scripts` or `ryan_library`.
After commit `5420a4b`, 158 redundant/obsolete files were deleted. The remaining files are all
candidates worth keeping — this document tracks which ones should be upgraded and where they belong.

Status key: `[ ]` not started · `[/]` in progress · `[x]` done · `[-]` won't do

---

## Tier 1 — Strong Candidates

These have clear reuse value, are substantial, and fit naturally into existing `ryan-scripts` categories.

### Raster differencing suite

- [ ] **`raster_difference_auto_v2.py`** (16 KB) → `ryan-scripts/gdal-python/`
  - Auto-discovers EXG↔DEV raster pairs using fuzzy filename matching, runs GDAL diff, applies cutoff.
  - Depends on `close_filenames_v1.py` for the matching logic.
- [ ] **`close_filenames_v1.py`** (9 KB) → `ryan_library/functions/`
  - Fuzzy filename matcher that strips AEP/duration/scenario tokens to pair rasters.
  - Reusable logic — should be a library function that other scripts can import.
- [ ] **`raster_difference_manual_list_v2.py`** (7.6 KB) → merge into auto version
  - Manual file-pair version. Could become a `--manual` mode of the auto script.

### Excel protection removal

- [x] **`excel_remove_protection_v3.py`** (2.8 KB) → `ryan-scripts/unsorted-python/remove_excel_protection.py`
- [x] **`excel_remove_protection_v3_xlsm.py`** (2.8 KB) → merged with above
  - Removes sheet protection by modifying XML inside xlsx/xlsm zip.
  - Merged into a single script that handles both extensions.

### LAS/point cloud tools

- [ ] **`las-coverage.py`** (2.5 KB) → `ryan-scripts/point-cloud-python/`
  - Reads LAS/LAZ headers with laspy, creates bounding-box footprint GeoPackage.
  - Similar pattern to `gdal_raster_footprint.py`.

### QGIS project audit

- [ ] **`qgz_parser_v7.py`** (4.9 KB) → `ryan-scripts/other/`
  - Parses QGZ/QGS XML to list layers and data sources.
  - Useful for finding broken paths. No equivalent exists.

### GDAL clip/VRT

- [x] **`gdalwarp_clip_to_polygon.py`** (2 KB) → `ryan-scripts/unsorted-python/gdalwarp_clip_to_polygon.py`
  - Clips TIFs to a polygon shapefile using `osgeo.gdal.Warp()`. Modernised to remove `os.system()` and use `argparse`.
- [ ] **`make_vrt_list_v1.py`** (6.5 KB) → `ryan-scripts/gdal-python/`
  - Builds VRT with filtering. Extends/complements existing `build_VRT.py`.

### File audit

- [ ] **`file_info_v1.py`** (6 KB) → `ryan-scripts/file-management-python/`
  - Recursive file metadata (name, size, date, ext) → Excel export.

---

## Tier 2 — Useful, Lower Priority

Worth upgrading eventually but less urgent. Can be picked up opportunistically.

### Raster QA / conversion

- [x] **`flt-tif-check_v2.py`** (3.2 KB) → `ryan-scripts/unsorted-python/check_flt_tif.py`
  - Finds `.flt` files with no matching `.tif`. Modernised with `pathlib` and `argparse`.
- [ ] **`tif-to-csv-valid-only_v3.py`** (2.2 KB) → compare with `12D-python/tif-to-csv-valid-only_v6.py`
  - Older version. Check if v6 fully supersedes it or if this variant has unique logic.
- [ ] **`dario_thinning-tif_v4.py`** (4.4 KB) → `ryan-scripts/12D-python/` or `gdal-python/`
  - GeoTIFF → thinned XYZ CSV for 12D import.
- [ ] **`make_VRT_for_matching_v1.py`** (2.9 KB) → merge into `make_vrt_list_v1.py`
  - VRT builder for matching raster extents before differencing. Might fold into the main VRT tool.

### 3D / mesh geometry

- [ ] **`extent_of_obj_v1.py`** (5.4 KB) → `ryan-scripts/cad-python/`
  - OBJ mesh bounding-box → GeoPackage footprints. No equivalent exists.

### Coordinate transforms

- [ ] **`simil.py`** + **`simil-master/`** (11 KB) → `ryan_library/functions/`
  - 6-parameter conformal similarity transformation solver. Maths-heavy, reusable.
- [ ] **`translate_las_MGA94Z50_to_YMG_v1-80.py`** (4 KB) → `ryan-scripts/point-cloud-python/`
  - LAS coordinate translation via laspy. Reference for mine-grid transforms.
- [ ] **`params_6param_PHG_rough.py`** / **`params_david_cosmos_6param_noZ-reverse.py`** → keep as reference data
  - Specific mine-grid transform parameters. Not scripts to upgrade, just parameter sets.

### Hydrology / data processing

- [ ] **`rainfall-import.py`** (11 KB) → `ryan-scripts/hydrology-python/` (new category)
  - Rainfall gauge data import and processing.
- [ ] **`combine_2d_q_outlet.py`** (2.8 KB) → `ryan-scripts/TUFLOW-python/`
  - Combines TUFLOW 2D Q outlet CSVs. Related to PO combine workflow.
- [ ] **`ptu_data_processing_script_with_main.py`** (8 KB) → `ryan-scripts/other/`
  - PTU `.his` sensor file processing. Niche but well-structured.
- [ ] **`esdat_to_water.py`** (2.8 KB) → `ryan-scripts/other/`
  - ESDat environmental chemistry CSV → cleaned Excel. Domain-specific.

### TUFLOW tools

- [ ] **`TUFLOW_Circular_Velocity_v2-incomplete.py`** (17.7 KB) → `ryan-scripts/TUFLOW-python/`
  - Circular culvert velocity checker using 1d_Cmx results. Substantial logic but marked incomplete.
- [ ] **`wrb_wrr_check.py`** (0.9 KB) → `ryan-scripts/TUFLOW-python/`
  - Checks for matching `.wrb`/`.wrr` boundary result files.

### Plotting / visualisation

- [ ] **`Plot_Profiles.py`** (5.7 KB) → `ryan-scripts/TUFLOW-python/` or `plotting/`
  - WSE profile plotting from ArcMap StackProfile exports. Template for reuse.
- [ ] **`Burn_Timestep_v2.py`** (2 KB) → `ryan-scripts/other/`
  - PIL-based time label overlay on animation frames. Pairs with `ffmpeg_merge.bat`.

### Volume calculation

- [ ] **`calculate_volumes_and_plot.py`** / **`refactored_calculate_volumes_and_plot.py`** → `ryan-scripts/gdal-python/`
  - Stage-storage volume curve from DEM rasters. The refactored version (3.8 KB) is cleaner.

### asc_to_asc batch grouping

- [ ] **`asc2asc_groups_v5.py`** (21.8 KB) → `ryan-scripts/TUFLOW-python/`
  - Groups rasters by model/AEP/duration, generates asc_to_asc max/median commands.
- [ ] **`asc2asc_groups_diff_v5.py`** (18.8 KB) → merge with above
  - Same logic but for `-dif` (differencing) mode.

### RORB analysis

- [ ] **`RORB_KTP4_v2.py`** + **`RORB_boxes_*.py`** + **`boxes_from_single_v11.py`** → `ryan-scripts/RORB-python/`
  - Suite of RORB ensemble analysis and box-and-whisker plotting scripts.
  - `boxes_from_single_v11.py` (21.5 KB) is the most advanced version.

---

## Tier 3 — Keep as-is (reference / tiny utilities)

These are too small to warrant a formal upgrade or are reference/parameter files. Keep in `unsorted/`.

| File | Reason to keep as-is |
|---|---|
| `Library.py` | 3 AEP↔ARI functions, 12 lines. Reference snippet. |
| `CellCounter.py` | Pixel counter for flood extent. QGIS-dependent, tiny. |
| `NaN_to_-9999.py` | ASC nodata text fixer. 23 lines. |
| `del-999.py` | XYZ nodata line stripper. 25 lines. |
| `dxf-to-csv_v2.py` / `GMdxf-to-csv_v2.py` | DXF text extraction. Simple, `cad-python/` has related tools. |
| `AddLayersToTheme_v3.py` / `DEV` | QGIS theme automation. Hardcoded paths, useful as reference. |
| `TUFLOW_Circular_Velocity_v4.py - Shortcut.lnk` | Just a shortcut file. |
| `postprocess/` (20 files) | Self-contained TUFLOW postprocessing framework. Works as-is. |
| `rorb/` subdirectory (8 files) | Older RORB scripts, superseded by root-level versions. Reference. |
| `matplotlib/` (2 files) | Cross-section plotting references. |
| `oversize_text_file/` (2 files) | Log file readers for huge TUFLOW logs. Tiny. |
| `sharepoint-check/` (4 files) | SharePoint file audit. Project-specific but useful. |
| `xyz files/` (2 files) | XYZ thinning and bounding-box generation. |
| `decarb-stuff/read-lines.py` | Chunked DXF reader. 19 lines. |
| `other/arch_d.ipynb` | Surpac `.arch_d` parser notebook. Exploratory. |
| `simil-master/` | Full simil repo. Keep alongside `simil.py`. |
| `asc2asc_groups.ipynb` | Notebook version of grouping logic. |

---

## BAT scripts — keep or convert

### Worth converting to Python

- [ ] **`gdal_contour_v13.bat`** → `ryan-scripts/gdal-python/`
  - Contour generation. No Python equivalent exists.
- [ ] **`reclass_clip_v3.bat`** → `ryan-scripts/gdal-python/`
  - Reclassify + clip workflow. Fairly complex logic.
- [ ] **`asc_to_asc_median.bat`** → merge with `asc2asc_groups_v5.py`
  - Comprehensive asc_to_asc median envelope. 5.4 KB of batch logic.

### Keep as bat (templates / too simple to convert)

| File | Notes |
|---|---|
| `ogr2ogr_*.bat` (6 files) | Useful OGR command templates. Copy-paste reference. |
| `gdal_translate_*.bat` (5 files) | Format conversion one-liners. |
| `gdal_retile_*.bat` (2 files) | Retiling templates. |
| `gdal_calc_simple.bat` | Raster calculator template. |
| `gdalwarp_MGA_to_KCG_v1.bat` | CRS reprojection reference. |
| `asc2asc_diff_for_multiple_aeps_2.bat` | Multi-AEP loop template. |
| `asc2asc_max_median_for_multiple_durations.bat` | Multi-duration loop template. |
| `7z_batch*.bat` (5 files) | Compression/extraction batch scripts. |
| `ffmpeg_merge.bat` | PNG → MP4 video merge. |
| `del_XF_v2.bat` | Recursive TUFLOW `xf/` directory cleaner. |
| `las2txt_tiles.bat` | LAS → text conversion. |
| `ts1_convert_noDT*.bat` (2 files) | TUFLOW TS1 format conversion. |
| `txt_to_shp*.bat` (2 files) | Text → SHP conversion. |
| `shp-to-DXF-LAYER.bat` | SHP → DXF with layer mapping. |
| `TIF-to-TIF-for-GM.bat` | TIF re-encoding for Global Mapper. |
| `convert_gdb.bat` | GDB → GPKG conversion. |
| `Convert_to_brkline.bat` | Feature → breakline conversion. |
| `VACUUM.BAT` | GeoPackage VACUUM. |
| `unsetNodata.bat` | Unset raster nodata. |
| `dir.bat` / `dir_recursive.bat` | File listing one-liners. |
| `shutdown-a.bat` | Cancel Windows shutdown loop. |
| `jren.bat` | 3rd-party batch rename utility. |
