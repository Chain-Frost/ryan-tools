# Unsorted → ryan-tools Upgrade Roadmap

## Conversion Standards

When upgrading and moving a script from `unsorted/` to `ryan-scripts/`, apply the following rules in addition to the repository guidelines in `docs/DEVELOPMENT_GUIDE.md` and `ryan-scripts/WRAPPER_STANDARD.md`:

1. **Strict File Structure**: Adhere to `WRAPPER_STANDARD.md` ordering.
   - Raw module docstring
   - `__future__` imports and minimal imports needed by editable defaults
   - `WRAPPER_VERSION`
   - `DEFAULT_*` editable variables (with uppercase naming)
   - Standard library imports (`import os`, `import argparse`, etc.)
   - Third-party imports (`from loguru import logger`, `osgeo`, etc.)
   - `ryan_library` imports (absolute imports only)
   - `main()` and `_parse_cli_arguments()`
2. **Defensive Pathing**: Wrap `DEFAULT_*` variables in `Path(...)` inside `main()` rather than typing them strictly, so users can lazily edit defaults as strings without breaking `.resolve()` or `.mkdir()` calls.
3. **Flexible Inputs**: Use `to_path_list` or `to_single_path` from `ryan_library.functions.path_stuff` at the wrapper/library boundary when a public callable accepts path-like values. Let `argparse` define the CLI's scalar or repeated-input shape explicitly.
4. **No Print Statements**: Replace all `print()` with `loguru` parameterised logging (`logger.info("Processed {} files", count)`).
5. **Execution Boundary**: Prefer supported Python APIs when they provide the required behaviour. Use `subprocess` for external command-line tools when that is their supported interface, with explicit arguments, return-code handling and captured diagnostics. Bound concurrent child processes to avoid oversubscription.
6. **Strict Pyright**: Modified Python files must pass strict Pyright. Use narrow, rule-specific suppressions only where incomplete third-party stubs make them necessary.

Tracks candidates for modernisation from `unsorted/` into `ryan-scripts` or `ryan_library`.
After commit `5420a4b`, 158 redundant/obsolete files were deleted. This document tracks the disposition of the
remaining candidates and active migrations. Because `unsorted` is a separate submodule, its source deletion and the
parent repository's replacement must be reviewed and committed independently.

New or mechanically converted scripts remain in `ryan-scripts/unsorted-python/` while they are unreviewed. Move them
into a maintained category such as `gdal-python/` only after their behaviour, architecture, typing, logging and focused
validation have been reviewed.

Status key: `[ ]` not started · `[/]` in progress · `[x]` done · `[-]` won't do

Use `[x]` only after the replacement is tracked, its focused validation is complete, and the source disposition in the
`unsorted` submodule has been finalised. An untracked replacement or an uncommitted source deletion remains `[/]`.

---

## Active review plan — 12 August 2026

This plan covers the experimental candidates currently parked in `ryan-scripts/unsorted-python/`. Passing these gates
means a candidate is internally reviewable; it does not by itself promote the script into a maintained category or
justify moving its implementation into `ryan_library`.

### Review gates

- [x] **ASC-to-ASC raster operations and grouping**
  - Validate non-empty inputs, matching raster dimensions/transforms/CRS, nodata and output dtype behaviour.
  - Avoid partially written final outputs, repeated dataset opening and silently ignored CLI arguments.
  - Cover maximum, statistics, difference and grouping behaviour with synthetic tests; pass strict Pyright and Black.
- [x] **Vector clipping**
  - Bound concurrency, propagate partial failures, validate input/extents and protect existing or partial outputs.
  - Check layer creation, feature-write return codes, CRS assumptions and GDAL diagnostics; pass strict Pyright.
- [x] **Raster-to-XYZ conversion**
  - Bound child processes, deduplicate deterministic discovery, validate output collisions and remove partial outputs.
  - Confirm useful failure diagnostics and non-interactive execution; pass strict Pyright.
- [x] **Stage-storage calculation**
  - Validate levels, raster geometry and finite/nodata handling; preserve exact cell-area volume calculations.
  - Validate output bounds and step size, close plots explicitly and cover known synthetic volumes; pass strict Pyright.
- [x] **RORB ensemble plotting**
  - Preserve legacy first-to-last exceedance duration without mutating caller data.
  - Validate required columns, empty filters and plot resource cleanup; cover reader/calculation/plot smoke tests.
- [x] **Candidate-set completion checks**
  - Run focused tests from a pre-created repository-local pytest temp directory.
  - Run Black, strict Pyright on every changed Python file, compilation, wrapper `--help`, Loguru formatting,
    documentation checks and `git diff --check`.
  - Record remaining environment-specific checks rather than marking an untested GDAL/QGIS workflow complete.

### Review outcome

The normal Python review passed with 19 focused synthetic tests, strict Pyright with zero diagnostics across the entire
`unsorted-python` candidate set, Black, compilation, Loguru formatting, documentation checks, `git diff --check`, and
`--help` smoke checks for ten candidate wrappers. The implementations now use bounded concurrency, checked exit codes,
temporary outputs, deterministic discovery and explicit validation where applicable.

The candidates remained `[/]` after this review. Their later disposition is recorded in the 20 August promotion review
below. The promoted plotting helper now calls Matplotlib's `orientation` API directly, avoiding the deprecated `vert`
parameter used by Seaborn 0.13.2 with Matplotlib 3.11.

### Source and promotion policy

The `unsorted` and `tests/test_data` submodules remain at the commits intentionally recorded by this branch. Source
deletions in the `unsorted` submodule are not reversed by this plan. Candidate tests live beside the experimental code
under `ryan-scripts/unsorted-python/tests/` so they did not imply a maintained `ryan_library` API. Promotion into a
maintained script category remained a separate decision after the gates above passed.

---

## Promotion review — 20 August 2026

The complete `ryan-scripts/unsorted-python/` set was reviewed and promoted into its domain folders. Every relocated
production Python file carries `# moved from unsorted, not tested in production yet - 2026-08-20` immediately after its
module docstring. This distinguishes focused automated validation from use on a real project.

Shared behavior was consolidated during promotion:

- native ASC-to-ASC raster operations now use `ryan_library/functions/tuflow/local_raster_calc.py`; the duplicate local
  candidate was removed;
- vector format metadata, layer inspection, filtering, reprojection and atomic translation now use
  `ryan_library/functions/gdal/vector_conversion.py`; the duplicate candidate module was removed;
- stage-storage calculations live in `ryan_library/functions/gdal/stage_storage.py` behind a thin GDAL wrapper;
- RORB ensemble reading, calculation and plotting live under `ryan_library/functions/RORB/` and
  `ryan_library/orchestrators/rorb/` behind a thin RORB wrapper;
- repeated 7-Zip discovery and checked execution live in `ryan_library/functions/archive_utils.py`; and
- clearing raster NoData metadata now uses the shared GDAL raster-processing API.

The wrappers moved to `TUFLOW-python/`, `gdal-python/`, `RORB-python/`, `hydrology-python/`,
`file-management-python/`, `misc-python/` and `other/` according to their operational domain. Focused tests moved to
`tests/scripts/promoted_scripts/`. Real-project checks with the external TUFLOW/GDAL/QGIS/RORB workflows remain useful;
the source warning is intentionally retained until those occur.

Promotion validation passed 58 focused tests, strict Pyright for all changed production files and the promoted-test
configuration, Black, compilation, Loguru formatting, documentation and JSON checks, and `git diff --check`. All 20
promoted CLI wrappers passed `--help`; seven shared-library consumers also passed after being copied outside the
checkout and run against the rebuilt and installed `ryan_functions-26.8.20.11-py3-none-any.whl`. The focused plotting
plotting tests run with deprecation warnings treated as errors after replacing the Seaborn boxplot compatibility path.

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
- [x] **`raster_difference_manual_list_v2.py`** (7.6 KB) → `ryan-scripts/TUFLOW-python/asc2asc_py.py`
  - Manual file-pair version of raster differencing using `rasterio`.
  - Uses the maintained local raster-calculation API after focused alignment, NoData and output-safety validation.

### Excel protection removal

- [x] **`excel_remove_protection_v3.py`** (2.8 KB) → `ryan-scripts/misc-python/remove_excel_protection.py`
- [x] **`excel_remove_protection_v3_xlsm.py`** (2.8 KB) → merged with above
  - Removes sheet protection by modifying XML inside xlsx/xlsm zip.
  - Merged into a single script that handles both extensions.

### LAS/point cloud tools

- [ ] **`las-coverage.py`** (2.5 KB) → `ryan-scripts/point-cloud-python/`
  - Reads LAS/LAZ headers with laspy, creates bounding-box footprint GeoPackage.
  - Similar pattern to `gdal_raster_footprint.py`.

### QGIS project audit

- [x] **`qgz_parser_v7.py`** (4.9 KB) → `ryan-scripts/other/audit_qgis_projects.py`
  - Parses QGZ/QGS XML to list layers and data sources.
  - Useful for finding broken paths. Representative project-provider validation remains a production check.

### Hydrology Utilities

- [x] **`Library.py`** (12 lines) → `ryan-scripts/hydrology-python/aep_ari_conversions.py`
  - Simple validated AEP/ARI mathematical conversion functions.
- [x] **`refactored_calculate_volumes_and_plot.py`** (3.7 KB) → `ryan-scripts/gdal-python/gdal_stage_storage.py`
  - Uses shared block-streamed calculations with known-volume and invalid-level coverage.

### GDAL clip/VRT

- [x] **`gdalwarp_clip_to_polygon.py`** (2 KB) → `ryan-scripts/gdal-python/gdalwarp_clip_to_polygon.py`
  - Clips TIFs to a polygon shapefile using `osgeo.gdal.Warp()`. Modernised to remove `os.system()` and use `argparse`.
- [ ] **`make_vrt_list_v1.py`** (6.5 KB) → `ryan-scripts/gdal-python/`
  - Builds VRT with filtering. Extends/complements existing `build_VRT.py`.
- [x] **`ogr2ogr_clipper.bat`** → `ryan-scripts/gdal-python/batch_vector_clip.py`
  - Bounded parallel clipping with checked failures, temporary outputs and collision validation.

### File audit

- [ ] **`file_info_v1.py`** (6 KB) → `ryan-scripts/file-management-python/`
  - Recursive file metadata (name, size, date, ext) → Excel export.

---

## Tier 2 — Useful, Lower Priority

Worth upgrading eventually but less urgent. Can be picked up opportunistically.

### Raster QA / conversion

- [x] **`flt-tif-check_v2.py`** (3.2 KB) → `ryan-scripts/gdal-python/check_flt_tif.py`
  - Finds `.flt` files with no matching `.tif`. Modernised with `pathlib` and `argparse`.
- [x] **`tif-to-csv-valid-only_v3.py`** (2.2 KB) → `ryan-scripts/gdal-python/gdal_raster_to_xyz.py`
  - Uses bounded `gdal2xyz` workers, deterministic discovery and partial-output cleanup.
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
- [ ] **`2d_q_to_csv_v3.py`** (6 KB) → `ryan-scripts/tuflow-python/`
  - Combines TUFLOW 2D Q outlet CSVs. Related to PO combine workflow.
- [ ] **`esdat_to_water.py`** (2.8 KB) → `ryan-scripts/other/`
  - ESDat environmental chemistry CSV → cleaned Excel. Domain-specific.

### Project-specific candidates

- [-] **`ptu_data_processing_script_with_main.py`**
  - Highly project-specific data logging script parsing PTU files; no parent-repository replacement is planned.

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

- [x] **`calculate_volumes_and_plot.py`** / **`refactored_calculate_volumes_and_plot.py`** → `ryan-scripts/gdal-python/gdal_stage_storage.py`
  - Shared stage-storage calculation with a thin CSV/plot wrapper.

### asc_to_asc batch grouping

- [x] **`asc2asc_groups_v5.py`** (21.8 KB) → `ryan-scripts/TUFLOW-python/asc2asc_groups.py`
  - Groups rasters by model/AEP/duration, generates asc_to_asc max/median commands.
- [x] **`asc2asc_groups_diff_v5.py`** (18.8 KB) → merged with above
  - Same logic but for `-dif` (differencing) mode.

### RORB analysis

- [x] **`RORB_KTP4_v2.py`** + **`RORB_boxes_*.py`** + **`boxes_from_single_v11.py`** → shared RORB analysis and plotting plus `ryan-scripts/RORB-python/plot_ensemble_results.py`
  - Suite of RORB ensemble analysis and box-and-whisker plotting scripts.
  - `boxes_from_single_v11.py` (21.5 KB) is the most advanced version.
  - Shared analysis, plotting and orchestration modules now sit behind the promoted wrapper.

---

## Tier 3 — Keep as-is (reference / tiny utilities)

These are too small to warrant a formal upgrade or are reference/parameter files. Keep in `unsorted/`.

| File | Reason to keep as-is |
| --- | --- |
| `CellCounter.py` | Pixel counter for flood extent. QGIS-dependent, tiny. |
| `NaN_to_-9999.py` | ASC nodata text fixer. 23 lines. |
| `del-999.py` | XYZ nodata line stripper. 25 lines. |
| `dxf-to-csv_v2.py` / `GMdxf-to-csv_v2.py` | DXF text extraction. Simple, `cad-python/` has related tools. |
| `AddLayersToTheme_v3.py` / `DEV` | QGIS theme automation. Hardcoded paths, useful as reference. |
| `TUFLOW_Circular_Velocity_v4.py - Shortcut.lnk` | Just a shortcut file. |
| `postprocess/` (20 files) | Removed by approved `unsorted` cleanup commit `c7ef077`; recover from submodule history if needed. |
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
  - Contour generation. Deleted in `c7ef077`; recover from submodule history when implementing a replacement.
- [-] **`reclass_clip_v3.bat`** → `ryan-scripts/gdal-python/`
  - Not present in the current `unsorted` checkout and no replacement was found. Recover it from history and reassess
    if this workflow is still wanted.
- [x] **`asc_to_asc_median.bat`** → merged with `asc2asc_groups_v5.py`
  - Comprehensive asc_to_asc median envelope. 5.4 KB of batch logic.
  - Source disposition was finalised in `c7ef077`; use submodule history for the remaining executable parity check.
- [x] **`gdal_retile_v4_asc.bat`** + **`gdal_retile_v4_tif.bat`** → `ryan-scripts/gdal-python/gdal_retile_wrapper.py`
  - Both sources were deleted in `c7ef077`; use submodule history during environment-specific parity validation.
- [x] **Five `ogr2ogr_*` conversion templates** → maintained GDAL vector wrappers in `ryan-scripts/gdal-python/`
  - `gdal_vector_translate.py` and `split_vector_by_attribute.py` use the shared vector-conversion API. The sources were
    deleted in `c7ef077`; real-project checks remain flagged in the promoted files.

### Keep as bat (templates / too simple to convert)

| File | Notes |
| --- | --- |
| `ogr2ogr_clipper.bat` | Useful OGR command template. Copy-paste reference. |
| `ffmpeg_merge.bat` | PNG → MP4 video merge. |
| `ts1_convert_noDT*.bat` (2 files) | TUFLOW TS1 format conversion. |
| `txt_to_shp*.bat` (2 files) | Text → SHP conversion. |

### Pending BAT source disposition

- [x] **`convert_gdb.bat`** → `ryan-scripts/gdal-python/convert_gdb_to_gpkg.py`
  - The replacement is tracked and supports selectable vector formats and per-layer or combined-database output. Source
    disposition was finalised in `c7ef077`.
- [ ] **`gdal_calc_simple.bat`**
  - No direct replacement is identified. It was deleted in `c7ef077` and remains recoverable from submodule history.
- [/] **`asc2asc_diff_for_multiple_aeps_2.bat`** and **`asc2asc_max_median_for_multiple_durations.bat`**
  - Both sources were deleted in `c7ef077`. Include their historical behaviour in the remaining executable parity review.

Review source inventory against the current `unsorted` checkout before changing a BAT file's disposition.
