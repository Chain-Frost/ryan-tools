# Per-file installed-script inventory — evidence ledger (2026-10-19)

**Status: incomplete review.** This ledger originated with 146 files at `1ac42c6` and now covers all **149** Git-tracked source files under `ryan-scripts/` on PR #137 after incorporating main. It excludes submodules. It does not enter or inspect the `unsorted` submodule.
There are **83 selected wheel files**, **66 excluded source files** and
**33 distinct existing MCP script paths**. These counts are inventory evidence,
**not proof** that every source file is maintained, safe to import, or correctly excluded.

The original [requirements baseline](2026-10-17-installed-scripts-original-requirements.md) requires each executable
to be independently assessed for owner, lifecycle, import-time effects, hard-coded paths,
dependencies/sibling resources, invocation and `--help`, headless `--no-pause`, mutation/approval behavior,
external software, Windows/mapped/UNC compatibility and output/exit semantics.
No file is marked *reviewed* without those assessments and supporting tests.

**Current wheel selection remains provisional** pending audit and application-specific validation. The previous PR #130 made the selection
without completing this inventory. “Yes” means currently in the install manifest; “No” means source is
available in the repository but excluded from the wheel. MCP “Yes” denotes a catalogue reference,
**not** guaranteed runtime readiness.

| Source path relative to `ryan-scripts/` | In wheel | MCP target | Owner/lifecycle & import audit | CLI/headless/exit audit | Dependencies/mutation/external evidence |
| --- | --- | --- | --- | --- | --- |
| `12D-python/generate_culvert_lines.py` | No | No | Source inspected; provisional repository-only; maintainer owner pending | Guarded `main()`; no argparse/help/no-pause or tested exit contract | Hard-coded Q: project CSV; `process_12D_culverts`, GeoPandas/Shapely; writes `culvert_geometries.gpkg` (28350); representative data and output acceptance needed |
| `12D-python/generate_culvert_polygons.py` | No | No | Source inspected; provisional repository-only; experimental geometry; owner pending | Guarded `main()`; no argparse/help/no-pause or tested exit contract | Hard-coded Q: project CSV; GeoPandas/Shapely; writes two shapefiles into working directory; top-level logger setup; CAD/GIS geometry acceptance needed |
| `12D-python/thin-raster-terrain-for-12D_v5.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; no argparse/help/no-pause or tested exit contract | Looks for exact `main-section-clip.tif` beside script; writes `thinned-data4` there; Rasterio, NumPy, pandas, multiprocessing; high-volume output not accepted |
| `12D-python/tif-to-LAS-valid-only_v6.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; no argparse/help/no-pause or tested exit contract | Hard-coded P: network raster source; laspy and shared terrain processor; writes LAS 1.2 tiles or full LAS; scales, coordinates and outputs need acceptance |
| `12D-python/tif-to-csv-valid-only_v6.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; no argparse/help/no-pause or tested exit contract | Script-adjacent TIFF input/output; writes `csv_processing.log` to working directory; shared terrain processor; configured tiling and outputs unverified |
| `12D-python/tif-to-csv-valid-only_v6a.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; no argparse/help/no-pause or tested exit contract | Script-adjacent TIFF input/output; 0.001 XYZ rounding; shared terrain processor; output and logging acceptance outstanding |
| `AutoCAD-python/dwg-polygons-txt-to-csv.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded entry; editable constants, not an argparse CLI; exit/output untested | Hard-coded C: input/output; standard-library text-export parser writes deduplicated XYZ CSV; project format and error handling need test fixtures |
| `AutoCAD-python/dwg-to-points.py` | No | No | Source inspected; possible general-purpose CLI candidate; maintain repo-only pending tests | Argparse help advertised; `--force` for existing outputs; CLI/output/exit parity not yet tested | ezdxf + pyarrow; DXF mesh to Parquet and optional XYZ; explicit input/output and overwrite contract require disposable-data test |
| `AutoCAD-python/group-by-entity-colour-and-dissolve-to-hatch.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; editable project configuration; no tested help/no-pause or exit contract | ezdxf + Shapely; dissolves entities into hatches and writes DXF; tolerance and CAD geometry require manual review |
| `AutoCAD-python/group-by-entity-colour-and-dissolve-to-hatch_dask3.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; editable project configuration; no tested help/no-pause or exit contract | ezdxf/Shapely/Dask distributed; parallel chunk dissolve into hatches; CAD output and worker dependencies unverified |
| `AutoCAD-python/group-by-entity-colour-and-dissolve.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; hard-coded project invocation; no help/no-pause or tested exit contract | Hard-coded UNC working directory and input/output DXF; ezdxf + Shapely; geometry fidelity and network path unverified |
| `AutoCAD-python/group-by-entity-colour-and-dissolve3.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; editable project invocation; no help/no-pause or tested exit contract | ezdxf + Shapely; colour-group dissolve with tolerance; CAD geometry and output conditions unverified |
| `AutoCAD-python/group-by-entity-colour.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded `main()`; hard-coded project invocation; no help/no-pause or tested exit contract | Hard-coded UNC working directory; ezdxf layer grouping to output DXF; network and CAD fidelity unverified |
| `GlobalMapper/DXF-to-float_v1.gms` | No | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `README.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `RORB-python/RORB-find-closure-durations.py` | Yes | Yes | Source inspected; selected library-backed RORB wrapper; maintainer acceptance pending | Argparse common execution overrides/thresholds; guarded entry and no-pause; existing help coverage, new failure tests pending CI | Reads batch.out/hydrographs recursively via closure orchestrator; emits CSV/Parquet summaries; threshold and dataset parity not established |
| `RORB-python/extract_rainfall-to-TUFLOW.py` | Yes | No | Source inspected; provisional installed, manually configured RORB converter; acceptance pending | Guarded main; edits directory and start/end rows in source; no argparse/no-pause or tested exit code | Reads RORB .out tables and writes *_Rainfall.csv; script-adjacent default output and row/heading interpretation need real RORB data checks |
| `RORB-python/hydrograph_extract_rorb_csv.py` | Yes | No | Source inspected; provisional installed RORB plot utility; acceptance pending | Guarded main followed by unconditional Enter pause; no argparse/no-pause; headless parity not satisfied | Requires naming-specific hydrograph CSV input beside script; timestamped Excel/PNG outputs, column parsing and plot labels unverified |
| `RORB-python/plot_ensemble_results.py` | Yes | Yes | Provisional installed/MCP; source-tree error-path test only; owner pending | Copied-wrapper missing Parquet input with `--no-pause`: exit 1; output directory not created; help in selected smoke matrix | RORB/Parquet/plotting dependencies and valid result/plot parity unverified |
| `RORB-python/rorb-peaks_v2.py` | No | No | Source inspected; repository-only required due import-time effects; owner pending | No protected entry; no argparse/help/no-pause or safe import semantics | On import changes working directory, recursively reads RORB `.out`, may raise, and writes `processed_output.csv`; never auto-import or wheel-promote |
| `TUFLOW-bat/asc2asc_max_by_search.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-bat/asc_to_asc-DIFF.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-bat/asc_to_asc-MAX.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/README.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/culvert_results/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/culvert_results/combine_culvert_maximums.py` | Yes | Yes | Source inspected; selected maintained library-backed wrapper; owner sign-off pending | Argparse common flags, guarded main, embedded version and no-pause; existing help test only | Orchestrator merges culvert maximum CSVs and optional raw result types to Excel/Parquet; workbook content/format unverified |
| `TUFLOW-python/culvert_results/combine_culvert_timeseries.py` | Yes | Yes | Source inspected; selected maintained library-backed wrapper; owner sign-off pending | Argparse common flags and no-pause; help/invalid-directory tests added, CI pending | Orchestrator combines culvert Q/V/H/CF/Chan/EOF CSVs; editable Excel default and path selection; result/data-type output parity pending |
| `TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py` | Yes | Yes | Source inspected; selected library-backed mean report; acceptance pending | Argparse common options, guarded main and no-pause; help test added, CI pending | Culvert mean peak summary via shared orchestrator; default result filters, Excel/raw output and engineering aggregation unverified |
| `TUFLOW-python/culvert_results/create_median_culvert_peak_report.py` | Yes | Yes | Source inspected; selected library-backed median report; acceptance pending | Argparse common options, guarded main and no-pause; help test added, CI pending | Culvert median peak summary via shared orchestrator; expected TP/duration aggregation and outputs unverified |
| `TUFLOW-python/gis_processing/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/gis_processing/apply_qgis_styles_to_results.py` | Yes | No | Source inspected; selected library-backed GIS styling wrapper; acceptance pending | Argparse execution flags/guarded main/no-pause; help and safe failure-path tests added, CI pending | Loads packaged QML or custom overrides and applies styles; QGIS/GDAL operations and raster/vector styling acceptance outstanding |
| `TUFLOW-python/gis_processing/rename_geopackage_layer_to_filename.py` | Yes | No | Source inspected; selected but legacy destructive standalone; promotion merits review | No argparse/no-pause; Windows completion keypress, no reliable process exit on caught failures | GeoPandas/pyogrio read and rebuild single-layer GPKG then move over original; back up data; layer metadata and overwrite parity unverified |
| `TUFLOW-python/gis_processing/split_network_layers_by_culvert_id.py` | Yes | No | Source inspected; selected bespoke GIS converter; acceptance pending | Guarded project-specific main, no argparse/no-pause, output error handling untested | Uses Fiona/GeoPandas and fixed network/boundary fields to write many shapefiles and .trd includes; geospatial integrity unverified |
| `TUFLOW-python/log_processing/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py` | Yes | Yes | Source inspected; selected workbook mutation wrapper; acceptance pending | Argparse common flags and guarded no-pause; help test added, CI pending | Hard-coded default Q: results folder/master workbook; appends only new rows via orchestrator; deduplication, original workbook integrity unverified |
| `TUFLOW-python/log_processing/check_tuflow_user_activity.py` | Yes | No | Provisional: protected main and embedded version; inspect local configuration | argparse help/no-pause; external environment still unverified | Read-only ProgramData TUFLOW logs, local CodeMeter optional; verify on Windows |
| `TUFLOW-python/log_processing/create_log_summary_report.py` | Yes | Yes | Source inspected; selected library-backed summary wrapper; acceptance pending | Argparse common flags, guarded no-pause; help and invalid working-directory tests added, CI pending | Parses TUFLOW log files recursively through orchestrator; generated summary schema, missing log cases, and live dashboard unverified |
| `TUFLOW-python/model_management/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/model_management/collect_result_and_check_files.py` | Yes | No | Source inspected; selected bespoke file collector; acceptance pending | Guarded main, editable RUNS patterns but no argparse/no-pause, return code contract untested | Copies result/check matches into TFfiles_ext_tmp and refuses existing target; basename collision/volume/network QA outstanding |
| `TUFLOW-python/model_management/delete_duplicate_check_files.py` | Yes | Yes | Source inspected; selected high-risk cleanup utility; acceptance pending | Guarded main, DRY_RUN and typed DELETE controls, no standard argparse/no-pause | Hashes/deletes duplicates, leaves audit placeholders/manifests; retention/rollback/permission and full destructive acceptance not established |
| `TUFLOW-python/model_management/run_tuflow_simulations.py` | Yes | Yes | Source inspected; selected Windows TUFLOW runner; acceptance pending | Guarded CLI with demo dashboard; editable get_parameters, no standard help/no-pause parity established | Windows TUFLOW executable and subprocess scheduling, model flags/priority/GPU settings; real TUFLOW and licence acceptance necessary |
| `TUFLOW-python/model_management/verify_converted_tuflow_rasters.py` | Yes | No | Source inspected; selected raster verification/cleanup utility; acceptance pending | Argparse --root/--tif-root/no-pause; read-only default; help already tested | Rasterio exact raster/CRS/nodata comparisons; convert-missing and typed DELETE can mutate files; benchmark and validation required |
| `TUFLOW-python/po_and_timeseries/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/po_and_timeseries/calculate_po_closure_durations.py` | Yes | Yes | Source inspected; selected library-backed PO threshold wrapper; acceptance pending | Argparse common flags and guarded no-pause; help test added, CI pending | TUFLOW PO Flow CSV closure durations to Excel/Parquet through orchestrator; thresholds/results unverified |
| `TUFLOW-python/po_and_timeseries/check_po_timeseries_peaks.py` | Yes | Yes | Source inspected; selected library-backed PO peak checker; acceptance pending | Argparse common flags, guarded no-pause; help test added, CI pending | Compares peaks in TUFLOW PO CSV via orchestrator; data-type selection and engineering tolerances need validation |
| `TUFLOW-python/po_and_timeseries/check_timeseries_stability.py` | Yes | Yes | Source inspected; selected stability-report wrapper; acceptance pending | Argparse common options, additional report option, no-pause; help test added, CI pending | PO/1D Q time-series rolling tolerance calculations and report exports; model timestep thresholds/parity unverified |
| `TUFLOW-python/po_and_timeseries/combine_po_results.py` | Yes | Yes | Source inspected; selected library-backed PO combiner; acceptance pending | Argparse common filtering, guarded no-pause; help test added, CI pending | Merges TUFLOW PO CSV by filename/time and configurable filters; Excel/Parquet schemas and original defaults unverified |
| `TUFLOW-python/po_and_timeseries/combine_pomm_results.py` | Yes | Yes | Source inspected; selected library-backed POMM combiner; owner acceptance pending | Argparse common options/guarded main/no-pause; original CLI and defaults not yet test-accepted | Orchestrator merges POMM and RLL_Qmx CSV to Excel/Parquet; event/TP filtering, location parsing and outputs unverified |
| `TUFLOW-python/po_and_timeseries/create_pomm_mean_peak_report.py` | Yes | Yes | Source inspected; selected maintained mean peak report; owner acceptance pending | Argparse common options/guarded no-pause; representative help already tested | Creates timestamped mean-peaks Excel from POMM/RLL_Qmx; aggregation, input discovery and workbook schema unverified |
| `TUFLOW-python/po_and_timeseries/create_pomm_median_peak_report.py` | Yes | Yes | Source inspected; selected maintained median peak report; owner acceptance pending | Argparse common options/guarded no-pause; original defaults and CLI behaviour not yet test-accepted | Creates timestamped median-peaks Excel from POMM/RLL_Qmx; ensemble statistics and output schema unverified |
| `TUFLOW-python/po_and_timeseries/generate_2d_po_label_list.py` | Yes | No | Source inspected; selected self-contained GeoPackage label utility; owner acceptance pending | Argparse path/options with protected main; no general no-pause or tested exit/output contract | Reads SQLite/GPKG labels and comments; SQL quoting/feature filtering and expected label mapping require synthetic GPKG QA |
| `TUFLOW-python/po_and_timeseries/plot_po_scenario_timeseries.py` | Yes | No | Source inspected; selected project-specific plotting workflow; owner acceptance pending | Guarded main, editable Q: paths, no general argparse/help or no-pause; headless behaviour unverified | Parses TUFLOW PO CSV or Parquet cache, writes cache, opens interactive Matplotlib; scenario filters and user settings tied to project |
| `TUFLOW-python/po_and_timeseries/plot_water_level_profiles.py` | Yes | Yes | Source inspected; selected configurable GIS profile wrapper; owner acceptance pending | Argparse input/spacing/output flags, guard and no-pause; representative help already tested | Project-default E: GPKG/terrain/water level TIFFs; profile-line CRS/resampling, no-overwrite PNG and 300 dpi QA unverified |
| `TUFLOW-python/raster_processing/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/raster_processing/calculate_raster_differences.py` | Yes | No | Source inspected; selected raster-difference executable; owner acceptance pending | Argparse with -change/--no-change, -b/--no-pause; CLI and output contract not yet accepted | Uses Rasterio and ryan_library diff processing, optionally compares native ASC_to_ASC; wet/dry/nodata, grid alignment and output QA needed |
| `TUFLOW-python/raster_processing/mask_velocity_by_depth.py` | Yes | No | Source inspected; selected project-specific in-place raster transform; owner acceptance pending | Guarded configured main; no argparse/no-pause and interactive/headless exit unverified | Default Q: target directories; copies source to *_original backup then replaces velocity TIFF with depth-masked raster; resampling and repeat-run integrity critical |
| `TUFLOW-python/raster_processing/run_asc_to_asc_maximum_searches.py` | Yes | No | Source inspected; selected native raster max wrapper; owner acceptance pending | Argparse common flags, dry-run, worker override and guarded no-pause; help acceptance already tested | Native ASC_to_ASC max jobs from template axes and glob; executable discovery, duplicates, outputs and process concurrency unverified |
| `TUFLOW-python/raster_processing/run_asc_to_asc_mean_then_maximum.py` | Yes | No | Source inspected; selected native raster mean/max wrapper; owner acceptance pending | Argparse common flags, dry-run/strictness/worker flags and no-pause; representative help already tested | Per-duration TP mean then duration max via native raster tool; minimum contributing value, ensemble assumptions, result parity unverified |
| `TUFLOW-python/raster_processing/run_asc_to_asc_median_then_maximum.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/raster_processing/run_grouped_asc_to_asc_statistics.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/raster_processing/run_python_raster_calculations.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `WRAPPER_STANDARD.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `cad-python/dtm_str_converter_to_gpkg.py` | No | No | Not assessed | Not assessed | Not assessed |
| `cad-python/extract_dxf_polyface.py` | No | No | Not assessed | Not assessed | Not assessed |
| `cad-python/polyface_parquet_to_gpkg.py` | No | No | Not assessed | Not assessed | Not assessed |
| `cad-python/process_arch_d.py` | No | No | Not assessed | Not assessed | Not assessed |
| `culvert.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `docx-python/word_style_audit.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `file-management-python/archive_compress_folder.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `file-management-python/archive_compress_individual.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `file-management-python/archive_extract.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `file-management-python/copy_file.py` | No | No | Repository-only: unguarded import executes Win32 share connection | No safe CLI/help; editable settings run on import | Network copy/credentials, overwrite and external Windows MPR; unsafe for installed wrapper |
| `file-management-python/copy_folder.py` | No | No | Repository-only: unguarded import executes Win32 share connection | No safe CLI/help; editable settings run on import | Network copy/credentials, overwrite and external Windows MPR; unsafe for installed wrapper |
| `file-management-python/file_inventory.py` | Yes | No | Provisional installed; guarded main and embedded version; owner pending | Copied-wrapper CSV export, traversal rejection (exit 2), SHA-256/Unicode, file filtering and `--no-recursive` regression coverage; no-pause exercised | Input bytes preserved on tested paths; `--no-excel` verified; default Excel production workflow, network paths and error cases still unverified |
| `file-management-python/list_files_recursive.py` | Yes | No | Provisional installed; guarded main/version; owner pending | Copied-wrapper recursive text listing with `--no-pause`: success; missing directory returns exit 1 and creates no output | Original `filenames.txt` output in input directory checked; output permissions, UNC, long paths and resource parity outstanding |
| `file-management-python/rename-msg-files-argparse-folder.py` | No | No | Not assessed | Not assessed | Not assessed |
| `fix-chatgpt-type-hints.py` | No | No | Not assessed | Not assessed | Not assessed |
| `floodway.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-bat/LAZ-to-LAS.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-bat/README.md` | No | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `gdal-bat/gdal_edit_Set_nodata.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-bat/gdal_flood_extent_sieve.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-bat/gdal_merge.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-bat/gdal_merge_CLI.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-bat/gdal_merge_xyz_extent.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-bat/gdal_raster_footprint.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/README.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `gdal-python/audit_CRS.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/batch_vector_clip.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/build_VRT.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/check_flt_tif.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/convert_gdb_to_gpkg.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_flood_extent.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_merge.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_merge_by_extent.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_raster_footprint.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_raster_to_xyz.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_retile_wrapper.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_set_nodata.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_stage_storage.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_translate_TIF_ovr.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_unset_nodata.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdal_vector_translate.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdaladdo_tif_pyramids.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/gdalwarp_clip_to_polygon.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/laz_to_las.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `gdal-python/recompress_geotiffs.py` | Yes | Yes | Provisional installed/MCP; guarded main/version; owner pending | Copied-wrapper default audit of empty disposable directory with `--no-pause` returns success without changing TIFFs | `--replace`/GDAL backend and real raster output comparison not exercised here; externally validated compression behaviour pending |
| `gdal-python/split_vector_by_attribute.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/vacuum_geopackages.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/ARF-calcs.py` | No | No | Source inspected; provisional repository-only reference/calculation utility; owner pending | Protected example `__main__` with plotting; no standalone argparse/no-pause or tested CLI exit | Pandas/Matplotlib regional ARF parameters; validate formula, domain, provenance and plots against ARR before distribution |
| `hydrology-python/EAS.py` | No | No | Source inspected; provisional repository-only educational example; owner pending | Protected example `__main__`; no argparse/no-pause or tested CLI exit | NumPy/SciPy/Shapely/Matplotlib equal-area-slope demo; saves visual plots; hydrological parity/input-domain checks needed |
| `hydrology-python/RFFE_Only_v6.py` | No | No | Source inspected; provisional repository-only network client; owner pending | Guarded argparse CLI; no-pause and errors not yet accepted | Requests/Pandas/Loguru + ryan_library; batch HTTP POST to RFFE URL; live external service and raw/combined CSV outputs unverified |
| `hydrology-python/RFFE_Only_v6_standalone.py` | No | No | Source inspected; provisional repository-only alternative client; owner pending | Guarded argparse CLI; no-pause and errors not yet accepted | Standalone Requests/Pandas/Loguru variant; batch HTTP POST and CSV exports; confirm duplication and API security before lifecycle decision |
| `hydrology-python/aep_ari_conversions.py` | No | No | Source inspected; provisional repository-only pure helper; owner pending | No CLI or `__main__` (library-like); numerical round-trip/domain pytest covers functions, not installed execution | Only math dependency; AEP/ARI formula and invalid finite-domain behavior tested in `test_repository_only_hydrology.py`; hydrological design acceptance pending |
| `hydrology-python/combine_ifds_v2.py` | No | No | Source inspected; provisional repository-only legacy BOM CSV combiner; owner pending | Guarded `__main__`; no argparse/no-pause or tested exit contract | Reads script-adjacent IFD CSV variants; writes combined AEP lookup CSV; CSV schema and current BOM download parity unverified |
| `misc-python/heic_to_jpeg.py` | No | No | Source inspected; provisional repository-only photo utility; owner pending | Guarded argparse CLI; optional dependency import can raise `SystemExit`; help not portable without `pillow-heif` | Pillow and pillow-heif, multiprocessing and filesystem copies/conversions; default placeholder P: paths; EXIF/GPS integrity and overwrite policy unverified |
| `misc-python/remove_excel_protection.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `other/PDFXChangeEditorSettings_250206.xces` | No | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `other/audit_qgis_projects.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `other/shapefile-explorer-types.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/break_pdf_signatures.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/ocg_clean_file.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/ocg_clean_file_alternative.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/pypdf_decrypt.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/remove_pdf_restrictions.py` | No | No | Not assessed | Not assessed | Not assessed |
| `point-cloud-python/clean_xyz.py` | No | No | Source inspected; keep repository-only until destructive in-place behaviour accepted; owner pending | Guarded argparse CLI with worker options; no validated dry-run/backup or no-pause contract | Atomically replaces affected XYZ files in place; no backup/dry-run; test on copied data before promotion |
| `point-cloud-python/clip_las_to_polygon.py` | No | No | Source inspected; provisional repository-only; owner pending | Guarded argparse CLI; error codes and no-pause contract untested | Fiona/laspy/Shapely/NumPy; clips LAS/LAZ using shapefile, creates outputs and temporary files; uses `.resolve()` default; CRS/chunking need validation |
| `point-cloud-python/parallel-reorder-xyz-updated.py` | No | No | Source inspected; provisional repository-only project utility; owner pending | Guarded project-default main; no argparse/no-pause or tested exit semantics | Hard-coded working directory; pandas/NumPy/multiprocessing; fills missing XY with -9999 and writes `*_mod.xyz`; grid order/data parity unverified |
| `powershell/date_to_front_of_msg_v3.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `powershell/self-signing.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py` | No | No | Source inspected; provisional repository-only project experiment; owner pending | Guarded `main()` followed by blocking Enter prompt; no argparse/no-pause; cannot run headless unchanged | Script-adjacent Opt2 RORB CSV inputs; writes timestamped Excel and plots; filename assumptions and output parity require project-data validation |
| `raster-python/square_raster_cells.py` | No | No | Source inspected; provisional repository-only bespoke raster utility; owner pending | Guarded main, editable file/cell-size constants; no argparse/no-pause/exit acceptance | Rasterio and ryan_library GDAL reader; changes raster extent while retaining array size; spatial output requires explicit GIS QA |
| `raster-python/tif_to_xyz_drop_na_v1.py` | No | No | Source inspected; provisional repository-only legacy converter; owner pending | Guarded main, fixed parallel defaults; unconditional Windows shell pause; no headless support | Rasterio/Pandas/NumPy; in-memory raster-to-XYZ CSV and threads; hard-coded working/output folders, output and pause need review |
| `raster-python/tif_to_xyz_drop_na_v2.py` | No | No | Source inspected; provisional repository-only chunked converter; owner pending | Guarded main, fixed worker defaults; unconditional `pause_console()`; no headless support | Rasterio/NumPy/ryan_library GDAL; chunked XYZ CSV, fixed working/output folders; spatial sampling/nodata output not verified |
| `repair-folder-permissions.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `tuflow/PROJECT_SETUP.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `tuflow/find_missing_tlf_runs.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `tuflow/init_tuflow_project.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `tuflow/tuflow_culvert_from_1d_nwk.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `tuflow/tuflow_culvert_from_maximums.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `tuflow/tuflow_to_hy8.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `unsorted-bat/VACUUM.BAT` | No | No | Not assessed | Not assessed | Not assessed |
| `unsorted-bat/del_xf.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `unsorted-bat/dir.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `unsorted-bat/dir_recursive.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `unsorted-bat/shutdown-a.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `unsorted-python/convert_shapefiles_to_gpkg.py` | No | No | Standalone unreviewed, repository-only | Standalone CLI/interface to assess before promoting | GDAL/Fiona/TUFLOW template GeoPackages required |
| `unsorted-python/export_water_level_profile_tables_50m.py` | No | No | Not assessed | Not assessed | Not assessed |
| `unsorted-python/plot_water_level_profiles.py` | No | No | Not assessed | Not assessed | Not assessed |


## Bounded selected-source inspections — PO/POMM and raster workflows

Ten more **wheel-selected** wrappers were individually inspected, in addition
to the prior twenty selected RORB/TUFLOW sources. The current selected-source
inspection tranche therefore covers **30 files**, with original output/CLI
acceptance still pending. The new tranche includes TUFLOW result combiners,
mean/median reports, an independent SQLite GeoPackage reader, project-specific
scenario plots, water-level profiles and native raster processing.

Potentially consequential behaviours include project-configured paths, an
interactive plotting workflow, a velocity-masking utility that replaces TIFFs
after creating backups, and native ASC_to_ASC jobs. Source inspection alone
does **not** verify those operations. No wheel selection was changed.

## Bounded individual source inspections — selected RORB and TUFLOW wrappers

Twenty additional **wheel-selected** source files were individually read and
given specific behavioural/dependency evidence in the rows above. These
include seven RORB/culvert wrappers, three TUFLOW GIS scripts, two log
processing scripts, four model-management scripts and four PO/series utilities.
Their lifecycle status is **provisional**, not maintainer approval.

The highest-risk selected scripts in this tranche include a GeoPackage
rebuild/replace operation, duplicate-file deletion, batch file collection,
master-workbook mutation and launching TUFLOW executables. Existing manifest
membership is preserved while their safety, original defaults and engineering
output acceptance are examined. The additional CLI-help and invalid-input
subprocess regressions are tracked separately, and **do not establish**
full installed-wheel parity or real TUFLOW/GDAL/QGIS application readiness.

## Bounded individual source inspections — point-cloud, raster and image group

Seven additional repository-only sources have been inspected, taking the
bounded source-inspection tranche to **28 files**. The mutable
`point-cloud-python/clean_xyz.py` replaces XYZ data without a backup or
dry-run, the two legacy TIFF-to-XYZ converters pause unconditionally, and
`misc-python/heic_to_jpeg.py` may raise at import when optional imaging
dependencies are missing. They remain excluded from wheel/MCP promotion.
No destructive workflow was executed or approved in this source audit.

## Bounded individual source inspections — legacy hydrology/RORB group

Eight additional **repository-only** Python files have source-specific
lifecycle evidence above, bringing this source-inspected tranche to
**21 files** (six 12D, seven AutoCAD, eight hydrology/RORB). The unguarded
`RORB-python/rorb-peaks_v2.py` performs input scanning, may raise, and writes
CSV at import time; its repository-only disposition must be preserved.
The AEP/ARI mathematical helper is deliberately not promoted to the wheel;
`tests/scripts/test_repository_only_hydrology.py` adds bounded pure numerical
tests of that source file's formula and invalid-input contract.

The two RFFE variants contact an external HTTP service and require deliberate
duplication, transport, ownership, and environment review before any lifecycle
promotion. The source checks do not validate live RFFE responses, ARR design
methods, performance, or other output parity; maintainers retain final approval.

## Bounded installed-wrapper behaviour evidence

Four selected wrappers have named source-tree subprocess regressions in
`tests/scripts/test_installed_wrapper_behaviour.py`. These tests run
disposable data with `--no-pause`. The existing isolated-wheel smoke is a
**separate** package gate and does not prove all resources or external tools.
The ledger above now distinguishes verified safe/error paths from outstanding
original-default, Excel, GDAL, RORB plotting and Windows network acceptance.
Neither a passing smoke test nor a successful source inspection constitutes
maintainer sign-off for the wrapper's complete original behaviour.

## Bounded individual source inspections — 12D and AutoCAD group

Thirteen **repository-only** Python files (six 12D and seven AutoCAD) have been
read to establish initial lifecycle evidence in the corresponding ledger rows.
Their existing exclusion is retained: the majority are project-configured
utilities without a general-purpose installed CLI; one DXF conversion script
has an argparse interface but still lacks a tested packaging decision.

This is **source inspection**, not complete A/B acceptance. The named owner,
original output/default parity, import-time runtime behaviour, headless exit
semantics and relevant 12d/CAD/GDAL/QGIS integration tests remain unverified.
No source files were deleted, imported for analysis, or added to the wheel.
Maintainers should explicitly approve future promotion, retirement or ownership
changes, particularly for project-specific scripts with fixed network paths.

## Generate reproducible static evidence

Run from the repository root, without executing any candidate script:

```powershell
python repo-scripts/audit_installed_scripts.py --check --output script-inventory-audit.json
```

The output enumerates Git-tracked files only (never traverses submodules), whether
they are included in the current wheel or targeted by MCP, and Python AST signals
including syntax status, top-level call expressions, main guard, embedded wrapper
version, argparse/no-pause literals, `__file__` references, absolute drive literals
and subprocess imports. The `--check` option rejects manifest/catalogue paths
missing from the tracked tree; it does **not** mark scripts as safe.

Static indicators are only an input to the manual file-by-file assessment.
Unparsed/legacy scripts remain marked pending; no heuristic automatically
moves a script into/out of the wheel or MCP catalogue.

## Review evidence added in PR #137

- The wheel manifest now excludes `file-management-python/copy_file.py` and
  `file-management-python/copy_folder.py`. Both execute authentication and
  network copy operations during import. The original editable scripts are
  retained in the repository, but they are **not installed nor MCP exposed**.
- `repo-scripts/audit_installed_scripts.py --check` now inspects the tracked
  149-file inventory and rejects selected Python scripts with missing main
  guards, syntax problems or detected import-time mutators. The test suite
  verifies every selected Python row, plus both excluded network copy scripts.
- The new `file_inventory.py` wrapper is selected after a protected-entry-point review and disposable CSV test. The new read-only TUFLOW activity wrapper from merged main is also selected, with Windows/CodeMeter acceptance pending.
- The existing `ryan-scripts list` and `ryan-scripts copy` operations expose
  a narrower *original-file* wrapper selection than the wheel file count:
  `README.md`, package `__init__.py` and other companion documentation are
  distributed resources, not human-facing executable wrappers.
- The separate MCP catalogues currently reference 33 distinct script paths.
  An installed script is **not automatically MCP-catalogued**.
- Static checks do not validate original defaults, CLI output/exit codes,
  external binaries, GDAL/QGIS/TUFLOW/HY-8 calculations or network paths.
  These require per-workflow tests or external application acceptance. Legacy
  scripts without an argparse interface should not be advertised as supporting
  `--help` or `--no-pause` merely because they are packaged.

## Required disposition before calling the inventory complete

For each executable row, record an owner/reviewer and evidence-backed lifecycle and
one of **maintained package**, **packaged but not MCP catalogued**, **repository only**
or **remove from packaging**; justify that disposition with import-time, CLI and
external-tool test evidence. Verify companions and resource dependencies in the wheel.

Priority: currently MCP catalogued scripts, destructive/privileged scripts, excluded
source files that were shipped before PR #130, and frequently copied TUFLOW/GDAL/RORB
utilities. Do not change the current 83-file allowlist from this inventory alone:
make deliberate, tested additions/removals once reviewed.

## Completion definition

This ledger is **not complete** until the assessment cells are replaced with
actual findings for all relevant executable scripts and the decisions are accepted
in the [living requirements record](../work/2026-10-17-installed-scripts-mcp-pip-distribution.md).
