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
| `12D-python/generate_culvert_lines.py` | No | No | Not assessed | Not assessed | Not assessed |
| `12D-python/generate_culvert_polygons.py` | No | No | Not assessed | Not assessed | Not assessed |
| `12D-python/thin-raster-terrain-for-12D_v5.py` | No | No | Not assessed | Not assessed | Not assessed |
| `12D-python/tif-to-LAS-valid-only_v6.py` | No | No | Not assessed | Not assessed | Not assessed |
| `12D-python/tif-to-csv-valid-only_v6.py` | No | No | Not assessed | Not assessed | Not assessed |
| `12D-python/tif-to-csv-valid-only_v6a.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/dwg-polygons-txt-to-csv.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/dwg-to-points.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/group-by-entity-colour-and-dissolve-to-hatch.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/group-by-entity-colour-and-dissolve-to-hatch_dask3.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/group-by-entity-colour-and-dissolve.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/group-by-entity-colour-and-dissolve3.py` | No | No | Not assessed | Not assessed | Not assessed |
| `AutoCAD-python/group-by-entity-colour.py` | No | No | Not assessed | Not assessed | Not assessed |
| `GlobalMapper/DXF-to-float_v1.gms` | No | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `README.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `RORB-python/RORB-find-closure-durations.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `RORB-python/extract_rainfall-to-TUFLOW.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `RORB-python/hydrograph_extract_rorb_csv.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `RORB-python/plot_ensemble_results.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `RORB-python/rorb-peaks_v2.py` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-bat/asc2asc_max_by_search.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-bat/asc_to_asc-DIFF.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-bat/asc_to_asc-MAX.bat` | No | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/README.md` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/culvert_results/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/culvert_results/combine_culvert_maximums.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/culvert_results/combine_culvert_timeseries.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/culvert_results/create_median_culvert_peak_report.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/gis_processing/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/gis_processing/apply_qgis_styles_to_results.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/gis_processing/rename_geopackage_layer_to_filename.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/gis_processing/split_network_layers_by_culvert_id.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/log_processing/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/log_processing/check_tuflow_user_activity.py` | Yes | No | Provisional: protected main and embedded version; inspect local configuration | argparse help/no-pause; external environment still unverified | Read-only ProgramData TUFLOW logs, local CodeMeter optional; verify on Windows |
| `TUFLOW-python/log_processing/create_log_summary_report.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/model_management/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/model_management/collect_result_and_check_files.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/model_management/delete_duplicate_check_files.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/model_management/run_tuflow_simulations.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/model_management/verify_converted_tuflow_rasters.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/po_and_timeseries/calculate_po_closure_durations.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/check_po_timeseries_peaks.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/check_timeseries_stability.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/combine_po_results.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/combine_pomm_results.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/create_pomm_mean_peak_report.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/create_pomm_median_peak_report.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/generate_2d_po_label_list.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/plot_po_scenario_timeseries.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/po_and_timeseries/plot_water_level_profiles.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/raster_processing/__init__.py` | Yes | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `TUFLOW-python/raster_processing/calculate_raster_differences.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/raster_processing/mask_velocity_by_depth.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/raster_processing/run_asc_to_asc_maximum_searches.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `TUFLOW-python/raster_processing/run_asc_to_asc_mean_then_maximum.py` | Yes | No | Not assessed | Not assessed | Not assessed |
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
| `file-management-python/file_inventory.py` | Yes | No | Provisional: protected main and embedded version | argparse help/no-pause and CSV smoke; originals preserved | Project data filesystem and optional Excel output; runtime checks retained in CI |
| `file-management-python/list_files_recursive.py` | Yes | No | Not assessed | Not assessed | Not assessed |
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
| `gdal-python/recompress_geotiffs.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/split_vector_by_attribute.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `gdal-python/vacuum_geopackages.py` | Yes | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/ARF-calcs.py` | No | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/EAS.py` | No | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/RFFE_Only_v6.py` | No | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/RFFE_Only_v6_standalone.py` | No | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/aep_ari_conversions.py` | No | No | Not assessed | Not assessed | Not assessed |
| `hydrology-python/combine_ifds_v2.py` | No | No | Not assessed | Not assessed | Not assessed |
| `misc-python/heic_to_jpeg.py` | No | No | Not assessed | Not assessed | Not assessed |
| `misc-python/remove_excel_protection.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `other/PDFXChangeEditorSettings_250206.xces` | No | No | Resource/companion; verify dependencies | N/A | Check packaging / consumers |
| `other/audit_qgis_projects.py` | Yes | Yes | Not assessed | Not assessed | Not assessed |
| `other/shapefile-explorer-types.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/break_pdf_signatures.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/ocg_clean_file.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/ocg_clean_file_alternative.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/pypdf_decrypt.py` | No | No | Not assessed | Not assessed | Not assessed |
| `pdf-python/remove_pdf_restrictions.py` | No | No | Not assessed | Not assessed | Not assessed |
| `point-cloud-python/clean_xyz.py` | No | No | Not assessed | Not assessed | Not assessed |
| `point-cloud-python/clip_las_to_polygon.py` | No | No | Not assessed | Not assessed | Not assessed |
| `point-cloud-python/parallel-reorder-xyz-updated.py` | No | No | Not assessed | Not assessed | Not assessed |
| `powershell/date_to_front_of_msg_v3.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `powershell/self-signing.ps1` | No | No | Not assessed | Not assessed | Not assessed |
| `python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py` | No | No | Not assessed | Not assessed | Not assessed |
| `raster-python/square_raster_cells.py` | No | No | Not assessed | Not assessed | Not assessed |
| `raster-python/tif_to_xyz_drop_na_v1.py` | No | No | Not assessed | Not assessed | Not assessed |
| `raster-python/tif_to_xyz_drop_na_v2.py` | No | No | Not assessed | Not assessed | Not assessed |
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
