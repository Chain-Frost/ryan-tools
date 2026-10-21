# Workstream C — curated MCP target assessment (21 October 2026)

**Review base:** PR #137 head `a453f6f`. **Scope:** 35 workflow IDs sharing **33 distinct installed-script targets** (27 general workflows plus 8 GDAL workflows). Metadata and wrapper files were inspected; execution of proprietary applications and complete CLI/output parity **were not established**. Wheel selection remains provisional and unchanged.

## Readiness language

- **Catalogue / file reviewed:** path and metadata inspected at the review SHA; isolated synthetic tests can prove path discovery without executing the wrapper.
- **Entry point only:** discovery proves neither import compatibility nor a functioning QGIS/GDAL/TUFLOW/HY-8 installation.
- **External acceptance pending:** no real Windows network drive, QGIS 4, GDAL/OGR, licensed TUFLOW or HY-8 execution was performed here.
- **MCP is non-executing:** generated commands are proposals to a separate shell. `privileged` must be explicitly enabled and user approval remains mandatory.

## Individual targets

| Script path relative to `ryan-scripts/` | Curated workflow ID(s) | Exposure and effect | Safety, dependencies and acceptance findings |
| --- | --- | --- | --- |
| `other/audit_qgis_projects.py` | `audit_qgis_projects` | analysis; read_only | Read-only project-source audit; QGIS project reference resolution, paths and output require QGIS 4 checks. |
| `culvert.py` | `culvert_analysis_design` | create; creates_outputs | Creates hydraulic reports; native solver dependency and engineering result parity independent. |
| `floodway.py` | `floodway_design_assessment` | create; creates_outputs | Creates overtopping/design outputs; native solver and input schemas require engineering data. |
| `TUFLOW-python/log_processing/create_log_summary_report.py` | `tuflow_log_summary` | create; creates_outputs | Creates workbook; process logs from synthetic copies and check missing-data failures. |
| `TUFLOW-python/po_and_timeseries/combine_pomm_results.py` | `tuflow_pomm_combine` | create; creates_outputs | Creates combined POMM outputs; check report naming and default target folder. |
| `TUFLOW-python/po_and_timeseries/combine_po_results.py` | `tuflow_po_combine` | create; creates_outputs | Creates combined PO outputs; check report naming and default target folder. |
| `TUFLOW-python/po_and_timeseries/create_pomm_mean_peak_report.py` | `tuflow_pomm_mean_peaks` | create; creates_outputs | Creates POMM mean summaries; supports working-directory and locations overrides. |
| `TUFLOW-python/po_and_timeseries/create_pomm_median_peak_report.py` | `tuflow_pomm_median_peaks` | create; creates_outputs | Creates POMM median summaries; supports working-directory and locations overrides. |
| `TUFLOW-python/po_and_timeseries/calculate_po_closure_durations.py` | `tuflow_closure_durations` | create; creates_outputs | Creates duration outputs; validate threshold semantics with reference series. |
| `TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py` | `tuflow_culvert_mean_peaks` | create; creates_outputs | Creates mean culvert peaks; confirm no source overwrites. |
| `TUFLOW-python/culvert_results/create_median_culvert_peak_report.py` | `tuflow_culvert_median_peaks` | create; creates_outputs | Creates median culvert peaks; confirm no source overwrites. |
| `TUFLOW-python/culvert_results/combine_culvert_maximums.py` | `tuflow_culvert_maximums` | create; creates_outputs | Combines maximums; validate output paths and input preservation. |
| `TUFLOW-python/culvert_results/combine_culvert_timeseries.py` | `tuflow_culvert_timeseries` | create; creates_outputs | Combines timeseries; validate output paths and input preservation. |
| `tuflow/tuflow_culvert_from_maximums.py` | `tuflow_culvert_evaluate_maximums`, `tuflow_culvert_evaluate_maximums_hy8` | create / privileged; creates_or_replaces / executes_external | Native creates/replaces CSV only on explicit overwrite; HY-8 mode external/privileged; workspace handling needs Windows run. |
| `tuflow/tuflow_culvert_from_1d_nwk.py` | `tuflow_culvert_evaluate_1d_nwk`, `tuflow_culvert_evaluate_1d_nwk_hy8` | create / privileged; creates_or_replaces / executes_external | Native creates/replaces CSV only on explicit overwrite; HY-8 mode external/privileged; GeoPackage/1d_nwk and workspace acceptance needed. |
| `TUFLOW-python/po_and_timeseries/check_po_timeseries_peaks.py` | `tuflow_timeseries_peak_check` | create; creates_outputs | Creates peak-check outputs; check missing/invalid series and diagnostic exit. |
| `TUFLOW-python/po_and_timeseries/check_timeseries_stability.py` | `tuflow_timeseries_stability` | create; creates_outputs | Creates stability outputs; check timeseries/CLI result-type options. |
| `TUFLOW-python/po_and_timeseries/plot_water_level_profiles.py` | `tuflow_water_level_profiles` | create; creates_outputs | Creates plots/profiles; has --overwrite-existing flag, GPKG layer/CRS checks and GIS dependencies. |
| `RORB-python/RORB-find-closure-durations.py` | `rorb_closure_durations` | create; creates_outputs | Creates closure outputs; not an RORB executable launch; verify units and thresholds. |
| `RORB-python/plot_ensemble_results.py` | `rorb_ensemble_plots` | create; creates_outputs | Creates plots; separate input/output CLI; verify source-preserving failure. |
| `TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py` | `tuflow_log_summary_append_master` | privileged; in_place | Changes existing XLSX table in-place; project-specific default Q: path; use disposable workbook only. |
| `TUFLOW-python/raster_processing/run_grouped_asc_to_asc_statistics.py` | `tuflow_asc2asc_groups` | privileged; executes_external | Runs external ASC_to_ASC via --executable; has --dry-run; requires actual executable and synthetic raster tests. |
| `TUFLOW-python/model_management/run_tuflow_simulations.py` | `run_tuflow_batch` | privileged; executes_external | Configured TUFLOW executable, run_simulations=True and pause_on_finish=True; NOT a standard help CLI, never invoke on real project for probing. |
| `TUFLOW-python/model_management/delete_duplicate_check_files.py` | `delete_duplicate_tuflow_files` | privileged; deletes | DRY_RUN=False; requires typed DELETE confirmation but writes proposal/report even when cancelled; no safe --help. |
| `misc-python/remove_excel_protection.py` | `remove_excel_protection` | privileged; creates_or_replaces | Modifies/replaces workbook protection; preserve input and test on disposable file. |
| `gdal-python/recompress_geotiffs.py` | `audit_and_recompress_geotiffs` | privileged; in_place | Default audit-only, but --replace --yes can replace GeoTIFFs; now privileged/in-place; stage and compare bytes/raster on copies. |
| `gdal-python/gdal_translate_TIF_ovr.py` | `translate_rasters_to_geotiff` | create; creates_outputs | GDAL raster conversion; --overwrite available, not in suggested scenarios; test FLT/ASC/TIF output. |
| `gdal-python/gdaladdo_tif_pyramids.py` | `build_external_overviews` | create; creates_or_replaces_sidecars | Creates/refreshes .ovr sidecars; --refresh may replace sidecars; preserve source .tif. |
| `gdal-python/gdal_raster_footprint.py` | `create_raster_footprints` | create; creates_outputs | Creates GPKG/SHP output; --overwrite available; test layer names and CRS. |
| `gdal-python/batch_vector_clip.py` | `batch_vector_clip` | privileged; executes_external | Executes ogr2ogr subprocess with bounded workers; --overwrite, --dry-run and --executable remain CLI options. Explicit approval required; executable presence is unverified. |
| `gdal-python/gdal_stage_storage.py` | `calculate_stage_storage` | create; creates_outputs | Creates CSV/plots; --dry-run and --overwrite; not yet production-validated in wrapper comments. |
| `gdal-python/gdalwarp_clip_to_polygon.py` | `clip_rasters_to_polygon` | create; creates_outputs | Uses GDAL Python osgeo import; verify availability inside QGIS/OSGeo4W environment and output safety. |
| `gdal-python/split_vector_by_attribute.py` | `split_vector_by_attribute` | create; creates_outputs | Creates vector outputs, may replace using temp files; inspect input/output path handling. |

## Corrections and open findings

1. **Corrected:** `audit_and_recompress_geotiffs` advertised `--replace --yes` in its default `create` catalogue. The MCP GDAL conversion now treats this workflow as `privileged`, `in_place` and explicitly approval-required; the underlying wrapper and manifest are unchanged. Read-only audit may still be run directly by a human, but a single mixed-capability MCP workflow cannot safely advertise an in-place scenario in `create`.
2. **Corrected:** catalogue script targets now reject absolute, traversal, empty-segment and Windows-backslash paths before command discovery. GDAL script targets use the same validation.
3. **Confirmed existing:** `run_tuflow_batch` and `delete_duplicate_tuflow_files` already have `help_safe=false` in the catalogue; never use `--help` as a discovery smoke for these wrappers. Both are privileged and require approval; the former has a configured real-run default, the latter can write audit files before deletion confirmation.
4. **Corrected:** `batch_vector_clip` launches `ogr2ogr` subprocesses, and now requires `privileged` with explicit approval and `executes_external` mutation; its declared external program is `ogr2ogr`. CLI defaults remain unchanged, including `--dry-run` and `--overwrite`. Its existing overwrite implementation removes destination dataset members before promoting replacement sidecars, so replacement failure can lose prior outputs. **Do not approve `--overwrite` on production data until Agent 1 reviews and fixes rollback semantics.**
5. **Corrected:** installed and checkout-based MCP discovery now refuses symlinked/junction-linked script files or path components rather than advertising a command into an arbitrary external file. This is a discovery policy, not a filesystem sandbox for independently run wrappers. Both the TUFLOW batch executable (configured by the wrapper) and `asc_to_asc_w64.exe` are now explicitly declared as external prerequisites, with status **not verified**.\n6. **Needs subsequent engineering validation:** historic wrappers with editable or project-specific defaults may not be headless/portable despite declaring `--no-pause` or providing a discoverable path. Agent 1 owns lifecycle and installed-wrapper parity. No new scripts were exposed.

## Gate tracking

| Gate | Current evidence | Still needed |
| --- | --- | --- |
| C — 33 target entries/metadata/path sanity | Per-target source and catalogue inspection, installed-file membership and catalogue regression tests | Individual default/parameter behavioural runs, versions, dependency import acceptance |
| C — safety/profile separation | Regression for replacement, deletion, external execution, symlink/junction traversal and invalid paths | Maintainer sign-off for GDAL recompression profile change and subprocess-based vector clip |
| C — installed wheel without checkout | Simulated site-packages layout + installed-wheel CI smoke | Actual wheel isolation check in Windows acceptance document |
| F — spaces/Unicode/long path/UNC/mapped-drive | Disposable synthetic tests with opt-in Windows/share cases | Execution on Windows with real mapped drive and UNC test share |
| F — applications | Static executable identification, mock checks and opt-in executable-backed GDAL/OGR fixture tests | Real QGIS 4/GDAL, TUFLOW and HY-8 acceptance and input/output comparisons |

See [Windows engineering acceptance](2026-10-21-cf-windows-acceptance.md) for runnable procedures. Record real executable versions, exact Python runtime, path kind, OS, stdout/stderr, exit codes, input hashes and evidence files when executed.

**Coordination with Agent 1:** This change does not edit the wheel manifest, installed wrappers, source inventory or central A/B audit. Agent 1 must reassess any wheel-selection or wrapper-behaviour disposition that the external acceptance later shows to be defective. Add these C/F records to the central docs/work index when coordinating the final PR #137 integration, rather than independently overwriting another agent's working ledger.
