# Script remediation and acceptance backlog — PR #137

**Status: open; not a merge gate completion certificate.** This is the durable
work queue for the A/B audit of `ryan-scripts/`, maintained alongside the
[149-file lifecycle inventory](2026-10-19-installed-scripts-inventory.md)
and [original requirements](2026-10-17-installed-scripts-original-requirements.md).
New findings about individual scripts must be added or updated here, not lost
inside CI logs or an informal PR comment.

## How to use this record

- Each path below is relative to `ryan-scripts/`. **Fix** means code may need
  changing; **Decision** requires the maintainer to approve lifecycle/CLI
  semantics; **Verify** needs tests and evidence; **External** needs a configured
  Windows engineering application or representative engineering data.
- **Open** means identified through source inspection, **not** yet fixed or
  acceptance-tested. **Partially verified** means only the named bounded
  regression exists. **Blocked** indicates a decision, fixture, or application
  still required. A source-inspected ledger row is not a completed task.
- Do not silently change existing defaults, retire scripts, alter package or
  MCP allowlists, or execute destructive workflows on production data.
  Any proposed change to a selected script first needs existing-user
  compatibility assessment and focused regression coverage.
- Agent 1 owns A/B implementation and this record; Agent 2 owns MCP/C and
  external-environment/F acceptance. Coordinate overlapping integration
  evidence without modifying each other's files.
- Close an item only with a commit/test or documented maintainer decision
  linked here. Keep items visible if only a synthetic test passed.

## Priority script work

| ID | Priority / kind | Script | Work still required and acceptance condition | State |
| --- | --- | --- | --- | --- |
| AB-001 | High / Decision + Fix | `RORB-python/rorb-peaks_v2.py` | Runs `os.chdir`, scans RORB outputs, may raise and writes CSV **at import time**. Keep repository-only. If retained as a supported CLI, guard execution and validate extraction from real batch results before any packaging decision. | Open; excluded |
| AB-002 | High / Decision + Fix | `TUFLOW-python/gis_processing/rename_geopackage_layer_to_filename.py` | Selected despite GUI keypress, no standard `--no-pause`, replacement of original single-layer GeoPackage and caught errors without reliable nonzero exit. Define atomic metadata-safe operation, multi-layer policy, skip/success diagnostics and copied-GPKG tests. Coordinate with parallel PR #143 before changing this wrapper. | Open; do not duplicate PR #143 work |
| AB-003 | High / Decision + Fix | `TUFLOW-python/model_management/delete_duplicate_check_files.py` | Selected destructive deduplicator: verify typed confirmation/dry-run, symlinks/junctions, same-file identity, placeholders, retention, rollback, audit manifests and interrupted runs using disposable trees. Never run against real check outputs in CI. | Open |
| AB-004 | High / Decision + Fix | `TUFLOW-python/raster_processing/mask_velocity_by_depth.py` | Project-default Q: path; moves the original velocity TIFF to a backup and produces a replacement. Require explicit input/confirmation, idempotence and CRS/nodata/resampling tests before calling the selected workflow safe. | Open |
| AB-005 | High / Decision + Fix | `file-management-python/archive_extract.py` | Existing temporary `.<stem>.extracting` folder is removed before extraction. Verify archive path traversal, symlinks/junctions, stale workspace safety, independent extraction destinations, cancellation/partial failure and original archive integrity. Decide bounded worker count and no unexpected deletions. | Open |
| AB-006 | High / Decision + Fix | `gdal-python/gdal_raster_to_xyz.py` | On `--overwrite`, existing output is `unlink`ed before the staged file is promoted; a failed promotion could lose the old result. Review atomic replacement and cross-filesystem/Windows behaviour; test failure injection and preservation of pre-existing output. | Open |
| AB-007 | High / Decision + Fix | `gdal-python/gdal_set_nodata.py`, `gdal-python/gdal_unset_nodata.py` | Both modify TIFF metadata in place. Decide whether explicit mutation approval/dry-run is needed. Validate band selection, pixel preservation, nodata and transparency in QGIS/GDAL, errors and no-overwrite assumptions. | Open |
| AB-008 | High / Decision + Fix | `gdal-python/vacuum_geopackages.py` | Runs SQLite `VACUUM` directly against selected GeoPackages without a dry-run/backup option. Establish opt-in/backup and locked/invalid database safety, preserve layer schemas/metadata and use only disposable GPKG fixtures for tests. | Open |
| AB-009 | High / Decision + Fix | `TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py` | Defaults to a project Q: workbook and appends rows. Confirm explicit workbook selection, duplicate recognition, workbook/Excel-table preservation, lock handling, backup/atomicity and appropriate nonzero errors. | Open |
| AB-010 | High / Decision + Fix | `docx-python/word_style_audit.py` | Non-null `HARDCODED_DOCX_PATH` currently overrides **even an explicit positional DOCX CLI argument**, pointing to one local C: file. Obtain precedence decision and test intended `--out-dir`/missing-DOCX/report semantics; don't silently change a deliberately editable default. | Open |
| AB-011 | High / Decision + Fix | `RORB-python/hydrograph_extract_rorb_csv.py`, `python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py` | Each waits for Enter after `main()` and lacks a supported `--no-pause`; the first is wheel-selected. Preserve interactive double-click behaviour but specify safe headless invocation and explicit failure codes, with copied RORB CSV fixtures. | Open |
| AB-012 | High / Decision + Verify | `TUFLOW-python/model_management/run_tuflow_simulations.py` | Windows/TUFLOW launcher uses configurable binary/project paths and can execute many models. Validate correct script defaults, no unintended launches, `--demo-dashboard` behaviour, command quoting, event/scenario expansion, progress and Ctrl-C, licensing, exit status and network paths on a controlled workstation. | Blocked: local TUFLOW |
| AB-013 | High / Decision + Verify | `tuflow/init_tuflow_project.py` | Creates templates and asks a real TUFLOW executable for canonical empty GIS files. Verify `--overwrite` refusal/preservation, copied resources, path independence of installed wheel and correct project scenarios; C: executable is only an editable default, not guaranteed user environment. | Blocked: local TUFLOW |
| AB-014 | Medium / Fix + Verify | `point-cloud-python/clean_xyz.py` | Repository-only XYZ cleaner atomically replaces input files with no dry-run or backup. Decide backup/preview requirements before any use or promotion, validate sentinel matching and malformed rows on copied samples. | Open; excluded |
| AB-015 | Medium / Fix + Verify | `raster-python/tif_to_xyz_drop_na_v1.py`, `raster-python/tif_to_xyz_drop_na_v2.py` | Repository-only converters have unconditional Windows/console pause, project-default folders and distinct memory/chunking behaviour. Retain exclusion; compare cell-centre/nodata/row order and add headless handling only if maintainers adopt them. | Open; excluded |
| AB-016 | Medium / Decision + Verify | `hydrology-python/RFFE_Only_v6.py`, `hydrology-python/RFFE_Only_v6_standalone.py` | Two RFFE clients target an HTTP service. Decide supported implementation and service endpoint/transport policy; validate request/response schema, unsafe filenames, rate limits, failures and CSV outputs against controlled fixtures before promotion. | Open; excluded |
| AB-017 | Medium / Fix + Verify | `TUFLOW-python/po_and_timeseries/plot_po_scenario_timeseries.py` | Wheel-selected but project-specific Q: results/cache and interactive plots without a normal CLI/no-pause path. Decide whether repository-only is more appropriate; verify scenario filtering and a headless plot/export path if retained. | Open |
| AB-018 | Medium / Decision + Verify | `gdal-python/audit_CRS.py` | Selected script has placeholder Q: roots, hard-coded CSV report, uses `Path.resolve()` for routine output and has no argparse/no-pause. Determine if a reusable CLI is intended and verify GPKG/FLT/GeoTIFF CRS reporting in GDAL-enabled Windows. | Open |
| AB-019 | Medium / Verify | `file-management-python/archive_compress_folder.py`, `file-management-python/archive_compress_individual.py` | Prove 7-Zip discovery, existing archive skip semantics, input preservation, excluded folders/extensions, exit codes, no-pause and concurrent worker behaviour on disposable folders/UNC. | Blocked: local 7-Zip acceptance |
| AB-020 | Medium / Verify | `gdal-python/build_VRT.py`, `gdal-python/gdal_merge.py`, `gdal-python/gdal_merge_by_extent.py` | Use synthetic rasters to validate filename grouping, CRS preconditions, extent filtering, actual VRT/GeoTIFF content, nodata/resampling, sidecars and original-default compatibility. | Open |
| AB-021 | Medium / Verify | `gdal-python/gdalwarp_clip_to_polygon.py`, `gdal-python/batch_vector_clip.py`, `gdal-python/split_vector_by_attribute.py` | Test clipped outputs with generated vector/raster data; check output collision, SQL quoting, attribute NULL/Unicode, CRS, failure rollback and overwrite boundaries, including native GDAL. | Blocked: GDAL integration |
| AB-022 | Medium / Verify | `gdal-python/gdal_stage_storage.py`, `gdal-python/gdal_flood_extent.py`, `gdal-python/gdal_raster_footprint.py` | Verify elevation-volume units, nodata, mask polygons, cell areas and input/output formats with reference datasets and known answers; keep staged outputs/overwrites safe. | Open |
| AB-023 | Medium / Verify | `tuflow/tuflow_culvert_from_1d_nwk.py`, `tuflow/tuflow_culvert_from_maximums.py`, `tuflow/tuflow_to_hy8.py`, `culvert.py`, `floodway.py` | Verify real HY-8/ryan-culverts numerical parity, shape/attribute limits and SI units; coordinate numerical findings with their independent repos rather than duplicating hydraulic solver development here. | Blocked: external HY-8 / owner acceptance |
| AB-024 | Medium / Verify | `TUFLOW-python/po_and_timeseries/generate_2d_po_label_list.py` | Add disposable SQLite/GPKG-like label, comment and missing-input regression. Confirm full real GeoPackage schema/layer compatibility and output overwrite policy before broader acceptance. | Partially verified: synthetic test committed; CI pending |
| AB-025 | Medium / Verify | `TUFLOW-python/culvert_results/`, `TUFLOW-python/po_and_timeseries/` | Complete original CLI/default/no-pause/output and results tests for 1D/2D culvert/PO/POMM wrappers; verify per-event/duration/TP aggregation, version banners, Excel/Parquet and no hidden project paths. | Open; source read, selected CLI tests bounded |
| AB-026 | Medium / Verify | `gdal-python/convert_gdb_to_gpkg.py`, `gdal-python/laz_to_las.py`, `gdal-python/gdal_retile_wrapper.py`, `gdal-python/gdal_translate_TIF_ovr.py` | Verify feature/point/raster counts, CRS/metadata and layer identity, tiling/overviews and driver availability from installed wheel in GDAL/OSGeo4W on Windows. | Blocked: GDAL/point-cloud fixtures |
| AB-027 | Medium / Verify | `misc-python/remove_excel_protection.py`, `docx-python/word_style_audit.py` | Use synthetic macro/nonmacro and style-rich Office files; prove only intended ZIP/XML changes and metadata/relationships preservation; validate output and error codes. | Open |
| AB-028 | Medium / Verify | `other/audit_qgis_projects.py`, `tuflow/find_missing_tlf_runs.py` | Test QGZ relative/absolute/UNC/provider paths and missing model run permutations against generated fixtures; keep reporting read-only and returned status meaningful. | Open |
| AB-029 | Medium / Verify | `TUFLOW-python/raster_processing/run_asc_to_asc_*.py`, `TUFLOW-python/raster_processing/run_grouped_asc_to_asc_statistics.py`, `TUFLOW-python/raster_processing/run_python_raster_calculations.py` | Verify dry-run is truly non-mutating, executable selection and batching, per-duration means/medians/maxima, output naming/collision, source rasters and exact parity against native ASC_to_ASC for comparable inputs. | Blocked: native TUFLOW raster tools |
| AB-030 | Medium / Verify | `file-management-python/file_inventory.py`, `file-management-python/list_files_recursive.py`, `RORB-python/plot_ensemble_results.py`, `gdal-python/recompress_geotiffs.py` | Existing copied-wrapper and isolated-wheel smoke tests prove only named CSV/list/error paths. Finish original defaults, Excel export, plotting, real raster replacement, network paths, resources and representative output parity. | Partially verified |
| AB-031 | Medium / Lifecycle | All **54** still-unassessed inventory entries, primarily excluded legacy, shell and companion resources | Finish per-file owner, purpose, import/execution hazards, consumer/companion and explicit keep/promote/retire decision; do not infer “safe” from exclusion. Record each disposition in the 149-row ledger. | Open |
| AB-032 | Medium / Completion | The **83 wheel-selected files** / maintained wrappers | Complete A/B installed-wrapper execution matrix: source defaults versus copied/wheel execution, `--help`, `--no-pause`, version, exit code, file resources, successful and failing runs. Distinguish 83 packaged resources from executable wrappers and 33 MCP targets; maintainer signs off decisions only after evidence. | Open |

## Recently verified bounded results

- CI passed all six jobs on PR #137 at `1a3b336` (before the latest
  inventory and test changes). Later commit CI must be checked independently.
- `tests/scripts/test_installed_wrapper_behaviour.py` exercises disposable
  input/report scenarios. The PO-label test uses a minimal SQLite representation
  of a GPKG table, **not** a full driver-valid GeoPackage and not production GIS.
- `tests/scripts/test_repository_only_hydrology.py` verifies AEP/ARI
  mathematical round trips and invalid input handling of a pure helper; it does
  not demonstrate ARR-engineering method validation or package eligibility.

## Tracking and ownership

Record the fixing PR/commit, tested platform and acceptance evidence beside the
corresponding item when implemented. Retain blocked items until the maintainer
provides a real Windows/QGIS/TUFLOW/HY-8/7-Zip result or explicitly changes scope.
The [original A–F requirements](2026-10-17-installed-scripts-original-requirements.md)
remain authoritative. PR #137 stays open until the required A/B work is
implemented or deliberately deferred through a visible maintainer decision.
