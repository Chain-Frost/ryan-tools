# TUFLOW Python wrappers

This folder contains maintained, library-backed wrappers as well as older
standalone utilities. New and updated thin wrappers follow the repository-wide
[`WRAPPER_STANDARD.md`](../WRAPPER_STANDARD.md); versioned standalone scripts
retain their own self-contained behaviour until they are deliberately migrated.

## Maintained wrappers

The maintained wrappers delegate processing to `ryan_library` orchestrators and
keep editable project paths, globs, filters, output modes and thresholds near
the top of each file. This includes the log-summary, POMM/PO combination,
culvert reporting, time-series checking, result styling and ASC_to_ASC search
wrappers.

`raster_processing/run_grouped_asc_to_asc_statistics.py` and
`raster_processing/run_python_raster_calculations.py` were promoted from `unsorted-python` on 20 August 2026. They retain an explicit
source warning because they have focused automated coverage but have not yet been used on a production project. The
native-Python wrapper uses the shared raster-calculation implementation; the grouped wrapper coordinates the external
TUFLOW `asc_to_asc` executable. The native implementation's supported command mapping, NoData policies, closest-source
mean, upper median and optional executable-comparison boundary are documented in
[`../../docs/ASC_TO_ASC_NATIVE_OPERATIONS.md`](../../docs/ASC_TO_ASC_NATIVE_OPERATIONS.md).

`raster_processing/run_asc_to_asc_mean_then_maximum.py` and
`raster_processing/run_asc_to_asc_median_then_maximum.py` share one generic
statistic-then-maximum orchestrator. They aggregate temporal patterns for each
duration, then take the maximum across durations. With `--source`, the final
source legend resolves directly to the original temporal-pattern rasters.
The mean wrapper defaults to ASC_to_ASC selection: its value raster contains
the arithmetic mean and its source identifies the lowest contributing TP value
at or above the mean. `MEAN_VALUE_METHOD` or `--mean-value-method` can instead
select the closest contributing value or retain the arithmetic mean with
closest-source provenance. Median uses the upper middle contributing value
when the TP count is even.

[`plot_water_level_profiles.py`](po_and_timeseries/plot_water_level_profiles.py) creates terrain
and TUFLOW water-level profile PNGs along GeoPackage lines.  CLI arguments can override paths and profile settings. The workflow validates CRS metadata and
requires exactly one result raster for every requested AEP. Missing CRS metadata
is inferred from any tagged profile or raster input; when every input is
untagged, their source coordinates are assumed to already align.
The default masked-bilinear sampler returns NoData only when the profile
station's containing raster cell is NoData; adjacent NoData cells are excluded
and the remaining interpolation weights are renormalised.

## Wrapper folders

| Folder | Purpose |
| --- | --- |
| `culvert_results` | Combine and summarise TUFLOW culvert maximum and time-series results. |
| `gis_processing` | Process GeoPackage layers, network layers and QGIS result styles. |
| `log_processing` | Parse TUFLOW logs and maintain log-summary workbooks. |
| `model_management` | Launch simulations and collect or clean model result files. |
| `po_and_timeseries` | Process PO and POMM outputs, check time series and plot profiles. |
| `raster_processing` | Run ASC_to_ASC and native-Python raster calculations. |

Only imports needed to construct or annotate those editable values, such as
`Path` and `Literal`, appear before the settings. Operational third-party and
`ryan_library` imports follow the editable block so users see configuration
before implementation wiring.

Each maintained wrapper embeds a `WRAPPER_VERSION` that remains available when
the file is copied into a job folder. At both the start and end of processing it
prints:

```python
print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
```

The closing call sets `leading_blank_line=True` so the completion banner is
visually separated from processing output without repeating three statements
in every wrapper.

The wrapper version identifies the copied entry-point file; the separately
installed `ryan-tools` version identifies the shared implementation.
Interactive completion uses `pause_console()`, which collects unreachable
Python objects immediately before waiting. Redirected/headless runs do not
pause, and `--no-pause` explicitly disables the wait for automation.

## Shared command-line arguments

Common `argparse` declarations live in
`ryan_library.functions.wrapper_utils` so wrappers do not repeat boilerplate:

| Helper | Arguments added |
| --- | --- |
| `add_execution_cli_arguments()` | `--working-directory`, `--console-log-level`, `--no-pause` |
| `add_filter_cli_arguments()` | `--data-types`, `--locations` |
| `add_live_dashboard_cli_arguments()` | Dashboard enable/disable, refresh rate and maximum rows |
| `add_common_cli_arguments()` | All three groups above |
| `add_export_mode_cli_argument()` | `--export-mode {excel,parquet,both}` |

`parse_common_cli_arguments()` converts the resulting `argparse.Namespace` to
the typed `CommonWrapperOptions` object. A wrapper should select only the groups
its workflow supports, then declare specialised options locally. For example,
ASC_to_ASC worker, dry-run and strictness options remain in those wrappers
because their meaning and help text are workflow-specific.

Run any maintained wrapper with `--help` for its current arguments. For a
headless invocation, include `--no-pause`:

```powershell
py -3.14 .\log_processing\create_log_summary_report.py --working-directory "D:\Model\results" --no-pause
py -3.14 .\po_and_timeseries\combine_pomm_results.py --export-mode parquet --no-pause
```

## Check recent TUFLOW user activity

[`log_processing/check_tuflow_user_activity.py`](log_processing/check_tuflow_user_activity.py) is a read-only,
library-backed utility that checks each user folder below `%ProgramData%\TUFLOW\log`. It reports every distinct
simulation start in the selected period rather than assuming that a user's latest event represents all of their
concurrent runs. The default window is 24 hours and the enforced maximum is seven days (`--recent-hours 168`).

```powershell
py -3.14 .\ryan-scripts\TUFLOW-python\log_processing\check_tuflow_user_activity.py
py -3.14 .\ryan-scripts\TUFLOW-python\log_processing\check_tuflow_user_activity.py --recent-hours 48
```

For each start, the utility reads the quoted `.tlf` with the maintained TUFLOW parser. A valid completion footer is
labelled `COMPLETED`; an accessible TLF without that footer is `POSSIBLY RUNNING`; missing or inaccessible TLFs remain
`UNKNOWN`. Each TLF is assessed independently, so one model finishing does not imply that another model owned by the
same user has stopped. The tables include the computer, licence seat and parsed GPU device IDs. Recent completions show
their elapsed runtime, and a final details section prints the full inferred TUFLOW project folder and TLF path. Use
`--log-root` to inspect a non-default copy of the licence-log tree.

By default, the wrapper also reads local CodeMeter WebAdmin at `http://localhost:22352`. It reports current TUFLOW
product utilisation, each active user's process ID, first and last access, licence allocation handles, recent
no-more-licences events, and the recent simulation whose start time most closely matches each active process. Known
TUFLOW product codes are 20 (Local), 21 (Local Threads), 22 (Local Multiple Domains), and 26 (Local GPU). Any product
code not recognised by the Python mapping is shown as a warning instead of being silently guessed. Use
`--no-codemeter` when WebAdmin is unavailable or `--codemeter-url` to select another local endpoint.

## POMM peak wrapper migration

The maintained mean and median POMM peak wrappers use
`ryan_library.orchestrators.tuflow.pomm_max_items` directly. The deprecated
`PommPeakWrapperDefaults`, `PeakReportExporter` and `run_pomm_peak_report_wrapper`
helpers were removed from `ryan_library.functions.wrapper_utils` on 18 October
2026. Older copied wrappers using these helpers must be replaced by maintained
versions under `ryan-scripts/TUFLOW-python` before upgrading.

## Verify converted FLT/ASC output rasters

The tool reports the embedded wrapper revision and the installed `ryan-tools`
package version at startup and completion. It also displays total source data
size, overall file-count progress, processed source volume, active source and
GeoTIFF sizes, and the current comparison. Live counters track verified matches,
CRS warnings, newly created TIFFs, and excluded files. Non-interactive output
reports each file's relative path, sizes, result, and whether its GeoTIFF was
created in the current run. The completion summary distinguishes verified
existing GeoTIFFs from newly generated and verified GeoTIFFs. Batch sizes are
on-disk source file sizes, **not bytes actually read or written**. Use
`--no-pause` for scheduled or unattended execution.


Use [`model_management/verify_converted_tuflow_rasters.py`](model_management/verify_converted_tuflow_rasters.py)
to optionally **convert -> verify -> delete** legacy TUFLOW `.flt` / `.asc` outputs in a single run.
Existing same-stem `.tif` / `.tiff` files are checked rather than overwritten. With
`--convert-missing`, any absent GeoTIFF is generated via the existing
`ryan_library.functions.gdal.raster_processing.translate_to_geotiff()` using the
lossless `tuflow`/DEFLATE profile, without creating unnecessary overviews.
The independent Rasterio comparison runs before any source deletion.
This standalone utility uses Rasterio and NumPy (both included in the project dependencies).
It reuses `ryan_library.functions.file_verification` for file snapshots and
symlink/junction checks. Its semantic raster comparison remains local: only
valid-cell values and NoData-mask locations must agree, not NoData sentinel
values or entire decoded byte sequences.

- Recursively searches below `ROOT_DIR` (default: current working directory).
  By default the TIF must be in the same directory. Set `TIF_ROOT_DIR` or `--tif-root`
  when converted GeoTIFFs are in a parallel folder tree with the same relative directory layout.
  With `--convert-missing`, missing child directories in an existing TIFF root are created as needed.
- **Optional creation:** `--convert-missing` (or `CONVERT_MISSING = True`) converts
  only when neither `.tif` nor `.tiff` is found; existing/stale/mismatching TIFFs
  are never overwritten, and ambiguous counterparts block conversion. GDAL writes
  into a temporary sibling file before publishing a completed TIFF, with
  exclusive-create fallback for network filesystems that cannot create hard links.
  Conversion failures and exact-comparison failures leave the source intact.
- Compares width, height, band count, **exact grid origin, cell dimensions and full
  geotransform**, NoData validity masks and every valid cell without any tolerance
  or resampling. Raster band scale/offset metadata must also match, because it
  changes the interpretation of stored pixel values. Different NoData sentinels
  are allowed only when their invalid cells coincide. These grid and pixel
  properties are critical, non-regenerable content.
- **If the source declares a CRS, the GeoTIFF must declare the same CRS.** For example,
  source EPSG:28350 and GeoTIFF EPSG:28351 is a blocking mismatch. Strict CRS
  equality is intentional: OGC:CRS84 in a source PRJ versus EPSG:4326 in a GeoTIFF
  also fails (axis-order differences are not ignored). A GeoTIFF without
  CRS when its source declares one is also a blocking mismatch. If the *source*
  lacks CRS metadata (common in legacy FLT/ASC outputs without PRJ files), matching
  pixels and geometry remain eligible under `MATCH_CRS_UNVERIFIED`, with a warning
  that the spatial reference cannot be independently verified.
- Computed statistics tags and regenerable auxiliary-statistics sidecars are
  **not compared** and cannot invalidate matching pixel and grid content. The tool
  deliberately does not require byte-identical FLT/ASC and TIFF files.
- The GeoTIFF must have a **strictly newer modification timestamp** than the source;
  for FLT files also newer than the paired `.hdr` header when scheduled for deletion.
- Paths are converted to non-resolving absolute paths, not `resolve()`, and are rejected if
  any component is a symbolic link or Windows directory junction. Nested linked folders are
  not scanned and linked paths are rechecked before deletion.
- Uses chunked raster reads and a Rich progress indicator, not binary file hashes.
- Creates non-overwriting `tuflow_raster_conversion_delete_candidates.csv`,
  `tuflow_raster_conversion_errors.csv`, and `tuflow_raster_conversion_warnings.csv`
  in the source root. Missing-CRS cases appear in the warning CSV and are clearly marked
  in the deletion candidates (status and warning columns), **not** in the errors CSV.
  True mismatches are excluded and listed in the errors CSV. No per-raster placeholders.
- **Default is validation only.** Use `--convert-missing --delete` to convert,
  verify and clean up in one invocation. Add `--delete` or set `DELETE_FILES = True`,
  then type exactly `DELETE` to remove confirmed matches. Before each removal it checks
  source, GeoTIFF and associated header size and modification timestamps again.
  This protects against ordinary changes, but does not lock files against concurrent writes.
- For verified FLT files, the associated `.hdr` is included by default. The `.prj`,
  `.aux.xml` and other sidecars are retained, as they can contain reusable metadata.
  Set `DELETE_FLT_HEADERS = False` to retain FLT headers.

Example from PowerShell (run verification before considering deletion):

```powershell
py -3.14 .\ryan-scripts\TUFLOW-python\model_management\verify_converted_tuflow_rasters.py --root "D:\Models\results"
py -3.14 .\ryan-scripts\TUFLOW-python\model_management\verify_converted_tuflow_rasters.py --root "D:\Models\results" --convert-missing --delete
```

Check the candidate list, back up the source rasters and make sure model/QGIS references have
been updated to use the GeoTIFFs. The tool does not modify references, nor does it delete
unmatched files. This is an exact-value/georeferencing check rather than a guarantee that the
converted files preserve every possible GDAL metadata tag.

## Duplicate check-file cleanup

`model_management/delete_duplicate_check_files.py` removes byte-identical files within TUFLOW
folders named `check` or `checks` (case-insensitive), retaining one copy per unique content group. Run from the intended result/model root
or change `ROOT_DIR` in the script.

- **Default `DRY_RUN = False`:** the script writes a proposal and report, then requires typing exactly
  `DELETE` before removal. Set `DRY_RUN = True` for review-only operation (no prompt or deletion).
- **Default `COMPARE_ACROSS_CHECK_FOLDERS = True`:** keep one copy of each content group across all scanned
  folders. With `False`, keep one representative in each check folder instead.
- **Default `INCLUDE_RASTER_FILES = True`:** GeoTIFFs and auxiliary files are included. Set `False` to
  exclude them. Identical bytes do **not** mean the original file paths are safe to remove: linked QGIS
  projects or scripts may stop finding their inputs.
- A new numbered proposal CSV and TXT report are written **without overwriting older runs**
  (`duplicate_files_proposed.csv`, then `duplicate_files_proposed_001.csv`, and likewise for reports).
  Review their listed keep/delete choices before confirming.
- A confirmed run writes `duplicate_files_manifest.csv` before deletion and records all original
  filenames, retained paths, hashes and outcomes. It refuses another deletion if a recovery manifest
  already exists. Archive that manifest before starting another run.
- Each affected check folder gets one `.DUPLICATE_REMOVED.txt` placeholder per unique deleted
  content group, including groups whose kept copy is **in the same folder**. It lists every original
  deleted filename and the location of the retained file.
- During SHA-256 candidate scanning, `SHOW_HASH_PROGRESS = True` displays a Rich live progress bar with
  **bytes hashed / total candidate bytes**, percentage, throughput, ETA and files completed. The current
  filename is shown while it is being read, including for individual multi-gigabyte files. The
  update frequency is set by `HASH_PROGRESS_REFRESH_PER_SECOND` (default 2). The progress view uses
  the same Rich terminal approach as the repo's other dashboards, not a separate GUI window.
  The total counts only files requiring content hashes (equal extension and size), not every
  discovered file. Redirected/headless execution prints the completion summary without live frames.
  `rich` is included in the repo requirements; copied standalone scripts also need it installed.
- Set `SHOW_HASH_PROGRESS = False` to disable the live display.
- The script reuses `ryan_library.functions.file_verification.hash_file()` for streaming
  SHA-256, with the existing 1 MiB chunks, per-chunk Rich callback and hexadecimal
  manifests unchanged. Final metadata checks also reject symlink/junction paths.
- SHA-256 reads candidate contents **once** during grouping. Deletion checks existence, size and
  modification time of both paths, rechecking the retained path before each unlink. It does **not**
  hash file content again. Run only on static/inactive results; metadata checks are not file locks.
- Set `RESTORE_FROM_MANIFEST = True` to copy verified retained content back into missing original
  paths. With the default `DRY_RUN = False`, restoration requires typing `RESTORE`. Existing
  files are never overwritten; a recovery manifest (not the pre-deletion proposal) is required.

Verify first on a disposable copy of model results, particularly on network drives.
