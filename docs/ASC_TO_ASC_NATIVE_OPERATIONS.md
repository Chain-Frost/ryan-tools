# Native ASC_to_ASC-style raster operations

The maintained Python implementation covers a deliberate subset of TUFLOW's
ASC_to_ASC value-raster operations. Normal Python workflows do not require or
launch `asc_to_asc.exe`. A specific executable build can be supplied to
`run_asc_to_asc_job()` when producing an independent comparison raster.
Operation names and source-output terminology follow the
[official TUFLOW ASC_to_ASC documentation](https://wiki.tuflow.com/ASC_to_ASC).

## Responsibility split

| Module | Responsibility |
| --- | --- |
| `ryan_library/functions/tuflow/asc_to_asc_raster_operations.py` | Windowed Rasterio calculations, alignment validation, NoData policies and safe output replacement |
| `ryan_library/functions/tuflow/asc_to_asc_runner.py` | Execute one native job or explicitly launch one ASC_to_ASC comparison job |
| `ryan_library/functions/tuflow/raster_performance.py` | Per-process cache/chunk/thread settings, memory admission estimates and phase timings |
| `ryan_library/functions/tuflow/tuflow_result_naming.py` | Parse, replace, format and validate TUFLOW result filename components |
| `ryan_library/orchestrators/tuflow/asc_to_asc_batch.py` | Coordinate native jobs in a bounded process pool and report dashboard progress |

## Supported operation mapping

| ASC_to_ASC operation | Native implementation | Important differences |
| --- | --- | --- |
| `-max` | `compute_max()` | Writes the value raster only; no `_src` raster or legend |
| `-dif` / `-diff` | `compute_diff()` | Supports `-change` and `-nowetdry`; `combine_wd` is a Python-only extension |
| `-statMean` | `compute_stat("mean", ...)` | Closest-source value is the Python default; arithmetic and ASC_to_ASC-compatible selection remain selectable |
| `-statMedian` | `compute_stat("median", ...)` | Uses ASC_to_ASC's upper median for even contributor counts |
| `-statMin` | `compute_stat("min", ...)` | Writes value and source outputs |
| `-statMax` | `compute_stat("max", ...)` | Writes value and source outputs |

`-statFrac`, `-statRank` and `-statAll` are intentionally not implemented.

## Mean-value methods

`closest_source` is the default. The numeric mean is calculated using the
selected NoData policy, then the nearest policy-adjusted contributing value is
written. Equal-distance ties select the higher value. With the `zero` NoData
policy, substituted zeroes are eligible values.

`arithmetic` writes the numeric arithmetic mean directly.

`asc_to_asc` writes the numeric arithmetic mean and selects the lowest
contributing source value at or above it, matching the executable's value and
source outputs. Use it with the executable-compatible NoData policy when
running parity comparisons.

The generic `compute_stat()` and `RasterOperationJob` APIs retain
`closest_source` as their general-purpose default. The maintained
mean-then-maximum workflow defaults to `asc_to_asc` and exposes
`--mean-value-method {asc_to_asc,closest_source,arithmetic}`. Its editable
`MEAN_VALUE_METHOD` wrapper setting provides the same choice without CLI
arguments.

## Source rasters and legends

Every supported `compute_stat()` operation can write a source raster by passing
`write_source=True`; the general-purpose library default remains disabled. For
`result.tif`, the auxiliary outputs are `result_src.tif` and
`result_src_legend.csv`. Source IDs are 1-based and follow the caller-provided
input order. Each CSV row contains `source_id`, `source_file` (absolute original
input path), and `cell_count` (number of output cells assigned that ID).
Unused source IDs remain in the CSV with a count of zero. Counts exclude source
ID `0` (NoData), and their sum equals the number of valid source-ID cells.
Counts are accumulated per raster window, without loading entire grids into
memory. Per-result legend names avoid races and overwrites.

For minimum and maximum, the source identifies the first input supplying the
selected value, with the valid-zero preference described below. Median uses a
stable sort and selects the input at the upper-middle rank; duplicates retain
their input order, so the selected rank can identify a later equal contributor.
For `closest_source` and `arithmetic` means, provenance identifies the closest
policy-adjusted contributing value; an equal-distance tie selects the higher
value and then the first matching input. For `asc_to_asc` means, provenance
identifies the lowest contributing value at or above the arithmetic mean, then
the first matching input. Cells that are NoData in the value raster receive
source ID `0`, which is the source raster's NoData value.

The maintained mean-then-maximum and median-then-maximum wrappers enable source
rasters/legends by default; `--no-source` or `WRITE_SOURCE_RASTERS = False`
disables them, while `--source` explicitly enables them. Explicit
`source_output_file` and `source_legend_file` paths are also supported by the
library API. ASC_to_ASC's counter-intuitive `-src` switch suppresses source
output rather than enabling it.

For the mean-then-maximum and median-then-maximum workflows, source output is
propagated through both stages. Per-duration statistic source rasters identify
the original temporal-pattern rasters. The maximum stage composes its
intermediate selection with those source rasters, so the final source IDs and
CSV legend point directly to the original temporal-pattern rasters rather than
the generated mean or median files. It reads each selected duration's source
IDs alongside its value window and writes the final original-input IDs
directly. A separate full-raster source rewrite is no longer required. The
stored mean/median values and their precision still determine the maximum;
this optimisation does not bypass the intermediate value rasters.

Both workflows currently recalculate their prepared outputs when run; they do
not yet expose a force/rebuild switch or validate existing outputs for an
incremental skip.

## GeoTIFF compression

Native GeoTIFF value rasters and auxiliary source rasters use the repository's
shared TUFLOW creation profile by default: `COMPRESS=DEFLATE`, `PREDICTOR=2`,
and `BIGTIFF=IF_SAFER`. Native operations use an explicit `NUM_THREADS`
allowance from their execution options rather than each worker requesting all
CPUs. A direct library call defaults to one thread; the maintained ensemble
workflows divide the detected logical CPU budget across workers. This also applies when
flattening the final maximum-of-means/medians source IDs: the input raster's
`meta` does not include its compression creation options, so these are applied
again explicitly rather than silently writing a large uncompressed source TIFF.
Source ID rasters use `uint8` (up to 255 inputs), `uint16` (up to 65,535),
or `uint32` for larger source counts, with ID 0 reserved for NoData. Reducing
the integer width and enabling lossless compression changes storage only, not
source selection, georeferencing, IDs, or legend `cell_count` values.
Explicit `-co NAME=VALUE` creation options override value-raster defaults
when another lossless profile is required.

## Performance controls and diagnosis

The maintained mean/median-then-maximum wrappers expose the following editable
settings and matching CLI flags. Memory quantities are MiB (1,048,576 bytes).
Source rasters and legends remain enabled by default.

| Editable setting | CLI flag | Default and meaning |
| --- | --- | --- |
| `GDAL_CACHE_MB` | `--cache-mb` | `128`: decoded GDAL block cache per worker; passed to Rasterio in bytes |
| `CHUNK_MEMORY_MB` | `--chunk-mb` | `256`: estimated NumPy working memory per processing window |
| `TOTAL_MEMORY_BUDGET_MB` | `--memory-budget-mb` | `None`: use 75% of currently available RAM to admit workers |
| `GDAL_THREADS` | `--gdal-threads` | `None`: divide available logical CPUs across active workers, subject to the memory estimate |
| `VSI_CACHE_MB` | `--vsi-cache-mb` | `0`: additional raw per-input file caching disabled; account for every input when enabled |
| `REPORT_PHASE_TIMINGS` | `--report-phases` / `--no-report-phases` | `True`: log phase times and the effective resource plan |

`--workers` remains a requested upper limit. Each stage limits its worker count
using job count, CPU availability and the memory estimate. The estimate includes
the decoded cache, chunk workspace, optional per-input file caches, 128 MiB
process overhead and 8 MiB per GDAL thread. Nested source rasters count as inputs
in the maximum stage. This is an admission estimate, **not an OS-enforced RSS
limit**: library/driver buffers and actual workload memory can vary. A budget
that cannot fit one worker fails before the workflow starts producing outputs.

For a twelve-worker run, start with the defaults and collect the phase report:

```powershell
python run_asc_to_asc_mean_then_maximum.py --workers 12 --report-phases --no-pause
```

The initial report records effective workers, cache/chunk budgets and threads.
Each completed job reports dimensions, input block shape, window count and time
spent reading values, reading independent masks, calculating values, selecting
and counting sources, composing original-input provenance, writing, and closing
the rasters. Close time includes deferred output compression/flush. Stage phase
totals are **summed worker wall times**, which can exceed elapsed batch time when
jobs overlap; the elapsed stage duration is reported separately. These
workflows print their progress and timing reports to the console; redirect or
capture console output to retain the report.

Windows follow input blocks where the memory budget permits; striped input
retains full-width row windows when possible. Values and necessary masks are
read together. Only GDAL all-valid and NoData-only masks are skipped because
their validity can be derived from the values; independent masks remain in use.
Mean and median reuse a preallocated Float64 stack, while extrema stream through
inputs. Output compression, source IDs, NoData rules and reduction semantics are
preserved. With `--no-source`, arithmetic and ASC_to_ASC means also skip source
selection; `closest_source` still needs that selection to calculate its value.

For reproducible synthetic comparisons, see the
[source-enabled raster benchmark](../repo-scripts/benchmarks/README.md#source-enabled-raster-statistics)
and the [implementation work record](work/2026-11-18-raster-ensemble-performance.md).

## NoData policies

These policies are explicit Python workflow behaviour and do not universally
replicate ASC_to_ASC:

- `require_all`: output NoData unless every input cell is valid.
- `zero`: treat invalid inputs as zero if at least one input cell is valid at
  that location. Where **all** contributing inputs are invalid, preserve
  NoData and source ID `0`. This distinguishes outside-model coverage from
  a dry temporal pattern in an otherwise valid ensemble cell. For minimum
  and maximum, when substituted zero ties an originally valid zero, retain
  the valid contributor's source ID rather than selecting an all-NoData
  duration and losing final flattened provenance.
- `exclude`: calculate from valid contributors and output NoData only when all
  contributors are NoData.

The ensemble wrapper selects `zero` for `d_HR_Max` and `V_Max`, and `exclude`
for `h_Max` and `h_HR_Max`. Elevation results must not substitute zero.
Both the first-stage statistic and final duration maximum use the same policy.
Input validity uses Rasterio's band mask as well as the NoData sentinel and
finite-value check; an explicitly masked numeric value is not data. An entirely
dry area with NoData in *every* input cannot be distinguished from an
outside-model cell using result rasters alone and remains NoData.

## Executable comparison

`run_asc_to_asc_job()` requires an executable only for that explicit call. It
can write the external result to a separate comparison path without changing
the native job definition:

```python
comparison = run_asc_to_asc_job(
    executable=Path(r"C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe"),
    job=job,
    output_file=Path("comparison.tif"),
)
```

Compare only operations and policies expected to have parity. Project-specific
NoData policies and closest-source mean selection may intentionally differ.

### Executable parity test

The optional integration test generates a fresh aligned GeoTIFF set in
pytest's temporary directory, runs `-statMean`, `-statMedian`, `-statMin` and
`-statMax` through both implementations, and compares values, validity masks,
alignment metadata and source IDs exactly. It also verifies that both legends
reference the generated input rasters. No generated comparison rasters are
stored in the repository.

The tests use `C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe` when
`ASC_TO_ASC_EXE` is unset. To select a different build, set the override before running:

```powershell
$env:ASC_TO_ASC_EXE = 'C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe'
python -m pytest tests/functions/tuflow/test_asc_to_asc_executable_parity.py -m slow
```

The tests skip when `ASC_TO_ASC_EXE` is unset and the default executable is missing.
An explicit override is always respected and fails clearly when it points to a missing file.
