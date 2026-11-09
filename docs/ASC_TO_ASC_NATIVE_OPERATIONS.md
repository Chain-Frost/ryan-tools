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

For minimum, maximum and median, the source identifies the input supplying the
value. Duplicate values select the first matching input. For either mean-value
method, it identifies the closest contributing value; an equal-distance tie
selects the higher value and then the first matching input. Cells that are
NoData in the value raster receive source ID `0`, which is the source raster's
NoData value.

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
the generated mean or median files.

Both workflows currently recalculate their prepared outputs when run; they do
not yet expose a force/rebuild switch or validate existing outputs for an
incremental skip.

## GeoTIFF compression

Native GeoTIFF value rasters and auxiliary source rasters use the repository's
shared TUFLOW creation profile by default: `COMPRESS=DEFLATE`, `PREDICTOR=2`,
`BIGTIFF=IF_SAFER`, and `NUM_THREADS=ALL_CPUS`. This also applies when
flattening the final maximum-of-means/medians source IDs: the input raster's
`meta` does not include its compression creation options, so these are applied
again explicitly rather than silently writing a large uncompressed source TIFF.
Source ID rasters use `uint8` (up to 255 inputs), `uint16` (up to 65,535),
or `uint32` for larger source counts, with ID 0 reserved for NoData. Reducing
the integer width and enabling lossless compression changes storage only, not
source selection, georeferencing, IDs, or legend `cell_count` values.
Explicit `-co NAME=VALUE` creation options override value-raster defaults
when another lossless profile is required.

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
