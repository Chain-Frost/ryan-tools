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

## Source rasters and legends

Every supported `compute_stat()` operation can write a source raster by passing
`write_source=True`; source output is disabled by default. For `result.tif`,
the auxiliary outputs are `result_src.tif` and `result_src_legend.csv`. Source
IDs are 1-based and follow the caller-provided input order; the CSV records the
absolute input path for each ID. Per-result legend names avoid races and
overwrites when independent statistics run in the same directory.

For minimum, maximum and median, the source identifies the input supplying the
value. Duplicate values select the first matching input. For either mean-value
method, it identifies the closest contributing value; an equal-distance tie
selects the higher value and then the first matching input. Cells that are
NoData in the value raster receive source ID `0`, which is the source raster's
NoData value.

The maintained wrapper exposes the opt-in as `--source`. Explicit
`source_output_file` and `source_legend_file` paths are also supported. This
default differs from ASC_to_ASC, whose counter-intuitive `-src` switch
suppresses source output rather than enabling it.

For the mean-then-maximum workflow, `--source` is propagated through both
stages. Per-duration mean source rasters identify the original temporal-pattern
rasters. The maximum stage composes its intermediate mean selection with those
mean source rasters, so the final source IDs and CSV legend also point directly
to the original temporal-pattern rasters rather than the generated mean files.

## GeoTIFF compression

Native GeoTIFF value rasters and auxiliary rasters always use the repository's
shared TUFLOW creation profile by default: `COMPRESS=DEFLATE`, `PREDICTOR=2`,
`BIGTIFF=IF_SAFER`, and `NUM_THREADS=ALL_CPUS`. This does not depend on whether
the input GeoTIFF is compressed. Explicit `-co NAME=VALUE` creation options
override the corresponding defaults when another lossless profile is required.

## NoData policies

These policies are explicit Python workflow behaviour and do not universally
replicate ASC_to_ASC:

- `require_all`: output NoData unless every input cell is valid.
- `zero`: treat input NoData as zero.
- `exclude`: calculate from valid contributors and output NoData only when all
  contributors are NoData.

The ensemble wrapper selects `zero` for `d_HR_Max` and `V_Max`, and `exclude`
for `h_Max` and `h_HR_Max`. Elevation results must not substitute zero.

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

Select the ASC_to_ASC build explicitly and run the test with:

```powershell
$env:ASC_TO_ASC_EXE = 'C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe'
python -m pytest tests/functions/tuflow/test_asc_to_asc_executable_parity.py -m slow
```

The test skips when `ASC_TO_ASC_EXE` is unset and fails clearly when it points
to a missing file.
