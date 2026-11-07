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

## Incomplete temporal-pattern bins

The mean-then-maximum and median-then-maximum wrappers require the configured
`EXPECTED_TPS` (TP01–TP10 by default) in each discovered model/scenario/AEP/
duration/result-type bin. Zero-byte files are ignored as missing TP inputs,
including when every file in a discovered bin is empty. Duplicate TP IDs and
TP IDs outside `EXPECTED_TPS` are errors in every mode.

`ALLOW_INCOMPLETE_TP_BINS = False` is the shipped setting. Complete bins can
still produce their mean or median, but any incomplete bin withholds its
model/scenario/AEP/result-type maximum. Unaffected maximum groups can finish;
the overall run returns exit code 1 to report the incomplete result. With
`STRICT_INCOMPLETE_GROUPS = True` or `--strict`, incompleteness stops the run
before either processing stage.

To deliberately process incomplete bins, edit the setting in **either** wrapper:

```python
ALLOW_INCOMPLETE_TP_BINS = True
```

Or override it for one run:

```powershell
python run_asc_to_asc_mean_then_maximum.py --allow-incomplete-tp-bins --no-pause
python run_asc_to_asc_median_then_maximum.py --allow-incomplete-tp-bins --no-pause
```

`--no-allow-incomplete-tp-bins` overrides an edited `True` setting. The override
cannot be combined with strict mode; use `--no-strict` when a copied wrapper
has strict mode enabled. `--dry-run` previews the same grouping and warnings
without writing rasters, and returns a nonzero status when the selected mode
cannot produce the requested complete results.

Under the override, each nonempty bin uses only its available inputs. For
example, TP values 2, 4 and 12 produce a mean of 6 and an upper median of 4;
the seven missing TP files are not seven zero-valued inputs. Existing NoData
cell policies still apply inside the available rasters. Warnings identify
the model, directory, AEP, duration, result type, available/missing TPs and
zero-byte paths. Source legends contain only the original rasters actually
used, and source IDs and cell counts remain valid for unequal bin sizes.

A bin with no nonempty inputs is omitted with an explicit warning even under
the override; if no usable statistic jobs remain, the run fails. Corrupt
nonempty rasters, grid-alignment errors and processing failures still stop the
maximum stage. Missing durations with no discovered files cannot be inferred
from `EXPECTED_TPS`: this option checks discovered TP bins, not an expected
duration schedule. The override does not change output filenames or rebuild
behaviour, so use a separate `OUTPUT_DIRECTORY_NAME` to retain earlier outputs.
Withheld maxima from a previous run are not deleted or treated as new results.

## Source rasters and legends

Every supported `compute_stat()` operation can write a source raster by passing
`write_source=True`; source output is disabled by default. For `result.tif`,
the auxiliary outputs are `result_src.tif` and `result_src_legend.csv`. Source
IDs are 1-based and follow the caller-provided input order; the CSV records the
absolute input path and the number of cells assigned to each ID in the source
raster. Per-result legend names avoid races and overwrites when independent
statistics run in the same directory.

For minimum, maximum and median, the source identifies the input supplying the
value. Duplicate values select the first matching input. For `asc_to_asc`, the
source identifies the lowest contributing value at or above the arithmetic
mean. The other mean methods identify the closest contributing value; an
equal-distance tie selects the higher value and then the first matching input. Cells that are
NoData in the value raster receive source ID `0`, which is the source raster's
NoData value. A source cell is also `0` when every contributing input cell is
NoData, including when the `zero` policy writes a numeric zero to the value
raster.

The maintained mean-then-maximum and median-then-maximum wrappers enable
source rasters and legends by default. Use `--no-source` or set
`WRITE_SOURCE_RASTERS = False` to disable them; `--source` explicitly enables
them. The general-purpose `compute_stat()` API remains opt-in. Explicit
`source_output_file` and `source_legend_file` paths are also supported.
ASC_to_ASC's counter-intuitive `-src` switch suppresses source output.

For the mean-then-maximum and median-then-maximum workflows, the source setting
is propagated through both stages. Per-duration statistic source rasters identify
the original temporal-pattern rasters. The maximum stage composes its
intermediate selection with those source rasters, so the final source IDs and
CSV legend point directly to the original temporal-pattern rasters rather than
the generated mean or median files.

Both workflows currently recalculate their prepared outputs when run; they do
not yet expose a force/rebuild switch or validate existing outputs for an
incremental skip.

## GeoTIFF compression

Native GeoTIFF value rasters and auxiliary rasters always use the repository's
shared TUFLOW creation profile by default: `COMPRESS=DEFLATE`, `PREDICTOR=2`,
`BIGTIFF=IF_SAFER`, and `NUM_THREADS=ALL_CPUS`. This does not depend on whether
the input GeoTIFF is compressed. Explicit `-co NAME=VALUE` creation options
override the corresponding defaults when another lossless profile is required.
Flattening multi-stage source provenance preserves the source raster's complete
creation profile.

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

The tests use `C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe` when
`ASC_TO_ASC_EXE` is unset. To select a different build, set the override before running:

```powershell
$env:ASC_TO_ASC_EXE = 'C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe'
python -m pytest tests/functions/tuflow/test_asc_to_asc_executable_parity.py -m slow
```

The tests skip when `ASC_TO_ASC_EXE` is unset and the default executable is missing.
An explicit override is always respected and fails clearly when it points to a missing file.
