# TUFLOW processor development notes

This document describes the maintained processor infrastructure under `ryan_library.processors.tuflow`. Use it when
loading supported TUFLOW result formats, combining processed outputs or adding a processor-backed data type.

For copyable direct-use snippets, see the repository's [TUFLOW API examples](../../../examples/tuflow/README.md).

The authoritative data-type and suffix configuration is
[`../../classes/tuflow_results_validation_and_datatypes.json`](../../classes/tuflow_results_validation_and_datatypes.json).
That registry also contains raster classifications used for discovery; a configured suffix is processor-backed only
when its entry names a processor class.

## Supported run identifiers and culvert maximums

**Every distinct simulation combined in a result workflow must have a unique full raw run code.** Model authors must
use filenames that encode the intended scenario, AEP, duration and temporal pattern (TP); the report tools do not
recover ambiguous names or reconcile collisions. For example, the supported filename parser recognises:

| File | Interpretation |
| --- | --- |
| `M11_01p_00120m_TP01_1d_Cmx.csv` | Full raw run code `M11_01p_00120m_TP01`; scenario `M11`; AEP `01p`; duration `00120m` (120 min); `TP01` |
| `M11_01p_00120m_TP02_1d_Cmx.csv` | A **different** simulation because the TP changed |
| `M11_01p_00180m_TP01_1d_Cmx.csv` | A **different** simulation because the duration changed |
| `M12_01p_00120m_TP01_1d_Cmx.csv` | A **different** scenario and simulation |

The companion `M11_01p_00120m_TP01_1d_Q.csv`, `_1d_V.csv`, `_1d_H.csv`, `_1d_Chan.csv`,
`_1d_Nmx.csv`, `_1d_Cmx.csv`, `_POMM.csv` and `.eof` products from that **same simulation** legitimately
share `M11_01p_00120m_TP01` before their respective suffixes. The
`TuflowStringParser.trim_run_code` grouping key is `M11` for these examples: it removes recognised AEP,
duration and TP components so related simulations can be grouped into an ensemble. It is **not** a unique simulation
identifier; the raw run code (`internalName` in processed tables) retains that role.
`ProcessorCollection.check_duplicates()` checks the combination of raw run code **and data type**.

The maintained 1D culvert reports assume the existing simple culvert convention: `Chan ID` identifies a culvert
channel, and the `_1d_Nmx.csv` node IDs `<Chan ID>.1` and `<Chan ID>.2` denote its upstream and downstream
ends respectively. `NmxProcessor` reshapes them to `US_h` and `DS_h`. The reports also use matching `Chan`
and `EOF` attributes. They do **not** implement arbitrary junctions, shared nodes, pits or general node-to-channel
network tracing; that wider scope is deferred in [#221](https://github.com/Chain-Frost/ryan-tools/issues/221).

**Maximum discharge (`Q`), velocity (`V`) and upstream/downstream water levels (`US_h`, `DS_h`) are
independent extrema within the selected simulation.** In particular, `CmxProcessor` preserves distinct Qmax and
Vmax records and their own `Time` values; `NmxProcessor` takes each reported end-node Hmax. The maxima need not
occur at the same timestep. A combined Maximums row or a selected governing/representative simulation identifies
which run supplied the reported values, **not** a simultaneous hydraulic state. No coincident-hydraulics extraction
or peak-time schema change is implied. See the [culvert-engine report interpretation](../../../docs/TUFLOW_CULVERT_ENGINES.md#maximums-workbook)
when using such maxima in a hydraulic check.

## Processor lifecycle

Create processors through `BaseProcessor.from_file()` so the filename parser, suffix registry and configured module
hint select the concrete class:

```python
from pathlib import Path

from ryan_library.processors.tuflow.base_processor import BaseProcessor

processor = BaseProcessor.from_file(Path("M11_01p_00120m_TP01_1d_Q.csv"))
processor.process()

if processor.processed:
    print(processor.df.head())
```

A successful processor must expose its processed rows through `processor.df` and set `processor.processed = True`.
`processor.raw_df` may retain an intermediate input table for diagnostics; call `discard_raw_dataframe()` after it is
no longer needed.

`ProcessorCollection.add_processor()` adds a processor only when it is marked processed **and** its processed
DataFrame is non-empty. A processed-but-empty result is logged and skipped, as is an unprocessed object.

## Build and inspect a collection

```python
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

collection = ProcessorCollection()
collection.add_processor(processor)

timeseries_collection = collection.get_processors_by_data_type(["Q", "H", "V", "CF"])
duplicates = collection.check_duplicates()
```

These inspection methods have different return types:

- `get_processors_by_data_type(...)` returns a new `ProcessorCollection` containing the matching processor objects.
- `check_duplicates()` returns `dict[tuple[str, str], list[BaseProcessor]]`, keyed by
  `(raw_run_code, data_type)`. Only keys with more than one processor are included.
- `copy()` returns a deep copy of the collection and its processors.

For example:

```python
for (run_code, data_type), processors in collection.check_duplicates().items():
    files = [item.file_name for item in processors]
    print(run_code, data_type, files)
```

## Combine processed outputs

Choose a combination method that matches the configured `processingParts.dataformat`:

| Format selector | Combination helper | Behavior |
| --- | --- | --- |
| `Timeseries` | `combine_1d_timeseries()` | Enriches time-series rows with matching `Chan`/`EOF` data, then groups by `internalName`, `Chan ID` and `Time` using maximum aggregation. |
| `Maximums`, `ccA` | `combine_1d_maximums()` | Merges matching EOF attributes, drops timing/path metadata and groups by `internalName` and `Chan ID`. |
| `PO` | `po_combine()` | Concatenates PO long-form rows and sorts by available run, location, type and time columns. |
| `POMM` | `pomm_combine()` | Concatenates processed POMM tables without additional grouping. |
| Any | `combine_raw()` | Concatenates every non-empty processed DataFrame without specialist grouping. |

`combine_1d_timeseries()` and `combine_1d_maximums()` automatically align truncated channel labels found in EOF
reports with full IDs from matching non-EOF results before combining. Call `align_eof_channel_ids()` explicitly when
the aligned IDs are also required in a raw export prepared before those combination methods.

The combination helpers use the shared DataFrame utilities to keep output schemas predictable:

- `reorder_long_columns()` moves file/path metadata to the right.
- `reorder_columns()` applies the domain-specific priority order used by grouped maximum and time-series outputs.
- `reset_categorical_ordering()` sorts category values and normalises missing values to `pd.NA`.

## Filter locations

`filter_locations(locations)` normalises the supplied identifiers, applies them to every processor and removes
processors whose DataFrames become empty. It returns the normalised `frozenset[str]` that was applied:

```python
applied_locations = collection.filter_locations(["Culvert_01", "Culvert_02"])
```

Repeated application of the same normalised filter is skipped by processors that already record it.
The filter uses `Location` when present, otherwise the first available identifier declared by the processor
(normally `Chan ID` for culvert results). It operates on processed rows, so Nmx upstream/downstream node IDs
have already been converted to their channel IDs. Matching is exact after trimming whitespace for comparison;
the retained identifier values, DataFrame types and numerical results are preserved. An empty selection applies
no additional filtering.

Before selecting rows, the collection aligns shortened EOF labels against its complete, unfiltered set of
channel references. Ambiguous labels remain literal. Once an EOF processor has been selected, later combination
or repeated filtering does not reinterpret its labels against a reduced reference set; removing a conflicting
channel must not make an ambiguous label appear unambiguous.

## Compact repeated path metadata

Large collections repeat file and path columns on every row. `compact_basic_info_columns()` replaces those columns in
each processor DataFrame with an integer `processor_id` and stores the corresponding lookup table on the collection:

```python
lookup = collection.compact_basic_info_columns()
combined = collection.combine_raw()
combined_with_paths = collection.attach_basic_info(df=combined, drop_id=True)
```

Use `build_basic_info_lookup()` when a lookup is needed without mutating processor DataFrames. `attach_basic_info()`
returns its input unchanged when no lookup or ID column is available.

To release retained raw input tables across the collection, call:

```python
discarded_count = collection.discard_raw_dataframes()
```

## Single-file Parquet processor collections

To export and later restore **individual ryan-tools processor objects**, use the
single-file `.rtparquet` archive format. It contains one Parquet table per
processor plus a versioned JSON manifest. This is **not** a conventional
`.parquet` file; `pandas.read_parquet()` reads ordinary exported tables, not
a whole collection bundle.

```python
from pathlib import Path

from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

bundle = Path("processed_tuflow.rtparquet")
collection.to_parquet(bundle)  # Never overwrites an existing archive by default.
restored = ProcessorCollection.from_parquet(bundle)
# Or restore only selected types/locations:
selected = ProcessorCollection.from_parquet(
    bundle, data_types=["PO", "POMM"], locations=["Culvert_01"]
)
```

`to_parquet(..., overwrite=True)` explicitly replaces an existing bundle
through an owned temporary file. The archive uses individual compressed
Parquet members and a ZIP container without redundant ZIP compression.
Corruption, unexpected entries, processor-configuration mismatch, and
changed/missing sources fail closed. `verify_sources=True` (default)
compares source path, size and timestamp, including available GeoPackage WAL
metadata. `verify_sources=False` explicitly permits reading historical
snapshots after source files have moved or changed; it does not disable archive
integrity or registry validation. No source files are rewritten.

The full loaded collection still occupies memory. For workflows exceeding
available RAM, retain the independent partitioned-Parquet/export and
per-run reduction strategy instead of materialising a large collection.
The bundle records processed DataFrames, filters, source identities and the
compacted basic-info lookup; it deliberately does not serialize raw input
DataFrames or execute arbitrary Python classes from the archive.

## Processed data storage and retired HDF5 snapshot API

**Breaking change (8 November 2026):** `ProcessorCollection.to_hdf()` and
`ProcessorCollection.from_hdf()` no longer read or write HDF5. They raise
`NotImplementedError`. The error-raising migration notices remain **through
31 March 2027** and may be removed from **1 April 2027**. The optional
`ryan-tools[cache]` extra and PyTables dependency are removed.

The last **published PyPI release containing these HDF5 methods** is
[`ryan-tools==26.11.8.3`](https://pypi.org/project/ryan-tools/26.11.8.3/). To use that historical API in a
separate environment:

```powershell
python -m pip install "ryan-tools==26.11.8.3"
```

The older HDF5 implementation has known categorical/nullable DataFrame
round-trip limitations; its availability does not guarantee that every
historical snapshot can be restored.

**Use Parquet for current processed tabular results.** The existing
`ryan-tools` TUFLOW processors remain the result readers. Export combined
DataFrames using `to_parquet()` and load tables using `pandas.read_parquet()`.
Conventional Parquet tables do not restore processor objects. Use the separate
`.rtparquet` collection bundle above for a validated single-file snapshot.

## MCP inspection and advanced queries

The MCP server exposes bounded, read-only processor queries through `inspect_tuflow_result` and
`inspect_tuflow_collection`. They support location/entity selection, collection data-type selection, numeric bounds on
the processed `Time` column and capped samples. `Time` normally represents simulation hours; filename duration remains
separate metadata from `parse_tuflow_filename`.

Agents that need richer grouping, joins, specialised collection combinations or domain-specific query logic should
read the `ryan-tools://guidance/tuflow-processors` MCP resource and use `BaseProcessor.from_file` and
`ProcessorCollection` directly in Python. This keeps the MCP tool surface stable while exposing the authoritative
processor factory, collection methods, registry and source locations. Processing is read-only until an export or write
method is explicitly called.

## Reporting-location point levels

The factory now accepts `*_RLP_H.csv` and `*_RLP_Hmx.csv` for source-provided reporting **points**. These are
not arbitrary network nodes, reporting lines or reporting regions. The two source products remain separate:

| Suffix | Output schema | Interpretation |
| --- | --- | --- |
| `_RLP_H.csv` | `Time`, `Location`, `Type`, `Value`, `AbsValue` | One level sample per point and simulation-hour timestamp. A requested location is selected before CSV columns are materialised. |
| `_RLP_Hmx.csv` | `Location`, `Type`, `Hmax`, `Time Hmax`, `dHmax`, `Time dHmax`, `Q` | TUFLOW-supplied summary with its own value/time associations; not recomputed independent maxima. |

The source units remain those of the original model (in the pinned metric EG02 examples, levels are metres,
simulation time is hours). The CSV readers keep printed numerical precision in float64 and treat documented
`-99999`, `**********` and empty samples as missing, never as hydraulic extrema. Do not infer a coincident
structure flow from `Q` or `dHmax`. The original source value `dHmax` may itself reflect a dry sentinel;
retaining the field is not an engineering validity endorsement.

Use the existing protected exporter to produce separate time-series and supplied-summary sheets:

```python
from pathlib import Path

from ryan_library.orchestrators.tuflow.reporting_points import main_processing

success = main_processing(
    paths_to_process=[Path("TUFLOW/results/EG02/plot/csv")],
    include_data_types=["RLP_H", "RLP_Hmx"],
    locations_to_include=["RL_01"],
    export_mode="both",
    use_live_dashboard=False,
)
```

This is a Python API rather than a newly supported copied CLI wrapper. Outputs are published under the current
working directory, and existing report files are not replaced. The generic MCP result-inspection tools can
inspect both registered types without a new endpoint. `RLP_H` is PO-shaped, but not automatically added to
a combined PO batch, so no alternate input is silently counted as an additional ensemble event.

The selected scope is tracked in [WP12's living record](../../../docs/work/2026-11-08-wp12-hu-hd-coverage.md);
other reporting lines/regions, structure SQ/SHmx, 2D split equivalents and general networks are not included.

## Reporting-location lines and regions

The registered source products keep TUFLOW's geometry and measurement categories distinct:

- `*_RLL_Q.csv`: signed reporting-**line** flow histories (not a reconstructed channel network). The
  existing `*_RLL_Qmx.csv` processor supplies source Qmax/time and its separate dQ/time fields.
- `*_RLR_Vol.csv`: reporting-**region** volume histories. `*_RLR_Volmx.csv` supplies source
  maximum volume and change data with separate times and associated `H`.

`Time` in history outputs is simulation hours. Source flow and volume units are retained
(m³/s and m³ in the metric EG02 fixtures), with float64 conversion of the printed CSV values.
`-99999` and `**********` are missing in the new readers, while valid negative values and
zero are preserved. The pre-existing line Qmx source `H` still carries its original missing
sentinel until the common validity work in #219 is accepted.

```python
from pathlib import Path

from ryan_library.orchestrators.tuflow.reporting_lines import main_processing as report_lines
from ryan_library.orchestrators.tuflow.reporting_regions import main_processing as report_regions

csv_path = Path("TUFLOW/results/EG02/plot/csv")
report_lines(
    paths_to_process=[csv_path],
    locations_to_include=["RL_03"],
    export_mode="both",
    use_live_dashboard=False,
)
report_regions(
    paths_to_process=[csv_path],
    locations_to_include=["RL_06"],
    export_mode="both",
    use_live_dashboard=False,
)
```

Each maintained Python workflow uses the existing discovery/processor/collection pipeline and
protected exporter, with separate history and source-summary sheets and matching Parquet
products. When a paired history or source-summary product is missing for a processed
run, the exporter emits a per-run **WARNING** naming its expected CSV suffix and
still publishes the available data; missing, invalid or location-filtered
companions must not be silently reported as a complete pair.
They do **not** insert the line/region output into POMM extrema or the PO
median/mean ensemble, and there is no implied coincident maximum-flow state.
The generic read-only MCP inspection interfaces work through the shared registry.
See [WP12's record](../../../docs/work/2026-11-08-wp12-hu-hd-coverage.md) for native acceptance and unsupported formats.

## ccA SQLite source safety and copy costs

ccA GeoPackage reads copy the database and any applicable WAL to invocation-owned
local scratch before opening SQLite. The source is never opened with SQLite;
SQLite may create its index/sidecars only in scratch. A rollback journal,
incomplete WAL/SHM pair, unavailable source or unstable copy is refused.

Detected source changes allow up to **three fresh copy attempts** with 50 ms and
100 ms delays. Every attempt repeats the unchanged source identity, size, mtime
and journal checks. Rejected copies are deleted before the next attempt. This
bounds copying to three attempts, not wall-clock time on an unresponsive share.
A continuously changing source still fails explicitly; close/checkpoint its
writer and retry after it stabilises. No immutable or writable-source fallback
is used. A successful copy can predate later committed writes.

Scratch needs at least database plus WAL bytes, with additional space for SQLite
runtime sidecars. Retries can transfer up to three times that volume. The earlier
synthetic native probe copied 67,637,904 bytes in 0.026-0.057 seconds; this is not a
multi-GB production benchmark. Stat signatures are not distributed locks or content
checksums: same-metadata rewrites, unsupported network locking and SQLite
journal_mode=OFF are outside this acceptance. Cross-host concurrent SQLite WAL
use on a network filesystem is not qualified. See the
[WP05 record](../../../docs/work/2026-11-08-wp05-processor-storage.md) for evidence.

## Add a processor-backed data type

### 1. Choose the appropriate base class

- Extend `TimeSeriesProcessor` for a time series that uses the shared read, clean and melt pipeline. Implement
  `process_timeseries_raw_dataframe()`, update `self.df` in place and return the appropriate `ProcessorStatus`. For a
  simple time series with one numeric value column, reuse `_normalise_value_dataframe()` where applicable.
- Extend `MaxDataProcessor` for maximums or ccA-style tables. Use the shared maximums reader, reshape the data and add
  common run metadata before applying configured output transformations.
- Extend `BaseProcessor` directly for a specialised layout that does not fit either shared pipeline.

Keep processor modules focused on one supported format. Reusable parsing and transformation logic belongs in
`ryan_library.functions` when it is not specific to processor state.

### 2. Register the type

Add an entry to `ryan_library/classes/tuflow_results_validation_and_datatypes.json` with:

- `processor`: concrete processor class name;
- `suffixes`: every filename suffix that identifies the type;
- `output_columns`: final column-to-dtype mapping;
- `processingParts.dataformat`: selector used by collection combination helpers;
- `processingParts.module`: optional relative subpackage or absolute module hint used to find the processor class;
- `processingParts.columns_to_use` or `expected_in_header`: input selection and validation where required.

Do not duplicate the suffix list in a wrapper. Load it through `SuffixesConfig` so filename parsing, discovery and the
processor factory use one source of truth.

### 3. Implement and validate the lifecycle

Before marking processing successful:

- validate required headers and columns;
- reshape into the configured output schema;
- remove or report malformed rows deliberately;
- add common filename/run metadata unless the format intentionally omits it;
- apply configured output transformations;
- run the processor's data validation;
- set `self.processed = True` only when the processed result is valid.

Failure paths should log enough file and column context to diagnose the input, clear `self.df` to an empty DataFrame
and leave `self.processed = False`. This prevents invalid or partial results from being added to a collection.

### 4. Validate a processor change

Follow the repository [development guide](../../../docs/DEVELOPMENT_GUIDE.md#validation-by-change-type). For a
processor change this normally includes Ruff formatting, strict Pyright on modified files, focused tests or a
representative smoke check, and a package build. Use synthetic fixtures when project result files cannot be shared.

If the new processor type has recurring agent-facing value, verify that the generic MCP inspection tools handle it and
update their guidance or focused coverage as needed. Do not add a separate MCP endpoint for each basic processor.
