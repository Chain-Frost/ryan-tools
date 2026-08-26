# TUFLOW processor development notes

This document describes the maintained processor infrastructure under `ryan_library.processors.tuflow`. Use it when
loading supported TUFLOW result formats, combining processed outputs or adding a processor-backed data type.

For copyable direct-use snippets, see the repository's [TUFLOW API examples](../../../examples/tuflow/README.md).

The authoritative data-type and suffix configuration is
[`../../classes/tuflow_results_validation_and_datatypes.json`](../../classes/tuflow_results_validation_and_datatypes.json).
That registry also contains raster classifications used for discovery; a configured suffix is processor-backed only
when its entry names a processor class.

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

## Cache a collection with HDF5

`to_hdf()` writes each processed DataFrame and enough processor metadata to one HDF5 cache. `from_hdf()` reconstructs
the concrete processor classes and can apply an optional location filter while loading:

```python
from pathlib import Path

cache_path = Path("cache/tuflow-results.h5")
collection.to_hdf(cache_path)

restored = ProcessorCollection.from_hdf(cache_path, locations=["Culvert_01"])
```

Treat this as a processing cache rather than a stable interchange format. Rehydration depends on the configured
processor classes remaining importable and compatible with the stored metadata.

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
processor change this normally includes Black, strict Pyright on modified files, focused tests or a representative
smoke check, and a package build. Use synthetic fixtures when project result files cannot be shared.

If the new processor type has recurring agent-facing value, verify that the generic MCP inspection tools handle it and
update their guidance or focused coverage as needed. Do not add a separate MCP endpoint for each basic processor.
