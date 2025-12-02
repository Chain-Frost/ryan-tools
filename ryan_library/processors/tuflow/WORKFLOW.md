# TUFLOW Processor Workflow

This document describes how TUFLOW CSV/CCA files are parsed, matched to the
correct processor implementation, and enriched with shared metadata before they
are merged downstream.

## High-level flow

```mermaid
flowchart TD
    A[Filename from disk] --> B[TuflowStringParser
    • identifies suffix
    • extracts run code]
    B --> C[SuffixesConfig
    • maps suffix→data type
    • exposes DataTypeDefinition]
    C --> D[BaseProcessor.from_file
    • resolves processor class
    • instantiates concrete processor]
    D --> E[Processor.process()
    • dataset specific parsing]
    E --> F[BaseProcessor.add_common_columns()
    • enrich metadata]
    F --> G[BaseProcessor.apply_output_transformations()
    • enforce configured dtypes]
    G --> H[ProcessorCollection
    • scenario level aggregation]
```

> If Mermaid is not available, read the diagram as the ordered list of steps
> shown in the nodes from **A** through **H**.

### Bullet flow

1. Parse the filename with `TuflowStringParser` to derive the data type and run
   code parts.
2. Ask `SuffixesConfig` for the `DataTypeDefinition` that corresponds to the
   parsed data type.
3. Let `BaseProcessor.from_file` import and instantiate the processor class
   declared in the configuration.
4. Run the processor-specific `process()` implementation to load and reshape the
   raw dataset.
5. Call `add_common_columns()` and `apply_output_transformations()` on the base
   class to enrich the DataFrame with shared metadata and enforce configured
   dtypes.
6. Feed the resulting processors into `ProcessorCollection` to combine runs by
   scenario, time, and channel identifiers.

## Filename parsing and metadata extraction

`TuflowStringParser` is responsible for all filename parsing. When a file path is
provided it loads the shared suffix map from `SuffixesConfig`, determines the
`data_type` using the filename suffix, extracts the raw run code, splits it into
`R01`, `R02`, … parts, and captures the TP/Duration/AEP components in both text
and numeric forms.【F:ryan_library/classes/tuflow_string_classes.py†L10-L111】

The parser is instantiated as part of every `BaseProcessor`, so the parser's
fields are immediately available when metadata needs to be added to the DataFrame.【F:ryan_library/processors/tuflow/base_processor.py†L47-L71】

## Configuration lookup and processor selection

The configuration lives in a single JSON file that is parsed by the
`ConfigLoader`. `Config` wraps these definitions as a singleton so processors can
reuse the in-memory cache. `SuffixesConfig` uses the same instance to invert the
configuration into a suffix→data type lookup map.【F:ryan_library/classes/suffixes_and_dtypes.py†L1-L167】【F:ryan_library/classes/suffixes_and_dtypes.py†L191-L274】

Each `DataTypeDefinition` ties together the file suffixes, the concrete
processor class name, the output column dtype map, and the `processingParts`
metadata (data format category, import module hints, expected CSV headers, etc.).【F:ryan_library/classes/suffixes_and_dtypes.py†L125-L209】  When
`BaseProcessor.from_file` runs it resolves the file `data_type`, looks up the
corresponding `DataTypeDefinition`, and then calls
`get_processor_class` to dynamically import the configured processor. The JSON
can supply a module override or rely on the default search inside
`ryan_library.processors.tuflow` so that both standard and specialised processor
implementations can be loaded without code changes.【F:ryan_library/processors/tuflow/base_processor.py†L73-L180】

## How processors are imported

1. `TuflowStringParser` loads the shared suffix map from `SuffixesConfig` and
   sets `data_type` on construction, so every processor instance starts with an
   identified type based solely on the filename suffix.【F:ryan_library/classes/tuflow_string_classes.py†L65-L114】
2. `SuffixesConfig` and `Config` resolve that `data_type` to a
   `DataTypeDefinition` by reading `tuflow_results_validation_and_datatypes.json`
   once, caching it, and exposing both the suffix lookup and the rich
   configuration block for the data type.【F:ryan_library/classes/suffixes_and_dtypes.py†L228-L340】【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L1-L116】
3. `BaseProcessor.from_file` passes the definition into
   `BaseProcessor.get_processor_class`, which tries the hinted modules, falls
   back to package defaults, and caches the imported class so subsequent files of
   the same type reuse the resolved processor without repeating the import
   work.【F:ryan_library/processors/tuflow/base_processor.py†L80-L200】

### Example: wiring a new suffix

To support a new results file such as `_1d_X.csv`, add an entry to
`tuflow_results_validation_and_datatypes.json` that points the suffix at the
target processor. The structure mirrors the existing definitions (for example
`Cmx`).【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L35-L74】

```jsonc
"X": {
  "processor": "XProcessor",
  "suffixes": ["_1d_X.csv"],
  "output_columns": {
    "Chan ID": "string",
    "Time": "float",
    "X": "float"
  },
  "processingParts": {
    "dataformat": "Timeseries",
    "module": "1d_timeseries",
    "expected_in_header": ["Time", "Chan ID", "X"]
  }
}
```

Once this block is present, `SuffixesConfig` maps `_1d_X.csv` to the `X` data
type, `Config` exposes the definition to `BaseProcessor`, and
`get_processor_class` imports `XProcessor`. No additional wiring is required
inside the codebase.

### Configuration knobs in action

- **`columns_to_use`** is loaded for `Maximums`, `ccA`, and `POMM` formats and
  drives how `MaxDataProcessor.read_maximums_csv` selects CSV columns and dtypes
  before validating the header order.【F:ryan_library/processors/tuflow/base_processor.py†L215-L238】【F:ryan_library/processors/tuflow/max_data_processor.py†L14-L54】
- **`expected_in_header`** is attached for timeseries-style formats and is used
  by the timeseries pipeline when reshaping long-form DataFrames and validating
  the resulting column order.【F:ryan_library/processors/tuflow/base_processor.py†L215-L241】【F:ryan_library/processors/tuflow/timeseries_processor.py†L253-L344】
- **`output_columns`** always loads with the data type definition and feeds
  `BaseProcessor.apply_output_transformations`, which coerces the processed
  DataFrame to the configured dtypes before downstream aggregation.【F:ryan_library/processors/tuflow/base_processor.py†L206-L371】

## Processor lifecycle and shared post-processing

After a concrete processor reads its dataset it calls back into the base class
helpers to apply consistent metadata enrichment:

- `add_common_columns()` orchestrates three steps: `add_basic_info_to_df()`
  adds path-related columns and the canonical `internalName`,
  `run_code_parts_to_df()` adds the `R01`/`R02` parts, and
  `additional_attributes_to_df()` attaches the TP/Duration/AEP text and numeric
  projections together with the trimmed run code.【F:ryan_library/processors/tuflow/base_processor.py†L182-L289】
- `apply_output_transformations()` applies the dtype mapping declared in the
  configuration for each data type, ensuring downstream processes receive
  consistent schemas regardless of the raw CSV typing.【F:ryan_library/processors/tuflow/base_processor.py†L291-L329】

These helpers also convert categorical metadata to ordered pandas `category`
columns to keep grouping and sorting deterministic.【F:ryan_library/processors/tuflow/base_processor.py†L215-L289】

## Shared metadata columns

The metadata columns added by the base class fall into three groups:

- **Identification & provenance**: `internalName`, `rel_path`, `path`,
  `directory_path`, `rel_directory`, and `file` describe the source file and
  run name.【F:ryan_library/processors/tuflow/base_processor.py†L204-L226】
- **Run segmentation**: `R01`, `R02`, … capture the run code tokens in
  order, allowing comparisons across run code components or scenario families.【F:ryan_library/classes/tuflow_string_classes.py†L78-L105】【F:ryan_library/processors/tuflow/base_processor.py†L228-L246】
- **Hydrologic attributes**: `trim_runcode` plus the text and numeric variants
  of TP (`tp_text`, `tp_numeric`), Duration (`duration_text`,
  `duration_numeric`), and AEP (`aep_text`, `aep_numeric`) standardise the key
  storm descriptors in both human-readable and numeric form.【F:ryan_library/classes/tuflow_string_classes.py†L33-L71】【F:ryan_library/processors/tuflow/base_processor.py†L248-L289】

Because these columns are injected into every processed DataFrame they form the
key alignment attributes for the aggregation steps.

## Processor-specific workflows

### `POMMProcessor`

`POMMProcessor` loads the chart-style CSV with `pd.read_csv(..., header=None)`
so that the first column of row labels can be dropped before transposing the
matrix into a tidy orientation.【F:ryan_library/processors/tuflow/POMMProcessor.py†L43-L60】  The first row after the transpose
becomes the header that is validated against `processingParts.expected_in_header`
from the configuration, which mirrors the original POMM row labels prior to the
rename step.【F:ryan_library/processors/tuflow/POMMProcessor.py†L75-L89】【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L2-L35】  Once the expected strings are present the
processor renames them to the canonical `Type`, `Location`, `Max`, `Min`,
`Tmax`, and `Tmin` columns and casts the subset covered by `columns_to_use`
using the types declared in the same JSON block.【F:ryan_library/processors/tuflow/POMMProcessor.py†L80-L104】【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L18-L35】

Two defensive checks guard the downstream metrics calculation. Missing rename
sources raise a `DataValidationError`, while `_derive_abs_metrics` short-circuits
if either `Max` or `Min` slipped through the earlier validation so that partial
files do not trigger a crash.【F:ryan_library/processors/tuflow/POMMProcessor.py†L82-L104】【F:ryan_library/processors/tuflow/POMMProcessor.py†L26-L36】  When both extrema exist, the helper derives
`AbsMax` and `SignedAbsMax` before the shared metadata is attached.

POMM outputs are assigned their own `dataformat` in
`tuflow_results_validation_and_datatypes.json`, so the collection class combines
them with `ProcessorCollection.pomm_combine`—a straight concatenation without
grouping—rather than the standard time- or channel-based aggregations that are
used for other formats.【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L18-L35】【F:ryan_library/processors/tuflow/processor_collection.py†L182-L206】

### `POProcessor`

`POProcessor` also starts with `pd.read_csv(..., header=None, dtype=str)` to
capture the two header rows that describe measurement type and location before
handing the DataFrame to `_parse_point_output`.【F:ryan_library/processors/tuflow/POProcessor.py†L16-L37】  The parser drops the first
column (which contains run metadata), splits the remaining table into the
measurement row, location row, and data body, then coerces all numeric rows to
floats.【F:ryan_library/processors/tuflow/POProcessor.py†L59-L83】  `_locate_time_column` searches both header rows for a
`Time` label so that even files with inconsistent casing or placement can be
normalised; failing to find one aborts processing early with a clear log message.【F:ryan_library/processors/tuflow/POProcessor.py†L71-L144】

Each measurement column is converted to `float64` values and discarded when the
series is entirely NaN, ensuring that blank sensors do not pollute the long-form
output. Remaining columns are stacked into tidy frames with `Time`, `Location`,
`Type`, and `Value` columns, mirroring the schema declared in the JSON
configuration.【F:ryan_library/processors/tuflow/POProcessor.py†L85-L131】【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L38-L56】  The `processingParts.dataformat` stays set to
`Timeseries` so PO results participate in the same downstream expectations and
grouping rules as other time-stepped outputs even though the raw layout is
re-shaped in bespoke code.【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L49-L56】【F:ryan_library/processors/tuflow/processor_collection.py†L35-L81】

At the collection stage PO processors can either flow through the generic
timeseries combiner or use the dedicated `po_combine` helper, which simply
concatenates tidy outputs and sorts them by run, location, type, and time. This
contrasts with the POMM path where `pomm_combine` performs no grouping because
the processor already materialises the max/min rows it needs.【F:ryan_library/processors/tuflow/processor_collection.py†L35-L206】【F:ryan_library/processors/tuflow/processor_collection.py†L208-L238】

## Downstream aggregation

`ProcessorCollection` consumes processed `BaseProcessor` instances and merges
their DataFrames according to the configured data format:

- `combine_1d_timeseries` groups by `internalName`, `Chan ID`, and `Time` after
  dropping file path columns, so the shared run metadata ensures all runs align
  before calculating per-time-step maxima.【F:ryan_library/processors/tuflow/processor_collection.py†L24-L71】
- `combine_1d_maximums` removes redundant provenance columns, groups by
  `internalName` and `Chan ID`, then reorders the output so the shared run code
  and hydrologic attributes (`trim_runcode`, `aep_text`, `duration_text`,
  `tp_text`, plus their numeric counterparts) remain adjacent for reporting and
  deduplication.【F:ryan_library/processors/tuflow/processor_collection.py†L73-L132】
- `combine_raw` and `pomm_combine` rely on the same metadata when concatenating
  heterogeneous results, with the categorical ordering applied by the base class
  keeping downstream sorting predictable.【F:ryan_library/processors/tuflow/processor_collection.py†L134-L178】

Together these steps let the pipeline ingest arbitrary TUFLOW outputs, discover
an appropriate processor at runtime, enrich the data with a stable metadata
backbone, and merge results across scenarios with minimal manual wiring.

## Testing

The processor library is supported by a comprehensive test suite located in `tests/processors/tuflow`.

### Running Tests

Run the full suite using `pytest`:

```bash
pytest tests/processors/tuflow
```

### Testing Strategy

1.  **Robustness Tests**: `test_robustness.py` verifies that the `BaseProcessor` and its factory methods handle invalid files, missing configurations, and malformed data gracefully without crashing.
2.  **Concrete Processor Tests**: Each processor type (e.g., `Cmx`, `H`, `POMM`) has a dedicated test file (e.g., `test_cmx_processor.py`) that checks:
    *   **Happy Path**: Loading a valid file produces a DataFrame with the correct schema and data.
    *   **Edge Cases**: Empty files, missing headers, or data type mismatches are handled correctly (usually by logging an error and returning `processed=False` or an empty DataFrame).
    *   **Integration**: Verifies that the processor integrates correctly with `ProcessorCollection` (where applicable).

### Adding New Tests

When adding a new processor or fixing a bug:
1.  Add a sample CSV file to `tests/data` (or mock it in the test).
2.  Create or update the corresponding `test_<type>_processor.py`.
3.  Use the shared fixtures in `conftest.py` to simplify test setup.
