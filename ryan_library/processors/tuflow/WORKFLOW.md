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
