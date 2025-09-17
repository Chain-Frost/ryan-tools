# TUFLOW processor development notes

This document summarises how the processor infrastructure within `ryan_library.processors.tuflow` fits
together and how to extend it safely.

## Combining processed outputs

### Building a collection

Use `ProcessorCollection` when you need to collate multiple processed objects into a single DataFrame. The
collection only accepts processors whose `processed` flag is `True`, so call the appropriate `process()`
method before adding an instance:

```python
collection = ProcessorCollection()
processor = BaseProcessor.from_file(file_path)
processor.process()
collection.add_processor(processor)
```

`get_processors_by_data_type()` can subset a larger collection for downstream grouping, and
`check_duplicates()` reports files that share the same run-code (`internalName`) and data type. Both helpers
return a new `ProcessorCollection`, allowing you to chain additional combine calls.

### Combination helpers and column ordering

Each combination method targets a specific `dataformat` (or `data_type` in the case of PO files). Choose the
method that matches the format recorded in `processingParts.dataformat` in the configuration:

| Format selector | Combination helper | Purpose |
| --- | --- | --- |
| `Timeseries` | `combine_1d_timeseries()` | Groups by `internalName`, `Chan ID`, and `Time`, then aggregates values. |
| `Timeseries` (PO exports) | `po_combine()` | Concatenates and sorts PO long-form data with no extra grouping. |
| `Maximums`, `ccA` | `combine_1d_maximums()` | Drops timing metadata and groups by `internalName` and `Chan ID`. |
| `POMM` | `pomm_combine()` | Concatenates already tidy POMM tables. |
| Any | `combine_raw()` | Concatenates everything without grouping. |

All combination helpers delegate to the shared dataframe reordering utilities so downstream scripts see a
consistent schema:

* `reorder_long_columns()` pushes file path metadata columns (`file`, `rel_path`, etc.) to the right-hand
  side so key metrics remain visible.
* `reorder_columns()` (used by `combine_1d_maximums`) keeps scenario metadata and primary measurements at
  the front, then alphabetically appends derived columns.
* `reset_categorical_ordering()` alphabetises categorical columns and replaces missing values with `pd.NA`
  to avoid stale category states when multiple runs are combined.

After merging, expect the resulting DataFrame to retain the column order dictated by these helpers; custom
post-processing should preserve that order to avoid confusing downstream tooling.

## Validation checklist for new or updated processors

Before checking in a new processor, confirm the following:

* The processor sets `self.processed = True` when successful so `ProcessorCollection.add_processor()`
  accepts the instance.
* `self.df` is non-empty for successful runs; log and return an empty DataFrame when validation fails.
* The columns defined in the configuration are present in `self.df` before calling
  `apply_output_transformations()` so dtype coercion succeeds.
* `add_common_columns()` has been called (unless deliberately skipped) to append run metadata required by
  the grouping helpers.
* Any subclass-specific validation (for example header checks or NaN pruning) happens before the DataFrame
  is exposed to calling code.

## Adding a new data type

1. **Pick a base class.**
   * Extend `TimeSeriesProcessor` when the raw export is a time series that needs the shared read → clean →
     melt pipeline. Implement `process_timeseries_raw_dataframe()` to reshape the long-form frame produced
     by the base class.
   * Extend `MaxDataProcessor` for 1D maximums/ccA style tables. Call `read_maximums_csv()` inside your
     `process()` implementation, reshape the DataFrame, then finish with `add_common_columns()` and
     `apply_output_transformations()`.
   * Extend `BaseProcessor` directly for specialised formats (for example the POMM transpose workflow).

2. **Register configuration.** Add a new entry to
   `ryan_library/classes/tuflow_results_validation_and_datatypes.json` containing:
   * `processor`: the class name.
   * `suffixes`: every filename suffix that should instantiate the processor.
   * `output_columns`: the final column → dtype mapping expected after processing.
   * `processingParts`: include `dataformat` (used by `ProcessorCollection` helpers), and set `module` if the
     class lives in a subpackage (for example `"1d_timeseries"`). Supply `columns_to_use` or
     `expected_in_header` when the reader needs to validate or trim inputs.
   Re-run the tool or script that loads the file to confirm the JSON remains valid.

3. **Implement subclass hooks.**
   * Timeseries processors must return a `ProcessorStatus` from
     `process_timeseries_raw_dataframe()` and update `self.df` in place. Use helpers like
     `_normalise_value_dataframe()` to keep the dataset tidy.
   * Maximum/ccA processors should keep `self.raw_df` for debugging, reshape into the final schema, and
     remove malformed rows before calling `add_common_columns()`.
   * Custom processors should log clearly when required columns are missing and ensure `validate_data()`
     still runs before marking the instance as processed.

4. **Mark success consistently.** After any subclass-specific work, call `add_common_columns()`, apply the
   configured dtype mappings, run `validate_data()`, then set `self.processed = True`. This keeps the
   behaviour consistent with existing processors and allows collections to merge the outputs without
   special cases.
