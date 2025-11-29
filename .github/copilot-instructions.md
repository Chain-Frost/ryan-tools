# Copilot Instructions for ryan-tools

## Project Overview
- **ryan-tools** is a collection of Python utilities and scripts for geospatial and data processing, with a focus on TUFLOW, RORB, 12D, and GDAL workflows.
- The codebase is organized for modularity: core logic lives in `ryan-library/functions`, while `ryan-library/scripts` provides controllers, and `ryan-scripts` contains entry-point wrappers.
- Many scripts are wrappers that set up the environment and call into the main library.

## Key Directories
- `ryan-library/functions/`: Core reusable Python functions. Add new logic here.
- `ryan-library/scripts/`: Script controllers that orchestrate function calls. Keep these thin.
- `ryan-scripts/`: Entry-point scripts for end-user use that are copied to the relevant work locations. Should only handle calling into the library.
- `vendor/`: Vendored third-party code (e.g., PyHMA). Do not modify unless updating vendored code.
- `excel-tools/`, `QGIS-Styles/`: Not code—ignore for automation and code changes.
- `ryan-functions/`: Deprecated. Migrate any usage to `ryan-library`.

## Coding Conventions
- **Python 3.13** required. Use absolute imports from `ryan_library` or vendored packages only.
- Format code with [Black](https://github.com/psf/black) (120 character line length, see `pyproject.toml`).
- All public functions/methods must have type annotations (Python 3.13+ style).
- Use `mypy` for static analysis, but only on files you modify unless otherwise instructed.

## Developer Workflows
- **Build/Install**: Use `setup.py` or `requirements.txt` for dependencies. For local development, editable installs are supported.
- **Testing**: Tests are minimal. If adding tests, place them in `tests/` and follow the structure of the code under test.
- **Script Execution**: Most scripts in `ryan-scripts/` are run directly (e.g., `python ryan-scripts/TUFLOW-python/POMM-med-max-aep-dur.py`). They often change the working directory to their own location before running.
- **Logging**: Scripts typically accept a `log_level` argument and print to stdout.

## Patterns & Examples
- Controllers in `ryan-library/scripts` should delegate to functions in `ryan-library/functions`.
- Example entry-point: see `ryan-scripts/TUFLOW-python/POMM-med-max-aep-dur.py` for a typical wrapper pattern.
- Avoid duplicating logic between scripts—refactor into `functions/` as needed.

## Maximum/ccA processors
- **Pipeline overview**
  - *Read*: `MaxDataProcessor.read_maximums_csv` pulls only the configured columns/dtypes for each data type, stops early on empty frames and fails fast when headers diverge from the JSON contract.【F:ryan_library/processors/tuflow/max_data_processor.py†L11-L55】【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L59-L132】
  - *Reshape*: Each processor reshapes or augments the raw frame before the shared `BaseProcessor` post-processing kicks in:
    - `NmxProcessor._extract_and_transform_nmx_data` splits `Node ID`, filters out non-standard suffixes, pivots to `US_h`/`DS_h`, and enforces the expected column set.【F:ryan_library/processors/tuflow/1d_maximums/NmxProcessor.py†L12-L106】
    - `CmxProcessor._reshape_cmx_data` normalises the Q/V maxima into long form while `_handle_malformed_data` drops rows with no values so downstream aggregation stays stable.【F:ryan_library/processors/tuflow/1d_maximums/CmxProcessor.py†L12-L97】
    - `ChanProcessor.process` derives culvert `Height`, renames legacy fields, and bails out when required geometry columns are missing.【F:ryan_library/processors/tuflow/ChanProcessor.py†L6-L68】
    - `ccAProcessor.process` dispatches to `process_dbf` or `process_gpkg` to read shapefile/geopackage sources, renames `Channel` to `Chan ID`, and only continues once a populated frame is available.【F:ryan_library/processors/tuflow/ccAProcessor.py†L12-L121】
  - *Validate & finalise*: All processors converge on `BaseProcessor.add_common_columns`, `apply_output_transformations`, and `validate_data` so run-code metadata, dtype casting, and empty-frame checks remain consistent.【F:ryan_library/processors/tuflow/base_processor.py†L261-L391】
- **Edge-case handling conventions**
  - Header checks go through `BaseProcessor.check_headers_match`, giving clear logs for missing/extra fields and reordering hints before the processor proceeds.【F:ryan_library/processors/tuflow/base_processor.py†L393-L433】
  - `MaxDataProcessor.read_maximums_csv` returns explicit status codes (success, empty data, header mismatch, read failure) so subclass `process` methods can short-circuit safely.【F:ryan_library/processors/tuflow/max_data_processor.py†L14-L55】
  - Reshapers filter abnormal inputs: `_extract_and_transform_nmx_data` drops pit suffixes, `_reshape_cmx_data` verifies each required column, and `_handle_malformed_data` strips all-null rows; `ChanProcessor` exits when geometry columns are absent; `ccAProcessor.process_gpkg` ignores geopackages missing the `1d_ccA_L` layer.【F:ryan_library/processors/tuflow/1d_maximums/NmxProcessor.py†L65-L105】【F:ryan_library/processors/tuflow/1d_maximums/CmxProcessor.py†L53-L97】【F:ryan_library/processors/tuflow/ChanProcessor.py†L23-L48】【F:ryan_library/processors/tuflow/ccAProcessor.py†L79-L121】
- **Expected outputs**
  - `Cmx`: `Chan ID`, `Time`, `Q`, `V`
  - `Nmx`: `Chan ID`, `Time`, `US_h`, `DS_h`
  - `Chan`: `Chan ID`, `Length`, `n or Cd`, `pSlope`, `US Invert`, `DS Invert`, `US Obvert`, `Height`, `pBlockage`, `Flags`
  - `ccA`: `Chan ID`, `pFull_Max`, `pTime_Full`, `Area_Max`, `Area_Culv`, `Dur_Full`, `Dur_10pFull`, `Sur_CD`, `Dur_Sur`, `pTime_Sur`, `TFirst_Sur`
  - Use `output_columns` in `tuflow_results_validation_and_datatypes.json` as the contract for dtype casting and regression tests when extending these processors.【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L59-L168】

## Timeseries processors
- **Pipeline overview**
  - *Read*: `TimeSeriesProcessor.read_and_process_timeseries_csv` wraps `_read_csv`, `_clean_headers`, and `_reshape_timeseries_df` so every dataset starts from a tidy long-form frame, then `_apply_final_transformations` coerces numeric types.【F:ryan_library/processors/tuflow/timeseries_processor.py†L20-L282】
  - *Reshape*: `reshape_h_timeseries` handles upstream/downstream H series; otherwise the melt keeps either `Chan ID` or `Location`. Subclasses such as `QProcessor` and `VProcessor` invoke `_normalise_value_dataframe` to enforce `[Time, identifier, value]` order and strip empty measurements, while `POProcessor._parse_point_output` performs its own transpose-like cleanup of the multi-row header format.【F:ryan_library/processors/tuflow/timeseries_helpers.py†L1-L33】【F:ryan_library/processors/tuflow/1d_timeseries/QProcessor.py†L11-L33】【F:ryan_library/processors/tuflow/1d_timeseries/VProcessor.py†L11-L22】【F:ryan_library/processors/tuflow/timeseries_processor.py†L283-L343】【F:ryan_library/processors/tuflow/POProcessor.py†L11-L131】
  - *Validate & finalise*: After the dataset-specific hook runs, the base class adds run-code metadata, applies JSON-driven dtype mappings, and refuses to mark the processor complete if validation fails.【F:ryan_library/processors/tuflow/timeseries_processor.py†L33-L116】【F:ryan_library/processors/tuflow/base_processor.py†L261-L391】
- **Edge-case handling conventions**
  - `_clean_headers` drops the placeholder first column, normalises `Time (h)` aliases, and raises when a `Time` column cannot be recovered.【F:ryan_library/processors/tuflow/timeseries_processor.py†L149-L188】
  - `_reshape_timeseries_df` swaps between `Chan ID` and `Location`, reuses `reshape_h_timeseries` for two-value H exports, and sets `expected_in_header` so `check_headers_match` can reject malformed melts.【F:ryan_library/processors/tuflow/timeseries_processor.py†L216-L265】
  - `_normalise_value_dataframe` enforces a single identifier column, drops empty rows, and returns granular status codes (`FAILURE`, `EMPTY_DATAFRAME`, `HEADER_MISMATCH`) so callers can log precise causes; PO parsing similarly guards against missing header rows, non-numeric times, all-NaN columns, and absent measurement data.【F:ryan_library/processors/tuflow/timeseries_processor.py†L283-L343】【F:ryan_library/processors/tuflow/POProcessor.py†L47-L131】
  - Header validation ultimately runs through `BaseProcessor.check_headers_match`, so custom processors should always set `expected_in_header` before returning a frame.【F:ryan_library/processors/tuflow/base_processor.py†L393-L433】
- **Expected outputs**
  - `Q`: `Time`, `Chan ID`, `Q`
  - `V`: `Time`, `Chan ID`, `V`
  - `PO`: `Time`, `Location`, `Type`, `Value`
  - Keep the `output_columns` mapping in sync with new processors so dtype coercion continues to succeed for downstream reporting and tests.【F:ryan_library/classes/tuflow_results_validation_and_datatypes.json†L40-L247】

## Integration & Dependencies
- External dependencies are managed via `requirements.txt`.
- Some scripts expect specific directory structures or data files (see comments in wrappers for details).

## Special Notes
- Ignore `excel-tools/`, `QGIS-Styles/`, and `ryan-functions/` for new development.
- If you find code in `ryan-functions/`, migrate it to `ryan-library/`.
- When in doubt, prefer adding new logic to `ryan-library/functions` and keep scripts as thin as possible.

---

For more details, see `AGENTS.md` and `README.md` in the project root.
