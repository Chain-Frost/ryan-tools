# ryan-tools

`ryan-tools` is a collection of Python utilities for TUFLOW, RORB, 12D, GDAL, QGIS, and general data
processing workflows. The reusable code lives in `ryan_library`; the files under `ryan-scripts` are mostly
project wrappers that call into that library.

The package is published as `ryan_functions`, but most user imports are from `ryan_library`.

## Status


// This readme was AI generated and needs to be cleaned and made more succinct. //

This repository is actively being cleaned up as tools are reused. The TUFLOW processors and supporting
helpers are the most structured part of the codebase. Many legacy scripts are still useful, but their quality
varies.

Python support is currently:

- `pyproject.toml`: targets Python 3.13 for Black and Pyright.
- `setup.py`: allows Python >=3.12.
- Development should prefer Python 3.13 where possible.

Tests exist, but many are historical or environment-dependent. Treat targeted local validation as more useful
than running the whole test suite unless you are specifically working on tests.

## Repository Map

```text
ryan-tools/
|-- ryan_library/              # Main reusable Python package
|   |-- classes/               # Config, metadata, and TUFLOW filename parsing
|   |-- functions/             # Reusable workflow functions
|   |-- processors/            # Structured TUFLOW result processors
|   `-- scripts/               # Orchestrators used by wrapper scripts
|-- ryan-scripts/              # Human-facing wrappers and older standalone scripts
|-- repo-scripts/              # Build, venv, and snippet helper scripts
|-- vendor/                    # Vendored dependencies
|-- tests/                     # Historical and targeted tests
|-- excel-tools/               # Workbook/model assets, not Python package code
|-- QGIS-Styles/               # QGIS style/layout assets
|-- requirements.txt
|-- pyproject.toml
`-- setup.py
```

## Setup

On Windows, use Python 3.13 if available:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -r requirements.txt
```

The package can also be installed from the wheel in `dist/` when dependency isolation is awkward:

```powershell
python -m pip install --break-system-packages dist/ryan_functions-*.whl
```

For local development after changing `ryan_library/` or package metadata, run:

```powershell
python repo-scripts/build_library.py
```

Use `python repo-scripts/build_library.py --skip-artifacts` when wheel artifacts cannot be created or committed
in the current environment.

## Useful Entry Points

### Parse TUFLOW filenames and run codes

Use `TuflowStringParser` when you have a TUFLOW result path and want the inferred data type, raw run code,
cleaned run code, AEP, duration, and temporal pattern fields.

```python
from pathlib import Path

from ryan_library.classes.tuflow_string_classes import TuflowStringParser

parser = TuflowStringParser(Path("M11_01p_00120m_TP01_1d_Q.csv"))

print(parser.data_type)
print(parser.raw_run_code)
print(parser.clean_run_code)
print(parser.aep)
print(parser.duration)
print(parser.tp)
```

The parser uses suffix configuration from
`ryan_library/classes/tuflow_results_validation_and_datatypes.json`, so it should stay aligned with the
processor factory.

### Process one TUFLOW result file

Use `BaseProcessor.from_file()` for ad-hoc processing of a single supported result file. The factory chooses
the correct concrete processor from the configured suffix and data type.

```python
from pathlib import Path

from ryan_library.processors.tuflow.base_processor import BaseProcessor

processor = BaseProcessor.from_file(Path("M11_01p_00120m_TP01_1d_Q.csv"))
processor.process()

df = processor.df
print(processor.data_type)
print(df.head())
```

For large files or repeated analysis, call `processor.discard_raw_dataframe()` after `processor.df` has the
processed data you need.

### Collect and process many TUFLOW files

Use `collect_files()` and `process_file()` for small custom workflows, or `process_files_in_parallel()` for a
batch that returns a `ProcessorCollection`.

```python
from pathlib import Path

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.tuflow_common import collect_files, process_files_in_parallel

roots = [Path("results")]
data_types = ["Nmx", "Cmx", "Chan", "ccA", "EOF"]

files = collect_files(
    paths_to_process=roots,
    include_data_types=data_types,
    suffixes_config=SuffixesConfig.get_instance(),
)

with setup_logger(console_log_level="INFO") as log_queue:
    collection = process_files_in_parallel(
        file_list=files,
        log_queue=log_queue,
        log_level="INFO",
        entity_filters=["Culvert_01", "Culvert_02"],
    )

maximums = collection.combine_1d_maximums()
raw = collection.combine_raw()
```

`entity_filters` can be a single collection of IDs, or a mapping keyed by data type.

### Use `ProcessorCollection`

`ProcessorCollection` is the main object for combining processed TUFLOW outputs. Useful methods include:

- `combine_1d_maximums()` for `Nmx`, `Cmx`, `Chan`, `ccA`, `RLL_Qmx`, and optionally `EOF`.
- `combine_1d_timeseries()` for 1D `Q`, `H`, `V`, and `CF` time series, with optional `Chan`/`EOF` attributes.
- `po_combine()` for `_PO.csv` outputs.
- `pomm_combine()` for `_POMM.csv` outputs.
- `combine_raw()` for concatenating processed rows without a specialist grouping step.
- `filter_locations([...])` for post-load filtering.
- `compact_basic_info_columns()` and `attach_basic_info(...)` for reducing repeated path metadata in memory.
- `to_hdf(...)` and `from_hdf(...)` for caching a processed collection.
- `check_duplicates()` for run-code/data-type duplicate checks.

```python
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

collection = ProcessorCollection()
collection.add_processor(processor)

duplicates = collection.check_duplicates()
combined = collection.combine_raw()
```

### Notebook-friendly TUFLOW loading

For notebooks and quick exploration, `load_tuflow_data()` wraps file discovery, logging setup, processing, and
optional location filtering.

```python
from ryan_library.functions.tuflow.notebook_helpers import load_tuflow_data

collection = load_tuflow_data(
    paths=["results"],
    data_types=["Q", "V", "H", "Nmx", "Cmx", "Chan", "EOF"],
    parallel=True,
    locations=["Culvert_01"],
)

timeseries = collection.combine_1d_timeseries()
maximums = collection.combine_1d_maximums()
```

### Ready-made TUFLOW workflow orchestrators

The modules under `ryan_library.scripts.tuflow` are the preferred entry points when you want the existing
workflow behavior and export format.

```python
from pathlib import Path

from ryan_library.scripts.tuflow.tuflow_culverts_merge import main_processing as merge_culvert_maximums

merge_culvert_maximums(
    paths_to_process=[Path("results")],
    include_data_types=["Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF"],
    locations_to_include=["Culvert_01"],
    output_dir=Path("outputs"),
    export_mode="both",
)
```

Useful orchestrator modules include:

- `ryan_library.scripts.tuflow.tuflow_culverts_merge`
- `ryan_library.scripts.tuflow.tuflow_culverts_timeseries`
- `ryan_library.scripts.tuflow.tuflow_culverts_mean`
- `ryan_library.scripts.tuflow.pomm_combine`
- `ryan_library.scripts.tuflow.pomm_max_items`
- `ryan_library.scripts.tuflow.po_combine`
- `ryan_library.scripts.tuflow.peak_check_po_csvs`
- `ryan_library.scripts.tuflow.tuflow_timeseries_stability`
- `ryan_library.scripts.tuflow.tuflow_logsummary`
- `ryan_library.scripts.tuflow.tuflow_results_styling`

The files in `ryan-scripts/TUFLOW-python/` are ready-made wrappers for many of these workflows. They are useful
when you want a double-clickable or command-line script with editable constants near the top of the file,
rather than writing your own import snippet.

Common TUFLOW wrappers include:

- `ryan-scripts/TUFLOW-python/TUFLOW_Culvert_Maximums.py`: calls
  `ryan_library.scripts.tuflow.tuflow_culverts_merge`.
- `ryan-scripts/TUFLOW-python/TUFLOW_Culvert_Timeseries.py`: calls
  `ryan_library.scripts.tuflow.tuflow_culverts_timeseries`.
- `ryan-scripts/TUFLOW-python/POMM_combine.py`: calls `ryan_library.scripts.tuflow.pomm_combine`.
- `ryan-scripts/TUFLOW-python/PO_combine.py`: calls `ryan_library.scripts.tuflow.po_combine`.
- `ryan-scripts/TUFLOW-python/TUFLOW_Timeseries_Peaks_Check.py`: calls the PO peak-check workflow.
- `ryan-scripts/TUFLOW-python/TUFLOW_Timeseries_Stability.py`: calls the timeseries stability workflow.
- `ryan-scripts/TUFLOW-python/LogSummary.py`: calls the TUFLOW `.tlf` log summary workflow.
- `ryan-scripts/TUFLOW-python/TUFLOW_Results_Styling.py`: calls the results styling workflow.
- `ryan-scripts/TUFLOW-python/run_tuflow_batch.py`: ready-made batch runner for TUFLOW runs.
- `ryan-scripts/TUFLOW-python/set_layer_to_filename_v6.py`: utility for renaming a GeoPackage layer to match
  the file name.

Most wrappers support command-line overrides for common options such as working directory, data types, log
level, and location filters, while still allowing quick edits to script constants.

## Supported TUFLOW Processor Data Types

The currently configured TUFLOW data types are:

| Data type | Processor | Format | Suffixes |
| --- | --- | --- | --- |
| `POMM` | `POMMProcessor` | `POMM` | `_POMM.csv` |
| `PO` | `POProcessor` | `PO` | `_PO.csv` |
| `Cmx` | `CmxProcessor` | `Maximums` | `_1d_Cmx.csv` |
| `Nmx` | `NmxProcessor` | `Maximums` | `_1d_Nmx.csv` |
| `Chan` | `ChanProcessor` | `Maximums` | `_1d_Chan.csv` |
| `ccA` | `ccAProcessor` | `ccA` | `_1d_ccA_L.dbf`, `_Results1D.gpkg`, `_Results.gpkg` |
| `RLL_Qmx` | `RLLQmxProcessor` | `Maximums` | `_RLL_Qmx.csv` |
| `Q` | `QProcessor` | `Timeseries` | `_1d_Q.csv` |
| `H` | `HProcessor` | `Timeseries` | `_1d_H.csv` |
| `CF` | `CFProcessor` | `Timeseries` | `_1d_CF.csv` |
| `V` | `VProcessor` | `Timeseries` | `_1d_V.csv` |
| `EOF` | `EOFProcessor` | `EOF` | `.eof` |
| `TLF` | `TLFProcessor` | `TLF` | `.tlf` |

Update `ryan_library/classes/tuflow_results_validation_and_datatypes.json` when adding a processor or suffix.
See `ryan_library/processors/tuflow/README.md` for processor development notes.

## Other Ad-hoc Processing Helpers

### PO peak and stability checks

Use `ryan_library.functions.tuflow.po_timeseries_checks` for direct analysis of PO and Q CSV files:

```python
from pathlib import Path

from ryan_library.functions.tuflow.po_timeseries_checks import PeakCheckConfig, analyze_peak_csv

results = analyze_peak_csv(
    Path("M11_01p_00120m_TP01_PO.csv"),
    PeakCheckConfig(
        datatype_include=["Flow"],
        datatype_case_sensitive=False,
        location_include=[],
        location_exclude=[],
        location_case_sensitive=False,
        warn_2hours=2.0,
        warn_1hour=1.0,
        flat_tol=1e-6,
    ),
)
```

The same module exposes `StabilityCheckConfig`, `analyze_stability_csv()`,
`analyze_stability_q_csv()`, `flatten_peak_results()`, and `flatten_stability_results()`.

### POMM summary utilities

Use `ryan_library.functions.tuflow.pomm_utils` when you already have POMM-style outputs and want peak, median,
or mean summaries:

```python
from pathlib import Path

from ryan_library.functions.tuflow.pomm_utils import aggregated_from_paths, find_aep_dur_max, find_aep_max

aggregated = aggregated_from_paths([Path("results")])
aep_duration_max = find_aep_dur_max(aggregated)
aep_max = find_aep_max(aep_duration_max)
```

Related helpers include `find_aep_dur_median()`, `find_aep_median_max()`, `find_aep_dur_mean()`,
`find_aep_mean_max()`, and `save_peak_report()`.

### RORB hydrograph utilities

Use `ryan_library.functions.RORB.read_rorb_files` to find `batch.out` files, parse run metadata, and analyse
hydrograph CSV threshold durations:

```python
from pathlib import Path

from ryan_library.functions.RORB.read_rorb_files import find_batch_files, parse_batch_output

batch_files = find_batch_files([Path("rorb_outputs")])
runs = [parse_batch_output(path) for path in batch_files]
```

### 12D culvert utilities

Use `ryan_library.functions.process_12D_culverts` for parsing 12D `.rpt` and `.txt` culvert exports:

```python
from pathlib import Path

from ryan_library.functions.process_12D_culverts import get_combined_df_from_files

culverts = get_combined_df_from_files(Path("12d_exports"))
```

### Excel workbook and model tools

The `excel-tools/` folder contains non-Python assets that are still part of the working toolbox. They are not
importable package code, but they are useful starting points for manual workflows:

- `excel-tools/TUFLOW/TUFLOW culverts.xlsx`: culvert result and model-prep workbook.
- `excel-tools/TUFLOW/TUFLOW Nested Frequency Storms v2.xlsx`: nested frequency storm workbook.
- `excel-tools/TUFLOW/bc_dbase_trapezoid_03.xlsx`: boundary condition database helper.
- `excel-tools/Culvert Sizing-12D.xlsx`: culvert sizing workbook for 12D-style workflows.
- `excel-tools/Exceedance Probability Spreadsheet AEP_v4.xlsx`: AEP/exceedance probability calculations.
- `excel-tools/format_IFD.xlsx`: IFD formatting helper.
- `excel-tools/TUFLOW/*.model3`: QGIS processing model files for generating common TUFLOW inputs such as
  `1d_nwk`, `2d_bc`, `2d_sx`, and `2d_zsh` layers from tabular inputs.

Treat these as templates: copy or open them for a project workflow, but do not expect them to be installed with
the Python package.

### GDAL batch tools

The `ryan-scripts/gdal-bat/` folder contains Windows batch scripts for common raster processing jobs. They
try to load an OSGeo4W or QGIS GDAL environment, then run GDAL command-line tools over the current folder or a
supplied target folder.

Useful scripts include:

- `gdal_translate_TIF_ovr_v9.bat`: converts `flt`, `asc`, and `rst` rasters to tiled, DEFLATE-compressed
  GeoTIFFs and builds overviews.
- `gdaladdo_Pyramids_deflate_v5.bat`: builds compressed overview pyramids for `.tif` files.
- `gdal_merge_CLI.bat`: builds a VRT from matching rasters and translates it to a merged GeoTIFF.
- `gdal_edit_Set_nodata.bat`: applies a NoData value to rasters matching a pattern.
- `gdal_flood_extent_v9.bat`: creates flood extent polygons from rasters using a cutoff threshold.
- `gdal_flood_extent_sieve.bat`: flood extent variant with sieve-style cleanup.
- `gdal_raster_footprint.bat`: creates valid-data footprint polygons for rasters.
- `gdal_translate_TIF_ovr_PATH.bat` and variants: path-based conversion helpers for tiled GeoTIFF and
  overview generation.

Several older variants remain for reference, including files marked `old` or `not working`. Prefer the newest
plainly named version unless you are reproducing an old workflow.

### DataFrame and export helpers

Common helpers used across workflows:

- `ryan_library.functions.dataframe_helpers`: `merge_and_sort_data()`, `reorder_columns()`,
  `reorder_long_columns()`, `reset_categorical_ordering()`.
- `ryan_library.functions.misc_functions.ExcelExporter`: export one or more DataFrames to Excel and/or Parquet.
- `ryan_library.functions.file_utils`: recursive file discovery and output directory helpers.
- `ryan_library.functions.loguru_helpers`: serial and multiprocessing Loguru setup.

## Automation Support

This repository is configured for automated pull requests generated by tools such as ChatGPT Codex.
The pull request template at `.github/pull_request_template.md` captures the required summary, testing
commands, and checklists so that automated agents provide consistent information. Keep the template up to
date when workflows change to ensure the automation continues to produce useful PRs.

Automated code reviews by Codex should follow `.github/code_review_instructions.md`, which documents the
repository structure, review checklist, and feedback expectations. Update the guide when review patterns
change so that automated reviewers stay aligned with human preferences.

## VS Code + Codex Agent Setup On Windows

### 1. Pick a Python interpreter

**Recommended: repo-local virtual environment.** Keeping everything inside `.venv` gives Codex a stable
interpreter path so it stops asking for approvals every time it opens a terminal, and it isolates dependencies
from your global install to avoid version drift between human and agent sessions.

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
```

After those steps, the checked-in `.vscode/settings.json` automatically pins
`${workspaceFolder}\.venv\Scripts\python.exe` for both you and the Codex extension.

**Alternative: reuse an existing interpreter.** Update the `python.defaultInterpreterPath` setting, or set the
`RYAN_TOOLS_PYTHON` environment variable, to the interpreter you prefer. The repo task below falls back to
that variable or to `python` on `PATH` if `.venv` is missing. Expect VS Code to prompt for interpreter approval
more often in this mode because the path can change between sessions.

To save the back-and-forth, run `python repo-scripts/ensure_venv.py` or the `python:ensure-venv` task before
you start a Codex session. The script will create `.venv` if it is missing, upgrade `pip`, and install
`requirements.txt`. It also stores a hash so `python repo-scripts/ensure_venv.py --check-only` can tell you
whether the environment is still in sync when you come back later.

### 2. Keep the shell stable

Leave the integrated terminal on the default PowerShell profile. Changing shells mid-session forces the Codex
agent to request approvals again.

### 3. Provide a safe entry point for snippets

Use the pre-defined `python:stdin` task, available from Tasks -> Run Task -> `python:stdin`, and pipe code into
it. The task automatically selects the interpreter from step 1 and executes the code through
`repo-scripts/run_snippet.py`, so the agent does not have to craft fragile `python -c` strings. To launch a
different snippet runner, set `RYAN_TOOLS_SNIPPET` to the alternative script path.

Codex can also call the `python:ensure-venv` task whenever it needs to guarantee the repo-managed environment
exists. That command resolves the same interpreter heuristics as `python:stdin`, so it works whether you stick
with `.venv` or point `RYAN_TOOLS_PYTHON` at a different install.

For anything non-trivial, put the code into a reusable script such as `python repo-scripts/<name>.py` or another
module in the repo so it can be rerun easily.
