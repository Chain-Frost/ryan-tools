# TUFLOW simulation input preflight

The read-only preflight validates **proposed runs**, not just the literal contents of a TCF. It uses PyTUFLOW to
resolve each proposed combination of events and scenarios, recursively finds active input commands, and checks their
referenced files. It also reads the active BC database and checks its event-resolved Source CSV paths.

**No TUFLOW simulation is executed.** This is not an equivalent of the engine's full initialisation or validation.
It does not verify GIS geometry, hydrology, CSV time-series columns/content, the existence of all files referenced
inside every possible GIS attribute, or TUFLOW engine-specific syntax unknown to PyTUFLOW. A passing check does not
guarantee a successful hydraulic simulation.

## Install

Install the maintained package using your normal Python 3.14 installation:

    python -m pip install --upgrade ryan-tools

PyTUFLOW is a standard `ryan-tools` dependency and installs automatically, including when using the repository
`requirements.txt` (`-e .[dev,mcp]`). No separate extra or manual PyTUFLOW installation is needed. The installed
package is required for copied wrappers; do not insert checkout paths into sys.path.

## GPU sharing warning

The TUFLOW batch launcher checks the GPUs selected for each simulation immediately before starting its
process. By default, `warn_gpu_sharing=True`. If another **live local TUFLOW process** has an overlapping
explicit `-puN` assignment, the launcher prints a yellow warning identifying the GPU and process ID.
This includes other runs started by the current launcher and simulations launched separately.

No warning is issued for a GPU with no confirmed conflicting assignment. If an existing TUFLOW process has
no explicit `-puN` switch, or the process table cannot be inspected completely, the launcher reports that
GPU exclusivity cannot be established rather than incorrectly declaring the GPU free. No GPU allocation is
changed, no running process is terminated and no launch is delayed by this feature.

GPU IDs come from live process command lines. This follows the activity tracker's distinction between active
processes and historical TLF records, but deliberately does not infer GPU occupancy from a potentially stale TLF.
CodeMeter licence allocations also do not prove which local GPU is occupied.

## Launcher integration

Edit the settings at the top of the maintained
ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py:

    preflight_check=True,
    preflight_check_gpkg_layers=True,
    preflight_report_csv=Path("run_tuflow_simulations_preflight.csv"),

The preflight runs **after** the existing parameter-product/textfiles/both builders have created, deduplicated and
exported the exact simulation list, but **before** any TUFLOW process starts. The existing launcher defaults to
preflight_check=False until representative Windows models have been validated. Use command-line overrides:

    python run_tuflow_simulations.py --preflight-only --no-pause
    python run_tuflow_simulations.py --preflight --no-pause

The first command generates the run list and checks it without launching a simulation. The second checks it and
launches the batch only if every input check passes. The preflight-only option takes precedence over run_simulations.

## Standalone checker

Copy the maintained check_tuflow_inputs.py wrapper into the project and edit DEFAULT_TCF and DEFAULT_COMMANDS.
It uses the exact run combinations in the exported commands file. Alternatively pass CLI paths:

    python check_tuflow_inputs.py --tcf "runs\MODEL_01_~s1~_~e1~_~e2~.tcf" --runs run_tuflow_simulations_commands.txt --report input_preflight.csv --no-pause

The CSV lists one missing/unresolved dependency per affected run. A single missing BC source may therefore appear
for multiple runs. The console reports total proposed runs, unique file paths, issues and affected runs. For large
batches, file existence and GeoPackage layer checks are cached across runs.

The checker intentionally leaves model data untouched and writes only the report CSV. If an input is unresolved or
a model context cannot be parsed, the run **fails preflight** rather than being silently considered valid.

## Verification scope and limitations

- Full context resolution includes the supplied -e1..-e9 and -s1..-s9 switches and nested control files.
- Missing event-specific boundary source CSVs are checked from the resolved BC database Source column.
- GIS databases are checked for existence; when enabled, a referenced GeoPackage layer name must be present.
- Missing and unresolved dependencies have distinct issue statuses.
- File wildcard/variable references that cannot be expanded are reported as unresolved.
- Only files referenced by PyTUFLOW's recognised input commands and BC Source entries are covered. Input files
  referenced by specialised commands or unsupported data formats may need additional handlers.
- An empty or invalid run list is rejected. Output files which the engine will create are not required.
- The preflight reports cannot substitute for running TUFLOW with -x to check full input validity.
- PyTUFLOW compatibility with the user's project-specific variables and current TUFLOW engine is still subject to
  real-model Windows validation. Do not enable preflight by default until that is complete.

## Implementation and validation

- Shared parsing: ryan_library/functions/tuflow/run_flags.py
- Read-only orchestration: ryan_library/orchestrators/tuflow/input_preflight.py
- Standalone wrapper: ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py
- Existing launcher: run_tuflow_simulations.py
- Focused tests: tests/orchestrators/tuflow/test_input_preflight.py and tests/scripts/test_run_tuflow_simulations.py

Checks to run on the latest head in Python 3.14:

    python -m ruff check ryan_library/functions/tuflow/run_flags.py ryan_library/orchestrators/tuflow/input_preflight.py ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py tests/orchestrators/tuflow/test_input_preflight.py
    python -m ruff format --check ryan_library/functions/tuflow/run_flags.py ryan_library/orchestrators/tuflow/input_preflight.py ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py tests/orchestrators/tuflow/test_input_preflight.py
    python -m pytest -q tests/orchestrators/tuflow/test_input_preflight.py tests/scripts/test_run_tuflow_simulations.py
    python ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py --help
    python ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py --help

Follow up with Windows Python 3.14 + PyTUFLOW real-BC regression tests against representative 2026.3 HPC models.
