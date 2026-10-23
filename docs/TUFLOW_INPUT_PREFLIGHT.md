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
for multiple runs. The console reports total proposed runs, unique file paths, issues and affected runs.

For large batches, the checker first resolves the complete run-to-file dependency graph. It then deduplicates required
paths and scans each parent directory with at least 8 unique required files **once**, capped at 50,000 directory entries.
Sparse or exceptionally large directories use one targeted existence check per unique file. Referenced GeoPackage
layers are queried together using one read-only connection per GeoPackage. Results are mapped back to every affected
run and TUFLOW command, without repeat network file checks. These limits may be tuned through the library API;
the default avoids recursive network-drive scans. Output Folder, Log Folder and Write Check Files locations are
excluded because TUFLOW creates these destinations.

Network performance depends on filesystem layout, latency and number of entries per directory; a complete recursive
directory listing is deliberately avoided.


### Experimental shared-TCF batch collection (PR #148)

**Development-only Python API.** An opt-in `context_mode="batch"` uses
[PyTUFLOW batch optimisation PR #5](https://github.com/phantom-branch/PyTuflow/pull/5)
to collect immutable per-run source references for a whole proposed batch.
The common preflight then performs the *same* wildcard, Shapefile/GeoPackage,
secondary database source, raster metadata and file-existence checks.
The default is still `context_mode="individual"`, and the maintained
standalone checker and simulation launcher do not enable batch mode.

For a development fork installed in an isolated test environment:

```python
from pathlib import Path
from ryan_library.orchestrators.tuflow.input_preflight import (
    RunRequest,
    check_simulation_inputs,
)

runs = (
    RunRequest(1, {"s1": "EXG", "e1": "10.0p"}),
    RunRequest(2, {"s1": "DEV", "e1": "10.0p"}),
)
report = check_simulation_inputs(
    Path("runs/model.tcf"), runs, context_mode="batch"
)
```

The opt-in batch mode refuses an unavailable/incomplete fork API and
ambiguous stateful database activations; it does **not** silently fall
back or treat partial references as valid. No TUFLOW process launches.
The special three-way parity replay is `python -m
tests.fixtures.tuflow_example_repro.run_checks --batch` (temporary
repository diagnostic; not a shipped wrapper). Windows CI pins the
development fork separately; standard ryan-tools keeps its published
PyTUFLOW dependency and normal launcher defaults.

[Windows CI 39716802481](https://github.com/Chain-Frost/ryan-tools/actions/runs/39716802481)
verified eager, individual deferred and batch identities on all
161 archived TCFs / 174 simulations, with matching per-run public
preflight issues and checked paths. This does **not** validate
production network-drive performance, native model parity or complete
GIS BC feature-to-active-database selection. The user withdrew the
separate 42-run model as a merge gate.

### Ordered-input batch projection (experimental)

The [PyTUFLOW PR #6](https://github.com/phantom-branch/PyTuflow/pull/6)
batch API adds `ordered_inputs`, which describes **every** active command,
including output settings, variable assignments and database selections with
no file reference. The batch adapter now reproduces that exact command
sequence using each command's UUID, ordinal, owning control file and line.
It refuses duplicate/missing ordinals, orphaned file references and mismatched
provenance instead of silently accepting an incomplete inventory.

This enables the existing consumer's multiple-`BC Database` logic to bind
each `Read GIS BC/SA/RF` command to the **latest preceding active**
database declaration and select only its associated Name/Source entries.
The adapter exposes source rows by declaration UUID; requesting their
aggregate when multiple BC databases are active fails closed. Unsupported
schemas and ambiguous declarations remain unresolved.

For backwards compatibility, batches without `ordered_inputs` keep the
previous reference-bearing command projection and **reject** multiple
stateful BC activations. This is an opt-in development path; the default
individual collector and normal published PyTUFLOW dependency are unchanged.
The dedicated
`.github/workflows/tuflow-ordered-batch-contracts.yml` workflow installs
the pinned upstream PyTUFLOW PR #13 reference-count contract (`7fb9db0`, based on merged PRs #6–#10) for Windows-focused regressions and the public
161-TCF/174-run eager/deferred/batch differential replay. Any native SMB
benchmark requires the sole authorised SMB-capable agent; no project inputs
are uploaded, no solver runs, and passing synthetic contracts alone do not
establish materialised completeness.

### Deferred PyTUFLOW integration (development)

When installed PyTUFLOW implements the provisional API in
[phantom-branch/PyTuflow PR #1](https://github.com/phantom-branch/PyTuflow/pull/1),
the preflight calls `model.context(flags, file_resolution="deferred")` and reads
`input.file_references`, **never** `input.files`. Per-context path globbing and initial GIS
inspection can therefore be avoided by the upstream implementation. Context parsing still
has its original initial model-tree I/O; the checker still inspects the active BC database
for event-specific Source files. These operations are separately timed, not claimed to be
zero-I/O.

The checker aggregates file patterns across all runs, expands each wildcard filename
pattern by a bounded, nonrecursive directory listing, and validates distinct resolved
files using the existing shared inventory. A wildcard in a parent directory or a listing
exceeding the scan cap fails closed as unresolved. GeoPackage layer patterns (including
`>> all`) are checked through a read-only query per distinct GeoPackage. Wildcard scans
may require a separate directory scan before the ordinary exact-file inventory;
they are not yet guaranteed to share one scan.

The upstream supporting fork exposes `deferred_database_sources()`: it uses
cached, initially parsed hydraulic BC, AD BC, AD Global, materials, soils,
rainfall, pit/depth-discharge and cross-section database rows with
event/scenario translation. Their typed secondary path/layer references
are added to the single bulk inventory. Unknown database kinds, missing
cached source APIs, ambiguous declarations and unresolved rows still
**fail closed**, without eager `context.bc_dbase()` or `input.files`
calls in deferred mode. Numeric materials and supported inline soils
do not create fictitious filenames. Complete activation-scoped BC
association still requires validation.

**Eager completeness:** on the updated fork, `context.database_sources()` exposes the same typed
cached database sources in both eager and deferred contexts. The checker uses it when collecting
an eager baseline, and uses `input.file_references` to request the same active GIS curve-ID join.
`input.files` alone does not enumerate AD database sources or the files behind pit/depth-discharge
identifiers. Separate database entries retain their provenance even when they share a physical CSV.
Older eager parsers without this typed API remain unresolved for AD and curve database roles;
the checker does not silently certify those dependencies from primary-file existence alone.
Curve candidates are materialised only for identifiers referenced by active GIS features, while
database Source rows follow the existing conservative active-table inspection policy. This does
not claim hydraulic validation or select only time-series columns actually consumed by the engine.

**GIS attribute-linked files (new in PR #143):** after aggregating all contexts,
the checker scans each distinct file-valued **Shapefile or GeoPackage** source
once. The reader accesses only relevant DBF/SQLite attribute fields, using
**identical TUFLOW element-type rules for both formats**: `1d_nwk`, `1d_pit`,
`1d_xs`, `1d_tab`, `1d_xz`, `1d_bg`, `1d_lc`, `1d_cs`, `1d_hw` and
`1d_na`. Shapefiles use pyshp's attribute-only `iterRecords()` reader and
their single stem-named logical layer; GeoPackages use read-only SQLite and
support layer selectors. Neither format reads feature geometries.
Secondary file paths, including filename wildcards, enter one shared
file inventory and are checked for all affected simulations. The report
separately times this stage and counts distinct GIS-source opens.

**Channel and pit semantics:** In a `1d_nwk` layer, normal `Q` and
`P` values (and `1d_pit` `Q`/`VPI` identifiers) identify curves in the
`Depth Discharge Database` (network Q/P) or `Pit Inlet Database`
(pit features), rather than filenames in the GIS folder. The checker resolves those identifiers through
the documented `Name`/`Source` CSV fields and checks the referenced Source
files, caching each database by path. `QO` checks only the matrix filename
after its control ID; `M` checks matrix files. `1d_nwk` features with an
`Ignore` attribute of `Y` or `T` (case-insensitive) contribute no file
dependencies. Missing, duplicate or ambiguous curve identifiers, invalid
database schemas and blank required active identifiers fail closed.

**Limits:** MIF and other unsupported GIS formats, unknown TUFLOW
element types, missing/unsupported attribute schemas, mixed/unknown network
geometry and unexpanded GIS attribute variables remain **unresolved**, not
passed. Shapefile and GeoPackage field rules intentionally match for the
same element types, but the synthetic tests do not establish parity for all
real-world geometries, specialised model files or TUFLOW engine behaviour.
The original shared-attribute Windows sweep eliminated the 112 unsupported
Shapefile-format findings, but complete parity for supported database
types, model-specific curve activity and all GIS element schemas remains
an explicit merge gate. The original 440-run Higginsville
benchmark established *file/layer parity* before this feature was added, not
a passing attribute scan. Repeat the local benchmark against the new revision
and test the separate 42-run output-variable model before merging.

The normal `ryan-tools` package still depends on published `pytuflow>=1.2,<2`.
When that installation lacks `file_resolution`, the previous eager discovery remains
available with a warning. For integration validation, the dedicated `.github/workflows/deferred-pytuflow-consumer.yml`
pins CI-tested supporting PyTUFLOW PR #2 commit
`336efdea7f9c9d8ca27515284b042f48804f2bac`
on Windows/Python 3.13, sets `RYAN_TEST_DEFERRED_PYTUFLOW=1` and checks
typed sources, shared GIS/raster inspection and the full read-only
example-model diagnostics. Do not replace the production PyPI dependency
with an unreleased GitHub branch.

The checker separately reports shared GIS attribute and unique raster-metadata
opens/timings. The raster header phase uses read-only Rasterio/GDAL inspection
after context construction, never pixel-array reads. Unreadable or invalid
raster headers are unresolved rather than considered passing.

The checker reports separate timings for initial parse, context/reference collection
and the shared inventory. Repeat the representative 440-run network-drive test with
the pinned fork, compare dependencies against eager resolution, and measure I/O
operations and elapsed time before merging. Avoid monkeypatching upstream internals
or caching whole GIS/raster datasets.

The checker intentionally leaves model data untouched and writes only the report CSV. If an input is unresolved or
a model context cannot be parsed, the run **fails preflight** rather than being silently considered valid.

## Verification scope and limitations

- Full context resolution includes the supplied -e1..-e9 and -s1..-s9 switches and nested control files.
- Missing event-specific boundary source CSVs are checked from the resolved BC database Source column.
- GIS databases are checked for existence; when enabled, a referenced GeoPackage layer name must be present.
- Supported Shapefile and GeoPackage GIS attributes share one TUFLOW element classifier and are scanned once per distinct source/layer across simulations; unsupported cases fail closed.
- Missing and unresolved dependencies have distinct issue statuses.
- File wildcard/variable references that cannot be expanded are reported as unresolved.
- Only files referenced by PyTUFLOW's recognised input commands and BC Source entries are covered. Input files
  referenced by specialised commands or unsupported data formats may need additional handlers.
- An empty or invalid run list is rejected. Output directories and files which the engine will create are not required.
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
