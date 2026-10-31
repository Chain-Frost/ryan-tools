# TUFLOW simulation input preflight

The read-only preflight validates **proposed runs**, not just the literal contents of a TCF. It uses PyTUFLOW to
resolve each proposed combination of events and scenarios, recursively finds active input commands, and checks their
referenced files. It also reads the active BC database and checks its event-resolved Source CSV paths.

**No TUFLOW simulation is executed.** This is not an equivalent of the engine's full initialisation or validation.
It does not verify GIS geometry, hydrology, CSV time-series columns/content, the existence of all files referenced
inside every possible GIS attribute, or TUFLOW engine-specific syntax unknown to PyTUFLOW. A passing check does not
guarantee a successful hydraulic simulation.

## Install

Use your normal Python 3.14 installation and the supported installer or repository
setup in the [environment guide](ENVIRONMENTS.md). That setup installs the binary
GIS prerequisites before the package. A plain PyPI-only install on an empty
Windows environment is a different dependency-installation check from the
isolated fork integration workflow.

PyTUFLOW is a standard `ryan-tools` dependency (`pytuflow>=1.2,<2`) and installs
automatically; no separate extra is needed. Copied wrappers require the installed
package. Do not insert checkout paths into sys.path.

The launcher retains `context_mode="individual"`. The standalone checker defaults to the
established individual-context checker on the published dependency. `--files-only`
explicitly selects the PR C batch file-existence workflow below, which requires a
matching development installation. If the
installed parser lacks the deferred API, the checker uses eager discovery with a
warning. A compatible development fork enables individual deferred collection and
its typed secondary-source APIs; installing the normal package does not install
that fork. Explicit batch collection is described below. Parser capability limits
remain relevant to the dependencies that can be checked.

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

The launcher freezes independent, read-only flag mappings from the final ordered,
deduplicated simulation list after command export. `preflight_check=False` remains
the default. Enabling it with `--preflight` starts one background advisory check;
the scheduler begins launching without waiting for parsing or checking. Select
`preflight_context_mode="individual"` (default) or explicitly `"batch"` in settings,
or pass `--preflight-context-mode=batch`. Unsupported batch capability warns
accurately and never silently falls back or blocks the simulation queue.

    python run_tuflow_simulations.py --preflight --no-pause
    python run_tuflow_simulations.py --preflight --preflight-context-mode=batch --no-pause
    python run_tuflow_simulations.py --preflight-only --no-pause

Every missing/unresolved finding is logged with run, flags and source provenance.
The main thread drains feedback during scheduling, so the worker does not touch
the Rich dashboard or scheduler state. Complete successful validation and report
writing produce one INFO confirmation. Checker exceptions, partial results and
report failures warn; library reports retain their truthful validity semantics.
These findings never cancel, postpone, remove or reassign simulations.

At normal completion, interruption or launch failure, the runner drains finished
feedback without joining a blocked checker. Pending work gets an explicit
incomplete/cancellation warning; the daemon worker suppresses late feedback.
An in-flight parser, report write or SMB call cannot be forcibly cancelled and
may continue until it returns or the runner process exits. A complete CSV is not
guaranteed in that case. A chained post-run script does not wait for checking.
This policy avoids an implicit unbounded shutdown wait; it is not a parser-speed
or physical-I/O improvement claim.

`--preflight-only` takes precedence and never starts the scheduler or post-run
script. It checks synchronously, reports every finding, and fails with a meaningful
nonzero exit for failed/incomplete validation. With `run_simulations=False`,
enabled checking uses the same synchronous diagnostic path; otherwise the runner
only exports commands. Existing parameter validation still requires the configured
TCF/executable paths and valid priority/GPU settings for this runner diagnostic.
Use the standalone checker below when executable-free diagnostics are required.

## Standalone checker

Copy the maintained check_tuflow_inputs.py wrapper into the project and edit DEFAULT_TCF and DEFAULT_COMMANDS.
It uses the exact run combinations in the exported commands file. Alternatively pass CLI paths:

    python check_tuflow_inputs.py --tcf "runs\MODEL_01_~s1~_~e1~_~e2~.tcf" --runs run_tuflow_simulations_commands.txt --report input_preflight.csv --no-pause

The CSV lists one missing/unresolved dependency per affected run. A single missing BC source may therefore appear
for multiple runs. The console reports total proposed runs, unique file paths, issues and affected runs.

For large batches, the checker first collects the parser's active dependencies for every requested run. It then deduplicates required
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
[curated PyTUFLOW PR C](https://github.com/Chain-Frost/PyTuflow/pull/50)
to collect immutable per-run source references for a whole proposed batch.
The common preflight then performs the *same* wildcard, Shapefile/GeoPackage,
secondary database source, raster metadata and file-existence checks.
The `check_simulation_inputs` API and simulation launcher retain
`context_mode="individual"`. The standalone checker now uses the separate
file-existence batch entry point described below.

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

The opt-in batch mode calls `TCF.collect_file_dependencies()` once for the full ordered run plan. It refuses an unavailable/incomplete fork API and
ambiguous stateful database activations; it does **not** silently fall
back or treat partial references as valid. No TUFLOW process launches.
The supported repository regression replay is `python -m
tests.fixtures.tuflow_example_repro.run_checks --batch`; see the
[harness guide](../tests/fixtures/tuflow_example_repro/README.md). It is not a shipped wrapper. Windows CI pins the
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

The [PyTUFLOW PR C](https://github.com/Chain-Frost/PyTuflow/pull/50)
batch API adds `ordered_inputs`, which describes **every** active command,
including output settings, variable assignments and database selections with
no file reference. The batch adapter now reproduces that exact command
sequence using each command's UUID, ordinal, owning control file and line.
It refuses duplicate/missing ordinals, orphaned file references and mismatched
provenance instead of silently accepting an incomplete inventory.
Each ordered command must also supply the independent `reference_count` contract;
the adapter checks that its owned projection contains exactly that many references.
Absent counts or coordinated omissions from both flattened reference tuples fail
closed, while genuine zero-reference settings remain valid.

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
the pinned upstream PyTUFLOW PR #13 reference-count contract (`d4a47d1`, based on merged PRs #6–#10) for Windows-focused regressions and the public
161-TCF/174-run eager/deferred/batch differential replay. Native benchmarking is
separate follow-up work on the designated workstation. The owner has offered a
new model on a different SMB server; record its exact parser/consumer source pair
and workload rather than applying earlier model timings to it. No project inputs
are uploaded and no solver runs. Passing synthetic contracts alone does not
establish materialised completeness for a new model.

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
do not create fictitious filenames. The supported multiple-BC declaration path
uses the ordered activation binding described above; untested model-specific
ordering and unsupported schemas are not claimed as generally supported.

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
The strict public archive replay now covers all 161 TCFs / 174 runs with the
workflow's pinned source pair. Earlier 440-run, M480 and N42 measurements remain
evidence for their recorded revisions. The owner withdrew the separate 42-run
model as a merge gate and deferred the native two-BC investigation. Broader model
support and the new SMB workload belong in separate follow-ups; they do not
silently extend this opt-in release's capability claims.

The normal `ryan-tools` package still depends on published `pytuflow>=1.2,<2`.
When that installation lacks `file_resolution`, the previous eager discovery remains
available with a warning. For integration validation, the dedicated
[deferred consumer workflow](../.github/workflows/deferred-pytuflow-consumer.yml)
pins a development parser independently, uses Windows/Python 3.14 and
`RYAN_TEST_DEFERRED_PYTUFLOW=1`, and checks typed sources, shared GIS/raster
inspection and the full read-only archive replay. The
[ordered batch workflow](../.github/workflows/tuflow-ordered-batch-contracts.yml)
checks the independent reference-count contract separately. These workflow pins
are test dependencies; they do not replace the published package requirement.

The checker separately reports shared GIS attribute and unique raster-metadata
opens/timings. The raster header phase uses read-only Rasterio/GDAL inspection
after context construction, never pixel-array reads. Unreadable or invalid
raster headers are unresolved rather than considered passing.

The checker reports separate timings for initial parse, context/reference collection
and the shared inventory. Use the existing private-model harness for the new SMB
workload's eager/individual-deferred/batch comparison, after fixing both source
revisions and passing representative correctness checks. It does not expose a
same-revision graph-on/graph-off switch; that diagnostic extension and further
parser optimisations are separate follow-ups. Measure complete, matching work
before making a performance claim. Avoid monkeypatching upstream internals or
caching whole GIS/raster datasets.

The checker intentionally leaves model data untouched and writes only the report CSV. If an input is unresolved or
a model context cannot be parsed, the run **fails preflight** rather than being silently considered valid.

## Verification scope and limitations

- The checker passes the supplied -e1..-e9 and -s1..-s9 switches to PyTUFLOW and traverses its active nested control files; interpretation depends on the installed parser's supported semantics.
- Missing event-specific boundary source CSVs are checked from the resolved BC database Source column.
- GIS databases are checked for existence; when enabled, a referenced GeoPackage layer name must be present.
- Supported Shapefile and GeoPackage GIS attributes share one TUFLOW element classifier and are scanned once per distinct source/layer across simulations; unsupported cases fail closed.
- Missing and unresolved dependencies have distinct issue statuses.
- File wildcard/variable references that cannot be expanded are reported as unresolved.
- Only files referenced by PyTUFLOW's recognised input commands and BC Source entries are covered. Input files
  referenced by specialised commands or unsupported data formats may need additional handlers.
- An empty or invalid run list is rejected. Output directories and files which the engine will create are not required.
- The preflight reports cannot substitute for running TUFLOW with -x to check full input validity.
- Validate project-specific variables and unsupported input types on the relevant Windows model before relying on their coverage. Preflight remains opt-in; a future default-on change needs a separate decision.

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


## Archived streaming I/O diagnostic (not supported by PR C)

The [streaming I/O overlap work record](work/2026-10-25-tuflow-streaming-io-probe.md)
is retained as historical evidence. Its earlier per-run producer callback is
**not provided by curated PyTuflow PR C**. The callback consumer, tests and
workflow have been removed. There is no supported streaming-I/O or graph
on/off option in the active preflight interface; no old pilot measurements
are accepted as PR C throughput evidence.


## Batch file existence and immutable dependency reuse

The standalone `check_tuflow_inputs.py` defaults to the established
individual-context checker, compatible with the declared PyPI requirement.
Pass `--files-only` explicitly to opt in to PR C file-existence and BC CSV-header
checks, using four bounded I/O workers by default. This mode requires the
matching development PyTuflow build with `TCF.collect_file_dependencies()`;
it fails closed if unavailable, rather than silently changing checking modes.
Use `--io-workers 1` to select serial I/O for that opt-in mode.
`--no-check-gpkg-layers` remains an explicit choice; layer existence is checked by default.

The library entry point is:

```python
from pathlib import Path
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_batch_files

report = check_batch_files(
    Path("runs/model.tcf"),
    [RunRequest(1, {"s1": "EXG", "e1": "Q100"})],
    io_workers=4,
)
```

The parser evaluates reusable control-file sections by effective variable values
and branch decisions, including event/scenario slots, TEF replacements and
transitive Set Variable values. Cached fragments contain immutable dependency
records, never shared mutable run-state instances. TBC means boundary control
file; `bc_dbase` means the separate boundary-condition source database.

The consumer checks unique file paths and requested GeoPackage layers. It retains
essential GIS attribute-linked files and BC source discovery; those source reads
are required to discover dependencies, not to validate time-series/raster data.
Raster metadata validation is omitted. Existing conservative source-classification
errors still prevent a successful result. Linked-GIS discovery optimisation is
explicitly deferred; the existing once-per-source discovery is retained.

Dense directories use one bounded listing; sparse files may overlap. At most
`io_workers` tasks are outstanding (1-32 supported), and each SQLite connection
belongs to one worker and is closed there. Finished jobs immediately free capacity for the next check; a slow job does not
block the other workers. Results are assembled in deterministic order and routed back to every affected simulation. Network operations retain OS
I/O timeouts; thread count does not impose an operation deadline.

Reports expose section evaluation/reuse counts and ordinary-evaluator fallback
counts. Unsupported parser constructs retain the established batch evaluator and
are reported as fallbacks, never omitted. All caches end with the invocation.
The recorded 480-run comparison measured a 54.31% median reduction for fragment
reuse (two trials per condition). This is workload-specific evidence, not a general
speed guarantee or a measurement of the subsequent scheduler change.
See the [implementation record](work/2026-10-25-batch-file-fragments.md).

File counts describe unique paths, not rainfall selections. Multiple runs may
select different columns from the same BC Source CSV. The retained 480-run
workload has 480 source/column combinations in 24 rainfall files, all represented
in its 50-path inventory. This workflow checks the source files' existence,
and the required CSV column names, without validating rainfall values.

### Required BC CSV headers in rapid checks

`check_batch_files` and the wrapper's explicit `--files-only` mode verify the resolved
Column 1 (time) and Column 2 (value) names for ordinary BC-source CSV files.
PyTUFLOW expands TEF and Set Variable selectors and carries immutable column
requirements alongside each source path. TBC remains the boundary control file;
`bc_dbase` supplies these source/column selections.

The bounded I/O workers open each distinct existing source CSV once per invocation,
inspect at most five physical lines, including comments/preambles, then reuse
the first CSV record containing any requested column name across all
runs. They never load the rainfall series or convert/validate numeric values. A changed header is read again on the
next invocation. Matching trims surrounding whitespace and ignores case; missing
or ambiguous duplicate names produce run-specific unresolved issues. Missing
files retain the normal missing-file diagnostics.

UTF-8 with optional BOM, leading `!` comments, quoted fields and multiline headers
are supported. Header discovery uses the union of requested column names for
that file. It stops at the first record containing any requested name, even when
other required names are missing. Preambles need no fixed label or field count. Inspection is limited to 1 MiB including leading comments. Blank
selectors, FEWS CSV sources, unreadable/malformed headers and unavailable parser
column metadata fail closed. If no candidate header is found within five physical lines, the result is
unresolved. The limit includes blank/comment lines and every physical line of a
multiline record; line six is never read. When a header cannot be recognised,
only that bounded prefix is examined, not the remaining time series.
Non-CSV formats and inline constants retain their existing handling; this does not
claim schema validation of TS1, FEWS XML or rainfall values.

The lower-level `check_simulation_inputs` API retains its default behaviour;
`check_bc_headers=True` explicitly enables these checks there. Rapid mode always
enables them. Reports expose `csv_headers_checked` and
`csv_column_selections_checked`; the wrapper prints both counts.

## Consolidated checker and runner development

PR #159 owns the integrated work previously reviewed in #158, #160 and #161. The standalone checker retains stateful multi-TCF planning in default individual mode. Explicit `--files-only` uses the matching unpublished PR C batch capability per TCF/launch-directory group, with `--io-workers` controlling bounded unique-file I/O. `--plan-only` performs no dependency checks. CSV-header inspection and fragment-reuse counts sum group work; no cross-TCF cache is implied. See the [current integration record](work/2026-10-23-tuflow-runner-model-checks.md) for release gates and upcoming activity integration.
