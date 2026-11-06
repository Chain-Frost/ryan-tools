# TUFLOW proposed-run input preflight

The read-only input check uses the public PyTUFLOW batch validator from the
[pinned PyTUFLOW PR #63 source](https://github.com/Chain-Frost/PyTuflow/pull/63)
(commit [`b1026d0cf721`](https://github.com/Chain-Frost/PyTuflow/commit/5360c979a4da577c7eae04a2afded00eb21d5bf9)).
It validates the **actual planned runs**, not all possible combinations.
Input validation is not TUFLOW execution and never modifies model inputs.

## Installation and compatibility

The supported temporary PyTUFLOW requirement is maintained in
[`repo-scripts/pytuflow-requirement.txt`](../repo-scripts/pytuflow-requirement.txt).
The selected source is a fixed contributor-fork commit, not an upstream release.
It includes the public API from upstream PR #17 plus the validation fixes in fork PR #63.
The pin also fixes implicit generic and machine-specific TCF override parsing,
so the runner can discover redirected log folders through the public context API.
It remains under review and requires native acceptance before release. Public package metadata retains the publishable
version range `pytuflow>=1.2,<2`; a matching version number alone does **not**
prove compatibility. Another PyTUFLOW 1.2.7 build lacks the batch validation API.

For normal Windows installation, run `install-ryan-tools.bat`. It uses the
selected normal Python installation, installs the published package together
with the accepted PyTUFLOW source, and checks the result. No separate parser
installation or virtual environment is required. To preview the same PyPI route:

```powershell
py -3.14 repo-scripts\install_from_pypi.py --dry-run
```

For a built local `ryan-tools` wheel in `dist/`:

```powershell
py -3.14 repo-scripts\install_latest_wheel.py --dry-run
py -3.14 repo-scripts\install_latest_wheel.py
```

The normal installer preserves the GIS binary-wheel index and Fiona/Rasterio/GDAL
bootstrap. Both normal routes explicitly reinstall only the accepted parser,
then resolve it with the requested package and all dependencies, run `pip check`
and verify public capabilities. The parser-specific reinstall replaces an
incompatible build even when its version matches; the GIS dependencies are not
subject to forced reinstall.
Any installation or verification failure stops before reporting success.

Development installation can use:

```powershell
py -3.14 -m pip install --force-reinstall --no-deps -r repo-scripts\pytuflow-requirement.txt
py -3.14 -m pip install -r requirements.txt
py -3.14 -m pip check
```

The requirements file includes the same pinned PyTUFLOW source and resolves its
dependencies. The preceding parser-only replacement is necessary because pip
can retain an existing incompatible build when its version number matches.
Run the capability diagnostic below after installation. An existing PyPI
installation without the public API is not supported for input checking.
`--dependencies-only` installs GIS dependencies without a local wheel or
validation verification; `--force-reinstall` reinstalls only the wheel with
`--no-deps` and does not verify or alter the parser. A `--dry-run` performs
neither installation nor capability verification.

Programmatic capability diagnosis (no model parse, installation or network I/O):

```python
from ryan_library.functions.tuflow.pytuflow_compatibility import (
    check_validation_capabilities,
    require_validation_api,
)

print(check_validation_capabilities().message)
api = require_validation_api()  # Raises with an installation remedy if unavailable
```

Diagnostics include the selected interpreter, installed distribution version,
module location, available direct-url source provenance and missing capability or
transitive dependency. A local checkout shadowing the installed package is
diagnosed. The source metadata is advisory when not recorded; the public API and
actual validation result shape still need to pass.

## BAT/text input and multiple TCFs

The checker and Windows runner use the same static command reader. Both capture
the inherited environment; every input file then has its own SET, SETLOCAL and
directory state. Repeated `--runs` on the checker, or `INPUT_FILES` in the runner,
combine independent sources in their supplied order, discarding repeated equivalent TCF/flag selections and retaining their first source location.

For example, a text file may contain:

```bat
REM This row is a comment, not a simulation.
set "TCF_05=runs\model_05_~e1~.tcf"
TUFLOW.exe -e1 A "%TCF_05%"
TUFLOW.exe -e1 B "%TCF_05%"
set "TCF_06=runs\model_06_~e1~.tcf"
TUFLOW.exe -e1 A "%TCF_06%"
```

The first two runs use `TCF_05`; the third uses `TCF_06`. A bare row such as
`-e1 A` requires a current `SET TCF` value or an explicit Python TCF fallback.
Set the Python TCF to `None` for multiple imported models. When Python supplies
a TCF, every active imported TCF must match it; a mismatch reports the paths and
source location before validation, export, CSV history changes or execution.
TCF consistency checks compare the effective paths without querying network
drive mappings. Each versioned runner has its own adjacent CSV, and Resume
uses the TCF filename and effective flags rather than the complete path.

| Input feature | Supported behavior |
| --- | --- |
| Comments | REM/@REM and full-line `::`; text command lists also allow full-line `#`, `;` and `//`. |
| Variables | Quoted or plain SET, reassignment/unset, case-insensitive `%NAME%`, `%CD%` and `%~dp0`; undefined variables fail. Literal `!` characters are removed and reported because delayed expansion is not evaluated. |
| Directories | CD/CHDIR with optional `/D`, local/mapped PUSHD/POPD, and per-run START `/D`; paths retain their intended base. |
| Run rows | Direct TUFLOW invocations, supported START forms and bare numbered event/scenario rows. |
| Arguments | Numbered event/scenario pairs, supported engine switches, individual `-puN` tokens and numeric `-etN` switches. |
| Text | Quoted values and paths, UTF-8/BOM by default, explicit encoding override, and supported terminal-caret continuation. |
| Boundaries | No shell execution, IF/FOR/CALL, general GOTO, pipelines, redirection or delayed expansion. Every literal `!` is ignored with its source line recorded; the checker and runner warn that planned runs and outputs may be wrong. Other unsupported active syntax fails the complete plan. |

The standalone reader and checker select batch syntax for `.bat` and `.cmd`,
and command-list syntax for `.txt`. The Python runner reads all three extensions
as inert command lists, allowing UNC working directories and UNC directory
declarations without requiring CMD drive mapping. The reader does not execute
commands. It preserves each run's source
file/line, effective TCF, executable, working directory and model flags.

The Python runner treats every BAT/TXT file as **inert data**. It never runs
the source file or any source START, CD or executable command. Source SET and
directory declarations are interpreted only to resolve model selections and
TCF references. The Python wrapper exclusively supplies the solver executable,
child working directory, `BATCH_COMMANDS`, GPU groups, concurrency and
priority. Source executable, batch engine and GPU settings are ignored. The
checker preserves source path interpretation for reporting.
Generated and imported paths use the same lexical normalization for plan
comparison, so parent-relative spellings such as `..\runs\model.tcf` do not
create duplicate runs or fail TXT export. This does not query drive mappings
or dereference links.
The [wrapper guide](../ryan-scripts/TUFLOW-python/README.md) describes the fixed
TXT export and single-CSV Resume workflow.

The standalone checker has a visible, editable settings section. `RUN_FILES`
selects its BAT/TXT inputs, `TCF = None` permits multiple models, and
`WORKING_DIR` controls the base for imported commands **and relative
`RUN_FILES`/`--runs` filenames** (regardless of the invoking terminal's
cwd); absolute command-file paths remain unchanged. The shipped input is
`run_tuflow_simulations.txt` beside the checker; point `RUN_FILES` at a
version-specific TXT when applicable. No `DEFAULT_` prefix is required.

It prints a structured run plan, control-file summary, input-check totals,
timing, numbered issue details and a final pass/fail result in the terminal by
default. Paths inside the working directory are shown relative to that directory;
paths outside it remain absolute, and the optional JSON manifest retains absolute
paths:

Each issue identifies its TCF, event/scenario flags and run number. `Source`
locates the imported command-list row; `Control file` identifies the owning
model control file and physical line number when supplied by the validator,
and `Command` shows the complete original model command. CSV-header details include the requested column
names; BC database lookup errors retain the identifier that could not be resolved.

The reported script wall time starts before ryan-tools and PyTUFLOW are imported
and ends when the input check is complete. The separately listed parsing,
context and inventory values are internal input-check phases.

```powershell
py -3.14 check_tuflow_inputs.py --runs runs.txt --working-directory "T:\project" --no-pause
```

All supported checks run together, including selected GeoPackage layers and
boundary CSV headers. There is no separate layer toggle in the standalone
wrapper. Set `REPORT_CSV = Path("issues.csv")` or supply `--report issues.csv`
for a new CSV; existing reports are not overwritten.
`--plan-only` checks the static plan without inspecting model inputs. An explicit
`--plan-report new_plan.json` creates a new diagnostic manifest; Resume does not
require or use that manifest. If imported source text contains `!`, the console
shows a prominent warning for every affected source line and the JSON manifest
records the same warnings. The marks are removed before parsing, so the warning
states that planned runs and input-check outputs may be wrong.

## Supported checks

`check_simulation_inputs()` is the single physical-validation consumer.
PyTUFLOW checks referenced input files, filename wildcards, selected GeoPackage
layers, recognised GIS attribute-linked sources and named boundary-condition
CSV column selectors. The application explicitly enables
`ValidationOptions(check_bc_csv_headers=True)` when supplying resource limits.
The checks are bounded by upstream directory and I/O controls and operate
without a QGIS application or TUFLOW engine process.

The runner's `CHECK_INPUTS` setting enables or disables the complete advisory
check, including selected GeoPackage layers. There is no separate layer toggle
in the runner. Explicit preflight-only operation waits for those same checks
and launches no solver processes.

The pinned PyTUFLOW validator now inspects **selected raster headers by default**
without reading pixels, and compares selected GIS/raster CRS metadata against
the TCF projection where available. Missing or conflicting CRS metadata is
reported as a **non-failing advisory warning**. Invalid raster headers and
missing required inputs remain blocking issues. These metadata checks are
read-only and can add I/O cost for large or network-hosted models.

A finding retains the upstream source command, path, selected layer, owning
control file, database context, type, detail and original planned run identity.
Statuses are distinct:

- **missing:** a required file, wildcard match, layer or CSV selector is absent.
- **unresolved:** the parser or inspector cannot establish the requirement
  conclusively, including unreadable sources and failed run contexts.
- **unsupported:** the input feature/schema cannot safely be validated.

A passing preflight is an input-existence/selection check, not a guarantee of
model correctness or compatibility with a particular solver.

## Application boundary

The BAT/text planner and generated parameter plan retain the first occurrence
of each effective TCF/event/scenario/retained-model-option selection, with source
file/line provenance. The checker can group the parsed source context by TCF and
working directory. In the Python runner, executable and child working directory
are supplied exclusively by Python settings. The validator receives structured argument mappings
rather than flattened display strings (which can lose values containing spaces).
PyTUFLOW's zero-based finding indices are restored **once** to the supplied
global planned-run indices; interleaved distinct selections and sparse Resume
indices are retained, while duplicates are discarded.

```python
from pathlib import Path
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs

runs = [
    RunRequest(1, {"s1": "Existing", "e1": "Q100"}),
    RunRequest(2, {"s1": "Future Works", "e1": "Q100"}),
]
report = check_simulation_inputs(Path("runs/model.tcf"), runs)

if not report.passed:
    for issue in report.issues:
        print(issue.run_index, issue.status, issue.origin, issue.reference, issue.detail)
```

The group/plan orchestrator owns source-line attachment and per-group exceptions.
Its findings compose the original `PreflightIssue` with the `PlannedRun`, so the
full upstream detail is retained alongside command provenance. Original run
indices may be sparse after Resume selects pending work; they remain unique
positive identifiers and are never renumbered to satisfy the checker.
If a group fails to return complete findings, the affected runs must be reported
as failed/unresolved, never counted as passing. Bad command syntax, conflicting
configured Python TCF and other plan/configuration errors are **fatal** before
checks, exports or solver launch, rather than advisory input findings.

The standalone checker and explicit preflight-only runner mode fail with a
nonzero exit if validation is unavailable or incomplete. Normal runner preflight
is advisory: a failed check must be visible but must not block ordinary
simulation scheduling. Help, plan-only, command-export and checking-disabled
modes do not import or use PyTUFLOW's validation stack.

## Regression coverage

The dedicated Windows Python 3.14
[`TUFLOW input validation`](../.github/workflows/tuflow-input-validation.yml)
workflow is retained because it checks the immutable accepted PyTUFLOW source,
deliberately rejects an incompatible same-version build, and replays the entire
public example archive. The standalone command-plan and CSV unit tests already
run in the main Hosted tests; the separate scope-specific workflows are retired.
This workflow installs the exact pinned PR63 archive and exercises real public
file, selected-layer, BC CSV, GIS-linked-source and mapping checks, plus
same-version incompatibility, broken transitive imports, shadowed packages,
incomplete group results, lazy imports and installer dry-run commands.

The same workflow also runs
[`test_example_models_real.py`](../tests/orchestrators/tuflow/test_example_models_real.py)
against the checksum-verified permanent example archive in `tests/test_data`.
It reads all 21 simulation BAT files as text, retaining SET values and event/scenario
flags through the production command planner. Repeated aggregate selections are
deduplicated for this coverage sweep, retaining their first source location.
TCFs absent from the batches receive a no-flag context. Coverage must match the
fixture's per-TCF baseline: **161 TCFs / 174 unique run combinations**.
The separate empty-GIS creation BAT is excluded. The two aggregate BATs' fixed
executable-existence guards are stripped only by the fixture reader, preserving
source line numbers; this does not add general IF/EXIT support to the planner.

Run locally on Windows Python 3.14 with the accepted parser and native GIS dependencies:

```powershell
git submodule update --init tests/test_data
$env:TUFLOW_EXAMPLE_REPORT_DIR = Join-Path $env:TEMP ("tuflow-examples-" + [guid]::NewGuid())
python -m pytest -q --tb=short tests/orchestrators/tuflow/test_example_models_real.py
```

Use a new report directory for each invocation; issue CSVs never overwrite existing
files. Without the environment setting, reports use pytest's temporary directory.
The native replay is marked `gdal` and runs explicitly in the dedicated workflow;
the ordinary hosted suite excludes it while retaining the BAT inventory check.
The 161-TCF regression now requires all **161 TCFs and 174 selected run
contexts to pass**. The previous allowance for seven 1D boundary-schema errors
and one rainfall-triangle Name error was removed with the fix consolidated in
[PyTUFLOW PR #63](https://github.com/Chain-Frost/PyTuflow/pull/63).
[ryan-tools #181](https://github.com/Chain-Frost/ryan-tools/issues/181)
tracks this correction. Advisory CRS warnings are included in the JSON results
but do not fail validation; blocking raster/header/BC issues do. The new pin
needs fresh Windows/GDAL and installed-wheel acceptance; earlier green tests
refer to the old parser. JSON results and per-model issue CSVs
are uploaded even on failure.
No BAT or TUFLOW executable is launched, and source inputs remain in the submodule;
only a verified temporary extraction is inspected. This covers dependency validation,
not solver execution, hydraulic correctness or raster-header checks.

This is not a replacement for final installed-wheel and copied-wrapper acceptance
on the maintained Windows workstation. The
[ongoing integration record](work/2026-10-23-tuflow-runner-model-checks.md)
tracks that remaining validation and coordination with the runner/plan scopes.
