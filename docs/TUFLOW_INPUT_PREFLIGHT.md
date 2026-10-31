# TUFLOW proposed-run input preflight

The read-only input check uses the public PyTUFLOW batch validator from the
[accepted PR #17 source](https://github.com/Chain-Frost/PyTuflow/commit/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b).
It validates the **actual planned runs**, not all possible combinations.
Input validation is not TUFLOW execution and never modifies model inputs.

## Installation and compatibility

The supported temporary PyTUFLOW requirement is maintained in
[`repo-scripts/pytuflow-requirement.txt`](../repo-scripts/pytuflow-requirement.txt).
The source revision is immutable until a separately accepted official release
provides the required public API. Public package metadata retains the publishable
version range `pytuflow>=1.2,<2`; a matching version number alone does **not**
prove compatibility. Another PyTUFLOW 1.2.7 build lacks the batch validation API.

For Windows, with the user's normal Python 3.14 installation and a built local
`ryan-tools` wheel in `dist/`:

```powershell
py -3.14 repo-scripts\install_latest_wheel.py --dry-run
py -3.14 repo-scripts\install_latest_wheel.py
```

The normal installer preserves the GIS binary-wheel index and Fiona/Rasterio/GDAL
bootstrap, installs the exact accepted parser with the local wheel, runs
`pip check` and verifies public capabilities. The same-version wrong parser is
reinstalled from the immutable source rather than assumed compatible.

Development installation can use:

```powershell
py -3.14 -m pip install -r requirements.txt
```

The requirements file includes the same pinned PyTUFLOW source. An existing PyPI
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

## Supported checks

`check_simulation_inputs()` is the single physical-validation consumer.
PyTUFLOW checks referenced input files, filename wildcards, selected GeoPackage
layers, recognised GIS attribute-linked sources and named boundary-condition
CSV column selectors. The application explicitly enables
`ValidationOptions(check_bc_csv_headers=True)` when supplying resource limits.
The checks are bounded by upstream directory and I/O controls and operate
without a QGIS application or TUFLOW engine process.

**This release does not inspect raster headers, projections, pixel data or raster
validity.** Required raster files still undergo the ordinary file availability
checks. Rasterio remains installed because other maintained features use it.
Header validation will return only after it is supported by a public PyTUFLOW
option in a separately reviewed change.

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

The BAT/text planner and generated parameter plan own every immutable run's
original order, effective TCF, executable, launch directory, model arguments and
source file/line. In a multi-TCF input, the orchestrator groups runs by effective
TCF and working directory. The validator receives structured argument mappings
rather than flattened display strings (which can lose values containing spaces).
PyTUFLOW's zero-based finding indices are restored **once** to the supplied
global planned-run indices; repeated and interleaved planned runs are retained.

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
[`tuflow-batch-smoke.yml`](../.github/workflows/tuflow-batch-smoke.yml)
installs the exact accepted PR17 archive and exercises real public
file, selected-layer, BC CSV, GIS-linked-source and mapping checks, plus
same-version incompatibility, broken transitive imports, shadowed packages,
incomplete group results, lazy imports and installer dry-run commands.

This is not a replacement for final installed-wheel and copied-wrapper acceptance
on the maintained Windows workstation. The
[ongoing integration record](work/2026-10-23-tuflow-runner-model-checks.md)
tracks that remaining validation and coordination with the runner/plan scopes.
