# Development and execution environments

`ryan-tools` requires Python 3.14 or newer in the user's normal Python installation. Python 3.14 is the developer/CI baseline. Users are not expected to create, understand
or activate a virtual environment. Wrappers copied into project folders use the same installed `ryan-tools` package.

Some QGIS, OSGeo4W and GDAL workflows use their application's Python or command environment instead. An operation working
with the normal Python installation does not prove that it works inside those application environments, or conversely.

## Distribution migration

The distribution is now named `ryan-tools`; maintained Python imports remain `ryan_library`.
The PyPI and local-wheel installers automatically migrate an existing `ryan_functions` installation.
They validate the replacement first (verify the local wheel, or resolve the PyPI install with pip's `--dry-run`),
run any dependency bootstrap commands, then uninstall the old distribution immediately before reinstalling `ryan-tools`.
The force path retains `--no-deps`. No migration flag is needed in calling BAT files. Installer dry runs only print commands.
When using pip directly instead of these installers, uninstall the old distribution first:

```powershell
python -m pip uninstall ryan_functions
```

Do not uninstall the old distribution after installing the new one: that can remove files shared by both distributions.
The `ryan_functions` import alias is removed; replace those imports with `ryan_library.functions`.

## Install for repository development

From the repository root, install the checkout and development requirements into the user's Python 3.14 installation:

```powershell
py -3.14 -m pip install --upgrade pip
py -3.14 repo-scripts\install_latest_wheel.py --dependencies-only
py -3.14 -m pip install -r requirements.txt
```

`requirements.txt` installs the repository in editable mode with its development and MCP dependencies, plus
the current QGIS MCP Python bridge from upstream GitHub main. Python then imports the current checkout while developers and agents
edit `ryan_library`. The dependency-only command installs binary Fiona, Rasterio and GDAL packages from the
configured geospatial wheel index. It supersedes the former single-package batch installers and can be previewed with
`--dry-run`. For managed or offline environments, use the local approved source with
`repo-scripts/setup_codex_mcp.ps1 -Install -QgisSource <path>` instead of the GitHub-based requirements command.

For an end user, double-click [install-ryan-tools.bat](../install-ryan-tools.bat) (the existing
[install-latest-wheel.bat](../install-latest-wheel.bat) is an alias):

```powershell
.\install-ryan-tools.bat
```

This installs the current published `ryan-tools[mcp]` package from PyPI,
the upstream QGIS MCP Python bridge and the accepted PyTUFLOW PR17 archive in
[`repo-scripts/pytuflow-requirement.txt`](../repo-scripts/pytuflow-requirement.txt),
using Python 3.14 or later (automatic highest-installed 3.x selection, overrideable with `RYAN_TOOLS_PYTHON`).
Both normal installers bootstrap binary Fiona, Rasterio and GDAL first, then
resolve `ryan-tools` and the pinned PyTUFLOW source together. The explicit archive
replaces an incompatible official build even when its package version is the
same. Installation succeeds only after `pip check` and the public PyTUFLOW
capability guard pass; a failure stops before Codex registration. Dry runs print
these checks without claiming that they passed.
If Codex or its VS Code extension is installed,
the script safely configures its QGIS and ryan-tools MCP entries in the shared user-level Codex config. Without Codex,
it leaves Codex configuration untouched. Pass `--skip-codex` to disable automatic
registration, `--skip-qgis-mcp` for headless/offline installation, or
`--qgis-mcp-source <path>` to install the bridge from a local source.
After updating Codex configuration, restart Codex or reload the Codex extension in VS Code. Python 3.14 or newer is required for `ryan-tools`; `ryan-culverts` remains an independent dependency. The QGIS desktop plugin
must still be installed and started through QGIS Plugin Manager.

For developers working from the checkout, keep the local verified-wheel workflow:

```powershell
.\package_and_install.bat
.\install-local-wheel.bat
```

The first builds and installs the latest local wheel; the second installs an
existing wheel without rebuilding. These commands do not replace developer
editable installs (`pip install -r requirements.txt`).

## Disable Python bytecode for the Windows user

To opt out of `.pyc` writes across Python installations for your Windows user, run:

```powershell
.\disable-python-bytecode.bat
```

The standalone [disable-python-bytecode.bat](../disable-python-bytecode.bat) uses
`setx PYTHONDONTWRITEBYTECODE 1` without administrator rights.
It is not run automatically by the installer. Preview with `--dry-run`.
Restart terminals, editors and Codex to inherit the saved setting. When called from an existing
`cmd.exe` session, the batch file also sets the variable in that session; a parent PowerShell session
must be restarted or use `$env:PYTHONDONTWRITEBYTECODE = '1'` for immediate effect.
Existing `.pyc` files are retained. This controls ordinary Python import caching, not explicit compilation commands.

To undo the persistent setting in PowerShell, then restart applications:

```powershell
[Environment]::SetEnvironmentVariable('PYTHONDONTWRITEBYTECODE', $null, 'User')
Remove-Item Env:PYTHONDONTWRITEBYTECODE -ErrorAction SilentlyContinue
```

## VS Code interpreter and tasks

Select the user's normal Python 3.14 installation as the workspace interpreter. The checked-in settings do not activate
an environment in new terminals.

The repository tasks use `RYAN_TOOLS_PYTHON` when it contains an explicit interpreter path; otherwise they use
`py -3.14`. Available tasks include:

- `python:install-requirements`: installs `requirements.txt` into the selected user Python installation;
- `python:stdin`: sends standard input through `repo-scripts/run_snippet.py`;
- `qgis:python-current-file`: runs the active file using `C:\OSGeo4W\bin\python-qgis.bat`;
- `qgis:python-repl`: opens an OSGeo4W QGIS Python command prompt;
- `qgis:open-qgis`: launches QGIS through OSGeo4W.

Set `RYAN_TOOLS_SNIPPET` to use a different script with the `python:stdin` task. The integrated terminal defaults to
Windows PowerShell, and an `OSGeo4W Cmd` profile is available for workflows that need the OSGeo4W environment.

## Installed package and copied wrappers

Wrappers copied into project folders import the installed `ryan-tools` distribution. They do not add the repository
to `sys.path`, so the package must be installed into the same Python installation used to run the wrapper.

Maintained wrappers print both their embedded wrapper version and the installed library version. This helps distinguish
an older copied wrapper from an older installed package.

After changing `ryan_library` or package metadata, rebuild and install before validating a copied-wrapper workflow:

```powershell
.\package_and_install.bat
```

The underlying build command is:

```powershell
py -3.14 repo-scripts\build_library.py
```

The default build advances the calendar version. Use `--no-bump` to rebuild and verify the current version without
changing `pyproject.toml`, `--skip-pip` when the build dependency is already installed, or `--skip-artifacts` only in
environments that cannot create or retain wheel files. A candidate is built and verified in temporary storage before
it replaces the retained wheel in `dist`; failures restore package metadata and preserve the previous artifact. Inspect
Git status after every build.

The retained wheel can be checked independently with:

```powershell
py -3.14 repo-scripts\verify_wheel.py
```

## GitHub Actions CI

The enabled `.github/workflows/ci.yml` workflow runs on pull requests and pushes to `main` using Windows and Python
3.14, with a separate Python 3.14 MCP job (retaining the old required-check label until the GitHub ruleset is updated). Its independent jobs mean a policy failure does not hide
runtime or packaging results.

The hosted test job initialises only `qgis-resources` and
`tests/test_data`. The QGIS repository is used only as test/resource data; no QGIS application is launched. The job
installs the repository's binary GIS dependency bootstrap, sets `MPLBACKEND=Agg`, and runs:

```powershell
python -m pytest tests `
  --ignore=tests/functions/gdal `
  --ignore=tests/orchestrators/gdal `
  --ignore=tests/scripts/gdal/test_gdal_flood_extent.py `
  --ignore=tests/scripts/test_promoted_utilities.py `
  --ignore=tests/scripts/test_archive_compress.py `
  -m "not gdal and not gui and not external" `
  --deselect=tests/floodway/test_sweep.py::test_discharge_sweep_detects_overtopping_and_retains_non_aep_interior_states `
  --deselect=tests/floodway/test_sweep.py::test_discharge_sweep_honours_explicit_minimum_bound `
  -rs `
  --durations=15 `
  --junitxml=test-results/hosted-tests.xml
```

The ignored modules import `osgeo` directly or through production modules during collection, so marker deselection alone
cannot make them a valid normal-Python hosted boundary. Rasterio/Fiona tests, native ASC processing, wrapper dry runs,
mocked subprocess behavior remain in the hosted set. The real 7-Zip archive integration module is deliberately
excluded from hosted CI and remains an optional local check. Tests marked `external` require an explicitly supplied
application or executable; the ASC_to_ASC parity tests remain local and use `ASC_TO_ASC_EXE`.

Create `test-results` before running this command. The two temporary floodway deselections predate the fast-check work;
their review is tracked in the [exclusion record](work/2026-10-16-floodway-ci-exclusions.md).

The package job initialises `qgis-resources` and installs the two PyPI hydraulics dependencies, builds and verifies a fresh wheel,
performs a `--no-bump` rebuild, verifies the rebuilt artifact, confirms the declared version is unchanged, then
installs the wheel into a temporary target and runs the installed-wheel smoke test
outside the checkout with `--expected-root`. The verified wheel and hosted JUnit report are uploaded as workflow
artifacts.

Real `osgeo`/GDAL integration is deliberately not claimed by this initial workflow. Run those tests in a compatible
GDAL/QGIS/OSGeo4W environment until a reliable Python 3.14 hosted GDAL installation is established.

### Fast PR checks and measured runtime (17 October 2026)

[`fast-pr.yml`](../.github/workflows/fast-pr.yml) runs independently of the full
Windows [`ci.yml`](../.github/workflows/ci.yml) on pull requests and main pushes.
Its only installed development tool is the Ruff version already pinned in
`pyproject.toml`. It checks whitespace in the changed patch and lint/formatting
for changed first-party Python files, without checking out submodules or installing
GDAL/geospatial packages. Ruff and strict Pyright also remain in `Policy`, with the normal
Python 3.14/GIS dependencies, preserving the established release gate. Repository documentation and logging checks, Python
3.14 MCP smoke, hosted tests, and wheel verification remain separate.

The fast workflow does **not** gate or precede the full workflow: it provides an
earlier independent failure signal without adding to successful CI wall-clock
time. When changing Python files, both checks must pass.

Baseline from five recent GitHub Actions runs on 16 October 2026:

| Job | Measured duration | Main cost |
| --- | --- | --- |
| Hosted tests | 2:40–3:37 | Install dependencies 60–67 s; pytest 60–104 s |
| Policy | 1:30–2:06 | GIS package installation and static analysis |
| MCP smoke (legacy Python 3.13 check label) | 1:18–2:00 | Historical Python 3.13 run timings; Python 3.14 timings not yet measured |
| Package | 1:01–1:25 | Checkout, submodules, setup, and wheel prerequisites |

The existing `setup-python` pip cache reuses downloaded packages, not installed
site-packages, so dependency installations remain costly even when pip's cache
is warm. The new fast workflow needs only Ruff and produces earlier results without
waiting for a GIS install. The existing `Policy` job deliberately retains its
Ruff checks because release automation depends on the Python CI workflow's
success. Leave the actual Windows tests and GIS dependency bootstrap in place
until replacements are tested. Re-measure after several successful workflow runs before introducing
parallel pytest workers or a prebuilt GIS environment.

Verified PR #129 head `eaf4b0e` on 17 October 2026:
[Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/39001435683) and all four
[Python CI jobs](https://github.com/Chain-Frost/ryan-tools/actions/runs/39001435924) passed.
Runner logs span approximately 9 seconds for the fast job, 34 seconds for Policy and 146 seconds for hosted tests.
The fast job finished at 04:08:38 UTC, before Policy at 04:09:03 UTC and hosted tests at 04:10:56 UTC.
This YAML/Markdown-only PR skipped conditional Ruff installation, lint/format and Pyright execution in both workflows;
it validates early patch feedback, while a Python-changing PR is still needed to measure early Ruff feedback.

Hosted installation step boundaries show approximately 3 seconds for pip upgrade, 13 seconds for GIS bootstrap and
39 seconds for development/test dependencies. Pytest reported 1057 passed, 7 deselected and 63 warnings in 56.83 seconds,
with the slowest test call taking 9.46 seconds. These are one run's measurements, not an overall CI speedup claim.
Collect several Python-changing runs before choosing a setup optimization or test parallelism change: dependency setup
and test execution remain comparable costs. Delivery and follow-up are tracked in the
[fast-check work record](work/2026-10-17-fast-pr-checks-129.md).

## QGIS, OSGeo4W and GDAL

QGIS and OSGeo4W can supply different Python packages, GDAL binaries, drivers and environment variables from the normal
user Python installation. Use the application environment for PyQGIS code or workflows that require its specific GDAL
setup.

The dependency bootstrap above is for the normal Python 3.14 installation. It does not install packages into QGIS or
OSGeo4W and does not replace an application-environment smoke check.

Common entry points are:

```powershell
C:\OSGeo4W\bin\python-qgis.bat path\to\script.py
C:\OSGeo4W\bin\qgis.bat
```

Older scripts may initialise QGIS or OSGeo4W themselves. Review the script's expected installation path before running
it. Static analysis with the user Python installation is useful but is not an application-environment smoke test.

## Interactive and headless execution

Maintained wrappers support human-facing terminal or double-click use. They may show identity banners and pause once the
workflow has returned. Use `--no-pause` for automation, redirected input or agent-driven validation.

Where supported, begin with `--help` or `--dry-run`. Library functions and orchestrators are non-interactive and should
return or raise errors rather than pausing or calling `SystemExit`.

## Test and fixture environment

Initialise only the specific repository submodules required by the selected tests. For the normal maintained test and
build surfaces:

```powershell
git submodule update --init excel-resources qgis-resources tests/test_data
git lfs pull
```

`tests/test_data` is a required submodule. Resource submodules may also require their own Git LFS pull. Tests do not
automatically download unavailable proprietary data, and new committed fixtures should be synthetic unless sharing the
source data is permitted.

Never initialise, inspect or validate the `unsorted` submodule. Recursive submodule checkout is prohibited because it
would include that excluded repository.

Some focused pytest runs on Windows require a pre-created repository-local base temporary directory. Environment-specific
components, such as the PyPI-installed `run-hy8` package, may have additional import-path instructions in their local docs.

## Domain-joined Windows machines

On machines joined to `bge-resources.com` or the `BGER` domain, PowerShell may occasionally fail to stream file contents
reliably in agent terminals. When that occurs, use a command such as:

```powershell
cmd.exe /C type path\to\file
```

This is a terminal workaround, not a reason to change file encoding or rewrite the affected file.

## MCP server

The optional repository MCP server has separate installation and client configuration requirements. See
[`MCP_SETUP.md`](MCP_SETUP.md).
