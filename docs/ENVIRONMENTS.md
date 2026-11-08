# Development and execution environments

`ryan-tools` officially supports Python 3.14 or newer in the user's normal Python installation. Python 3.14 is the developer/CI baseline. The batch installers bypass pip's `Requires-Python` metadata for an explicitly selected older Python 3 interpreter, but compatibility is not guaranteed. Users are not expected to create, understand
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
py -3.14 repo-scripts\vendor_pytuflow_wheel.py
py -3.14 -m pip install --force-reinstall --no-deps -r repo-scripts\pytuflow-requirement.txt
py -3.14 -m pip install -r requirements.txt
py -3.14 -m pip check
py -3.14 repo-scripts\vendor_pytuflow_wheel.py --installed
py -3.14 -c "from ryan_library.functions.tuflow.pytuflow_compatibility import require_validation_api; print(require_validation_api().diagnostic.message)"
```

`requirements.txt` installs the repository in editable mode with its development and MCP dependencies, plus
the current QGIS MCP Python bridge from upstream GitHub main. Python then imports the current checkout while developers and agents
edit `ryan_library`. The dependency-only command installs binary Fiona, Rasterio and GDAL packages from the
configured geospatial wheel index. It supersedes the former single-package batch installers and can be previewed with
`--dry-run`. The parser-only reinstall replaces an incompatible official PyTUFLOW build even when its version matches
the accepted source; the following requirements install resolves all dependencies normally. The final checks verify
dependency consistency and the required public API. For managed or offline environments, use the local approved source with
`repo-scripts/setup_codex_mcp.ps1 -Install -QgisSource <path>` instead of the GitHub-based requirements command.

For an end user, double-click [install-from-pypi.bat](../install-from-pypi.bat)
([install-ryan-tools.bat](../install-ryan-tools.bat) is an alias):

```powershell
.\install-ryan-tools.bat
```

This installs the current published `ryan-tools[mcp]` package from PyPI,
the upstream QGIS MCP Python bridge and the [verified, local PyTuflow wheel](../repo-scripts/pytuflow-requirement.txt),
built from exact contributor commit `887534ea8c15` for upstream PR #17,
using the highest installed Python 3 by default (overrideable with `RYAN_TOOLS_PYTHON`). Every pip install step includes `--ignore-requires-python`, allowing Python 3.13 installations to bypass package metadata checks. Dependency wheels and runtime imports may still be incompatible.
Both normal installers bootstrap binary Fiona, Rasterio and GDAL, explicitly
reinstall only the accepted PyTuflow wheel, then resolve `ryan-tools` and the
pinned parser together with their dependencies. This replaces an incompatible
official parser even when its package version matches, without forcing a
reinstall of the GIS dependencies. Installation succeeds only after `pip check`,
installed wheel-origin/digest verification and the public PyTUFLOW capability guard pass; a failure stops before Codex registration. Dry runs print
these checks without claiming that they passed.
If Codex or its VS Code extension is installed,
the script safely configures its QGIS and ryan-tools MCP entries in the shared user-level Codex config. Without Codex,
it leaves Codex configuration untouched. Pass `--skip-codex` to disable automatic
registration, `--skip-qgis-mcp` for headless/offline installation, or
`--qgis-mcp-source <path>` to install the bridge from a local source.
After updating Codex configuration, restart Codex or reload the Codex extension in VS Code. Python 3.14 or newer remains the supported runtime for `ryan-tools`; `ryan-culverts` remains an independent dependency. The QGIS desktop plugin
must still be installed and started through QGIS Plugin Manager.

For developers working from the checkout, keep the local verified-wheel workflow:

```powershell
.\build-and-install-local-wheel.bat
.\install-local-wheel.bat
```

The first builds and installs the latest local wheel; the second installs an
existing wheel without rebuilding. These commands do not replace developer
editable installs (`pip install -r requirements.txt`).

The vendored dependency wheel is checked into ordinary Git under
[`repo-scripts/wheels/pytuflow/`](../repo-scripts/wheels/pytuflow/887534ea8c15b80d185a38a7ead1d42ab42a7167/README.md).
Run the repository requirements from the repository root, including if the checkout path contains spaces.
A missing or corrupt wheel fails verification; installation never silently falls back to source code.
Plain `pip install ryan-tools` resolves the published `pytuflow>=1.2,<2` version range through PyPI.
It does **not** automatically discover the wheel stored beside these repository scripts.
That normal PyPI contract is unchanged; use the repository installer to require the accepted fork.

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
.\build-and-install-local-wheel.bat
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

Windows/Python 3.14 is the primary runtime, typing and installed-package acceptance environment. Ubuntu runs
inexpensive portable checks and the existing focused POSIX filesystem-security regressions. Linux success is not
substitute evidence for Windows, QGIS, OSGeo4W or other native applications.

[`ci.yml`](../.github/workflows/ci.yml) runs on pull requests, every push to `main`, and manual dispatch. Its
`Ruff and whitespace` preflight is a prerequisite for expensive jobs; the former independent Fast PR workflow has
been consolidated into it. A lint, workflow, documentation or logging failure therefore stops Windows installation
from starting. The required `Policy` check also explicitly fails if preflight fails.

| Check | Environment and responsibility |
| --- | --- |
| Ruff and whitespace | Ubuntu/Python 3.14; complete-PR file scope, patch whitespace, pinned Ruff, actionlint, documentation and logging checks |
| Policy | Windows/Python 3.14 strict typing when Python inputs changed; inexpensive Ubuntu status reporting otherwise |
| Hosted tests | Windows/Python 3.14; existing full hosted selection, explicit MCP profiling, compilation and export checks |
| MCP smoke | Inexpensive status gate requiring successful Windows Hosted tests and explicit MCP acceptance output |
| Package | Windows/Python 3.14; real wheel construction, verification and isolated installed-package/copied-wrapper acceptance |
| POSIX wrapper security | Small Ubuntu/Python 3.14 static inventory, SQLite locking and descriptor-safety checks |

The scope helper compares the complete PR base-to-head diff with rename detection disabled. Moving Python code to a
Markdown filename still requires full runtime validation. Only a known, nonempty, entirely Markdown PR diff can skip
runtime/package/POSIX jobs. Main pushes and manual runs always request full validation. An unavailable comparison
selects all first-party static inputs and full validation; patch whitespace is reported as unavailable. A helper
execution failure fails required preflight rather than reporting a successful empty scope.

Changed-file typing uses the cumulative PR diff. Policy derives the strict GIS-stub group from
`pyright-initialiser.json` and checks that project separately from the remaining files. Scope lists use a JSON
manifest rather than large Windows environment variables; large Pyright argument lists are checked in bounded
batches. Changes to lint/type configuration expand the corresponding static-check scope.

PR concurrency cancels superseded runs. Every main push keeps its own release-validation run. On the first PR
attempt, ordinary hosted pytest stops at the first failure; rerunning the same revision retains complete diagnostics.
All required final-commit checks still apply to draft PRs. Batch related changes into one locally checked push.
See the [CI work record](work/2026-11-08-ci-resource-efficiency.md) for delivery and validation evidence.

### CI dependency profiles

Windows CI runs [`ci_gis_bootstrap.py`](../repo-scripts/ci_gis_bootstrap.py) around the existing binary GIS installation
command. A failed download qualifies for recovery only when pip reports HTTP 502, 503 or 504 for a Fiona, Rasterio or
GDAL wheel in `cgohlke/geospatial-wheels`. The helper resolves that exact release tag and filename through GitHub's
public API, requires the official asset size and SHA-256 digest, and verifies the download before installing it.
Recovery is limited to one asset per distribution and four normal bootstrap attempts in total. The final attempt
must resolve all dependencies successfully; repeated failures, other errors and invalid artifacts fail the job.
Downloads and subprocesses have time and size bounds. Normal user installers and dependency declarations are unchanged.

Normal user/developer installation remains [`requirements.txt`](../requirements.txt), with the unchanged runtime
metadata in [`pyproject.toml`](../pyproject.toml). CI profiles are narrower installation inputs for their particular
checks, not replacements for the expected user environment.

| Profile | Installation after the Windows binary GIS bootstrap | Scope |
| --- | --- | --- |
| Hosted tests | `python -m pip install -r repo-scripts/ci-test-requirements.txt` | All declared runtime dependencies, MCP extra, pytest and the immutable accepted PyTuflow wheel; omits unused development tools and the separate QGIS MCP bridge |
| Strict typing | `python -m pip install -r repo-scripts/ci-policy-requirements.txt` | Runtime, development and MCP extras plus immutable accepted PyTuflow wheel; omits the separate QGIS MCP bridge |
| Package smoke | Existing `repo-scripts/wheel-smoke-requirements.txt` workflow | Focused isolated wheel dependencies; real build and copied-wrapper checks remain |

Run these commands from the repository root. The hosted command does not invoke Ruff, Pyright, pandas-stubs,
pytest-cov or the build frontend; those tools are retained where needed by static checks or the Package job.
First-party tests do not import the interactive QGIS MCP bridge. Its normal installation/configuration contract is
unchanged. Build isolation still installs the declared build backend.

Do not remove runtime/GIS dependencies based only on an import search. Pandas loads Excel/Parquet engines dynamically,
hydraulic packages have transitive dependencies, and `importorskip` can silently hide coverage. Any narrower Windows
test group must demonstrate matching collected nodes, deselections, skip reasons and native capabilities against the
same source and fixture revisions. Extra groups also repeat setup; compare total runner minutes as well as elapsed
time before adopting a split. Pip caches store downloaded packages, not a ready-installed environment or test results.

### Hosted test and package boundaries

The hosted test job initialises only `qgis-resources` and `tests/test_data`. The QGIS repository supplies test/resource
data; no QGIS application is launched. The job installs the binary GIS bootstrap and hosted dependency profile,
sets `MPLBACKEND=Agg`, and runs:

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
python -m pytest tests/functions/gdal/test_profiling.py -m "not gui and not external" `
  --junitxml=test-results/mcp-profiling.xml
```

Create `test-results` before running these commands. The workflow also compiles `ryan_library` and asserts the MCP
module/tool exports. Most previous MCP tests already belong to the broad hosted selection; the explicit profiling
command preserves the former MCP job's unique test file despite the broad command's directory exclusion.

The ignored modules import `osgeo` directly or through production modules during collection, so marker deselection
alone cannot define this hosted boundary. Rasterio/Fiona tests, native ASC processing, wrapper dry runs and mocked
subprocess behaviour remain. The real 7-Zip integration module remains an optional local check. Tests marked
`external` require an explicitly supplied application or executable; ASC_to_ASC parity uses `ASC_TO_ASC_EXE` locally.
The two pre-existing floodway deselections remain in their
[exclusion record](work/2026-10-16-floodway-ci-exclusions.md).

Package initialises `qgis-resources`, installs its focused smoke prerequisites, builds and verifies a fresh wheel,
performs a `--no-bump` rebuild, confirms the declared version is unchanged, and checks isolated installation outside
the checkout with `--expected-root`. Installed MCP behaviour and copied wrappers remain independently verified.
Hosted JUnit and POSIX audit evidence are retained for seven days; the verified wheel is retained for 14 days.

Real QGIS/OSGeo4W acceptance remains a native-environment task. The hosted GIS bootstrap does not establish that every
application-specific GDAL workflow is supported. Historical independent-workflow timings are preserved in the
[17 October work record](work/2026-10-17-fast-pr-checks-129.md); they do not describe the consolidated workflow.

### TUFLOW input regression

[`tuflow-input-validation.yml`](../.github/workflows/tuflow-input-validation.yml) keeps its targeted PR paths and
manual trigger. The full 161-TCF regression starts only after the public-API and incompatible-parser rejection
checks pass. Each dependency profile has a distinct pip-cache hash; lightweight jobs cannot freeze the native GIS
profile's cache before its wheels are downloaded. Superseded PR runs are cancelled. All model reports remain
available, with seven-day artifact retention; this exhaustive regression does not stop at the first finding.

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
git submodule update --init qgis-resources tests/test_data
```

Source review, documentation and static checks need no submodules. Initialise `excel-resources` separately when a
specific workbook task needs it. For source-only automation, set `GIT_LFS_SKIP_SMUDGE=1` for checkout/submodule commands
and restore the prior environment setting afterwards. Fetch a needed LFS asset explicitly, for example
`git -C excel-resources lfs pull --include="*.xlsx"`; do not download every reference binary for routine CI.

`tests/test_data` is required for the broad hosted suite. Tests do not automatically download unavailable proprietary
data, and new committed fixtures should be synthetic unless sharing the source data is permitted.

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
