# Development and execution environments

`ryan-tools` is installed into the user's normal Python 3.14 installation. Users are not expected to create, understand
or activate a virtual environment. Wrappers copied into project folders use the same installed `ryan_functions` package.

Some QGIS, OSGeo4W and GDAL workflows use their application's Python or command environment instead. An operation working
with the normal Python installation does not prove that it works inside those application environments, or conversely.

## Install for repository development

From the repository root, install the checkout and development requirements into the user's Python 3.14 installation:

```powershell
py -3.14 -m pip install --upgrade pip
py -3.14 repo-scripts\install_latest_wheel.py --dependencies-only
py -3.14 -m pip install -r requirements.txt
```

`requirements.txt` installs the repository in editable mode with its development dependencies. Python then imports the
current checkout while developers and agents edit `ryan_library`. The dependency-only command installs binary Fiona,
Rasterio and GDAL packages from the configured geospatial wheel index. It supersedes the former single-package batch
installers and can be previewed with `--dry-run`.

The normal user-facing Windows entry point builds and installs the package:

```powershell
.\package_and_install.bat
```

To install the newest existing wheel without rebuilding it:

```powershell
.\install-latest-wheel.bat
```

Both commands use the user's available Python installation. Users should not need to change interpreters or activate an
environment before running the installed wrappers.

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

Wrappers copied into project folders import the installed `ryan_functions` distribution. They do not add the repository
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

Use `--skip-pip` when the build dependency is already installed. Use `--skip-artifacts` only in environments that cannot
create or retain wheel files. A local build may change package metadata and replace the wheel in `dist`; inspect Git
status afterward.

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

Initialise the repository submodules before running tests that require fixtures:

```powershell
git submodule update --init --recursive
git lfs pull
```

`tests/test_data` is a required submodule. Resource submodules may also require their own Git LFS pull. Tests do not
automatically download unavailable proprietary data, and new committed fixtures should be synthetic unless sharing the
source data is permitted.

Some focused pytest runs on Windows require a pre-created repository-local base temporary directory. Environment-specific
components, such as the bundled `run_hy8` submodule, may have additional import-path instructions in their local docs.

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
