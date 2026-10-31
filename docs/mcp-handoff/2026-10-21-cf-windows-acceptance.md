# Workstream F — Windows engineering acceptance (21 October 2026)

**Status:** bounded Windows acceptance executed on 22 October 2026; see evidence below. Complete engineering acceptance remains open. These steps require a Windows host, Python 3.14 or newer, a disposable local SSD location and optionally a writable test share. No model simulations should be started by these probes. Do **not** point destructive commands at live project inputs.

## Windows execution evidence — 22 October 2026

Tested supporting [PR #146](https://github.com/Chain-Frost/ryan-tools/pull/146),
branch `work/pr137-mcp-engineering-cf-20261021`, commit
`20cf28a91a1bc59e76ce42da9366299f07a354b3`. This advances the checklist's original
plan-only status; complete engineering acceptance remains open.

| Check | Result |
| --- | --- |
| Python 3.14.6, all `tests/mcp` with long-path, mapped/UNC, GDAL and QGIS opt-ins | **98 passed, 10 skipped** in 3.70 s; three Windows symlink privilege skips and seven POSIX-only skips |
| Local long paths | Passed with `LongPathsEnabled=1`; spaces, Unicode and >260-character copy paths exercised |
| Authorised share | Both `T:\temp` and `\\StrandServer\tuflow-agil-share\temp` passed; unique temporary children cleaned up |
| Real GDAL/OGR | Two tests passed using GDAL 3.13.3: translation, clipping, missing-input failures and unchanged source bytes |
| Real QGIS | Two tests passed using QGIS 4.2.3, Qt 6.11.1 and its Python 3.12.14: synthetic OGR provider and CLI version |
| Isolated wheel | Passed from unrelated scratch directory with `python -I`, no checkout discovery: 35 records, 33 targets, 83 selected files |
| Ruff lint / format | Passed; 14 files already formatted |
| Scoped strict Pyright | `models.py` and `registry.py`: zero errors/warnings |
| Documentation | Three-document `--links-only` check passed; original full command reports abbreviated catalogue paths and external/config paths as unresolved |
| QGIS MCP bridge | Retry after user enabled MCP: `ping` passed; `diagnose` returned degraded due to server 0.15.2 / plugin 0.15.1 mismatch. Desktop copied-GeoPackage probe passed |
| Python 3.13 local | Not run; user explicitly removed this local check from scope |
| Hosted CI | All six checks passed at tested head, including hosted Python 3.13; run 39638586619 and Ruff run 39638586518 |
| TUFLOW / HY-8 | Files located only; no licence check, simulation or hydraulic comparison performed |

The current-version wheel is `ryan_tools-26.10.20.3-py3-none-any.whl`, SHA-256
`6248494fdc5105013e863ca6067689cbe1ab869c1e775cfae704d26657f587d6`.
It was built with `python -m pip wheel --no-deps --wheel-dir <scratch>/wheel .`
and installed with `--no-deps --target <scratch>/site`. The initial
`--no-build-isolation` attempt could not import setuptools; build isolation
succeeded without changing the global installation or declared version.
This probe proves packaging/discovery, not dependency-complete wrapper execution.

Scratch evidence is retained at
`C:\Users\Ryan\AppData\Local\Temp\ryan-cf-acceptance-20261022-20cf28a`:
wheel, isolated site, `blank/probe.py`, and `all-checks.cmd`. The latter initializes
`o4w_env.bat` and `qt6_env.bat`, sets QGIS prefix/plugin/Python paths, clears
`PYTHONHOME` and `PYTHONEXECUTABLE` for the Python 3.14 pytest parent, and configures
the separate QGIS child to use `C:\OSGeo4W\apps\Python312\python.exe`.
All six filesystem/GIS opt-in variables from Sections 4, 5, 7 and 8 are enabled.
Run that command file from this repository root to reproduce `python -m pytest -q tests/mcp -ra`.

Environment troubleshooting matters: passing multiline `-c` code through
`python-qgis.bat` produced empty stdout and failed the provider assertion; the
initialized direct interpreter passed. Retaining OSGeo4W's `PYTHONHOME` initially
caused a Python standard-library mismatch, and retaining `PYTHONEXECUTABLE`
misdirected a subprocess; clearing both resolved these runner failures.
No test or product code was changed to obtain the passes.

Installed hydraulic dependencies: `ryan-culverts 26.10.18.3` and
`run-hy8 2026.10.18.2`. HY-8 file version is 8.0.1.2 at
`C:\Program Files\HY-8 8.00\HY864.exe`; TUFLOW executables exist under
`C:\TUFLOW\tuflow-2026.3` but have no reported Windows file version.
These are discovery evidence only. Outstanding: MCP version alignment and full project
acceptance, licensed TUFLOW/HY-8 baselines, real-tool read-only/interruption failure
cases, and full workflow parity. Network tests exercised wrapper copying, not
GDAL/QGIS processing over the share. No merge, publication or project simulation
was performed; documentation evidence is committed separately from the tested code; not pushed.

## 1. Prerequisites and recorded environment

In Windows PowerShell, from the supporting PR checkout (do not merge PR #137):

```powershell
$Repo = 'E:\Github\ryan-tools'  # replace with actual branch checkout
Set-Location -LiteralPath $Repo
git branch --show-current
git rev-parse HEAD
python --version
python -m pip show ryan-tools
python -m pip show ryan-culverts run-hy8
python -m pytest --version
python -m ruff --version
python -m pyright --version
```

Expected for the current PR: Python 3.14+ with `requires-python >=3.14`; all checks are tied to an exact commit. Python 3.13 support was explicitly dropped on 22 October. The older recorded Windows run below is historical evidence, not a rerun of the integrated branch. Use the user's standard Python installation; an isolated *wheel directory*, not a replacement interpreter environment, is used below.

## 2. Fast automated checks (safe, no external executables)

```powershell
python -m ruff check ryan_library/mcp/ tests/mcp/
python -m ruff format --check ryan_library/mcp/ tests/mcp/
python -m pyright ryan_library/mcp/models.py ryan_library/mcp/registry.py
python -m pytest -q tests/mcp/test_registry.py tests/mcp/test_catalogue_contracts.py tests/mcp/test_engineering_paths.py tests/mcp/test_external_program_discovery.py
python repo-scripts/check_documentation.py docs/mcp-handoff/2026-10-21-cf-target-assessment.md docs/mcp-handoff/2026-10-21-cf-windows-acceptance.md docs/MCP_SETUP.md
```

Expected: tests pass except explicitly skipped Windows/share/long-path cases on machines without opt-in; no source wrapper is executed by the catalogue tests. Record exact pytest pass/skip totals. Test coverage from mocks and disposable fixtures **cannot** substitute for external-application acceptance.

## 3. Real installed wheel, without checkout discovery or editable install

This uses a separate scratch directory, installs *only* a wheel into that directory, then runs an isolated Python process from an unrelated working directory. It leaves the globally installed `ryan-tools` unchanged.

```powershell
$Root = Join-Path $env:TEMP ('ryan-cf-wheel-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force -Path "$Root\wheel", "$Root\site", "$Root\blank" | Out-Null
python -m pip wheel --no-deps --no-build-isolation --wheel-dir "$Root\wheel" "$Repo"
$Wheel = Get-ChildItem "$Root\wheel\ryan_tools-*.whl" | Select-Object -First 1
if (-not $Wheel) { throw 'Wheel creation failed' }
python -m pip install --no-deps --target "$Root\site" "$($Wheel.FullName)"

$Probe = @'
import importlib.metadata
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(sys.argv[1]).absolute()))
from ryan_library.mcp.models import CapabilityProfile
from ryan_library.mcp.registry import WorkflowRegistry
from ryan_library.mcp.installed_scripts import installed_files
registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, discover_repository=False)
listing = registry.list_workflows(include_unavailable=True)
scripts = [w for w in listing["workflows"] if w["execution_kind"] == "script"]
assert registry.repository_root is None
assert str(registry.scripts_directory).startswith(str(Path(sys.argv[1]).absolute()))
assert len(scripts) == 35
assert len({w["execution_target"] for w in scripts}) == 33
assert all(w["available"] for w in scripts)
assert len(installed_files()) == 84
for item in scripts:
    detail = registry.get_workflow(item["id"])
    assert detail["availability_scope"] == "entry_point_only"
    assert detail["script_source"] == "installed_distribution"
    assert detail["external_prerequisites"]["status"] != "verified"
print(json.dumps({"script_workflow_records": len(scripts), "unique_script_targets": 33,
                  "wheel_scripts_directory": str(registry.scripts_directory),
                  "external_acceptance": "NOT PERFORMED"}, indent=2))
'@
Push-Location -LiteralPath "$Root\blank"
try {
    python -I -c $Probe "$Root\site"
    if ($LASTEXITCODE -ne 0) { throw "Isolated wheel probe failed: $LASTEXITCODE" }
} finally {
    Pop-Location
}
# Inspect $Root as evidence, then delete only that scratch directory when satisfied.
```

Expected after 31 October integration: **36** script workflow records, **34** unique script paths, **84** wheel-selected files, no source checkout. The 22 October evidence at the top is the earlier **35/33/83** baseline and does not certify the new head. No dependency imports or external applications are executed. The `--no-deps` install proves packaging/discovery, not end-to-end runnable wrappers; repeat with normal dependencies for functional acceptance if required.

## 4. Disposable local SSD, spaces, Unicode and long paths

```powershell
python -m pytest -q tests/mcp/test_engineering_paths.py
$env:RYAN_CF_TEST_LONG_PATHS = '1'
python -m pytest -q tests/mcp/test_engineering_paths.py -k long_path
Remove-Item Env:RYAN_CF_TEST_LONG_PATHS
```

Expected: newly copied files match the source bytes; existing targets are preserved on failure; invalid copy requests produce a non-zero CLI code and useful stderr. Long-path acceptance may fail where Windows long-path support or network policy is disabled—record the actual OS setting and failing filename instead of changing the test to pass. The opt-in case creates a path longer than 260 characters, containing Unicode and spaces.

## 5. Mapped drive and UNC paths (explicit opt-in)

Supply **two writable test directories**, one through the mapped-drive letter and one through its real UNC form. Do not supply model/project roots without first setting aside a dedicated empty testing folder. Tests create a unique `ryan-cf-acceptance-*` child and remove *only that child*.

```powershell
$env:RYAN_CF_MAPPED_ROOT = 'Q:\YOUR-TEST-SHARE\ryan-cf-test'
$env:RYAN_CF_UNC_ROOT = '\\server\share\ryan-cf-test'
Test-Path -LiteralPath $env:RYAN_CF_MAPPED_ROOT
Test-Path -LiteralPath $env:RYAN_CF_UNC_ROOT
python -m pytest -q tests/mcp/test_engineering_paths.py -k network_share_copy
Remove-Item Env:RYAN_CF_MAPPED_ROOT
Remove-Item Env:RYAN_CF_UNC_ROOT
```

Expected: both forms preserve their user-visible spelling through `Path.absolute()`, copy bytes match, and no overwrite occurs without permission. Run from the same authenticated Windows session used for actual engineering tools; a mapped drive visible in an interactive session may not exist for a service/scheduled task. Repeat on a local SSD to separate share latency/permissions from code defects.

## 6. Discover installed engineering executables without running simulations

The non-executing Python helper uses explicit configuration before PATH, and
reports only `path_found_unverified` or `not_found`. It never launches programs.
For example:

```powershell
python -c "from ryan_library.mcp.engineering import locate_external_program; print(locate_external_program('gdalinfo'))"
```


```powershell
Get-Command gdalinfo.exe, ogrinfo.exe, ogr2ogr.exe, qgis_process.exe -ErrorAction SilentlyContinue |
  Select-Object Name, Source
# Set these to the actual installed executables; never use model-file paths here.
$env:RYAN_CF_TUFLOW_EXE = 'C:\TUFLOW\YOUR-INSTALL\TUFLOW_iSP_w64.exe'
$env:RYAN_CF_HY8_EXE = 'C:\Program Files\YOUR-HY8-INSTALL\HY8.exe'
foreach ($name in 'RYAN_CF_TUFLOW_EXE', 'RYAN_CF_HY8_EXE') {
    $value = [Environment]::GetEnvironmentVariable($name)
    if (-not (Test-Path -LiteralPath $value -PathType Leaf)) { Write-Error "Missing configured executable: $name ($value)" }
    else { Get-Item -LiteralPath $value | Select-Object FullName, @{N='FileVersion';E={$_.VersionInfo.FileVersion}} }
}
gdalinfo --version
ogrinfo --version
qgis_process --version
```

Expected: version/provenance evidence for GDAL/OGR/QGIS 4, TUFLOW, HY-8. `Get-Command` and `Test-Path` detect paths only; `VersionInfo` can be blank. HY-8/TUFLOW binaries are **not invoked** by this section. Run the GDAL commands in the relevant OSGeo4W/QGIS shell if the standard shell lacks them; record which Python interpreter and DLL path were used. QGIS 4 Python plugin validation must be performed within QGIS 4's own Python environment (not assumed compatible with the user's standard Python 3.14).

## 7. Opt-in real GDAL/OGR subprocess acceptance

This is an executable-backed test, unlike the mocked file-path probes above.
It creates a six-cell ASCII DEM and two-feature GeoJSON in pytest temporary
directories with spaces and Unicode filenames. With the actual GDAL/OGR programs
available, it converts the raster to GeoTIFF, clips vector features, checks
the data/NoData values and feature count, then deliberately supplies missing
inputs and asserts non-zero exits and preserved input bytes.

**Run only in the intended QGIS 4/OSGeo4W shell on a disposable test machine.**
Nothing runs unless the environment flag is set.

```powershell
$env:RYAN_CF_GDAL_ACCEPTANCE = '1'
# Optional if these binaries are not on PATH:
# $env:RYAN_CF_GDAL_BIN_DIR = 'C:\OSGeo4W\bin'
python -m pytest -q tests/mcp/test_real_gdal_acceptance.py -rs
Remove-Item Env:RYAN_CF_GDAL_ACCEPTANCE
Remove-Item Env:RYAN_CF_GDAL_BIN_DIR -ErrorAction SilentlyContinue
```

Expected: **2 real-tool tests passed**, no skipped tests after opt-in.
A missing configured GDAL/OGR executable is a test failure, not a skip.
Keep `-rs` output, the active PATH and GDAL/OGR versions as acceptance evidence.
A successful test establishes only these synthetic GDAL/OGR operations; it
does not establish QGIS 4 desktop plugin, TUFLOW, HY-8, large-file or network
storage acceptance. Run the same tests with `TMP`/`TEMP` directed to a
dedicated network-share scratch directory only when the share is authorised.

## 8. Opt-in actual QGIS 4 Python-provider acceptance

These checks launch a **separate, real QGIS process**, not a mocked import,
and verify QGIS major version 4, the OGR provider, one synthetic GeoJSON
feature, and unchanged input bytes. A second check invokes
`qgis_process --version`. Run only with a configured QGIS 4 environment:

```powershell
$env:RYAN_CF_QGIS_ACCEPTANCE = '1'
# Supply the actual QGIS/OSGeo4W Python and qgis_process paths from this workstation.
# $env:RYAN_CF_QGIS_PYTHON = 'C:\OSGeo4W\bin\python-qgis.bat'
# $env:RYAN_CF_QGIS_PROCESS = 'C:\OSGeo4W\bin\qgis_process.exe'
python -m pytest -q tests/mcp/test_real_qgis4_acceptance.py -rs
Remove-Item Env:RYAN_CF_QGIS_ACCEPTANCE
Remove-Item Env:RYAN_CF_QGIS_PYTHON -ErrorAction SilentlyContinue
Remove-Item Env:RYAN_CF_QGIS_PROCESS -ErrorAction SilentlyContinue
```

Expected: **2 passes**, no skips once enabled; missing or misconfigured
QGIS imports or the wrong major version are failures. A QGIS Python wrapper
such as `python-qgis.bat` may require additional environment preparation
and must accept `-c` with the fixture script. These checks do **not** verify
the desktop plugin, project styling, QGIS MCP bridge, or proprietary layers.
Validate the QGIS MCP `ping` and `diagnose` in the running desktop session.

## 9. External functional acceptance (authorised disposable copies only)

| Tool | Acceptance action | Evidence / failure criteria |
| --- | --- | --- |
| QGIS 4 | In QGIS Python console, import `qgis.core`, load a small *copied* GeoPackage and check layer feature count and CRS; exercise the QGIS MCP `ping` and `diagnose` separately | Screenshot/version plus validity, CRS, feature count; plugin/bridge unavailable is a failure, not a pass |
| GDAL/OGR | Generate a tiny local DEM and simple polygon in the authorised OSGeo shell; test `gdalinfo`, `ogrinfo`, raster translation, clipping and vector layer selection on copies | Compare input SHA-256 before/after; check output CRS, nodata, extent, row/feature counts and exit codes |
| TUFLOW | Verify configured executable and licensed environment without launching a project; separately authorise a disposable tiny TCF test run if available | Record version/license, exact command, model start/end, complete log and return code; **not accepted** without a complete actual run |
| HY-8 | Verify installed HY-8 version and `run-hy8` dependencies; validate a supplied disposable baseline crossing through the supported wrapper | Compare retained HY-8 hydraulic output and native solver results within documented tolerances, capture errors and preserved inputs |
| Network shares | Run the exact same harmless fixture workflow on local SSD and mapped/UNC test share; capture logs, file hashes and timings | No unexpected writes outside scratch folder; distinguish filesystem contention and access denied from application bugs |

For each external tool also deliberately test missing executable, invalid input, read-only destination and interrupted/failed run using a scratch project. Required result: an informative diagnostic, non-zero exit status for genuine failure, and unchanged original inputs. If a wrapper currently lacks that contract, record the exact wrapper and fixture for Agent 1 rather than editing their files in parallel.

## Sign-off record — reconciled 31 October 2026

The following marks the **22 October supporting-revision results**, not new
verification of PR #137's merged head. Check the full evidence above before
retesting; preserve the explicit distinction between bounded and full acceptance.

| Requirement | Current evidence | Still needed |
| --- | --- | --- |
| Isolated installed wheel / no source checkout | Passed on #146: 83 selected files, 35 workflow records, 33 targets | Repeat for the reconciled 84/36/34 selection on exact PR head |
| Python 3.14 focused checks | 98 passed / 10 skipped on Windows, 22 October | Recheck current changed code; 3.13 is unsupported |
| SSD spaces, Unicode, long paths | Bounded synthetic Windows paths passed | Current wheel/full script output contracts as applicable |
| Mapped and UNC share | Wrapper-copy tests passed on authorised mapped and UNC scratch | Real application workflow network-path and interruption behaviour |
| Real QGIS 4 / GDAL/OGR | Two tests each passed in the specified Windows environment | Production/locked-file, real-provider and full data-parity cases |
| TUFLOW and HY-8 | Executables located only | Authorised licensed model/crossing runs and numerical evidence |
| Failure/input preservation | Bounded synthetic and selected GDAL negative cases passed | Full destructive-script and interruption/recovery acceptance |

**No merge or PyPI publishing** is authorised by this checklist. Keep source inputs and engineering runs separate from the metadata-only MCP discovery service.

### Desktop MCP retry ? 22 October 2026

After the user enabled QGIS MCP, `ping` returned `pong=true`. `diagnose`
confirmed QGIS 4.2.3, Python 3.12.15, Qt 6.11.0 and healthy processing providers
(including GDAL, native and TUFLOW), with one connected client. Overall status
was **degraded** because server/client 0.15.2 differs from plugin 0.15.1.
No plugin installation, cache clearing or restart was attempted.

A synthetic GeoPackage was generated in a unique temporary folder, copied,
and read through the actual desktop OGR provider via MCP: valid, one feature,
EPSG:4326, SHA-256 unchanged after reading. Project filename, dirty state and
registered layers were unchanged. The desktop had no loaded project/layers.
Retained fixture: `C:\Users\Ryan\AppData\Local\Temp\ryan-cf-desktop-5ov4synv\copied.gpkg`;
SHA-256 `9f7aa9fd20dd20943c81b8f51ae4ce3a51dd040c1e732a588a928563b8b57377`.
This passes bounded desktop bridge/provider acceptance; version alignment,
real-project styling/interaction and licensed hydraulic checks remain open.
