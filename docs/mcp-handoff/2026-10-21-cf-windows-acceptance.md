# Workstream F — Windows engineering acceptance (21 October 2026)

**Status:** repeatable test plan only; real Windows/engineering acceptance is **not** claimed. These steps require a Windows host, an installed Python 3.13 or 3.14, a disposable local SSD location and optionally a writable test share. No model simulations should be started by these probes. Do **not** point destructive commands at live project inputs.

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

Expected: the supporting branch is checked out; Python is 3.13+; repository checks are tied to a specific commit SHA. The Pyright config targets 3.14 syntax but the distribution has `requires-python >=3.13`. Preserve Python 3.13 support; additionally run the focused tests under that interpreter. Use the user's standard Python installation; an isolated *wheel directory*, not a replacement interpreter environment, is used below.

## 2. Fast automated checks (safe, no external executables)

```powershell
python -m ruff check ryan_library/mcp/ tests/mcp/
python -m ruff format --check ryan_library/mcp/ tests/mcp/
python -m pyright ryan_library/mcp/models.py ryan_library/mcp/registry.py
python -m pytest -q tests/mcp/test_registry.py tests/mcp/test_catalogue_contracts.py tests/mcp/test_engineering_paths.py
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
assert len(installed_files()) == 83
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

Expected: **35** script workflow records, **33** unique script paths, **83** wheel-selected files, no source checkout. No dependency imports or external applications are executed. The `--no-deps` install proves packaging/discovery, not end-to-end runnable wrappers; repeat with normal dependencies for functional acceptance if required.

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

## 7. External functional acceptance (authorised disposable copies only)

| Tool | Acceptance action | Evidence / failure criteria |
| --- | --- | --- |
| QGIS 4 | In QGIS Python console, import `qgis.core`, load a small *copied* GeoPackage and check layer feature count and CRS; exercise the QGIS MCP `ping` and `diagnose` separately | Screenshot/version plus validity, CRS, feature count; plugin/bridge unavailable is a failure, not a pass |
| GDAL/OGR | Generate a tiny local DEM and simple polygon in the authorised OSGeo shell; test `gdalinfo`, `ogrinfo`, raster translation, clipping and vector layer selection on copies | Compare input SHA-256 before/after; check output CRS, nodata, extent, row/feature counts and exit codes |
| TUFLOW | Verify configured executable and licensed environment without launching a project; separately authorise a disposable tiny TCF test run if available | Record version/license, exact command, model start/end, complete log and return code; **not accepted** without a complete actual run |
| HY-8 | Verify installed HY-8 version and `run-hy8` dependencies; validate a supplied disposable baseline crossing through the supported wrapper | Compare retained HY-8 hydraulic output and native solver results within documented tolerances, capture errors and preserved inputs |
| Network shares | Run the exact same harmless fixture workflow on local SSD and mapped/UNC test share; capture logs, file hashes and timings | No unexpected writes outside scratch folder; distinguish filesystem contention and access denied from application bugs |

For each external tool also deliberately test missing executable, invalid input, read-only destination and interrupted/failed run using a scratch project. Required result: an informative diagnostic, non-zero exit status for genuine failure, and unchanged original inputs. If a wrapper currently lacks that contract, record the exact wrapper and fixture for Agent 1 rather than editing their files in parallel.

## Sign-off record

| Requirement | Result | Evidence |
| --- | --- | --- |
| Isolated installed wheel / no source checkout | Pending Windows run | SHA, wheel file, output of Section 3 |
| Python 3.13 and 3.14 focused checks | Pending respective interpreters | Versions and pytest/Ruff/Pyright output |
| SSD spaces, Unicode, long paths | Pending Windows run | Path names, hashes, return codes |
| Mapped and UNC share | Pending authorised share | Root paths (redact sensitive information), timings, hashes |
| QGIS 4 and GDAL/OGR | Pending real app | Versions, smoke fixtures, outputs |
| TUFLOW and HY-8 | Pending licensed real app | Baselines, complete logs, numerical comparisons |
| Data preservation under failure | Synthetic tests only | Real tool failure hashes still pending |

**No merge or PyPI publishing** is authorised by this checklist. Keep source inputs and engineering runs separate from the metadata-only MCP discovery service.
