# ryan-tools MCP handoff

## Purpose

This document hands the ryan-tools MCP work to another agent. It records what has been implemented, how the current
server behaves, what has been validated, and the next architectural task: resolving CLI workflows from the installed
Python package instead of depending on a repository checkout on `Q:`.

Read the repository instructions before making changes:

- `AGENTS.md`
- `docs/DEVELOPMENT_GUIDE.md`
- `docs/README.md`
- `docs/MCP_SETUP.md`
- `ryan-scripts/README.md`
- `ryan-scripts/WRAPPER_STANDARD.md`
- The relevant domain README, especially `ryan-scripts/TUFLOW-python/README.md` or
  `ryan-scripts/gdal-python/README.md`

The repository is at `Q:\BGER\PER\RPRT\ryan-tools`. Use a nested approved Q: working directory, prefer `cmd.exe` for
network-drive commands, and do not run repository code from `C:\Temp\Codex-local`.

## User intent

The MCP server is primarily a discovery layer. It should help an agent find an appropriate existing CLI workflow and
return the information needed to run it through the agent's shell. It must not expose every script as an MCP tool and
must not execute catalogued workflows itself.

Requirements confirmed by the user:

- Keep the MCP endpoint surface small and expose scripts in stages through capability profiles.
- The default profile is `create`.
- Agents run the scripts through their CLI interfaces, not through the MCP server.
- Do not retain compatibility endpoints or compatibility entry modules.
- Python bytecode is acceptable generally. `-B` and `PYTHONDONTWRITEBYTECODE=1` are optional corporate-environment
  controls, not package requirements.
- Installed Python modules are preferred over a mapped-drive checkout where a supported packaged wrapper exists.

## Implemented MCP architecture

The package-native MCP implementation is under `ryan_library/mcp`:

- `models.py` defines workflow profiles, mutation classes and catalogue models.
- `registry.py` loads the packaged workflow catalogue, applies profile visibility, resolves CLI scripts and constructs
  command arrays without executing them.
- `server.py` registers the focused MCP tools and catalogue resource.
- `ryan_library/resources/mcp/workflows.json` contains the generic staged workflow catalogue.

The default MCP tools are:

1. `parse_tuflow_filename`
2. `inspect_tlf_log`
3. `inspect_raster_metadata`
4. `check_repo_health`
5. `list_workflows`
6. `get_workflow`

The visible catalogue is also available as the read-only MCP resource
`ryan-tools://workflows/catalogue`.

Profiles are cumulative:

| Profile | Purpose |
| --- | --- |
| `core` | Focused direct MCP helpers only. |
| `analysis` | Read-only CLI audits and inspections. |
| `create` | Workflows that create outputs; this is the default. |
| `privileged` | In-place changes, deletion or external execution requiring explicit configuration. |

A tool input cannot elevate beyond the server's configured `RYAN_MCP_PROFILE`. Under the current repository-backed
`create` configuration, the catalogue has 44 workflows in total, 36 visible workflows and 8 hidden privileged
workflows.

GDAL workflow records are currently extended dynamically from the authoritative
`ryan-scripts/gdal-python/gdal_cli_tools.json` when a checkout is available.

## Packaging and entry point changes

`pyproject.toml` now provides:

```toml
[project.optional-dependencies]
mcp = ["mcp[cli]>=2.1,<3"]

[project.scripts]
ryan-mcp = "ryan_library.mcp.server:main"

[tool.setuptools.package-data]
"ryan_library.resources.mcp" = ["*.json"]
```

The former top-level `ryan_mcp_server.py` has been deleted. The former `list_gdal_cli_tools` and
`get_gdal_cli_tool` compatibility tools and their environment switch have also been removed. Do not restore them.

`.vscode/mcp.json` invokes the package module:

```json
"args": ["-m", "ryan_library.mcp.server"]
```

Documentation was updated in:

- `README.md`
- `docs/MCP_SETUP.md`
- `ryan-scripts/gdal-python/README.md`

Focused tests were added under `tests/mcp`.

## Current installed configuration

The package was rebuilt and installed into the user's Python as version `26.8.26.4`. The wheel is:

```text
dist/ryan_functions-26.8.26.4-py3-none-any.whl
```

Python executable:

```text
C:\Program Files\Python314\python.exe
```

Installed package base:

```text
C:\Users\Ryan.Brook\AppData\Roaming\Python\Python314\site-packages
```

The user-level Codex configuration is `C:\Users\Ryan.Brook\.codex\config.toml`. Its current ryan-tools entry uses
the package-native module and the repository checkout:

```toml
[mcp_servers.ryan-tools]
command = 'C:\Program Files\Python314\python.exe'
args = ["-B", "-m", "ryan_library.mcp.server"]

[mcp_servers.ryan-tools.env]
PYTHONDONTWRITEBYTECODE = "1"
PYTHONUTF8 = "1"
RYAN_TOOLS_REPOSITORY_ROOT = 'Q:\BGER\PER\RPRT\ryan-tools'
RYAN_MCP_PROFILE = "create"
```

The bytecode controls are retained for this machine's corporate constraints. They should remain optional in project
documentation and code.

## Outstanding design problem

The installed wheel contains the `ryan_library.scripts` package, currently about 19 Python files, but the separate
repository `ryan-scripts` tree contains roughly 103 Python scripts and is not installed as a package.

Although the workflow catalogue itself is packaged, every `WorkflowSpec` currently stores a repository-relative
`script` path. `WorkflowRegistry._script_path()` therefore requires `RYAN_TOOLS_REPOSITORY_ROOT`. Without a checkout,
the catalogue loads but its workflows are unavailable. A neutral installed-package smoke test without the repository
root returned zero available workflows.

This makes the mapped Q: checkout a runtime dependency and can also allow the installed library version and checked-out
wrapper version to drift apart.

Do not solve this by constructing physical paths into `site-packages`. Use Python module invocation or installed console
entry points instead:

```text
C:\Program Files\Python314\python.exe -m ryan_library.scripts.tuflow.tuflow_logsummary --help
```

## Recommended next implementation

Implement installed-module workflow resolution in stages.

1. Extend `WorkflowSpec` with an explicit execution target. A discriminated model such as `module`, `script`, or
   `console_script` is preferable to guessing from a string.
2. For packaged modules, generate command prefixes as `[sys.executable, "-m", module_name]`.
3. Continue supporting repository-relative script targets temporarily for workflows that have not been migrated.
   This is staged migration support, not an old MCP compatibility endpoint.
4. Audit every existing `ryan_library.scripts` wrapper for a functional headless CLI and map suitable catalogue
   workflows to those modules first.
5. Move or wrap additional supported `ryan-scripts` CLIs under `ryan_library.scripts` in small domain-based batches.
   Preserve the wrapper standard: presentation and CLI handling belong in wrappers; reusable work belongs in
   orchestrators/functions.
6. Package the authoritative GDAL workflow metadata, or generate the packaged catalogue from it during the build, so
   installed GDAL discovery does not require Q:.
7. Make repository discovery an optional development/fallback facility. Do not remove
   `RYAN_TOOLS_REPOSITORY_ROOT` from user or workspace configuration until all default `create` workflows intended for
   installed use have package-native targets.
8. Once the installed catalogue is self-sufficient, remove `RYAN_TOOLS_REPOSITORY_ROOT` from the normal user
   configuration and retain it only for source-checkout development or unmigrated workflows.

Important considerations:

- Do not mechanically package all 103 scripts. Some may be interactive, experimental, privileged, dependent on
  adjacent files, or unsuitable for import/module execution.
- Keep lifecycle, mutation, headless-argument and explicit-approval metadata in the catalogue.
- Preserve profile gating. Package installation must not make privileged workflows visible under `create`.
- The MCP server must continue returning commands rather than running them.
- Prefer `python -m ...` over direct `site-packages` paths because module invocation is portable across user installs,
  virtual environments and Python patch versions.

## Tests to add for installed-module migration

At minimum, add coverage for:

- A module-backed workflow being available with no repository checkout.
- Its `command_prefix` and `help_command` using `sys.executable`, `-m` and the declared module.
- A repository-backed fallback workflow still resolving when an explicit checkout is supplied.
- A missing fallback script being reported unavailable without hiding working module workflows.
- Profile filtering behaving identically for module and repository targets.
- Privileged workflows remaining hidden under `create`.
- Packaged GDAL discovery working from an installed wheel in a neutral directory.
- An installed-wheel smoke test confirming the deleted `ryan_mcp_server` module remains absent.

Build and test the wheel from a neutral working directory such as `Q:\BGER\PER\RPRT\temp`, not from the repository,
so the source checkout cannot accidentally satisfy imports or script discovery.

## Validation already completed

The current changes were checked with:

- Black: passed.
- Strict Pyright on `ryan_library/mcp` and `tests/mcp`: 0 errors and 0 warnings.
- Focused pytest suite: 9 passed.
- Documentation checker: passed.
- `python repo-scripts/build_library.py`: passed and produced version `26.8.26.4`.
- Forced user installation of the built wheel: passed.
- Neutral installed-package import: passed.
- `importlib.util.find_spec("ryan_mcp_server")`: returned `None`, confirming the compatibility module is absent.
- Repository-backed installed smoke test: `create` profile with 36 visible workflows.

Known warning:

```text
PytestConfigWarning: Unknown config option: cache_dir
```

On Q:, pytest's cache provider also encountered a network-drive permission error during one parallel run. Rerunning
the focused suite with `-p no:cacheprovider` passed all tests. This was a cache infrastructure issue, not a test
failure.

After modifying library code or package metadata, run:

```bat
python repo-scripts\build_library.py
```

Report command exit codes, generated outputs, warnings and errors.

## Worktree safety

The worktree was already dirty before the MCP implementation. Preserve unrelated user changes. In particular, do not
overwrite or revert:

- `ryan-scripts/misc-python/remove_excel_protection.py`
- `ryan_library/orchestrators/tuflow/water_level_profiles.py`
- `ryan-scripts/unsorted-python/plot_water_level_profiles.py`
- Changes in the `tests/test_data` and `unsorted` submodules
- The pre-existing deletion of the older wheel in `dist`

MCP-related changes are primarily:

- `.vscode/mcp.json`
- `README.md`
- `docs/MCP_SETUP.md`
- `docs/mcp-handoff/README.md`
- `pyproject.toml`
- `ryan-scripts/gdal-python/README.md`
- deleted `ryan_mcp_server.py`
- `ryan_library/mcp/`
- `ryan_library/resources/mcp/`
- `tests/mcp/`
- `dist/ryan_functions-26.8.26.4-py3-none-any.whl`

No commit or staging operation has been performed.
