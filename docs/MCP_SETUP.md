# MCP Server Setup — ryan-tools

The **ryan-tools** MCP server gives AI agents a small set of focused inspection tools and a staged catalogue of
repository CLI workflows. The MCP server discovers scripts and returns safe command arrays; it does not execute the
catalogued scripts. The agent runs a selected script through its normal shell after reviewing the script's current
`--help`, input and output paths, mutation metadata and approval requirements.

## C/F integration and engineering acceptance

The [33-target MCP safety assessment](mcp-handoff/2026-10-21-cf-target-assessment.md)
records curated workflow paths, profiles, destructive flags and remaining readiness gates.
The [Windows engineering acceptance checklist](mcp-handoff/2026-10-21-cf-windows-acceptance.md)
includes safe automated checks, an isolated-wheel discovery probe and opt-in
mapped-drive/UNC/QGIS/GDAL/TUFLOW/HY-8 checks. These do **not** imply that
an installed executable has been tested or that a real model has run.

The `batch_vector_clip` wrapper launches `ogr2ogr` subprocesses and therefore
is also `privileged`, requires explicit approval and reports the executable
as declared but unverified. Its `--overwrite` option requires separate
output-preservation review; do not use it on production sources.

For GDAL recompression, the curated workflow includes a source-replacing
`--replace --yes` scenario and therefore now requires the `privileged` profile
and explicit approval. Users can continue to invoke the underlying audit-only
script outside MCP without replacement flags. Legacy TUFLOW batch and duplicate
deletion scripts do not have safe `--help` execution; MCP deliberately suppresses
their generated help commands.

## Default tools

The default MCP surface stays deliberately small:

| Tool | Description |
| --- | --- |
| `parse_tuflow_filename` | Parse a TUFLOW output filename into structured components. |
| `inspect_tuflow_result` | Process one supported TUFLOW result or log, with optional location and processed-time bounds, and return bounded metadata and sample rows. |
| `inspect_tuflow_collection` | Process up to 50 supported TUFLOW files and query their `ProcessorCollection` by location, data type and processed-time bounds. |
| `inspect_raster_metadata` | Inspect raster dimensions, resolution, CRS, transform, bounds, bands and NoData metadata. |
| `check_repo_health` | Report package versions, configured profile, checkout discovery and catalogue status. |
| `list_workflows` | Search paginated workflow summaries or request a compact, filtered domain overview. |
| `get_workflow` | Resolve one workflow's installed module or repository script, help command, scenarios and safety metadata. |

The complete visible catalogue is also available as the read-only resource
`ryan-tools://workflows/catalogue`. The read-only `ryan-tools://guidance/tuflow-processors` resource tells an agent
which Python processor factory, collection methods, registry and source files to use for queries beyond the bounded MCP
tools.

### Discover workflows in small pages

When the relevant domain is unclear, call `list_workflows(overview=True)` to receive a compact overview. Agents who
already know the domain or task can search directly; an overview call is not mandatory.
For example, `list_workflows(domain="gdal", query="crop")` discovers the existing
`clip_rasters_to_polygon` workflow (also found by `cropping`). A query for `pyramids` discovers
`build_external_overviews`, and `logs` finds `tuflow_log_summary`. Then call
`get_workflow("clip_rasters_to_polygon")` to inspect the canonical ID's details, declared keywords, safety metadata,
help command and scenarios before any normal client-shell execution. Search terms cannot be used as workflow IDs.

Search is **case-insensitive literal substring matching**, not fuzzy search or global synonym replacement. All
whitespace-separated query words must appear somewhere in the combined workflow ID, title, domain, purpose or curated
keywords. Multiword keywords are supported; they do not change IDs. `domain` is an exact case-insensitive domain
filter. Configured profile ceilings, lower requested profiles and `include_unavailable` apply before any result is
counted. Availability checks only whether the entry point is available, not external GDAL/TUFLOW/HY-8 binaries.

In normal mode, `list_workflows` returns workflow summaries in stable domain/title/ID order. It returns at most
**10 workflow summaries by default**; `limit` must be 1–25 and `offset` nonnegative. `workflow_count` counts all
matching workflows **before pagination**, `returned_count` counts summaries on the current workflow page, and
`has_more`/`next_offset` paginate workflows. `domains` remains the sorted list of matching domain-name strings.
The normal listing does not include the extra keyword arrays, command arrays or full scenario metadata.

With `overview=True`, the same `limit` and `offset` instead paginate **domains** in alphabetical order. The
response includes `overview: true`, total filtered `workflow_count`, total `domain_count`, the complete sorted
string list `domains`, and the current page `domain_overview`. Each domain row contains `domain`, its full filtered
`workflow_count`, and at most **three** workflow examples in stable ordering; each example contains only `id`,
`title` and `available`. The `workflows` field is omitted. `returned_count`, `has_more` and `next_offset`
describe the **domain page**. An empty query result or an offset past the last domain returns no rows while retaining
complete filtered totals. Repeat the same filters and limit with `offset=next_offset` when `has_more` is true.

Health counts and the explicitly requested full catalogue resource are never truncated. Neither the catalogue nor
script contents are preloaded into server startup instructions.

### TUFLOW result queries

Use `locations` for exact entity identifiers such as channel IDs or PO locations. On collections, use `data_types` for
case-insensitive selection such as `Q`, `H`, `POMM` or `TLF`. `minimum_time` and `maximum_time` filter the numeric `Time`
column produced by the processor, normally simulation hours; they do not filter the duration token embedded in a
filename. Use `parse_tuflow_filename` for filename duration, AEP and temporal-pattern metadata.

Samples are capped at 20 rows and collection calls at 50 files. For richer grouping, joins, specialised collection
combines or domain-specific queries, the agent should read the processor guidance resource and compose the maintained
`BaseProcessor.from_file` and `ProcessorCollection` APIs in Python. The inspection tools themselves do not write files.

## Capability profiles

Profiles are cumulative and control catalogue visibility rather than creating one MCP endpoint for every script:

| Profile | Visible workflows |
| --- | --- |
| `core` | Focused direct MCP helpers only. |
| `analysis` | Adds read-only CLI audits and inspections. |
| `create` | Adds workflows that create report, plot, archive, raster or vector outputs. This is the default. |
| `privileged` | Adds in-place, deleting or external-execution workflows. Requires explicit configuration. |

Set the maximum profile with `RYAN_MCP_PROFILE`. A tool call cannot request a profile above the configured value.
Workflows declare their lifecycle and mutation class, and privileged workflows also declare that explicit approval is
required.

The generic workflow catalogue and authoritative curated GDAL metadata are packaged with `ryan-tools`. Catalogued
workflows resolve through their existing maintained wrappers so their CLI behavior has one source of truth.
The `culvert_analysis_design` workflow exposes the maintained solve, analyse, compare, design, rating and saved-report
commands, plus a bounded or seeded `uncertainty_study` scenario, as output-creating hydraulic workflows.
Project-driven scenarios require an explicit versioned JSON or TOML path; saved-report rendering consumes existing
structured output without rerunning hydraulics.
Uncertainty discovery uses the same maintained wrapper with `--study`, `--console-log-level SUCCESS` and `--no-pause`.
The study owns selection and the evaluation bound; outputs retain excluded statuses and failures. Successfully written
studies return exit code 0; invalid CLI arguments return 2 and configuration, execution or export failures return 1.
See [culvert workflow](CULVERT_WORKFLOW.md#uncertainty-and-sensitivity-studies).

The `floodway_design_assessment` workflow exposes the maintained floodway wrapper, combining the culvert project with a
separate versioned floodway-formation JSON/TOML definition and creating scenario plus governing-envelope outputs.
Hydrograph and closure-duration processing is deliberately excluded from that workflow.

The `tuflow_culvert_evaluate_maximums` and `tuflow_culvert_evaluate_1d_nwk` workflows expose the maintained
TUFLOW culvert evaluation wrappers through the native `ryan-culverts` engine under the default `create` profile.
Their companion `tuflow_culvert_evaluate_maximums_hy8` and `tuflow_culvert_evaluate_1d_nwk_hy8` entries expose
the external HY-8 engine only under the `privileged` profile, declare `executes_external`, and require explicit
approval. Both wrappers refuse to replace an existing CSV unless `--overwrite` is supplied. Retained HY-8
workspaces are also refused when their run directory already exists unless the same explicit overwrite choice is
provided. The wrappers also accept an optional per-crossing CSV or vector/1d_nwk
attribute source for explicit material and loss values; this is resolved by the
wrapper at execution time rather than inferred from Manning roughness. The HY-8
catalogue entries retain `creates_or_replaces` as output-mutation metadata while
external execution remains the governing privileged safety classification.

## Installed scripts and development checkouts

The wheel installs **selected** `ryan-scripts` files beside `ryan_library` in `site-packages`, preserving their original relative layout. The [wheel script manifest](../ryan_library/resources/mcp/installed_scripts.json) lists the current, **provisionally selected** files (see the [outstanding individual audit](audits/2026-10-19-installed-scripts-inventory.md)); legacy and unreviewed scripts remain repository-only.
MCP locates that sibling folder automatically; users do not need a checkout or `RYAN_TOOLS_REPOSITORY_ROOT`.
The selected wheel files include Python wrappers and their required local documentation; the full source tree (including historical BAT/GMS/XCES helpers) is **not** installed automatically. To inspect or copy one installed wrapper without executing it, use `ryan-scripts list` and `ryan-scripts copy <relative-script-path> <existing-project-directory> [--dry-run]`. Existing targets require explicit `--overwrite`.
Packaging a script does not make it a supported MCP workflow: the curated catalogue and capability profiles still
control discovery. Mandatory Python dependencies, including `ryan-culverts` (imported as `culvert_solver`), are supplied
by the `ryan-tools` installation and are not probed separately for each workflow. Script availability indicates that
the CLI file exists, not that every runtime dependency or external application has been validated.
Application-specific dependencies such as QGIS, GDAL and HY-8 may require additional environment setup and are not
executed by MCP.
The server reports the installed folder as `scripts_directory` in health and workflow-list responses.

For development, an explicit `RYAN_TOOLS_REPOSITORY_ROOT` or an automatically discovered checkout takes precedence
over the installed scripts. Checkout discovery checks the package source tree, then the working directory and its parents.
If a selected checkout is missing a script, MCP reports it unavailable rather than silently running a wheel copy.
The MCP server returns commands and metadata; it does not execute scripts.
`get_workflow` also returns the selected `script_source`, `installed_distribution_version` and
`external_prerequisites` status. `check_repo_health` labels availability as
`entry_point_only` and external executable validation as `not_performed`.
A discoverable script is not proof that HY-8/TUFLOW/GDAL/QGIS binaries are present
or that its CLI works; verify those in the real application environment before running.


## Prerequisites

1. Python 3.14+ with the `ryan-tools` package installed, including `ryan-culverts`. Upgrade Python if dependency compatibility problems occur.
2. Install the MCP extra:

   ```bat
   python -m pip install ".[mcp]"
   ```

## Install both MCP servers

The recommended end-user launcher (`install-ryan-tools.bat`, also available through
the existing `install-latest-wheel.bat`) installs the newest published
`ryan-tools[mcp]` release **from PyPI**, without requiring a local wheel.
It also installs the external QGIS MCP Python bridge from upstream `main`.
If Codex is detected (CLI, existing user configuration, or the OpenAI `openai.chatgpt` VS Code extension), it automatically
adds/refreshes the QGIS and ryan-tools MCP registrations, preserving unrelated
settings and backing up changed configuration. If Codex is absent, no Codex
configuration is created.

`package_and_install.bat` and `install-local-wheel.bat` retain the
local verified-wheel workflow for developers. The QGIS bridge remains a
separate pip installation, not an embedded dependency of `ryan-tools`.
Development `requirements.txt` installs the checkout and upstream QGIS MCP.

To use the approved local QGIS MCP source instead of downloading from GitHub:

```powershell
.\install-ryan-tools.bat --qgis-mcp-source "Q:\BGER\PER\RPRT\qgis-mcp"
```

For machines without QGIS or access to the upstream source:

```powershell
.\install-ryan-tools.bat --skip-qgis-mcp
```

The developer `force-reinstall.bat` still uses a local wheel with
`--no-deps`; `install_latest_wheel.py --dependencies-only` remains a
binary GIS dependency bootstrap. Use `install-ryan-tools.bat --skip-codex`
to leave Codex untouched. Skipping QGIS MCP also skips automatic Codex
configuration; run the setup helper separately when both servers are installed.

**The QGIS desktop plugin is not installed by pip.** Install/update it in QGIS
through Plugins > Manage and Install Plugins > QGIS MCP, then start the server
from its dock. The Python bridge and QGIS plugin must be compatible.

## Set up both servers in Codex

Run this from the repository root in Windows PowerShell. The launcher selects
an available Python 3.14+ interpreter by default and discovers the checkout
relative to `repo-scripts`. For Codex running **natively on Windows**, the
user-level Codex config is shared with the extension in VS Code; restart or
reload the extension after changes.

**WSL exception:** If VS Code is configured to run Codex inside WSL, the
Windows batch installer configures the Windows Python and Codex environment,
not WSL's separate interpreter or `~/.codex/config.toml`. Set up both
servers in WSL explicitly instead and verify the active config using the
Codex sidebar's Settings > Open config.toml.

```powershell
.\repo-scripts\setup_codex_mcp.ps1 -DryRun
.\repo-scripts\setup_codex_mcp.ps1
```

To install/update the Python MCP packages at the same time, run:

```powershell
.\repo-scripts\setup_codex_mcp.ps1 -Install -QgisSource "Q:\BGER\PER\RPRT\qgis-mcp"
```

Without `-QgisSource`, `-Install` installs upstream QGIS MCP from `main`. The `-Install` option installs the checkout in editable
mode with the MCP extra. Omitting `-Install` only updates Codex registration.

The setup preserves unrelated Codex settings and MCP servers, retains QGIS
environment settings such as `QGIS_MCP_TOKEN`, refuses symlinked Codex config
files rather than replacing dotfiles-managed links, detects edits made after
reading or while staging the replacement, and backs up
`%USERPROFILE%\.codex\config.toml` before changes, and can be rerun.
Set `-Python` to override the interpreter. Existing `CODEX_HOME` is
respected by the Python utility. The default ryan-tools profile is `create`;
use `-Profile analysis` for read-only workflow discovery. Setup no longer writes a checkout path by default and removes
a previous generated `RYAN_TOOLS_REPOSITORY_ROOT`. Use `--repository-root <path>` with the Python helper or
`-RepositoryRoot <path>` with the PowerShell helper to explicitly select a development checkout.

Bytecode suppression defaults to off. Setup automatically disables `.pyc` writes when `USERDNSDOMAIN`,
`USERDOMAIN` or the host DNS suffix identifies `bge-resources.com` (including the `BGER` domain).
For other restricted environments, pass `--disable-bytecode` to `install-ryan-tools.bat` or
`setup_codex_mcp.py`, or `-DisableBytecode` to `setup_codex_mcp.ps1`.
This adds `-B` and `PYTHONDONTWRITEBYTECODE = "1"` to both MCP registrations.
Rerunning setup outside BGE without the flag removes these bytecode restrictions from both registrations;
unrelated environment settings are preserved. If domain/DNS detection is unavailable, use the explicit flag.

Restart Codex and check `codex mcp list`. In the running QGIS session,
start the plugin bridge, then invoke QGIS `ping` and `diagnose`; also run
ryan-tools `check_repo_health`.

## Running the server

After installation:

```bat
ryan-mcp
```

Or invoke the packaged module directly:

```bat
python -m ryan_library.mcp.server
```

Both forms use MCP's stdio transport.

## Codex configuration

Add this to the user's `~/.codex/config.toml`:

```toml
[mcp_servers.ryan-tools]
command = 'python'
args = ['-m', 'ryan_library.mcp.server']

[mcp_servers.ryan-tools.env]
PYTHONUTF8 = '1'
RYAN_MCP_PROFILE = 'create'
```

For an optional development checkout override, add `RYAN_TOOLS_REPOSITORY_ROOT`:

```toml
RYAN_TOOLS_REPOSITORY_ROOT = 'E:\Library\Automation\ryan-tools'
```

Restart Codex after changing MCP configuration.

On systems where Python bytecode files are prohibited, add `-B` before `-m` and set
`PYTHONDONTWRITEBYTECODE = '1'`. This is an environment-specific restriction, not the default ryan-tools behavior.

## VS Code

The repository ships `.vscode/mcp.json`. Opening the workspace registers the server with the workspace path as the
development `RYAN_TOOLS_REPOSITORY_ROOT` fallback and uses the `create` profile.

## Agent workflow

An agent should:

1. Call `list_workflows`, optionally filtering by domain.
2. Call `get_workflow` with the selected id.
3. Check `available`, `execution_kind`, `lifecycle`, `mutation`, `requires_explicit_approval` and the resolved target.
4. Run the returned `help_command` through the client shell when it is present.
5. Construct the final CLI call from the current help and returned scenarios.
6. Include the returned `required_headless_arguments` for non-interactive execution.
7. Obtain explicit user approval before privileged or replacement behavior.
8. Report the process exit code, generated outputs, warnings and errors.

The MCP server never invokes the returned command itself.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `mcp package is not installed` | Install the project with the `mcp` extra. |
| A repository workflow is unavailable | Set `RYAN_TOOLS_REPOSITORY_ROOT` to a checkout containing `pyproject.toml` and `ryan-scripts`. Installed-module workflows do not need it. |
| A workflow is unknown under `create` | It may be privileged; inspect the packaged catalogue and explicitly opt into `privileged` if appropriate. |
| A GDAL repository wrapper is unavailable | Configure the repository root. The packaged GDAL catalogue remains discoverable, but its existing wrappers require the checkout. |
| Script imports fail | Install the matching `ryan-tools` package version used by the checkout. |
| Changes do not appear in the client | Restart the MCP client so it reloads tool definitions and environment variables. |
