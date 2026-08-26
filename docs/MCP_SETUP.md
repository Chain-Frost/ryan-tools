# MCP Server Setup — ryan-tools

The **ryan-tools** MCP server gives AI agents a small set of focused inspection tools and a staged catalogue of
repository CLI workflows. The MCP server discovers scripts and returns safe command arrays; it does not execute the
catalogued scripts. The agent runs a selected script through its normal shell after reviewing the script's current
`--help`, input and output paths, mutation metadata and approval requirements.

## Default tools

The default MCP surface stays deliberately small:

| Tool | Description |
| --- | --- |
| `parse_tuflow_filename` | Parse a TUFLOW output filename into structured components. |
| `inspect_tuflow_result` | Process one supported TUFLOW result or log, with optional location and processed-time bounds, and return bounded metadata and sample rows. |
| `inspect_tuflow_collection` | Process up to 50 supported TUFLOW files and query their `ProcessorCollection` by location, data type and processed-time bounds. |
| `inspect_raster_metadata` | Inspect raster dimensions, resolution, CRS, transform, bounds, bands and NoData metadata. |
| `check_repo_health` | Report package versions, configured profile, checkout discovery and catalogue status. |
| `list_workflows` | Discover CLI workflows enabled by the configured profile, optionally filtered by domain. |
| `get_workflow` | Resolve one workflow's installed module or repository script, help command, scenarios and safety metadata. |

The complete visible catalogue is also available as the read-only resource
`ryan-tools://workflows/catalogue`. The read-only `ryan-tools://guidance/tuflow-processors` resource tells an agent
which Python processor factory, collection methods, registry and source files to use for queries beyond the bounded MCP
tools.

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

The generic workflow catalogue and authoritative GDAL metadata are packaged with `ryan_functions`. Catalogued workflows
resolve through their existing maintained repository wrappers so their CLI behavior has one source of truth.

## Repository workflow resolution

Catalogued workflows target human-facing repository scripts. The server resolves a checkout in this order:

1. The explicit `RYAN_TOOLS_REPOSITORY_ROOT` environment variable.
2. The source tree containing the imported package during repository development.
3. The current directory or one of its parents.

When no checkout is found, the MCP server still starts and its focused direct helpers remain available. Catalogued
workflows are reported as unavailable, while their packaged discovery metadata remains inspectable. Health and
workflow-list responses explain how to configure the root.

## Prerequisites

1. Python 3.14+ with the `ryan_functions` package installed.
2. Install the MCP extra:

   ```bat
   python -m pip install ".[mcp]"
   ```

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

During the staged migration, add `RYAN_TOOLS_REPOSITORY_ROOT` when repository-only workflows are needed:

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
| Script imports fail | Install the matching `ryan_functions` package version used by the checkout. |
| Changes do not appear in the client | Restart the MCP client so it reloads tool definitions and environment variables. |
