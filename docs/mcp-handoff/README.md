# ryan-tools MCP workflow-target implementation

## Status

The workflow-target work described by the original MCP handoff was reviewed on 26 August 2026. The registry now uses
explicit execution targets, and all current catalogue workflows resolve through their existing maintained wrappers.
This avoids duplicating CLI parsing in package modules. Focused MCP helpers continue using maintained processors and
functions directly without a repository workflow command.

Canonical repository policy remains in [`AGENTS.md`](../../AGENTS.md) and
[`docs/DEVELOPMENT_GUIDE.md`](../DEVELOPMENT_GUIDE.md). MCP setup and user configuration are documented in
[`docs/MCP_SETUP.md`](../MCP_SETUP.md).

## User intent

The MCP server is a discovery layer. It helps an agent select an existing workflow and returns the command needed to
run that workflow through the agent's shell. It does not expose every script as an MCP tool and does not execute
catalogued workflows itself.

- Keep the MCP endpoint surface small and expose workflows through cumulative capability profiles.
- Use `create` as the default profile.
- Prefer one maintained CLI entry point for each workflow. Do not duplicate a functional wrapper under the package
  merely to avoid repository discovery.
- Retain explicit execution-target support for a future package module or console script with a distinct justified use.
- Do not restore compatibility endpoints or the deleted `ryan_mcp_server` module.
- Keep privileged workflows hidden unless the server is explicitly configured with the `privileged` profile.

## Implemented architecture

The MCP implementation is under `ryan_library/mcp`:

- `models.py` defines capability profiles, mutation classes, execution kinds and catalogue models.
- `registry.py` loads packaged workflow metadata, resolves execution targets and constructs command arrays without
  executing them.
- `server.py` registers the focused MCP tools and the read-only catalogue resource.
- `ryan_library/resources/mcp/workflows.json` contains generic workflow records.
- `ryan_library/resources/mcp/gdal_cli_tools.json` is the packaged authoritative GDAL catalogue.

### Explicit execution targets

Every workflow defines exactly one target:

| Field | Command prefix | Availability |
| --- | --- | --- |
| `module` | `[sys.executable, "-m", module_name]` | The module is import-discoverable. |
| `script` | `[sys.executable, resolved_checkout_path]` | The configured checkout contains the script. |
| `console_script` | `[resolved_executable]` | The executable is available on `PATH`. |

The registry does not guess target type from a path-like string. `get_workflow` reports `execution_kind`,
`execution_target`, resolved command arrays and target-specific availability information.

### Existing wrappers remain authoritative

`tuflow_log_summary` resolves to
`ryan-scripts/TUFLOW-python/log_processing/create_log_summary_report.py`. The wrapper already owns the full headless
CLI, while `inspect_tuflow_result` and `inspect_tuflow_collection` provide generic processor-backed inspection through
the focused MCP surface. A second `ryan_library.cli.tuflow_log_summary` entry point would therefore duplicate behavior
without adding a capability.

The same policy applies to `create_flood_extents`, which continues to resolve to the maintained
`ryan-scripts/gdal-python/gdal_flood_extent.py` wrapper.

### Repository fallback and relocated scripts

Unmigrated workflows retain explicit `script` targets. Repository discovery is optional and checks:

1. An explicit root supplied to `WorkflowRegistry`.
2. `RYAN_TOOLS_REPOSITORY_ROOT`.
3. The source checkout containing the imported package.
4. The current directory and its parents.

The TUFLOW catalogue paths were updated after commit `bb1ac6c` grouped maintained wrappers into
`culvert_results`, `gis_processing`, `log_processing`, `model_management`, `po_and_timeseries` and
`raster_processing`. Catalogue paths must use these maintained locations rather than the former flat filenames.

Without a checkout, focused MCP helpers remain usable. Catalogue metadata remains visible with
`include_unavailable=true`, while workflow commands are reported unavailable with an actionable error from
`get_workflow`.

## Packaged GDAL metadata

The authoritative GDAL catalogue moved from `ryan-scripts/gdal-python` into package resources so discovery does not
depend on a mapped drive or checkout. Each GDAL record continues to preserve defaults, scenarios, mutation metadata,
explicit-approval requirements and wrapper versions. GDAL commands resolve to their maintained repository wrappers.
Do not add duplicate package CLI modules solely to avoid repository discovery; migrate a GDAL target only when there is
a clear single-entry-point design that does not duplicate its existing wrapper.

## Profiles and safety

Profiles remain cumulative:

| Profile | Purpose |
| --- | --- |
| `core` | Focused direct MCP helpers only. |
| `analysis` | Read-only CLI audits and inspections. |
| `create` | Workflows that create outputs; this is the default. |
| `privileged` | In-place changes, deletion or external execution requiring explicit configuration. |

A tool input cannot elevate beyond `RYAN_MCP_PROFILE`. Module and script targets use the same profile filtering.
Installing a module does not make a privileged workflow visible under `create`.

## Configuration

The normal installed configuration does not require a repository root:

```toml
[mcp_servers.ryan-tools]
command = "python"
args = ["-B", "-m", "ryan_library.mcp.server"]

[mcp_servers.ryan-tools.env]
PYTHONDONTWRITEBYTECODE = "1"
PYTHONUTF8 = "1"
RYAN_MCP_PROFILE = "create"
```

`-B` and `PYTHONDONTWRITEBYTECODE` are optional environment controls. Add `RYAN_TOOLS_REPOSITORY_ROOT` only when
repository-backed workflows are needed during migration or source-checkout development.

## TUFLOW processor query surface

`inspect_tuflow_result` selects the concrete processor with `BaseProcessor.from_file`; it is not a TLF-only adapter.
It accepts exact locations/entities and optional numeric bounds on the processed `Time` column. The collection tool
adds successfully processed, non-empty results to `ProcessorCollection`, supports case-insensitive data-type selection,
reports duplicates and per-file failures, and can return a capped combined sample. Calls are limited to 50 files and 20
sample rows.

The `ryan-tools://guidance/tuflow-processors` resource directs agents to the processor factory,
`ProcessorCollection`, the suffix/data-type registry and relevant source paths. Advanced grouping, joins, specialised
combines and domain-specific queries should be composed with those maintained Python APIs instead of expanding the MCP
server into an unrestricted dataframe interface. Processing remains read-only until an export or write API is called.

## Validation contract

Focused automated coverage includes:

- repository workflows being unavailable without repository discovery;
- `sys.executable` repository-script command and help prefixes;
- current relocated repository-script fallback paths;
- unavailable fallback scripts not hiding packaged discovery metadata;
- profile filtering for repository targets;
- privileged TUFLOW and GDAL workflows remaining hidden under `create`;
- packaged GDAL metadata discovery without a checkout;
- exact-one-target catalogue validation.
- generic TUFLOW processor selection, location/time filtering, collection data-type filtering, bounded samples and
  processor guidance-resource validity.

After modifying the MCP library, CLI entry points or package metadata, run Black, strict Pyright on modified Python
files, focused MCP tests, both installed CLI `--help` checks, the documentation checker and
`python repo-scripts/build_library.py`. Validate the built wheel from a neutral directory so the checkout cannot satisfy
imports accidentally. Confirm that packaged catalogue metadata loads, repository workflows remain unavailable, and
`importlib.util.find_spec("ryan_mcp_server")` returns `None`.

### Validation completed on 26 August 2026

- Black check: passed for all modified Python files.
- Strict Pyright: 0 errors and 0 warnings.
- Focused MCP tests: 22 passed; pytest retained the existing unknown `cache_dir` configuration warning.
- Documentation index/link checker: passed.
- Loguru formatting policy checker: passed.
- Both source and neutral installed-wheel `--help` checks: passed.
- `python repo-scripts/build_library.py`: passed and produced version `26.8.26.10`.
- Neutral wheel install with repository discovery disabled: catalogue metadata loaded and zero workflow commands were
  available, while generic Q-processor location/time filtering and the processor guidance resource worked as expected.
- Neutral `find_spec("ryan_mcp_server")`: returned `None`.

## Remaining staged migration

- Review additional maintained wrappers in small domain batches before changing execution targets.
- Do not mechanically package every repository script; exclude interactive, project-specific, experimental or unsafe
  candidates until their contracts are reviewed.
- Assess new supported reusable scripts and functions for workflow-catalogue, focused-tool or guidance-resource value,
  but do not list basic helpers or every public API automatically.
- Keep project paths, globs and editable output templates in repository wrappers.
- Continue moving reusable processing into orchestrators/functions rather than duplicating it in installed CLIs.
- Retain `RYAN_TOOLS_REPOSITORY_ROOT` for repository-backed workflow discovery.
