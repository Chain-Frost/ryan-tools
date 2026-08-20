# Project documentation index

This is the central discovery index for repository-owned Markdown. It links both repository-wide documents under
`docs/` and specialised guides that intentionally live beside the scripts, processors, examples or automation they
describe. User-facing setup and introductory navigation remain in the root [`README.md`](../README.md).

## How to find the relevant document

Use this order when investigating or changing the repository:

1. Start from the target file or directory and read the nearest `README.md` on the path back toward the repository
   root. A nearby guide contains the most specific workflow contract.
2. Use the topic tables below to find related repository-wide policy, environment requirements, examples and
   maintenance context.
3. Treat canonical policy as authoritative for repository-wide rules. A nearby guide may add stricter or more specific
   requirements for its subtree, but it must not override canonical policy.
4. Treat roadmaps, audits and implementation plans as historical or proposed work unless a canonical guide explicitly
   adopts their decisions.
5. For a Git submodule, use that submodule's own documentation and Git history. This index covers the parent repository
   and does not inventory documentation inside submodules.

Useful discovery commands from the repository root are:

```powershell
git ls-files "*.md"
rg -n -i "search terms" -g "*.md" -g "!unsorted/**" -g "!vendor/run_hy8/**"
```

## Canonical policy and starting points

| Document | Use it when | Authority |
| --- | --- | --- |
| [Repository README](../README.md) | Setting up the checkout, choosing a workflow or understanding the repository map | User-facing entry point |
| [Agent instructions](../AGENTS.md) | Making any automated change in this repository | Mandatory contributor instructions |
| [Development guide](DEVELOPMENT_GUIDE.md) | Deciding where code belongs, interpreting lifecycle labels or selecting validation | Canonical architecture and development policy |
| [Development and execution environments](ENVIRONMENTS.md) | Selecting Python, VS Code, installed-wheel, QGIS/OSGeo4W or headless execution | Canonical environment guide |
| [Logging guide](LOGGING.md) | Changing Loguru configuration, message formatting, multiprocessing or notebook logging | Canonical logging contract |
| [Ryan Scripts guide](../ryan-scripts/README.md) | Choosing and safely running human-facing wrappers or standalone scripts | Canonical script-selection guide |
| [Maintained wrapper standard](../ryan-scripts/WRAPPER_STANDARD.md) | Creating or changing a maintained library-backed wrapper | Canonical maintained-wrapper contract |

## Local workflow and implementation guides

These documents stay near the code they describe so that browsing a target directory exposes its local contract.

| Area | Local document | Relevant work |
| --- | --- | --- |
| TUFLOW processors | [Processor development notes](../ryan_library/processors/tuflow/README.md) | Processor lifecycle, collections, combinations, filtering, caching and extension |
| TUFLOW wrappers | [TUFLOW Python wrappers](../ryan-scripts/TUFLOW-python/README.md) | Maintained entry points, shared CLI behavior and wrapper selection |
| GDAL wrappers | [GDAL Python wrappers](../ryan-scripts/gdal-python/README.md) | Maintained GDAL workflows, recipes, mutation risks and automation catalogue |
| Legacy GDAL | [Legacy GDAL batch files](../ryan-scripts/gdal-bat/README.md) | Mapping retained BAT entry points to maintained Python replacements |
| Examples | [Examples index](../examples/README.md) | Choosing direct library examples instead of wrappers |
| TUFLOW examples | [TUFLOW API examples](../examples/tuflow/README.md) | Filename parsing, processors, batch loading, PO checks and POMM summaries |
| DataFrame examples | [DataFrame and export examples](../examples/dataframes/README.md) | Table assembly and Excel/Parquet export helpers |
| Rock-protection lookup data | [Outlet rock-protection data](../ryan_library/functions/data/README.md) | Source, interpretation and limits of the bundled multi-pipe lookup tables |
| Maintenance benchmarks | [Benchmark guide](../repo-scripts/benchmarks/README.md) | Running and interpreting file-collection or DataFrame backend benchmarks |

## Integrations

| Document | Relevant work |
| --- | --- |
| [MCP server setup](MCP_SETUP.md) | Configuring the repository's MCP tools and GDAL discovery in supported AI clients |

## File-format references

| Document | Relevant work |
| --- | --- |
| [Observed binary Surpac STR format](SURPAC_BINARY_STR_FORMAT.md) | STR record layout, validation rules, converter behavior and known limits |
| [Observed binary Surpac DTM format](SURPAC_BINARY_DTM_FORMAT.md) | DTM mesh blocks, triangle topology, validation evidence and known limits |

These references distinguish externally documented facts, repository observations, inferences and remaining limits.
Keep additional empirical format notes discoverable here even when their implementation lives elsewhere.

## Maintenance, migration and historical plans

These documents describe inventories, completed work, proposed work or unfinished migrations. They are useful context,
but they are not current architectural policy when they conflict with the canonical guides above.

| Document | Scope and status |
| --- | --- |
| [Compatibility policy and inventory](COMPATIBILITY_POLICY.md) | Compatibility namespaces, migration state and deprecation checklist |
| [Repository improvement roadmap](REPOSITORY_IMPROVEMENT_ROADMAP.md) | Repository-wide improvement milestones and remaining opportunities |
| [Unsorted upgrade roadmap](UNSORTED_UPGRADE_ROADMAP.md) | Disposition and review status of scripts migrating from the `unsorted` submodule |
| [README improvement implementation plan](../implementation_plan.md) | Implemented 8 August 2026 README audit and its recorded reasoning |
| [Logging pipeline implementation plan](audits/2026-08-08-logging-pipeline-implementation-plan.md) | Dated logging implementation outcome and supporting audit |
| [`ryan_library` lifecycle audit and implementation plan](audits/2026-08-08-ryan-library-lifecycle-plan.md) | Dated lifecycle inventory and proposed migration work |

## Contribution and review documents

| Document | Relevant work |
| --- | --- |
| [Code-review instructions](../.github/code_review_instructions.md) | Reviewing repository changes and reporting blocking versus non-blocking findings |
| [Pull-request template](../.github/pull_request_template.md) | Recording summary, validation and review checklist details for a pull request |

## Where new Markdown belongs

- Put repository-wide architecture, environments, logging, setup or cross-cutting maintenance guidance under `docs/`.
- Put script-, processor-, example- or tool-specific guidance beside the files it describes. Do not move a useful local
  README into `docs/` merely to centralise it.
- Put dated audits and implementation plans under `docs/audits/`. Clearly label their date and whether the work is
  proposed, active, complete or superseded.
- Keep the root README concise and user-facing. Keep mandatory automation rules in `AGENTS.md`.
- Add every new repository-owned Markdown document to the appropriate table in this index and link it from the nearest
  parent README when that improves local navigation.
- Prefer one authoritative explanation with links from other documents. Avoid copying detailed inventories into
  multiple READMEs because they drift independently.

Run `python repo-scripts/check_documentation.py` after changing the documentation structure. Its default run checks the
main README links and paths and verifies that every tracked Markdown document is linked from this index.
