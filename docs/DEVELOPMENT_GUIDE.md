# Development guide

This document explains how to classify, change and validate code in `ryan-tools`. It is the canonical repository-wide
reference for architecture, code lifecycle and development environments. Mandatory instructions for automated agents
remain in [`../AGENTS.md`](../AGENTS.md).

## Repository purpose

`ryan-tools` supports geospatial, hydraulic and general data-processing work, especially TUFLOW, RORB, 12D, GDAL and
QGIS workflows. It contains both maintained reusable code and practical scripts accumulated for narrower tasks. Do not
assume that every Python file has the same stability, portability or intended level of reuse.

The main users are:

- project users who edit or copy a wrapper and run it against model results;
- Python users who import reusable functions, processors or orchestrators;
- maintainers who improve shared workflows and build the `ryan-tools` package;
- QGIS, OSGeo4W and GDAL users working in application-specific environments;
- maintainers recovering useful behaviour from older standalone scripts.

## Code categories

Classify a file before changing it. If its category is unclear, inspect its module docstring, imports, callers, filename,
folder README and Git history before deciding to restructure it.

| Category | Typical location | Purpose and change policy |
| --- | --- | --- |
| Maintained library code | `ryan_library/functions`, `classes` | Reusable behaviour. Keep APIs typed, composable and independent of interactive process behaviour. |
| Processor | `ryan_library/processors` | Stateful handling of a recognised input or result format. Follow the processor factory and lifecycle documented locally. |
| Orchestrator | `ryan_library/orchestrators` | Coordinates a complete workflow: discovery, processing, logging, multiprocessing and export. Delegate reusable algorithms to functions or processors. |
| Maintained library-backed wrapper | Unversioned files under `ryan-scripts` | Human-editable entry point calling functions/processors directly, or an orchestrator if coordination is complex. Preserve editable defaults, CLI overrides, banners, working-directory handling, exit codes and optional pause. |
| Maintained standalone script | Commonly under `ryan-scripts` | Supported self-contained tool: apply the shared version, CLI, safety, path and validation principles without requiring a library split or orchestrator. |
| Legacy/project-specific standalone utility | Commonly under `ryan-scripts` | Narrow or historical code; preserve intentional behaviour and modernise only when requested and justified. |
| Project-specific script | `ryan-scripts` or a project checkout | Encodes project naming, paths, globs or output templates. Keep those choices visible rather than presenting them as universal library rules. |
| Legacy or compatibility code | Versioned scripts and explicitly retained compatibility modules | Keeps narrowly scoped old behaviour. `ryan_library/scripts` was removed on 2026-10-18; do not restore it. |
| Experimental or reference code | `ryan-scripts/python-not-polished`, files labelled accordingly | Example, incomplete or superseded material without a stability promise. Review carefully before operational use. |
| Vendored or submodule content | `vendor`, `tests/test_data`, resource and holding-area submodules | Independently maintained code or data. Do not refactor it as if it were ordinary first-party package code. |

Versioned script filenames normally identify historical standalone snapshots.
**Maintenance status does not imply an orchestrator or a library extraction.**
All maintained entry points follow the
[shared script principles](../ryan-scripts/README.md#shared-principles-for-maintained-scripts-and-wrappers).
Library-backed wrappers additionally follow
[`WRAPPER_STANDARD.md`](../ryan-scripts/WRAPPER_STANDARD.md).
Maintained self-contained scripts follow the
[standalone standard](../ryan-scripts/README.md#minimal-standard-for-maintained-standalone-scripts)
and can keep processing in one file.

## Architecture and dependency direction

Three valid execution paths are shown below. Do **not** insert unnecessary
layers just to produce the middle or bottom form:

```text
user / shell --> maintained standalone script --> external libraries / files

user / shell --> library-backed wrapper --> shared functions / processors --> external libraries / files

user / shell --> wrapper --> optional orchestrator --> shared functions / processors --> external libraries / files
```

A standalone script may handle its own focused processing and call external
dependencies directly. A library-backed wrapper may call shared functions
directly. Add an orchestrator only when multi-stage coordination is itself
substantial, reusable or independently testable; avoid layers that simply
forward arguments.

The layers have distinct responsibilities:

- Maintained entry points (standalone scripts and wrappers) own their process
  boundary: editable settings, argument parsing, working-directory selection,
  version/progress reporting, exit codes, and any interactive pause.
- Library-backed wrappers delegate reusable logic to functions or processors
  directly; modest workflow dispatch can remain in the wrapper.
- Orchestrators own substantial reusable multi-step coordination **when needed**;
  they must be callable without pausing or terminating the interpreter.
- Functions own reusable algorithms, transformations and focused I/O helpers.
- Processors own the state and lifecycle associated with supported file formats.
- Classes and configuration registries own shared domain models and authoritative metadata.

Reusable library code must not import human-facing wrappers. The removed `ryan_library.scripts`
namespace and the removed `ryan_functions` alias must not be reintroduced.

## Where new code belongs

Use these questions in order:

1. Can the maintained task be understood, configured, tested and run cleanly
   as one standalone script? Keep it self-contained and apply common safety/CLI
   conventions. Do not extract code simply to give it a library-backed label.
2. Is processing, discovery, validation or focused I/O reused by other scripts
   or Python callers? Put that shared part in `ryan_library/functions`;
   the entry point may call it directly.
3. Does the task need a stateful format-specific processor? Use or extend a
   processor only when that lifecycle adds value.
4. Is there substantial, independently reusable multi-step workflow
   coordination? Use `ryan_library/orchestrators` in that case, not for a
   one-step call or simple dispatch.
5. Keep project configuration, CLI, logging configuration, user-visible
   reporting, pause and `SystemExit` in the entry-point script.
6. Is the module only preserving a deprecated import? Keep any compatibility
   forwarding minimal; do not recreate removed legacy APIs.

For example, a project-specific TUFLOW filename pattern belongs in the script;
a generic source-path resolver is a shared function; combining many independent
model runs into a reusable pipeline may justify an orchestrator.

### Editable configuration for maintained entry points

The [common maintained-script standard](../ryan-scripts/README.md#shared-principles-for-maintained-scripts-and-wrappers)
requires **every operational CLI option** to have a user-editable default at
the top of a maintained script or wrapper, including low-frequency settings,
write/dry-run, expensive modes, logging and pausing. These are user-owned
entry-point settings, not orchestrator responsibilities. CLI overrides take
precedence even when explicitly set to `False` or `0`. Shipped values
remain safe; a deliberate edit may opt into costly behaviour without bypassing
file protection or independent confirmation.

### MCP discovery assessment

When adding or materially changing supported reusable automation, consider whether an AI agent should discover it
through the MCP integration. This is an assessment, not a requirement to expose every script or function.

- Add a maintained CLI workflow to the packaged MCP catalogue when it is
  useful across projects, has a stable headless interface and has explicit
  lifecycle, mutation and approval semantics. Point to the authoritative
  maintained script **or** wrapper, not a duplicate CLI under `ryan_library`.
- Add a focused MCP tool when a bounded, read-only or otherwise clearly controlled function provides recurring
  agent-facing value. Keep inputs typed and outputs small enough for an agent to inspect reliably.
- Add an MCP guidance resource when agents benefit more from knowing the supported Python import, processor,
  collection or source-of-truth registry than from another fixed endpoint.
- Do not automatically catalogue basic helpers, one-off scripts, project-specific wrappers, interactive utilities,
  experimental code or every public library function.

When MCP discovery changes, update the relevant packaged listing or resource, focused tests and
[`MCP_SETUP.md`](MCP_SETUP.md).

## Filesystem path conventions

Use `Path.absolute()` by default for ordinary project paths, script/working directories, file discovery and GDAL inputs/outputs. This preserves user-visible paths (including mapped network drives) and does not dereference junctions or symlinks. Resolve relative paths against their intended base directory before changing the process working directory.

Reserve `Path.resolve()` for operations that explicitly need canonical physical identity, security boundary checks, or symlink-aware containment. A lexical `.absolute()`/`.relative_to()` comparison is not a substitute for canonical containment in security checks. Do not replace physical-identity or linked-path safety checks in destructive operations; maintain or add tests for symlinks/junctions and same-file destinations. See [`../AGENTS.md`](../AGENTS.md#filesystem-paths).

## Common execution environments

The sections below define the important boundaries. See [`ENVIRONMENTS.md`](ENVIRONMENTS.md) for setup commands, VS Code
tasks, interpreter selection and environment-specific entry points.

### Repository development

Development uses the user's normal Python 3.14 installation, with the project installed through `requirements.txt`.
Users are not expected to create or activate a virtual environment. Imports should resolve to the current worktree
during development. After package changes, rebuild the package before validating a copied wrapper or another workflow
that imports the installed wheel.

### Installed package and copied wrappers

Wrappers may be copied into project folders. They must not depend on repository-relative `sys.path` changes. The matching
`ryan-tools` wheel or editable package must be installed, and the wrapper should report both its embedded version and
the installed library version.

### QGIS, OSGeo4W and GDAL

Some workflows rely on binaries, Python bindings, drivers or environment variables supplied by QGIS or OSGeo4W. Treat
those requirements as part of the workflow contract. Validation in the normal user Python installation does not prove
that an application-specific workflow can run.

### Interactive Windows use

Some scripts are designed for double-click or terminal use and intentionally show banners or pause when finished. Keep
that behaviour in wrappers. Library functions and orchestrators must remain non-interactive and return errors normally.

### Headless automation

Automation must be able to use CLI arguments, receive meaningful process exit codes and avoid interactive pauses.
Any maintained script that can pause should expose a headless `--no-pause`
or equivalent; destructive, in-place or potentially chargeable behaviour
requires explicit opt-in even in scripts with editable defaults.

## Lifecycle terms

Use these labels consistently in documentation and code reviews:

| Term | Meaning |
| --- | --- |
| Maintained | Expected to receive fixes and conform to current repository standards. |
| Stable | Has callers that should not be broken without migration or explicit agreement. |
| Project-specific | Intentionally encodes one project's assumptions and is not a general contract. |
| Experimental | May change substantially and requires review before operational use. |
| Legacy | Older implementation retained for reference or existing users; avoid extending it. |
| Deprecated | Supported temporarily while callers migrate; point users to the replacement. |
| Compatibility-only | Exists only to forward an old import or invocation to maintained behaviour. |
| Superseded | Replaced by a named maintained implementation. |
| Vendored | Maintained upstream or imported as a dependency; modify only with a clear reason. |
| Not working | Known not to be operational; do not recommend it as a usable entry point. |

## Validation by change type

Validation should be proportional to the category and risk of the change. Follow explicit task instructions when they
ask for more or less than this baseline.

| Change | Baseline validation |
| --- | --- |
| Library function, class or processor | Ruff format, strict Pyright on modified files, focused tests or a representative smoke check, then package build. |
| Orchestrator | Ruff format, strict Pyright, focused workflow validation, then package build. |
| Maintained library-backed wrapper | Ruff format/lint, strict Pyright, compilation, `--help`, focused success/failure and dry-run checks, and an installed-wheel copied-wrapper check if shared library code changed. |
| Maintained standalone script | Ruff format/lint, strict Pyright on changed maintained code, compilation, `--help` when present, and focused success/failure tests appropriate to its risk and environment; no mandatory library build or orchestration layer. |
| Legacy, archived or project-specific utility | Compilation and focused smoke check proportional to changed behaviour; do not mandate a wholesale structural migration. |
| Documentation-only change | Check links, commands, filenames and claims against current code; no Python test run is normally required. |
| Compatibility layer | Verify forwarding and deprecation behaviour without treating the legacy API as the main behavioural contract. |
| Submodule or vendored content | Validate using that component's own instructions and keep the parent repository's pinned state explicit. |

Do not run an unrelated full test suite merely as ritual. Do run focused tests when changing behaviour that has relevant
coverage, when adding tests, or when the user requests them. Tests should target active `ryan_library` APIs rather than
repository-owned deprecated wrappers.

For README-only changes, run `python repo-scripts/check_documentation.py` from the repository root. It checks the seven
repository-owned READMEs for unresolved relative Markdown links and unambiguous inline file references without entering
submodule content. Pass explicit repository-relative Markdown paths to check a different document set; add
`--links-only` for prose-heavy audits that use abbreviated source filenames.

## Data, scale and safety

- Model results may be large, numerous, located on network drives or unavailable outside their project. Avoid loading
  unnecessary full datasets into memory and preserve multiprocessing or streaming behaviour where it matters.
- Use synthetic fixtures for tests when proprietary project results cannot be committed. Do not turn one observed
  project folder hierarchy into a general contract without evidence.
- `tests/test_data`, `excel-resources`, `qgis-resources` and `unsorted` are
  submodules. The HY-8 and culvert solver libraries are published PyPI dependencies. Preserve their independent worktrees and do not assume that a parent-repository change includes their
  contents.
- The `unsorted` submodule is categorically outside automated inspection and validation. Do not initialise, traverse,
  search, lint, test or otherwise check it; select required submodules explicitly instead of using recursive checkout.
- File-management, raster and GIS scripts may overwrite, rename, delete or modify inputs. Prefer dry runs and temporary
  copies, and make destructive scope explicit.
- Windows paths, long paths, spaces, UNC paths and network locations are normal. Examples should be copy-ready for the
  environment they describe.
- Configuration registries and schemas are sources of truth. Update the registry and its consumers together rather
  than hard-coding a second list in a wrapper.

## Ongoing work and status records

Use [work tracking](WORK_TRACKING.md) as the canonical convention for concurrent projects, resumable handoffs and
review dates. Read the [work register](work/README.md) before starting repository work. Substantial work has one linked
status record with an actionable next step, validation evidence and delivery state; update it before handing work back.
The register exposes due reviews without authorizing unrelated implementation. Keep architectural policy here and
project progress in its status record; a proposed design is not adopted policy merely because it is registered.

## Sources of truth

| Subject | Canonical source |
| --- | --- |
| Human overview, setup and navigation | [`../README.md`](../README.md) |
| Mandatory agent behaviour | [`../AGENTS.md`](../AGENTS.md) |
| Architecture, code categories and validation policy | This guide |
| Environment setup, VS Code tasks and specialised runtimes | [`ENVIRONMENTS.md`](ENVIRONMENTS.md) |
| Package metadata, Python and Pyright configuration | [`../pyproject.toml`](../pyproject.toml) |
| Ruff lint and format configuration | [`../ruff.toml`](../ruff.toml) |
| Shared maintained-script principles and standalone contract | [`../ryan-scripts/README.md`](../ryan-scripts/README.md) |
| Library-backed maintained wrapper contract | [`../ryan-scripts/WRAPPER_STANDARD.md`](../ryan-scripts/WRAPPER_STANDARD.md) |
| TUFLOW processor implementation | [`../ryan_library/processors/tuflow/README.md`](../ryan_library/processors/tuflow/README.md) and local workflow documentation |
| Test fixture contents and provenance | [`../tests/test_data/README.md`](../tests/test_data/README.md) and dataset-local documentation |
| Current proposals and unfinished work | Roadmaps, audits and implementation plans; these are informative, not architectural authority |

When documents conflict, prefer the most specific canonical source above and correct or retire the stale guidance in the
same change when practical.
