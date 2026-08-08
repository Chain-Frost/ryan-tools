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
- maintainers who improve shared workflows and build the `ryan_functions` package;
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
| Maintained wrapper | Unversioned files under `ryan-scripts` | Human-editable entry point around library behaviour. Preserve editable defaults, CLI overrides, banners, working-directory handling, exit codes and optional pause. |
| Standalone utility | Commonly under `ryan-scripts` | A useful but narrow script that may reasonably remain self-contained. Improve it proportionally; do not move it into the library without a demonstrated reuse case. |
| Project-specific script | `ryan-scripts` or a project checkout | Encodes project naming, paths, globs or output templates. Keep those choices visible rather than presenting them as universal library rules. |
| Legacy or compatibility code | `ryan_functions`, `ryan_library/scripts`, versioned scripts | Preserves old imports or behaviour. Do not add new features here; migrate active callers to maintained APIs. |
| Experimental or reference code | `ryan-scripts/python-not-polished`, files labelled accordingly | Example, incomplete or superseded material without a stability promise. Review carefully before operational use. |
| Vendored or submodule content | `vendor`, `tests/test_data`, resource and holding-area submodules | Independently maintained code or data. Do not refactor it as if it were ordinary first-party package code. |

Versioned script filenames normally identify standalone snapshots. Maintained library-backed wrappers use descriptive,
unversioned filenames and an embedded wrapper version. See [`../ryan-scripts/WRAPPER_STANDARD.md`](../ryan-scripts/WRAPPER_STANDARD.md).

## Architecture and dependency direction

The preferred dependency direction is:

```text
user, shell or project configuration
                |
                v
      ryan-scripts wrapper
                |
                v
  ryan_library.orchestrators
                |
                v
functions / processors / classes
                |
                v
 external libraries and project files
```

The layers have distinct responsibilities:

- Wrappers own presentation and process-boundary behaviour: editable defaults, argument parsing, working-directory
  selection, user banners, exit codes and interactive pausing.
- Orchestrators own workflow sequencing and coordination. They must be callable without pausing or terminating the
  interpreter.
- Functions own reusable algorithms, transformations and focused I/O helpers.
- Processors own the state and lifecycle associated with supported file formats.
- Classes and configuration registries own shared domain models and authoritative metadata.

Reusable library code must not import human-facing wrappers. New code must not use the deprecated
`ryan_library.scripts` or `ryan_functions` compatibility namespaces.

## Where new code belongs

Use these questions in order:

1. Does it coordinate a complete existing workflow? Put it in an appropriate orchestrator.
2. Is it reusable processing, discovery, validation or export behaviour? Put it in `ryan_library/functions`.
3. Does it represent and process a supported data format with state? Add or extend a processor.
4. Is it project configuration or a convenient human entry point? Keep it in a wrapper.
5. Is it genuinely a narrow, single-purpose task with no expected reuse? A documented standalone script is acceptable.
6. Is it only preserving an old import or invocation? Keep the compatibility layer minimal and add no new behaviour.

For example, a TUFLOW output filename pattern or project prefix belongs in a wrapper; generic discovery and validation
belong in the library; coordinating discovery, aggregation and workbook export belongs in an orchestrator.

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
`ryan_functions` wheel or editable package must be installed, and the wrapper should report both its embedded version and
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
Maintained wrappers should expose `--no-pause`; destructive or in-place behaviour should require an explicit choice or
confirmation.

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
| Library function, class or processor | Black, strict Pyright on modified files, focused tests or a representative smoke check, then package build. |
| Orchestrator | Black, strict Pyright, focused workflow validation, then package build. |
| Maintained wrapper | Black, strict Pyright, compilation, `--help`, and relevant success/failure or dry-run behaviour; use the wrapper checklist. |
| Standalone or project-specific script | Compilation and a focused smoke check proportional to its risks and environment. |
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
- `tests/test_data`, `excel-resources`, `qgis-resources`, `unsorted` and parts of `vendor` are submodules. Preserve their
  independent worktrees and do not assume that a parent-repository change includes their contents.
- File-management, raster and GIS scripts may overwrite, rename, delete or modify inputs. Prefer dry runs and temporary
  copies, and make destructive scope explicit.
- Windows paths, long paths, spaces, UNC paths and network locations are normal. Examples should be copy-ready for the
  environment they describe.
- Configuration registries and schemas are sources of truth. Update the registry and its consumers together rather
  than hard-coding a second list in a wrapper.

## Sources of truth

| Subject | Canonical source |
| --- | --- |
| Human overview, setup and navigation | [`../README.md`](../README.md) |
| Mandatory agent behaviour | [`../AGENTS.md`](../AGENTS.md) |
| Architecture, code categories and validation policy | This guide |
| Environment setup, VS Code tasks and specialised runtimes | [`ENVIRONMENTS.md`](ENVIRONMENTS.md) |
| Package metadata, Python and tool configuration | [`../pyproject.toml`](../pyproject.toml) |
| Maintained wrapper contract | [`../ryan-scripts/WRAPPER_STANDARD.md`](../ryan-scripts/WRAPPER_STANDARD.md) |
| Script selection and operational safety | [`../ryan-scripts/README.md`](../ryan-scripts/README.md) |
| TUFLOW processor implementation | [`../ryan_library/processors/tuflow/README.md`](../ryan_library/processors/tuflow/README.md) and local workflow documentation |
| Test fixture contents and provenance | [`../tests/test_data/README.md`](../tests/test_data/README.md) and dataset-local documentation |
| Current proposals and unfinished work | Roadmaps, audits and implementation plans; these are informative, not architectural authority |

When documents conflict, prefer the most specific canonical source above and correct or retire the stale guidance in the
same change when practical.
