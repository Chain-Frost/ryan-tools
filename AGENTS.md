# AGENTS.md

This file contains mandatory working instructions for automated agents contributing to `ryan-tools`. Before making an
architectural change or deciding where code belongs, read [`docs/DEVELOPMENT_GUIDE.md`](docs/DEVELOPMENT_GUIDE.md).
That guide defines the repository's code categories, environments, dependency direction, lifecycle terms and validation
matrix.

## Repository boundaries

- `ryan_library/functions`: reusable algorithms, transformations and focused I/O helpers.
- `ryan_library/processors`: stateful handlers for supported file and result formats.
- `ryan_library/orchestrators`: complete workflow coordination, including discovery, processing, logging and export.
- `ryan-scripts`: human-facing maintained wrappers, project-specific entry points and older standalone utilities.
- `ryan_library/scripts` and `ryan_functions`: deprecated import-compatibility namespaces; do not add new behaviour.
- `vendor`, `tests/test_data`, `excel-resources`, `qgis-resources` and `unsorted`: vendored content or submodules; preserve
  their independent state.

Classify the target before changing it. Do not automatically modernise a standalone, project-specific or legacy script
into library code. Reuse must be demonstrated, not inferred solely because code could technically be shared.

## Coding conventions

- Target Python 3.14 and use current Python 3.14+ annotation syntax.
- Use absolute imports from `ryan_library` or vendored packages.
- Format Python with Black using the configured 120-character line length.
- Run Pyright in strict mode only on modified Python files.
- Add type annotations to public functions and methods.
- Preserve unrelated worktree and submodule changes. Do not stage or commit unless explicitly asked.

### Logging

- Prefer Loguru parameterized formatting for dynamic values at every level, including `SUCCESS`, for example
  `logger.success("Exported {} rows to {}", row_count, output_path)`. Use a plain string for a static message.
- Do not use eager f-strings, percent formatting or `str.format()` in `DEBUG` or `TRACE` calls. If computing a diagnostic
  value is expensive, use `logger.opt(lazy=True)` with a callable; ordinary function arguments are evaluated by Python
  before Loguru receives them.
- Log visibility is controlled by sink levels, not by eager versus parameterized message formatting. Do not treat
  `SUCCESS` as a formatting exception.
- Wrappers and top-level orchestrators own configuration; reusable functions and processors only emit records.
- Use console level `SUCCESS` for concise AI/MCP output while retaining detailed records in a `DEBUG` file sink.
- Notebook logging must be safe to reconfigure when cells are rerun and must not require multiprocessing.
- Do not expose internal helper names in user-facing output.
- Run `python repo-scripts/check_loguru_formatting.py` after changing Loguru calls. See `docs/LOGGING.md` for the full
  contract.

## Architecture rules

- Keep project paths, filename prefixes, input globs, output templates and frequently edited settings in wrappers.
- Keep reusable processing and validation in `ryan_library/functions` or processors.
- Keep end-to-end workflow coordination in `ryan_library/orchestrators`.
- Keep pausing, banners, CLI parsing, working-directory selection and `SystemExit` at the wrapper process boundary.
- Library helpers and orchestrators must not pause the console or terminate the interpreter.
- Follow [`ryan-scripts/WRAPPER_STANDARD.md`](ryan-scripts/WRAPPER_STANDARD.md) for maintained library-backed wrappers.
- Do not add repository-root `sys.path` manipulation to copied wrappers; require the matching package installation.

## Testing and validation

Use the change-type matrix in [`docs/DEVELOPMENT_GUIDE.md`](docs/DEVELOPMENT_GUIDE.md#validation-by-change-type).

- Do not create tests unless the user requests them or tests are necessary to express the requested behaviour.
- Run focused tests when changing covered behaviour, when adding or modifying tests, or when the user requests them.
- Do not run an unrelated full suite by default.
- Test active `ryan_library` functions, processors and orchestrators. Deprecated compatibility APIs normally need only
  forwarding or warning checks.
- Use synthetic fixtures where proprietary project data cannot be shared.
- For documentation-only changes, verify links, commands, paths and behavioural claims against the current repository.

## Build workflow

When modifying `ryan_library` or package metadata, run from the repository root:

```powershell
python repo-scripts/build_library.py
```

The build increments the date-based package version and rebuilds the wheel in `dist`. Use `--skip-pip` when the build
dependency is already installed. In an environment that cannot save binary artifacts, use `--skip-artifacts` if
supported and state that a maintainer must rebuild the wheel locally.

After a local build, inspect Git status for version, wheel and index changes. Preserve working-tree changes and do not
leave files staged unless the user asked for staging or a commit.

## Environment notes

- Use the user's normal Python 3.14 installation. This repository does not use a virtual environment by default; do not
  instruct users to create or activate one unless they explicitly request an isolated environment.
- Install through `requirements.txt` for development, or use the bundled wheel for the normal installed-package
  workflow.
- Some QGIS, OSGeo4W and GDAL workflows require their application environment; ordinary Python validation is not a
  substitute for an environment-specific smoke check.
- On machines joined to `bge-resources.com` or the `BGER` domain, PowerShell may fail to stream file contents reliably.
  Prefer `cmd.exe /C type <path>` when this occurs.
- Headless validation must use non-interactive options such as `--no-pause` or `--dry-run` where available.

## Pull requests and commits

- Use present-tense imperative commit messages, for example `Add raster validation`.
- Prefix PR titles with a scope, for example `[core] Add data validation`.
- Summarise what changed, why it changed, validation performed and any follow-up work.
- Use `enhancement`, `bug` or `docs` labels as appropriate.
- Update documentation when user-facing behaviour, architecture or validation expectations change.

## Documentation routing

- Use [`docs/README.md`](docs/README.md) as the complete index of repository-owned Markdown. When working in a subtree,
  read the nearest `README.md` from the target path first, then use the central index to find related policy, examples,
  format references and historical context.
- Keep script-, processor-, example- and tool-specific documentation beside the files it describes. Add it to the
  central index rather than relocating it solely for discoverability.
- Treat roadmaps, audits and implementation plans as historical or proposed work, not current policy, unless a
  canonical guide explicitly adopts their decisions.
- Human introduction and setup: [`README.md`](README.md)
- Repository architecture and development decisions: [`docs/DEVELOPMENT_GUIDE.md`](docs/DEVELOPMENT_GUIDE.md)
- Documentation index and discovery routes: [`docs/README.md`](docs/README.md)
- Script selection and safety: [`ryan-scripts/README.md`](ryan-scripts/README.md)
- Maintained wrapper rules: [`ryan-scripts/WRAPPER_STANDARD.md`](ryan-scripts/WRAPPER_STANDARD.md)
- Area-specific contracts: the nearest README or workflow document in that subtree

Plans, audits and roadmaps describe proposed or unfinished work. Do not treat them as current architectural policy when
they conflict with the canonical sources above.
