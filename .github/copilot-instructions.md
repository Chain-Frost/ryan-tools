# Copilot Instructions for ryan-tools

## Project Overview
- **ryan-tools** is a collection of Python utilities and scripts for geospatial and data processing, with a focus on TUFLOW, RORB, 12D, and GDAL workflows.
- The codebase is organized for modularity: core logic lives in `ryan-library/functions`, while `ryan-library/scripts` provides controllers, and `ryan-scripts` contains entry-point wrappers.
- Many scripts are wrappers that set up the environment and call into the main library.

## Key Directories
- `ryan-library/functions/`: Core reusable Python functions. Add new logic here.
- `ryan-library/scripts/`: Script controllers that orchestrate function calls. Keep these thin.
- `ryan-scripts/`: Entry-point scripts for end-user use that are copied to the relevant work locations. Should only handle calling into the library.
- `vendor/`: Vendored third-party code (e.g., PyHMA). Do not modify unless updating vendored code.
- `excel-tools/`, `QGIS-Styles/`: Not code—ignore for automation and code changes.
- `ryan-functions/`: Deprecated. Migrate any usage to `ryan-library`.

## Coding Conventions
- **Python 3.13** required. Use absolute imports from `ryan_library` or vendored packages only.
- Format code with [Black](https://github.com/psf/black) (120 character line length, see `pyproject.toml`).
- All public functions/methods must have type annotations (Python 3.13+ style).
- Use `mypy` for static analysis, but only on files you modify unless otherwise instructed.

## Developer Workflows
- **Build/Install**: Use `setup.py` or `requirements.txt` for dependencies. For local development, editable installs are supported.
- **Testing**: Tests are minimal. If adding tests, place them in `tests/` and follow the structure of the code under test.
- **Script Execution**: Most scripts in `ryan-scripts/` are run directly (e.g., `python ryan-scripts/TUFLOW-python/POMM-med-max-aep-dur.py`). They often change the working directory to their own location before running.
- **Logging**: Scripts typically accept a `log_level` argument and print to stdout.

## Patterns & Examples
- Controllers in `ryan-library/scripts` should delegate to functions in `ryan-library/functions`.
- Example entry-point: see `ryan-scripts/TUFLOW-python/POMM-med-max-aep-dur.py` for a typical wrapper pattern.
- Avoid duplicating logic between scripts—refactor into `functions/` as needed.

## Integration & Dependencies
- External dependencies are managed via `requirements.txt`.
- Some scripts expect specific directory structures or data files (see comments in wrappers for details).

## Special Notes
- Ignore `excel-tools/`, `QGIS-Styles/`, and `ryan-functions/` for new development.
- If you find code in `ryan-functions/`, migrate it to `ryan-library/`.
- When in doubt, prefer adding new logic to `ryan-library/functions` and keep scripts as thin as possible.

---

For more details, see `AGENTS.md` and `README.md` in the project root.
