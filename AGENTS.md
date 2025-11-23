## AGENTS.md

This file guides AI agents (e.g., ChatGPT Codex) on how to interact with and contribute to the **ryan-tools** repository.

---

### 1. Repository Overview

* **Purpose**: A collection of Python utilities and scripts for geospatial and data processing (`ryan-tools`).
* **Root Structure**:

  ```
  ryan-tools/
  ├── src/                # Main Python package (ryan_tools)
  ├── excel-tools/        # Excel Workbooks - not code - ignore.
  ├── QGIS-Styles/        # QGIS-Styles - not code - ignore.
  ├── tests/              # Unit and integration tests. Ignore.
  ├── vendor/             # Vendored third-party code (e.g., PyHMA)
  ├── docs/               # Documentation and design notes. Missing - ignore
  ├── ryan-scripts/       # Python entry points which call the ryan-tools library
  ├── ryan-library/       # Python library with all of the functions
  ├── ryan-library/functions # Python functions used by a variety of scripts
  ├── ryan-library/scripts  # Python scripts called by wrappers, that then execute a series of functions. Most of the work should be in functions in the functions folder, this is just the controller.
  ├── ryan-functions/     # backward compatability only. Ignore. If anything uses this location, then it is out of date and needs updating.
  ├── requirements.txt    # Python dependencies
  ├── AGENTS.md           # This guidance file
  └── README.md           # Getting Started & Setup instructions
  ```

---

### 2. Coding Conventions

* **Language**: Python 3.13
* **Import Style**: Absolute imports from `ryan_tools` or vendored packages only.
* **Formatting**: Format with [Black](https://github.com/psf/black) using a 120 character line length. A
  `pyproject.toml` is provided with this configuration.
* **Type Hints**: All public functions and methods should include type annotations. Always use Python 3.13+ style.
* **Linting**: Use `pyright` for static analysis in `strict` mode (configured via `pyproject.toml`). Only run Pyright on files you have modified (e.g., `pyright ryan_library/path/to_file.py`).

---

### 3. Dependency Management

* **requirements.txt**: Primary list of `pip`-installable packages.
* **Vendoring**: Third‑party modules like `PyHMA` are placed under `vendor/` and must have an `__init__.py`.
* **pyproject.toml**: black and pyright settings

---

### 4. Testing & Validation

* Tests are generally outdated and give errors.
* Do not create tests unless specifically requested.
* Do not run tests unless you are creating them or requested by the user to run them. Generally you should only run a subset related to your work items.

---

### 5. Pull Request & Commit Guidelines

* **Commit Messages**: Use present-tense, imperative mood (e.g., `Add new rainfall utility`).
* **PR Title**: Should start with a scope: e.g., `[core] Add data validation`.
* **Description**: Summarize what, why, and any next steps or manual verification.
* **Labels**: Tag PRs with `enhancement`, `bug`, or `docs` appropriately.

---

### 6. How to Interact as an Agent

1. **Analyze requests**: Read user prompts and test failures to identify required changes.
2. **Follow conventions**: Generate code adhering to the project standards (sections 2–5).
3. **Produce PR diffs**: Only modify relevant files; include clear commit messages.

#### Logging (loguru) guidance
- Success/error/exception logs shown to users must use f-strings (or equivalent eager formatting) for clarity.
- Info logs are also user-facing; prefer f-strings or explicit formatting so rendered messages are readable as-is.
- Debug logs should remain lazily formatted (loguru parameter style) to avoid unnecessary work when debug is disabled.
- TODO: Sweep the codebase and align existing log statements with these conventions; ensure logging helpers do not leak internal helper names into user-facing output.

---

### 7. Build Workflow

* When modifying anything inside `ryan_library/` or other package metadata, run `python repo-scripts/build_library.py` from the repository root.
* The script bumps the `setup.py` version using today's date plus a daily counter and rebuilds the wheel artefact under `dist/`.
* Binary artifacts in Codex Web Viewer:
  * `.whl` files and other binary artifacts cannot be saved/committed when using the Codex web viewer. This is a platform limitation.
  * When working in the web viewer or any read‑only environment, do not attempt to commit wheel artifacts. Submit the version bump and source changes only, and add a PR note requesting a maintainer to run the build locally and commit the wheel.
  * If supported, you may run `python repo-scripts/build_library.py --skip-artifacts` to skip artifact creation; otherwise just skip committing artifacts.
* Local builds:
  * When running locally (with write access), commit the regenerated wheel under `dist/` so the published package matches the source.
  * Use `--skip-pip` if the environment already has the `build` module installed.

---

### 8. Environment Notes

* On machines joined to the `bge-resources.com` domain (e.g., where `USERDNSDOMAIN=bge-resources.com` or `USERDOMAIN=BGER`), PowerShell sometimes fails to stream file contents reliably. When working on these systems, prefer running commands through `cmd.exe` (e.g., `cmd.exe /C type path\to\file`) so files load correctly in the Codex CLI.
* CI/CLI host commonly provides system Python 3.12 with PEP 668 (externally-managed) pip. `python -m venv` may fail unless `python3-venv` is installed. If you need repo deps, install the bundled wheel under `dist/` (e.g., `python3 -m pip install --break-system-packages dist/ryan_functions-*.whl`) so `ryan_library` and loguru/geopandas/fiona are available. If isolation is required, install venv tooling first or use user-level installs.

---

*End of AGENTS.md*
