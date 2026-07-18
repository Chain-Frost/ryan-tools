# AGENTS.md

This file guides AI agents (e.g., ChatGPT Codex) on how to interact with and contribute to the **ryan-tools** repository.

---

## 1. Repository Overview

* **Purpose**: A collection of Python utilities and scripts for geospatial and data processing (`ryan-tools`).
* **Root Structure**:

  ``` text
  ryan-tools/
  ├── ryan_library/           # Main Python package
  │   ├── functions/          # Reusable workflow logic
  │   └── scripts/            # Compatibility controllers and orchestrators
  ├── ryan-scripts/           # Human-facing wrappers and standalone entry points
  ├── ryan_functions/         # Deprecated import-compatibility package
  ├── tests/                  # Unit and integration tests
  │   └── test_data/          # Required test-data submodule
  ├── vendor/                 # Vendored PyHMA code and the run_hy8 submodule
  ├── unsorted/               # Separate holding-area submodule
  ├── excel-tools/            # Excel workbooks managed by Git LFS; not code
  ├── QGIS-Styles/            # QGIS resources; not code
  ├── repo-scripts/           # Repository build and maintenance scripts
  ├── requirements.txt        # Editable development-install entry point
  ├── pyproject.toml          # Black and Pyright configuration
  ├── AGENTS.md               # This guidance file
  └── README.md               # Getting started and repository overview
  ```

---

### 2. Coding Conventions

* **Language**: Python 3.14
* **Import Style**: Absolute imports from `ryan_library` or vendored packages only.
* **Formatting**: Format with [Black](https://github.com/psf/black) using a 120 character line length. A
  `pyproject.toml` is provided with this configuration.
* **Type Hints**: All public functions and methods should include type annotations. Always use Python 3.14+ style.
* **Linting**: Use `pyright` for static analysis in `strict` mode (configured via `pyproject.toml`). Only run Pyright on files you have modified (e.g., `pyright ryan_library/path/to_file.py`).

---

### 3. Dependency Management

* **requirements.txt**: Installs the project and its development extra in editable mode.
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
4. **Structure**: Keep heavy logic/parsing in `ryan_library/functions`; use `ryan_library/scripts` as orchestrators/controllers that wire functions together and handle I/O/logging; wrappers in `ryan-scripts` should call into these orchestrators.

#### Logging (loguru) guidance

* Success/error/exception logs shown to users must use f-strings (or equivalent eager formatting) for clarity.

* Info logs are also user-facing; prefer f-strings or explicit formatting so rendered messages are readable as-is.
* Debug logs should remain lazily formatted (loguru parameter style) to avoid unnecessary work when debug is disabled.
* TODO: Sweep the codebase and align existing log statements with these conventions; ensure logging helpers do not leak internal helper names into user-facing output.

---

### 7. Build Workflow

* When modifying anything inside `ryan_library/` or other package metadata, run `python repo-scripts/build_library.py` from the repository root.
* The script bumps the `pyproject.toml` version using today's date plus a daily counter and rebuilds the wheel artefact under `dist/`.
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
* The package and bundled `run_hy8` component require Python 3.14. Prefer a persistent virtual environment over modifying an externally managed system Python. If you need repo dependencies, install the bundled wheel under `dist/` or install the project in editable mode with its `dev` extra.

End of AGENTS.md
