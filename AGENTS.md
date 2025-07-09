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
* **Linting**: Use `mypy` for static analysis. Only run mypy on files you have modified. Do not run mypy on any other files unless the user asks you to.

---

### 3. Dependency Management

* **requirements.txt**: Primary list of `pip`-installable packages.
* **Vendoring**: Third‑party modules like `PyHMA` are placed under `vendor/` and must have an `__init__.py`.
* **pyproject.toml**: black and mypy settings

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

---

*End of AGENTS.md*
