# Repository improvement roadmap

## Scope

This roadmap follows the July 2026 test-data and Git LFS migration. The ordinary Git repository is now small,
so further work prioritises maintainability over additional storage splitting.

GitHub Actions are out of scope because GitHub is currently used as repository storage rather than as an
automated build or test service. Extracting QGIS resources is deliberately scheduled after the code and
packaging work below. Excel workbooks remain in the main repository under Git LFS.

## Completed: repository hygiene

- Align Black and Pyright with the documented Python 3.13 development target.
- Point Pyright at the real `ryan-scripts` directory.
- Remove the stale PyHMA submodule declaration while retaining its vendored package.
- Correct repository paths and static-analysis guidance in agent documentation.
- Stop tracking generated coverage output.

## Completed: package metadata and build workflow

Package metadata and dependencies now have one authoritative source in `pyproject.toml`. The build utility reads
and updates the project version there, and `setup.py` remains only as a minimal compatibility entry point.

The migration:

1. Preserved the `ryan_functions` distribution name and the `ryan_library` import package.
2. Preserved all 149 wheel paths and all dependency metadata from the previous build.
3. Verified all 145 non-metadata payload files byte-for-byte after normalising Windows and WSL line endings.
4. Added the documented `--skip-artifacts` version-bump mode to `repo-scripts/build_library.py`.
5. Retained QGIS resources in the wheel until the later QGIS repository decision is implemented.

## Completed: Windows installer consolidation

One typed Python utility now selects and installs the latest wheel. Normal mode retains the geospatial binary
wheel handling, and force-reinstall mode deliberately avoids dependency changes.

The supported workflow is:

1. One Python build implementation in `repo-scripts/build_library.py`.
2. One Python install implementation in `repo-scripts/install_latest_wheel.py`.
3. `install-latest-wheel.bat` for normal Windows installation.
4. Thin compatibility and force-reinstall wrappers around that implementation.
5. Thin build-and-install wrappers that stop immediately when the build fails.

The obsolete `installer-if-no-batch.py` was removed because it searched for a source archive that the build no
longer creates. Normal, force, and legacy-wrapper command construction was verified through Windows `cmd.exe`
in dry-run mode without changing the installed environment.

## Then: script triage

The domain folders under `ryan-scripts` are useful and should remain. Cleanup should concentrate on
`misc-python`, `python-not-polished`, and standalone Python files at the repository root.

For each script:

1. Identify whether it is actively used, retained only as a reference, or obsolete.
2. Move reusable parsing and processing into `ryan_library/functions`.
3. Keep orchestration in `ryan_library/scripts` and human-facing wrappers in `ryan-scripts`.
4. Delete genuine duplicates rather than creating more compatibility wrappers.
5. Process one workflow family per commit so behaviour changes remain reviewable.

## Then: test collection and documentation

- Resolve the existing `run_hy8`, `tkinter`, and `tuflow_logsummary` collection failures.
- Preserve `tests/test_data` as the only permissible test-data location.
- Distinguish fast tests from environment-dependent integration tests without introducing alternate data paths.
- Reduce the root README to setup, repository orientation, and common entry points.
- Move detailed architecture, testing, migration, and MCP material under `docs/` with links from the README.

## Later: QGIS resources repository

Create a separate QGIS resources repository only after the preceding component boundaries are stable. It should
contain styles, layouts, processing models, preview images, and resource-specific documentation. The main
repository can then reference it at one fixed path as a submodule if the resources are required alongside the
code.

Before extraction, inventory cross-references from scripts and documentation, decide whether QGIS processing
models currently under `excel-tools` belong with the QGIS resources, and preserve meaningful resource history.
Do not create a workspace repository until this component repository is stable.
