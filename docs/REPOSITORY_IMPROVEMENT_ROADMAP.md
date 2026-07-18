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

## Next: package metadata and build workflow

Package metadata currently lives in `setup.py`, while `pyproject.toml` contains only tool configuration. The
build utility also edits the version directly in `setup.py`, so these files must be migrated together.

The package change should:

1. Make `pyproject.toml` the single source of package metadata and dependencies using the standard `[project]`
   table.
2. Update `repo-scripts/build_library.py` to read and update the version from that same file.
3. Retain a minimal `setup.py` temporarily only if an existing local workflow still requires it.
4. Preserve the `ryan_functions` distribution name and the `ryan_library` import package during this change;
   renaming either would be a separate compatibility migration.
5. Build the wheel and compare its package contents with the currently published wheel before replacing it.

## Next: Windows installer consolidation

There are several overlapping root-level batch files. `installer-if-no-batch.py` is already incompatible with
the current wheel-only build because it searches for a `ryan_functions-*.tar.gz` archive.

The supported workflow should become:

1. One Python build implementation in `repo-scripts/build_library.py`.
2. One normal Windows install script for the newest wheel in `dist/`.
3. One explicitly named force-reinstall script for recovery use.
4. Thin convenience wrappers for build-and-install and build-and-force-reinstall.
5. Removal of obsolete installers only after their replacements have been exercised on Windows.

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
