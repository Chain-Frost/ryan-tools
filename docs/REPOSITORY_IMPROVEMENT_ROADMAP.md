# Repository improvement roadmap

## Scope

This roadmap follows the July 2026 test-data and Git LFS migration. The next structural change is to separate
QGIS and Excel resources from the Python code while retaining convenient checkouts inside `ryan-tools`.

GitHub Actions are out of scope because GitHub is currently used as repository storage rather than as an
automated build or test service. The resource repositories will use Git submodules, matching the established
`tests/test_data`, `unsorted`, and `vendor/run_hy8` pattern.

## Completed: repository hygiene

- Align Black and Pyright with the documented Python 3.14 development target.
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
5. Retained QGIS resources in the wheel pending the resource-repository extraction described below.

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

## Completed: QGIS and Excel resource repositories

Two lowercase, hyphenated repositories are loaded back into `ryan-tools` as Git submodules:

| Repository and checkout path | Content |
| --- | --- |
| `qgis-resources/` | Styles and layouts formerly under `QGIS-Styles/`, utilities formerly under `ryan-scripts/pyQGIS/`, ten QGIS `.model3` processing models, and the supporting `TUFLOW culverts.xlsx` workbook |
| `excel-resources/` | The other eight Excel workbooks formerly under `excel-tools/` plus `format to n sig figs in Excel.txt` |

Purpose-based folders replace the misleading source paths. QGIS assets use `styles/`, `processing-models/`, and
`scripts/`; the QGIS-dependent workbook is beside its models under `supporting-workbooks/`. General Excel assets
use `workbooks/` grouped by workflow.

Both repositories were created from dedicated `ryan-tools` clones filtered to their selected paths. Path renames
were applied during filtering, preserving original edits, authorship, dates, and messages rather than replacing
the history with copied working-tree files. Historical Git LFS objects were transferred to their destination
repositories.

The `ryan-tools` transition:

1. Removes the transplanted paths from the current tree.
2. Adds `qgis-resources/` and `excel-resources/` at pinned submodule commits.
3. Updates `.gitmodules`, setup documentation, workspace files, hard-coded paths, and QGIS model references.
4. Removes `QGIS-Styles` from Python package discovery and lets installed-wheel users pass an explicit styles
   path to `TUFLOWResultsStyler`.
5. Verifies a fresh recursive clone and the library build before publication.

Do not rewrite `ryan-tools` history after the extraction. Removing the old paths in the transition commit keeps
them out of future checkouts while retaining traceability in older commits. A destructive history rewrite and
force-push is justified only for secrets, licensing constraints, or a demonstrated storage problem; it is not
required merely because the paths now live in dedicated repositories.

## Then: script triage

The domain grouping under `ryan-scripts` is useful, but its child folder names are inconsistent. Cleanup should
concentrate on `misc-python`, `python-not-polished`, standalone Python files at the repository root, and one
workflow-family rename at a time.

For each script:

1. Identify whether it is actively used, retained only as a reference, or obsolete.
2. Move reusable parsing and processing into `ryan_library/functions`.
3. Keep orchestration in `ryan_library/orchestrators` and human-facing wrappers in `ryan-scripts`.
4. Delete genuine duplicates rather than creating more compatibility wrappers.
5. Process one workflow family per commit so behaviour changes remain reviewable.

`ryan_library/scripts` is already a deprecated compatibility namespace, with replacements under
`ryan_library/orchestrators` or `ryan_library/functions`. Migrate remaining imports and tests before the
31 December 2026 compatibility deadline; do not place new orchestration there.

### Directory naming direction

- Keep importable Python packages in lowercase `snake_case`, including `ryan_library`, `ryan_functions`, and
  their child packages.
- Use lowercase, hyphenated names for non-importable resource repositories, including `qgis-resources` and
  `excel-resources`.
- Normalise `ryan-scripts` domain folders such as `TUFLOW-python`, `RORB-python`, `AutoCAD-python`, and
  `12D-python` to concise lowercase domain names during their workflow-family triage. The `-python` suffix is
  redundant beneath a Python script collection.
- Keep `ryan-scripts` and `repo-scripts` until their callers, shortcuts, and documentation are inventoried. The
  names are valid for non-importable directories, although `scripts` and `tools` would be more conventional in
  a new repository.
- Treat `unsorted` and `python-not-polished` as temporary intake areas with explicit disposition work, not as
  permanent architectural components.
- Move root planning documents such as `TESTING_AND_ARCHITECTURE.md` and `TESTING_TASKS.md` under `docs/` with
  descriptive lowercase names when documentation is reorganised.

## Then: test collection and documentation

- Resolve the existing `run_hy8`, `tkinter`, and `tuflow_logsummary` collection failures.
- Preserve `tests/test_data` as the only permissible test-data location.
- Distinguish fast tests from environment-dependent integration tests without introducing alternate data paths.
- Reduce the root README to setup, repository orientation, and common entry points.
- Move detailed architecture, testing, migration, and MCP material under `docs/` with links from the README.
