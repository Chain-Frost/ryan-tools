# Repository improvement roadmap

## Baseline and purpose

| Item | Value |
| --- | --- |
| Roadmap updated | 8 August 2026 |
| Package version | `26.08.03.4` |
| Git branch | `main` |
| Git commit | `e9754c3` |

This roadmap is the high-level view of completed and remaining repository work. The
[`DEVELOPMENT_GUIDE.md`](DEVELOPMENT_GUIDE.md) is the canonical source for architecture, code categories, lifecycle
terms, validation expectations, and sources of truth. Dated audits under [`audits/`](audits/) contain the detailed
implementation evidence and task breakdowns.

The baseline checkout contained pre-existing user changes, including changes in the `excel-resources` and
`qgis-resources` submodules. This refresh did not treat the dirty working tree as completed or validated work.

GitHub Actions remain out of scope while GitHub is used primarily as repository storage. The only workflow file is
currently disabled. Revisit that decision if automated publication, pull-request checks, or scheduled maintenance
becomes a repository requirement.

## Completed milestones

### Repository hygiene and package configuration

- Black and strict Pyright target the documented Python 3.14 development environment.
- Pyright includes the real `ryan-scripts` source tree.
- The stale PyHMA submodule declaration was removed while its vendored package was retained.
- Generated coverage reports are ignored rather than tracked.
- Mandatory agent guidance and repository paths are documented in `AGENTS.md`.
- Package metadata and dependencies have one authoritative source in `pyproject.toml`.

### Build and installation workflow

- `repo-scripts/build_library.py` owns version bumping and package builds, including `--skip-artifacts` support.
- `setup.py` remains an active setuptools build hook that stages pinned TUFLOW QML resources from the
  `qgis-resources` checkout into the wheel.
- `repo-scripts/install_latest_wheel.py` owns normal and force-reinstall command construction.
- Windows batch files are thin entry points around the maintained Python build and installation utilities.
- The obsolete source-archive installer was removed.

The exact wheel-file counts recorded during the original migration are historical verification, not a permanent
package-size contract. Current builds should be validated against current source and package metadata.

### QGIS and Excel resource extraction

The QGIS and Excel extraction is complete. Both repositories are pinned Git submodules:

| Checkout | Purpose |
| --- | --- |
| `qgis-resources/` | QGIS styles, layouts, processing models, scripts, and QGIS-dependent supporting workbooks |
| `excel-resources/` | General Excel workbooks and supporting resources grouped by workflow |

The parent repository stages required QML files into builds without copying the complete resource repositories into the
Python source tree. The extracted history remains available through the original repository history and the destination
repositories; no parent-repository history rewrite is planned.

### Maintained wrapper foundation

- Maintained library-backed wrappers follow `ryan-scripts/WRAPPER_STANDARD.md`.
- Shared wrapper behaviour covers banners, editable defaults, common CLI options, working-directory handling, clear
  exit codes, optional pause, and installed-library version reporting.
- Reusable behaviour belongs in functions, workflow coordination in orchestrators, and user/project configuration in
  wrappers.
- Maintained wrappers use descriptive unversioned filenames; versioned scripts are treated as standalone snapshots,
  legacy code, or project-specific utilities unless reviewed otherwise.

### Documentation foundation

- The root README has been reduced to repository orientation, setup, common entry points, resources, development, and
  safety guidance.
- `docs/DEVELOPMENT_GUIDE.md` now provides the canonical repository-wide architecture and lifecycle guidance.
- `docs/ENVIRONMENTS.md` documents repository, installed-wheel, VS Code, QGIS/OSGeo4W, interactive, and headless
  execution environments.
- MCP setup and dated implementation plans live under `docs/` and are indexed by `docs/README.md`.

### Submodule boundaries and upstream syncs

1. **Clean up `run-hy8` upstream**: The external `run-hy8` repository contains scripts that violate the domain boundary by parsing TUFLOW exports directly (e.g., `culvert_demo-from-tuflow.py`). Since this mapping responsibility officially belongs to `ryan-tools` (via `ryan-scripts/tuflow/tuflow_to_hy8.py` and `ryan_library.functions.hy8`), the TUFLOW/12D-specific demo scripts in the upstream `run-hy8` repo should be removed or deprecated.

- Detailed wrapper guidance remains beside the code it governs.

## Current priorities

### 1. Repair and simplify package import surfaces

**Priority:** critical

Follow work package 1 in the
[`ryan_library` lifecycle plan](audits/2026-08-08-ryan-library-lifecycle-plan.md#work-package-1-repair-package-import-surfaces).

The immediate goals are:

1. Replace wildcard package imports with a deliberately minimal public surface.
2. Correct broken package-level compatibility aliases.
3. Replace eager `ryan_functions` module discovery with explicit lazy forwarding.
4. Prevent unrelated optional dependencies, including HY-8, from being imported by ordinary compatibility calls.
5. Verify source-checkout and installed-wheel imports separately.

This work should land before broad removal or consolidation because reliable import boundaries are needed to determine
real reachability.

### 2. Complete the logging pipeline work

**Priority:** high

Use the [logging pipeline implementation plan](audits/2026-08-08-logging-pipeline-implementation-plan.md) as the
actionable source. It supersedes `logging_review_tasks.txt` for current work.

The work includes:

1. Define and test serial and multiprocessing threshold behaviour.
2. Make console and file thresholds independent without discarding worker debug records prematurely.
3. Preserve the real originating module, function, line, exception, and traceback through queue transport.
4. Make repeated setup and shutdown deterministic and free of duplicate sinks or leaked listener processes.
5. Complete the active-code message-style sweep and add a focused AST-based policy check.
6. Verify representative Windows serial, multiprocessing, threaded, and GeoPackage workflows.

Do not combine logging-pipeline changes with unrelated script or processor cleanup.

### 3. Apply the `ryan_library` lifecycle decisions

**Priority:** high

The [dated lifecycle audit](audits/2026-08-08-ryan-library-lifecycle-plan.md) classified all 88 Python files under
`ryan_library`, excluding `ryan_library/processors`:

| Status | Count |
| --- | ---: |
| Maintained | 59 |
| Public API | 1 |
| Compatibility-only | 23 |
| Experimental | 2 |
| Removal candidate | 3 |

Implement its work packages in bounded changes:

1. Establish one compatibility inventory with working replacements and deadlines.
2. Decide whether `data_processing.py`, `tkinter_utils.py`, and `tlf_missing_runs.py` should be integrated or removed.
3. Give the HY-8 bridge a maintained entry point, a time-bounded experimental status, or a removal decision.
4. Consolidate duplicated PO/POMM combination workflows.
5. Share notebook and timeseries workflow logic instead of maintaining parallel implementations.
6. Consolidate logging helpers and clean maintained module boundaries.

Static absence of repository callers is not sufficient by itself to delete a published import. Check known external
scripts, documentation, and history before a breaking removal.

### 4. Triage `ryan-scripts` by workflow family

**Priority:** medium

The domain grouping remains useful, but folder names and script maturity are inconsistent. Current triage should focus
on:

- `ryan-scripts/python-not-polished`;
- `ryan-scripts/other`;
- standalone Python utilities at the repository root;
- legacy batch folders;
- one versioned or duplicated workflow family at a time.

For each script:

1. Classify it using `docs/DEVELOPMENT_GUIDE.md`: maintained wrapper, standalone, project-specific, compatibility,
   experimental/reference, superseded, or not working.
2. Identify actual callers, copied-project use, documentation, side effects, and replacement paths.
3. Move only demonstrated reusable behaviour into `ryan_library/functions`.
4. Put complete workflow coordination in an orchestrator when reuse warrants it.
5. Keep project paths, globs, naming templates, CLI behaviour, and user presentation in wrappers.
6. Delete genuine duplicates rather than creating new compatibility layers.
7. Process one workflow family per reviewable commit.

The former `misc-python` directory no longer exists and is not a current triage target.

### 5. Normalize directory names only after caller inventory

**Priority:** medium-low

Potential renames include `TUFLOW-python`, `RORB-python`, `AutoCAD-python`, and `12D-python` to concise lowercase domain
names. Do not rename folders merely for style. First inventory:

- copied wrappers and shortcuts;
- README links and command examples;
- workspace tasks and launch configurations;
- batch files and external project references;
- automation or packaging assumptions.

Keep importable packages in lowercase `snake_case`. Use lowercase, hyphenated names for non-importable resource
repositories. Retain `ryan-scripts` and `repo-scripts` until their external callers and documentation are understood.

### 6. Finish documentation consolidation

**Priority:** medium-low

The main architecture and user documentation has moved under `docs`, but cleanup remains:

1. Reconcile and then move or retire the root `TESTING_AND_ARCHITECTURE.md` and `TESTING_TASKS.md` files.
2. Retire `logging_review_tasks.txt` after the dated logging plan is implemented or explicitly supersedes every useful
   item.
3. Resolve the root `implementation_plan.md` through its own active work; do not overwrite its staged or unstaged
   content as part of unrelated documentation cleanup.
4. Keep `docs/README.md` as the navigation index and avoid duplicating detailed inventories in the root README.
5. Treat dated audits as snapshots. Create a new dated audit when the baseline changes materially rather than silently
   rewriting old evidence.

### 7. Maintain proportionate test and environment coverage

**Priority:** ongoing

The repository now configures the bundled HY-8 source path in `pytest.ini` and `repo-scripts/run_tests.bat`, and marks
Tkinter-dependent coverage explicitly. The earlier generic task to fix three named collection failures is therefore no
longer an adequate description of current work. This roadmap refresh did not rerun test collection or the full suite.

Ongoing work should:

1. Keep fast unit tests distinct from environment-dependent integration tests.
2. Use synthetic fixtures when proprietary model results cannot be committed.
3. Preserve `tests/test_data` as the repository's test-data checkout rather than creating alternate fixture roots.
4. Use a repository-local pytest cache and base temporary directory on Windows.
5. Include `vendor/run_hy8/src` when exercising HY-8 from the source checkout.
6. Run focused tests for bounded changes and the complete Windows runner when a change justifies repository-wide
   validation.
7. Remove tests that exist only to preserve code deliberately removed through the lifecycle process.

## Date-driven compatibility removals

### After 31 December 2026

- Confirm that no repository or known external caller uses `ryan_library.scripts`.
- Remove the expired compatibility namespace and package-level aliases that exist only to support it.
- Update documentation, focused compatibility tests, release notes, and built-wheel contents.

- Confirm that no supported caller uses `ryan_library.functions.gdal.gdal_environment` or
  `ryan_library.functions.gdal.gdal_runners`.
- Remove those modules and direct users to installed Python GDAL and `gdal.raster_processing`.

Do not leave expired shims permanently deprecated. Conversely, do not remove them before their stated date without an
explicit compatibility decision.

## Out of scope for the current roadmap

- A broad audit or opportunistic refactor of `ryan_library/processors`.
- Refactoring submodule or vendored contents as ordinary parent-repository code.
- Rewriting repository history for the completed resource extraction.
- Enabling GitHub Actions without a concrete automation requirement and an agreed maintenance owner.
- Renaming all script folders in one change.
- Treating formatting, type-check success, or import counts alone as proof that code is correct or actively used.

## Roadmap completion criteria

This roadmap can be replaced by a smaller maintenance backlog when:

- package imports are explicit and lazy where appropriate;
- all lifecycle removal and experimental decisions are recorded and implemented;
- duplicated PO/POMM and notebook/orchestrator workflows have one maintained implementation;
- the logging pipeline has deterministic thresholds, context, and lifecycle behaviour;
- script intake and legacy areas have explicit classifications and owners or dispositions;
- expired compatibility layers have been removed on schedule;
- remaining root planning documents have been integrated, archived, or deleted deliberately;
- focused and full-suite validation paths are documented and reproducible on the supported Windows environment.

### Python 3.15 Lazy Loading

Currently, the repository relies on PEP 562 (**getattr**) for lazy loading modules like pandas and numpy. When Python 3.15 becomes the common environment, scripts and library entry points should be updated to utilize native Python 3.15 lazy loading mechanisms (e.g. **lazy_modules** or standard module-level lazy imports) for better performance and simpler syntax.
