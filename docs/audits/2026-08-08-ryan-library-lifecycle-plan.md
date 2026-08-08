# `ryan_library` lifecycle audit and implementation plan

## Baseline

| Item | Value |
| --- | --- |
| Audit date | 8 August 2026 |
| Package version | `26.08.03.4` |
| Git branch | `main` |
| Git commit | `e9754c3` |
| Audited scope | `ryan_library/**/*.py`, excluding `ryan_library/processors/**` |
| Python files classified | 88 |

The package version is the value in `pyproject.toml` at the audit baseline. The commit identifies the repository
snapshot used for the static reference analysis. The working tree was not clean: `implementation_plan.md` and the
`excel-resources` and `qgis-resources` submodule checkouts contained pre-existing user changes. Those changes were not
part of this audit and must remain untouched during implementation.

The audit inspected imports and references across 523 tracked Python files, including initialized submodules. Three
syntactically invalid files under the `unsorted` submodule could not be parsed; they cannot be executable importers in
their current state. Raw repository searches were also used to identify documentation, notebook, configuration, and
dynamic-import references. No source under `ryan_library/processors` was audited.

## Lifecycle definitions

A file is not considered maintained merely because some other file imports it. Imports from tests, unfinished scripts,
deprecated shims, or eager package discovery are weaker evidence than reachability from a supported workflow.

### Maintained

Reachable from a supported wrapper, entry point, orchestrator, or other maintained library workflow. Package
infrastructure required by those paths is also maintained. A maintained classification does not mean the current code
needs no improvement.

### Public API

Deliberately documented for direct use in the README, examples, or other user-facing material. A public API may have no
internal production caller because external or interactive use is its purpose.

### Compatibility-only

Retained to forward an older import or call to a maintained replacement. It must have a documented replacement and,
where practical, a removal date. Compatibility-only code does not establish that the underlying implementation is
actively used.

### Experimental

Deliberately retained for evaluation or future integration, but not connected to a supported workflow. Experimental
modules need an owner, a stated purpose, and a review date; otherwise they become removal candidates.

### Removal candidate

No maintained caller, documented public API, dynamic configuration reference, or explicit compatibility commitment was
found. Removal still requires an external-use check and a review of repository history; static analysis cannot prove
that an installed library is never imported outside this repository.

### Accidentally loaded

Imported only as a side effect of broad package initialization or module discovery. This is an evidence qualifier, not
a permanent module status. Accidental loading does not count as usage and should be eliminated.

## Applied classification

The classifications below cover the 87 Python files remaining in scope after removal of the obsolete standard-library
logging compatibility module.

| Status | Count | Classification |
| --- | ---: | --- |
| Maintained | 59 | `ryan_library/__init__.py`; all four files under `ryan_library/classes`; all 24 files under `ryan_library/orchestrators`; all three files under `ryan_library/resources`; and all `ryan_library/functions` files not listed as exceptions below |
| Public API | 1 | `ryan_library/functions/tuflow/notebook_helpers.py` |
| Compatibility-only | 22 | All 20 files under `ryan_library/scripts`; `functions/gdal/gdal_environment.py`; `functions/gdal/gdal_runners.py` |
| Experimental | 2 | `functions/hy8/__init__.py`; `functions/hy8/run_hy8_bridge.py` |
| Removal candidate | 3 | `functions/data_processing.py`; `functions/tkinter_utils.py`; `functions/tlf_missing_runs.py` |
| Accidentally loaded | qualifier | Top-level modules discovered and imported by `ryan_functions/__init__.py`; this does not alter their primary classifications above |

### Classification notes

- `notebook_helpers.py` is a public API because it is documented in the root README and used in
  `examples/tuflow_workflow_demo.ipynb`. Its lack of a production `.py` caller is expected.
- The whole `ryan_library/scripts` namespace is compatibility-only. Its current deadline is 31 December 2026.
- The two deprecated GDAL modules have no repository callers. Their current compatibility deadline is 31 December 2026.
- The obsolete standard-library `logging_helpers.py` implementation and `misc_functions.setup_logging()` forwarder
  were removed after their RORB callers were superseded; maintained workflows use `loguru_helpers.py`.
- `data_processing.py` is used only by tests and an explicitly unfinished RORB script (`closure_period_RORB_TUFLOW_v9.py`) through the deprecated
  `ryan_functions` package. That does not qualify as a maintained workflow.
- `tkinter_utils.py` and `tlf_missing_runs.py` are referenced only by tests.
- The HY-8 bridge is re-exported by its package and tested, but no maintained wrapper, orchestrator, README example, or
  other production caller was found.
- `ryan_library/__init__.py` remains maintained package infrastructure, but its wildcard imports and broken legacy
  aliases are defects to fix rather than evidence for demoting the module.

## Implementation principles

1. Do not remove an API merely because the repository has no caller. First check published documentation, release
   notes, repository history, and any known external scripts.
2. Migrate callers before deleting compatibility code. Do not silence deprecation warnings.
3. Keep reusable behaviour in `ryan_library/functions`, workflow control in `ryan_library/orchestrators`, and editable
   human-facing settings and CLI behaviour in `ryan-scripts`.
4. Implement one bounded work package per commit or pull request. Avoid a single broad cleanup diff.
5. Preserve external behaviour unless a work package explicitly documents a breaking change.
6. When a maintained file is edited, review and validate the whole file rather than only the changed lines.
7. Do not audit or opportunistically refactor `ryan_library/processors` as part of this plan.

## Implementation plan

### Work package 1: repair package import surfaces

**Priority:** critical; **dependencies:** none

Files:

- `ryan_library/__init__.py`
- `ryan_library/functions/__init__.py`
- `ryan_functions/__init__.py`
- focused compatibility tests

Actions:

1. Replace `from .functions import *` and `from .processors import *` with a deliberately minimal package surface.
2. Decide which, if any, names are supported directly from `ryan_library`; list those names explicitly in `__all__`.
3. Correct the legacy alias map. The current package-level `__getattr__` tries to import nonexistent paths such as
   `ryan_library.scripts.tuflow_culverts_merge` rather than the physical
   `ryan_library.scripts.tuflow.tuflow_culverts_merge` module.
4. Replace eager `pkgutil.iter_modules()` importing in `ryan_functions` with lazy, explicit compatibility aliases.
5. Ensure importing one legacy function module does not import unrelated packages such as HY-8, Tkinter, GDAL, or
   notebook helpers.
6. Preserve the deprecated `ryan_functions.<module>` spellings that are still intentionally supported, and issue one
   clear deprecation warning that names the replacement.

Acceptance criteria:

- `import ryan_library` succeeds without eagerly importing all processors or optional workflow modules.
- Every advertised package-level legacy alias imports its intended replacement or is deliberately removed and
  documented as a breaking change.
- `import ryan_functions` and `from ryan_functions.misc_functions import ExcelExporter` work from the source checkout
  without adding `vendor/run_hy8/src` to `PYTHONPATH`.
- Focused tests cover lazy loading, alias resolution, warning text, and unknown attributes.
- No new wildcard imports are introduced.

Suggested commit: `Fix package compatibility imports`

### Work package 2: establish and enforce the compatibility inventory

**Priority:** high; **dependencies:** work package 1

Files:

- `ryan_library/scripts/**`
- `ryan_library/functions/gdal/gdal_environment.py`
- `ryan_library/functions/gdal/gdal_runners.py`
- `ryan_library/functions/misc_functions.py`
- compatibility documentation

Actions:

1. Create one authoritative compatibility table containing the legacy import, replacement, warning category, support
   deadline, and known callers.
2. Retain the `ryan_library/scripts` forwarding modules through 31 December 2026 unless the compatibility promise is
   explicitly revised.
3. Retain the two deprecated GDAL modules through 31 December 2026 unless external-use review supports an earlier breaking
   removal.
4. Keep the removed standard-library logging path out of maintained imports; use `loguru_helpers.py` for serial,
   notebook, and multiprocessing configuration.
5. Ensure every compatibility module delegates to maintained code and contains no independent workflow logic.
6. Add a removal checklist for each deadline: migrate callers, update documentation, remove shims and aliases, then
   verify the built wheel no longer contains them.

Acceptance criteria:

- Every compatibility-only module has an explicit replacement and policy.
- Maintained repository code imports no `ryan_library.scripts` module.
- Deprecation warnings point to a working replacement and are tested as warnings, not hidden by filters.
- Removal dates appear in one authoritative document rather than drifting across several modules.

Suggested commit: `Document compatibility module lifecycle`

### Work package 3: decide the three removal candidates

**Priority:** high; **dependencies:** work package 1 and external-use review

#### `functions/data_processing.py`

1. Migrate the unfinished `closure_period_RORB_TUFLOW_v9.py` script away from `ryan_functions.data_processing`, or
   formally archive/remove that unfinished script in its own script-triage change.
2. Replace `check_string_TP`, `check_string_duration`, and `check_string_aep` callers with the maintained
   `TuflowStringParser` API.
3. Do not retain `safe_apply()` merely for its tests. If a real caller needs it, narrow the caught exceptions and make
   failure handling explicit; otherwise remove it with the module.
4. Replace private `_collections_abc.Callable` imports with `collections.abc.Callable` if any code is retained.

Decision gate: remove the module unless a supported external caller is identified.

#### `functions/tkinter_utils.py`

Decision gate: **Removed.** Tkinter dependency caused deployment issues in corporate environments and has been superseded by the live dashboard. The module and its tests have been deleted.

#### `functions/tlf_missing_runs.py`

1. Properly describe the logic and expected workflow so it can be completed in the future.
2. Split reusable analysis from presentation.
3. Add an orchestrator where workflow coordination is needed, and add a thin wrapper with argparse, exit codes, working-directory handling, and documented inputs/outputs.
4. Replace broad exception handling and complete the public type annotations.

Decision gate: **Retained as planned functionality.** The logic needs to be documented and tidied up so the feature can be properly built in the future.

Acceptance criteria for the work package:

- Each candidate has a recorded keep/productize/remove decision and evidence.
- No test exists solely to preserve code that has no supported purpose.
- Removed modules are absent from compatibility maps, docs, tests, and wheel contents.

Suggested commits: one commit per candidate, for example `Remove unused Tkinter helpers`.

### Work package 4: decide the HY-8 bridge lifecycle

**Priority:** medium; **dependencies:** work package 1 and an owner/use-case decision

Files:

- `ryan_library/functions/hy8/__init__.py`
- `ryan_library/functions/hy8/run_hy8_bridge.py`
- optional future wrapper or orchestrator

Actions:

1. Identify the intended user and end-to-end workflow for converting culvert maximums into an HY-8 project.
2. If the bridge is active, document it, add a maintained entry point, and make the `run_hy8` dependency work from
   both editable source and the built wheel.
3. Clearly document the current rectangular-culvert limitation and other incomplete mapping assumptions.
4. If it remains experimental, add an owner and review date and avoid importing it from unrelated package paths.
5. If no intended workflow remains, remove the bridge and its tests.

Acceptance criteria:

- The bridge is either reachable and documented, explicitly time-bounded as experimental, or removed.
- Importing unrelated compatibility packages never requires `run_hy8`.

Suggested commit: `Clarify HY-8 bridge lifecycle`

### Work package 5: consolidate PO and POMM combination workflows

**Priority:** high; **dependencies:** stable import surfaces from work package 1

Files:

- `ryan_library/orchestrators/tuflow/po_combine.py`
- `ryan_library/orchestrators/tuflow/pomm_combine.py`
- a shared function or private orchestrator helper

Actions:

1. Extract the common normalize, warn, collect, process, export, and completion-warning sequence.
2. Parameterize accepted data types, combination callable, output prefix, sheet name, and metadata.
3. Keep the two public orchestrator functions as readable domain entry points rather than merging user-facing names.
4. Replace `hasattr()` plus type ignores in POMM combination with a typed callable or explicit protocol after checking
   the processor collection contract. This check may inspect processor APIs but must not expand into a processor audit.
5. Preserve wrapper arguments and output names.

Acceptance criteria:

- PO and POMM share one workflow implementation without reducing domain clarity.
- Existing wrappers retain their arguments, data-type defaults, output names, and exit behaviour.
- Focused PO/POMM tests cover empty input, invalid types, location filters, combination, and export metadata.

Suggested commit: `Share PO and POMM combination workflow`

### Work package 6: remove notebook and timeseries workflow duplication

**Priority:** high; **dependencies:** none, but easier after work package 5 establishes the extraction pattern

Files:

- `ryan_library/functions/tuflow/notebook_helpers.py`
- `ryan_library/functions/tuflow/po_timeseries_checks.py`
- `ryan_library/orchestrators/tuflow/peak_check_po_csvs.py`
- `ryan_library/orchestrators/tuflow/tuflow_timeseries_stability.py`
- related notebook and focused tests

Actions:

1. Extract public, reusable functions for result-type normalization, file discovery, serial analysis, result flattening,
   and output column ordering.
2. Let orchestrators add multiprocessing, logging context, and export.
3. Let notebook helpers call the same functions serially and return DataFrames.
4. Remove the notebook comments and duplicated implementations that say logic was inlined to avoid private imports.
5. Apply the same pattern to closure duration and log-summary notebook helpers where they still reconstruct
   orchestrator behaviour.
6. Keep plotting helpers notebook-focused and optional; importing the notebook module must not import Matplotlib.

Acceptance criteria:

- One implementation defines each file pattern, result-type rule, and presentation column order.
- Serial notebook and parallel orchestrator paths produce equivalent DataFrames for the same inputs.
- The documented notebook API remains stable or has a documented migration.

Suggested commit: `Share notebook and timeseries workflow logic`

### Work package 7: consolidate logging and align log-message style

**Priority:** medium; **dependencies:** compatibility inventory from work package 2

Files:

- `ryan_library/functions/loguru_helpers.py`
- `ryan_library/functions/misc_functions.py`
- affected maintained callers

Actions:

1. Keep `loguru_helpers.py` as the sole maintained logging implementation.
2. Keep maintained and legacy-wrapper callers on Loguru; the obsolete standard-library configurator has been removed.
3. Decide whether `LoggerManager`, `worker_process`, and `log_exception` are supported compatibility APIs; deprecate or
   remove test-only endpoints rather than preserving them indefinitely.
4. Change eager debug f-strings to Loguru's parameterized form. The audit counted 37 occurrences outside processors.
5. Prefer Loguru parameterized formatting for dynamic values at every level; use `logger.opt(lazy=True)` with callables
   where producing a diagnostic value is expensive.
6. Review broad exception handlers at logging boundaries so failures are either deliberately contained or propagated;
   avoid recursive logging failures.

Acceptance criteria:

- Maintained code has one logging stack and one multiprocessing logging lifecycle.
- Debug messages are lazy; user-facing info/success/warning/error/exception messages render clearly.
- No deprecated logging notice is emitted with `print()`.

Suggested commit: `Consolidate library logging helpers`

### Work package 8: clean maintained module boundaries

**Priority:** medium; **dependencies:** work packages 5-7 should land first to avoid conflicting edits

Actions:

1. Move the organization-specific UNC-to-drive mapping out of `functions/path_stuff.py` and pass mappings from a
   wrapper or configuration source. Resolve both paths before containment checks.
2. Split `misc_functions.py` into purpose-based modules, with `ExcelExporter` and its compatibility functions moved to
   a clearly named export module. Retain temporary forwarding imports only if external-use review requires them.
3. Review the mean/median convenience functions in `tuflow_culverts_mean.py`. Production already calls the generic
   statistic implementations; keep convenience functions only if they are intentional public APIs.
4. Remove stale `# ryan_library/scripts/...` headers from relocated orchestrators.
5. Remove direct `if __name__ == "__main__"` execution from maintained library modules when a maintained wrapper
   exists. Move diagnostic demonstrations, such as the suffix-registry dump, into `repo-scripts` or documented examples
   if they remain useful.
6. Keep orchestrators non-interactive. Working-directory changes, pauses, CLI parsing, and process exit codes remain in
   `ryan-scripts` wrappers.

Acceptance criteria:

- Reusable functions no longer contain organization-specific path policy.
- Generic grab-bag modules are reduced without breaking documented import paths unexpectedly.
- Maintained orchestrators have accurate headers and one supported human-facing entry path.
- Public convenience APIs are either documented and tested or removed through the compatibility process.

Suggested commits: separate path, exporter, culvert API, and header/entry-point commits.

### Work package 9: remove expired compatibility code

**Priority:** date-driven; **dependencies:** work package 2 and completion of caller migrations

#### After 31 December 2026

1. Confirm no repository or known external caller imports `ryan_library.scripts`.
2. Remove the compatibility namespace, package-level aliases that exist only for it, and related compatibility tests.
3. Confirm no supported caller uses `gdal_environment` or `gdal_runners`.
4. Remove both GDAL modules and direct users to installed Python GDAL and `gdal.raster_processing`.
5. Update architecture and user documentation.

Acceptance criteria:

- Expired compatibility modules are deleted rather than left permanently deprecated.
- Built-wheel inspection confirms removed paths are absent.
- Release notes identify breaking removals and replacements.

Suggested commits: `Remove expired script compatibility namespace` and `Remove expired GDAL compatibility modules`.

## Validation strategy

For every implementation work package:

1. Run Black with the repository's 120-character configuration on modified Python files.
2. Run strict Pyright only on modified Python files, as required by `AGENTS.md`.
3. Run focused tests for the modified workflow. Add or change tests only where the work package changes a supported
   contract or removes obsolete tests with obsolete code.
4. Run deprecation-sensitive paths with `-W error::DeprecationWarning` while migrating active callers; fix warnings at
   their source rather than filtering them.
5. Run `git diff --check` and inspect the complete diff.
6. Verify `git status` and preserve unrelated submodule and documentation changes.
7. When a work package modifies `ryan_library`, run `python repo-scripts/build_library.py --skip-pip` from the repository
   root, inspect the version bump and wheel, and ensure the build script has not left unrelated files staged.
8. Use `cmd.exe /C repo-scripts\run_tests.bat` only when the work package justifies a full-suite validation. Focused
   pytest runs should use a pre-created repository-local base temporary directory on this Windows checkout.

## Completion criteria

This plan is complete when:

- All 88 baseline modules have either retained their classification deliberately or moved through a recorded lifecycle
  decision.
- Supported imports are explicit, lazy where appropriate, and free of unrelated optional-dependency failures.
- Removal candidates and experimental modules have been integrated, time-bounded, or removed.
- Duplicate PO/POMM and notebook/orchestrator workflow logic has one maintained implementation.
- Maintained code uses the agreed logging and wrapper boundaries.
- Compatibility code is removed after its deadline rather than becoming permanent dead weight.
- Documentation and the built package reflect the resulting public surface.

Because this is a dated audit snapshot, future findings should be recorded in a new dated file rather than rewriting
the baseline. An evergreen lifecycle policy may later be extracted into `docs/architecture/` once the classifications
and removal process have been exercised.
