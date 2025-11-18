# Testing Initiative Planning

## Tests Collection Branch

The shared long-lived branch for aggregating new tests is named `tests-collection`. Create it locally with `git branch tests-collection` (already created in this repo) and keep it rebased onto `main` so follow-up PRs can target it directly.

## Review & Planning Tasks

:::task-stub{title="Inventory ryan_library.functions for test planning"}
1. Catalogue every top-level module under `ryan_library/functions` (including subpackages such as `pandas`, `gdal`, `tuflow`, and `RORB`) and note their primary responsibilities.
2. Identify critical functions, IO operations, and dependency boundaries that require regression coverage, distinguishing between pure utilities and wrappers around external tools.
3. Produce a testing priorities matrix (e.g., high/medium/low) referencing current usage in `ryan-scripts` to inform which modules should receive tests first.
4. Summarise fixtures, mocks, and external resources each module would need for feasible unit testing, highlighting any blockers or data dependencies.
:::

## Automated Coverage Inventory

The following tasks aim to systematise backlog generation by first discovering every Python source file, correlating it with existing tests, and then spawning follow-up tasks for gaps. Contributors should prefer scripting repeatable inventories over manual spot-checks so the resulting backlog scales to hundreds of actionable work items.

:::task-stub{title="Produce source-to-test coverage matrix"}
1. Write a lightweight discovery script (e.g., Python + `pathlib`) that enumerates every `.py` file under `ryan_library`, `ryan-scripts`, and `repo-scripts`, skipping known vendored or legacy paths (`vendor/`, `ryan-functions/`, `unsorted/`).
2. For each source file, detect whether a corresponding test module already exists (e.g., by mirroring the path under `tests/` or grepping for imports) and record the linkage in a CSV/Markdown table.
3. Surface the inventory in `TESTING_TASKS.md` (or a linked document) with columns for "Has tests?", "Priority", and "Notes" so future backlog tasks can be generated programmatically.
4. Automate a weekly refresh of this matrix (documenting the command in the repo-scripts folder) to keep the backlog aligned with code churn.
:::

:::task-stub{title="Generate backlog entries for uncovered files"}
1. Consume the coverage matrix above to identify every source file lacking an associated test module.
2. For each uncovered file, create a dedicated `task-stub` describing the scope of tests required (unit, integration, smoke) and any fixtures or external dependencies that must be mocked.
3. Group the generated tasks by package/feature area, ensuring large modules are further decomposed into per-function subtasks to keep individual assignments manageable.
4. Embed the resulting backlog into `TESTING_TASKS.md`, targeting at least 50 new task entries in the first pass and documenting the script/command used to regenerate them.
:::

:::task-stub{title="Cross-check backlog against pytest coverage reports"}
1. Configure `pytest --cov` to run selectively on modules with existing tests, capturing coverage data without executing unready suites (respect exclusions such as `ryan_library/processors`).
2. Parse the resulting coverage XML/JSON to flag functions or branches still untested despite having nominal test modules.
3. Feed those findings back into the backlog by appending gap-specific subtasks (e.g., "Add branch coverage for error handling in X") beneath the relevant module headings.
4. Document the coverage workflow (commands, expected runtime, environment prerequisites) so future contributors can routinely validate backlog completeness.
:::

## Task Backlog Expansion

The following meta-tasks focus on exploding the testing backlog by breaking down each module into granular work items. Each assignee should generate numerous downstream `task-stub`s (aim for at least one per public function or behavioural pathway) rather than implementing tests directly.

:::task-stub{title="Generate detailed backlog for data_processing helpers"}
1. Review every callable in `ryan_library/functions/data_processing.py`, noting pure utilities versus IO-heavy helpers.
2. For each behaviour (success path, exception handling, logging side effects), draft discrete `task-stub`s describing the exact unit tests required.
3. Document fixture and monkeypatch requirements (e.g., fake iterables, exception-raising callables) within those new tasks.
4. Collate the generated tasks under a dedicated subsection in `TESTING_TASKS.md` for easy tracking.
:::

:::task-stub{title="Break down dataframe_helpers into per-function test tasks"}
1. Catalogue the DataFrame manipulation utilities exposed in `ryan_library/functions/dataframe_helpers.py`.
2. Produce separate task entries for validation, aggregation, and cleanup routines, ensuring edge cases (empty frames, mismatched schemas, dtype coercion) are explicitly called out.
3. Specify pandas fixtures or sample CSV inputs each future test will need.
4. Insert the resulting tasks into `TESTING_TASKS.md`, grouped beneath a new dataframe_helpers heading.
:::

:::task-stub{title="Explode file_utils coverage requirements"}
1. Map every function in `ryan_library/functions/file_utils.py`, including internal helpers that manage concurrency, logging, and filesystem safeguards.
2. For each major branch (e.g., queue worker lifecycle, logging paths, UNC path handling), author individual `task-stub`s detailing the necessary test cases and expected fixtures/mocks.
3. Ensure the generated tasks distinguish between fast unit tests and slower integration-style checks so they can be scheduled appropriately.
4. Append the backlog to `TESTING_TASKS.md`, targeting at least 10 downstream tasks for this module.
:::

:::task-stub{title="Plan logging_helpers formatter/regression tests"}
1. Audit `ryan_library/functions/logging_helpers.py` to identify formatters, handlers, and utilities needing verification.
2. Draft dedicated tasks for each public helper, covering both nominal logging flow and attribute-driven switches (e.g., `ConditionalFormatter`).
3. Document logging fixture needs (e.g., `caplog`, fake `LogRecord` instances) within each new task.
4. Capture the backlog beneath a fresh logging_helpers section in `TESTING_TASKS.md`.
:::

:::task-stub{title="Enumerate loguru_helpers testing surfaces"}
1. Inspect `ryan_library/functions/loguru_helpers.py`, categorising wrappers, configuration helpers, and sink management logic.
2. Create targeted tasks for each behaviour, including error propagation, default configuration, and interaction with external loguru sinks.
3. Note any third-party dependencies or monkeypatch requirements per task.
4. Publish the resulting tasks inside `TESTING_TASKS.md` alongside links to supporting documentation where relevant.
:::

:::task-stub{title="Design misc_functions testing backlog"}
1. List every helper inside `ryan_library/functions/misc_functions.py`, grouping related utilities (math helpers, string parsing, concurrency heuristics).
2. Generate a suite of new tasks, each focusing on a single function or closely-related group, emphasising edge cases and documented behaviour.
3. Identify any platform-specific considerations (Windows/Linux) and record them in the spawned tasks.
4. Organise the backlog additions beneath a misc_functions heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Scope parse_tlf parsing/regression tasks"}
1. Review `ryan_library/functions/parse_tlf.py` to understand the parsing pipeline and supported file formats.
2. Produce tasks covering happy paths, malformed input handling, and logging/reporting branches.
3. Describe required sample fixtures (e.g., representative `.tlf` snippets) within each new task.
4. Add the backlog entries under a parse_tlf subsection in `TESTING_TASKS.md`.
:::

:::task-stub{title="Plan path_stuff normalisation test tasks"}
1. Analyse `ryan_library/functions/path_stuff.py` for path conversions, relative path checks, and network drive handling.
2. Create granular tasks that cover Windows vs. POSIX expectations, error handling, and mapping overrides.
3. Highlight monkeypatch requirements (e.g., `Path.cwd`, custom network mapping) in each new task.
4. Record the generated backlog beneath a path_stuff heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Derive process_12D_culverts test backlog"}
1. Examine `ryan_library/functions/process_12D_culverts.py` to understand data inputs, file outputs, and dependency graph.
2. Draft tasks for each processing stage, from input validation to final output generation, specifying mocks for external tools where needed.
3. Include negative path considerations (missing files, invalid schema) when creating each task.
4. Append the tasks to `TESTING_TASKS.md` under a dedicated process_12D_culverts section.
:::

:::task-stub{title="Outline terrain_processing regression tasks"}
1. Review algorithms inside `ryan_library/functions/terrain_processing.py`, distinguishing pure calculations from GDAL/NumPy integrations.
2. Generate tasks targeting each computational branch, ensuring numerical stability, coordinate handling, and error reporting are covered.
3. Specify required raster/vector fixtures or synthetic datasets per task.
4. Publish the backlog under a terrain_processing heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Expand tkinter_utils UI helper backlog"}
1. Inspect `ryan_library/functions/tkinter_utils.py`, noting GUI factory functions, dialogs, and side effects.
2. Produce tasks describing how to unit test these helpers (e.g., via `unittest.mock` or `pytest-tkinter`), including environment guards for headless CI.
3. Account for platform-specific behaviour within each generated task.
4. Insert the backlog beneath a tkinter_utils heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Plan pandas median_calc coverage"}
1. Understand the algorithms inside `ryan_library/functions/pandas/median_calc.py`, including DataFrame expectations and performance shortcuts.
2. Create tasks for varying dataset sizes, null handling, and dtype corner cases.
3. Note fixture requirements (e.g., synthetic DataFrames) in each new task.
4. Add the backlog beneath a pandas.median_calc subsection in `TESTING_TASKS.md`.
:::

:::task-stub{title="Map gdal_environment test generation"}
1. Inspect `ryan_library/functions/gdal/gdal_environment.py` to identify configuration helpers and environment setup logic.
2. Write tasks covering environment variable manipulation, path resolution, and error branches without invoking real GDAL binaries.
3. Document necessary monkeypatches or fixtures (e.g., temporary env vars) in each new task.
4. Organise the backlog within `TESTING_TASKS.md` under a gdal_environment heading.
:::

:::task-stub{title="Break down gdal_runners behaviour"}
1. Review command execution and output parsing logic in `ryan_library/functions/gdal/gdal_runners.py`.
2. Generate tasks for success, failure, and timeout paths, including logging assertions and subprocess mocking strategies.
3. Highlight external dependency considerations (GDAL CLI availability) within each task.
4. Append the backlog to `TESTING_TASKS.md` beneath a gdal_runners heading.
:::

:::task-stub{title="Enumerate RORB read utilities backlog"}
1. Study `ryan_library/functions/RORB/read_rorb_files.py` to understand file parsing, validation, and data transformation steps.
2. Draft tasks covering diverse file variants, error handling, and integration with downstream consumers.
3. Specify fixture datasets and encoding considerations in the generated tasks.
4. Record the backlog under a RORB.read_rorb_files heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Expand tuflow closure_durations backlog"}
1. Inspect `ryan_library/functions/tuflow/closure_durations.py` for algorithms and data dependencies.
2. Produce tasks targeting calculation logic, input parsing, and boundary cases (e.g., empty datasets, overlapping durations).
3. Include notes about required fixture CSVs or shapefiles in each task.
4. Add the backlog beneath a tuflow.closure_durations heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Explode tuflow pomm_combine coverage"}
1. Analyse `ryan_library/functions/tuflow/pomm_combine.py`, identifying workflows for combining datasets and handling duplicates.
2. Generate tasks for each logical pathway, including sorting, grouping, and error conditions.
3. Document fixture creation strategies (e.g., synthetic hydrograph tables) per task.
4. Append the backlog to `TESTING_TASKS.md` under a tuflow.pomm_combine heading.
:::

:::task-stub{title="Plan tuflow_common helper backlog"}
1. Audit `ryan_library/functions/tuflow/tuflow_common.py` for shared utilities used across TUFLOW workflows.
2. Create tasks per utility (file IO, dataframe shaping, path handling), ensuring cross-platform behaviours are covered.
3. Identify external dependencies (GDAL, pandas) and note mocking requirements within each task.
4. Add the backlog under a tuflow.tuflow_common heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Review functions __init__ exports for coverage gaps"}
1. Examine `ryan_library/functions/__init__.py` to understand the public API surface.
2. Generate tasks ensuring each exported helper has a corresponding test backlog entry, cross-referencing modules above.
3. Flag deprecated or re-exported utilities that may need targeted smoke tests.
4. Update `TESTING_TASKS.md` with any additional tasks required to cover gaps.
:::

:::task-stub{title="Assess loguru example relevance for testing"}
1. Evaluate `ryan_library/functions/loguru-example.py` to determine whether it represents runnable documentation or legacy code.
2. If still in scope, create tasks describing lightweight regression tests or documentation validation; otherwise, author a task discussing archival/removal.
3. Note dependencies on loguru configuration when generating those tasks.
4. Capture the decisions and resulting tasks in `TESTING_TASKS.md`.
:::

:::task-stub{title="Generate classes module backlog"}
1. Review `ryan_library/classes` modules (`suffixes_and_dtypes.py`, `tuflow_string_classes.py`) to catalogue dataclasses and utilities.
2. Produce tasks addressing construction/validation logic, serialization pathways, and integration with functions.
3. Identify shared fixtures (e.g., sample schema definitions) and record them within each new task.
4. Append the backlog beneath a classes heading in `TESTING_TASKS.md`.
:::

:::task-stub{title="Seed ryan-scripts orchestration backlog"}
1. Inventory the scripts under `ryan-scripts/`, grouping them by domain (12D, TUFLOW, GDAL, AutoCAD, RORB, misc).
2. For each group, create high-level tasks describing how to derive smoke/integration test plans (mocking external binaries, sample datasets, CLI harnesses).
3. Ensure the generated tasks highlight dependencies on the underlying library modules so scheduling can be coordinated.
4. Document the backlog additions under a new ryan-scripts section in `TESTING_TASKS.md`.
:::

:::task-stub{title="Assess ryan_library.processors and scripts for integration test needs"}
1. Review controllers in `ryan_library/processors` and `ryan_library/scripts` to map their orchestration flow and dependencies on the functions modules.
2. Document existing CLI entry points, configuration files, or environment variables they expect so future integration tests can simulate realistic scenarios.
3. Recommend smoke-test candidates that validate processor pipelines end-to-end without invoking heavyweight external binaries, outlining required stubs.
4. Flag modules that should remain untested due to deprecation or reliance on unavailable licensed software, with rationale.
:::

:::task-stub{title="Evaluate ryan_library/classes and shared utilities for foundational tests"}
1. Inspect data classes and helper abstractions in `ryan_library/classes` and cross-reference where they are instantiated across the repository.
2. Determine serialization/deserialization behaviours, validation logic, or computational algorithms that merit unit-level assertions.
3. List reusable fixtures (e.g., sample configuration objects) and propose how to structure them under a future `tests/conftest.py`.
4. Record potential side effects (file system, network) that will require isolation via monkeypatching or temporary directories.
:::

:::task-stub{title="Compile overarching testing roadmap"}
1. Consolidate findings from the above audits into a single roadmap document under `tests/README.md` detailing sequencing, ownership, and scope for upcoming test suites.
2. Define coding standards for the new tests (naming, fixtures, mocking approach) aligned with repository conventions.
3. Outline continuous integration implications, including any additional dependencies or environment setup steps that tests will require.
4. Provide estimates for effort and identify opportunities to parallelise work across contributors.
:::
