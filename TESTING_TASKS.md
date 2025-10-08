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
