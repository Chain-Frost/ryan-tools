# Culvert workflow implementation

## Scope

Implement GitHub issues #80 through #85 as one cohesive CulvertMaster-style workflow in `ryan-tools`, delivered through
draft PR #86 and powered exclusively by the public `culvert_solver` API.

## Status

Active. The agreed local implementation for issues #80 through #85 is complete and passes the combined validation
matrix. PR #86 remains draft because these changes are not yet committed, pushed or reviewed on the remote branch.

GUI work has been separated into future aspirational GitHub issue #89. It is outside PR #86 and does not block this
work front.

## Architectural placement

- Typed project/configuration/result models: `ryan_library/classes/culvert/`
- Reusable adapter, candidate generation, assessment, and export helpers: `ryan_library/functions/culvert/`
- End-to-end solve/analyse/design/rating/report coordination: `ryan_library/orchestrators/culvert/`
- Maintained human-facing wrapper: `ryan-scripts/culvert.py`
- Focused tests: `tests/culvert/`

No culvert hydraulic equations are to be duplicated in `ryan-tools`; all hydraulic calculations must delegate to `culvert_solver`.

## Current milestone

The combined implementation now includes the versioned project contract, expanded design criteria and typed rejection
evidence, distinct compare/report commands, result-driven plotting, and imported discharge/headwater event targets.

## Validation required

- Ruff format/check on modified Python files.
- Strict Pyright on modified Python files.
- Focused pytest coverage for the integration boundary and workflows.
- Maintained wrapper compilation and `--help` smoke check.
- Package build/verification because maintained `ryan_library` code is changing.

## Delivery state

Branch: `feature/culvert-workflow-80`

PR: draft #86, with the local branch synchronized to its remote baseline before the current changes.

Local delivery: the current #81 through #85 changes are unstaged and uncommitted. The preceding #80 increment is in
local commit `6c01d84`, which is also ahead of the remote PR head. Nothing from this resumed session has been pushed.

## Next action

Review the complete combined diff, then commit and push the accepted #81 through #85 implementation to PR #86. Keep the
PR draft until its remote head and checks reflect the recorded validation; do not merge before final review.

## Next review

2026-09-15

## Validation and delivery

Validated on Windows with the user's Python 3.14 installation on 2026-09-13:

- Ruff format and check passed on all modified Python files.
- Strict Pyright passed on all modified Python files with 0 errors and 0 warnings.
- `python -m pytest tests/culvert tests/mcp/test_registry.py -q`: 25 passed, including real wrapper success and
  missing-directory process-boundary checks.
- Wrapper compilation and `python ryan-scripts/culvert.py --help` passed.
- Repository documentation, touched-document link checks, Loguru formatting and `git diff --check` passed.
- Versioned package build produced and verified `ryan_functions-26.9.13.1-py3-none-any.whl`; a no-bump rebuild against
  the current solver checkout also passed.
- Isolated wheel imports, a real crossing solve and packaged MCP catalogue discovery passed without source-checkout
  imports.

Combined #80 through #85 validation refreshed on 2026-09-14:

- Ruff format and check passed across the culvert classes, functions, orchestrators, wrapper and focused tests.
- Strict Pyright passed on the modified Python scope with 0 errors and 0 warnings.
- `python -m pytest tests/culvert tests/mcp/test_registry.py -q`: 35 passed.
- Wrapper compilation and headless `--help` passed with solve, analyse, compare, design, rating and report commands.
- Documentation, Loguru-formatting and `git diff --check` passed.
- Package version advanced to `26.9.14.1`; build and no-bump verification produced
  `ryan_functions-26.9.14.1-py3-none-any.whl`.
- Isolated wheel import verified the versioned configuration, imported-event and public inverse-solver boundary without
  loading Matplotlib.

The `unsorted` submodule was not inspected or validated. Repository policy, `.gitmodules` and environment guidance now
exclude it categorically from automated work.

## Progress

### 2026-09-13

Completed the acceptance implementation, corrected lint/type issues, expanded engineering provenance and focused
coverage, added MCP discovery, rebuilt the package, and made the `unsorted` exclusion durable. The delivery decision was
then expanded: PR #86 will remain draft and will not merge until issues #81 through #85 are implemented in the same PR.

### 2026-09-14

Separated the GUI from #84 into future aspirational issue #89. PR #86 and #84 now retain plotting only; GUI work does
not block the combined #80 through #85 delivery.

Implemented the remaining local #81 through #85 scope: strict schema-versioned JSON/TOML projects, realistic example,
Manning tailwater, metadata and design criteria; expanded candidate assessment and typed failures; compare and
saved-result report commands; result-only longitudinal/rating plotting; and no-hydrology CSV/list event imports with
public inverse solving for target headwater. Rebuilt and validated package version 26.9.14.1.
