# Culvert workflow implementation

## Scope

Implement GitHub issues #80 through #85 as one cohesive CulvertMaster-style workflow in `ryan-tools`, delivered through
PR #86 and powered exclusively by the public `culvert_solver` API.

## Status

Implementation complete and pushed to PR #86. The combined #80 through #85 workflow passed the recorded validation
matrix before the final provenance/export follow-up on 2026-09-14. That follow-up was committed through the GitHub
connector and still requires a normal-checkout validation rerun before merge.

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

The combined implementation includes the versioned project contract, expanded design criteria and typed rejection
evidence, distinct compare/report commands, result-driven plotting, imported discharge/headwater event targets, explicit
event tailwater-override provenance, inverse-target residual reporting and richer solver-detail JSON export.

## Validation required before merge

The pre-follow-up branch already passed the combined validation matrix. Re-run the following after the final
provenance/export commits:

- Ruff format/check on modified Python files.
- Strict Pyright on modified Python files.
- `python -m pytest tests/culvert tests/mcp/test_registry.py -q`.
- Maintained wrapper compilation and `--help` smoke check.
- Documentation/link checks, Loguru formatting and `git diff --check`.
- Package build/verification because maintained `ryan_library` code changed after the last recorded build.

## Delivery state

Branch: `feature/culvert-workflow-80`

PR: #86, currently pushed and available for final review/validation.

The last follow-up was intentionally limited to auditability and reporting: imported event tailwater overrides are now
retained explicitly, target-headwater scenarios expose the forward-check residual, and detailed JSON retains additional
public `culvert_solver` result/provenance fields. No new hydraulic equations were introduced.

## Next action

Run the final validation matrix in a normal checkout, resolve any lint/type/test findings, update the PR validation
summary, then merge #86 if review is satisfactory.

## Next review

2026-09-15 or immediately after the final validation rerun.

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

Combined #80 through #85 validation refreshed on 2026-09-14 before the final provenance/export follow-up:

- Ruff format and check passed across the culvert classes, functions, orchestrators, wrapper and focused tests.
- Strict Pyright passed on the modified Python scope with 0 errors and 0 warnings.
- `python -m pytest tests/culvert tests/mcp/test_registry.py -q`: 35 passed.
- Wrapper compilation and headless `--help` passed with solve, analyse, compare, design, rating and report commands.
- Documentation, Loguru-formatting and `git diff --check` passed.
- Package version advanced to `26.9.14.1`; build and no-bump verification produced
  `ryan_functions-26.9.14.1-py3-none-any.whl`.
- Isolated wheel import verified the versioned configuration, imported-event and public inverse-solver boundary without
  loading Matplotlib.

The subsequent GitHub-connector follow-up has **not** been linted, type-checked, tested or rebuilt in this environment.
Those checks are intentionally left to a normal checkout/agent before merge.

The `unsorted` submodule was not inspected or validated. Repository policy, `.gitmodules` and environment guidance now
exclude it categorically from automated work.

## Progress

### 2026-09-13

Completed the acceptance implementation, corrected lint/type issues, expanded engineering provenance and focused
coverage, added MCP discovery, rebuilt the package, and made the `unsorted` exclusion durable. The delivery decision was
then expanded so issues #81 through #85 would be implemented in the same PR before merge.

### 2026-09-14

Separated the GUI from #84 into future aspirational issue #89. PR #86 and #84 retain plotting only; GUI work does not
block the combined #80 through #85 delivery.

Implemented #81 through #85 scope: strict schema-versioned JSON/TOML projects, realistic example, Manning tailwater,
metadata and design criteria; expanded candidate assessment and typed failures; compare and saved-result report commands;
result-only longitudinal/rating plotting; and no-hydrology CSV/list event imports with public inverse solving for target
headwater. Rebuilt and validated package version 26.9.14.1.

A final auditability follow-up then retained event-level tailwater override provenance, added target-headwater forward
residual evidence, expanded scenario JSON with critical/normal depth, adopted roughness/coefficient/loss provenance and
convergence records, and added focused tests/documentation for those additions. That follow-up still requires the final
normal-checkout validation rerun described above.
