# Culvert workflow implementation

## Scope

Implement the first usable increment of GitHub issue #80: a CulvertMaster-style solve/analyse/design workflow in `ryan-tools` powered exclusively by the public `culvert_solver` API.

## Status

Active. The implementation and local acceptance validation are complete; reviewed changes still need to be committed and
pushed to draft PR #86.

## Architectural placement

- Typed project/configuration/result models: `ryan_library/classes/culvert/`
- Reusable adapter, candidate generation, assessment, and export helpers: `ryan_library/functions/culvert/`
- End-to-end solve/analyse/design/rating/report coordination: `ryan_library/orchestrators/culvert/`
- Maintained human-facing wrapper: `ryan-scripts/culvert.py`
- Focused tests: `tests/culvert/`

No culvert hydraulic equations are to be duplicated in `ryan-tools`; all hydraulic calculations must delegate to `culvert_solver`.

## Current milestone

The first usable increment is implemented. Design JSON now retains applied criteria and full candidate definitions;
scenario and rating JSON retain structured warning messages, applicability sources and tailwater-resolution provenance.
The maintained wrapper is also discoverable through the MCP workflow catalogue.

## Validation required

- Ruff format/check on modified Python files.
- Strict Pyright on modified Python files.
- Focused pytest coverage for the integration boundary and workflows.
- Maintained wrapper compilation and `--help` smoke check.
- Package build/verification because maintained `ryan_library` code is changing.

## Delivery state

Branch: `feature/culvert-workflow-80`

PR: draft #86, with the local branch synchronized to its remote baseline before the current changes.

Local delivery: changes are unstaged and uncommitted except for a separately staged `vendor/ryan_culverts` gitlink
update that appeared during the session and was preserved. The temporary branch-only validation workflow is deleted
locally. Nothing from this session has been pushed.

## Next action

Review the local diff and separately staged solver gitlink, then commit and push the accepted changes to PR #86. Remove
draft status after the pushed head reflects the recorded validation.

## Next review

2026-09-14

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

The `unsorted` submodule was not inspected or validated. Repository policy, `.gitmodules` and environment guidance now
exclude it categorically from automated work.

## Progress

### 2026-09-13

Completed the acceptance implementation, corrected lint/type issues, expanded engineering provenance and focused
coverage, added MCP discovery, rebuilt the package, and made the `unsorted` exclusion durable. The remaining action is
Git delivery and final PR review.
