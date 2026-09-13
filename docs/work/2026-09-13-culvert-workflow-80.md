# Culvert workflow implementation

## Scope

Implement the first usable increment of GitHub issue #80: a CulvertMaster-style solve/analyse/design workflow in `ryan-tools` powered exclusively by the public `culvert_solver` API.

## Status

In progress.

## Architectural placement

- Typed project/configuration/result models: `ryan_library/classes/culvert/`
- Reusable adapter, candidate generation, assessment, and export helpers: `ryan_library/functions/culvert/`
- End-to-end solve/analyse/design/rating/report coordination: `ryan_library/orchestrators/culvert/`
- Maintained human-facing wrapper: `ryan-scripts/culvert.py`
- Focused tests: `tests/culvert/`

No culvert hydraulic equations are to be duplicated in `ryan-tools`; all hydraulic calculations must delegate to `culvert_solver`.

## Current milestone

Create the typed domain model, bounded `culvert_solver` adapter, solve/analyse/design orchestration, machine-readable exports, CLI wrapper, and focused synthetic tests.

## Validation required

- Ruff format/check on modified Python files.
- Strict Pyright on modified Python files.
- Focused pytest coverage for the integration boundary and workflows.
- Maintained wrapper compilation and `--help` smoke check.
- Package build/verification because maintained `ryan_library` code is changing.

## Delivery state

Branch: `feature/culvert-workflow-80`

PR: pending.

## Next action

Implement the first usable workflow increment and validate it against the vendored `culvert_solver` API.

## Next review

2026-09-14
