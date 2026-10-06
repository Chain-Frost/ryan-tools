# Culvert uncertainty and sensitivity workflow

| Field | Value |
| --- | --- |
| Status | Complete (local implementation and validation) |
| Owner | ChatGPT |
| Created | 2026-10-06 |
| Updated | 2026-10-06 |
| Next review | — |
| Baseline | `feature/culvert-uncertainty-91` from `main` at `1d8aa177be7b074003e0e355b5db56f4cd764e99`; `vendor/ryan_culverts` initially at `00f8274b4702bfe721d111436a15ae435f672460` |

## Outcome and scope

Implement GitHub issue #91 as the application/orchestration layer for culvert uncertainty and sensitivity analysis.
The workflow consumes public uncertainty contracts and deterministic hydraulic solver APIs from `ryan-culverts`
rather than defining competing hydraulic-domain classes or equations.

Upstream dependency: `ryan-culverts` issue #19 was implemented and merged through PR #25 at
`0213eac2bf10b5751feb13ae7d88d00026eeb9f1`.

## Current state

The initial issue #91 workflow is implemented on `feature/culvert-uncertainty-91` in `E:\Github\ryan-tools`.
`UncertaintyStudy` owns selection, sampling and aggregation policy, reusing public bounds/distribution/sample contracts.
Bounded Cartesian sweeps and seeded Monte Carlo samples are evaluated across selected base crossings, alternatives and
scenarios through the authoritative public crossing solver. Sourced roughness and entrance loss apply to all groups;
sampled discharge and tailwater replace absolute crossing inputs.

All valid, advisory, approximate, unresolved and failed outcomes retain their evidence. Conditional metric summaries
include complete status counts, denominators, envelopes, percentiles and governing sample IDs. The default statistics
population contains valid and advisory results; approximate inclusion is explicit, and unresolved/failed results remain
visible outside statistics. Evaluation limits are checked before sampling/solving.

Strict JSON/TOML project loading, the maintained `uncertainty --study` wrapper, CSV/JSON/Markdown outputs, the TOML
example and MCP workflow discovery are integrated. API-supplied public rating-curve boundaries retain their definitions
and out-of-range sample failures. Sampled tailwater supersedes an imported event override without losing its original
provenance. See the [workflow contract](../CULVERT_WORKFLOW.md#uncertainty-and-sensitivity-studies).

The user-authorized `vendor/run_hy8` update is included at `0baaa2ac83e38aaad1a0419dc6e39c40445cb98d`.
`vendor/ryan_culverts` remains at `0213eac2bf10b5751feb13ae7d88d00026eeb9f1`. Neither submodule has local edits.

The existing culvert application boundary remains the integration point:
`CulvertProject` / crossing / scenario models -> solver adapter -> public `culvert_solver` APIs.

No uncertainty-domain distribution, bounds or sampled-parameter classes are reimplemented in `ryan-tools`.

## Next action

Review the completed local commits and publish them to the existing draft PR when requested. No implementation or
validation step remains for this initial workflow. No push, PR mutation or merge was performed in this session.
The user explicitly permits commits on this branch and prohibits merging.

Group-specific variations, correlated distributions, solved capacity studies and automatic engineering acceptance or
alternative ranking are outside the initial contract. The existing design-search workflow owns acceptance/ranking;
uncertainty reports imposed flows and conditional hydraulic output envelopes, not inferred capacity or confidence limits.

## Completion criteria

- Public `ryan-culverts` uncertainty contracts are consumed directly.
- Seeded stochastic studies are reproducible.
- Deterministic bounded sweeps do not require random sampling.
- Failed, unresolved, approximate and advisory solver outcomes remain visible.
- Base crossings and alternatives can be evaluated across selected scenarios.
- CSV/JSON outputs and a concise review summary preserve sample provenance and solver warning/status information.
- Focused Ruff, strict Pyright, pytest, wrapper/documentation checks and package verification pass.

## Validation and delivery

2026-10-06 local validation uses the user's normal Python 3.14.6, Ruff 0.16.6 and strict Pyright 1.1.411.
The earlier connector-only sampling handoff had not run checks; it is superseded by this evidence:

- `python -m pytest tests/culvert tests/mcp/test_registry.py -q --tb=short`: **58 passed**. This includes the real
  24-evaluation crossing/scenario/alternative matrix, source/flow-split retention, all status classes, expected failures,
  unexpected-error propagation, seeded complete-study reproducibility, selectors/limits, JSON round trips and wrapper
  success/configuration/review-exit behavior.
- `python -m ruff check ryan_library/classes/culvert ryan_library/functions/culvert ryan_library/orchestrators/culvert
  ryan-scripts/culvert.py tests/culvert/test_uncertainty_study.py tests/culvert/test_uncertainty_workflow.py
  tests/culvert/test_wrapper.py tests/mcp/test_registry.py`: **passed**.
- `ruff format --check` and `python -m pyright` on the 17 modified Python files: **passed**, Pyright **0 errors**.
  Targets: the three modified/new class modules and class package initializer; function package initializer, config,
  sampling, evaluation, export and statistics modules; orchestrator package initializer, study and report modules;
  maintained wrapper; uncertainty workflow, wrapper and MCP registry tests.
- `python repo-scripts/check_loguru_formatting.py`: **passed**.
- `python repo-scripts/check_documentation.py`: **passed** (seven READMEs and index coverage), plus explicit
  `--links-only` checks of the workflow, MCP, example and status documents.
- `python -m compileall -q ryan-scripts/culvert.py` and wrapper `--help`: **passed**. Source smoke commands use
  `PYTHONPATH=.;vendor/ryan_culverts/src;vendor/run_hy8/src` to avoid the older separately installed solver.
- In `vendor/run_hy8`, with `PYTHONPATH=src`,
  `python -m pytest tests/test_roadway.py tests/test_results.py tests/test_reader.py -q -m 'not requires_hy8'`:
  **77 passed, 23 deselected**. No HY-8 executable or external hydraulic parity result is claimed.
- `python repo-scripts/build_library.py --skip-pip --version 26.10.6.1`: **passed**; a final verification rebuild with
  `--skip-pip --no-bump` also **passed**. Promoted wheel:
  `dist/ryan_functions-26.10.6.1-py3-none-any.whl`, **638118 bytes**,
  SHA-256 `e3f31cdbd550b3d919a6b99448c5cab58def9624ee65070624147ba0b25b8d71`.
- Installed that wheel with `pip install --no-deps --target` in a temporary directory, then ran
  `repo-scripts/smoke_test_installed_wheel.py --expected-root` outside the checkout: **passed**. A copied wrapper ran
  the complete TOML roughness example using only the isolated installation: **12 evaluations, 0 excluded, exit 0**;
  all four outputs present. Imports for the study module, `culvert_solver` and `run_hy8` resolved within the installation.
- `git diff --check`: **passed**. No unrelated full repository suite was run.

Branch: `feature/culvert-uncertainty-91`.

Implementation commit: `f27201c38561f639b49be6602278f99715f583f5`.
Included HY-8 pointer commit: `2bb912b`.
The completion/status documentation is committed separately on the same branch. Delivery is local commits; no files
remain staged after committing, and nothing was pushed or merged.

Draft PR: [#94](https://github.com/Chain-Frost/ryan-tools/pull/94)
(`[core] Add culvert uncertainty and sensitivity workflow`), verified still draft on 2026-10-06. Its remote description
and eight-commit head still describe the earlier sampling increment; they have not been updated with these local commits.

## Progress

### 2026-10-06

Created the feature branch from current `main`, confirmed upstream PR #25 is merged, mapped the existing culvert
application/solver boundary, and advanced the vendored solver pointer to the merged uncertainty-contract commit.

Added the first typed application increment: project-level uncertainty study policy and multi-parameter bounded/seeded
sampling orchestration built exclusively from public `ryan-culverts` uncertainty contracts, with focused tests for
bounded Cartesian sampling and seeded reproducibility.

### 2026-10-06 local completion

Resumed the actual PR branch at `950cd49`. Fixed the original sampling complexity/type-check failures and completed
evaluation, conditional aggregation, exports, project/wrapper integration, discoverability and focused failure/matrix
coverage. Work was carried out in the requested existing checkout; the user-authorized staged HY-8 pointer was committed
with its validation evidence. Built and verified the package and proved the copied-wrapper installed-package path.
No merge or remote publication was performed. The unrelated backlog/#80 review entries remain unchanged.
