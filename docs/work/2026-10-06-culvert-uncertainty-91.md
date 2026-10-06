# Culvert uncertainty and sensitivity workflow

| Field | Value |
| --- | --- |
| Status | Ready for final review |
| Owner | ChatGPT |
| Created | 2026-10-06 |
| Updated | 2026-10-06 |
| Next review | Final PR review |
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

Post-review-fix validation is complete. After merging current `main` into the feature branch, the final synced source
was revalidated with the expanded 65-test culvert/MCP suite, lint/type/documentation checks, package verification and
isolated installed-wheel smoke. The current bundled distribution is `26.10.6.2`. A final review follow-up corrected
the wrapper's concise uncertainty count label from "samples" to "evaluations"; because this changes wrapper-visible
output, `WRAPPER_VERSION` is now `2026-10-06.3`. The matching wrapper regression assertion was updated. This wrapper
file is not packaged into the wheel, so the validated `26.10.6.2` library artifact is unchanged.

## Next action

Obtain final PR review against the published head. PR #94 is synced with current `main`, mergeable, and remains
intentionally unmerged. The package validation remains current; the final wrapper-only wording change has a matching
regression assertion and should receive the normal focused wrapper check before merge if a local runner is available.

The latest Codex review identified that empty `alternative_names` meant "all alternatives" with no way to express a
base-crossings-only study. The application policy now adds `include_alternatives` (default `true`); setting it to
`false` selects zero alternatives while preserving existing empty-selector behavior for enabled target classes.

The earlier target-headwater event and stale-wheel review findings were addressed and validated at `ca31687`. The `include_alternatives` selection-policy change is now validated in the refreshed `26.10.6.2` wheel;
the earlier `26.10.6.1` wheel evidence remains historical.

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

### Post-review-fix validation on 2026-10-06

Validated the `1c445e58a0cb604e4241cb569194df259dfc3fd1` review-fix checkout, then reran affected checks after the
wrapper helper extraction and formatting fix. The final tested implementation and refreshed wheel are committed as
`ca31687c59924e2a631c2fc22b54e4c9184ff886` on the existing `feature/culvert-uncertainty-91` branch in `E:\Github\ryan-tools`.
Environment: normal user Python **3.14.6**, Ruff **0.16.6**, strict Pyright **1.1.411**.

- `python -m pytest tests/culvert tests/mcp/test_registry.py -q --tb=short`: **60 passed**, including the selected-crossing
  target-headwater regression and ambiguous-target rejection. The complete focused suite passed again after extraction.
- `python -m ruff check ryan_library/classes/culvert ryan_library/functions/culvert ryan_library/orchestrators/culvert
  ryan-scripts/culvert.py tests/culvert tests/mcp/test_registry.py`: **passed**. The first run found C901 in wrapper `main`;
  the helper extraction corrected it without changing event selection or hydraulic behavior.
- `python -m ruff format --check ryan-scripts/culvert.py ryan_library/orchestrators/culvert/__init__.py
  ryan_library/orchestrators/culvert/uncertainty.py tests/culvert/test_wrapper.py`: **four files already formatted** after
  fixing the first-run regression-fixture whitespace failure.
- `python -m pyright ryan-scripts/culvert.py ryan_library/orchestrators/culvert/__init__.py
  ryan_library/orchestrators/culvert/uncertainty.py tests/culvert/test_wrapper.py`: **0 errors, 0 warnings**.
  Only the four Python files modified since the earlier validation baseline were checked.
- `python repo-scripts/check_loguru_formatting.py`: **passed**.
- `python repo-scripts/check_documentation.py`: **passed**; explicit `--links-only` checks of the workflow, work record,
  register and central index also **passed**.
- `python -m compileall -q ryan-scripts/culvert.py` and wrapper `--help`: **passed**. Source commands used
  `PYTHONPATH=.;vendor/ryan_culverts/src;vendor/run_hy8/src` to avoid older separately installed solver code.
- In `vendor/run_hy8`, with `PYTHONPATH=src`,
  `python -m pytest tests/test_roadway.py tests/test_results.py tests/test_reader.py -q -m 'not requires_hy8'`:
  **77 passed, 23 deselected**. This is package-level validation; executable-dependent HY-8 parity was not run.
- `python repo-scripts/build_library.py --skip-pip --no-bump`: **passed** with candidate verification and promotion.
  `pyproject.toml` was unchanged. Refreshed `dist/ryan_functions-26.10.6.1-py3-none-any.whl`: **638194 bytes**,
  SHA-256 `77a2a2f39e7915905cf7a955531cbed9d42c9746b1edea72a525db0e0f4c4228`.
- Installed this wheel with `pip install --no-deps --target` into temporary storage and ran
  `repo-scripts/smoke_test_installed_wheel.py --expected-root` from outside the checkout: **passed**. Confirmed study,
  `culvert_solver` and `run_hy8` imports resolve inside that installation. Copied-wrapper tests using version
  `2026-10-06.2` passed for the following cases:
  - complete TOML example: **12 evaluations**, all four outputs, exit **0**;
  - selected large crossing and target-headwater CSV: **one evaluation**, residual below **1e-4 m**, exit **0**;
  - selected alternative-only study and target-headwater CSV: **one evaluation**, residual below **1e-4 m**, exit **0**;
  - ambiguous multi-crossing target event: explicit rejection, exit **1**, no result file;
  - discharge-only CSV over both differently sized crossings: **two evaluations**, each at the supplied flow, exit **0**.
- `git diff --check`: **passed**. No unrelated full repository suite was run.

The validation follow-up has now been published to PR #94. The rebuilt wheel is committed, and the installed-package
and copied-wrapper checks recorded above are the current validation evidence. PR #94 remains unmerged.

### Earlier validation baseline (historical)

2026-10-06 validation at commit `70af395` used the user's normal Python 3.14.6, Ruff 0.16.6 and strict Pyright 1.1.411.
The earlier connector-only sampling handoff had not run checks; it is superseded by this evidence. These checks predate
the subsequent Codex review fix for target-headwater event selection and therefore must be rerun on the new head:

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

Validated implementation baseline: `70af395`.
Included HY-8 pointer commit: `2bb912b`.
The branch is synced to GitHub through the post-validation merge head. PR #94 is ready for final review and remains
unmerged. The earlier wheel/hash below are historical.

PR: [#94](https://github.com/Chain-Frost/ryan-tools/pull/94)
(`[core] Add culvert uncertainty and sensitivity workflow`).

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

### 2026-10-06 review follow-up

After the branch was synced, Codex review identified that uncertainty `--events-csv` target-headwater rows could be
materialized using the first project crossing rather than the study-selected hydraulic target. The wrapper now resolves
those inverse events only against the study's sole selected crossing/alternative and rejects ambiguous multi-target
studies. Focused wrapper regression tests cover both the selected-crossing path and the explicit ambiguity rejection.
The stale delivery/work-register language was also corrected. Revalidation is intentionally delegated to a separate agent.

### 2026-10-06 post-review-fix validation

Completed the requested revalidation in the existing PR checkout. Fixed Ruff complexity and formatting failures,
reran the focused checks, rebuilt the wheel without changing its declared version, and proved the selected-crossing,
alternative-only, ambiguity-rejection and discharge-only behaviors using a copied wrapper against an isolated installed
wheel. Committed the source/format/artifact follow-up as `ca31687`; this record and the register now separate current
passing validation from the historical `70af395` baseline. No push or merge was performed.


### 2026-10-06 alternative-selection review follow-up

A subsequent Codex review found that studies could disable base crossings but could not disable alternatives. Added the
explicit `include_alternatives` policy flag, defaulting to `true` for backwards compatibility. The base and alternative
target classes now use symmetric enable/disable semantics: disabled classes select nothing and reject contradictory named
selectors. Configuration/serialization preserve the new flag, and focused tests cover base-only studies, round trips,
contradictory selectors and the no-target failure path. This change
postdates the recorded 60-test/wheel validation and therefore requires revalidation and a refreshed bundled wheel.

### 2026-10-06 PR #94 merge-conflict resolution

Resolved the local merge of `origin/main` (`beb1011`) into PR head `c404635`. The squash landing of PR #86
caused shared culvert files to appear independently added. Retained the PR uncertainty implementation and documentation;
the main-side solver pointer is an ancestor of retained `0213eac`. Replaced conflicting legacy wheels with
`dist/ryan_functions-26.10.6.2-py3-none-any.whl`, and corrected two formatting issues in the latest selection-policy changes.

Validation in normal user Python 3.14:

- `python -m pytest tests/culvert tests/mcp/test_registry.py -q --tb=short`: 65 passed.
- Focused culvert/MCP Ruff lint and formatting, strict Pyright on affected Python files, and Loguru policy: passed.
- `python repo-scripts/build_library.py --skip-pip`, followed after formatting by `--skip-pip --no-bump`: passed.
- Wheel verification: 638356 bytes; SHA-256 `603b1cbc9a0307e15ecfc449f907fe3c917d07ce74df9722e358024e1b1a4a31`.
- Isolated target install proved all three public packages import below the install root; copied-wrapper TOML
  base-only uncertainty smoke: six evaluations, no alternatives, exit 0.
- Default documentation/index check, explicit links-only check of six affected documents, and `git diff --check`: passed.
- `git ls-files -u` and `git diff --cached --stat`: empty; all conflict entries cleared and nothing staged.

Git delivery: the resolved merge was committed and pushed as `cfd621a0e9e4bfbeb7ae73938b4b82a655c5f13f`.
The branch is now based on current `main`, GitHub reports PR #94 as mergeable, and the refreshed
`dist/ryan_functions-26.10.6.2-py3-none-any.whl` is published on the branch. Neither solver submodule was changed
locally during conflict resolution. Next action: final PR review; do not merge without an explicit request.


### 2026-10-06 final review wording follow-up

Codex identified that the wrapper logged the full target × scenario × sample matrix count as "uncertainty samples".
The summary now calls these "uncertainty evaluations", matching the actual `len(result.evaluations)` semantics.
`WRAPPER_VERSION` was bumped to `2026-10-06.3` per the maintained wrapper standard and the existing wrapper regression
assertion now checks the corrected wording. No `ryan_library` or packaged module changed, so the previously verified
`26.10.6.2` wheel remains the current package artifact.
