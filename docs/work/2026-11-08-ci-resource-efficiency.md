# CI resource efficiency and Windows acceptance

| Field | Value |
| --- | --- |
| Status | Implemented; hosted acceptance and review pending |
| Owner | ChatGPT / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Delivery | [Draft PR #272](https://github.com/Chain-Frost/ryan-tools/pull/272), branch `ci/resource-efficiency-20261108` |
| Previous head | `1ae1235c2369950c287c4fafe24ef23848880fe0` |

## Scope and decisions

Windows/Python 3.14 remains the target acceptance environment. Ubuntu supplies inexpensive portable preflight and
the existing narrow POSIX filesystem checks. Normal package and developer dependencies remain unchanged. This work
reduces repeated environments and unnecessary starts; it does not remove engineering or installed-wheel acceptance.

The preceding draft added Markdown-aware routing, shorter artifact retention and selective submodule instructions.
This checkpoint makes cheap validation a prerequisite for expensive jobs, consolidates duplicated MCP tests, adds
CI-specific dependency profiles, and documents batched pushes. No merge, release or branch-rule change is authorized.

## Implemented behaviour

- `Ruff and whitespace` now runs inside Python CI before runtime, packaging and native type-check installation.
  It also checks workflow syntax, documentation and logging policy. The independent Fast PR workflow is removed.
- The existing required names remain: `Ruff and whitespace`, `Policy`, `Hosted tests`, `Package` and `MCP smoke`.
  Policy explicitly fails after a failed preflight rather than inheriting a skipped dependency status.
- `repo-scripts/ci_scope.py` classifies complete PR diffs with rename detection disabled. Missing or invalid
  comparisons select full validation and all first-party static inputs. Helper execution errors fail preflight.
  Nonempty, entirely Markdown PR changes can skip runtime jobs; main and manual runs request full validation.
- Scope manifests avoid Windows environment-variable limits. Policy recomputes the same complete-PR inputs locally
  and bounds Pyright command length. Lint/type configuration changes expand the appropriate static-check selection.
- Hosted tests use the new test profile: all 29 runtime requirements, the MCP extra, pytest and pinned PyTUFLOW.
  Unused development tools and the independent QGIS MCP bridge are omitted only from this CI installation.
- The old MCP environment is removed. Its overlapping source tests remain in Hosted tests; explicit profiling,
  compilation and MCP import/export acceptance are retained on Windows. The cheap required MCP gate verifies both
  successful Hosted tests and explicit MCP evidence. Real isolated wheel checks remain in Package.
- The Windows typing profile preserves the full runtime/development/MCP import environment but omits the unused
  QGIS MCP bridge. Package retains its existing focused smoke profile and all real build checks.
- First-attempt PR pytest stops at the first failure. Reruns on the same revision and main runs retain complete
  diagnostics. Existing PR cancellation and per-revision main release validation remain.
- TUFLOW input validation cancels superseded PR runs and starts the exhaustive 161-TCF regression only after its
  API/rejection prerequisites pass. Three distinct pip-cache profiles prevent lightweight jobs from freezing the
  native-wheel cache. Every model report is still collected; retention is seven days.
- AGENTS and canonical guides now require Windows-first acceptance, selective source/fixture checkout, focused
  local checks and one batched push per review/hosted-validation milestone. Normal user installation is unchanged.

See [environment and CI guidance](../ENVIRONMENTS.md#github-actions-ci) for current commands and profiles.

## Evidence and validation

The previous head's [Python CI run 42449907300](https://github.com/Chain-Frost/ryan-tools/actions/runs/42449907300)
passed. Its Hosted job used 280 seconds, including 79 seconds installing GIS/development dependencies and 169 seconds
running pytest. MCP used 133 seconds, including 86 seconds installing dependencies. These are individual observations,
not a controlled saving estimate. Removing the second source-test environment targets that repeated setup.

The initial audit used Linux/Python 3.12.14. Final scope/static checks also ran on Linux/Python 3.14.9; neither
establishes Windows/PowerShell or native GIS acceptance:

- 26 focused real-Git scope tests pass, including cumulative changes, code-to-Markdown renames, deleted files,
  unknown comparisons, configuration changes, excluded submodules, manifest output and manifest-write failure.
- Scope plus existing QGIS setup and installer checks pass: 70 tests without the external QGIS MCP bridge.
- Modified Python files pass Ruff lint/format and strict Pyright with no diagnostics.
- Default documentation/index coverage, all changed-document links and the Python 3.14 logging policy check pass.
- Actionlint passes the final workflow structure. Independent review checked required-status propagation, explicit
  MCP evidence, bounded Pyright arguments, preserved wheel validation and distinct cache hashes.
- Package/developer metadata and the existing hosted selection/exclusions remain unchanged. The only additional
  test file covers the changed CI routing behaviour.

The PR description records the published commit and hosted results after the batched update, avoiding another
source push solely to log CI. Older green runs do not validate a changed head. Review current-head Policy, Hosted,
MCP, Package, preflight, POSIX and targeted TUFLOW results before any merge.

## Acceptance and next action

1. Confirm current-head Windows jobs and preserved required names pass, including the unique profiling/export
   steps and isolated copied-wrapper acceptance.
2. Compare hosted collected/passed/skipped/deselected outcomes against the previous head, accounting for the new
   scope tests. Investigate any new dependency-related skip or missing native capability.
3. Check the new artifact retention and measure installation, test execution, total job time and total runner minutes.
   Do not extrapolate a monthly saving from one run or confuse lower wall time with lower usage.
4. Exercise Markdown-only routing through the stacked dependency-audit handoff. A full source-changing PR still
   requests Windows acceptance; no skip-CI markers or empty commits are needed.
5. Further non-GIS/GIS grouping requires a separate benchmarked change with collection, skip-reason and capability
   parity. Extra jobs repeat setup and may cost more than a shared environment.

Several unrelated work-register rows already have overdue review dates. Their owners and histories are preserved;
this CI task does not reopen or implement those work fronts.
