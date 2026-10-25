# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
| [TUFLOW proposed-run input preflight](2026-10-21-tuflow-input-preflight.md) | Active | ChatGPT | 2026-10-23 | 2026-10-30 | #156 merged into parent #143 at 298fb13 with all five workflows and final review passing. Publish bounded supported-harness lifecycle/docs cleanup and finish fresh exact-source paired Windows checks. Standalone/API/opt-in launcher are implemented; full 161/174 replay remains required. Earlier Nmx/42-run gates are superseded; native two-BC remains deferred. Parent is not merged to main. |
| [TUFLOW batch preflight optimisation](2026-10-22-tuflow-batch-preflight-optimisation.md) | Active | ChatGPT | 2026-10-23 | 2026-10-30 | Consumer #156 is merged into #143 at 298fb13. Parser #12 at 126ec64 includes constructor safety and the required reference count; 263 focused tests pass on Python 3.12/3.14, paired hosted checks pending. Keep literal-setting/append work and new-SMB-model benchmarking in separate PRs with exact source pairs. Existing three-mode harness has no graph on/off CLI. No production repin or release. |
| [TUFLOW streaming I/O overlap diagnostic](2026-10-25-tuflow-streaming-io-probe.md) | In validation | ChatGPT | 2026-10-25 | 2026-11-01 | PR #161 pinned streaming integration passes; hosted Policy fails on test typing. Local typing repair and lifecycle regressions pass Ruff, strict Pyright and 14 focused tests (2 fork skips); edits remain uncommitted/unpushed. Deliver correction when authorised, verify new-head CI, then assess safe inventory reuse. No merge, publication or native measurement. |
| [Vendor simil and PyHMA integration](2026-10-19-vendor-integration.md) | Needs review | ChatGPT | 2026-10-19 | 2026-10-21 | PR #139: verify exact-head CI, review numerical and wheel verification, then obtain maintainer approval. No merge authorised. |
| [Installed scripts and distribution rename](2026-10-18-installed-scripts.md) | Needs review | ChatGPT | 2026-10-18 | 2026-10-19 | Installed MCP 26.10.18.8 passes live stdio checks without checkout override; 37 tests pass. Restart Codex to reload config; review local edits. |
| [Installed scripts, MCP and pip distribution plan](2026-10-17-installed-scripts-mcp-pip-distribution.md) | In validation (PR #130) | ChatGPT | 2026-10-19 | Exact-head CI | Finish curated script allowlist and copy-helper validation, refresh PR, then review for merge. PRs #118/#119 are independent and not blockers. |
| [PyPI release automation](2026-10-15-release-automation.md) | Needs review | Unassigned | 2026-10-18 | 2026-10-19 | PR #135 open with skip-release; verify all checks and review feedback on its latest head. No merge authorized. |
| [Temporary floodway CI exclusions](2026-10-16-floodway-ci-exclusions.md) | Deferred | Unassigned | 2026-10-16 | 2026-10-22 | Review or restore the two slow sweep tests when requested or at the review date; local tests remain available. |
| [Safe GeoTIFF recompression](2026-10-15-safe-geotiff-recompression.md) | Ready for review | ChatGPT | 2026-10-15 | 2026-10-22 | Review and merge the pull request after CI passes. |
| [TUFLOW shared filesystem verification](2026-10-15-tuflow-shared-verification-110.md) | In validation | ChatGPT | 2026-10-15 | 2026-10-22 | Review and commit local CRS test correction and verified 26.10.15.4 wheel; confirm CI on the resulting PR #116 head before merging. |
| [Culvert analysis/design workflow](2026-09-13-culvert-workflow-80.md) | Ready for final validation | ChatGPT | 2026-09-14 | After final validation rerun | Re-run Ruff, strict Pyright, focused pytest, wrapper/documentation checks and package verification after the final provenance/export follow-up; merge PR #86 only after those checks pass. |
| [Floodway design and reporting workflow](2026-09-13-floodway-design-87.md) | Needs review | Unassigned | 2026-10-06 | 2026-10-13 | Current PR head a90bbae checked; local lint repairs and verified 26.10.6.2 wheel unstaged. Review and commit repairs; historical full-suite failures tracked in #95. |
| [Repository improvement backlog](../REPOSITORY_IMPROVEMENT_ROADMAP.md) | Deferred | Unassigned | 2026-10-06 | 2027-04-05 | No repository-wide improvement task is selected. If a maintainer selects a bounded opportunity earlier, register that implementation front separately; otherwise do not surface this backlog as due before the review date. |
| [Unsorted migration](../UNSORTED_UPGRADE_ROADMAP.md) | Deferred | Unassigned | 2026-10-19 | 2027-04-05 | Bounded documentation review reconciled maintained raster/VRT tools, the vendored simil algorithm and file-inventory reuse; no migration resumed. Future implementation only when explicitly selected. |
| [Early compatibility removals](../COMPATIBILITY_POLICY.md) | Implemented; awaiting PR merge | ChatGPT | 2026-10-18 | PR #120 validation | Deleted unused scripts, GDAL/data-processing modules and POMM shim APIs; review CI and breaking-change release notes. |

Initial register created on 2026-09-06 from the roadmap review. These are known follow-ups, not an exhaustive audit or
an instruction to start them all. Existing staged code changes are not inferred to be separately authorized work
fronts.

## Closed work

| Work front / status record | Status | Owner | Updated | Next review | Outcome |
| --- | --- | --- | --- | --- | --- |
| [TUFLOW culvert engine migration](2026-10-08-tuflow-culvert-engines-99.md) | Complete | Unassigned | 2026-10-17 | None | PR #100 squash-merged as 6283a9c after all five checks passed at 8b386d8; final-review fixes included. Engineering follow-ups #102/#103 deferred. |
| [Fast PR checks and hosted timing](2026-10-17-fast-pr-checks-129.md) | Complete | ChatGPT | 2026-10-17 | None | All five required checks passed on a2c26dc; PR #129 squash-merged as 037cff3. Main ruleset requires Ruff and whitespace; tag ruleset unchanged. |
| [Mapped-drive-safe paths, raster and archive progress](2026-10-16-absolute-path-policy.md) | Complete | ChatGPT | 2026-10-16 | None | PR #128 validated at `08b00de0`: all four required CI jobs passed; verified 26.10.16.2 wheel uses latest hydraulic dependency releases and is authorized for merge. |
| [CI validation efficiency](2026-10-15-ci-validation.md) | Complete | ChatGPT | 2026-10-16 | None | PR #125 and release #126 merged; four main CI jobs and both publication jobs passed; public 26.10.16.1 wheel smoke passed. |
| [GitHub CI implementation](2026-10-07-github-ci.md) | Complete | ChatGPT | 2026-10-07 | None | Added Windows/Python 3.14 Policy, Hosted tests and Package jobs with explicit capability boundaries. Run #7 at `ad69ef4` passed all three jobs; Hosted tests reported 937 passed and 5 deselected. PR #96 is ready for maintainer review and manual merge. |
| [Culvert uncertainty/sensitivity workflow](2026-10-06-culvert-uncertainty-91.md) | Complete | ChatGPT | 2026-10-06 | None | 144 focused tests and policy checks pass; verified 26.10.6.3 wheel and installed-wrapper smoke pass. Final artifact published in 35035b8; authorized PR #94 squash merge follows final head check. |
| [Transactional package build and verification](2026-09-13-transactional-packaging.md) | Complete | Unassigned | 2026-09-13 | — | Added no-bump transactional builds, wheel verification, installed-wheel smoke coverage and focused failure-path tests; recorded upstream follow-ups. |
| [TUFLOW statistic-then-maximum raster workflow](2026-09-09-tuflow-stat-then-maximum.md) | Complete | Unassigned | 2026-09-10 | — | Shared mean/median orchestration, configurable ASC_to_ASC-default mean selection, flattened TP provenance and compatibility shims completed. |

Completed historical milestones remain in the
[repository roadmap](../REPOSITORY_IMPROVEMENT_ROADMAP.md). Keep detailed records discoverable through the
documentation index.
