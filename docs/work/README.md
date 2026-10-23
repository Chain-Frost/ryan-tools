# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
| [Native TUFLOW graph benchmark](2026-10-23-tuflow-native-graph-benchmark.md) | Active | ChatGPT | 2026-10-23 | 2026-10-30 | PR #158: four-mode graph controls implemented; focused Python 3.14 validation passes. Complete exact-head Windows 161-TCF/174-run archive and Codex review gates, then provide the frozen command for the new private SMB workload. Parser baseline e162c04; public preflight remains a separate untimed default-batch gate. |
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
| [TUFLOW proposed-run input preflight](2026-10-21-tuflow-input-preflight.md) | Complete | ChatGPT | 2026-10-23 | — | Bounded #143 integration, including #156, delivered from final source fa6ae1e; the record owns the mainline merge identity. Standalone/API/opt-in launcher and supported 161/174 regression harness are delivered. Native benchmarks and broader support remain separate follow-ups. |
| [TUFLOW batch preflight optimisation](2026-10-22-tuflow-batch-preflight-optimisation.md) | Complete | ChatGPT | 2026-10-23 | — | Parser #12 merged at e162c043 with the tested 126ec64 tree; consumer #156/#143 integration and diagnostic memoisation delivered. Parser #14/#15 and the planned native graph comparison own further optimisation. No old-model rerun gate or new performance claim. |
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
