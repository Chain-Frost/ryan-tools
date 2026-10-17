# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
| [TUFLOW culvert engine migration](2026-10-08-tuflow-culvert-engines-99.md) | Blocked | Unassigned | 2026-10-08 | 2026-10-15 | Wheel 26.10.8.4 and 15 HY-8 comparisons validated against run-hy8 28e7909; locate representative Maximums, mixed 1d_nwk and blockage matrix, publish wheel and pass final-head CI. |
| [Fast PR checks and hosted timing](2026-10-17-fast-pr-checks-129.md) | In validation | ChatGPT | 2026-10-17 | 2026-10-22 | Commit/push documentation, verify all five checks on the new PR #129 head, then perform the authorized squash merge. |
| [Temporary floodway CI exclusions](2026-10-16-floodway-ci-exclusions.md) | Deferred | Unassigned | 2026-10-16 | 2026-10-22 | Review or restore the two slow sweep tests when requested or at the review date; local tests remain available. |
| [Safe GeoTIFF recompression](2026-10-15-safe-geotiff-recompression.md) | Ready for review | ChatGPT | 2026-10-15 | 2026-10-22 | Review and merge the pull request after CI passes. |
| [TUFLOW shared filesystem verification](2026-10-15-tuflow-shared-verification-110.md) | In validation | ChatGPT | 2026-10-15 | 2026-10-22 | Review and commit local CRS test correction and verified 26.10.15.4 wheel; confirm CI on the resulting PR #116 head before merging. |
| [Culvert analysis/design workflow](2026-09-13-culvert-workflow-80.md) | Ready for final validation | ChatGPT | 2026-09-14 | After final validation rerun | Re-run Ruff, strict Pyright, focused pytest, wrapper/documentation checks and package verification after the final provenance/export follow-up; merge PR #86 only after those checks pass. |
| [Floodway design and reporting workflow](2026-09-13-floodway-design-87.md) | Needs review | Unassigned | 2026-10-06 | 2026-10-13 | Current PR head a90bbae checked; local lint repairs and verified 26.10.6.2 wheel unstaged. Review and commit repairs; historical full-suite failures tracked in #95. |
| [Repository improvement backlog](../REPOSITORY_IMPROVEMENT_ROADMAP.md) | Deferred | Unassigned | 2026-10-06 | 2027-04-05 | No repository-wide improvement task is selected. If a maintainer selects a bounded opportunity earlier, register that implementation front separately; otherwise do not surface this backlog as due before the review date. |
| [Unsorted migration](../UNSORTED_UPGRADE_ROADMAP.md) | Deferred | Unassigned | 2026-10-07 | 2027-04-05 | Migration remains incomplete but is not actively progressed. Resume only when explicitly selected; otherwise do not surface it as due before the review date. |
| [Scheduled compatibility removals](../COMPATIBILITY_POLICY.md) | Deferred | Unassigned | 2026-09-06 | 2027-01-04 | Support runs through 2026-12-31; then verify callers and select removals using the inventory. |

Initial register created on 2026-09-06 from the roadmap review. These are known follow-ups, not an exhaustive audit or
an instruction to start them all. Existing staged code changes are not inferred to be separately authorized work
fronts.

## Closed work

| Work front / status record | Status | Owner | Updated | Next review | Outcome |
| --- | --- | --- | --- | --- | --- |
| [Mapped-drive-safe paths, raster and archive progress](2026-10-16-absolute-path-policy.md) | Complete | ChatGPT | 2026-10-16 | None | PR #128 validated at `08b00de0`: all four required CI jobs passed; verified 26.10.16.2 wheel uses latest hydraulic dependency releases and is authorized for merge. |
| [CI validation efficiency](2026-10-15-ci-validation.md) | Complete | ChatGPT | 2026-10-16 | None | PR #125 and release #126 merged; four main CI jobs and both publication jobs passed; public 26.10.16.1 wheel smoke passed. |
| [PyPI release automation](2026-10-15-release-automation.md) | Complete | ChatGPT | 2026-10-16 | None | PRs #123/#124 merged; 26.10.15.5 published via OIDC and downloaded-wheel smoke passed. Separate improvements in PR #125. |
| [GitHub CI implementation](2026-10-07-github-ci.md) | Complete | ChatGPT | 2026-10-07 | None | Added Windows/Python 3.14 Policy, Hosted tests and Package jobs with explicit capability boundaries. Run #7 at `ad69ef4` passed all three jobs; Hosted tests reported 937 passed and 5 deselected. PR #96 is ready for maintainer review and manual merge. |
| [Culvert uncertainty/sensitivity workflow](2026-10-06-culvert-uncertainty-91.md) | Complete | ChatGPT | 2026-10-06 | None | 144 focused tests and policy checks pass; verified 26.10.6.3 wheel and installed-wrapper smoke pass. Final artifact published in 35035b8; authorized PR #94 squash merge follows final head check. |
| [Transactional package build and verification](2026-09-13-transactional-packaging.md) | Complete | Unassigned | 2026-09-13 | — | Added no-bump transactional builds, wheel verification, installed-wheel smoke coverage and focused failure-path tests; recorded upstream follow-ups. |
| [TUFLOW statistic-then-maximum raster workflow](2026-09-09-tuflow-stat-then-maximum.md) | Complete | Unassigned | 2026-09-10 | — | Shared mean/median orchestration, configurable ASC_to_ASC-default mean selection, flattened TP provenance and compatibility shims completed. |

Completed historical milestones remain in the
[repository roadmap](../REPOSITORY_IMPROVEMENT_ROADMAP.md). Keep detailed records discoverable through the
documentation index.
