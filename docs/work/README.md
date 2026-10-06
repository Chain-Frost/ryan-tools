# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
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
| [Culvert uncertainty/sensitivity workflow](2026-10-06-culvert-uncertainty-91.md) | Complete | ChatGPT | 2026-10-06 | None | 144 focused tests and policy checks pass; verified 26.10.6.3 wheel and installed-wrapper smoke pass. Final artifact published in 35035b8; authorized PR #94 squash merge follows final head check. |
| [Transactional package build and verification](2026-09-13-transactional-packaging.md) | Complete | Unassigned | 2026-09-13 | — | Added no-bump transactional builds, wheel verification, installed-wheel smoke coverage and focused failure-path tests; recorded upstream follow-ups. |
| [TUFLOW statistic-then-maximum raster workflow](2026-09-09-tuflow-stat-then-maximum.md) | Complete | Unassigned | 2026-09-10 | — | Shared mean/median orchestration, configurable ASC_to_ASC-default mean selection, flattened TP provenance and compatibility shims completed. |

Completed historical milestones remain in the
[repository roadmap](../REPOSITORY_IMPROVEMENT_ROADMAP.md). Keep detailed records discoverable through the
documentation index.
