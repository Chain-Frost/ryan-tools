# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
| [Floodway design and reporting workflow](2026-09-13-floodway-design-87.md) | Active | Unassigned | 2026-09-14 | 2026-09-27 or after PR #86 merges | Calculation specification and validation vectors prepared; continue bounded source verification, then refresh from fresh `main` after PR #86 merges before substantial implementation. |
| [Repository improvement backlog](../REPOSITORY_IMPROVEMENT_ROADMAP.md) | Planned | Unassigned | 2026-09-06 | 2026-09-20 | Select a bounded lifecycle review or script family and register its implementation record. |
| [Unsorted migration](../UNSORTED_UPGRADE_ROADMAP.md) | Needs review | Unassigned | 2026-09-06 | 2026-09-20 | Reconcile unchecked/in-progress items with current parent/submodule source and validation; do not assume older parity gaps remain unchanged. |
| [Scheduled compatibility removals](../COMPATIBILITY_POLICY.md) | Deferred | Unassigned | 2026-09-06 | 2027-01-04 | Support runs through 2026-12-31; then verify callers and select removals using the inventory. |

Initial register created on 2026-09-06 from the roadmap review. These are known follow-ups, not an exhaustive audit or
an instruction to start them all. The unsorted row records a need to reconcile existing evidence, not fresh validation
of every migration. Existing staged code changes are not inferred to be separately authorized work fronts.

## Closed work

| Work front / status record | Status | Owner | Updated | Next review | Outcome |
| --- | --- | --- | --- | --- | --- |
| [Transactional package build and verification](2026-09-13-transactional-packaging.md) | Complete | Unassigned | 2026-09-13 | — | Added no-bump transactional builds, wheel verification, installed-wheel smoke coverage and focused failure-path tests; recorded upstream follow-ups. |
| [TUFLOW statistic-then-maximum raster workflow](2026-09-09-tuflow-stat-then-maximum.md) | Complete | Unassigned | 2026-09-10 | — | Shared mean/median orchestration, configurable ASC_to_ASC-default mean selection, flattened TP provenance and compatibility shims completed. |

Completed historical milestones remain in the
[repository roadmap](../REPOSITORY_IMPROVEMENT_ROADMAP.md). Keep detailed records discoverable through the
documentation index.
