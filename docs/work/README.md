# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
| [Floodway design and reporting workflow](2026-09-13-floodway-design-87.md) | Active | Unassigned | 2026-09-14 | After `ryan-culverts` PR #13/#14 changes materially or merges | Post-#86 refresh complete; branch is current with `main`. Continue bounded source verification and wait for the final public roadway-segment result contract before substantive floodway integration. |
| [Repository improvement backlog](../REPOSITORY_IMPROVEMENT_ROADMAP.md) | Planned | Unassigned | 2026-09-06 | 2026-09-20 | Select a bounded lifecycle review or script family and register its implementation record. |
| [Scheduled compatibility removals](../COMPATIBILITY_POLICY.md) | Deferred | Unassigned | 2026-09-06 | 2027-01-04 | Support runs through 2026-12-31; then verify callers and select removals using the inventory. |

Initial register created on 2026-09-06 from the roadmap review. These are known follow-ups, not an exhaustive audit or
an instruction to start them all. Existing staged code changes are not inferred to be separately authorized work
fronts.

## Closed work

| Work front / status record | Status | Owner | Updated | Next review | Outcome |
| --- | --- | --- | --- | --- | --- |
| [Unsorted migration](../UNSORTED_UPGRADE_ROADMAP.md) | Cancelled | Unassigned | 2026-09-13 | — | The `unsorted` submodule is excluded from all automated inspection and validation; no further migration review is authorised. |
| [Transactional package build and verification](2026-09-13-transactional-packaging.md) | Complete | Unassigned | 2026-09-13 | — | Added no-bump transactional builds, wheel verification, installed-wheel smoke coverage and focused failure-path tests; recorded upstream follow-ups. |
| [TUFLOW statistic-then-maximum raster workflow](2026-09-09-tuflow-stat-then-maximum.md) | Complete | Unassigned | 2026-09-10 | — | Shared mean/median orchestration, configurable ASC_to_ASC-default mean selection, flattened TP provenance and compatibility shims completed. |

Completed historical milestones remain in the
[repository roadmap](../REPOSITORY_IMPROVEMENT_ROADMAP.md). Keep detailed records discoverable through the
documentation index.
