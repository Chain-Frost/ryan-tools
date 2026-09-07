# Work register

Read this at the start of repository work. Follow [work tracking](../WORK_TRACKING.md) for status meanings, review rules
and the handoff template. The linked record owns detailed progress; this table is the compact discovery summary.

## Open work

| Work front / status record | Status | Owner | Updated | Next review | Next action / dependency |
| --- | --- | --- | --- | --- | --- |
| [Repository improvement backlog](../REPOSITORY_IMPROVEMENT_ROADMAP.md) | Planned | Unassigned | 2026-09-06 | 2026-09-20 | Select a bounded lifecycle review or script family and register its implementation record. |
| [Unsorted migration](../UNSORTED_UPGRADE_ROADMAP.md) | Needs review | Unassigned | 2026-09-06 | 2026-09-20 | Reconcile unchecked/in-progress items with current parent/submodule source and validation; do not assume older parity gaps remain unchanged. |
| [Scheduled compatibility removals](../COMPATIBILITY_POLICY.md) | Deferred | Unassigned | 2026-09-06 | 2027-01-04 | Support runs through 2026-12-31; then verify callers and select removals using the inventory. |

Initial register created on 2026-09-06 from the roadmap review. These are known follow-ups, not an exhaustive audit or
an instruction to start them all. The unsorted row records a need to reconcile existing evidence, not fresh validation
of every migration. Existing staged code changes are not inferred to be separately authorized work fronts.

## Closed work

No projects closed through this register yet. Completed historical milestones remain in the
[repository roadmap](../REPOSITORY_IMPROVEMENT_ROADMAP.md); add future complete/cancelled rows here with their final
status, updated date and linked evidence. Keep detailed records discoverable through the documentation index.
