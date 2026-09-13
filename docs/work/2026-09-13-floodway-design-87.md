# Floodway design and reporting workflow

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | Unassigned |
| Created | 2026-09-13 |
| Updated | 2026-09-13 |
| Next review | 2026-09-27 or immediately after PR #86 merges |
| Baseline | `feature/floodway-design-87` created from `main` at `da4b00765b6bd351a748a08ebc4fbddcffc60aad`; PR #86 is not yet merged |

## Outcome and scope

Deliver issue #87: a maintained floodway overtopping design assessment and reporting workflow using `ryan-culverts` as the authoritative crossing-hydraulics engine and `ryan-tools` for road-formation hydraulic demand, protection/design checks, event envelopes and reporting.

Current activity is source research and procedure definition only. Substantial implementation waits for the prerequisite public APIs and repository architecture to stabilise.

Dependencies:

- `ryan-culverts` issue #4 / PR #13;
- `ryan-culverts` issue #14, intended to be implemented in PR #13;
- `ryan-tools` issue #80 / PR #86.

Debris impact/loading and debris blockage are future considerations only and are outside current scope.

Research baseline:

- [Floodway design research baseline](../audits/2026-09-13-floodway-design-research.md)

## Current state

A documentation-first branch has been created before PR #86 merges so useful literature review can proceed independently of the changing culvert-workflow code.

The research baseline currently records:

- current MRWA hierarchy and 2023 floodway guidance;
- the 2006 MRWA A-F failure-zone framework and published regression targets;
- FHWA HEC-23 embankment-overtopping failure modes and riprap-design research direction;
- HEC-15/HEC-22 tractive-force guidance;
- FHWA Federal Lands low-water crossing concepts;
- US Forest Service low-water crossing geometry considerations;
- TxDOT common-headwater roadway-overtopping architecture;
- Chen and Anderson embankment-overtopping research as a candidate basis for sectional hydraulics;
- the WA Willare Crossing failure/model-test case as a priority source for downstream-shoulder pressure behaviour;
- a proposed compliance/enhanced-assessment split, A-F result model, event envelope, 2D escalation conditions and validation plan.

No floodway Python implementation is authorised on the pre-#86 architecture by this branch.

## Next action

Continue research while PR #86 is open, prioritising authoritative primary sources for:

1. HEC-23 Design Guideline 5 equations, SI conversions, applicability and worked examples;
2. Chen and Anderson sectional overtopping hydraulics and shear-stress method;
3. Willare Crossing / Patterson-Abercromby model-test evidence for shoulder negative pressure and rounded geometry;
4. the source relationships behind the 2006 MRWA velocity procedure;
5. a bounded decision on toe/impingement/scour checks versus mandatory specialist/2D review.

After PR #86 merges, refresh this branch from the new `main` **before** substantial code is added. Prefer a clean rebase onto fresh `main` if practical; merging fresh `main` into the branch is acceptable if preserving published history is preferred.

Then re-read the final culvert workflow architecture and decide package placement against the merged code rather than the pre-#86 tree.

## Completion criteria

- The research baseline identifies the adopted and rejected calculation methods with authoritative sources and applicability limits.
- The branch has been refreshed from `main` after PR #86 is merged.
- Public `ryan-culverts` roadway-segment results required by #87 are stable and available.
- Typed floodway formation/configuration/result models are implemented against the post-#86 architecture.
- Event-envelope analysis identifies the governing event/discharge for each supported limit state.
- MRWA compliance results remain distinct from enhanced engineering checks.
- Published MRWA worked examples are reproduced for the portions implemented.
- Any adopted HEC-23 or other protection method reproduces authoritative worked examples before design use.
- Unsupported shoulder uplift, seepage/piping, debris and complex 2D effects fail closed or are clearly flagged for specialist review.
- Console plus machine-readable/reviewable output is delivered per issue #87.
- Required repository validation is complete and recorded.

## Validation and delivery

2026-09-13:

- Documentation/source research only; no Python modified.
- Repository documentation checker has not been run in this connector-only environment and remains a follow-up validation item.
- Branch created and pushed through GitHub as `feature/floodway-design-87`.
- Draft PR to `main` is to remain open during research and must be refreshed from post-#86 `main` before implementation.

## Progress

### 2026-09-13

Created the research-first delivery branch for issue #87 and recorded an initial literature/source baseline. The architecture intentionally keeps crossing hydraulics in `ryan-culverts` and formation-response/design assessment in `ryan-tools`. Recorded that current research may continue before PR #86 merges, but implementation must use a refreshed post-#86 `main` baseline.
