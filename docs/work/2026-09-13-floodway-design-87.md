# Floodway design and reporting workflow

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | Unassigned |
| Created | 2026-09-13 |
| Updated | 2026-09-14 |
| Next review | 2026-09-27 or immediately after PR #86 merges |
| Baseline | `feature/floodway-design-87` created from `main` at `da4b00765b6bd351a748a08ebc4fbddcffc60aad`; PR #86 is not yet merged |

## Outcome and scope

Deliver issue #87: a maintained floodway overtopping design assessment and reporting workflow using `ryan-culverts` as
the authoritative crossing-hydraulics engine and `ryan-tools` for road-formation hydraulic demand, protection/design
checks, event envelopes and reporting.

Current activity is source research, calculation specification and validation-vector preparation only. Substantial
implementation waits for the prerequisite public APIs and repository architecture to stabilise.

Dependencies:

- `ryan-culverts` issue #4 / PR #13;
- `ryan-culverts` issue #14, intended to be implemented in PR #13;
- `ryan-tools` issue #80 / PR #86.

Debris impact/loading and debris blockage are future considerations only and are outside current scope.

Research documents:

- [Floodway design research baseline](../audits/2026-09-13-floodway-design-research.md)
- [Floodway calculation specification](../audits/2026-09-14-floodway-calculation-specification.md)
- [Floodway validation vectors](../audits/2026-09-14-floodway-validation-vectors.md)

## Current state

A documentation-first branch was created before PR #86 merges so literature review and engineering-procedure definition
can proceed independently of the changing culvert-workflow code.

The research baseline records:

- current MRWA hierarchy and 2023 floodway guidance;
- the 2006 MRWA A-F failure-zone framework and published regression targets;
- FHWA HEC-23 embankment-overtopping failure modes and riprap-design research direction;
- HEC-15/HEC-22 tractive-force guidance;
- FHWA Federal Lands low-water crossing concepts;
- US Forest Service low-water crossing geometry considerations;
- TxDOT common-headwater roadway-overtopping architecture;
- Chen and Anderson embankment-overtopping research as a candidate basis for sectional hydraulics;
- the WA Willare Crossing failure/model-test case as a priority source for downstream-shoulder pressure behaviour;
- a proposed compliance/enhanced-assessment split, A-F result model, event envelope, 2D escalation conditions and
  validation plan.

The 2026-09-14 calculation specification now converts the principal research findings into an explicit pre-code
procedure, including:

- the production boundary between `ryan-culverts` crossing hydraulics and `ryan-tools` formation response;
- the MRWA detailed and simplified capacity equations for legacy reproduction;
- the MRWA Equation 4-9 pavement/batter velocity procedure;
- explicit preservation of the source discrepancy between the Section 4.4.3 `D/H < 0.76` free-flow applicability
  statement and the Appendix C/D `D/H = 0.8` operational submergence point;
- graph/digitisation requirements for MRWA Figures 4.2, 4.5 and 4.6;
- A-F zone-specific demand/applicability rules rather than a generic floodway-force value;
- HEC-23 DG5 Equations 5.1-5.3 and the mild/steep-slope routing logic as a candidate enhanced riprap check;
- event-envelope and governing-state requirements;
- method-level applicability statuses and 2D/specialist escalation;
- a deliberate decision not to create an unsupported downstream-shoulder suction coefficient from the Willare evidence.

The validation-vector document now records future test targets for:

- Seven Mile Creek Table D1, maximum pavement/batter velocities and the `Q=150 m3/s` event;
- Majors Creek Table D2, transition/intersection values and the practical Q50 pavement/batter velocities;
- MRWA Figure 4.6 graph-read `K` anchors;
- the 0.76 versus 0.8 legacy-threshold distinction;
- HEC-23 DG5 Equation 5.2 and both published mild/steep-slope examples;
- the apparent HEC-23 indexed-text SI inconsistency that labels 12 inches as 0.15 m in the steep example, which must be
  checked visually against the authoritative document before coding;
- event-envelope, irregular-crest, inactive-flow, applicability and report-provenance acceptance scenarios.

No floodway Python implementation is authorised on the pre-#86 architecture by this branch.

## Next action

Continue bounded research while PR #86 is open. Highest-value remaining tasks are:

1. independently verify/digitise the MRWA Figure 4.2, 4.5 and 4.6 relationships if they are to be automated, preserving
   source domains and prohibiting extrapolation;
2. obtain/verify the original Chen-Anderson/FHWA-RD-86-126 equations before deciding whether a sectional 1D formation
   water-surface solver belongs in the first increment;
3. obtain the best available Patterson-Abercromby Willare model-test material for quantitative shoulder pressure data;
4. decide whether a source-backed toe/impingement/scour method belongs in the first increment or remains a
   specialist/2D trigger;
5. verify HEC-23 DG5 gradation selections and the steep-example SI conversion visually from an authoritative copy;
6. review current Austroads Part 5B requirements when the full licensed text is available.

After PR #86 merges, refresh this branch from the new `main` **before** substantial code is added. Prefer a clean
rebase onto fresh `main` if practical; merging fresh `main` into the branch is acceptable if preserving published
history is preferred.

Then re-read the final culvert workflow architecture and public `ryan-culverts` #14 result contract, map the calculation
specification onto the merged shared configuration/result/reporting types, and begin implementation.

## Completion criteria

- The research baseline identifies the adopted and rejected calculation methods with authoritative sources and
  applicability limits.
- The calculation specification is sufficiently explicit that implementation does not require rediscovering the core
  MRWA/HEC procedure.
- Published/recomputed validation vectors exist before design-use code is enabled.
- The branch has been refreshed from `main` after PR #86 is merged.
- Public `ryan-culverts` roadway-segment results required by #87 are stable and available.
- Typed floodway formation/configuration/result models are implemented against the post-#86 architecture.
- Event-envelope analysis identifies the governing event/discharge for each supported limit state.
- MRWA compliance results remain distinct from enhanced engineering checks.
- Published MRWA worked examples are reproduced for the portions implemented.
- Any adopted HEC-23 or other protection method reproduces authoritative worked examples before design use.
- Unsupported shoulder uplift, seepage/piping, debris and complex 2D effects fail closed or are clearly flagged for
  specialist review.
- Console plus machine-readable/reviewable output is delivered per issue #87.
- Required repository validation is complete and recorded.

## Validation and delivery

### 2026-09-14

- Documentation/source research only; no Python modified.
- Added a detailed calculation specification and source-derived validation-vector pack.
- Direct arithmetic in the validation pack was independently recomputed while preparing the research, but this is not
  a substitute for repository tests.
- Repository documentation checker, Markdown checks, Ruff, Pyright and pytest were **not run** in the connector-only
  environment. They remain explicit follow-up validation for an agent/local checkout; no unrun check is reported as
  passed.
- Changes were committed directly to the existing remote branch through the GitHub connector.
- Draft PR #88 remains the delivery/research PR.

### 2026-09-13

- Documentation/source research only; no Python modified.
- Repository documentation checker was not run in the connector-only environment and remained a follow-up validation
  item.
- Branch created and pushed through GitHub as `feature/floodway-design-87`.
- Draft PR to `main` is to remain open during research and must be refreshed from post-#86 `main` before implementation.

## Progress

### 2026-09-14

Reconstructed the main MRWA 2006 hydraulic/velocity procedure into a pre-code calculation specification rather than
leaving it as disconnected literature notes. Recorded the Equation 2-9 logic, source-dependent transition/submergence
handling, A-F demand boundaries, HEC-23 DG5 riprap equations and routing, event-envelope requirements, curve
digitisation controls and explicit unsupported/specialist states.

Created a validation pack from the Seven Mile Creek and Majors Creek worked examples and HEC-23 DG5 examples. This
includes intermediate values so future tests can diagnose errors in the calculation path rather than only compare final
velocities. The pack also records source ambiguities/inconsistencies instead of silently correcting them.

### 2026-09-13

Created the research-first delivery branch for issue #87 and recorded an initial literature/source baseline. The
architecture intentionally keeps crossing hydraulics in `ryan-culverts` and formation-response/design assessment in
`ryan-tools`. Recorded that current research may continue before PR #86 merges, but implementation must use a refreshed
post-#86 `main` baseline.
