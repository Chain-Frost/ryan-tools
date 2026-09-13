# Floodway design and reporting workflow

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | Unassigned |
| Created | 2026-09-13 |
| Updated | 2026-09-14 |
| Next review | Immediately after `ryan-culverts` PR #13/#14 is merged or materially updated |
| Baseline | `feature/floodway-design-87` is refreshed onto post-#86 `main` at `1d8aa177be7b074003e0e355b5db56f4cd764e99` |

## Outcome and scope

Deliver issue #87: a maintained floodway overtopping design assessment and reporting workflow using `ryan-culverts` as
the authoritative crossing-hydraulics engine and `ryan-tools` for road-formation hydraulic demand, protection/design
checks, event envelopes and reporting.

The research, calculation specification and validation-vector preparation can continue independently. The `ryan-tools`
workflow architecture prerequisite is now complete because PR #86 has merged. Substantial code that consumes advanced
roadway segment hydraulics still waits for the public `ryan-culverts` PR #13/#14 result contract to stabilise.

Dependencies:

- `ryan-culverts` issue #4 / PR #13 — still open;
- `ryan-culverts` issue #14, intended to be implemented in PR #13 — still required before downstream segment-state
  integration is finalised;
- `ryan-tools` issue #80 / PR #86 — merged into `main` on 2026-09-14 at
  `1d8aa177be7b074003e0e355b5db56f4cd764e99`.

Debris impact/loading and debris blockage are future considerations only and are outside current scope.

Research documents:

- [Floodway design research baseline](../audits/2026-09-13-floodway-design-research.md)
- [Floodway calculation specification](../audits/2026-09-14-floodway-calculation-specification.md)
- [Floodway validation vectors](../audits/2026-09-14-floodway-validation-vectors.md)

## Current state

The mandatory post-#86 refresh has occurred. Repository comparison confirms `feature/floodway-design-87` is based on the
current `main` merge commit for PR #86, is `0` commits behind `main`, and contains only the floodway research/documentation
commits above that baseline.

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

The 2026-09-14 calculation specification converts the principal research findings into an explicit pre-code procedure,
including:

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

The validation-vector document records future test targets for:

- Seven Mile Creek Table D1, maximum pavement/batter velocities and the `Q=150 m3/s` event;
- Majors Creek Table D2, transition/intersection values and the practical Q50 pavement/batter velocities;
- MRWA Figure 4.6 graph-read `K` anchors;
- the 0.76 versus 0.8 legacy-threshold distinction;
- HEC-23 DG5 Equation 5.2 and both published mild/steep-slope examples;
- the apparent HEC-23 indexed-text SI inconsistency that labels 12 inches as 0.15 m in the steep example, which must be
  checked visually against the authoritative document before coding;
- event-envelope, irregular-crest, inactive-flow, applicability and report-provenance acceptance scenarios.

## Post-#86 architecture review

The merged culvert workflow gives #87 a stable downstream workflow shape to build against:

- `ryan_library/classes/culvert/` owns project/crossing/scenario/result models. `ScenarioResult` retains the complete
  authoritative `CrossingHydraulicResult` plus AEP, source/notes, target-headwater residual evidence and event tailwater
  override provenance.
- `ryan_library/functions/culvert/` owns strict project parsing, the bounded adapter into `culvert_solver`, assessment and
  detailed machine-readable export. Floodway code should consume typed in-memory results rather than parse those JSON
  exports back into hydraulics.
- `ryan_library/orchestrators/culvert/` owns project-level solve/analyse/design/rating/report coordination. The floodway
  workflow should call/reuse this boundary where crossing hydraulics are needed rather than reproduce it.
- Imported event tables and programmatic `EventDefinition` sequences already provide AEP/name/discharge or target-HW
  event identity with provenance. The floodway event-envelope layer can reuse this event identity rather than create a
  parallel hydrology/event format.
- The present `RoadwayDefinition` is constant-crest only. Breaking project-schema changes remain acceptable during this
  development series, so irregular/profile roadway configuration should be added only after PR #13/#14 establishes the
  final upstream public objects and result semantics.

Provisional package placement for #87, subject to implementation review, is:

- `ryan_library/classes/floodway/` — formation geometry, material/protection metadata, applicability/result models and
  event-envelope result types;
- `ryan_library/functions/floodway/` — MRWA legacy calculations, enhanced sourced demand/protection calculations,
  method applicability and machine-readable export helpers;
- `ryan_library/orchestrators/floodway/` — crossing-result adaptation, event-envelope execution, governing-state
  selection and report coordination;
- `ryan-scripts/floodway.py` — maintained human-facing wrapper once the reusable workflow is stable.

The culvert package should only be changed where required to expose/configure the final public roadway profile API. A-F
formation geometry, shear/tractive stress, pavement/batter/toe protection checks and MRWA floodway reporting do not
belong in `classes/culvert` or `culvert_solver`.

## Next action

The repository-architecture refresh is complete. The remaining primary implementation dependency is `ryan-culverts`
PR #13/#14. Until that public roadway-segment result contract is stable, continue only work that does not require guessing
its API:

1. independently verify/digitise the MRWA Figure 4.2, 4.5 and 4.6 relationships if they are to be automated, preserving
   source domains and prohibiting extrapolation;
2. obtain/verify the original Chen-Anderson/FHWA-RD-86-126 equations before deciding whether a sectional 1D formation
   water-surface solver belongs in the first increment;
3. obtain the best available Patterson-Abercromby Willare model-test material for quantitative shoulder pressure data;
4. decide whether a source-backed toe/impingement/scour method belongs in the first increment or remains a
   specialist/2D trigger;
5. verify HEC-23 DG5 gradation selections and the steep-example SI conversion visually from an authoritative copy;
6. review current Austroads Part 5B requirements when the full licensed text is available;
7. after PR #13/#14 merges, update the `ryan_culverts` dependency, inspect the final roadway result API and then begin the
   typed floodway implementation against the package placement above.

## Completion criteria

- The research baseline identifies the adopted and rejected calculation methods with authoritative sources and
  applicability limits.
- The calculation specification is sufficiently explicit that implementation does not require rediscovering the core
  MRWA/HEC procedure.
- Published/recomputed validation vectors exist before design-use code is enabled.
- The branch is refreshed from post-#86 `main`. **Complete.**
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

### 2026-09-14 — post-#86 refresh

- Confirmed PR #86 is merged into `main` at `1d8aa177be7b074003e0e355b5db56f4cd764e99`.
- Confirmed `feature/floodway-design-87` has that commit as its merge base and is `0` commits behind `main`; no additional
  rebase/merge operation is required.
- Re-read the merged culvert workflow/result/export architecture and recorded the package-integration decisions above.
- No floodway Python implementation was added in this refresh.
- Repository lint/type/test/build commands were not run in the GitHub connector environment; no unrun check is reported
  as passed.

### 2026-09-14 — research increment

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

## Progress

### 2026-09-14

PR #86 merged and the mandatory refresh condition was checked. The floodway branch already has the #86 merge commit as
its merge base, so it is current with `main` and no history rewrite is needed. The merged workflow was reviewed: #87 can
reuse typed `ScenarioResult`/`CrossingHydraulicResult`, event provenance, the existing culvert orchestrator boundary and
detailed result/export conventions. Floodway-specific formation response remains a separate package family, while
roadway-profile input/output adaptation waits for the final PR #13/#14 public API.

Earlier on 2026-09-14, reconstructed the main MRWA 2006 hydraulic/velocity procedure into a pre-code calculation
specification rather than leaving it as disconnected literature notes. Recorded the Equation 2-9 logic,
source-dependent transition/submergence handling, A-F demand boundaries, HEC-23 DG5 riprap equations and routing,
event-envelope requirements, curve digitisation controls and explicit unsupported/specialist states.

Created a validation pack from the Seven Mile Creek and Majors Creek worked examples and HEC-23 DG5 examples. This
includes intermediate values so future tests can diagnose errors in the calculation path rather than only compare final
velocities. The pack also records source ambiguities/inconsistencies instead of silently correcting them.

### 2026-09-13

Created the research-first delivery branch for issue #87 and recorded an initial literature/source baseline. The
architecture intentionally keeps crossing hydraulics in `ryan-culverts` and formation-response/design assessment in
`ryan-tools`.
