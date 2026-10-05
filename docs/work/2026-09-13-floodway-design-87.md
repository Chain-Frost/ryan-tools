# Floodway design and reporting workflow

| Field | Value |
| --- | --- |
| Status | Active implementation |
| Owner | Unassigned |
| Created | 2026-09-13 |
| Updated | 2026-10-06 |
| Next review | After MRWA regression and repository validation |
| Branch | `feature/floodway-design-87` / PR #88 |
| Baseline | Post-#86 `main`; branch is 0 commits behind `main` as checked 2026-10-06 |

## Outcome and scope

Deliver issue #87: a maintained floodway overtopping design-assessment and reporting workflow using `ryan-culverts` as
the authoritative crossing-hydraulics engine and `ryan-tools` for formation response, protection/design checks,
event envelopes and reporting.

The prerequisite roadway API is no longer blocked:

- `ryan-culverts` PR #13 is merged;
- upstream issues #4 and #14 are closed;
- the vendored `ryan_culverts` revision on this branch points to the merged roadway-overtopping implementation;
- `ryan-tools` PR #86 / issue #80 is merged and provides the shared culvert workflow boundary.

Debris impact/loading and debris blockage remain future scope.

Hydrograph/overtopping-duration and road-closure-duration analysis is **not part of this floodway formation-design
workflow**. It is tracked separately in issue #92 because it requires a different time-series workflow, scripts and
reporting contract.

## Current implementation

PR #88 now contains substantive Python implementation rather than research-only material:

- typed floodway A-F formation, hydraulic-state, applicability, demand, envelope and protection result models;
- adapter from authoritative `ScenarioResult` / roadway segment results without re-deriving roadway-weir hydraulics;
- irregular roadway profile support through the merged `ryan-culverts` public API;
- MRWA Equation 4/6 surface-velocity primitives;
- MRWA Equation 7 limiting-velocity calculation;
- source-bounded Figure 4.5 plunging/surface-flow transition relation;
- Figure 4.6 `K` relation reconstructed from the guide's own Equation 3 + Equation 6 energy relation rather than
  hand-digitised;
- event-envelope selection retaining independent governors for velocity, dynamic pressure and momentum flux;
- HEC-23 DG5 overtopping-riprap equations and worked-example regression tests;
- JSON, CSV and Markdown envelope outputs;
- focused integration tests across the culvert/floodway boundary.

Unsupported zone mechanisms continue to fail closed rather than receiving invented scalar methods.

## MRWA source verification — 2026-10-06

The authoritative 2006 MRWA Floodway Design Guide was visually re-checked for Figures 4.5 and 4.6 and Appendix D.

### Figure 4.6

Figure 4.6 does not require hand digitisation.

Using the guide's simplified free-flow relation

```text
q = 1.69 H^(3/2)
```

and Equation 6 no-loss energy relation with

```text
V = K sqrt(H)
delta = delta_p / H
```

gives the dimensionless equation

```text
1 + delta = K^2/(2g) + 1.69/K
```

The larger positive root is the Figure 4.6 high-velocity branch. The reconstructed values reproduce the published
Appendix D graph reads within graph-reading precision:

| delta_p/H | Published graph read | Reconstructed |
| ---: | ---: | ---: |
| 0.104 | 3.50 | ~3.48 |
| 0.150 | 3.70 | ~3.68 |
| 0.307 | 4.20 | ~4.22 |
| 0.318 | 4.25 | ~4.25 |

The implementation enforces the displayed Figure 4.6 domain `0 <= delta_p/H <= 1.8` and refuses extrapolation.

### Figure 4.5

Figure 4.5 remains a graphical source and has been visually digitised over its displayed `H/l` domain. The
implementation uses bounded linear interpolation and refuses extrapolation.

Independent Appendix D checks support the digitisation:

- Seven Mile Creek transition: `H/l = 0.90/9.0 = 0.10`, with `(D/H)trans ~= 0.60`;
- Majors Creek transition: `H/l ~= 1.48/9.0 = 0.164`, with `(D/H)trans ~= 0.67-0.68`.

### Figure 4.2

Figure 4.2 is not required for the production crossing flow split because `ryan-culverts` remains authoritative for
roadway/culvert hydraulics. It should only be automated if an explicit MRWA legacy-capacity reproduction path is later
required. The floodway design workflow must not create a competing production crossing solver.

## Architectural boundary

### `ryan-culverts` owns

- common headwater/tailwater solution;
- culvert/roadway flow split;
- irregular roadway crest integration;
- roadway submergence correction;
- local roadway unit discharge and segment-state provenance.

### `ryan-tools` owns

- floodway formation geometry and material/protection metadata;
- MRWA formation-response calculations;
- A-F zone demand and applicability assessment;
- enhanced sourced protection checks such as HEC-23 DG5;
- event-envelope and governing-state selection;
- specialist/2D escalation;
- floodway reporting and exports.

## Remaining first-increment work

1. **MRWA regression and regime completion**
   - complete focused regression against Seven Mile Creek and Majors Creek for the portions that are implemented;
   - map the guide's submerged pavement `q/D` approximation into the typed result model if retained;
   - preserve the Section 4.4.3 `D/H < 0.76` versus Appendix C/D `D/H = 0.8` source distinction rather than silently
     reconciling it.

2. **Protection/design integration**
   - integrate the existing HEC-23 DG5 result into the typed assessment/report path;
   - keep enhanced HEC-23 checks distinct from MRWA compliance.

3. **Formation/configuration**
   - expose only the additional geometry/material inputs needed by supported methods;
   - do not add unsupported shoulder-pressure, complete piping or generic toe-scour models merely to fill A-F fields.

4. **Reporting and human-facing entry point**
   - expand the report with governing event/state, method provenance, applicability/warnings and supported protection
     results;
   - add the maintained floodway wrapper/CLI once the reusable API is stable.

5. **Validation**
   - run Ruff formatting/lint;
   - run strict Pyright on changed Python;
   - run focused and full pytest;
   - run documentation/Markdown checks and the repository documentation checker;
   - run package/build verification where required by repository policy.

## Explicit fail-closed boundaries

The first increment does not invent numerical methods for:

- downstream-shoulder suction/uplift without a validated pressure relationship;
- complete seepage/piping/internal-erosion analysis;
- complex downstream toe scour/impingement where a bounded analytical method has not been adopted;
- debris impact/loading or debris blockage;
- spatial hydraulic effects that require 2D verification.

These states remain `SOURCE_DATA_REQUIRED`, `SPECIALIST_REVIEW_REQUIRED`,
`OUTSIDE_SOURCE_RANGE` or `TWO_D_VERIFICATION_RECOMMENDED` as appropriate.

## Validation and delivery record

### 2026-10-06

- Confirmed PR #88 is open, mergeable and 0 commits behind `main`.
- Confirmed `ryan-culverts` PR #13 is merged and issue #14 is closed.
- Posted a current-status comment to PR #88.
- Raised issue #92 for the separate hydrograph/overtopping-duration workflow.
- Visually verified MRWA Figures 4.5 and 4.6 against the authoritative guide.
- Added bounded Figure 4.5 interpolation with Appendix D anchors.
- Reconstructed Figure 4.6 analytically from MRWA Equations 3 and 6 and added Appendix D regression anchors.
- Routed free-flow pavement/downstream-batter assessment through the source-backed Equation 4/7 velocity limit where
  sufficient geometry is available.
- Added `crest_flow_length` to formation geometry for Figure 4.5 `H/l` classification.
- Added focused tests for Figure 4.5/4.6 relations and plunging-versus-surface routing.
- Full repository Ruff/Pyright/pytest/build validation has **not yet been run**; no unrun check is reported as passed.

### 2026-09-14

- Refreshed against post-#86 `main`.
- Added the calculation specification and validation-vector pack.
- Reviewed the shared culvert workflow boundary and package placement.

### 2026-09-13

- Opened the research-first branch/PR and established the source baseline and architectural boundary.
