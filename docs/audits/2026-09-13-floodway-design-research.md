# Floodway design research baseline

Date: 2026-09-13

Status: Research baseline for issue #87. This document records source findings, proposed analysis boundaries and unresolved research questions. It is not yet an adopted calculation specification.

## Purpose

Issue #87 proposes a maintained floodway overtopping design and reporting workflow in `ryan-tools`, with `ryan-culverts` remaining authoritative for culvert/roadway flow split, irregular crest overtopping, submergence and common-headwater hydraulics.

The immediate objective is to complete source review and define defensible analysis procedures before substantial implementation begins. The implementation branch must be refreshed from the latest `main` after PR #86 is merged so the floodway workflow is built on the final culvert-workflow architecture rather than the pre-#86 repository layout.

Debris impact/loading and debris blockage are future considerations only and are explicitly outside the current scope.

## Source hierarchy and current WA position

### Main Roads Western Australia

The 2006 Main Roads WA *Floodway Design Guide* remains the controlling floodway reference for Main Roads work. The current MRWA Supplement to Austroads Guide to Road Design Part 5B states that the 2006 Floodway Design Guide takes precedence over the Austroads floodway section:

- https://www.mainroads.wa.gov.au/technical-commercial/technical-library/road-traffic-engineering/guide-to-road-design/mrwa-supplement-to-austroads-guide-to-road-design-part-5b/

Main Roads also publishes a current floodway guidance page, version 3 dated 12 June 2023:

- https://www.mainroads.wa.gov.au/technical-commercial/technical-library/road-traffic-engineering/drainage-waterways/floodways/

Important current requirements/additions on that page include:

- cement-stabilised pavement protection should extend to the 1% AEP water level or vertical-crest VPI, whichever is lower;
- embankment scour protection should extend beyond the nominal floodway length to the relevant water-level extent, with 2% AEP given as the desirable water level for the extent of embankment scour protection;
- the floodway is to remain trafficable up to the nominated design AEP;
- flow up to 2% AEP should remain contained within the floodway approach rather than spilling elsewhere along the road;
- a relief culvert should be placed at the natural low point where needed to drain perennial/frequent flows and avoid upstream ponding and pavement softening.

The software should therefore provide a recognisable **MRWA compliance layer** rather than silently replacing the Main Roads procedure with a different international method.

### 2006 MRWA Floodway Design Guide findings

The guide provides a useful road-formation failure-zone framework and remains valuable for regression testing.

For the typical floodway cross section (Figure 5.1), the guide identifies:

- **A — downstream toe:** impinging supercritical velocity;
- **B — downstream batter:** drag/shear resistance;
- **C — downstream shoulder:** uplift associated with embankment geometry;
- **D — road surface:** shear/drag resistance;
- **E — upstream batter:** approach velocity;
- **F — below formation:** piping or riverbed instability/sediment-transport effects.

The guide also makes several important observations that should directly influence the software design:

1. The maximum floodway velocity may occur at a discharge lower than the maximum design discharge. A floodway assessment therefore needs a discharge/event sweep and governing-envelope calculation, not just one design-peak solve.
2. Free flow can be subdivided into plunging and surface-flow regimes, while further tailwater rise causes submerged flow.
3. Plunging flow is particularly important for the downstream batter because the high-velocity jet can persist to the tailwater surface.
4. The pavement and downstream batter do not necessarily have the same governing velocity or governing discharge.
5. The downstream shoulder is sensitive to abrupt grade changes and negative pressures; the guide recommends rounding the shoulder, approximately 3.3 m radius, to reduce that risk.
6. The guide's published worked examples provide useful regression targets. Example 1 reports a maximum batter velocity of 3.51 m/s and maximum pavement velocity of 3.99 m/s. Example 2 reports a Q50 batter velocity of 3.01 m/s and pavement velocity of 2.97 m/s.

The legacy method should be reproducible where practical, but its velocity-based protection tables should not be treated as a complete physical model of erosive demand.

## International and research sources reviewed

### FHWA HEC-23 Design Guideline 5 — embankment overtopping

FHWA HEC-23 Volume II, Design Guideline 5, *Riprap Design for Embankment Overtopping* is directly relevant:

- https://www.fhwa.dot.gov/engineering/hydraulics/pubs/09111/09112.pdf

HEC-23 describes two distinct erosion patterns:

- **submerged overtopping:** erosion commonly initiates at the downstream shoulder, where a surging hydraulic jump/nick point can form and migrate;
- **free overtopping with low tailwater:** flow accelerates down the downstream face, erosion commonly initiates near the toe, and erosion can migrate upslope/upstream.

This independently supports the MRWA A/B/C zoning concept and confirms that a single roadway velocity is insufficient for design.

HEC-23 also provides a dedicated riprap sizing procedure for overtopping embankments based on near-prototype physical testing. The method is not a universal substitute for the MRWA tables: its applicability depends on embankment slope, riprap gradation/uniformity and the flow condition. The implementation should therefore preserve the selected method and its applicability limits in the result provenance.

A key research item before coding is to transcribe and independently verify all equations, variable definitions, SI conversions, slope limits, gradation limits and example calculations from Design Guideline 5. Do not implement from isolated secondary quotations.

### NCHRP Report 568 — riprap design evidence

NCHRP Report 568, *Riprap Design Criteria, Recommended Specifications, and Quality Control*, contains the research basis and discussion for overtopping-flow riprap methods, including near-prototype embankment tests:

- https://www.engr.colostate.edu/CIVE510/Manuals/nchrp_rpt_568.pdf

This is useful as supporting evidence for HEC-23 and for identifying where the HEC-23 procedure is empirical. It should be reviewed alongside HEC-23 rather than used to create an independent competing method without a clear need.

### FHWA HEC-15 / current HEC-22 discussion — tractive-force design

FHWA HEC-15 uses maximum permissible tractive force (shear stress) as the design basis for flexible channel linings:

- https://www.fhwa.dot.gov/engineering/hydraulics/library_arc.cfm?CFID=1764819903&CFTOKEN=faadf409cd3ab45-A323BF3E-D606-174A-0D4CA6D37139053D&id=32&pub_number=15

The 2024 HEC-22 fourth edition summarises the tractive-force approach as physically based and preferable to a simple permissible-velocity method for flexible lining stability:

- https://www.fhwa.dot.gov/engineering/hydraulics/pubs/hif24006.pdf

This supports adding shear/tractive-stress demand to the enhanced engineering assessment. It does **not** prove that ordinary uniform-channel shear equations can be applied indiscriminately at the downstream shoulder, hydraulic jump or impingement zone. The software must distinguish between:

- locations where a quasi-uniform boundary-shear approximation is defensible;
- locations dominated by nonuniform acceleration, separation, hydraulic-jump turbulence or impingement, where another method or a specialist/2D check is required.

### FHWA Federal Lands low-water crossing guidance

FHWA Federal Lands PDDM Chapter 7 contains low-water crossing criteria and reporting requirements:

- https://highways.dot.gov/federal-lands/pddm/Chapter_07.pdf

Relevant concepts include:

- assess crossing stability separately from capacity/serviceability;
- demonstrate embankment stability within the protected extent under the adopted stability-design flood;
- consider drop scour, outlet scour and long-term degradation as applicable;
- protect the roadway/embankment over the wetted width of the stability-design flood;
- report water levels, velocities, scour components and countermeasure calculations;
- use water-surface-profile modelling where local hydraulic impacts cannot be represented adequately by a simple crossing calculation.

This reinforces the proposed separation between hydraulic capacity, formation protection and failure-mode assessment.

### US Forest Service low-water crossing guidance

The US Forest Service publication *Low-Water Crossings: Geomorphic, Biological, and Engineering Design Considerations* provides useful geometry and siting guidance:

- https://www.fs.usda.gov/t-d/pubs/pdf/LowWaterCrossings/LoWholeDoc.pdf

Relevant findings include:

- keep the crossing on a straight, stable reach where practical;
- align the structure approximately perpendicular to the channel and avoid geometry that focuses energy into banks;
- use a sag/dip to concentrate overtopping flow toward the desired part of the crossing;
- keep the roadway profile compatible with the natural channel where feasible.

These concepts are useful for design warnings and reporting but are not intended to become hard-coded universal pass/fail rules without an adopted project/authority criterion.

### TxDOT roadway overtopping

The TxDOT Hydraulic Design Manual explicitly requires a flow-distribution analysis when water passes both through a culvert and over the roadway, using a common headwater and iterative solution:

- https://www.txdot.gov/content/txdotoms/us/en/manuals/des/hyd/chapter-8--culverts/section-3--hydraulic-operation-of-culverts/roadway-overtopping.html

This is consistent with the architectural decision already made in `ryan-culverts`: culvert and roadway flow splitting belongs in the hydraulic engine, not in the downstream floodway design layer.

### Chen and Anderson / embankment overtopping research

Chen and Anderson's FHWA embankment-overtopping work is an important research basis for failure mechanisms and sectional hydraulic calculations. A TRR paper describing the method includes water-surface profiles, critical depth/slope, hydraulic-jump condition, velocity, shear stress and erosion calculations along the embankment:

- https://onlinepubs.trb.org/Onlinepubs/trr/1987/1151/1151-001.pdf

This source should be reviewed in full before deciding whether `ryan-tools` should implement a sectional water-surface solver across the road formation or use a smaller set of supported closed-form relationships.

### Willare Crossing, Western Australia

The Willare Crossing floodway case history is particularly relevant because it is a Western Australian full-scale failure/reinstatement case:

- TRID record: https://trid.trb.org/View/1197539

The record describes a rare flood that damaged the crossing, subsequent hydraulic model testing, negative pressure at the downstream shoulder, and design changes including flattening the downstream batter from 2H:1V to 3H:1V and rounding the shoulder to about a 3 m radius. This appears to be part of the evidence base reflected in the MRWA Floodway Design Guide.

If the underlying ARRB paper or model-test report can be obtained legally, it should be reviewed as a priority source because it may provide better quantitative support for shoulder pressure, transition geometry and protection details than the 2006 summary guide.

## Proposed analysis architecture

The proposed calculation path is intentionally split between `ryan-culverts` and `ryan-tools`.

### 1. Crossing hydraulics — `ryan-culverts`

For each total discharge/headwater state, `ryan-culverts` should remain authoritative for:

- culvert flow;
- roadway overtopping flow;
- common headwater/tailwater coupling;
- irregular roadway crest segmentation;
- roadway submergence correction;
- local roadway segment/unit-discharge state and hydraulic provenance.

Issue #14 / PR #13 should expose enough public segment state that `ryan-tools` does not need to reverse engineer Gaussian integration weights or recompute the roadway weir equation.

### 2. Floodway formation model — `ryan-tools`

A separate formation/cross-section model is required in the direction of flow. It should be distinct from the longitudinal station/elevation crest profile in `ryan-culverts`.

Candidate inputs include:

- pavement/crest width;
- pavement crossfall;
- upstream batter slope;
- downstream batter slope;
- shoulder radius/grade-break geometry;
- pavement roughness/material;
- batter roughness/protection material;
- downstream toe/apron geometry;
- relevant cut-off/filter/protection metadata;
- optional project/authority criteria.

### 3. Event/discharge envelope

The analysis should not evaluate only nominated peak events. It should sweep a configurable hydraulic range including, where resolvable:

1. incipient overtopping;
2. shallow free overtopping;
3. plunging-flow range;
4. transition toward surface flow;
5. incipient submergence;
6. supported submerged flow;
7. nominated design events;
8. larger/extreme events used to identify failure modes.

For every design check, record the hydraulic state/event that governs.

### 4. Zone-specific hydraulic demand

Do not expose one generic `floodway_force`.

The first implementation should aim to report the following only where supported by the selected method:

| Zone | Primary demand quantities | Initial design use |
| --- | --- | --- |
| E upstream batter | approach velocity, depth, shear where defensible | upstream erosion/protection |
| D pavement | depth, velocity, unit discharge, Froude, shear/drag | pavement/lining and trafficability context |
| C downstream shoulder | edge depth/velocity, regime, local energy; pressure/uplift only if sourced | flag shoulder vulnerability and protection detail |
| B downstream batter | unit discharge, velocity, Froude, shear, flow regime | rock/mattress/lining protection |
| A downstream toe/apron | velocity, momentum flux, energy, jump/impingement state | toe/apron/scour protection |
| F below formation | hydraulic heads/duration where available | flag seepage/piping/internal erosion review |

Candidate enhanced reporting quantities include:

- unit discharge `q` (m²/s);
- depth `y` (m);
- velocity `V` (m/s);
- Froude number `Fr = V / sqrt(g y)` where meaningful;
- velocity head `V²/(2g)`;
- specific energy `y + V²/(2g)`;
- dynamic pressure `0.5 rho V²` as a demand metric, not a universal design force;
- momentum flux per unit width `rho q V` where meaningful;
- boundary shear/tractive stress using an explicitly named supported method.

Each calculated field should carry method/source identity or be traceable to a result object that does.

## Candidate calculation methods and readiness

| Topic | Candidate method/source | Proposed status |
| --- | --- | --- |
| Roadway/culvert flow split | `ryan-culverts`, FHWA roadway overtopping | Required dependency; do not duplicate |
| Irregular crest/submergence | `ryan-culverts` issue #4/#14 | Required dependency; do not duplicate |
| MRWA free/plunging/surface/submerged regime checks | 2006 MRWA guide, underlying Major/Kindsvater work | Research/verification required before coding |
| MRWA pavement/batter velocity procedure | 2006 MRWA equations/worked examples | Strong candidate for legacy compliance module and regression tests |
| Flexible lining shear check | FHWA HEC-15 / supported tractive-force method | Candidate enhanced check; applicability must be explicit |
| Overtopping riprap sizing | FHWA HEC-23 DG5 + NCHRP 568 evidence | Strong candidate enhanced check after equation/example verification |
| Shoulder negative pressure/uplift | MRWA/Willare/Chen-Anderson evidence | **Not** first-increment calculation unless a validated bounded relationship is established |
| Toe impingement/scour | MRWA + HEC-23/HEC-14 or other sourced method | Research required; avoid inventing from dynamic pressure alone |
| Seepage/piping | geotechnical/hydraulic review trigger | Flag only in first increment; no complete solver |
| Debris impact/blockage | future research | Explicitly out of current scope |

## Important distinction: compliance versus enhanced assessment

The report should distinguish at least two result families.

### MRWA compliance / recognised procedure

Where applicable, reproduce the relevant Main Roads calculations and protection classifications with source references and current MRWA geometric/protection extents.

The objective is traceability: a reviewer should be able to recognise how the software result corresponds to Main Roads practice.

### Enhanced engineering assessment

Supplement the compliance result with sourced physical demand measures and alternative protection checks where they improve engineering understanding.

Enhanced checks must not silently override or relabel the MRWA result. If an international method is outside its published applicability range, the result should be unsupported/not-applicable rather than extrapolated.

## Proposed result/reporting model

The eventual report should provide at least:

1. source hierarchy and selected methods;
2. crossing/culvert/roadway crest definition;
3. floodway formation geometry and protection materials;
4. hydraulic event/discharge sweep;
5. culvert/roadway flow split and overtopping onset;
6. regime transitions/submergence state;
7. zone A-F hydraulic demand envelope;
8. governing event for every reported limit state;
9. MRWA compliance results;
10. enhanced shear/riprap/energy/momentum checks where supported;
11. warnings and applicability limits;
12. extreme-event/failure-mode notes;
13. explicit recommendation for 2D/specialist verification where the analytical model is exceeded;
14. full calculation/source provenance.

Machine-readable JSON/CSV results should use the same underlying typed result objects as the console/Markdown report so reporting cannot change the engineering outcome.

## 1D analytical applicability and 2D escalation

The analytical workflow should include a clear escalation flag rather than trying to solve every site.

Potential triggers for `2D_VERIFICATION_RECOMMENDED` or similar include:

- strongly skewed approach flow;
- significant cross-road variation in approach velocity not represented by the crest segmentation;
- multiple competing flow paths or outflanking;
- nearby structures controlling tailwater or generating strong recirculation;
- hydraulic jumps moving over complex/nonuniform formation geometry;
- local shoulder/toe pressure fields important to structural design;
- major channel migration/sediment-transport effects;
- a design decision depending on a result outside the adopted empirical method's applicability range.

The first increment should only flag these conditions; automatic TUFLOW/HEC-RAS model execution/import remains outside scope.

## Validation plan

### Published regression targets

At minimum, reproduce the supported portions of the MRWA worked examples before claiming parity with the legacy procedure:

- Seven Mile Creek example — batter velocity 3.51 m/s and pavement velocity 3.99 m/s at the stated governing conditions;
- Seven Mile Creek selected Q = 150 m³/s check — pavement velocity 2.79 m/s and batter velocity 2.82 m/s;
- Majors Creek Q50 example — batter velocity 3.01 m/s and pavement velocity 2.97 m/s.

The test should verify intermediate quantities where the source provides them, not only final velocity.

### Independent method examples

For any adopted HEC-23 overtopping-riprap method, reproduce the worked examples from the authoritative publication in SI units before enabling the method for design use.

### Numerical and integration tests

Add tests for:

- event-envelope selection where the governing velocity/shear occurs below the maximum discharge;
- constant-crest and irregular-crest roadway states from the public `ryan-culverts` API;
- supported free and submerged states;
- deterministic source/provenance retention;
- explicit unsupported/not-applicable outcomes at method limits;
- report/export parity with the typed result model.

## Research backlog before implementation

Priority items that can be completed before #86 is merged:

1. Obtain and review the full Chen and Anderson embankment-overtopping methodology, including the equations used for profile velocity, shear and hydraulic-jump positioning.
2. Transcribe HEC-23 DG5 completely from the authoritative publication, including SI equations, slope/gradation limits, filter requirements and worked examples.
3. Locate the original or best available Willare Crossing / Patterson-Abercromby model-test material to understand the basis for downstream-shoulder negative pressure and the recommended rounded geometry.
4. Review the references used by the 2006 MRWA guide (Major 1990, Patterson & Abercromby 1986, Kindsvater 1964, Cameron & McNamara 1966) and record which relationships remain suitable for software implementation.
5. Determine whether current Austroads guidance adds any mandatory/recommended checks that should appear in the non-MRWA mode. Do not infer content from citations if the full text is unavailable.
6. Define the boundary between a supported analytical toe-scour/impingement check and a mandatory specialist/2D review.
7. Establish a source-backed shear-stress formulation for each location where shear will be reported; do not reuse a uniform-flow formula at clearly nonuniform locations without evidence.
8. Decide whether the first implementation should solve a 1D water-surface profile across the formation or use the bounded MRWA/HEC relationships only.
9. Define method-specific uncertainty/applicability metadata and result statuses before writing protection-selection logic.
10. Review how PR #86 finalises configuration, scenario, reporting and adapter models after it is merged; then map floodway models into those established patterns.

## Branch and PR integration rule

This research PR is intentionally allowed to start before PR #86 merges because documentation/source review does not depend on the new culvert workflow code.

**No substantial floodway implementation should be added against the pre-#86 architecture.**

After PR #86 merges:

1. fetch the new `main`;
2. rebase `feature/floodway-design-87` onto that fresh `main` (or merge fresh `main` into the branch if preserving branch history is preferred);
3. resolve documentation/index conflicts while preserving #86's architecture and work records;
4. re-read `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, the final culvert workflow documentation and public adapter/result APIs;
5. only then finalise package placement and begin the bulk implementation for #87.

The PR should remain draft until the research baseline is sufficiently complete and the post-#86 refresh has occurred.

## References reviewed in this baseline

- Main Roads WA, Floodways (current guidance page, version 3, 12 June 2023): https://www.mainroads.wa.gov.au/technical-commercial/technical-library/road-traffic-engineering/drainage-waterways/floodways/
- Main Roads WA Supplement to Austroads Guide to Road Design Part 5B (states 2006 Floodway Design Guide precedence): https://www.mainroads.wa.gov.au/technical-commercial/technical-library/road-traffic-engineering/guide-to-road-design/mrwa-supplement-to-austroads-guide-to-road-design-part-5b/
- FHWA HEC-23 Volume II, Design Guideline 5: https://www.fhwa.dot.gov/engineering/hydraulics/pubs/09111/09112.pdf
- FHWA HEC-15 publication page: https://www.fhwa.dot.gov/engineering/hydraulics/library_arc.cfm?CFID=1764819903&CFTOKEN=faadf409cd3ab45-A323BF3E-D606-174A-0D4CA6D37139053D&id=32&pub_number=15
- FHWA HEC-22, Fourth Edition: https://www.fhwa.dot.gov/engineering/hydraulics/pubs/hif24006.pdf
- FHWA Federal Lands PDDM Chapter 7: https://highways.dot.gov/federal-lands/pddm/Chapter_07.pdf
- US Forest Service, *Low-Water Crossings: Geomorphic, Biological, and Engineering Design Considerations*: https://www.fs.usda.gov/t-d/pubs/pdf/LowWaterCrossings/LoWholeDoc.pdf
- TxDOT Hydraulic Design Manual, Roadway Overtopping: https://www.txdot.gov/content/txdotoms/us/en/manuals/des/hyd/chapter-8--culverts/section-3--hydraulic-operation-of-culverts/roadway-overtopping.html
- Chen and Anderson, TRR 1151 embankment overtopping paper: https://onlinepubs.trb.org/Onlinepubs/trr/1987/1151/1151-001.pdf
- Willare Crossing TRID record: https://trid.trb.org/View/1197539
- NCHRP Report 568: https://www.engr.colostate.edu/CIVE510/Manuals/nchrp_rpt_568.pdf
