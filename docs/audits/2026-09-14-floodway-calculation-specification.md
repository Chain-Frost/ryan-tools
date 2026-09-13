# Floodway calculation specification

Date: 2026-09-14

Status: **Research calculation specification for issue #87. Not yet an adopted production calculation contract.**

This document converts the source review in
[`2026-09-13-floodway-design-research.md`](2026-09-13-floodway-design-research.md) into a more explicit calculation
procedure. It is intended to remove engineering ambiguity before implementation begins.

Substantial Python implementation remains blocked on the post-PR #86 repository architecture and the public roadway
hydraulic state from `ryan-culverts` issue #14 / PR #13. The branch must be refreshed from fresh `main` after PR #86
merges before the package/API layout in this document is translated into code.

Debris impact/loading and debris blockage are future considerations only and are outside the current scope.

## 1. Source hierarchy and interpretation rules

The initial implementation should keep source identity visible rather than blending unlike design methods.

1. **Current Main Roads WA requirements** define the WA compliance context. The Main Roads supplement to Austroads
   Guide to Road Design Part 5B states that the 2006 *Floodway Design Guide* takes precedence for floodways. The
   current Main Roads floodway page also contains later protection-extent and serviceability requirements.
2. **Main Roads WA Floodway Design Guide (2006)** supplies the legacy floodway hydraulic and velocity procedure,
   A-F failure-zone framework, Manning roughness guidance and published worked examples.
3. **FHWA HEC-23 Volume II, Design Guideline 5** is a candidate enhanced method for riprap subjected to embankment
   overtopping. It is not a silent replacement for the Main Roads procedure.
4. **Chen and Anderson (1987) / FHWA-RD-86-126** supplies research evidence for overtopping flow patterns, sectional
   hydraulics, shear and erosion. Exact equations from this source are not adopted here until the original equation
   presentation can be independently verified.
5. **Willare Crossing model-test evidence** supports the importance of the downstream shoulder and rounded transition
   geometry. It is engineering evidence, not by itself a general pressure-coefficient model.

Primary/reference sources used here:

- Main Roads WA Floodway Design Guide (2006):
  <https://www.mainroads.wa.gov.au/4ac81f/globalassets/technical-commercial/technical-library/structures-engineering/structure-design/technical-design-guidelines/floodway-design-guide.pdf>
- Main Roads WA current Floodways guidance:
  <https://www.mainroads.wa.gov.au/technical-commercial/technical-library/road-traffic-engineering/drainage-waterways/floodways/>
- Main Roads WA Supplement to Austroads Guide to Road Design Part 5B:
  <https://www.mainroads.wa.gov.au/technical-commercial/technical-library/road-traffic-engineering/guide-to-road-design/mrwa-supplement-to-austroads-guide-to-road-design-part-5b/>
- FHWA HEC-23 Volume II, Design Guideline 5, *Riprap Design for Embankment Overtopping*:
  <https://www.fhwa.dot.gov/engineering/hydraulics/pubs/09111/09112.pdf>
- Chen and Anderson (1987), *Methodology for Estimating Embankment Damage Caused by Flood Overtopping*:
  <https://onlinepubs.trb.org/Onlinepubs/trr/1987/1151/1151-001.pdf>
- Patterson and Abercromby (1986), *Floodway model tests*, TRID record:
  <https://trid.trb.org/View/1196378>
- Van Kleef and Goh (1988), Willare Crossing case history, TRID record:
  <https://trid.trb.org/View/1197539>

### 1.1 Method separation

The reporting model should distinguish at least:

- `MRWA_COMPLIANCE`: a recognisable reproduction of the adopted Main Roads procedure and requirements;
- `ENHANCED_ASSESSMENT`: additional sourced physical-demand/protection checks;
- `DIAGNOSTIC`: useful hydraulic quantities that are not themselves pass/fail criteria.

An enhanced result must not overwrite or be relabelled as a Main Roads compliance result.

### 1.2 Applicability status

Every method-level result should have an explicit applicability status. Candidate statuses are:

- `SUPPORTED`: calculation is within the adopted source method and required inputs are available;
- `LEGACY_REPRODUCTION`: result intentionally reproduces a historical/legacy method for compliance or regression;
- `NOT_APPLICABLE`: method does not apply to this hydraulic state or design zone;
- `SOURCE_DATA_REQUIRED`: a source relationship/ordinate or required input has not yet been supplied or verified;
- `OUTSIDE_SOURCE_RANGE`: inputs exceed the documented method range and no extrapolation is permitted;
- `SPECIALIST_REVIEW_REQUIRED`: the limit state cannot be reduced to a supported scalar check;
- `TWO_D_VERIFICATION_RECOMMENDED`: the analytical model may be insufficient because of spatial hydraulic effects.

The exact enum names can be aligned with the post-#86 result/status conventions after the branch refresh.

## 2. Architectural boundary

### 2.1 `ryan-culverts` remains authoritative for crossing hydraulics

Production `ryan-tools` floodway calculations should consume the public `ryan-culverts` result rather than recreate
crossing hydraulics. This includes:

- total crossing discharge and common headwater/tailwater solution;
- culvert discharge;
- roadway overtopping discharge;
- irregular roadway crest segmentation;
- free/submerged roadway state and the adopted submergence correction;
- source-traceable local roadway segment/unit-discharge state exposed through issue #14.

The 2006 Main Roads broad-crested-weir equations described below are retained for:

- understanding and reproducing the legacy procedure;
- regression against the published worked examples;
- an explicitly labelled legacy/compliance calculation where required.

They should **not** become a second production crossing solver competing with `ryan-culverts`.

### 2.2 `ryan-tools` owns formation response and design assessment

The floodway layer should own:

- the cross-section in the direction of flow: upstream batter, pavement/crest, downstream shoulder, downstream batter
  and toe/apron;
- material/protection properties relevant to those zones;
- event/discharge envelope generation and governing-state selection;
- MRWA legacy/compliance calculations downstream of the crossing hydraulic state;
- enhanced demand/protection checks where independently supported;
- applicability/warning logic and 2D/specialist escalation;
- reporting and export.

Longitudinal crest geometry across the floodway length remains distinct from the formation cross-section in the flow
direction.

## 3. Nomenclature required by the MRWA legacy method

The following symbols preserve the 2006 guide's intent. Production models may use clearer field names but should retain
source mapping in documentation/provenance.

| Symbol | Meaning | SI unit |
| --- | --- | --- |
| `Q` | total discharge considered by the legacy floodway calculation | m3/s |
| `Q_u` | discharge through associated bridge/culvert in the Appendix C workflow | m3/s |
| `L` | floodway overtopping length across the floodplain/road alignment | m |
| `l` | floodway width in the direction of flow | m |
| `q` | unit roadway/floodway discharge | m2/s |
| `p` | floodway crown/invert elevation used as datum | m |
| `USWL` | upstream water level | m |
| `TWL` | tailwater level | m |
| `h` | static headwater depth above floodway crest | m |
| `H` | total upstream head above crest, including approach velocity head where using the detailed method | m |
| `D` | tailwater depth above crest used in `D/H` | m |
| `S` | pavement/batter slope as m/m | - |
| `n` | Manning roughness coefficient | s/m^(1/3) |
| `V_s` | steady-state velocity on the pavement/batter from Equation 4 | m/s |
| `E_s` | specific-energy quantity from Equation 6 | m |
| `V_m` | maximum attainable velocity from Equation 7 | m/s |
| `V_bu` | maximum batter velocity in the `TWL <= p` regime | m/s |
| `V_bo` | maximum batter velocity in the `TWL > p` regime at plunging-to-surface transition | m/s |
| `V_b` | governing batter velocity, `max(V_bu, V_bo)` | m/s |
| `V_p` | pavement velocity | m/s |
| `Delta p` | vertical fall used by Equation 7 | m |
| `K` | Figure 4.6 coefficient as a function of `Delta p/H` | m^0.5/s in the SI form implied by Equation 7 |

For an irregular longitudinal crest, production `q` should come from the public local roadway state from
`ryan-culverts`, not blindly from `Q/L`. `Q/L` is retained where reproducing the 2006 uniform-floodway examples.

## 4. MRWA 2006 capacity procedure - legacy reproduction

### 4.1 Detailed method

For a raised floodway, Section 4.4.2 defines the detailed procedure as follows.

1. Establish the natural-section stage-discharge relationship and obtain the tailwater and approach velocity for the
   discharge under consideration.
2. Select floodway crest level and overtopping length `L`; assume a static head `h` above the crest.
3. Calculate total head:

   ```text
   H = h + V_approach^2 / (2 g)
   ```

4. Calculate `H/l`, where `l` is the floodway width in the direction of flow.
5. Read the free-flow coefficient `C_f` from Figure 4.2. The guide directs the user to curve B, except that curve A
   should be used where `H/l < 0.15`.
6. If submerged, calculate `D/H` and obtain the submergence ratio `C_s/C_f` from Figure 4.2.
7. Apply Equation 2. In an implementation, retain the guide's coefficient terms separately for provenance rather than
   algebraically hiding the correction:

   ```text
   Q = C_f * L * H^(3/2) * (C_s / C_f)
   ```

   For free flow the correction is unity. For submerged flow the corrected coefficient is effectively `C_s`.
8. If the submerged result does not match the required total discharge, adjust `h` and repeat. With an associated
   culvert/bridge, the procedure iterates the combined flows at a common upstream level.

Figure 4.2 is a graphical source. No production ordinate table should be created by casual visual estimation. Any
future digitisation must record the source/version, preserve the original range, disallow extrapolation and be checked
against published examples or another independent transcription.

### 4.2 Simplified free-flow method

Section 4.4.3 gives Equation 3:

```text
Q = 1.69 * L * H^(3/2)
```

The guide states this is valid only where the floodway remains a hydraulic control/free outfall, with `D/H < 0.76`.
When `D/H > 0.76`, the guide directs the user to the detailed method for early submergence and to open-channel
Manning analysis when fully submerged.

### 4.3 Important source discrepancy: 0.76 versus 0.8

The legacy material contains two related but non-identical thresholds:

- Section 4.4.3 describes the broad-crested free-flow equation as valid for `D/H < 0.76` and submerged above that
  range.
- Appendix C's operational flowchart and Appendix D worked-example narrative determine the "point of submergence"
  using `D/H = 0.8`.

This specification does **not** silently harmonise those values. An implementation should either:

- reproduce the Appendix C/D procedure explicitly when operating in `LEGACY_REPRODUCTION`; and
- retain the Section 4.4.3 applicability threshold separately in method metadata/warnings;

or resolve the discrepancy from an authoritative clarification before declaring a single value canonical.

## 5. MRWA flow-regime and velocity procedure

### 5.1 Regime definitions

Section 4.6 distinguishes:

- low-tailwater free outfall, with a supercritical jet accelerating down the downstream batter;
- free plunging flow as the hydraulic jump moves upstream and the jet enters the tailwater;
- surface flow when the flow separates from the floodway and passes over the downstream water body;
- submerged flow when depth over the floodway remains greater than critical depth.

The guide states that plunging flow is generally the more severe condition for downstream-batter erosion.

The plunging-to-surface upper transition is defined graphically by Figure 4.5 as `(D/H)_trans` versus `H/l`.
A source-traceable digitisation is required before automating this boundary. The published Appendix D transition
states should be used as regression checks on any digitised curve.

### 5.2 Steady-state velocity - Equation 4

For unit discharge `q`, slope `S` and Manning roughness `n`:

```text
V_s = [ (1/n) * q^(2/3) * S^(1/2) ]^(3/5)
```

The 2006 guide's Table 4.1 gives the following legacy roughness values:

| Protection | Manning `n` |
| --- | ---: |
| Bitumen seal | 0.013-0.016 |
| Concrete | 0.012 |
| Grass | 0.030 |
| Rock mattress | 0.050 |
| Dumped rock - Facing Class | 0.055 |
| Dumped rock - Light Class | 0.055 |
| Dumped rock - 1/4 tonne | 0.060 |
| Dumped rock - 1/2 tonne | 0.060 |
| Dumped rock - 1 tonne | 0.060 |

These should be marked as values reproduced from the 2006 guide, not general modern defaults.

### 5.3 Energy relation - Equations 5 and 6

The guide describes acceleration down a batter using Equation 5:

```text
V_s^2/(2g) + q cos(theta)/V_s = H + Delta p = E_s
```

It states that setting `cos(theta) = 1` introduces less than approximately 3% error for the intended procedure. The
simplified Equation 6 is therefore:

```text
E_s = V_s^2/(2g) + q/V_s
```

For a range of `q`, the legacy graphical procedure plots `TWL + E_s` against the upstream-water-level/backwater curve.

### 5.4 Batter velocity for `TWL <= p`

For the low-tailwater regime:

1. Calculate `V_s(q)` and `E_s(q)` for a range of unit discharges.
2. Form the steady-state energy line `TWL(Q) + E_s(q)`.
3. Find its intersection with the `USWL(Q)` curve while `TWL <= p`.
4. If an intersection exists, `V_bu` is `V_s` at that discharge.
5. If the energy line lies above the `USWL` curve for the event of interest, the available upstream head limits the
   velocity and Equation 7 is used with `Delta p = p - TWL`.

The original guide describes this graphically. A numerical implementation may solve the same equality directly, but
it must preserve the source logic and a bounded root-solving strategy.

### 5.5 Maximum attainable velocity - Equation 7

The 2006 guide defines:

```text
V_m = K * sqrt(H)
```

where `K` is read from Figure 4.6 as a function of `Delta p/H`.

For the `TWL > p` plunging regime, use:

```text
Delta p = p - downstream_shoulder_elevation
```

At the plunging-to-surface transition:

```text
V_bo = min(V_s, V_m)
```

The governing legacy batter velocity is:

```text
V_b = max(V_bu, V_bo)
```

For an arbitrary event, the guide directs the batter calculation to use the lesser of `V_s` and `V_m`, with:

- `Delta p = p - TWL` where tailwater is below the crown; or
- `Delta p = p - downstream_shoulder_elevation` where tailwater is above the crown.

Figure 4.6 is another graphical dependency. It must not be implemented from an eyeballed curve. Useful published
regression anchors from Appendix D include:

| `Delta p/H` | `K` read by the worked example |
| ---: | ---: |
| 0.104 | 3.50 |
| 0.150 | 3.70 |
| 0.307 | 4.20 |
| 0.318-0.320 | 4.25 |

These are validation anchors, not a complete interpolation table.

### 5.6 Pavement velocity

The guide states that the peak pavement velocity occurs at the downstream edge just before submergence. At the legacy
submergence state:

```text
V_p = min(V_s, V_m)
```

with pavement `n` and slope used for `V_s`, and `Delta p` taken from the crown to the downstream shoulder for `V_m`.

For another discharge, use the same lesser-of-`V_s`/`V_m` approach. For discharge beyond submergence, the guide says
pavement velocity may be approximated as:

```text
V_p ~= q / D
```

while noting that this is normally subcritical and of less concern in the legacy procedure.

### 5.7 Critical depth and velocity - Equations 8 and 9

For free outfall only (`D/H < 0.76`), the guide gives:

```text
V_c = sqrt((2/3) g H)
y_c = (2/3) H
```

These quantities must not be populated for submerged conditions merely because the equations are numerically
available.

## 6. Mapping the MRWA hydraulic procedure to A-F design zones

The production result should not report one generic `floodway_force`. Each zone has different physically relevant
demands and failure mechanisms.

| Zone | Initial supported/target quantities | Explicit first-increment limits |
| --- | --- | --- |
| E - upstream batter | approach depth/velocity; shear only with a sourced method | no invented local pressure field |
| D - pavement | local `q`, depth, velocity, Froude, velocity head/specific energy where supported; legacy `V_p`; sourced shear/drag | no single generic force |
| C - downstream shoulder | edge `q`, depth/velocity, regime and energy; geometric warning for abrupt grade break | suction/uplift only if a validated pressure relationship is later adopted |
| B - downstream batter | legacy `V_b`; local `q`, velocity/Froude where supported; HEC-23 riprap check; sourced shear where appropriate | do not apply ordinary uniform-flow shear blindly to a highly nonuniform/plunging zone |
| A - downstream toe/apron | velocity, momentum flux, energy and jump/impingement state where supported | no scour-depth result derived from dynamic pressure alone |
| F - below formation | hydraulic-head/duration indicators and piping/internal-erosion warning | no complete geotechnical piping solver |

The 2006 guide additionally recommends approximately 3.3 m downstream-shoulder rounding to reduce negative pressure,
notes possible uplift under impervious batter protection, warns about leakage/cut-off-wall pressure and discourages
upstands near the downstream shoulder. These should initially be geometry/detail warnings rather than unsupported
pressure calculations.

## 7. Enhanced demand quantities

Where the hydraulic state and selected method support them, useful diagnostic/enhanced quantities include:

```text
unit discharge:          q                 [m2/s]
velocity:                V                 [m/s]
Froude number:           Fr = V/sqrt(g y) [-]
velocity head:           V^2/(2g)          [m]
specific energy:         y + V^2/(2g)     [m]
dynamic pressure metric: 0.5 rho V^2      [Pa]
momentum flux/width:     rho q V           [N/m]
```

Dynamic pressure and momentum flux are **demand metrics**, not universal design forces. Converting them into a force on
a specific structural element requires geometry and an appropriate coefficient/model.

Boundary shear/tractive stress should be reported only through an explicitly selected source method with stated
applicability. A uniform-channel shear approximation is not automatically valid at a plunging jet, downstream
shoulder, moving hydraulic jump or impingement zone.

## 8. FHWA HEC-23 Design Guideline 5 - candidate enhanced riprap method

HEC-23 DG5 is directly applicable to riprap on overtopped embankments and should be implemented as a distinct enhanced
protection check once the equations/examples have been regression-tested.

### 8.1 Applicability and failure model

HEC-23 describes locally high velocities at the downstream shoulder and along the downstream slope. It distinguishes
submerged overtopping, where erosion commonly initiates at the downstream shoulder/nick point, from free low-tailwater
overtopping, where acceleration down the slope can initiate erosion nearer the toe.

The DG5 method is empirical and based on near-prototype overtopping tests. HEC-23 specifically treats flow through a
coarse riprap layer; it should not be recast as an ordinary smooth-boundary tractive-stress check.

HEC-23 divides the design logic at a 1V:4H slope (`S = 0.25`):

- for slopes **steeper** than 1V:4H, all design flow is required to be carried within the riprap layer as interstitial
  flow;
- for **milder** slopes, part of the total flow may pass over the top of the riprap layer.

### 8.2 Equation 5.1 - interstitial velocity

```text
V_i = 2.48 * sqrt(g d50) * S^0.58 / C_u^2.22
```

where:

- `V_i` = interstitial velocity, m/s;
- `d50` = median riprap size, m;
- `C_u = d60/d10`;
- `S` = embankment slope, m/m.

Average velocity through the voids used by the example is:

```text
V_avg = eta * V_i
```

where `eta` is rock-layer porosity.

### 8.3 Equation 5.2 - preliminary median rock size

In SI units, HEC-23 gives `K_u = 0.55` and:

```text
              K_u q_f^0.52                         sin(alpha)
d50 = --------------------------- * [ ----------------------------------------------- ]^1.11
       C_u^0.25 S^0.75              (S_g cos(alpha) - 1)(cos(alpha) tan(phi) - sin(alpha))
```

where:

- `q_f` = unit discharge at failure/design, m3/s/m = m2/s;
- `S_g` = riprap specific gravity;
- `alpha` = embankment slope angle;
- `phi` = riprap angle of repose.

The preliminary `d50` is then rounded **up** to an available riprap gradation/class per the applicable gradation table.
The calculation must retain both the theoretical result and selected class size.

### 8.4 Equation 5.3 - allowable surface-flow depth for milder slopes

For `S < 0.25`:

```text
h = 0.06 (S_g - 1) d50 tan(phi) / (0.97 S)
```

This is the allowable depth flowing over the riprap used by the DG5 thickness procedure.

### 8.5 Thickness procedure

The design sequence from HEC-23 should be preserved rather than replaced with a single size equation:

1. Determine overtopping unit discharge `q_f` and, where useful, broad-crested-weir depth for the source example.
2. Calculate preliminary `d50` using Equation 5.2.
3. Select the next appropriate riprap class/gradation.
4. Calculate `V_i` with Equation 5.1 and `V_avg = eta V_i`.
5. Calculate the depth required if all flow were interstitial:

   ```text
   y = q_f / V_avg
   ```

6. If `y <= 2 d50`, a `2 d50` layer satisfies the HEC-23 example logic.
7. If `y > 2 d50` and `S > 0.25`, increase the riprap class and repeat rather than allowing surface flow.
8. If the slope is milder, calculate `h` from Equation 5.3, calculate surface-flow capacity over the layer with the
   HEC-23 Manning-Strickler roughness procedure, and assign the residual discharge to interstitial flow.
9. Check whether `2 d50` interstitial thickness carries the residual flow. If not, HEC-23 proceeds to a thicker layer
   and then a larger gradation if required.

The HEC-23 example uses a Manning-Strickler coefficient of `0.0414` in SI for the roughness relationship. Exact code
should follow the SI source presentation and its unit convention, with an authoritative regression test before release.

### 8.6 HEC-23 warnings/provenance

The result should retain at least:

- HEC-23 edition/publication number;
- DG5 method identity;
- input `q_f`, `S`, `alpha`, `C_u`, `S_g`, `phi`, `eta`;
- theoretical and selected `d50`;
- chosen thickness multiplier/actual thickness;
- whether all flow is required to be interstitial;
- source-range/applicability status;
- any gradation/class lookup source.

The method should not automatically declare MRWA compliance merely because HEC-23 produces a stable riprap size.

## 9. Chen and Anderson / FHWA embankment-damage research

The 1987 Transportation Research Record paper supports several architectural decisions:

- free-plunging, free-surface and submerged overtopping are distinct hydraulic states;
- hydraulic-jump/tailwater position materially changes the downstream-slope demand;
- sectional velocity and shear are more meaningful than one crossing-wide velocity;
- erosion depends on both hydraulic stress and material resistance/duration;
- varying roadway elevation can be represented by separate reaches rather than one equivalent crest.

The paper and its underlying FHWA-RD-86-126 report include equations for surface/plunging flow and shear, but the
available text extraction is not sufficiently reliable to adopt those exact formulae here without visual verification of
the originals. Therefore:

- use the source now as support for regime/state architecture and research direction;
- do **not** implement its OCR-derived equations from this document;
- retain a research item to obtain/verify the original equation presentation and bounds before deciding whether a
  sectional 1D formation solver belongs in the first production increment.

## 10. Willare Crossing / downstream shoulder evidence

The 1986 Willare model tests and 1988 case history are directly relevant to Zone C. The available source records report
that testing identified negative pressure near the downstream shoulder, after which the downstream batter was flattened
from approximately 2H:1V to 3H:1V, the edge was rounded to a smooth curve of about 3 m radius and the rock-mattress /
concrete-slab joint was moved clear of the low-pressure zone.

This supports:

- treating downstream-shoulder geometry as an explicit floodway design input;
- warning on abrupt grade breaks;
- retaining the 2006 MRWA approximately 3.3 m rounding recommendation in the compliance/detail report;
- keeping local suction/uplift as a separate limit state.

It does **not** yet support a general-purpose numerical shoulder suction coefficient. Until the underlying model-test
pressure data/method is available and validated, shoulder pressure should remain `SOURCE_DATA_REQUIRED` or
`SPECIALIST_REVIEW_REQUIRED` rather than being fabricated from velocity head.

## 11. Event/discharge envelope specification

The 2006 guide explicitly warns that maximum design flow rarely corresponds to peak floodway velocity. The analysis
therefore needs a hydraulic-state envelope rather than a single design-peak calculation.

The event set should include, where resolvable:

1. first roadway overtopping;
2. shallow free-flow states immediately above overtopping;
3. candidate low-tailwater `TWL <= p` intersection controlling `V_bu`;
4. plunging-to-surface transition;
5. legacy point of submergence;
6. initial supported submerged roadway flow;
7. nominated serviceability/design events;
8. larger/extreme events used for failure-mode review;
9. hydrograph points required to determine overtopping/closure duration when a hydrograph is supplied.

Between these mandatory states, use a configurable/adaptive sweep dense enough to capture interior maxima. Do not
assume monotonicity of velocity, shear or protection demand with total discharge.

For every output/limit state retain:

- governing event name/AEP where available;
- total discharge;
- culvert discharge;
- roadway discharge;
- local roadway unit discharge;
- headwater/tailwater;
- flow regime/submergence state;
- the demand value and method source;
- whether the governing state is a supplied event, a detected transition or an interpolated/refined envelope point.

The post-#86 implementation should use the repository's final scenario/event model rather than introducing a parallel
one prematurely.

## 12. 2D/specialist escalation

The first analytical workflow should flag, not pretend to solve, cases where local spatial hydraulics control the design.
Candidate reasons include:

- strongly skewed approach flow;
- substantial cross-road variation in velocity/depth not represented by the longitudinal crest segmentation;
- multiple competing overtopping paths or outflanking;
- complex tailwater/recirculation caused by nearby structures;
- hydraulic jumps migrating over complex/nonuniform formation geometry;
- a structural decision depending on local downstream-shoulder pressure/uplift;
- major toe scour, channel migration or sediment-transport interaction;
- source-method inputs outside validated empirical ranges;
- a design result materially sensitive to a quantity that the analytical method can only approximate.

A result may therefore be numerically available but still carry `TWO_D_VERIFICATION_RECOMMENDED`.

## 13. Figure/curve digitisation policy

Figures 4.2, 4.5 and 4.6 are critical legacy graphical inputs. If digitised:

1. record document title, issue/date, figure number and source image/page;
2. store the digitised ordinates separately from calculation code;
3. retain the original x/y domain and prohibit extrapolation by default;
4. record digitisation method and any interpolation method;
5. verify independent points against the published Appendix D worked examples;
6. preserve the ability to report the source ordinate/factor used;
7. require a review before treating a hand-digitised curve as design-authoritative data.

The worked-example `K` values listed in Section 5.5 are regression anchors only and are insufficient to reconstruct the
full Figure 4.6 curve.

## 14. Initial report contract

A reviewable result should eventually contain:

1. source hierarchy and selected methods;
2. crossing geometry and final `ryan-culverts` hydraulic inputs/results;
3. floodway formation geometry and protection materials;
4. event/discharge envelope and detected regime transitions;
5. per-zone A-F demand envelope;
6. governing event/discharge for each reported quantity;
7. MRWA compliance/legacy results;
8. enhanced HEC-23 or other checks where supported;
9. applicability statuses and source warnings;
10. serviceability/overtopping duration where relevant data exist;
11. 2D/specialist-review reasons;
12. machine-readable provenance sufficient to reproduce the calculation.

Console, Markdown, JSON and CSV outputs should be views of the same typed result model after implementation.

## 15. Pre-implementation decisions now substantially resolved

The research to date supports the following decisions:

- crossing flow split remains in `ryan-culverts`;
- the legacy MRWA procedure should be reproducible and recognisable rather than silently replaced;
- maximum-demand search requires an event/discharge envelope;
- pavement, shoulder, batter and toe are separate hydraulic/design zones;
- HEC-23 DG5 is a strong candidate enhanced riprap method for overtopping embankments;
- shoulder negative pressure is a real failure mechanism but is not yet supported by a bounded general calculation;
- dynamic pressure/momentum/shear are separate demand metrics, not one generic force;
- debris remains future scope;
- complex/local cases should fail/escalate cleanly rather than extrapolate empirical methods.

## 16. Remaining research before substantive coding

The following can continue before PR #86 merges:

- independently verify/digitise the required MRWA Figure 4.2, 4.5 and 4.6 relationships if they are to be automated;
- obtain the best available original Patterson-Abercromby Willare model-test material and pressure evidence;
- verify the Chen-Anderson/FHWA-RD-86-126 equations visually before deciding on a sectional 1D formation solver;
- decide whether an analytically supported toe/impingement/scour method belongs in the first increment or remains a
  specialist/2D trigger;
- verify the HEC-23 DG5 SI worked examples and gradation selections against the authoritative document;
- compare current Austroads Part 5B requirements where the full licensed text is available.

After PR #86 and `ryan-culverts` #14 are complete, refresh the branch from fresh `main`, map these calculations onto the
final shared configuration/result/reporting models, and only then begin substantial Python implementation.
