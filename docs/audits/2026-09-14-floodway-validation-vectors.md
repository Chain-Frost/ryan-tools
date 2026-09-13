# Floodway validation vectors

Date: 2026-09-14

Status: **Source-derived validation pack for issue #87. No production tests have been implemented yet.**

This document records published worked-example values and independently recomputed arithmetic checks that can be
turned into regression tests after PR #86 is merged and the floodway implementation is built against refreshed
`main`.

It accompanies:

- [`2026-09-13-floodway-design-research.md`](2026-09-13-floodway-design-research.md); and
- [`2026-09-14-floodway-calculation-specification.md`](2026-09-14-floodway-calculation-specification.md).

The purpose is to distinguish **source targets** from values inferred or recomputed during implementation. A future
test should not silently replace a published value with a cleaner recalculation.

## 1. Validation principles

### 1.1 Evidence classes

Each future test vector should retain one of these evidence labels:

- `PUBLISHED`: value is explicitly printed in an authoritative source/worked example;
- `RECOMPUTED_FROM_PUBLISHED_INPUTS`: arithmetic has been independently recomputed using printed inputs/equations;
- `GRAPH_READ`: result depends on reading a published graph such as MRWA Figure 4.6;
- `DIGITISED`: result depends on a traceable digitised ordinate set;
- `DERIVED_SANITY_CHECK`: useful equation/unit check not itself printed as a worked-example target.

### 1.2 Tolerances

Do not use one universal tolerance.

Suggested initial policy after implementation:

- direct closed-form arithmetic using exact stated inputs: tight floating-point tolerance, for example relative
  tolerance around `1e-6` before comparison to rounded source output;
- comparison to a source value printed to two decimals: tolerance should reflect source rounding, typically at least
  `0.005` in the printed unit and potentially larger if upstream inputs are rounded;
- graph-read values such as `K` from MRWA Figure 4.6: tolerance must reflect graph resolution/digitisation uncertainty;
- final worked-example velocities that depend on graph reads: set tolerance only after the adopted curve digitisation
  has been independently checked.

The published rounded result is the engineering regression target. A future test may additionally assert the
higher-precision recomputation where the inputs are unambiguous.

## 2. Main Roads WA 2006 equations - arithmetic unit vectors

Source: Main Roads WA *Floodway Design Guide*, Sections 4.4 and 4.6.

### 2.1 Equation 4 - steady-state velocity

```text
V_s = [ (1/n) q^(2/3) S^(1/2) ]^(3/5)
```

Useful direct checks:

| Case | `q` m2/s | `S` | `n` | recomputed `V_s` m/s | expected rounded |
| --- | ---: | ---: | ---: | ---: | ---: |
| Seven Mile batter table row | 0.10 | 1/3 | 0.040 | 1.9753 | 1.98 |
| Seven Mile batter table row | 0.20 | 1/3 | 0.040 | 2.6064 | 2.61 |
| Seven Mile batter table row | 0.30 | 1/3 | 0.040 | 3.0653 | 3.07 |
| Seven Mile batter table row | 0.40 | 1/3 | 0.040 | 3.4392 | 3.44 |
| Seven Mile batter table row | 0.50 | 1/3 | 0.040 | 3.7602 | 3.76 |
| Seven Mile pavement at `Q=150` | 0.50 | 0.03 | 0.015 | 3.2890 | 3.29 |
| Majors Creek pavement Q50 | 0.44 | 0.03 | 0.015 | 3.1251 | 3.12 in source |

The last row illustrates source/input rounding: the guide prints `V_s = 3.12 m/s` using its displayed values. Do not
force a future test to an unjustifiably tighter agreement than the source data allow.

### 2.2 Equation 6 - steady-state energy

```text
E_s = V_s^2/(2g) + q/V_s
```

With `g = 9.81 m/s2`, the Seven Mile `q=0.50`, `V_s=3.76024` case gives approximately `E_s = 0.8536 m`, matching
the source table value `0.85 m` after rounding.

### 2.3 Equation 7 - maximum attainable velocity

```text
V_m = K sqrt(H)
```

Published worked-example graph-read anchors provide useful checks:

| Case | `H` m | `Delta p/H` | published `K` | recomputed `V_m` m/s | source rounded m/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Seven Mile pavement, Q=150 | 0.44 | 0.307 | 4.20 | 2.7860 | 2.79 |
| Seven Mile batter, Q=150 | 0.44 | 0.318 | 4.25 | 2.8191 | 2.82 |
| Majors Creek batter, Q50 | 0.50 | 0.320 | 4.25 | 3.0052 | 3.01 |
| Majors Creek pavement, Q50 | 0.50 | 0.300 | 4.20 | 2.9698 | 2.97 |

`K` is a `GRAPH_READ` value from Figure 4.6. These rows validate the use of a supplied `K`; they do not by themselves
validate a future Figure 4.6 digitisation.

### 2.4 Equations 8 and 9 - critical state sanity check

For free outfall only:

```text
V_c = sqrt((2/3) g H)
y_c = (2/3) H
```

For `H=0.44 m`, a derived sanity check is approximately:

```text
V_c = 1.696 m/s
y_c = 0.2933 m
```

This is `DERIVED_SANITY_CHECK`, not a published Appendix D target.

## 3. MRWA Example 1 - Seven Mile Creek

Source: 2006 Main Roads WA *Floodway Design Guide*, Appendix D, Example 1.

### 3.1 Published geometry and simplification

The example states:

- proposed culvert: three-barrel 1200 mm x 750 mm RCB;
- expected culvert flow only about 8 m3/s, ignored because it is small relative to floodway flow;
- floodway overtopping length `L = 300 m`;
- floodway crown/invert `p = 380.50 m`;
- floodway width in the flow direction `l = 9.0 m`;
- downstream batter `S = 1/3`, `n = 0.04`.

The example first finds the legacy submergence point using `D/H = 0.8`, then the upper plunging-to-surface transition.

### 3.2 Table D1 - published `V_s` / `E_s` targets

Because culvert flow is ignored, the source uses `Q = q L`.

| `q` m2/s | `Q` m3/s | published `V_s` m/s | published `E_s` m |
| ---: | ---: | ---: | ---: |
| 0.0 | 0 | 0.00 | 0.00 |
| 0.1 | 30 | 1.98 | 0.25 |
| 0.2 | 60 | 2.61 | 0.42 |
| 0.3 | 90 | 3.07 | 0.57 |
| 0.4 | 120 | 3.44 | 0.71 |
| 0.5 | 150 | 3.76 | 0.85 |

These are primary regression targets for Equations 4 and 6.

### 3.3 Governing maximum batter velocity

Published target sequence:

- intersection of `TWL + E_s` with `USWL`: `Q = 105 m3/s`, `q = 0.35 m2/s`;
- this occurs below crown level;
- `V_bu = 3.26 m/s`;
- at plunging-to-surface transition: `q = 1.443 m2/s`, `H = 0.90 m`;
- Equation 4 gives `V_s = 5.75 m/s`;
- `Delta p = 0.135 m`, hence `Delta p/H = 0.150`;
- Figure 4.6 gives `K = 3.70`;
- Equation 7 gives `V_m = 3.51 m/s`;
- `V_bo = min(V_s, V_m) = 3.51 m/s`;
- governing `V_b = max(V_bu, V_bo) = 3.51 m/s`.

The source then selects 1/4-tonne-class rock with a 1.00 m section thickness using its Table 5.1. That protection
selection should be tested separately from the hydraulic velocity calculation.

### 3.4 Published maximum pavement velocity

The source uses:

- pavement `n = 0.015`;
- pavement slope `S = 0.03`;
- Equation 4 result `V_s = 6.27 m/s` at the lower limit of submergence;
- `H = 1.30 m`;
- `Delta p = 0.135 m`, hence `Delta p/H = 0.104`;
- Figure 4.6 `K = 3.50`;
- Equation 7 `V_m = 3.99 m/s`;
- governing `V_p = min(V_s, V_m) = 3.99 m/s`.

The parsed source represents the corresponding `q` poorly in the typeset equation. Until it is independently read
from the original worksheet/figure, the validation vector should rely on the explicitly published `V_s`, `H`, ratio,
`K`, `V_m` and `V_p` rather than inventing an exact `q` from OCR.

### 3.5 Seven Mile arbitrary-discharge check at `Q=150 m3/s`

This is a particularly useful event-level regression vector because the source prints most intermediate values.

Because culvert flow is neglected:

```text
q = Q/L = 150/300 = 0.50 m2/s
```

Pavement:

- Equation 4 `V_s = 3.29 m/s`;
- `USWL = 380.94 m`;
- `H = 0.44 m`;
- `Delta p = 0.135 m`;
- `Delta p/H = 0.307`;
- Figure 4.6 `K = 4.20`;
- `V_m = 2.79 m/s`;
- published `V_p = 2.79 m/s`.

Batter:

- `TWL = 380.36 m`, below crown;
- `Delta p = p - TWL = 0.14 m`;
- `Delta p/H = 0.318`;
- Figure 4.6 `K = 4.25`;
- `V_m = 2.82 m/s`;
- published `V_b = 2.82 m/s`.

The source then selects Light Class rock with 0.75 m section thickness for this event.

## 4. MRWA Example 2 - Majors Creek

Source: 2006 Main Roads WA *Floodway Design Guide*, Appendix D, Example 2.

### 4.1 Published geometry/context

The example uses:

- three 2.7 m x 2.7 m RCBs;
- floodway overtopping length `L = 215 m`;
- floodway crown/invert `p = 75.30 m`;
- floodway width `l = 9.0 m`;
- downstream batter `S = 1/3`, `n = 0.03`;
- a culvert backwater curve established separately;
- submerged flow only at a very large event beyond the practical pavement design range in the example.

### 4.2 Table D2 - published values

| `q` m2/s | `H` m | `USWL` m | published `V_s` m/s | published `E_s` m |
| ---: | ---: | ---: | ---: | ---: |
| 0.0 | 0.00 | 75.30 | 0.00 | 0.00 |
| 0.1 | 0.15 | 75.45 | 2.35 | 0.33 |
| 0.2 | 0.24 | 75.54 | 3.10 | 0.55 |
| 0.3 | 0.32 | 75.64 | 3.64 | 0.76 |
| 0.4 | 0.38 | 75.68 | 4.09 | 0.95 |
| 0.5 | 0.44 | 75.74 | 4.47 | 1.13 |

These are direct Equation 4/6 regression targets.

### 4.3 Maximum theoretical batter-velocity sequence

Published values:

- `E_s`/`USWL` intersection at `Q = 136 m3/s`;
- `USWL = 75.60 m`, `H = 0.30 m`, `q = 0.28 m2/s`;
- `V_bu = V_s = 3.53 m/s`;
- transition `q = 3.043 m2/s`, `H = 1.48 m`;
- transition `V_s = 9.17 m/s`;
- `Delta p = 0.15 m`, ratio approximately `0.10`;
- Figure 4.6 `K = 3.50`;
- `V_m = 4.26 m/s`;
- `V_bo = 4.26 m/s` and theoretical governing `V_b = 4.26 m/s`.

The guide notes that this corresponds to approximately a 1000-year flow and instead demonstrates a practical Q50
protection design.

### 4.4 Q50 event - `Q=195 m3/s`

Batter published inputs/results:

- `TWL = 75.14 m`;
- `USWL = 75.80 m`;
- `H = 0.50 m`;
- `q = 0.60 m2/s` for the batter event calculation;
- `Delta p = 0.16 m`;
- `Delta p/H = 0.32`;
- Figure 4.6 `K = 4.25`;
- `V_m = 3.01 m/s`;
- published Q50 design batter velocity `V_b = 3.01 m/s`.

Pavement published inputs/results:

- pavement `S = 0.03`, `n = 0.015`;
- the printed Equation 4 calculation uses `q = 0.44 m2/s`;
- source `V_s = 3.12 m/s`;
- `Delta p = 0.15 m` and `Delta p/H = 0.30`;
- Figure 4.6 `K = 4.20`;
- `V_m = 2.97 m/s`;
- published Q50 design pavement velocity `V_p = 2.97 m/s`.

These final Q50 targets are valuable because they demonstrate that the governing protection event need not be the
source's theoretical maximum-velocity event.

## 5. MRWA legacy-threshold tests

A future legacy implementation should explicitly test the source distinction between:

- Section 4.4.3 free-flow applicability: `D/H < 0.76`;
- Appendix C/D operational "point of submergence": `D/H = 0.8`.

Suggested tests:

1. a state below `0.76` is accepted by the simplified free-flow equation;
2. a state above the Section 4.4.3 threshold does not silently use the simplified method;
3. `LEGACY_REPRODUCTION` can reproduce the Appendix C/D `0.8` target without relabelling `0.8` as the universal
   physical threshold;
4. provenance/warnings expose which threshold is being applied.

## 6. MRWA figure-digitalisation validation anchors

### 6.1 Figure 4.6 - `K(Delta p/H)`

At minimum, a future digitised/interpolated Figure 4.6 representation should reproduce the following printed reads to
within an agreed graph-resolution tolerance:

| `Delta p/H` | source `K` | source occurrence |
| ---: | ---: | --- |
| 0.104 | 3.50 | Seven Mile maximum pavement calculation |
| 0.150 | 3.70 | Seven Mile transition batter calculation |
| 0.307 | 4.20 | Seven Mile pavement at Q=150 |
| 0.318 | 4.25 | Seven Mile batter at Q=150 |
| 0.300 | 4.20 | Majors Creek Q50 pavement |
| 0.320 | 4.25 | Majors Creek Q50 batter |

Do not construct the production curve from these six points alone.

### 6.2 Figure 4.5 - transition curve

The Appendix D worksheets contain transition states that should be retained as end-to-end checks once Figure 4.5 is
digitised. In particular:

- Seven Mile transition: `q = 1.443 m2/s`, `H = 0.90 m`;
- Majors Creek transition worksheet converges near `USWL = 76.78 m`, `H = 1.48 m`, `q = 3.04 m2/s`, with
  `D/H = 0.68`, `H/l = 0.16` and source transition ordinate approximately `0.67`.

The second vector is especially useful for checking both curve interpolation and iterative convergence.

## 7. FHWA HEC-23 DG5 equation vectors

Source: FHWA-NHI-09-112, HEC-23 Volume II, Design Guideline 5.

### 7.1 Equation 5.2 - theoretical `d50`

SI form:

```text
              K_u q_f^0.52                         sin(alpha)
d50 = --------------------------- * [ ----------------------------------------------- ]^1.11
       C_u^0.25 S^0.75              (S_g cos(alpha) - 1)(cos(alpha) tan(phi) - sin(alpha))
```

with `K_u = 0.55` in SI.

Common example inputs:

```text
q_f = 0.186 m2/s
C_u = 2.1
S_g = 2.65
phi = 42 deg
```

Independent recomputation gives:

| Slope case | `S` | `alpha` | recomputed theoretical `d50` | source rounded |
| --- | ---: | ---: | ---: | ---: |
| 1V:5H | 0.20 | 11.3 deg | ~0.0940 m | 0.094 m / 3.7 in |
| 1V:2H | 0.50 | 26.6 deg | ~0.290 m | 0.29 m / 11.5 in |

These should become direct Equation 5.2 unit tests.

### 7.2 Example 5.5.2 - slope milder than 1V:4H

Published inputs:

```text
Q = 56.63 m3/s
L = 304.8 m
q_f = 0.186 m2/s
C = 1.57 m^0.5/s
K_u = 0.55
S = 0.20
alpha = 11.3 deg
S_g = 2.65
C_u = 2.1
eta = 0.45
phi = 42 deg
Manning-Strickler coefficient = 0.0414 (SI)
```

Regression sequence:

1. Broad-crested example depth:

   ```text
   H = (Q/(C L))^(2/3) = ~0.241 m -> source 0.24 m
   ```

2. Equation 5.2 theoretical `d50 = ~0.094 m` -> source `0.094 m`.
3. Source selects Class I `d50 = 6 in = 0.15 m`.
4. Equation 5.1 with selected class gives `V_i = ~0.228 m/s`.
5. `V_avg = eta V_i = ~0.103 m/s`.
6. Interstitial-only depth `y = q_f/V_avg = ~1.81 m`.
7. Equation 5.3 gives allowable surface depth `h = ~0.069 m`.
8. SI Manning-Strickler roughness with selected `d50` gives approximately `n = 0.0302`; source English worked value
   rounds to `0.030`.
9. Surface-flow capacity is approximately `0.17 m3/s/m`; source gives `0.173 m3/s/m` based on its rounded English
   calculation.
10. Residual interstitial flow source value is `q_2 = 0.013 m3/s/m`.
11. Capacity of a `2 d50` interstitial layer is source `0.031 m3/s/m`.
12. Because capacity exceeds residual demand, the source design completes at thickness `2 d50`.

The SI recomputation from unrounded values can differ slightly from the English-first published example. Preserve both
source rounded values and direct-SI arithmetic in tests rather than treating the difference as a failure.

### 7.3 Example 5.5.3 - slope steeper than 1V:4H

Same common inputs except:

```text
S = 0.50
alpha = 26.6 deg
```

Published/recomputed sequence:

1. `H ~= 0.24 m`.
2. Equation 5.2 theoretical `d50 ~= 0.29 m` (`11.5 in`).
3. Source selects the next class at **12 in**. Twelve inches is approximately **0.305 m**.
4. With the 12-in class, source reports `V_i ~= 0.548 m/s`, `V_avg ~= 0.247 m/s`, and `y ~= 0.75 m`.
5. `y` is greater than `2 d50`, and because `S > 0.25`, surface flow is not permitted by the DG5 steep-slope
   procedure; increase the class.
6. Source next trial selects Class IV `d50 = 15 in` (~0.381 m).
7. Source reports approximately `V_i = 0.617 m/s`, `V_avg = 0.278 m/s`, `y = 0.67 m`.
8. Since `y < 2 d50` for the larger class, the design completes with the source's selected class and `2 d50`
   thickness.

### 7.4 Published SI inconsistency to guard against

Some indexed copies/text extraction of HEC-23 display the steep-example Class III selection as:

```text
d50 = 12 inches (0.15 m)
```

That conversion is dimensionally impossible; 12 inches is approximately 0.305 m, and the following source
calculation uses `1.0 ft`, confirming the 12-in size. A future implementation must use an independently verified visual
copy of the authoritative page/gradation table and should not ingest the erroneous/extracted `0.15 m` value.

This is precisely the kind of source-unit inconsistency the validation pack is intended to expose.

## 8. HEC-23 routing/limit tests

Future method tests should include:

- `S < 0.25`: allow the mild-slope branch that may split total discharge into surface and interstitial components;
- `S > 0.25`: require all flow within the riprap thickness and increase gradation if `y > 2 d50`;
- boundary `S = 0.25`: define behaviour explicitly from the authoritative wording before implementation rather than
  relying on a floating-point accident;
- reject/flag nonphysical denominators in Equation 5.2 rather than returning a complex/negative rock size;
- validate `C_u > 0`, `S > 0`, positive `q_f`, physically valid `S_g`, and angle units;
- retain theoretical `d50` separately from the selected gradation/class;
- no extrapolation of a gradation table beyond available classes without an explicit unsupported result.

## 9. Event-envelope regression scenarios

After crossing integration is available, include synthetic tests whose purpose is algorithmic rather than tied to one
published site:

1. **Interior velocity maximum**: supplied discharge states are arranged so `V_b` peaks below maximum `Q`; the
   envelope must retain the interior governing state.
2. **Different zone governors**: pavement and batter maxima occur at different events; result must preserve both.
3. **Transition injection**: a regime-transition state not present in the user's nominal event list is inserted/refined
   and becomes governing.
4. **Submergence boundary**: states immediately below/above the supported submergence boundary retain distinct
   applicability/regime metadata.
5. **Irregular crest**: local roadway `q` from `ryan-culverts` differs materially between crest segments; floodway
   assessment must use the public segment state rather than `Q_road/L_total`.
6. **No-roadway-flow event**: formation demand returns an inactive/not-applicable state rather than dividing by zero.
7. **2D escalation**: a scenario flagged for skew/outflanking/local-pressure dependence still reports supported scalar
   quantities but carries `TWO_D_VERIFICATION_RECOMMENDED`.

Exact fixtures should be designed after #86/#14 finalise the public scenario/result models.

## 10. Reporting/provenance acceptance checks

For every published-method regression, future tests should verify not just the number but also:

- source/method identifier;
- equation or curve identity where practical;
- applicability status;
- governing event/state;
- input units;
- whether a value was calculated, graph-read/digitised, selected from a class table or supplied by the crossing solver;
- warning if a method is legacy-only or enhanced/non-MRWA;
- explicit unsupported result rather than extrapolation outside the source method.

JSON/CSV/Markdown views must not independently recalculate the engineering result.

## 11. Validation still deliberately deferred

No repository tests, Ruff, Pyright or documentation checker were run while preparing this research increment through
the GitHub connector environment. These are deliberately recorded as **not run**, not passed.

After the branch is available in a normal checkout, an agent should at minimum run the repository-required
documentation/link checks for this documentation-only increment. Python/lint/type validation becomes relevant when
substantive implementation begins after the post-#86 refresh.
