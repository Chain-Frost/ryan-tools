# Rainfall-loss screening (experimental)

The `ryan_library.functions.hydrology.rainfall_loss_screening` functions screen
**rainfall excess**, not routed peak flow. They never delete simulations or
silently discard rainfall: a result of `ZERO` is safe to exclude only for the
specified loss convention, event, catchment, and selected temporal patterns.
A result of `UNRESOLVED` must be retained. `POSITIVE` also remains in the run
set, even if its runoff is small.

## Maintained command-line wrapper

The maintained, copyable wrapper is
[ryan-scripts/hydrology-python/screen_rainfall_losses.py](../ryan-scripts/hydrology-python/screen_rainfall_losses.py).

It follows the repository's [wrapper standard](../ryan-scripts/WRAPPER_STANDARD.md):
editable values are at the start of the file; CLI arguments override them;
the opening and closing banners identify the source and installed package;
and --no-pause is available for automated execution. This tool only reads
rainfall/ARR data, optionally makes network requests, and prints a Rich
command-line table. Use --csv-report to **create a new** machine-readable
summary; an existing file is never overwritten.

### Live ARR 2026 data at the uploaded BoM site's coordinates

```powershell
py -3.14 ryan-scripts/hydrology-python/screen_rainfall_losses.py --source arr --latitude -23.08632 --longitude 119.2755 --aep-mode common --view all --no-pause
```

The ARR backend is PyTUFLOW's `pytuflow.arr.api_client.ArrApiClient`
and `ArrEngine`. This command queries the public Data Hub 2026 API at the
*requested coordinate*, selects the Data Hub's **2030 RecIFD** by default,
and uses the **complete-storm method by default**: it prepends preburst rainfall
before the design burst and applies full storm initial loss once.
It is **not** the same as using the attached BoM CSV depths; 2030 RecIFD
may differ from a 2016-methodology BoM export. To use the historical ARR
1990 baseline, set `IFD_BASELINE_YEAR=1990` or `--baseline-year 1990`.
A catchment area of zero means **point rainfall only**; supply an actual
`--catchment-area KM2` to use areal patterns/ARF as applicable. This is
not an assumed physical project catchment area.

`--storm-mode complete` is the **default**, including preburst rainfall
before the main design burst with full storm initial loss. Use
`--storm-mode main-burst-only` to omit explicit preburst and instead
use preburst-adjusted burst IL. The `--preburst-percentile recommended`
default uses the Data Hub's preferred
preburst ratio/shape subject to its jurisdictional availability. The
selected initial-loss mode must match the intended hydraulic model.
PyTUFLOW may automatically switch particular burst-loss cells to complete
storm when a fixed burst IL is unavailable.

### Worked wrapper settings: 350 km² catchment

The following is a **configuration example**, not a claim that the source
coordinate has a precisely 350 km² catchment or any particular recommended
losses. Edit these values near the top of the script:

```python
SOURCE = "arr"
LATITUDE: float | None = -23.08632
LONGITUDE: float | None = 119.2755
CATCHMENT_AREA_KM2 = 350.0
INITIAL_LOSS_MM: float | None = 50.0
CONTINUING_LOSS_MM_PER_HOUR: float | None = 5.0
RUNOFF_COEFFICIENT: float | None = None
ARF_OVERRIDE: float | None = None  # Use Data Hub ARF; do not force unity.
LOSS_TIMING = "full_increment"  # Default experimental RORB profile; fractional remains optional.
STORM_MODE = "complete"
LOSS_REFERENCE_YEAR = 1990  # User losses use historical 1961–1990; set 2030 for current losses.
CLIMATE_SCENARIOS: list[str] = ["2050:SSP2"]
AEP_MODE = "common"
DURATION_SELECTION_MIN: list[int] = []  # All available RecIFD durations.
SHOW_MATRIX = True
```

With `ARF_OVERRIDE=None`, PyTUFLOW's ARR ARF is applied by the
ARR engine **before rainfall losses**; the point rainfall, actual
per-AEP/per-duration ARF, and adjusted rainfall are printed in different
columns. Note that **63.2% AEP is above the normal ARR 50%-AEP upper
limit for ARF**, so the factor remains **1.0** by default, even at
350 km². **63.2% is removed from the default `COMMON_AEPS` selection**,
but remains available using `--aep 63.2%` or `--aep-mode all`.
PyTUFLOW permits a deliberate extension of ARF calculations above 50%
AEP, outside the default policy. The wrapper does not enable it. For e.g. 20% AEP at 120 minutes, the ARR short-duration
ARF at 350 km² is approximately 0.7814; the exact factor may vary
with source selection and policy. An explicitly supplied
`--arf-override` applies the same factor to every AEP/duration and
is labelled in the output.

**BoM** screening also uses PyTUFLOW's ARF calculator, not a copied
implementation. For a 350 km² catchment it needs ARR `ARFParams`
from a saved `--arr-response` JSON or live Data Hub request.
If a local JSON is available it overrides online lookup. A BoM-based
rainfall multiplier (`RAINFALL_FACTOR` / `--rainfall-factor 1.15`)
applies to rainfall depth **before IL/CL**; this is not a substitute
for the ARR 2026 recommended climate-change dataset.

### Climate-adjusted user losses (ARR SSP)

For ARR events, configure the climate scenarios near the baseline-year inputs,
e.g. `CLIMATE_SCENARIOS = ["2050:SSP2"]`, or pass `--cc 2050:SSP2`.
Baseline is always included. **User loss reference year** is a separate input:
`LOSS_REFERENCE_YEAR = 1990` or `--loss-reference-year 2030`. This does
**not** change the IFD rainfall baseline `IFD_BASELINE_YEAR`.

The Data Hub's `ClimateChange.loss_factors` tables provide independent
initial-loss and continuing-loss multipliers. A historical 1961–1990 user
loss uses each scenario's factors. A loss input already referenced to **2030**
uses the scenario/2030 **ratio for the same SSP**, and requires both values
to exist in the saved/online Data Hub response. Baseline user inputs remain
unchanged. The two factors must not be combined with rainfall SSP factors.

For one uniform historical IL/CL setting, no per-AEP overrides and
`STORM_MODE="complete"`, the wrapper passes the user losses to PyTUFLOW's
`ArrConfig.losses.user_initial_loss/user_continuing_loss` instead of
overwriting the engine's climate-adjusted event losses. Where a preburst
period remains present, screening uses the **native adjusted IL/CL**. When
PyTUFLOW suppresses a very small preburst (<1% of point rainfall), the native
burst IL may instead be `(baseline storm IL - baseline preburst) × CC factor`,
which differs from the intended `CC storm IL - CC preburst`. This wrapper
**reconciles only those omitted-preburst cases** against the source scenario
ratio and adjusted full storm IL; if that reconciliation cannot be established,
the event is **UNRESOLVED** rather than an unsafe DROP. The CSV distinguishes
native and omitted-preburst-corrected loss sources.
For per-AEP/duration overrides and 2030
user baselines the selected user values are adjusted per scenario from
PyTUFLOW's response loss factors, with **full storm IL** adjusted before
preburst reconciliation. In `main-burst-only` mode a user IL is explicitly
interpreted as an already-reduced **burst IL** and scaled directly, which
is not the same as PyTUFLOW's storm-loss-derived CC burst IL method.

User IL plus runoff coefficient is **not** treated as climate-adjusted IL/CL:
a requested SSP scenario with such an override is rejected rather than
scaling the coefficient or reporting an unjustified DROP. Missing,
nonpositive or nonfinite required factors also fail closed, including
offline runs. Do not replace unavailable factors with unity, and do not
treat screening as native TUFLOW loss-timing qualification (#319).

### ARF validity warnings and Ladson interpolation guidance

The wrapper uses **PyTUFLOW's ARR ARF calculations**, not a second ARF
implementation. The published [ARR Book 2 v4.2](https://www.arr-software.org/pdfs/ARR_190514_Book2_V4.2.pdf)
and [Tony Ladson's ARF review](https://tonyladson.wordpress.com/2020/04/05/arr2019-areal-reduction-factors/)
agree on the interpolation equations: **duration** interpolation for 12–24 h
is Eq. **2.4.3**; **area** interpolation for 1–10 km² is Eq. **2.4.4**.
For areas below 10 km² and durations 12–24 h, apply duration
interpolation at 10 km² first, followed by area interpolation.
Ladson notes that ARR Table 2.4.1 incorrectly refers to the wrong
interpolation equation in some rows. PyTUFLOW uses the correct equations.

Console warnings and per-event CSV flags now cover:

- **AEP >50%**, including **63.2%**: normal PyTUFLOW policy leaves ARF
  at **1.0000 (unity fallback)** rather than calculating an ARF outside
  its published probability range. AEP below 0.05% also defaults to unity.
- **Durations <30 min**: derived ARFs are extrapolations and must be
  checked for plausible values. Do **not** automatically exclude
  flagged DROP events on that basis.
- **Short durations up to 12 h for area >1000 km²**, **areas
  >30,000 km²**, or **durations >168 h**: published generalised
  equation limits are exceeded.
- **ARF = 0**: PyTUFLOW's selected `min_arf=0` floors unphysical
  negative equation values at zero, or a user may have explicitly
  selected zero. Verify zero-rainfall handling and the physical
  plausibility of any DROP classification affected by a zero factor.

Affected factors have an `*` in the ARF column; every CSV record
includes semicolon-separated `arf_cautions` values. This is an
**engineering review flag**, not an automatic rewrite of a factor
or DROP/KEEP result. The wrapper's default common-AEP selection still
omits 63.2% unless explicitly selected.

### Climate-change rainfall

For ARR Data Hub rainfall, `--cc 2050:SSP2 --cc 2090:SSP5`,
or editable `CLIMATE_SCENARIOS = ["2050:SSP2", "2090:SSP5"]`,
uses PyTUFLOW's climate-change engine to assemble both baseline
and climate-adjusted rainfall/loss events. The engine derives
the rainfall from the Data Hub's climate-change factors and
corresponding baseline IFD, not from a manually assumed constant
uplift. Each scenario is printed separately; the matrix is split
by scenario to avoid mixing different climate periods.

**Complete versus main-burst-only storm — two alternative loss conventions:**

- `STORM_MODE = "complete"` **(default)**: explicitly prepend the **preburst rainfall**
  (at its own temporal steps), then analyse the design burst. Apply
  the **full storm initial loss** once across this combined series.
- `STORM_MODE = "main-burst-only"`: analyse only the **main design burst**.
  Use a **burst IL** already reduced for preburst; do not subtract the preburst
  a second time. This is not the default.
- For ARR cells whose recommended burst IL is unavailable or would be
  negative, PyTUFLOW may require the complete-storm method even when
  the user selected `main-burst-only`. The output shows the actual preburst depth
  where supplied; model equivalence is still subject to acceptance.
- **User-specified** `--il 50 --cl 5` overrides the loss parameters
  after ARR event assembly. In `main-burst-only` mode this is interpreted as
  **burst IL**; in `complete` mode it is **full storm IL**. The wrapper
  does not automatically infer or reconcile those interpretations.

### Why CL can leave excess when total rain barely exceeds IL

Consider 30.2 mm of burst rainfall, 0.3926 mm of explicit
preburst, 30 mm storm IL and 5 mm/h CL. The full-storm total is
30.5926 mm, leaving **0.5926 mm** after IL, but whether CL removes
the remainder depends on **the actual TP increment duration** and
when IL is exhausted.

If the crossing occurs in a 6-minute bin, full-bin CL capacity
is only `5 × 6/60 = 0.5 mm`, leaving at least 0.0926 mm. If the
crossing occurs in a 12-minute bin, the full-bin capacity is
`1.0 mm`, which could remove the remainder. The default
**fractional** treatment further reduces CL capacity within
the crossing bin. Thus a positive result is possible despite
a 5 mm/h continuing loss. Use `--tp-details` to see individual
TP results and `--loss-timing full_increment` for a qualified
sensitivity comparison, not as an unverified silent switch to
RORB/TUFLOW equivalence.

### Reporting, TP counts and matrix

The console prints source coordinates when available, catchment area,
ARF method, selected loss timing and climate scenarios, plus the
actual **TP source and regional labels** (for example,
`areal: Rangelands West`). TP regions come from the input
patterns, not inferred from the coordinates.

The detailed results table has **two-line column headings**, coloured
`DROP` (green) and `KEEP` (yellow) decisions, and one horizontal
divider **between successive AEP groups**. The duration-by-AEP status
matrix uses the same decision colours.

Each row shows the point IFD, ARF, adjusted rainfall, loss
parameters, **applied preburst depth (PB mm)** and a separate
**PB ratio**. For ARR mode, the ratio is selected from the
**original Data Hub preburst-ratio layer** using PyTUFLOW's existing
source/interpolation procedure: `RecPreburst` (where available),
or the `Preburst50` fallback when `recommended` is selected.
The adopted ratio is independent of climate-change rainfall magnitude;
the **applied PB depth** uses the scenario's point IFD and therefore
changes with rainfall. This includes cases where a preburst period
is dropped as negligible: a nonzero source ratio can still be shown
while applied PB mm is blank.

For example, a 1.50% selected source ratio with a 100 mm point
storm produces 1.50 mm PB, while a 140 mm climate-adjusted point
storm gives 2.10 mm PB **with the same 1.50% ratio**.
The ratio may be interpolated by PyTUFLOW across the original ARR
duration/AEP table; it is not necessarily a literal exported cell.

Manual **BoM** mode has no ARR source ratio for its optional
explicit PB depth. In that case only, the ratio is derived as
`applied PB depth / point IFD`, and the CSV labels it
`applied_depth_over_point_ifd`. A missing original ARR ratio
remains blank and is **never silently set to zero**.

**Complete-storm IL overrides and omitted preburst:** PyTUFLOW may omit a
preburst period when its original ratio is below 1%. The wrapper interprets
user-supplied `--il` / per-AEP full-storm IL overrides as full-storm IL in
`--storm-mode complete`. If PyTUFLOW omits such a small preburst, the wrapper
subtracts its sourced preburst depth **once** to obtain an equivalent burst
IL before screening. The applied IL is labelled in the report. If the
original ratio or the conversion cannot be established (for example,
preburst rainfall could already exceed the full-storm IL), the affected
event is **UNRESOLVED, never DROP**. The existing
`--storm-mode main-burst-only` override remains a burst IL and is not reduced.

The CSV retains `preburst_mm` and `preburst_ratio_fraction` as
distinct fields and now adds `preburst_ratio_basis`, which says
whether the ratio is `arr_selected_source_ratio`, inferred,
unavailable or not applicable.

The TP counts mean:

- `Zero TP`: the number of available TP patterns proven to have zero
  excess. When the entire storm is at or below IL, the kernel proves
  **all supplied patterns** zero at once. Thus if ten patterns were
  supplied the table correctly shows 10, not 1. If no TPs were
  supplied but the total-depth bound is sufficient, the table prints
  `bound` instead of inventing a count of one evaluated TP.
- `Pos TP`: selected TP patterns producing nonzero excess. These
  events remain `KEEP`; a positive depth does not establish
  material routed discharge.
- `Unknown TP`: missing, malformed or incompatible TP inputs. This
  column is omitted when every displayed combination is resolved.

The ARF column uses a **grey-to-white scale**: lower factors
(stronger rainfall reductions) appear darker grey; a factor of 1.0
is white. Excess-rainfall cells have a distinct rule:
**an exact zero is grey**, while any genuinely positive number stays
white. Positive values below the four-decimal display resolution are
shown as `<0.0001` rather than as a misleading grey `0.0000`.
The CSV retains the unrounded positive excess values.

### Maximum TP total versus maximum single bin

- `Max TP sum (mm)` = the **largest sum of effective-rainfall increments
  across one complete temporal pattern** for the event.
- `Max bin (mm)` = the **largest effective-rainfall increment in any
  single time bin across all selected temporal patterns**.

These values can be from **different** patterns. Example, after losses:

| TP | Excess bin 1 (mm) | Bin 2 | Bin 3 | Total (mm) | Max single bin (mm) |
| --- | ---: | ---: | ---: | ---: | ---: |
| TP01 | 1.0 | 2.0 | 0.0 | **3.0** | 2.0 |
| TP02 | 0.0 | 0.0 | 2.5 | 2.5 | **2.5** |

The event's `Max TP sum` is **3.0 mm** (TP01), but its
`Max bin` is **2.5 mm** (TP02). If all runoff-producing rain
falls within just one bin in a particular TP, its total and max
bin are equal; they do not need to be equal across the ensemble.

Neither measure is a peak flow. The full TP-by-TP diagnostic table
is available with `--tp-details`. Display precision for point IFDs
is source-derived, while adjusted rainfall retains at least two
decimal places. Matrix columns contain only the selected AEPs.


### Materials rainfall losses vs soils ILCL infiltration in TUFLOW

This screening model concerns **rainfall excess before applying rainfall
to the 2D cells**, conceptually matching TUFLOW materials-based IL/CL.
**Exact numerical parity with TUFLOW materials-file rainfall losses is
NOT established.** TUFLOW's documented default rainfall interpolation
is STEPPED: input hyetograph depths become rainfall rates (mm/h) held
constant during each input interval. CL=5 mm/h represents 0.5 mm over
a 6-minute interval, not 5 mm per time step.

The official material-loss documentation does **not specify whether CL
is prorated within the computational timestep in which IL is exhausted
or allowed a full timestep capacity**. The screening tool currently
offers two *non-certified* assumptions:

- `LOSS_TIMING="fractional"` (alternative): prorates CL by the post-IL
  portion of the **temporal pattern bin**, generally giving a more
  conservative KEEP decision when events are near the threshold.
- `LOSS_TIMING="full_increment"` **(wrapper default)**: applies the complete TP-bin CL
  capacity after initial loss is consumed. This mirrors a bounded
  experimental RORBwin 6.52 candidate, **not proven TUFLOW behaviour**.

The TP bin duration need not equal TUFLOW's hydrodynamic timestep.
Before using DROP results to suppress actual TUFLOW runs, perform
a short native comparison with [TUFLOW example EG05_002](https://wiki.tuflow.com/TUFLOW_Example_Models),
comparing the
available RFML (material rainfall losses), RFC (cumulative rainfall)
and RFR (applied rainfall rate) outputs. Vary when IL is exhausted
within a rainfall bin and retain the TUFLOW build/timestep evidence.

It is **not an infiltration model**:
TUFLOW `.tsoilf` `ILCL` applies water to cells first, then
infiltrates into soil depending on water, wetting and soil conditions.
A zero rainfall-excess verdict cannot automatically be used to
prune all TUFLOW soil-infiltration runs.

See the [TUFLOW Classic/HPC 2026.3 rainfall-loss manual](https://docs.tuflow.com/classic-hpc/manual/2026.3/07-Boundaries-and-Initial-Conditions.html)
and [soil versus material definitions](https://docs.tuflow.com/classic-hpc/manual/2026.3/06-2D-Domains.html)
for the authoritative distinctions. In particular, a continuing
loss rate of 5 mm/h does not mean 5 mm is removed **per TP bin**.


### Saved ARR Data Hub inputs and strict offline mode

The wrapper supports **local overrides** for both the Data Hub JSON response
and ARR temporal-pattern increments. The local paths take precedence over
the corresponding live API/download source; they are read, never modified.

- `ARR_RESPONSE_JSON` / `--arr-response`: full downloaded **2026 Data Hub JSON**
  containing the requested `RecIFD`/loss/preburst datasets. Local JSON replaces
  the live Data Hub response; the data are passed to PyTUFLOW's existing
  `ArrApiResponse` reader, not re-parsed using a second ARR implementation.
- `POINT_TP_CSV` / `--point-tp`: saved point temporal-pattern
  `*_Increments.csv` file, which replaces the point-pattern HTTP download.
- `AREAL_TP_CSV` / `--areal-tp`: saved areal temporal-pattern
  `*_Increments.csv` file, which replaces the areal-pattern HTTP download.
  PyTUFLOW's `TemporalPatternSet.from_sources` uses each local CSV in
  preference to the corresponding Data Hub URL.
- `OFFLINE=True` / `--offline`: **strict no-network mode**. For ARR mode,
  a readable response JSON and point CSV are mandatory. If that response
  advertises an areal TP download link but the local areal increments CSV
  is absent, the wrapper can explicitly use point TPs and print a warning
  (default `ALLOW_POINT_TP_FALLBACK=True`). It removes only the areal URL
  **in memory**, keeping the local source JSON unchanged and preventing
  PyTUFLOW from downloading the missing dataset. If fallback is disabled,
  the missing areal file is a hard error.
  A saved JSON may be used without specifying latitude/longitude because
  its contents already identify the data source, but users must verify that
  the saved dataset belongs to the intended catchment. Coordinates, when
  supplied, are not used to replace local source data.

Example (offline, no coordinates required):

```powershell
py -3.14 ryan-scripts/hydrology-python/screen_rainfall_losses.py --source arr --offline --arr-response "D:\ARR\Site_ARR_response.json" --point-tp "D:\ARR\Point_Increments.csv" --areal-tp "D:\ARR\Areal_Increments.csv" --aep-mode common --no-pause
```

In BoM mode, a supplied `--areal-tp` file is honoured **even if
`--point-tp` is omitted and the point temporal patterns must be
downloaded**. PyTUFLOW's `TemporalPatternSet.from_sources` handles the
point and areal sources independently; an explicit local areal CSV is
never replaced by the Data Hub URL when a point download is required.

A **hybrid** run using `--arr-response` but without `--offline` is also
supported: the JSON is local, but any unspecified temporal-pattern CSV may
still be downloaded from the Data Hub. Similarly, local point/areal CSVs
override the pattern downloads even with a live API JSON response.
`--no-offline` overrides an edited `OFFLINE=True` default.

For BoM CSV screening, `--offline --ifd <file> --point-tp <file>` uses
the existing local BoM reader and PyTUFLOW pattern loader. If patterns
are intentionally unavailable, use `--no-fetch-tps`: only events whose
entire rainfall depth is proven below IL are excluded, and the remaining
events are reported as unresolved. A standalone Data Hub JSON is not a
substitute for the independent point/areal increments CSVs.

**Supported local ARR input format:** the complete Data Hub **JSON**
download, not the Data Hub human-readable text report or an individual
IFD CSV. Keep the matching temporal-pattern CSVs and record their provenance,
baseline year, catchment area, and date. A saved JSON from another site
or a mismatched TP set is not automatically reconciled.


### Screen the original BoM CSV depths

```powershell
py -3.14 ryan-scripts/hydrology-python/screen_rainfall_losses.py --source bom --storm-mode main-burst-only --ifd "depths_hd4_temp_all_design.csv" --il 30 --cl 5 --aep 20% 10% 1% --view zero --no-pause
```

BoM IFD CSV does not itself include the complete preburst hyetograph.
Consequently, BoM mode **requires `--storm-mode main-burst-only`**;
the default `complete` is reserved for ARR Data Hub event assembly.

This mode uses **exact original BoM rainfall depths, durations, and AEP
labels** through the `python-runoff-routing` reader and uses PyTUFLOW's
point/areal temporal-pattern loader. If `FETCH_TPS_FOR_BOM=True` (default),
it requests the 2026 pattern downloads for the BoM CSV's *requested*
coordinates. To enforce no network access, use --offline with `--point-tp` (and optionally
`--areal-tp`) or `--no-fetch-tps`; without patterns, rainfall above IL
is unresolved rather than automatically excluded. The BoM reader is a
separate optional development package and must be installed for this mode.
There is no copy-pasted parser in `ryan-tools`.

When BoM IFD rainfall is combined with a catchment area >1 km²,
regional ARF coefficients must be available from a matching Data Hub JSON
(or obtained live) unless an explicit ARF override is supplied.
In **strict offline** BoM mode, missing coefficients fail before a request
rather than downloading silently. For **area ≥75 km²**, if the matching areal TP set cannot be obtained,
point temporal patterns are permitted by default, but the wrapper prints
a prominent warning for the affected durations. Use
`--no-point-tp-fallback` to disable this substitution.

A BoM burst-only IL supplied by `--il` is assumed **already adjusted for
preburst**. To provide a storm IL plus a known preburst depth, use
`--bom-preburst-depth MM`; subtraction is applied once, using
`reduced_burst_initial_loss`, and a depth greater than storm IL is rejected.
This is a *user-selected simple reduction*, not the Data Hub recommended
burst-loss methodology. Complete-storm preburst temporal patterns are
supported via `--source arr --storm-mode complete`, not by inserting an
arbitrary preburst into an unmatched BoM export.

### Missing TP durations and point fallback

ARR Data Hub RecIFD and BoM IFD can contain short durations that are
**not available in the matching temporal-pattern set**. For example,
a source IFD may contain a **5-minute** duration while the selected
Rangelands West point data begin at a longer duration. The wrapper now
preflights **all selected AEP bands per duration** before event assembly:

- A duration with no suitable TP in any required AEP band is **omitted
  from the event table and duration-by-AEP matrix**. One consolidated
  WARNING lists all such durations **once at the top** of the output.
  These events have **not** been evaluated, and are **not DROP**. If no
  durations remain, the wrapper exits with a single clear error.
- PyTUFLOW's own `TemporalPatternSet.patterns` prefers an applicable
  areal TP and **falls back to point TPs** if the areal area bucket or
  duration is unavailable. For catchment areas ≥75 km² the wrapper
  reports actual substitutions in one warning at the top. Point TPs
  selected intentionally **below the shortest available areal TP
  duration** are normal ARR behaviour, not fallback warnings.
- For an offline ARR response that still advertises an areal TP URL but
  lacks a local areal increments CSV, the wrapper suppresses that URL
  **in memory**, uses the available local point CSV and warns. No
  network request is made; the original JSON is preserved.
- For a failed **areal-only ZIP download** in online ARR mode, the
  wrapper can retry with point TPs. It does not suppress unrelated API,
  point-TP or data-parsing errors.
- Set `ALLOW_POINT_TP_FALLBACK = False` or use
  `--no-point-tp-fallback` to disable fallback. A duration without its
  required areal TP will then be omitted rather than silently retained
  as a point event. Source rainfall depths and ARF policy are unchanged
  by the TP fallback.

Consolidated warnings are shown before the results table, **not once
per AEP**. This avoids hundreds of repeated missing-duration rows while
retaining an auditable record of what was not screened.


### AEP and duration selection

- `--aep-mode common` (the default) includes standard design percentages
  from **50% AEP and rarer** (plus listed 1-in-200/500 labels). **63.2%
  is excluded by default**, but can still be explicitly requested with
  `--aep 63.2%` or `--aep-mode all`. Edit `COMMON_AEPS` in the wrapper to change the explicit list.
- `--aep-mode all` includes all labels provided by the source.
- `--aep 20% 10% 1%` or `--aep "1 in 200"` overrides either mode. The
  requested names must exactly match source labels; no implicit ARI/EY
  conversion.
- `--duration 60 120 360 1440` selects minutes. An empty duration
  selection uses every BoM CSV row or the wrapper's editable
  source RecIFD duration rows for the Data Hub. Set `ARR_DURATIONS_MIN`
  only if an explicit ARR-specific default subset is intended.
- `--view all` shows every combination; `--view zero` shows only
  `DROP` combinations and `--view keep` shows the remaining items.

Losses can vary by AEP and duration using the editable
`LOSS_OVERRIDES_BY_AEP` dictionary or `--losses-json filename.json`:

```json
{
  "20%": {"il_mm": 30, "cl_mm_per_hour": 5},
  "1%": {
    "60": {"il_mm": 25, "runoff_coefficient": 0.3},
    "1440": {"il_mm": 20, "cl_mm_per_hour": 4}
  }
}
```

If no user losses are supplied in ARR mode, PyTUFLOW's selected Data Hub
losses are retained. In BoM mode, missing loss entries produce **unresolved**
statuses unless a pointwise total-depth upper bound proves zero. Explicit
per-AEP overrides are interpreted as **storm IL** in complete-storm mode
and **burst IL** in `main-burst-only` mode; an override is not itself adjusted
by the Data Hub preburst calculator.

### Console result columns

The detailed Rich console table draws a **horizontal rule between AEP
groups**, and when the climate scenario group changes. It does not insert
a rule after every event or a blank record into exported CSV data.

The two rainfall-excess depth columns display **exact numerical zero in
grey**. A positive depth that rounds to the displayed zero (for example,
0.00004 mm shown to four decimals) is shown as `<0.0001` in the normal
text colour, including the optional per-TP detail table. Colour is based on
the **unrounded numerical value**, not the display string. Missing values
continue to display a dash.

Each AEP/duration row shows rainfall depth, effective losses/preburst
information, zero/positive/unresolved selected TP counts, and one of
`DROP`, `KEEP`, or `UNRESOLVED`. The summary includes total zero,
positive and unresolved combinations and a duration-by-AEP matrix. A zero verdict is conditional on
the selected TPs and stated loss convention; it does **not** indicate
an independently verified RORB/TUFLOW no-flow result.


## Inputs and parser reuse

* `screen_bom_ifd_csv(path, ...)` delegates the official BoM "IFD Design" or
  "All Design Rainfall" CSV (including 2026 exports) to the **existing**
  `runoff_routing.bom_ifd.read_bom_ifd_csv` reader. This in-development package
  is **optional** and is not vendored or made a required `ryan-tools` dependency.
  If it is not installed, pre-read the table and call `screen_bom_ifd(...)`.
* `screen_bom_ifd(imported_ifd, ...)` accepts any existing BoM reader object
  exposing `.probabilities`, `.durations_min`, and `.depth_mm_at()`.
  Its `losses_by_aep` is keyed by the **exact** BoM probability label (e.g.
  `"20%"`, `"0.5EY"` or `"1 in 200"`). Each AEP entry may be a `LossSpec`
  or a map of integer duration (minutes) to `LossSpec`. An absent loss is
  `UNRESOLVED`, not zero. No AEP/ARI/EY conversion or interpolation occurs.
* `temporal_patterns` accepts the existing PyTUFLOW
  `pytuflow.arr.temporal_patterns.TemporalPatternSet` instance. Pass it to
  `screen_bom_ifd`/`screen_bom_ifd_csv` to screen individual TP events. Without
  it, only the all-rainfall-below-IL bound or zero coefficient can exclude an
  event; otherwise it is unresolved.
* `screen_arr_datahub(config, response)` uses the existing
  `pytuflow.arr.engine.ArrEngine` on **ARR 2026** API response data, with
  PyTUFLOW's own IFD baseline, ARF, AEP, TP, climate scenario, loss and preburst
  choices. A response may be a raw JSON dict or `ArrApiResponse`.
  `screen_arr_events(engine.run())` also takes already assembled event objects.
  The current public PyTUFLOW implementation may not yet be present in the
  installed PyPI version: require its ARR 2026-capable version before using
  this route. A Data Hub query/temporal-pattern download is not performed by
  `screen_arr_events` and is governed by PyTUFLOW when using `ArrEngine`.

## Loss accounting

Choose exactly one `LossSpec` form:

* `LossSpec(initial_loss_mm=30, continuing_loss_mm_per_hour=5)` uses sequential
  initial loss followed by continuing loss. Each temporal pattern increment is
  assumed uniformly distributed across its timestep. The library's **fractional** API default
  uses the post-IL fraction of the IL-crossing increment for CL. The **wrapper's
  `LOSS_TIMING="full_increment"` default** uses the full TP-bin CL capacity
  when IL has been exhausted, as a **separately qualified experimental
  RORBwin 6.52 candidate**, not an established TUFLOW method.
* `LossSpec(initial_loss_mm=30, runoff_coefficient=0.25)` applies the constant
  coefficient to rainfall remaining after IL. The coefficient must be in [0,1].

Both modes calculate rainfall-excess depth and peak **excess rainfall rate**.
Neither computes routed flow or infiltration parameters such as soil storage.

ARR preburst must not be counted twice:

* For a standard **burst-only** event, supply a **burst IL** already adjusted for
  the selected preburst treatment. If you only know the storm IL and a suitable
  preburst depth, `reduced_burst_initial_loss(storm_il, preburst_depth)` applies
  a simple subtraction and rejects preburst depth greater than storm IL.
  This is a user-selected approximation, **not** a replacement for ARR's
  recommended burst-loss calculation.
* For a **complete-storm** event, supply a `Preburst` temporal profile before
  the design burst, together with the **full storm IL**. PyTUFLOW's assembled
  `EventResult` already distinguishes these two cases; do not reduce its IL
  again. A preburst depth greater than storm IL can generate rainfall excess
  during the preburst itself.

```python
from pathlib import Path

from ryan_library.functions.hydrology.rainfall_loss_screening import (
    LossSpec,
    screen_bom_ifd_csv,
    summarize_events,
)

# `tps` is an already loaded PyTUFLOW TemporalPatternSet, using the required
# point/areal patterns for the catchment and matching the chosen ARR guidance.
screens = screen_bom_ifd_csv(
    Path("depths_hd4_temp_all_design.csv"),
    temporal_patterns=tps,
    losses_by_aep={
        "20%": LossSpec(initial_loss_mm=30, continuing_loss_mm_per_hour=5),
        "1%": {60: LossSpec(initial_loss_mm=25, runoff_coefficient=0.3)},
    },
)
for event in summarize_events(screens):
    print(event.aep, event.duration_min, event.status)
```

The Python API example assumes `tps` has been configured; the wrapper above can fetch or load TPs instead.

## Verdicts and limitations

`screen_event` reports one record per **provided** TP unless total storm depth
is at or below IL or coefficient is zero, where it emits one pattern-independent
certificate. When a TP is missing, malformed or incompatible with the duration,
the event is `UNRESOLVED`. `summarize_events` is `ZERO` only if **all considered
patterns** are zero and none unresolved. Unexamined TP regions, missing AEP
bands, and additional catchment zones cannot be inferred from a supplied set.

Synthetic tests validate mass accounting and boundary behaviour, including the
23/31/37 mm scenarios. **No RORBwin 6.52, rorb-cmd, TUFLOW, native model,
authentic ARR 2026 Data Hub response, or installed-PyTUFLOW end-to-end
numerical acceptance has been performed.** Results should be used for design
run pruning only after loss-convention parity and representative event coverage
have been independently verified against the target model.
