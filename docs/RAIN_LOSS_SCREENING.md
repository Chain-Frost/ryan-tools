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
and uses its recommended, preburst-adjusted **burst initial losses**.
It is **not** the same as using the attached BoM CSV depths; 2030 RecIFD
may differ from a 2016-methodology BoM export. To use the historical ARR
1990 baseline, set `IFD_BASELINE_YEAR=1990` or `--baseline-year 1990`.
A catchment area of zero means **point rainfall only**; supply an actual
`--catchment-area KM2` to use areal patterns/ARF as applicable. This is
not an assumed physical project catchment area.

`--storm-mode complete` instead requests explicit preburst rainfall
before the design burst, using the full storm initial loss. The
`--preburst-percentile recommended` default uses the Data Hub's preferred
preburst ratio/shape subject to its jurisdictional availability. The
selected initial-loss mode must match the intended hydraulic model.
PyTUFLOW may automatically switch particular burst-loss cells to complete
storm when a fixed burst IL is unavailable.

### Screen the original BoM CSV depths

```powershell
py -3.14 ryan-scripts/hydrology-python/screen_rainfall_losses.py --source bom --ifd "depths_hd4_temp_all_design.csv" --il 30 --cl 5 --aep 20% 10% 1% --view zero --no-pause
```

This mode uses **exact original BoM rainfall depths, durations, and AEP
labels** through the `python-runoff-routing` reader and uses PyTUFLOW's
point/areal temporal-pattern loader. If `FETCH_TPS_FOR_BOM=True` (default),
it requests the 2026 pattern downloads for the BoM CSV's *requested*
coordinates. For offline use, provide `--point-tp` (and optionally
`--areal-tp`) or `--no-fetch-tps`; without patterns, rainfall above IL
is unresolved rather than automatically excluded. The BoM reader is a
separate optional development package and must be installed for this mode.
There is no copy-pasted parser in `ryan-tools`.

A BoM burst-only IL supplied by `--il` is assumed **already adjusted for
preburst**. To provide a storm IL plus a known preburst depth, use
`--bom-preburst-depth MM`; subtraction is applied once, using
`reduced_burst_initial_loss`, and a depth greater than storm IL is rejected.
This is a *user-selected simple reduction*, not the Data Hub recommended
burst-loss methodology. Complete-storm preburst temporal patterns are
supported via `--source arr --storm-mode complete`, not by inserting an
arbitrary preburst into an unmatched BoM export.

### AEP and duration selection

- `--aep-mode common` (the default) includes familiar percentages and
  1-in-200/500 events but omits frequent EY classes and other unconventional
  columns. Edit `COMMON_AEPS` in the wrapper to change the explicit list.
- `--aep-mode all` includes all labels provided by the source.
- `--aep 20% 10% 1%` or `--aep "1 in 200"` overrides either mode. The
  requested names must exactly match source labels; no implicit ARI/EY
  conversion.
- `--duration 60 120 360 1440` selects minutes. An empty duration
  selection uses every BoM CSV row or the wrapper's editable
  `ARR_DURATIONS_MIN` for Data Hub.
- `--view all` shows the complete table; `--view zero` shows only
  wiped-out combinations and `--view keep` shows the rest.

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
and **burst IL** in burst-only mode; an override is not itself adjusted
by the Data Hub preburst calculator.

### Console result columns

Each AEP/duration row shows rainfall depth, effective losses/preburst
information, zero/positive/unresolved selected TP counts, and one of
`WIPE OUT`, `KEEP`, or `UNRESOLVED`. The summary includes total zero,
positive and unresolved combinations. A zero verdict is conditional on
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
  assumed uniformly distributed across its timestep. In the increment that
  exhausts IL, only the remaining fraction of that increment's time is
  available for CL. CL does not act before IL is exhausted in this convention.
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
