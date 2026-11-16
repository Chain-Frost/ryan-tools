# Rainfall-loss screening (experimental)

The `ryan_library.functions.hydrology.rainfall_loss_screening` functions screen
**rainfall excess**, not routed peak flow. They never delete simulations or
silently discard rainfall: a result of `ZERO` is safe to exclude only for the
specified loss convention, event, catchment, and selected temporal patterns.
A result of `UNRESOLVED` must be retained. `POSITIVE` also remains in the run
set, even if its runoff is small.

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

This snippet assumes `tps` has been configured; it will not run without that
variable and the optional BoM reader package. It is an integration example,
not a self-contained download tool.

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
