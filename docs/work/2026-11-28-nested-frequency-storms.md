# Nested frequency storms (experimental)

| Field | Value |
| --- | --- |
| Status | Active — draft implementation, independent qualification open |
| Owner | Unassigned |
| Created | 2026-11-28 |
| Updated | 2026-11-28 |
| Next review | 2026-12-12 |
| Baseline | main 5eda4f06cafbbdc5f528bb108909438302a13ce5 |
| Branch | feat/nested-frequency-storms |

## Outcome and scope

Implement the user-selected [v2 workbook](https://github.com/Chain-Frost/excel-resources/blob/6cb86e7c4bece209099891ab7004d074bd02598b/workbooks/TUFLOW/TUFLOW%20Nested%20Frequency%20Storms%20v2.xlsx) as reusable Python hydrology code and an offline experimental CLI. No RORB storm export or model execution. The attached workbook has SHA-256 3b7fd4066d209dca109db239d27f78efda72cff77e070d087b820617aad59959 (39,427,881 bytes). Credit the workbook's original author, Krey Price / Surface Water Solutions. Do not commit the original workbook or cached site data.

The nested algorithm follows the workbook's piecewise-linear depth-duration calculation and FL/ML/BL order, not arbitrary intensity sorting. The ML one-minute conventions for 360/1440/4320 minutes rotate the sequence by one. The known Calculations!B1837 band transition at 30 h is not reproduced: conserve the intended design depth. Record discrepancies against the workbook rather than regenerate golden values. Other timesteps are an explicit extension, not Excel parity. Exact target anchor and source coverage at/below timestep are mandatory; interpolation is linear between adjusted depth anchors, with zero origin. Missing 1-minute ARR IFD data must not be silently extrapolated: choose a timestep supported by the input.

## API and adjustments

Use `ryan_library.functions.hydrology.nested_storms`. `RainfallCurve` is one site/source, exact AEP label, scenario and increasing `DepthAnchor` records. Each anchor stores point depth, ARF and rainfall factor separately. Compute scenario point depth then apply ARF at EVERY anchor, not just the target duration. An explicit `arf_override=1.0` disables reduction across all anchors. Do not treat adjusted areal rainfall as point rainfall. A falling adjusted depth curve is invalid; rolling maximum checks expose any departures from the input nested depths (not all curves are concave). Report aligned durations only.

- `curve_from_bom` accepts the existing parsed BoM IFD object; the maintained reader lives in `runoff_routing.bom_ifd` and is already used by rainfall-loss screening. No RORB conversion is included. Factors may be provided by duration; no implicit missing-factor fallback.
- `curves_from_arr` consumes the public PyTUFLOW ARR `EventResult` fields, groups by AEP/scenario and checks point x ARF against areal depth. Scenario point rainfall is already climate adjusted, so leave the additional rainfall factor at unity. It requires a single site's events and adequate source IFD anchor coverage. Do not infer absent short-duration anchors from available ARR temporal patterns. A direct ARR depth-only source API is a follow-up if ArrEngine cannot supply the necessary IFD durations without TP selection; avoid copying its private methods.
- Reuse the existing [rainfall-loss screening](../RAIN_LOSS_SCREENING.md) IL/CL and IL/coefficient calculation. Each requested target event receives its own `EventLosses`, including separate initial/continuing loss multipliers and crossing-bin convention. Apply loss multipliers before preburst reduction. No default site losses are invented.
- `burst`: supply already adjusted burst IL; do not subtract preburst again. `burst_losses_from_arr` reuses a burst-only upstream event's IL/CL without reapplying its factors.
- `storm_reduce`: supply storm IL plus an explicit scenario preburst depth, or an ARR-style preburst ratio multiplied by the adjusted POINT depth at the target duration. The adopted burst IL is adjusted storm IL minus PB. If PB exceeds storm IL, reject and require complete storm; no clipping.
- `complete`: supply a deliberate preburst hyetograph in mm at the chosen nested timestep, with full storm IL and no additional PB subtraction. Prefix is already scenario-adjusted. An upstream per-TP preburst pattern is not automatically assigned to the synthetic nested storm.
- For climate change, distinguish already adjusted upstream events from baseline user loss values requiring multipliers. Scaling an already reduced burst IL and recomputing from adjusted storm IL minus PB are DIFFERENT conventions. No second adjustment to upstream ARR outputs.
- For gross rainfall export, apply the corresponding loss basis in the hydraulic model. For excess rainfall, do not apply the same losses again. Complete storm includes preburst; reduced-burst output omits it by design. CL units mm/h.

The original workbook also applies proportional loss before IL/CL. The shared IL + coefficient mode applies the coefficient AFTER IL and is not equivalent. Full spreadsheet proportional-plus-ILCL parity, including nonzero losses and zero-rainfall boundary cases, remains open. No implicit claim of ARR AEP-equivalent runoff or native TUFLOW loss equivalence.

## Experimental CLI

The repository-only script `ryan-scripts/hydrology-python/generate_nested_storms.py` accepts normalized JSON with explicit per-event options. It does not fetch data, select a preburst percentile, infer ARF or independently derive SSP factors. Existing PyTUFLOW/BoM workflows may produce the normalized inputs; a source-specific CLI adapter should follow the existing public APIs. Example synthetic config:

```json
[
  {
    "name": "1pct_60min_baseline",
    "curve": {
      "aep": "1%",
      "source": "synthetic",
      "anchors": [
        {"duration_min": 1, "point_depth_mm": 4, "arf": 0.95},
        {"duration_min": 60, "point_depth_mm": 60, "arf": 0.85}
      ]
    },
    "duration_min": 60,
    "options": {
      "loading": "ML",
      "arf_override": 1.0,
      "losses": {
        "spec": {"initial_loss_mm": 30, "continuing_loss_mm_per_hour": 3},
        "mode": "storm_reduce",
        "preburst_ratio": 0.1,
        "initial_loss_factor": 1.0,
        "continuing_loss_factor": 1.0
      }
    }
  }
]
```

Preview: `python ryan-scripts/hydrology-python/generate_nested_storms.py --config events.json`.

Write a NEW file: `python ryan-scripts/hydrology-python/generate_nested_storms.py --config events.json --output gross.csv --kind gross --write`. Use `--kind excess` only when losses are provided for every event. Separate gross and excess outputs. CSV uses hours, mm per interval, leading/trailing zero, blanks after shorter columns. Native TUFLOW ingestion, BC database/TEF and event-loss control exports are still unqualified/unimplemented; do not assume correct production model timing. CLI is experimental and not yet an installed/MCP-discoverable supported wrapper.

## Next action and completion criteria

1. In an isolated Windows Python 3.14 checkout, review exact head; run focused tests including existing screening tests, Ruff format/lint, strict Pyright, compile/CLI preview and no-clobber/failure tests, package build and installed-wheel checks. Ordinary hosted PR CI is Python 3.11; do not change CI policy.
2. Independently reproduce all 324 workbook storm combinations and per-interval comparisons against the authenticated source, with separate disposition of the 30 h defect and ML variations. Validate nonzero proportional/IL/CL conventions without silently preserving spreadsheet artefacts. Record test environment and exact hashes, no site data in public fixtures.
3. Obtain real BoM and ARR JSON/RecIFD/ARF/preburst/loss sources for a permission-cleared test site. Use existing PyTUFLOW public APIs and exact matching durations/AEPs; provide source-specific CLI wiring without new scientific formula duplication. Preserve ARF cautions (notably extrapolation below 30 min and published area/frequency limits), distinguish calculated vs overridden unity. Test baseline and SSP rainfall and loss changes, event-specific overrides, NSW probability-neutral losses, missing PB and overflow, explicit prefix and no double reduction.
4. Check native TUFLOW RF ingestion and stepped time convention with a minimal model; compare processed rainfall and independently verify correct loss application or loss disabling for excess. Produce BC database/TEF and event loss control outputs if required. Promote and register wrapper/MCP only after stable acceptance.
5. Do not merge or release without owner approval.

## Validation and delivery

Container access to the original workbook/prototype was confirmed and its SHA-256 checked. Direct git clone in the container failed due to DNS/network isolation. The connector permits source inspection and publishing a coherent commit, not executing Python or installing the repository. Ruff, Pyright, exact-head Python tests, package/installed-wheel checks, ARR-capable PyTUFLOW installation and native TUFLOW validation were not available in this session. Synthetic tests are included but require execution on the exact pushed head. No verified claims of passed tests for this new implementation. The previous prototype comparison is historical only.

Related existing work [#317](https://github.com/Chain-Frost/ryan-tools/issues/317), [#319](https://github.com/Chain-Frost/ryan-tools/issues/319), [#322](https://github.com/Chain-Frost/ryan-tools/issues/322). The rainfall screening work record's review date is overdue; reconcile independently without starting unrelated tasks.
