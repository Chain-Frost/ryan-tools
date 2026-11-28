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

Implement the user-selected [v2 workbook](https://github.com/Chain-Frost/excel-resources/blob/6cb86e7c4bece209099891ab7004d074bd02598b/workbooks/TUFLOW/TUFLOW%20Nested%20Frequency%20Storms%20v2.xlsx) as reusable Python hydrology code and an offline experimental CLI. No RORB storm export or model execution. The attached workbook has SHA-256 3b7fd4066d209dca109db239d27f78efda72cff77e070d087b820617aad59959 (39,427,881 bytes). Credit the workbook's original author, Krey Price / Surface Water Solutions. Instructions!A7/D7 links directly to [the original frequency storm calculator](https://surfacewater.biz/frequency/), which now points to [HydroSchool](https://www.hydroschool.org/frequency/). Instructions!A9 credits its creator. [Australian Water School](https://awschool.com.au/) is a possible distribution channel recalled by the owner, not verified for this v2 adaptation. The original public page links V1.0 and describes a conservative sensitivity/screening storm, not a probability-equivalent ARR ensemble. Do not commit the original workbook or cached site data.

The nested algorithm follows the workbook's piecewise-linear depth-duration calculation and FL/ML/BL order, not arbitrary intensity sorting. The ML one-minute conventions for 360/1440/4320 minutes rotate the sequence by one. The known Calculations!B1837 band transition at 30 h is not reproduced: conserve the intended design depth. Record discrepancies against the workbook rather than regenerate golden values. Other timesteps are an explicit extension, not Excel parity. Exact target anchor and source coverage at/below timestep are mandatory; interpolation is linear between adjusted depth anchors, with zero origin. Missing 1-minute ARR IFD data must not be silently extrapolated: choose a timestep supported by the input.

## API and adjustments

Use `ryan_library.functions.hydrology.nested_storms`. `RainfallCurve` is one site/source, exact AEP label, scenario and increasing `DepthAnchor` records. Each anchor stores point depth, ARF and rainfall factor separately. Compute scenario point depth then apply ARF at EVERY anchor, not just the target duration. An explicit `arf_override=1.0` disables reduction across all anchors. Do not treat adjusted areal rainfall as point rainfall. A falling adjusted depth curve is invalid; rolling maximum checks expose any departures from the input nested depths (not all curves are concave). Report aligned durations only.

- `curve_from_bom` accepts the existing parsed BoM IFD object; the maintained reader lives in `runoff_routing.bom_ifd` and is already used by rainfall-loss screening. No RORB conversion is included. Factors may be provided by duration; no implicit missing-factor fallback.
- `curves_from_arr` consumes the public PyTUFLOW ARR `EventResult` fields, groups by AEP/scenario and checks point x ARF against areal depth. Scenario point rainfall is already climate adjusted, so leave the additional rainfall factor at unity. It requires a single site's events and adequate source IFD anchor coverage. Do not infer absent short-duration anchors from available ARR temporal patterns. A saved-response depth-only adapter now supplies exact IFD rows independently of TP selection; see the follow-up below.
- Reuse the existing [rainfall-loss screening](../RAIN_LOSS_SCREENING.md) IL/CL and IL/coefficient calculation. Each requested target event receives its own `EventLosses`, including separate initial/continuing loss multipliers and crossing-bin convention. Apply loss multipliers before preburst reduction. No default site losses are invented.
- `burst`: supply already adjusted burst IL; do not subtract preburst again. `burst_losses_from_arr` reuses a burst-only upstream event's IL/CL without reapplying its factors.
- `storm_reduce`: supply storm IL plus an explicit scenario preburst depth, or an ARR-style preburst ratio multiplied by the adjusted POINT depth at the target duration. The adopted burst IL is adjusted storm IL minus PB. If PB exceeds storm IL, reject and require complete storm; no clipping.
- `complete`: supply a deliberate preburst hyetograph in mm at the chosen nested timestep, with full storm IL and no additional PB subtraction. Prefix is already scenario-adjusted. An upstream per-TP preburst pattern is not automatically assigned to the synthetic nested storm.
- For climate change, distinguish already adjusted upstream events from baseline user loss values requiring multipliers. Scaling an already reduced burst IL and recomputing from adjusted storm IL minus PB are DIFFERENT conventions. No second adjustment to upstream ARR outputs.
- For gross rainfall export, apply the corresponding loss basis in the hydraulic model. For excess rainfall, do not apply the same losses again. Complete storm includes preburst; reduced-burst output omits it by design. CL units mm/h.

The original workbook applies proportional loss before IL/CL (Enter Losses!B5:B6 and 1-hr FL!C5, AQ5, BK5). The optional proportional_loss_fraction now scales rainfall before the shared loss kernel; gross remains unmodified. For storm_reduce, subtract PB after proportional loss, consistently with complete-storm accounting. The shared IL + coefficient mode applies the coefficient AFTER IL and is not equivalent; combining it with nonzero proportional loss is rejected. The workbook subtracts full CL capacity per minute; select full_increment to reproduce that convention. Its IL formula references the previous post-IL cell (including a header at the first interval) rather than persistently tracking remaining IL; this can mis-handle first-bin exhaustion or a dry gap. The shared kernel deliberately tracks remaining IL instead. Synthetic tests cover nonzero proportional loss, PB reduction and dry gaps. Cached workbook loss settings are all zero; nonzero formula comparison/acceptance remains outstanding. No implicit claim of ARR AEP-equivalent runoff or native TUFLOW loss equivalence.

## Experimental CLI

The repository-only script `ryan-scripts/hydrology-python/generate_nested_storms.py` accepts normalized JSON with explicit per-event options. It reads either normalized rainfall curves or saved BoM/ARR inputs, using the existing readers and public ARF and climate factor interfaces. Saved inputs remain the default; point retrieval requires explicit network opt-in. See the source settings below. Example synthetic config:

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

Write a NEW file: `python ryan-scripts/hydrology-python/generate_nested_storms.py --config events.json --output gross.csv --kind gross --write`. Use `--kind excess` only when losses are provided for every event. Separate gross and excess outputs. CSV uses hours, mm per interval, leading/trailing zero, blanks after shorter columns. The candidate bundle exporter also writes BC database, TEF and event loss variable controls; native TUFLOW ingestion and loss timing remain unqualified. CLI is experimental and not yet an installed/MCP-discoverable supported wrapper.

## Next action and completion criteria

1. Continue implementation and portable validation through GitHub and the available Python container. Review exact-head hosted Python 3.11 CI and address preflight, Ruff, strict Pyright and test failures without changing CI policy. Reserve Windows Python 3.14, installed-wheel or native model checks for a local agent only where they cannot be run here.
2. Independently reproduce all 324 workbook storm combinations and per-interval comparisons against the authenticated source, with separate disposition of the 30 h defect and ML variations. Validate nonzero proportional/IL/CL conventions without silently preserving spreadsheet artefacts. Record test environment and exact hashes, no site data in public fixtures.
3. Obtain real BoM and ARR JSON/RecIFD/ARF/preburst/loss sources for a permission-cleared test site. Use existing PyTUFLOW public APIs and exact matching durations/AEPs; provide source-specific CLI wiring without new scientific formula duplication. Preserve ARF cautions (notably extrapolation below 30 min and published area/frequency limits), distinguish calculated vs overridden unity. Test baseline and SSP rainfall and loss changes, event-specific overrides, NSW probability-neutral losses, missing PB and overflow, explicit prefix and no double reduction.
4. Check native TUFLOW RF ingestion and stepped time convention with a minimal model; compare processed rainfall and independently verify correct loss application or loss disabling for excess. Verify the generated BC database/TEF and event loss variable controls. Promote and register wrapper/MCP only after stable acceptance.
5. Do not merge or release without owner approval.

## Validation and delivery

The earlier pushed head 853189d passed all seven hosted CI jobs (run 46142159802), including 3493 Python 3.11 tests (16 skipped, 8 deselected), 10 additional focused checks, 64 copied-wrapper tests, strict Pyright on Windows Python 3.14, package/installed-wheel smoke checks, and Ruff. The accepted PyTUFLOW 1.3.1 wheel was verified by CI. The independent gross workbook algorithm comparison is recorded below. Current changes require fresh CI. Direct container Git clone/raw retrieval remains blocked by DNS. Native TUFLOW acceptance and Windows Python 3.14 focused numerical acceptance remain open.

Related existing work [#317](https://github.com/Chain-Frost/ryan-tools/issues/317), [#319](https://github.com/Chain-Frost/ryan-tools/issues/319), [#322](https://github.com/Chain-Frost/ryan-tools/issues/322). The rainfall screening work record's review date is overdue; reconcile independently without starting unrelated tasks.

## 28 November 2026 — follow-up

The original uploaded workbook is still accessible in the Python container; SHA-256 rechecked. Instructions!A9 confirms Krey Price attribution. Direct Git clone remains unavailable (DNS resolution failure), but GitHub read/write and CI log inspection work. First CI run 46139540185 failed at lifecycle inventory preflight, before Ruff or Python tests; corrected the inventory/index counts and recorded the new experimental script. Reinspected actual worksheet loss formulas and added proportional-before-ILCL support and regression tests. These new tests have not yet executed. Keep substantive code, ARR integration and export development on this PR; request local assistance only for genuinely inaccessible native/Windows validation. Do not merge or release.

## 28 November 2026 — source adapters, bundle export and workbook evidence

- CI run 46140098703 reached Ruff after the inventory fix. Address its RUF021, RUF007 and C901 findings by parenthesising the loss-mode predicates, using pairwise, and separating interpolation. Exact-head CI remains required.
- Independently reran the duration-edge interpolation and workbook loading-order algorithm in Python 3.13.5 against the SHA-256-matched original workbook using XML cached values. Compared all 324 storms / 382,320 gross depth ordinates (one gross series, not three copies of zero-loss equivalents), and all loading-order indices. At absolute tolerance 1e-10 mm, 382,266 match; 54 differences confined to original interval 1800 of each 72-h FL/ML/BL storm across 18 probabilities. Maximum absolute difference 0.03333333333333081 mm; no order differences; exact target totals with fsum. This is a reproduced numerical comparison of the algorithm, not an executed full PR checkout or nonzero-loss Excel recalculation. Original workbook/site data are not committed.
- Source APIs: nested_storm_sources.curve_from_arr_response, curve_from_bom_file, arr_preburst_ratio, apply_arr_loss_factors. Exact AEP percent columns and duration rows only; no synthetic short-duration IFDs. Read public RecIFD and CC-factor interfaces (historical 1990 IFD for an SSP scenario; default 2030 for current). Log-duration interpolation of public CC factors matches the inspected ArrEngine; existing PyTUFLOW ARF function performs ARF calculations. Unity override bypasses ARF calculation. PB ratio selects exact RecPreburst or Preburst50 fallback or explicit percentile. Missing rows are refused, not interpolated by this adapter. Baseline IL/CL are explicitly supplied per event, optionally scaled using public Data Hub loss factors. Automatic jurisdictional loss lookup, live retrieval, complete-storm PB-pattern assembly and NSW probability-neutral loss source acceptance remain outstanding.
- Source settings in JSON may replace each event's "curve" with a "source" object. For example:
  - ARR: {"kind":"arr","arr_response":"response.json","aep":"1%","area_km2":350,"arf_override":1,"climate":[2050,"SSP2"]}.
  - BoM: {"kind":"bom","path":"ifd.csv","aep":"1% AEP","arf_override":1}; use "arr_response" for calculated areal factors, and optional "rainfall_factors" mapping all anchor minutes to explicitly supplied factors.
  - In "options.losses" use "mode":"storm_reduce", "spec":{"initial_loss_mm":30,"continuing_loss_mm_per_hour":3}, "arr_preburst_percentile":"recommended", "arr_climate_loss_factors":true with an ARR response and SSP scenario. The supplied spec must be baseline storm loss in that case. For preadjusted event losses use explicit burst/complete mode, with no second climate adjustment. Use a timestep supported by the available IFD rows. No network fallback. Paths relative to config; saved ARR responses cached in memory per invocation.
- Bundle: pass --boundary-name RF --output new_folder --write --kind gross (or excess). A new directory contains one CSV per event in rf_inflow, bc_dbase.csv, Event_File.tef, rainfall_losses.trd and manifest.json. Composite event tokens select one entire AEP/duration/loading/climate combination. Use safe case-insensitive unique ASCII alphanumeric/underscore event tokens and one site/source per bundle. Mixed timestep events have separate CSVs. TRD sets IL_RF and CL_RF variables but does not modify or configure existing materials/soils; connect these explicitly to the chosen model mechanism. Excess exports set both variables to zero; gross requires explicit ordinary IL/CL (including zeros) and refuses proportional/coefficient losses. Native TUFLOW acceptance remains open. Existing CSV-only mode remains available.
- Remaining: execute new unit tests and strict CI, verify actual BoM and ARR response datasets and installed PyTUFLOW version, automate supported live retrieval and loss selection where the upstream public APIs permit, review source factors/provenance, native TUFLOW ingestion and loss application. No RORB routing integration. No merge/release.

## CI correction

Run 46142010593 reported RUF100 (unnecessary C901 suppression) and RUF001 (Unicode dash in ARF diagnostic). Corrected both and reconciled stale CLI descriptions. The new tests were not reached in that run. Direct raw.githubusercontent.com retrieval to the Python container also failed on DNS; the independent workbook calculation comparison remains distinct from full source testing. Exact-head CI remains required.

## 28 November 2026 — user loss preference, optional regional defaults

The owner clarified that project-specific user-supplied IL/CL should be the normal input; regional generic values must be optional. Keep each event's explicit "spec" as the primary contract. There is no automatic fallback to regional values if a user spec is absent, incomplete, or invalid. Opt in with "arr_regional_losses": true inside options.losses (instead of spec). This reads full-storm IL/CL from an exact NewStormLosses row when present, else StormLossesNonNSW, else the legacy StormLosses summary when both newer layers are absent. The source layer is recorded, and missing/ambiguous current values fail closed. In NSW no interpolation/extrapolation or substitution of probability-neutral burst losses is claimed; use explicit user values or a qualified upstream assembled event for those cases. No regional equations duplicated; the direct adapter reads source fields through PyTUFLOW's public response rather than requiring unrelated ensemble TPs.

Regional mode defaults to storm_reduce with recommended preburst (RecPreburst where present, otherwise Preburst50); exact cell only. Explicit user PB depth or ratio can override the default. Complete mode requires explicit prefix or an ARR percentile plus a user-selected constant-rate preburst_duration_min, divisible by the timestep. No automatic PB shape or duration selection; no zero-clamping of negative burst IL. For a regional climate event, ARR climate loss factors default on; for explicit user loss values they default off to avoid adjusting already-adjusted inputs. Set arr_climate_loss_factors explicitly to change that choice. Record applied source, preburst layer and CC source in the QA manifest; unity factors still retain provenance and cannot be applied twice.

Example options.losses for regional baseline values: {"arr_regional_losses":true,"mode":"storm_reduce","arr_preburst_percentile":"recommended"}. For a complete storm with an explicit constant preburst: {"arr_regional_losses":true,"mode":"complete","arr_preburst_percentile":"50%","preburst_duration_min":120}. For manual values continue to use spec and explicit loss basis, e.g. {"spec":{"initial_loss_mm":25,"continuing_loss_mm_per_hour":3},"mode":"burst"}. To use live ARR data replace arr_response with latitude/longitude in a source object and pass --allow-network (off by default). Saved response and coordinates are mutually exclusive. Live requests use the public ArrApiClient (no TP download, no model execution); cache by coordinates and whether CC layer was requested for the invocation. A baseline and a climate event at the same point may require two requests. Saved JSON remains the recommended reproducible input. Live point retrieval is not exercised in tests; no native model runs.

Added focused synthetic tests for exact NSW rows, non-NSW regional losses, missing sources, preburst overflow-safe constant prefix, and climate factors; added offline integration check of the actual pinned PyTUFLOW saved-response reader. Next: exact-head hosted CI, CLI integration and real permission-cleared Data Hub/BoM source acceptance, review native TUFLOW ingestion/loss controls. Probability-neutral automatic loss lookup and recommended ensemble PB shapes are not needed for the generic-region option and remain separate optional extensions. No merge or release.

Follow-up: added CLI regressions covering manual versus regional opt-in, no implicit fallback, explicit complete prefix, preview and output manifest, and mocked live request caching. The cached public NSW and Queensland PyTUFLOW fixtures at accepted source revision 0121101650fd8e931af8d09cadb23bd43f6fefbd confirm the NewStormLosses and StormLossesNonNSW field schemas. CI run 46144163743 failed Ruff import spacing before tests; corrected I001 and reviewed formatting. Exact-head CI remains required.

Run 46144343611 passed Ruff lint but reported three formatter-only changes; applied the exact reported replacements. Tests were not reached on that revision.


## 28 November 2026 — real-source regression and strict typing

At 5d35097, hosted run 46144403042 passed six jobs and 3500 Python 3.11 source tests (16 skipped, 8 deselected), including the regional and CLI tests, plus 10 focused checks, 64 installed-wrapper checks, package/MCP/POSIX and Ruff. Windows strict Pyright reported only four unknown-lambda-type errors in the new CLI tests; use an annotated test double, without weakening policy.

Inspected the accepted PyTUFLOW source revision [0121101650fd8e931af8d09cadb23bd43f6fefbd](https://github.com/TUFLOW-Support/PyTuflow/tree/0121101650fd8e931af8d09cadb23bd43f6fefbd/tests/arr/fixtures). Added minimal real-number regression extracts (one 1% AEP column, 5–60 min, one PB/loss row, shared ARF parameters), not entire response files. Source blob identities: NSW 95621ea57f915dc74a72d86ff0aa02f1309938e9, SEQ 508653d3bb0b98c2ae70b8af98b894ff84b80ee0. Expected 60-minute ARFs independently evaluated from ARR short-duration equation 2.4.1 and, for SEQ area 5 km², area interpolation 2.4.4. The corrected numerical values below supersede the erroneous first benchmark in commit 4581098: the factor (0.3 + log10(AEP fraction)) multiplies the exponential term rather than entering its exponent. These are pinned numerical checks of the existing library, not new production ARF equations.

| Reference | Area (km²) | 2030 point depth (mm) | ARF | Areal depth (mm) | Storm IL (mm) | CL (mm/h) | PB depth (mm) | Reduced burst IL (mm) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| NSW | 11.4 | 88.3 | 0.8901146437226994 | 78.59712304071435 | 20 | 1 | 40.1765 | Refuse (PB exceeds IL) |
| SEQ | 5 | 93.8 | 0.9374810668222707 | 87.93572406792899 | 14 | 2.1 | 9.6614 | 4.3386 |

The NSW complete-storm check requires a deliberately chosen 60-minute constant-rate PB prefix and verifies positive preburst excess. SEQ verifies point-depth-based PB reduction independent of ARF override, and gross mass conservation. Both tests use actual installed PyTUFLOW response/ARF interfaces. The genuine response fixtures lack a CC layer; climate-factor tests remain synthetic. Added actual published BoM-reader checks on synthetic, schema-correct depth and intensity CSVs (exact rare AEP identity retained, explicit unit conversion, original bytes unchanged). The test values do not assert a site-specific BoM download. RainfallCurve now preserves source baseline year, area and source ARF override separately in bundle manifests; unknown metadata remains absent. Confirm direct native model acceptance and real live data separately. These new tests require exact-head CI; no claim that they have already passed.

## 28 November 2026 — correction of the independent ARF benchmark and integration

At 4581098, run 46148315993 passed Ruff, strict Pyright, package and POSIX security checks. Hosted tests stopped after 340 passes and one NSW reference assertion failure (8 deselected); the downstream required test/MCP gates therefore failed. Investigation compared the pinned PyTUFLOW equation with a fresh Python 3.13.5 evaluation. The prior independent reference mistakenly moved the AEP multiplier into the exponent of the third term of Eq. 2.4.1. Correct NSW 60-minute ARF is 0.8901146437226994 (the actual CI result), gross areal depth 78.59712304071435 mm. Correct SEQ ARF is 0.9374810668222707, gross 87.93572406792899 mm. Both curves remain monotonic over the eight duration anchors. Revised the expected constants and the earlier evidence table, not the production ARF implementation. PB depths and adopted loss conclusions are unchanged. The wrong numerical benchmark and failed run remain visible in Git history.

Reopened the original workbook: its BoM headers explicitly include `1% AEP`, `50% AEP`, EY and one-in-N forms. Added ARR-boundary notation adaptation while preserving the exact source rainfall label. Accept equivalent percent and one-in-N labels in exact PB/NSW table lookup, without rounding, nearest-column selection or EY/ARI inference. Calculated ARF still delegates to PyTUFLOW; its supported EY notation is retained there. Added actual BoM-reader plus calculated-ARF/PB lookup checks using `1% AEP`, exact rare AEP label tests, and unsupported frequency refusal checks. No real live request or native TUFLOW execution. Fresh CI required. Direct container git ls-remote again failed DNS; independent Python arithmetic was run successfully.

Integrate main 5a237cb94841dfff250b30a376451c26680440c8 (unrelated #118 conversion) into this feature branch with a two-parent commit, preserving all main code and work records; combine inventory and documentation counts (165 sources, 84 selected, 81 excluded). No change to main, RORB integration or release.

## 28 November 2026 — pinned PyTUFLOW output readback

The integrated head `7593ee1` passed all seven jobs in [run 46150395422](https://github.com/Chain-Frost/ryan-tools/actions/runs/46150395422): 3554 source tests passed (16 skipped, 8 deselected), 57 preliminary tests, 10 further focused checks and 64 installed-wrapper checks. Ruff, strict typing, package and security checks passed. This includes corrected ARF/source regressions. The user requested actual PyTUFLOW checks of output CSVs and use of the repository-pinned version.

Pin: `repo-scripts/pytuflow-requirement.txt`, accepted wheel `repo-scripts/wheels/pytuflow/0121101650fd8e931af8d09cadb23bd43f6fefbd/pytuflow-1.3.1-py3-none-any.whl`, SHA-256 `b223a803d5c900b85c50855fc18128855dde016ac60e191de63043b440538fc3`. Do not replace with a newer PyPI release. Inspected the matching upstream TCF/BCDatabase public context interfaces, CSV driver, and `tests/test_bc_event_file_substitutions.py`. Added `tests/functions/hydrology/test_nested_storm_pytuflow.py` to exercise actual generated CSVs for gross and excess, generated event/BC database resolution through a minimal TCF and `validate_batch_inputs`, FL/ML/BL, climate/ARF override and explicit PB, mixed timesteps, every time/value ordinate, leading/trailing zero and depth conservation. Separate combined CSV test checks shorter columns and NaN blank tails through actual BCDatabase readback. Unknown event must not pass. Tests do not alter the generated files or call the native solver. Existing CI wheel verification supplies the exact pin; these new cases require exact-head execution (no skip accepted for the pinned environment).

Remaining independent acceptance: Windows Python 3.14 focused source and copied-wrapper/installed-wheel tests against the pin, actual workbook/BoM/Data Hub data availability and live point query (including actual CC source factors), and a disposable native TUFLOW RF model verifying stepped interval timing and gross versus precomputed excess loss application, including preburst overflow. The CSV parser only verifies readable inputs and event selection; it cannot prove solver timing, infiltration or rainfall-loss equivalence. Retain native_qualified=false until independently verified. No new runtime dependency, CI policy change, RORB integration or merge to main.
