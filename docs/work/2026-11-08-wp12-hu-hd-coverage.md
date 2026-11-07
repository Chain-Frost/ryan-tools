# WP12 — HU/HD capability and staged TUFLOW format coverage

| Field | Current status |
| --- | --- |
| Status | Needs review — HU/HD and selected RLP_H/Hmx code delivered with hosted CI passing; full #236/native acceptance open |
| Owner | WP12; coordinate numerical, validity and export contracts with WP03, WP04, WP08, WP10 and WP11 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 or after #219's no-data contract is accepted |
| Delivery | HU/HD draft [#262](https://github.com/Chain-Frost/ryan-tools/pull/262) based on [#251](https://github.com/Chain-Frost/ryan-tools/pull/251); reporting-point child draft [#269](https://github.com/Chain-Frost/ryan-tools/pull/269) based on #262; no main merge |
| Tracking | [#237](https://github.com/Chain-Frost/ryan-tools/issues/237), [#236](https://github.com/Chain-Frost/ryan-tools/issues/236), [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), [#212](https://github.com/Chain-Frost/ryan-tools/issues/212) |

## Decision from the first-delivery investigation

**Do not register split _2d_HU.csv and _2d_HD.csv as independently supported processor inputs yet.** Their upstream/downstream measurements are already handled by the combined PO and POMM processors. The split representation contains the same observations as combined PO in the pinned sample; automatically ingesting both would duplicate an event. The current public PyTuflow TPC API already has a reader and inventory mechanism, so creating another TPC or generic network parser is unwarranted.

A future explicit-source adapter should use the existing time-series contract (simulation hours, Location, distinct Type, Value and full source-run identity), permit one input representation per run/measurement, preserve the model's level datum, and apply the accepted no-data contract before any maxima or QA operation. No dependency repin, general culvert connectivity, or coincident-maxima calculation is authorised.

## Current reader and consumer matrix

| Source / consumer | Current behaviour | Limitation / owner |
| --- | --- | --- |
| Combined PO / POProcessor | Reads two-row headers; preserves separate U/S Structure Water Level and D/S Structure Water Level measurements, location, hour, signed Value, run metadata; location filtering and ProcessorCollection.po_combine/export are present | The reserved -99999 remains numeric and its absolute value becomes 99999: [#219](https://github.com/Chain-Frost/ryan-tools/issues/219), WP04. Early selection/materialisation: [#229](https://github.com/Chain-Frost/ryan-tools/issues/229), WP11 |
| Combined POMM / POMMProcessor | Retains both distinct types and source Max, Tmax, Min, Tmin fields; existing POMM combine/export exists | A -99999 Min incorrectly creates AbsMax=99999, SignedAbsMax=-99999; #219/WP04. Contributing counts and event provenance: [#216](https://github.com/Chain-Frost/ryan-tools/issues/216)/[#217](https://github.com/Chain-Frost/ryan-tools/issues/217), WP03 |
| Pinned PyTuflow TPC | Public ids(), data_types() and time_series() expose both structure-level categories, with existing CSV, NetCDF and dedicated-GPKG dispatch | No ryan-tools split-format factory→report adapter; CSV loading uses float32 and the public TPC time_series() output normalises the 44 reserved -99999 samples to NaN (unlike the existing POProcessor); referenced result groups materialise before selected time-series output |
| Standalone _2d_HU.csv / _2d_HD.csv factory input | Neither is in the current ryan-tools processor registry | Unsupported until source selection, valid no-data semantics, precision and consumer/export parity are proven. No double counting |
| PO peak/stability QA | Separate PO parser can select HU/HD explicitly; Flow is the normal default | No-data contaminates ranges and tolerances; malformed/nonfinite input health is #219/WP04 |
| PO scenario plotting | Has a PO input path | Mixed measurement/run grouping and cache/filter parity remain [#225](https://github.com/Chain-Frost/ryan-tools/issues/225), WP10 |
| Closure and hydraulic capacity consumers | Flow-oriented thresholds and culvert quantities have defined meaning | HU/HD is not a new default flow/closure/capacity method; preserve independent extrema and supported culvert scope |

A format being parseable, producing hydraulically valid observations, and producing a correct consumer export are different levels of support. Current accepted combined PO reading does **not** imply an accepted HU/HD extrema or QA result.

PyTuflow's TPC constructor is lazy, but its [time-series loader](https://github.com/Chain-Frost/PyTuflow/blob/887534ea8c15b80d185a38a7ead1d42ab42a7167/pytuflow/_outputs/tpc.py) materialises several referenced categories; a selection on returned columns is not proof of selective disk I/O. Its [CSV loader](https://github.com/Chain-Frost/PyTuflow/blob/887534ea8c15b80d185a38a7ead1d42ab42a7167/pytuflow/_outputs/info.py) uses float32 for numeric samples. Upcasting cannot recover lost precision. The upstream [structure-group test](https://github.com/Chain-Frost/PyTuflow/blob/887534ea8c15b80d185a38a7ead1d42ab42a7167/tests/test_time_series.py) asserts type availability and two 181×1 shapes, not independently expected values. Evaluate selected-read RSS and numeric tolerance with #229 before adopting its results silently.

## Actual pinned numerical evidence — EG02_011

The parent repository points to ryan-tools-test-data commit **d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58**. Read the real [combined PO](https://github.com/Chain-Frost/ryan-tools-test-data/blob/d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58/tuflow/TUFLOW_Example_Model_Dataset/EG02/EG02_011_PO.csv), [POMM](https://github.com/Chain-Frost/ryan-tools-test-data/blob/d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58/tuflow/TUFLOW_Example_Model_Dataset/EG02/EG02_011_POMM.csv), [HU](https://github.com/Chain-Frost/ryan-tools-test-data/blob/d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58/tuflow/TUFLOW_Example_Model_Dataset/EG02/plot/csv/EG02_011_2d_HU.csv), [HD](https://github.com/Chain-Frost/ryan-tools-test-data/blob/d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58/tuflow/TUFLOW_Example_Model_Dataset/EG02/plot/csv/EG02_011_2d_HD.csv) and [TPC](https://github.com/Chain-Frost/ryan-tools-test-data/blob/d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58/tuflow/TUFLOW_Example_Model_Dataset/EG02/plot/EG02_011.tpc).

| Actual observation | HU / upstream | HD / downstream |
| --- | ---: | ---: |
| Printed time samples | 181 | 181 |
| Reserved -99999 samples | 44 | 44 |
| Usable observations | 137 | 137 |
| First usable at 0.733333 h | 38.6572 m | 38.3981 m |
| Maximum usable level | 40.7877 m | 40.6995 m |
| POMM time of maximum | 1.1333 h | 1.1333 h |
| Largest float32 error among valid levels | 0.000001904297 m | 0.000001879883 m |

All 181 split HU and HD timestamps and printed values matched the corresponding combined-PO columns numerically, including the first 44 reserved values. The largest float32 time conversion error was about 0.000000110809 h. POMM's source Max agrees with the usable PO maxima, but the POMM source Min is -99999, not a valid minimum elevation. These checks compare archived source CSVs, not a PyTuflow execution or report-export run. They prove pinned fixture equivalence, not general-version support or memory behaviour.

The accompanying focused regression targets the real POProcessor/ProcessorCollection path, source-split parity, POMM source columns and a Parquet export readback. It deliberately does **not** freeze the known wrong AbsMax as expected behaviour.

## 2026-11-08 hosted-CI reconciliation

The original [WP12 child head `69e15789`](https://github.com/Chain-Frost/ryan-tools/pull/262) ran Windows Python 3.14.10 hosted CI. Four jobs passed (Ruff/whitespace, Package including wheel/copy checks, MCP smoke, POSIX wrapper security). Policy failed with **one** strict-Pyright finding: PyTuflow's `time_series()` method has partially unknown `*args/**kwargs` typing at the call site. Hosted tests reported **2,530 passed, 1 failed, 14 skipped, 8 deselected**: the sole failing test assumed the raw sentinel `-99999` was retained in TPC's public result, but the **actual TPC time_series() converts the first 44 sentinel samples to NaN**. This does not mean the ryan-tools PO/POMM readers already handle reserved values correctly; they still need #219. The failed checks were real and must not be called green.

The WP12 test now binds only the explicitly exercised public TPC interface through a typed Protocol and checks 44 missing NaNs, 137 valid values, and the known levels/maxima with an explicit float32 tolerance. The source PO/POProcessor regression still checks that the legacy source keeps its 44 `-99999` values, documenting the present mismatch rather than masking it. All six checks passed at the HU/HD refreshed head `db356814`; no upstream dependency change or main merge is authorised.

## 2026-11-08 second delivery — selected reporting-point results (#236)

The implementation is staged as [draft #269](https://github.com/Chain-Frost/ryan-tools/pull/269), targeting #262 (which targets #251), not main. This is a **bounded reporting-point slice only**, not completion of all #236 formats. The prior HU/HD branch was refreshed against the current #251 feature branch by the normal merge commit `db356814`, retaining other work packs' changes and existing WP12 history without a force push.

| Product / source | Current new public reader | Destination / preservation |
| --- | --- | --- |
| `_RLP_H.csv` | `RLPHProcessor`, `data_type=RLP_H`, registry `dataformat=PO` | `Time` simulation hours, `Location` reporting-point ID, `Type=Reporting Point Water Level`, signed float64 `Value` and nullable `AbsValue`; labels selected during CSV read |
| `_RLP_Hmx.csv` | `RLPHmxProcessor`, `data_type=RLP_Hmx` | Source-supplied `Hmax`, `Time Hmax`, `dHmax`, `Time dHmax` and `Q` retained without generating coincident maxima |
| Python orchestration | `ryan_library.orchestrators.tuflow.reporting_points.main_processing` | Uses existing finder/collection, location selection, dashboard and protected exporter; separate `Water_Level_Series` and `Source_Maxima` sheets/Parquet files |
| Existing MCP inspection | `inspect_tuflow_result` / `inspect_tuflow_collection` via the registered processor factory | Read-only selected rows; no extra TPC or specialised MCP endpoint |

The source headers and current automated fixture target are the `EG02_013`/`EG02_014` `plot/csv` files at the pinned test-data revision. For `EG02_013`, RL_01 has 181 samples and 22 reserved missing levels, and its **largest printed time-series sample is 42.4816**, whereas the source **Hmax is 42.4817** at 1.2814 hours. `dHmax=100038.01562` is the source value (likely affected by no-data), **not** a newly computed maximum stage or validated physical increase; `Q=-99999` is reserved and becomes missing. In the selected processing path `-99999` and `**********` map to NaN; legitimate negative and zero values remain. Unknown numeric tokens/nonfinite values fail rather than silently becoming observations. Neither a new TPC parser nor a PyTuflow pin change is included.

**Run/report limitations:** The two families retain original run codes; history and source maxima are exported separately, not as two ensemble realisations. The new Python workflow is API-accessible and generic MCP inspection works via the registry; no new copied CLI wrapper or direct integration into the POMM mean/median reports is claimed. The new readers use explicit time/header/location validation but **do not replace WP04's shared #219 validity contract**, and they must not mask accepted cross-format/zero/missing correctness gates. Existing RLL_Qmx (#218), general lines/regions, SQ/SHmx, generic split H/Q/V, loss coefficients, mass balance and native NetCDF/GPKG remain staged or unsupported unless separately selected.

**Validation:** The selected reporting-point code/tests passed **all six GitHub checks** at [`0c917568`](https://github.com/Chain-Frost/ryan-tools/commit/0c91756885f0f00e30dd9e5cdf5126c7263010dd) in Windows Python **3.14.10**: **2,565 passed, 14 skipped, 8 deselected** (Hosted); Policy including strict Pyright; Ruff/whitespace; Package including installed-wheel/copied-wrapper smoke; MCP smoke; POSIX security. New focused assertions exercise the actual EG02 factory, pre-read location projection, signed/zero/NaN/invalid input outcomes, protected Excel/Parquet readback, generic MCP inspection and the public discovery→report workflow. **Native Windows/SMB and measured large-file selected-read memory remain unverified**, and neither the shared #219 validity contract nor all #236 formats are claimed delivered. These successes apply to the cited code head; documentation-only follow-up commits must still pass their current-head checks.

## #236 selected next: reporting-location point water levels (initial decision)

Select the reporting-location family next, starting with **_RLP_H.csv** (point water-level series) and **_RLP_Hmx.csv** (source-provided maximum and associated fields) using the existing PO-style report consumer. These have genuine EG02_013 source files, and TPC already has reporting-location readers. Reporting point/line/region types and measurement units must stay distinct even if their labels coincide; dHmax, Q and time columns must retain their source associations rather than all becoming independent maxima.

Acceptance before marking either suffix supported: real factory/dispatch, location filters, signed data and no-data handling, source-provided maximum meanings, collection values, Excel/Parquet readback, explicit units/provenance and an entry in [#228](https://github.com/Chain-Frost/ryan-tools/issues/228)'s supported-format matrix. Do not modify WP03's [#218](https://github.com/Chain-Frost/ryan-tools/issues/218) RLL_Qmx reporting lane or assume that general line/region formats have been implemented. SQ/SHmx, generic H/Q/V, L/CF and other candidate formats remain under #236, not delivered here.

## Validation and handoff

- **Passed source-level review:** current pin, registry, PO/POMM, collection/export, QA/plot and TPC source/API inspection; source-file pair comparisons and independent maxima/sentinel/precision calculations above.
- **Local authoring limitation:** The changes were made through GitHub's connector on a dedicated branch, not in an actual local Git worktree. No independently verified local Python 3.13 compile is claimed. Windows Python 3.14 hosted compilation/test evidence is tracked separately above.
- **Executed in hosted Windows Python 3.14 CI on prior head `69e15789`:** Ruff/whitespace, Package (installed/copy smoke), MCP smoke and POSIX wrapper security passed; strict Pyright and one numerical TPC assertion failed as above. The corrected HU/HD head `9e375778` passed all six GitHub checks (including Hosted, strict Pyright, Package, Ruff and MCP). Validate the final commit again, and do not infer passing native Windows/SMB acceptance from ordinary CI.
- **Not run / native:** Windows/SMB/UNC selected-read memory testing, licensed solver checks and any genuine network-output acceptance.

Next action: review the #269 source and hosted code head, reconcile native Windows/SMB and selected-memory/precision evidence with WP11, and agree the #219 no-data contract with WP04 before declaring full HU/HD and reporting-point consumer acceptance. Keep the remaining #236 formats staged and separately allocated. #237 remains open until actual reader/consumer parity and required validity are accepted; broad #236 remains open. Do not merge, release or change the PyTuflow pin.
