# WP12 — HU/HD capability and staged TUFLOW format coverage

| Field | Current status |
| --- | --- |
| Status | Active — #237 investigation documented; runtime acceptance and #236 integration remain open |
| Owner | WP12; coordinate numerical, validity and export contracts with WP03, WP04, WP08, WP10 and WP11 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 or after #219's no-data contract is accepted |
| Delivery | Draft WP12 child PR based on [#251](https://github.com/Chain-Frost/ryan-tools/pull/251); no main merge |
| Tracking | [#237](https://github.com/Chain-Frost/ryan-tools/issues/237), [#236](https://github.com/Chain-Frost/ryan-tools/issues/236), [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), [#212](https://github.com/Chain-Frost/ryan-tools/issues/212) |

## Decision from the first-delivery investigation

**Do not register split _2d_HU.csv and _2d_HD.csv as independently supported processor inputs yet.** Their upstream/downstream measurements are already handled by the combined PO and POMM processors. The split representation contains the same observations as combined PO in the pinned sample; automatically ingesting both would duplicate an event. The current public PyTuflow TPC API already has a reader and inventory mechanism, so creating another TPC or generic network parser is unwarranted.

A future explicit-source adapter should use the existing time-series contract (simulation hours, Location, distinct Type, Value and full source-run identity), permit one input representation per run/measurement, preserve the model's level datum, and apply the accepted no-data contract before any maxima or QA operation. No dependency repin, general culvert connectivity, or coincident-maxima calculation is authorised.

## Current reader and consumer matrix

| Source / consumer | Current behaviour | Limitation / owner |
| --- | --- | --- |
| Combined PO / POProcessor | Reads two-row headers; preserves separate U/S Structure Water Level and D/S Structure Water Level measurements, location, hour, signed Value, run metadata; location filtering and ProcessorCollection.po_combine/export are present | The reserved -99999 remains numeric and its absolute value becomes 99999: [#219](https://github.com/Chain-Frost/ryan-tools/issues/219), WP04. Early selection/materialisation: [#229](https://github.com/Chain-Frost/ryan-tools/issues/229), WP11 |
| Combined POMM / POMMProcessor | Retains both distinct types and source Max, Tmax, Min, Tmin fields; existing POMM combine/export exists | A -99999 Min incorrectly creates AbsMax=99999, SignedAbsMax=-99999; #219/WP04. Contributing counts and event provenance: [#216](https://github.com/Chain-Frost/ryan-tools/issues/216)/[#217](https://github.com/Chain-Frost/ryan-tools/issues/217), WP03 |
| Pinned PyTuflow TPC | Public ids(), data_types() and time_series() expose both structure-level categories, with existing CSV, NetCDF and dedicated-GPKG dispatch | No ryan-tools split-format factory→report adapter; CSV reader uses float32, explicitly treats ********** as missing but not -99999, and loads referenced result groups before filtered time-series output |
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

## #236 selected next: reporting-location point water levels

Select the reporting-location family next, starting with **_RLP_H.csv** (point water-level series) and **_RLP_Hmx.csv** (source-provided maximum and associated fields) using the existing PO-style report consumer. These have genuine EG02_013 source files, and TPC already has reporting-location readers. Reporting point/line/region types and measurement units must stay distinct even if their labels coincide; dHmax, Q and time columns must retain their source associations rather than all becoming independent maxima.

Acceptance before marking either suffix supported: real factory/dispatch, location filters, signed data and no-data handling, source-provided maximum meanings, collection values, Excel/Parquet readback, explicit units/provenance and an entry in [#228](https://github.com/Chain-Frost/ryan-tools/issues/228)'s supported-format matrix. Do not modify WP03's [#218](https://github.com/Chain-Frost/ryan-tools/issues/218) RLL_Qmx reporting lane or assume that general line/region formats have been implemented. SQ/SHmx, generic H/Q/V, L/CF and other candidate formats remain under #236, not delivered here.

## Validation and handoff

- **Passed source-level review:** current pin, registry, PO/POMM, collection/export, QA/plot and TPC source/API inspection; source-file pair comparisons and independent maxima/sentinel/precision calculations above.
- **Passed local syntax check:** Python 3.13 compile of the new focused test module in an isolated authoring worktree; this is not Python 3.14 execution of the repository.
- **Not run:** Ruff, strict Pyright, repository pytest against the pinned submodule, installed-wheel and copied-wrapper smoke, PyTuflow runtime selection, or actual native/export performance checks. The connected GitHub environment has no full repository checkout with normal Python 3.14, so no such pass is claimed.
- **Not run / native:** Windows/SMB/UNC selected-read memory testing, licensed solver checks and any genuine network-output acceptance.

Next action: run the focused regression with a complete Python 3.14 installation and pinned test-data submodule, review its hosted CI result, coordinate #219 with WP04 and #229 with WP11, then take reporting-point integration in a separate reviewable stage. #237 remains open until actual reader/consumer parity and required validity are accepted; broad #236 remains open. Do not merge, release or change the PyTuflow pin.
