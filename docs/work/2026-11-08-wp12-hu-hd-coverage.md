# WP12 — HU/HD capability and staged TUFLOW format coverage

| Field | Current status |
| --- | --- |
| Status | Needs review — native NTFS/UNC/mapped happy paths passed; partial-companion warning fix under CI; other acceptance gates open |
| Owner | WP12; coordinate numerical, validity and export contracts with WP03, WP04, WP08, WP10 and WP11 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 or after #219's no-data contract is accepted |
| Delivery | Draft stack: [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) → [#262](https://github.com/Chain-Frost/ryan-tools/pull/262) → [#269](https://github.com/Chain-Frost/ryan-tools/pull/269) → [#270](https://github.com/Chain-Frost/ryan-tools/pull/270); all unmerged to main |
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

## 2026-11-08 third delivery — complete selected reporting-location histories and summaries (#236)

[WP12 draft #270](https://github.com/Chain-Frost/ryan-tools/pull/270) extends the
approved reporting-point path with the **source-provided reporting-line and region families**.
It is based on [#269](https://github.com/Chain-Frost/ryan-tools/pull/269), itself stacked on
[#262](https://github.com/Chain-Frost/ryan-tools/pull/262) and [#251](https://github.com/Chain-Frost/ryan-tools/pull/251).
No branch has been integrated into `main`.

| Source suffix | Reader / consumer | Value and source meaning |
| --- | --- | --- |
| `_RLL_Q.csv` | `RLLQProcessor`, `reporting_lines` Python workflow | `Time` (simulation hours), reporting **line** ID `Location`, `Type=Reporting Line Flow`, signed `Value` and nullable `AbsValue` in the source flow units (m³/s for metric EG02); selected labels are read as columns before long-form expansion |
| `_RLL_Qmx.csv` | **Existing** `RLLQmxProcessor` (no implementation changes) | `Chan ID` retains the reporting-line identifier, `Q` and `Time` mean source Qmax and Time Qmax, with separate `dQ`, `Time dQ` and `H` fields; the source `H=-99999` remains a #219 validity limitation |
| `_RLR_Vol.csv` | `RLRVolProcessor`, `reporting_regions` Python workflow | `Time`, reporting **region** ID, `Type=Reporting Region Volume` and source volumes in model units (m³ for metric EG02) |
| `_RLR_Volmx.csv` | `RLRVolmxProcessor`, `reporting_regions` Python workflow | Source `Vol max`, `Time Vol max`, `dVolmax`, `Time dVolmax`, `H` retained with their independent associations |

Point, line and region histories share a single specialised wide-CSV reader;
point and region source maxima share a specialised source-summary reader.
They retain the previously accepted point output schema and missing-value treatment,
and they do not add a competing TPC parser or modify the upstream PyTuflow pin.
The protected reporting exporters share an internal publication helper.
The three workflow entry points have **separate output names and sheets** so that
a reporting point and reporting line with the same source identifier cannot be
silently combined as identical measurements or counted as separate events.
None of these are automatically added to the existing PO/POMM ensemble calculations.

**Archived numerical values (pinned EG02):** `EG02_013` reporting line `RL_03` has
181 sampled flows and a sampled maximum **96.812**, but source `RLL_Qmx`
reports **96.842** at **0.9741 h**. Reporting region `RL_06` has
181 sampled volumes with a printed maximum **8773.318**, while `RLR_Volmx`
reports **8773.601** at **1.2697 h**. These are true source-product
differences (sample time vs maximum event) and must not be rounded away,
overwritten or reconstructed into coincident hydraulic states.
Legitimate negative values and zeros remain; the selected new readers map
`-99999`, `**********` and empty numeric cells to missing.

**Verified code-head acceptance:** Windows Python **3.14.10** CI passed **all six checks** at [`da7fb3ac`](https://github.com/Chain-Frost/ryan-tools/commit/da7fb3ac39adfc796174bb01e3d4e4648d355531) (Hosted **2,590 passed, 14 skipped, 8 deselected**, Policy including strict Pyright, Ruff/whitespace, Package with installed-wheel/copied-wrapper smoke, MCP smoke and POSIX security). Genuine-fixture and synthetic regressions exercised the factory, early-column selection, maximum/time associations, signed/zero/missing cases, invalid inputs, geometry-name identity, optional companions, collection, public MCP and protected Excel/Parquet workflow. Codex identified a P1 list/tuple header comparison in the shared summary reader; it was corrected on this head and its review thread is resolved. **Native Windows/SMB/UNC, real batch memory/precision benchmarking, shared no-data acceptance and remaining #236 formats are not established by these CI passes.** WP04 owns
common `#219` validity; WP11 owns `#228` fixture matrix and `#229`
selected-read memory/precision measurement. Structure `_SQ/_SHmx`, split
2D formats, loss/MB, network and native container extensions remain under the
broader staged #236 backlog; this delivery does not close #236 or #237.

## 2026-11-08 native Windows report and follow-up correction

The independent [native Windows validation report on draft #270](https://github.com/Chain-Frost/ryan-tools/pull/270#issuecomment-6717962107) tested the
**exact source commit `fe475d43`**, with pinned test-data `d2dae3ec` and PyTuflow 1.3.0 at
`887534ea`; the source branch was unchanged during those tests. The local agent did not run an
installed-wheel validation or a licensed solver.

| Acceptance case | Observed result |
| --- | --- |
| Focused Python 3.14.6 tests | **44 passed** in 4.10 seconds; one warning. Pinned PyTuflow was isolated in scratch storage |
| Native local NTFS, real SMB UNC and mapped drive | **PASS** on all three reporting-point, line and region workflows; separate source and output scratch folders |
| Excel and Parquet | **PASS**: each workflow read back 181 series rows and one source-summary row; source maxima remained independent |
| Input immutability and no-clobber | **PASS**: source hashes/mtimes unchanged; a fixed export timestamp and identical existing paths raised `FileExistsError` without modification |
| Selected-column reads | **PASS on small fixtures only**: 12 source CSVs totalling 67,108 bytes, 1,092 selected vs 2,184 all rows; process peak working sets approximately 114–115 MB without meaningful improvement |
| Missing optional product diagnostics | **FAIL on tested `fe475d43`**: history-only `RLL_Q.csv` exported successfully with no warning for absent `RLL_Qmx.csv` |
| Native exclusive write locks, permissions and large/project-sized memory | **NOT RUN**; cannot claim these behaviors are accepted |
| Optional HDF dependency health and installed-wheel native acceptance | **UNRESOLVED / NOT RUN**: optional `h5py` DLL diagnostic `0xc0000139` appeared; CSV/Excel/Parquet paths did not use it |

**Action taken after the local report:** A focused correction on this same WP12 child PR adds
per-run warnings when one reporting-location product is not available in the selected processed
collection, identifying the absent `*_RLP_Hmx.csv`, `*_RLL_Qmx.csv`, `*_RLR_Volmx.csv` or
history counterpart by filename suffix. It preserves successful, read-only partial reporting
and existing export/no-clobber semantics. Tests now cover the reported history-only case with
`console_log_level="SUCCESS"` and an INFO log file, summary-only cases, a mixed complete/incomplete
multi-run collection, and a fully matched pair without spurious warnings. This **code change has
not itself been retested on native UNC/SMB**; check its exact-head hosted CI and request fresh
independent review before any acceptance or integration decision.

The native happy-path evidence is recorded; full #236/#237 acceptance remains open for
WP04/#219 shared missing-value behavior, WP11/#228 fixture matrix and #229 realistic memory,
optional binary/HDF environment health, native exclusive locks and permissions, and other
still-unimplemented source families such as `_SQ.csv` and `_SHmx.csv`. No merge to `main`,
automatic issue closure or release is authorised.

## #236 selected next: reporting-location point water levels (initial decision)

Select the reporting-location family next, starting with **_RLP_H.csv** (point water-level series) and **_RLP_Hmx.csv** (source-provided maximum and associated fields) using the existing PO-style report consumer. These have genuine EG02_013 source files, and TPC already has reporting-location readers. Reporting point/line/region types and measurement units must stay distinct even if their labels coincide; dHmax, Q and time columns must retain their source associations rather than all becoming independent maxima.

Acceptance before marking either suffix supported: real factory/dispatch, location filters, signed data and no-data handling, source-provided maximum meanings, collection values, Excel/Parquet readback, explicit units/provenance and an entry in [#228](https://github.com/Chain-Frost/ryan-tools/issues/228)'s supported-format matrix. Do not modify WP03's [#218](https://github.com/Chain-Frost/ryan-tools/issues/218) RLL_Qmx reporting lane or assume that general line/region formats have been implemented. SQ/SHmx, generic H/Q/V, L/CF and other candidate formats remain under #236, not delivered here.

## Validation and handoff

- **Passed source-level review:** current pin, registry, PO/POMM, collection/export, QA/plot and TPC source/API inspection; source-file pair comparisons and independent maxima/sentinel/precision calculations above.
- **Local authoring limitation:** The changes were made through GitHub's connector on a dedicated branch, not in an actual local Git worktree. No independently verified local Python 3.13 compile is claimed. Windows Python 3.14 hosted compilation/test evidence is tracked separately above.
- **Executed in hosted Windows Python 3.14 CI on prior head `69e15789`:** Ruff/whitespace, Package (installed/copy smoke), MCP smoke and POSIX wrapper security passed; strict Pyright and one numerical TPC assertion failed as above. The corrected HU/HD head `9e375778` passed all six GitHub checks (including Hosted, strict Pyright, Package, Ruff and MCP). Validate the final commit again, and do not infer passing native Windows/SMB acceptance from ordinary CI.
- **Historical authoring-phase limit:** No local native Windows test was performed in the GitHub-connector authoring environment. The later exact-source native NTFS/UNC/mapped happy-path tests and outstanding scale/lock/HDF gaps are documented above; no licensed solver was run.

Next action: validate the current-head companion-warning fix in CI, obtain fresh independent review and native spot-check of the changed diagnostic, then reconcile remaining scale/precision with WP11 and the shared #219 no-data contract with WP04. The next separate ready #236 candidate is `_SQ.csv` / `_SHmx.csv` structure outputs (state at maximum upstream level), not a re-interpretation of independent maxima. Keep remaining formats staged and separately allocated. #237 remains open until actual reader/consumer parity and required validity are accepted; broad #236 remains open. Do not merge, release or change the PyTuflow pin.
