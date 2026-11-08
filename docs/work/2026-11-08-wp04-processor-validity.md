# WP04 — TUFLOW processor input health, lifecycle and culvert selection

**Status:** Implementation staged for draft child review; exact-head CI and native Windows acceptance not yet verified.
**Updated:** 2026-11-08. **Next review:** 2026-11-15.
**Allocation:** [#240](https://github.com/Chain-Frost/ryan-tools/issues/240).
**Original issues:** [#219](https://github.com/Chain-Frost/ryan-tools/issues/219) input health;
[#223](https://github.com/Chain-Frost/ryan-tools/issues/223) lifecycle;
[#220](https://github.com/Chain-Frost/ryan-tools/issues/220) culvert filters.
**Parent:** draft [#251](https://github.com/Chain-Frost/ryan-tools/pull/251), not `main`.
**Parent base at start:** `dc6d74455e092d3611b975f0fc3975a977578ecc`.
**Parent integration reconciliations:** `e62e19b47c0e1f1185170d5d378ea851eb8fc8b9`, `47e360d97c37704a5d929a7660d25107ba878341`, and `741803bc58c03e01e549bb217ed884fe55f01eea` (active work packs and shared indexes retained; no #251/main merge).

## Delivered implementation under review

- Lifecycle: clear `df`, `raw_df`, success, applied filters and diagnostics at each
  invocation in the selected maintained concrete processors. A TimeSeries inner
  read no longer marks success before subclass transform and final validation.
  Incomplete/replaced TLF logs cannot retain previous finished rows.
- Selection: postpone maximums entity filtering until Cmx, Nmx and Chan have
  canonical `Chan ID`; preserve requested-only direct/collection outputs.
  A correctly parsed but unmatched selection remains `filtered_empty` for
  the existing SKIP classifier; this is not a recovered full dataset.
  Preserve pre-selection EOF alignment in `ProcessorCollection` rather than
  inventing fuzzy channel-ID recovery.
- Hydraulic values: shared sample classifier rejects missing/nonfinite/non-numeric
  values and explicitly handles `**********`. TUFLOW's `-99999` is masked for
  recognised structure water levels before PO `AbsValue`, POMM
  `AbsMax`/`SignedAbsMax`, timeseries H and PO QA.
  Raw PO `Value` and POMM `Max`/`Min` retain source dry-level sentinel
  rows/values for EG02 provenance, while derived `AbsValue`,
  `AbsMax`/`SignedAbsMax` explicitly mask their invalid numerical effects. Usable POMM maxima
  survive a missing minimum; wholly unusable extrema do not rank as zero.
  Unusable POMM rows retain NaN extrema and event/TP provenance for
  `pomm_utils` population counts. All-invalid POMM files can remain
  structurally processed for aggregation but are classified as workflow FAIL
  by `input_health="INVALID"`.
  Negative discharge, negative level datums and real zero remain valid.
- QA: report rejected-sample counts/reasons, partial/invalid input health and
  separate hydraulic heuristic status. Reject malformed-width CSV records,
  nonfinite/reversed/duplicate timestamps and invalid warning/tolerance
  settings before ordinary OK/FLAT. The stability heuristic is whole-record
  significant-sign-change counting, not an implemented rolling window.
- Independent synthetic regressions for direct factory filtering, Q retries
  and invalid timestamps, POMM actual maxima and zero/negative preservation,
  PO masking, TLF finished/incomplete/recovery, selected HU QA, malformed
  records and tolerance configuration. Existing mocked reader tests now
  simulate a genuinely fresh read after state reset.

## Boundaries and known remaining acceptance

WP04 does not take ownership of WP09 closure population (#215), WP05 ccA
storage/cache processing, WP12 reporting-point adapters, WP10 Parquet plotting,
or general network connectivity (#221). Existing EOF truncated-ID alignment
requires complete unfiltered channel references in the collection; validate
direct EOF selection and wrapper ordering before claiming all of #220 complete.
WP12 [#269](https://github.com/Chain-Frost/ryan-tools/pull/269)
already carries its own HU/HD adapter masking; compare its integration semantics
without overwriting that branch.

This work record is **not evidence that tests have run**. The GitHub-API delivery
environment has no Windows Python 3.14, no checked-out Git worktree, and cannot
perform original PyTuflow/SMB/GDAL/QGIS licensed solver campaigns. Hosted checks,
regression snapshots, Module04 and EG02 source values, ten-TP mean/median,
independent malformed tail/parity, Windows/copy-wheel and Codex review still
require verification at the final child head. Do not claim #219/#220/#223
closed, complete or integrated until the acceptance results are recorded.

## Focused local agent handoff

Use an isolated Windows Python 3.14 checkout/worktree of the **current draft
WP04 child head**, with current vendored PyTuflow wheel and latest CI profile.
Do not modify `main`, merge, release or regenerate source snapshots without
independent expected-value review.

```powershell
python -m pytest tests/processors/tuflow/test_wp04_processor_contracts.py tests/processors/tuflow/test_collection_channel_selection.py tests/processors/tuflow/test_timeseries_processor.py tests/processors/tuflow/test_base_processor.py tests/processors/tuflow/test_max_data_processor.py tests/processors/tuflow/maximums_1d tests/processors/tuflow/other_processors tests/functions/tuflow/test_po_timeseries_checks.py -q
python -m ruff check ryan_library/processors/tuflow ryan_library/functions/tuflow/sample_validity.py ryan_library/functions/tuflow/po_timeseries_checks.py tests/processors/tuflow/test_wp04_processor_contracts.py
python -m ruff format --check ryan_library/processors/tuflow ryan_library/functions/tuflow/sample_validity.py ryan_library/functions/tuflow/po_timeseries_checks.py tests/processors/tuflow/test_wp04_processor_contracts.py
```

Run strict Pyright **only on changed Python files** using the repository's
current command, the Loguru formatting check if logging calls changed, the
docs/link checks, and installed-wheel/copied-wrapper behaviour for the affected
QA and culvert wrappers. Validate actual Module04 and EG02 POMM/PO values,
HU/HD stability outcomes, 10-TP mean/median counts/provenance, EOF alignment
with two selected channels and no-match, direct Chan/CF/H/Q/V/ccA selection,
and 1D Q malformed/truncated rows under source and wheel. Record exact SHA,
number of passes/fails/skips and environment-specific limitations in PR;
request fresh Codex review and resolve actionable findings.


## 2026-11-08 targeted CI preflight repair

Reviewed failed run 42476665887 at 8d4f6c30. Formatted processor_collection.py
and test_wp04_processor_contracts.py with the branch Ruff policy. Both initial
format repairs are AST-equivalent. Strict validation also exposed Unknown key/value
types after narrowing rejection metadata to dict: an explicit dict[object, object]
cast retains every existing key/value/count validation without changing the data.

Windows Python 3.14.6: Ruff lint/format and strict two-file Pyright pass; **33 focused
WP04/HDF tests pass**, zero skips with PyTables 3.11.1 and the accepted parser wheel.
No-bump 26.11.8.4 wheel build/verification passes. This is a narrow CI correction,
not acceptance of every native WP04 case. Source/installed parity and other review
requirements stay attached to their own evidence. No parent/main merge or release.


## 2026-11-08 EG02 derived no-data regression contract

After the preflight correction at 0e1be2bb, hosted run 42477372985 reached tests
and found the PO golden comparison still expected AbsValue=99999 for dry level
samples. A complete focused EG02 run reproduced PO and POMM failures only
(11 passes / 2 failures). This is not dependency/download infrastructure.

The WP04 implementation deliberately retains raw -99999 provenance while masking
its derived numeric effect, as specified above. The regression now explicitly
amends only the legacy snapshot's 88 upstream/downstream structure-level PO
AbsValue cells and the two POMM rows whose dry Min previously dominated a valid
Max. The fixture Parquet files are unchanged. Exact-count assertions prevent
scope expansion. Every raw Value/Max/Min, valid sample, non-structure type,
column/dtype and remaining cell still uses the strict frozen comparison. No
processor calculation was weakened or changed to recover the old sentinel bug.

Windows focused acceptance: **46 passes / zero skips** across all 13 EG02 cases,
WP04 contracts and HDF persistence. Strict modified-test Pyright, Ruff and format
pass. Confirm fresh hosted CI and review on the resulting child head before
integration. No main/parent merge, release or issue closure.

## 8 November 2026 — final invalid PO selection and storage reconciliation

The remaining P2 finding concerned an existing PO source location whose
readings were wholly rejected (e.g. `inf`). After unfiltered processing,
`ProcessorCollection.filter_locations()` could silently remove the location
and report SKIP as though it had never been present. WP04 now retains
source-header evidence, reclassifies known all-rejected selections as
INVALID/FAIL, and keeps an empty selected processor for explicit diagnostics.
No invalid readings are converted into valid hydraulic values. Tests cover
public filtering, selected data-type subcollections, genuinely absent
locations, valid locations, combined selection and repeated file processing.

The `rejected_samples` counter now counts rejected **source readings**,
not an additional derived POMM `all_missing_extrema` row. That row count
remains available in `rejection_reasons` for TP population diagnostics.

The separate unused HDF5 snapshot implementation was removed by #291
with an upgrade notice pointing to published PyPI `26.11.8.3` through
31 March 2027. WP04 no longer exercises retired HDF5 persistence:
its validation uses native `ryan-tools` processors and in-memory
collection selection/copy. Future directory-based Parquet import/export
belongs to draft #293, not this work pack. Parent reconciliation preserves
new WP12 source-format processors, WAL/ccA protections and other packs.

**Acceptance caveat:** original local Windows 10/Python 3.14 source/wheel,
copied-wrapper, genuine ten-TP provenance and live SMB acceptance
have not been independently provided for this reconciled head.
Hosted CI and Codex must be reported with the exact SHA. No `main` merge,
automatic release or claim of native acceptance is authorised.

## Source-confirmed nonfinite 1D channels (review follow-up)

After PO's invalid-location correction, exact-head Codex review found the
same source-presence gap for Q/H time-series processors. The shared
`TimeSeriesProcessor` now captures canonical channel IDs from CSV column
headers before numeric cleaning or H pivoting drops all-invalid series.
Public collection-level filtering retains an INVALID/FAIL diagnostic
processor for a requested source-confirmed unusable channel, while a
genuine absent channel still SKIPs and a usable C2 remains OK.
Tests include direct Q/H raw CSV, public/type selection and repeated
processing reset, without replacing the native ryan-tools TUFLOW readers.
