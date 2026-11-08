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
  `AbsMax`/`SignedAbsMax`, timeseries H and PO QA. Usable POMM maxima
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
