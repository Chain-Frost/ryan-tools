# WP03 — POMM and TP report correctness

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | WP03 allocation agent |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | PR #207 `fix/tuflow-src-profile-cell-counts` at `c7d063502e43391560fdd877e117cc6bcd064faf`; #238 integrated into #207 as `8550b511` |
| Git delivery | Draft [#253](https://github.com/Chain-Frost/ryan-tools/pull/253) and stacked draft [#260](https://github.com/Chain-Frost/ryan-tools/pull/260), committed to separate remote branches |

## Outcome and assigned boundaries

Follow allocation [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), WP03:
#238's lint findings, [#227](https://github.com/Chain-Frost/ryan-tools/issues/227)
pandas API compatibility, [#218](https://github.com/Chain-Frost/ryan-tools/issues/218)
RLL_Qmx retention, then [#216](https://github.com/Chain-Frost/ryan-tools/issues/216)
finite TP observations and [#217](https://github.com/Chain-Frost/ryan-tools/issues/217)
POMM counts/representative-event provenance.

Do not merge main, auto-merge, release, or expand into another pack. Coordinate changes to
`pomm_utils.py`, `median_calc.py`, culvert statistical workflows and shared documentation.
Proper unique run naming is the model author's responsibility (#234); duplicate-name repair is out of scope.
Preserve the upper-middle median and greatest nearest-mean representative-duration rule.

## Current state

- **#238 prerequisite:** Existing PR #238 was already merged into #207 (not main). The parent has
  documented completed four lint/type repairs and 117 focused tests, verified wheel and six passing hosted checks.
  The real-project raster override acceptance belongs to #207, not WP03.
- **#227:** Draft #253 changes the two POMM `Series.map(func=...)` calls to positional mappers
  without tightening the global pandas dependency; adds an actual grouped POMM statistics regression.
  A hosted run passed tests, Package, POSIX security and MCP; format-only Ruff/Policy failures
  were repaired at `39b8c777`. **Updated-head hosted checks pending.**
- **#218:** Draft #260 uses `combine_raw()` for the already selected POMM/RLL inputs, normalizes
  RLL location and Q magnitude while keeping signed Q, source filename and distinct measurement type,
  and adds synthetic RLL-only/POMM-only/mixed file→real processor→workbook regressions.
  Ruff's initial adjacent-literals formatting finding was repaired at `5f832fb8`.
  **Updated-head hosted checks pending.**
- **#216 and #217:** Numerical correctness, finite-population/count/provenance rules and their
  linked regression coverage are **not delivered** by #253 or #260; a separate next stage is required.

## Validation and limitations

- **Environment:** This agent has Python 3.13.5/pandas 2.2.3 on Linux, with neither
  a local clone/worktree nor Python 3.14, Ruff or Pyright. GitHub clone failed due unavailable DNS.
  Isolated branches were committed through GitHub; this is **not** a claim of an actual git worktree.
- **Passed local API smoke (2026-11-08):** pandas 2.2.3 rejects `Series.map(func=...)`,
  accepts positional mapper; mixed categorical POMM/RLL locations and signed/absolute Q conversions
  yield `Location=[C1,C2]`, `Type=[Flow,Q]`, `AbsMax=[12,7]`, `SignedAbsMax=[12,-7]`.
- **Historical native evidence:** PR #207 reports 117 focused tests, Python 3.14.10, six passing
  hosted checks and an isolated installed-wheel copied-wrapper smoke, before WP03's changes.
  Do not attribute those passes to #253/#260.
- **Not yet passed/required:** exact-head Windows/Python 3.14 hosted tests, Ruff, strict Pyright;
  pandas 2.3.3 and current 3.x compatibility runs, full POMM/RLL factory and workbook tests,
  installed copied-wrapper run against the new wheel, and native real-file export parity.
  RLL `Qmax` provides no independent time-series negative minimum and is not equivalent to POMM
  signed absolute-maximum semantics.

## Next action

1. Review exact-head #253 and #260 hosted checks; resolve genuine failures without broad refactoring.
2. Implement finite-event/count/provenance fixes for #216 and #217 in a new **reviewable stacked draft**;
   add numerical regressions and workbook round trips before claiming these issues complete.
3. Run focused Python 3.14 validation, build verified package with normal repository rules and
   document installed/copy-wrapper and native acceptance gaps.
4. Reconcile PR stack and update this record at the next handoff. No main merge is authorized.

## Progress

### 2026-11-08

Reconciled integrated #238 with open parent #207 and current work records. Created and
pushed #253/#260 as separate GitHub draft branches. Added real POMM aggregation and
synthetic RLL/POMM workbook regression tests; documented distinct source semantics.
Checked hosted CI and repaired the reported formatting failures. No change to
`pomm_combine()` public behaviour, shared suffix registry, or unrelated raster work.
