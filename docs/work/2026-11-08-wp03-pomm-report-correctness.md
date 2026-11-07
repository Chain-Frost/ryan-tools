# WP03 — POMM and TP report correctness

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | WP03 allocation agent |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | WP01 parent PR #251 `fix/tlf-hyphenated-variables-239` at `3a6dc23622b062f7d20467025e6e72d7f34addfe`; earlier WP03 work derived from separate PR #207 |
| Git delivery | Single draft [WP03 #267](https://github.com/Chain-Frost/ryan-tools/pull/267) stacked on [WP01 parent #251](https://github.com/Chain-Frost/ryan-tools/pull/251); #253/#260 retained as superseded review records |

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

- **#238 separation:** Existing PR #238 remains integrated into separate, unmerged #207.
  The accepted coverage warnings, raster defaults and historical native evidence remain on #207,
  rather than being duplicated in WP03. WP03's POMM changes are based on #251; coordinate with
  #207's owner before eventual integration of overlapping POMM files.
- **#227:** Superseded review PR #253 changes the two POMM `Series.map(func=...)` calls to positional mappers
  without tightening the global pandas dependency; adds an actual grouped POMM statistics regression.
  **All six exact-head Windows CI checks passed** at `39b8c777`, including hosted tests, Ruff, Policy,
  Package, security and MCP. Native real-project validation is separate.
- **#218:** Superseded review PR #260 uses `combine_raw()` for the already selected POMM/RLL inputs, normalizes
  RLL location and Q magnitude while keeping signed Q, source filename and distinct measurement type,
  and adds synthetic RLL-only/POMM-only/mixed file→real processor→workbook regressions.
  Hosted Windows/Python 3.14.10 initial run at `5f33e497` passed 2,272 tests with one newly added
  test fixture failing from a mismatched keyword (`paths` vs `paths_to_process`). Fixed at `4f3aa910`;
  **six exact-head checks rerun/pending**. Previously passing Ruff, Policy, Package, security and MCP
  checks cannot be attributed to the new head until rerun.
- **#216 and #217:** Implemented in **draft #267** on a separate stacked branch. POMM/culvert
  finite-value ranks, upper-middle/nearest-mean identities, deterministic tie ordering, finite and
  unusable event/bin counts, Q-governed culvert peaks, and selected source-file metadata are included.
  Synthetic numeric, input-order permutation and Excel readback regressions are committed.
  **Exact-head Windows CI and independent native real-data verification remain pending**; do not
  treat either issue as accepted/complete before these checks.

## Validation and limitations

- **Environment:** This agent has Python 3.13.5/pandas 2.2.3 on Linux, with neither
  a local clone/worktree nor Python 3.14, Ruff or Pyright. GitHub clone failed due unavailable DNS.
  Isolated branches were committed through GitHub; this is **not** a claim of an actual git worktree.
- **Passed local API smoke (2026-11-08):** pandas 2.2.3 rejects `Series.map(func=...)`,
  accepts positional mapper; mixed categorical POMM/RLL locations and signed/absolute Q conversions
  yield `Location=[C1,C2]`, `Type=[Flow,Q]`, `AbsMax=[12,7]`, `SignedAbsMax=[12,-7]`.
- **Historical native evidence:** PR #207 reports 117 focused tests, Python 3.14.10, six passing
  hosted checks and an isolated installed-wheel copied-wrapper smoke, before WP03's changes.
  Do not attribute those passes to #253/#260/#267.
- **Still required:** exact-head Windows/Python 3.14 CI for #260 and #267, strict Pyright/Ruff
  and integrated XLSX tests; additional pandas 2.3.3 compatibility, installed copied-wrapper run
  against the new wheel, and native real-file POMM/RLL/culvert export parity.
  RLL `Qmax` provides no independent time-series negative minimum and is not equivalent to POMM
  signed absolute-maximum semantics.

## Next action

1. Review exact-head #260 and #267 Windows hosted tests, Ruff and strict Pyright; resolve genuine
   regressions without broad refactoring, keeping each PR's individual fixes reviewable.
2. Validate synthetic file→real processor→XLSX round trips on the final stack, and record outcomes.
3. Run native real TUFLOW file/export parity, pandas 2.3.3 and installed copied-wrapper acceptance
   from a separate local Windows worktree; document measured pass/fail evidence and any gaps.
4. Reconcile final PR stack and keep drafts unmerged until explicit approval. No main merge is authorized.

## Progress

### 2026-11-08

Reconciled integrated #238 with open parent #207 and current work records. Created and
pushed #253/#260/#267 as separate GitHub draft branches. Added real POMM aggregation and
synthetic RLL/POMM workbook regression tests; documented distinct source semantics.
Checked hosted CI and repaired the reported formatting failures. No change to
`pomm_combine()` public behaviour, shared suffix registry, or unrelated raster work.

The #267 numerical stack changes `median_calc.py`, `pomm_utils.py` and culvert statistics, with
finite TP/count/representative-source provenance regressions and updated column definitions.
Its draft was opened, and #240 coordination was posted; exact-head checks remain authoritative.

### 2026-11-08 — Integrate onto WP01 #251

The maintainer requested a single eventual squash merge to main. WP03 changes are consolidated
into existing draft #267, stacked directly on WP01 parent #251 rather than maintaining
multiple independently mergeable PRs. A normal two-parent merge commit retains historical
#253/#260/#267 work while resolving the tree against #251 to include only WP03 files:
pandas mapping compatibility, RLL retention, finite TP statistics, test coverage, column
definitions and this work record. Unrelated raster and default-source changes inherited
from #207 are excluded. No force-push or main merge is authorised.

Treat #253/#260's previously passing CI as historical; run exact-head CI against the new
#267 integration tree before claiming native or packaged acceptance. The parent #251 must
integrate a reviewed WP03 child before the one eventual, explicitly approved squash
merge to main. Native Windows project-file and installed/copied-wrapper parity remain pending.
