# WP03 — POMM and TP report correctness

| Field | Value |
| --- | --- |
| Status | Active; native acceptance partial, real RLL/mixed inputs required |
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
  and integrated XLSX tests; the Windows pandas 2.3.3 focused compatibility and current 3.x
  hosted tests have now passed (see evidence below). Native real-file POMM/RLL/culvert export parity,
  POMM mean/median copied-wrapper execution and UNC/mapped-drive input remain outstanding.
  RLL `Qmax` provides no independent time-series negative minimum and is not equivalent to POMM
  signed absolute-maximum semantics.

## Next action

1. Keep all six exact-head CI gates green, including Windows Python 3.14 current-pandas
   full-suite and pandas 2.3.3 focused POMM/culvert compatibility. Investigate only new failures.
2. Validate synthetic file→real processor→XLSX round trips on the final stack, and record outcomes.
3. Run native real TUFLOW file/export parity, pandas 2.3.3 and installed copied-wrapper acceptance
   from a separate local Windows worktree; document measured pass/fail evidence and any gaps.
4. Reconcile final PR stack and keep drafts unmerged until explicit approval. No main merge is authorized.
   Add the four WP03 closing keywords to parent #251 only after the reviewed child is
   actually incorporated; a stacked child's base is not the default branch.

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

### 2026-11-08 — Exact-head CI follow-up

Hosted Windows Python 3.14.10 CI on WP03 head `d6af50f1` completed:
Package, Ruff/whitespace, MCP and POSIX security passed. Hosted tests ran
2,547 selected tests: 2,546 passed and one failed when Pandas 3.1.1 raised
`TypeError: boolean value of NA is ambiguous` in a `median_TP == mean_TP`
comparison for an all-invalid hydraulic bin. Strict Policy/Pyright reported
four errors: a retained but unused legacy metric selector, a malformed
file-level Pyright directive, and two intended private-helper test uses.

The follow-up WP03 commit converts TP comparisons to nullable string equality,
calls the retained metric helper while requiring `Q` as the governing
statistic, and fixes the test directive. CI on the follow-up exact head
must be checked before acceptance. These findings do **not** establish native
real-file, pandas 2.3.3, wheel/copy-wrapper, or Windows/UNC acceptance.

### 2026-11-08 — pandas compatibility and cross-PR acceptance review

**Code + CI head:** `5b52f4f5965b758acb6c4983e55f1f05c236e180`,
[all six checks passed](https://github.com/Chain-Frost/ryan-tools/commit/5b52f4f5965b758acb6c4983e55f1f05c236e180/checks)
on Windows Python 3.14.10 (2026-11-07 UTC).
The current pandas **3.1.1** hosted suite reported **2,566 passed,
14 skipped, 8 deselected** (147 warnings); after installing the
Python 3.14-compatible pandas **2.3.3** and its `pytz` dependency,
the focused POMM/culvert regression set reported **48 passed**
(8 warnings). Policy strict Pyright 0 errors/0 warnings, Ruff/whitespace,
Package, MCP and POSIX security all passed. The Package job built/verified
an isolated wheel and exercised six copied TUFLOW result wrappers
including synthetic workbook/dashboard parity; those six did **not**
include dedicated POMM mean/median copied-wrapper real-file execution.
The pandas 2.3.3 check was added to the established hosted CI without
changing the runtime dependency range.

**Cross-PR reconciliation:** Separate, still-open [#207](https://github.com/Chain-Frost/ryan-tools/pull/207)
contains merged-in #238 POMM coverage warnings, its expected TP count and
duration schedule, and coverage-settings metadata; no copy of that work
has been merged into WP03 or parent #251.
The [integration review](https://github.com/Chain-Frost/ryan-tools/pull/207#issuecomment-6710799402)
records that #238's raw observed TP coverage must remain distinct from
WP03's **finite contributing** `count_TP`, with known unusable counts
reported separately. Preserve both implementations, the latest list-based
wrapper settings and the union of column definitions; then test
`test_pomm_coverage_findings.py` alongside WP03 regressions on
the eventual integrated candidate. This is a **pending integration
review**, not accepted combined behaviour.

**Native handoff:** [Local Windows acceptance prompt](https://github.com/Chain-Frost/ryan-tools/pull/267#issuecomment-6710844466).
Actual Windows project CSVs, UNC/mapped-drive input, source-file
non-mutation, copied mean/median POMM wrappers and real POMM/RLL/culvert
workbook parity are **NOT RUN/NOT VERIFIED**. Access to a normal
Windows project environment and representative files is required.
No Git installation, solver execution, production/share mutation or
automatic merge is authorised or required.

**Issue closure:** Draft child #267 contains `Fixes #227`, `Fixes #218`,
`Fixes #216`, `Fixes #217`. Because the child does not target `main`
and is not incorporated, all four issues stay open. Transfer these
closing keywords to parent #251 only after the child is accepted and
incorporated; do not close issues merely because an intermediate PR is
stacked or CI is green.

### 2026-11-08 — Native Windows acceptance, partial pass

Tested exact implementation head `dcbec3aead0d8e63a201e49b607f9f1b77c3d9bc` using its successful Package run 42398158770/artifact 13584103558. All six checks passed on that SHA. Installed wheel `ryan_tools-26.11.7.13-py3-none-any.whl` into a disposable local target with `pip --no-deps --target`; SHA-256 `206c195221ba78f92c459ddaaccf6701c8a79eeca92c356dcf74fcc3c8486bf4`. Python 3.14.6/pandas 3.0.5 imported from the installed target, not the checkout. All four requested wrappers were copied through installed `copy_wrapper`.

Measured passes: real POMM 20 unchanged CSV copies/two durations/ten TPs (2,604 independent workbook comparisons); real Cmx 20 unchanged copies/two durations/ten TPs (676 comparisons); direct actual UNC POMM 110 CSVs/11 durations/ten TPs (13,485 comparisons); synthetic POMM-only/RLL-only/mixed and culvert finite/zero/invalid/tie/provenance cases (505 comparisons); 38 data-dictionary sheet checks; 20 shuffled tie checks. Selected original source hashes and UNC independent-read hashes remained unchanged. All outputs stayed on local scratch; no solver, release, merge or normal installation replacement.

Full sanitized evidence, expected/actual values, environment, commands and limitations: [native acceptance comment](https://github.com/Chain-Frost/ryan-tools/pull/267#issuecomment-6711270903). Private raw workbooks/logs/manifests remain only under local scratch `C:\Temp\wp03-native-20261108`; do not publish their identifiers or paths. Scratch drivers and JSON comparisons preserve reproduction details.

**INCOMPLETE:** Suitable real TP-named RLL_Qmx-only and mixed POMM/RLL ensembles were not located; real-input path clarification is pending. Tutorial inputs without TP metadata produced missing-column diagnostics and empty tables despite wrapper success; do not count these as passing ensemble acceptance. No new WP03 regression was established; missing-metadata failure signaling remains a triage limitation. #207/#238 combined coverage semantics remain separate and unverified; preserve observed TP/file coverage versus finite contributing counts and rerun both regression sets before any authorized integration.

Next action: maintainer supplies suitable real RLL/mixed ensembles; run copied wrappers and independent readback against the exact verified wheel and record results. Keep native acceptance open and next review 2026-11-15. This handoff is a documentation-only follow-up on the existing WP03 branch; wheel evidence stays anchored to the tested implementation SHA, and later-head CI must be checked separately. No production code or unrelated checkout state changed.

### 2026-11-08 — Supplied tutorial data follow-up

The maintainer identified `tests/test_data/tuflow` as the intended data source. Its four RLL_Qmx files lack TP naming tokens; Module 11 POMM files contain no hydraulic columns. Byte-identical local copies of two Module 11 RLL files and Module 09 POMM files were given explicitly synthetic filename metadata, preserving every original hydraulic value. Both copied installed-wheel wrappers passed 98 independent CSV-to-XLSX comparisons for RLL-only/mixed reporting, statistics, source names, assigned TP, finite counts and governing duration. RLL mean 108.059, nearest actual event 106.803, upper-middle median 109.315 matched exactly. Originals and copies retained matching SHA-256 values. Commands: `python run_followup.py` and `python check_followup.py` from existing private scratch.

[Detailed follow-up evidence](https://github.com/Chain-Frost/ryan-tools/pull/267#issuecomment-6717348747). This proves real-value compatibility under synthetic labels, not authentic RLL TP/duration provenance. Native acceptance remains incomplete at that boundary; the register's open status and 2026-11-15 review date remain appropriate. The current tested wheel stays anchored to dcbec3ae; 1ef7e255 changed only two work-tracking Markdown files and had no returned Actions run/artifact at recheck. This follow-up changes documentation only; do not relabel earlier CI as current-head evidence. No implementation change, merge or release.
