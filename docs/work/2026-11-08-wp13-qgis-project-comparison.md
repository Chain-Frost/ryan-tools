# WP13 — read-only QGIS project structural comparison

| Field | Value |
| --- | --- |
| Status | Implemented; 6/6 exact-head hosted CI, clear Codex review and Windows 10/Python 3.14 installed-wheel copied-wrapper acceptance PASS |
| Owner | WP13 allocation agent |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Scope | [#240 WP13](https://github.com/Chain-Frost/ryan-tools/issues/240), selected slice of [#169](https://github.com/Chain-Frost/ryan-tools/issues/169) |
| Parent | Draft [#251](https://github.com/Chain-Frost/ryan-tools/pull/251), branch **fix/tlf-hyphenated-variables-239**; no merge into main |
| Delivery branch | [feat/wp13-qgis-structural-comparison-169](https://github.com/Chain-Frost/ryan-tools/tree/feat/wp13-qgis-structural-comparison-169) (draft child PR targets #251) |

## Selection and current behaviour

The existing [audit_qgis_projects.py](../../ryan-scripts/other/audit_qgis_projects.py)
already inventories and checks QGS/QGZ layer sources. Do **not** add a competing
inventory implementation. The selected read-only capability is **project layer
structural comparison** exposed via
[qgis_project_comparison.py](../../ryan_library/functions/qgis_project_comparison.py)
and [compare_qgis_projects.py](../../ryan-scripts/other/compare_qgis_projects.py).

It reads QGS XML using the existing bounded **read_project_xml** helper, including
QGS embedded in QGZ, and matches unique, nonempty layer IDs across two projects.
Reports distinguish added, removed and modified layers and the exact former/new
layer names, provider IDs, source URIs and layer CRS authids, plus project CRS
authid. Comparisons are order-independent and machine-readable as JSON. Provider
categories are deliberately **file-capable**, **service**, **non-file** or
**unknown**; classification never claims that a URI is a local accessible path.
Missing/duplicate IDs fail explicitly rather than matching the wrong layers.

Neither project nor linked source is written or opened beyond reading the project
XML; embedded QGD databases, unknown archive members and their bytes remain
untouched. The CLI writes to stdout only and exits 0 for no supported structural
changes, 2 for changes, 1 for errors. No output file or temporary file is
created by this feature.

**Not included:** file inventory reimplementation; portable packaging, path
repair, rebase or bulk mutation; layer ordering, visibility, group tree,
relations, styling, provider execution or cartographic validation. Those
criteria remain on #169 for separate selections. The comparison reports raw
XML and does not claim semantic GIS data equality or full QGIS interpretation.

## Validation and evidence

- Disposable synthetic QGS and QGZ comparisons cover a QGZ with QGD and unknown
  archive members; exact source project/archive/data byte preservation;
  independent expected change values, provider classification, CRS and
  out-of-order layers; duplicate/missing/whitespace-only ID refusal; invalid archive handling;
  CLI JSON values and distinct exit codes.
- **Local portable smoke:** 9 focused tests passed on Linux/Python 3.13.5,
  using a temporary test harness with minimal stand-ins for the already reviewed
  **read_project_xml** and wrapper-utility interfaces because this environment
  cannot clone the repository (GitHub DNS unavailable). Python syntax
  compilation passed for all four new Python files. This is **not** a claim of
  complete repository Python 3.14 validation.
- **Initial hosted preflight at 102477bb:** Ruff identified two EM102 exception-style, one RUF043 regex and one PT013 import violation. Policy did not reach Pyright; remaining jobs were skipped. Corrected in follow-up, not credited as a passing run.
- **Windows Python 3.14 hosted run [42473143587](https://github.com/Chain-Frost/ryan-tools/actions/runs/42473143587), commit `0b2749d7`:** Ruff/format/docs/actionlint/Loguru checks **PASS**; strict Pyright **0 errors / 0 warnings**; wheel build, installed generic copied-wrapper smoke and POSIX security **PASS**. Hosted regression reached **2,950 passed, 14 skipped, 8 deselected, 1 failed**. All 9 new QGIS tests passed. The one failure was `test_documentation_indexes_track_live_inventory_counts`: it correctly detected this WP13 wrapper increases inventory from 160 to **161 source paths, 83 selected, 78 excluded**. MCP gate consequently failed because hosted results were not successful. The documentation/index counts have been corrected in the next commit; the new exact-head CI must establish success before claiming it.
- **Codex:** source and exact-head reviews through `0b2749d7` finished **without findings**. A fresh review remains appropriate after any substantive code change.
- **Final exact-head hosted CI:** [run 42474460637](https://github.com/Chain-Frost/ryan-tools/actions/runs/42474460637) at `ef103594`: all 6 jobs passed, 2,982 hosted passes, 14 skips and 8 deselections; Ruff, strict Pyright, Package, MCP and POSIX successful. Exact-head Codex review found no major issues.
- **Windows native installed-wheel acceptance:** [report on #283](https://github.com/Chain-Frost/ryan-tools/pull/283#issuecomment-6722226430) at `ef103594`, Windows 10/Python 3.14.6: nine focused tests, Ruff/format, strict Pyright, documentation and `--no-bump` wheel build passed; **29 copied-wrapper subprocess cases passed**, including local Unicode/space paths, mapped drive and genuine UNC, correct 0/1/2 exit codes and JSON provenance. Verified freshly installed wheel physical import under site-packages, matching input/QGD/unknown ZIP member hashes, and audit proving linked source and QGD were not opened. Initial missing-submodule setup attempts were resolved before the passing execution. No source code changes after this acceptance.
- **Native QGIS:** no semantic PyQGIS/provider/cartographic claims are made, so
  a QGIS runtime test is not an implementation acceptance requirement for this
  bounded XML-only feature. Windows mapped-drive/UNC project locations still
  warrant an installed-wrapper smoke on a Windows Python 3.14 workstation.

## Next steps

1. Reconcile the reviewed and natively accepted WP13 slice with the current #251 parent and check exact merged-head CI/Codex.
2. Integrate the child into #251 once approved; retain #169 OPEN for unrelated future capabilities and do not merge #251 to **main** or release.
