# WP13 — read-only QGIS project structural comparison

| Field | Value |
| --- | --- |
| Status | Implemented, isolated focused smoke passed; repository CI/review and Windows installed-wheel acceptance pending |
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
  out-of-order layers; duplicate/missing ID refusal; invalid archive handling;
  CLI JSON values and distinct exit codes.
- **Local portable smoke:** 7 focused tests passed on Linux/Python 3.13.5,
  using a temporary test harness with minimal stand-ins for the already reviewed
  **read_project_xml** and wrapper-utility interfaces because this environment
  cannot clone the repository (GitHub DNS unavailable). Python syntax
  compilation passed for all four new Python files. This is **not** a claim of
  complete repository Python 3.14 validation.
- **Not yet run at initial delivery:** repository-backed focused tests, changed-file
  Ruff, strict Pyright, Python 3.14, installed-wheel/copied-wrapper, hosted
  CI final-head and independent Codex review. Record their actual status in the
  PR after they occur; do not call unexecuted gates passed.
- **Native QGIS:** no semantic PyQGIS/provider/cartographic claims are made, so
  a QGIS runtime test is not an implementation acceptance requirement for this
  bounded XML-only feature. Windows mapped-drive/UNC project locations still
  warrant an installed-wrapper smoke on a Windows Python 3.14 workstation.

## Next steps

1. Review the draft child PR against #251; address substantive Codex findings.
2. Confirm focused repository tests, Ruff and strict Pyright and installed/copied
   wrapper on Windows Python 3.14, including Unicode URI and actual QGZ input.
3. Reconfirm base/head after any parent advancement. Retain #169 open for its
   unselected backlog. Do not integrate child to #251 or merge #251 to **main**
   without explicit authorisation and applicable accepted gates.
