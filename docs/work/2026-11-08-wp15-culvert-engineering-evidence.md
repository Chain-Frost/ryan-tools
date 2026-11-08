# WP15 — culvert / HY-8 engineering evidence and ellipse integration gates

| Field | Value |
| --- | --- |
| Status | Evidence first delivery complete; numerical implementation blocked on published native package and source inputs |
| Owner | WP15 allocation agent / maintainer |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 or earlier if blockers resolve |
| Issues | [#102](https://github.com/Chain-Frost/ryan-tools/issues/102), [#103](https://github.com/Chain-Frost/ryan-tools/issues/103), [#101](https://github.com/Chain-Frost/ryan-tools/issues/101) |
| Allocation | [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) / WP15 |
| Assessed refs | main `8de77a67fb614ad32cd3c151a338f23eeb871412`; PR #251 `dc6d74455e092d3611b975f0fc3975a977578ecc` |
| Delivery | `docs/wp15-culvert-evidence-101-103-20261108`, draft child of #251; no main merge permitted |

## Scope decision

This is the **first required evidence delivery**, not approval of ellipse numerical support or closure of the three issues. The accepted circular-only migration [PR #100](https://github.com/Chain-Frost/ryan-tools/pull/100), its [validation record](2026-10-08-tuflow-culvert-engines-99.md) and historical [AB-023](../audits/2026-10-22-script-remediation-backlog.md) remain intact. No solver, public API, TUFLOW wrapper, production source or dependency pin is modified by this assessment.

Merged upstream source does **not** establish published installed-package availability. [run-hy8 PR #7](https://github.com/Chain-Frost/run-hy8/pull/7) merged; verified published [run-hy8 `2026.10.18.2`](https://pypi.org/project/run-hy8/2026.10.18.2/) supports its bounded HY-8 8.0.1.2 ellipse catalogue. [ryan-culverts PR #29](https://github.com/Chain-Frost/ryan-culverts/pull/29) merged on 18 October, but latest verified [ryan-culverts `26.10.16.1`](https://pypi.org/project/ryan-culverts/) was published **15 October**, before that merge. Current `ryan-tools` bounds (`run-hy8>=2026.10.16.1`, `ryan-culverts>=26.10.16.1`) do not guarantee the new native ellipse API is installed. Do not import unpublished symbols or silently substitute a source checkout/`PYTHONPATH` for supported installed-wheel operation.

## Retained hydraulic evidence (not rerun here)

| Source | Established | Limit |
| --- | --- | --- |
| [Migrated #100](https://github.com/Chain-Frost/ryan-tools/pull/100) and [record](2026-10-08-tuflow-culvert-engines-99.md) | Genuine tutorial EOF-to-Maximums workbook, four installed-Windows HY-8/native forward/inverse scenarios, maximum recorded differences 0.18138% Q, 0.00367773 m headwater and 0.16326% outlet velocity. | Physical inlet/material settings were assumptions, not verified from source. No real mixed `1d_nwk`/active blockage matrix or universal parity. |
| [run-hy8 ShapeDB/inlet research](https://github.com/Chain-Frost/run-hy8/blob/main/docs/hy8_v8_inlet_configurations.md) | HY-8 8.0.1.2 `ShapeDB.dat` SHA-256 `2479e9444feaff529313e18a1b26fc2de4f6b6c541db58477da6bebc602164a7`; material-aware catalogue, reader/writer, executable results. | HY-8 catalogue products are not unrestricted mathematical ellipses. |
| [ryan-culverts #29 independent review](https://github.com/Chain-Frost/ryan-culverts/blob/main/docs/work/2026-10-18-pr29-hydraulic-review.md) and [18-case evidence](https://github.com/Chain-Frost/ryan-culverts/blob/main/docs/validation_data/hy8_8_0_1_2_ellipse_sweep/comparison.csv) | Six baseline and 18-case HY-8 comparisons, analytical geometry and source-backed concrete inlet applicability. | Only 2/18 sweep cases meet **both** existing rounding-only gates; ~34–36 m³/s critical/profile behavior for 128 × 82 in remains unexplained. No numerical identity or construction-grade acceptance. |

These figures are **upstream historical evidence**, not new WP15 test results. Do not tune coefficients, widen tolerances or replay all upstream HY-8 acceptance to manufacture parity.

## Current supported shape / inlet / material matrix — #101

| Path | Supported evidence | Excluded or fail-closed |
| --- | --- | --- |
| ryan-tools project/adapter | `RectangularBarrelDefinition` and `CircularBarrelDefinition`; [solver adapter](https://github.com/Chain-Frost/ryan-tools/blob/main/ryan_library/functions/culvert/adapter.py). | No typed ellipse project geometry or explicit shape-specific ellipse inlet fields. Generic HDPE without verified native coefficients can fail during hydraulics. |
| TUFLOW Maximums and `1d_nwk` engines | Circular-only source contract; material/inlet must be explicit, not derived from n or losses. [Documentation](../TUFLOW_CULVERT_ENGINES.md). | `Type=C/R`, `Flags`, Manning n, EntryC and pBlockage **do not** establish ellipse span/rise/orientation/material/inlet. No extension to migrated wrappers now. |
| Public `run_hy8` HY-8 v8 | Elliptical shape project code **3**. Concrete material **context code 2**: 23 catalogued sizes, square-edge headwall / grooved-edge headwall / grooved-edge projecting. Steel-or-Aluminum **context code 1**: 40 sizes, headwall / mitered / beveled / thin-edge projecting. | Use [material-specific public enums and catalogues](https://github.com/Chain-Frost/run-hy8/blob/main/src/run_hy8/__init__.py); reject arbitrary, reversed/non-catalogued and nearest sizes, synthetic rotation, missing inlet mapping and treating Steel-or-Aluminum as circular CSP. Nonzero `Br/Tr/Cr/B` are part of the catalogue geometry. |
| Merged native solver *source*, not available in verified PyPI wheel | `HorizontalEllipseGeometry` with span > rise and `VerticalEllipseGeometry` with rise > span; concrete HDS-5 ellipse-specific inlet/entrance-loss records. [Public API](https://github.com/Chain-Frost/ryan-culverts/blob/main/src/culvert_solver/__init__.py). | Requires **explicit** physical inlet coefficients and source-applicable concrete material. No steel-or-aluminum native ellipse coefficient substitution; mathematical ellipses are not HY-8 four-arc catalogue profiles. |
| Merged native `Hy8ConcreteOvalGeometry` *source* | Separate four-circular-arc horizontal concrete ShapeDB profile; 83 × 53 and 128 × 82 inch supported examples. [Source](https://github.com/Chain-Frost/ryan-culverts/blob/main/src/culvert_solver/geometry/hy8_oval.py). | **8/23** concrete catalogue rows pass independent *unscaled* span/area checks within 0.5%; 60 × 38 and 68 × 43 in are rejected. The 0.5% limit is a **geometry/source reconciliation**, not a hydraulic error tolerance. No vertical catalogue rotation. |

**Next bounded code slice, after native wheel publication and verification:** add one project-defined **concrete horizontal mathematical ellipse** with explicit square-edge/headwall physical inlet and the matched native HDS-5 inlet/entrance-loss records, preserving span/rise/orientation, requested and solved Q, headwater, HW/D based on **rise**, warning codes and source provenance in project/CSV round-trips. Prove native-only operation without the HY-8 EXE; invalid material, missing physical inlet, wrong axis/orientation and unsupported mappings must fail closed. An optional later HY-8 comparison must use a *source-reconcilable four-arc oval on both backends*, not compare a mathematical ellipse to the ShapeDB profile as though geometry were identical. Preserve existing circular/CSP/HDPE/box behavior.

## Missing same-model sources and acceptance matrix — #102/#103

The [#100 migration audit](2026-10-08-tuflow-culvert-engines-99.md) examined 162 supplied GeoPackages / 950 layers and relevant control files without finding a suitable real `1d_nwk`/`pBlockage` source set. **No new physical source model was supplied to WP15.** The tutorial workbook cannot establish event-specific blockage provenance.

| Required input | Required engineering conclusion |
| --- | --- |
| Executed TCF/TBC/TEF/TRD, exact event/run, active `1d_nwk`/`1d_nwkb` and source attributes for matched circular, rectangular, ignored, pits and ≥2 barrels | Identify actual channel/source identity, numeric vs character `pBlockage`, true physical geometry, nominal dimensions, inherited defaults, material/inlet/roughness and loss mapping. `Flags` does not substitute for EOF `Type`. |
| Active blockage CSV/matrix, method `RAM` or `ELM`, AEP/ARI, category, defaults and overrides | Determine which **event-specific method and percentage** were actually used. RAM reduces effective area; ELM adjusts energy loss without reducing geometry. Test category, blank character, ordinary blank numeric, overrides and **100%** blocked cases; no assumed default or silent ELM-to-diameter conversion. |
| Same channel's EOF `CULVERT AND PIPE DATA` and Maximums workbook, including `Height`, `Entry Loss`, `Exit Loss`, `Fixed Loss` | Prove whether source dimensions/losses are nominal or already effective; prevent double blockage adjustment. Retain raw nominal/effective parameters, source method and nominal HW/D denominator in exported rows. Fail closed on unresolved status. |
| Approved installed HY-8 8.0.1.2 with executable path/version/hash and disposable scratch workspace, raw `.hy8`/`.rst`/`.rsql`/`.plt` and normalized outputs | Run Maximums Q forward at DS_h and outlet invert, inverse at US_h, HW/D target; network inverse HW/D **1.5 and 2.0** at active tailwater, ≥2-barrel cases, physical inlet/material/loss variations. Compare discharge, headwater, control/flow, outlet depth/velocity, barrel/road split, warning and preserved workspaces with **pre-agreed** engineering tolerances. |

Official [TUFLOW 2026.3 `5.7.1.1`](https://docs.tuflow.com/classic-hpc/manual/2026.3/05-1D-Domains.html) distinguishes Reduced Area and Energy Loss methods; [2026.3 TCF commands](https://docs.tuflow.com/classic-hpc/manual/2026.3/80-AppendicesA-TCFCommands.html) require `Blockage Method` when a blockage matrix is active. The existence of these documented methods is **not** proof of which method a particular model executed.

## Windows 3.14 local-agent handoff — NOT EXECUTED

Use only a permitted licensed HY-8 Windows host and an **authorised, disposable** same-model source fixture. Do not change model files or run chargeable/licensed solvers without approval. Do not copy confidential inputs into the PR.

1. Read `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, this record, the original issues and PR #100 record. Record `py -3.14 -V` and `py -3.14 -m pip show ryan-tools run-hy8 ryan-culverts`; verify import origins. Record HY-8 executable exact path, product/file version and SHA-256. **Do not test #101 native ellipse** against a pre-ellipse wheel, unpublished code or altered `PYTHONPATH`.
2. Trace the active source channel from `1d_nwk` through event/matrix/EOF/Maximums. Write an auditable mapping of nominal/effective dimension, pBlockage type/category/value/method, event override, raw/effective loss, roughness, material, physical inlet and tailwater. Return **BLOCKED** for fields that cannot be established.
3. Discover the current maintained wrapper regressions in `tests`. On copies in a fresh unique workspace, perform the #102 forward/inverse scenarios and #103 blockage cases above **only where physical inputs are matched**. Retain raw project/result artifacts, exact source and executable hashes, input/output CSV, requested/solved Q and warnings. Establish tolerances independently **before** marking parity; preserve errors rather than excluding cases.
4. After the correctly published native package is installed, implement/test #101's bounded concrete project schema/adapter. Check finite geometry and units, project serialization, true native-only forward/inverse, nonzero flow, rise-denominator HW/D, material/inlet validation and preserved warning/provenance. Add comparative HY-8 only for source-backed four-arc equivalent geometry.
5. On the final exact commit run focused pytest, `ruff check` / `ruff format --check`, strict Pyright on changed files, `python repo-scripts/check_documentation.py` and installed-wheel/copied-wrapper smokes as appropriate. Record pass/fail/skip/not-run and explicit permission/native limitations. Request Codex review; fix substantive findings and re-review. **Do not merge #251 into main or close partial issues.**

## Completion checklist and delivery state

- [x] Source-backed first-delivery assessment; retained #100, AB-023, and upstream comparisons without overclaim.
- [x] Physical matrix, published-version blocker, source-model fixture requirements and exact Windows-agent handoff.
- [ ] #102 actual source-backed mixed `1d_nwk` and installed HY-8 same-model multi-scenario verification.
- [ ] #103 actual TUFLOW event/blockage/matrix/EOF provenance and no-double-application evidence.
- [ ] #101 published native ellipse wheel; project-defined concrete ellipse implementation/tests; bounded optional HY-8 comparison.
- [ ] Codex review, final exact-head hosted checks and separate approval for any child integration into #251.

**Validation 2026-11-08:** GitHub repository/source-path and upstream API inspection, PyPI version/provenance page verification and TUFLOW 2026.3 documentation review. Newly authored relative links verified using GitHub file lookups. No model data or executable available. **Functional pytest: not run (0 passed, 0 failed, 0 skipped); Ruff/Pyright: not run (no Python changes); Windows HY-8/installed-wheel: not run.** The canonical `python repo-scripts/check_documentation.py` is not run because the execution environment has no network-accessible Git checkout; hosted CI/Codex are separately pending. Changes committed only to the WP15 child branch.

**Next action:** secure a published native solver ellipse release and actual matched TUFLOW source/event fixtures; then implement the smallest #101 case and separately run only remaining #102/#103 native acceptance. Review by 2026-11-15 or on unblock.
