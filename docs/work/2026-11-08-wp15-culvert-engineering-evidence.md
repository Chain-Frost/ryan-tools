# WP15 — culvert / HY-8 engineering evidence and ellipse integration gates

| Field | Value |
| --- | --- |
| Status | Bounded native ellipse implemented and locally validated; source-model/HY-8 acceptance blocked |
| Owner | WP15 allocation agent / maintainer |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 or earlier if blockers resolve |
| Issues | [#102](https://github.com/Chain-Frost/ryan-tools/issues/102), [#103](https://github.com/Chain-Frost/ryan-tools/issues/103), [#101](https://github.com/Chain-Frost/ryan-tools/issues/101) |
| Allocation | [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) / WP15 |
| Assessed refs | main `8de77a67fb614ad32cd3c151a338f23eeb871412`; PR #251 `dc6d74455e092d3611b975f0fc3975a977578ecc` |
| Delivery | `docs/wp15-culvert-evidence-101-103-20261108`, draft child of #251; no main merge permitted |

## Scope decision

This is the **first required evidence delivery**, not approval of ellipse numerical support or closure of the three issues. The accepted circular-only migration [PR #100](https://github.com/Chain-Frost/ryan-tools/pull/100), its [validation record](2026-10-08-tuflow-culvert-engines-99.md) and historical [AB-023](../audits/2026-10-22-script-remediation-backlog.md) remain intact. The initial assessment changed documentation only. The Windows continuation below adds a bounded native project API and updates the published dependency minimum; TUFLOW wrappers and production sources remain unchanged.

**Historical initial package assessment (superseded by the Windows verification below):** merged upstream source does **not** establish published installed-package availability. [run-hy8 PR #7](https://github.com/Chain-Frost/run-hy8/pull/7) merged; verified published [run-hy8 `2026.10.18.2`](https://pypi.org/project/run-hy8/2026.10.18.2/) supports its bounded HY-8 8.0.1.2 ellipse catalogue. [ryan-culverts PR #29](https://github.com/Chain-Frost/ryan-culverts/pull/29) merged on 18 October, but latest verified [ryan-culverts `26.10.16.1`](https://pypi.org/project/ryan-culverts/) was published **15 October**, before that merge. Current `ryan-tools` bounds (`run-hy8>=2026.10.16.1`, `ryan-culverts>=26.10.16.1`) do not guarantee the new native ellipse API is installed. Do not import unpublished symbols or silently substitute a source checkout/`PYTHONPATH` for supported installed-wheel operation.

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
| Active blockage CSV/matrix, configured `RAM` or `ELM`, AEP/ARI, category, defaults and overrides, plus actual culvert flow-control regimes | Determine the **event-specific percentage, configured method and effective regime-dependent method**. RAM reduces area. ELM increases entrance energy loss **only under outlet control** without reducing geometry; **under inlet control TUFLOW instead applies RAM and no ELM loss**. Confirm the actual regime for each evaluated case/time interval, including regime transitions. Test category, blank character, ordinary blank numeric, overrides and **100%** blockage; reject unknown regime or silent ELM-to-diameter conversion. |
| Same channel's EOF `CULVERT AND PIPE DATA` and Maximums workbook, including `Height`, `Entry Loss`, `Exit Loss`, `Fixed Loss` | Prove whether source dimensions/losses are nominal or already effective; prevent double blockage adjustment. Retain raw nominal/effective parameters, **configured method, effective flow-regime-specific treatment**, source flow regime and nominal HW/D denominator in exported rows. Fail closed on unresolved status. |
| Approved installed HY-8 8.0.1.2 with executable path/version/hash and disposable scratch workspace, raw `.hy8`/`.rst`/`.rsql`/`.plt` and normalized outputs | Run Maximums Q forward at DS_h and outlet invert, inverse at US_h, HW/D target; network inverse HW/D **1.5 and 2.0** at active tailwater, ≥2-barrel cases, physical inlet/material/loss variations. Compare discharge, headwater, control/flow, outlet depth/velocity, barrel/road split, warning and preserved workspaces with **pre-agreed** engineering tolerances. |

Official [TUFLOW 2026.3 `5.7.1.1.2` and `5.7.1.2`](https://docs.tuflow.com/classic-hpc/manual/2026.3/05-1D-Domains.html#limitations) specify the crucial exception: **ELM operates only under outlet control; inlet-control flow uses RAM instead** (without the ELM loss). [2026.3 TCF commands](https://docs.tuflow.com/classic-hpc/manual/2026.3/80-AppendicesA-TCFCommands.html) require `Blockage Method` when a blockage matrix is active. A configured method and event percentage therefore **do not establish the effective method without the actual control regime**. This is source-backed behavior, not proof of a particular model's effective treatment.

## Windows 3.14 local-agent handoff — NOT EXECUTED

Use only a permitted licensed HY-8 Windows host and an **authorised, disposable** same-model source fixture. Do not change model files or run chargeable/licensed solvers without approval. Do not copy confidential inputs into the PR.

1. Read `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, this record, the original issues and PR #100 record. Record `py -3.14 -V` and `py -3.14 -m pip show ryan-tools run-hy8 ryan-culverts`; verify import origins. Record HY-8 executable exact path, product/file version and SHA-256. **Do not test #101 native ellipse** against a pre-ellipse wheel, unpublished code or altered `PYTHONPATH`.
2. Trace the active source channel from `1d_nwk` through event/matrix/EOF/Maximums. Write an auditable mapping of nominal/effective dimension, pBlockage type/category/value/**configured method**, event override, **actual inlet/outlet control regime at each evaluated case/time**, effective RAM versus ELM treatment (including the ELM-to-RAM fallback under inlet control), raw/effective loss, roughness, material, physical inlet and tailwater. Return **BLOCKED** for fields that cannot be established.
3. Discover the current maintained wrapper regressions in `tests`. On copies in a fresh unique workspace, perform the #102 forward/inverse scenarios and #103 blockage cases above **only where physical inputs and effective regime-specific blockage treatment are matched**. Include **outlet-control ELM and inlet-control RAM fallback** for ELM-configured models, or explicitly record the absence of one regime. Retain raw project/result artifacts, exact source and executable hashes, input/output CSV, requested/solved Q, actual regime and warnings. Establish tolerances independently **before** marking parity; preserve errors rather than excluding cases.
4. After the correctly published native package is installed, implement/test #101's bounded concrete project schema/adapter. Check finite geometry and units, project serialization, true native-only forward/inverse, nonzero flow, rise-denominator HW/D, material/inlet validation and preserved warning/provenance. Add comparative HY-8 only for source-backed four-arc equivalent geometry.
5. On the final exact commit run focused pytest, `ruff check` / `ruff format --check`, strict Pyright on changed files, `python repo-scripts/check_documentation.py` and installed-wheel/copied-wrapper smokes as appropriate. Record pass/fail/skip/not-run and explicit permission/native limitations. Request Codex review; fix substantive findings and re-review. **Do not merge #251 into main or close partial issues.**

## Completion checklist and delivery state

- [x] Source-backed first-delivery assessment; retained #100, AB-023, and upstream comparisons without overclaim.
- [x] Physical matrix, published-version blocker, source-model fixture requirements and exact Windows-agent handoff.
- [ ] #102 actual source-backed mixed `1d_nwk` and installed HY-8 same-model multi-scenario verification.
- [ ] #103 actual TUFLOW event/blockage/matrix/EOF provenance, verified inlet/outlet-control-dependent ELM/RAM treatment and no-double-application evidence.
- [x] #101 published native ellipse wheel and bounded project-defined horizontal concrete ellipse implementation/tests. Optional equivalent-geometry HY-8 comparison remains unexecuted.
- [ ] Codex review, final exact-head hosted checks and separate approval for any child integration into #251.

**Historical initial assessment validation 2026-11-08:** GitHub repository/source-path and upstream API inspection, PyPI version/provenance page verification and TUFLOW 2026.3 documentation review. Newly authored relative links verified using GitHub file lookups. No model data or executable available. **Functional pytest: not run (0 passed, 0 failed, 0 skipped); Ruff/Pyright: not run (no Python changes); Windows HY-8/installed-wheel: not run.** The canonical `python repo-scripts/check_documentation.py` is not run because the execution environment has no network-accessible Git checkout; hosted CI/Codex are separately pending. Changes committed only to the WP15 child branch.

**Next action:** review the bounded native integration and obtain authorised matched source/event fixtures, explicit HY-8 execution permission and independently agreed engineering tolerances for remaining #102/#103 acceptance. Review by 2026-11-15 or on unblock.


## Windows continuation ? 2026-11-08

Separate worktree `E:/Github/ryan-tools-wp15-282` starts at the verified expected
PR #282 head `9a5f4725599f6c210b914e242a88354f30f796ba` on the existing draft branch.
Read allocation #240 and original #101/#102/#103; no new issue closure or merge is authorised.
Unrelated register entries with review dates before 2026-11-08 remain due; no unrelated work resumed.

### Published and installed dependencies

Windows Python **3.14.6**, `C:/Program Files/Python314/python.exe`. Initial installed
`run-hy8==2026.10.18.2` and `ryan-culverts==26.10.18.3` have no direct-source URL override.
Both import from the normal Python314 user `site-packages`, outside any repository.
The old native wheel lacks the public ellipse exports. Live PyPI now supplies
[ryan-culverts 26.10.19.1](https://pypi.org/project/ryan-culverts/26.10.19.1/).
Downloaded wheel SHA-256 **9fed3f0bf529ffc8250034bb0c0b2c349c88833efffabcd201b9ae09ddb67bfd**
matches PyPI metadata. Installed that published wheel using `python -m pip install
--no-deps ryan-culverts==26.10.19.1`; verified public horizontal/vertical ellipse,
concrete oval, shape-specific inlet and entrance-loss exports. No private checkout,
PYTHONPATH override or copied solver equations. Raised the project minimum to that version.

### Completed native slice

`HorizontalEllipseBarrelDefinition` requires finite positive span > rise, concrete
pipe material and explicit `physical_inlet="square_edge_headwall"`. Adapter uses
public `HorizontalEllipseGeometry`, HDS-5 Table A.2 Chart 29 Scale 1 inlet and Table
C.2 entrance loss. JSON/TOML parsing, project JSON export and crossing audit records
preserve axes and physical inlet. Existing result JSON preserves sources and warnings;
summary CSV retains scalar/warning fields and must accompany project/detailed JSON.
HW/D assessment uses rise. Existing circular and box dispatch is preserved.
Vertical, steel/aluminum, HDPE, other inlets and HY-8 oval project integration remain
unsupported. TUFLOW demos remain circular-only. Existing MCP project CLI discovery
is sufficient; no additional tool is warranted.

Fourteen new tests cover independent ellipse area, published coefficient/source
values, independent HDS-5 A.3 SI submerged headwater, warning preservation, rise-based
HW/D rejection, native forward/inverse, project round-trip and unsupported/missing
configuration. The 1 m3/s headwater regression is 1.556387336974289 m; inverse Q
returns 1 m3/s within 1e-6. Numerical regression tolerances are **not** agreed HY-8
engineering parity tolerances.

### Native executable and source acceptance ? unexecuted

Read-only installed executable inspection: `C:/Program Files/HY-8 8.00/HY864.exe`,
file/product version **8.0.1.2**, SHA-256
**0b1c5e7fb6b48be9c675b5486ae9a84346f436c5c649aa5ed09a1552267ee806**.
It was **not launched**: explicit execution permission and authorised matched
same-model input paths were requested and remain pending. No new model sources were
supplied. No model search beyond the selected repository sources, production edits,
new forward/inverse HY-8 results or blockage acceptance is claimed. The ELM outlet
control versus RAM inlet-control fallback matrix above remains the required contract.

### Local checks and delivery checkpoint

- **Passed:** focused `python -m pytest tests/culvert tests/scripts/test_tuflow_culvert_wrappers.py -q`:
  **175 passed, 2 skipped**. Skips are existing Windows symlink privilege checks.
- **Passed:** Ruff lint/format and strict Pyright on the six changed Python files.
- **Passed:** documentation checker default README set and changed prose with `--links-only`;
  plain prose-path mode flags existing generated-output filenames and a branch name,
  so those are not represented as broken links.
- **Passed:** `python repo-scripts/build_library.py --skip-pip`, verified retained wheel
  **26.11.8.3**, SHA-256 **3b03107d07919542f8fe26163e99faf62d77ec7a5603d1bf5ec6dc0aca2a17d2**.
  Initial build failed for absent QGIS styles; initialised only required qgis-resources
  (plus tests/test_data for pytest), then build passed. No unsorted access.
- **Passed:** installed retained wheel with `--no-deps --force-reinstall`, ran
  `repo-scripts/smoke_test_installed_wheel.py` outside checkout. Existing vendor/simil
  ComplexWarning is retained; smoke exits zero.
- **Passed:** copied `ryan-scripts/culvert.py` outside checkout, installed site-packages
  import verified, synthetic horizontal ellipse solve exits zero with CSV/JSON/Markdown.
  Raw input/output/stdout/stderr retained privately under local temp directory
  `wp15-copied-wrapper-ty81q8gj`. Wrapper 2026-11-07.1 / library 26.11.8.3.
- **Failed, corrected:** initial new test API assumptions (area property, inverse float,
  fixed-tailwater normalization and crossing depth access); final focused run above passes.
- **Unexecuted:** real model provenance, HY-8 numerical acceptance, engineering tolerances,
  ELM/RAM blockage cases. No Python checks are counted as those acceptance gates.
- **Delivery:** one locally validated batch on existing draft #282; hosted final-head
  CI and fresh Codex review are recorded in the PR after push. No merge or release.
