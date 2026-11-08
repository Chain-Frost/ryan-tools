# WP15 — culvert / HY-8 engineering evidence and ellipse integration gates

| Field | Value |
| --- | --- |
| Status | Authorised assumed circular EG11 comparison executed with limitations; source-equivalent #102/#103 acceptance remains blocked |
| Owner | WP15 allocation agent / maintainer |
| Created / updated | 2026-11-08 / 2026-11-09 |
| Next review | 2026-11-15 or earlier if blockers resolve |
| Issues | [#102](https://github.com/Chain-Frost/ryan-tools/issues/102), [#103](https://github.com/Chain-Frost/ryan-tools/issues/103), [#101](https://github.com/Chain-Frost/ryan-tools/issues/101) |
| Allocation | [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) / WP15 |
| Assessed refs | main `8de77a67fb614ad32cd3c151a338f23eeb871412`; PR #251 `dc6d74455e092d3611b975f0fc3975a977578ecc` |
| Delivery | #282 integrated into draft #251 at 63c7da77. New scoped docs/wp15-assumed-eg11-20261109 branch preserves prior evidence and adds conditional executable results; no parent/main merge. |

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

**Next action (superseded by continuation below):** reconcile #282 with current #251 and validate the integrated head. EG11 source traces and HY-8 permission are now available, but physical material/inlet mapping, ELM/2026.3 evidence, and accepted numerical tolerances remain open. Review by 2026-11-15.


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


## 2026-11-09 integration and source-trace checkpoint

**Git reconciliation:** this checkpoint preserves draft #251 parent `93169e809ebd596c04741d71cc7139f42c94433d` and the bounded WP15 source delta from `2bbbae3b2ba95a45767af5bac7ca74d422f70c57`, with the current parent controlling all unrelated files. The newer parent dependency, Python type/stub configuration and reduced CI are retained; only the published native solver minimum is raised to `ryan-culverts>=26.10.19.1`. New combined development version: `26.11.9.1`. A wheel for that combined version has **not** been locally built or smoke-tested in this GitHub connector session; exact-head hosted package validation remains mandatory. No merge into `main` is authorised.

**New authentic TUFLOW source evidence:** pinned tutorial-data `d2dae3ec` supplies EG11 RAM files in `tuflow/example_input_preflight/examples.zip`, including the active TCF, TEF, ECF, `1d_nwk` shapefile and `Matrix_Blockages.csv`. A previous local agent [traced identical-channel EOF and matrix data](https://github.com/Chain-Frost/ryan-tools/pull/282#issuecomment-6720722253):

| Channel | Nominal opening | Event blockage | EOF effective dimension |
| --- | --- | --- | --- |
| `FC04.1_C`, three circular barrels | diameter 1.2 m | 20% and 50% | diameters 1.073 and 0.849 m |
| `FC01.1_R`, rectangular | width 1.8 m | 50% and 70% | widths 0.900 and 0.540 m |
| `FC01.2_R`, rectangular | width 2.4 m | 70% and 99% | widths 0.720 and 0.024 m |

These EOF dimensions already incorporate the source blockage; **do not reapply the same blockage percentage** to these effective values. This is source evidence from **TUFLOW 2025.2.1-iSP-w64**, not direct acceptance of 2026.3, ELM, inlet-control ELM-to-RAM fallback, an executed 100%-blockage case, or HY-8 hydraulic parity. There is no matching Maximums XLSX or proven physical material/inlet mapping in that source; Manning `n=0.015` and entry loss `0.5` do **not** identify material or inlet. Preserve source and output hashes and do not fabricate a mapping.

**HY-8 permission:** explicit maintainer [approval for Windows HY-8 8.0.1.2 execution on disposable copies](https://github.com/Chain-Frost/ryan-tools/pull/282#issuecomment-6720767040) is now established. There is no need to request that same permission again. The executable was previously verified read-only at `C:/Program Files/HY-8 8.00/HY864.exe`, version `8.0.1.2`, SHA-256 `0b1c5e7fb6b48be9c675b5486ae9a84346f436c5c649aa5ed09a1552267ee806`. This WP15 checkpoint did **not** execute HY-8; source-specific numerical validation still requires true physical inlet/material and representative matched configurations.

**Validation ownership:** historical 175 Windows focused passes/2 privilege skips, Ruff/Pyright on six Python files and wheel/copied-wrapper checks apply to child `2bbbae3b` only. Its historical hosted red CI reflects a stale moving-parent merge scope, not demonstrated regressions in the WP15 diff. The reconciled merge head still requires new complete CI, fresh Codex review and final installed-wheel smoke. #101 bounded code is present, but do not close #101–#103 while their remaining criteria are unverified.


## WP15 independent Windows engineering acceptance — 2026-11-09

**Overall: BLOCKED — physical inlet/material unproven.** Task A is **PASS WITH LIMITATIONS** for the selected historical EG11 RAM dimension trace. Task B is **NOT RUN / BLOCKED**. No new HY-8 or TUFLOW execution, accepted Python regression replay, upstream synthetic comparison, production edit, release, issue closure or merge was performed.

### Revision and environment

Reviewed PR source: `b18e9a0c1b99dc52d188d6d1b097c7bac3e04143`; pinned test-data source: `d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58`. This is source-data inspection, not a new hydraulic test of that code revision. Windows 10 build 19045, normal Python 3.14.6 (`C:/Program Files/Python314/python.exe`), PyShp 3.1.0. Installed package inventory: ryan-tools 26.11.8.8, run-hy8 2026.10.18.2, ryan-culverts 26.10.19.1; none was used to claim hydraulic acceptance.

Live API reports #282 **closed/merged**, merge commit `63c7da77676b9ad44e20e7f1729b738985d400cc`; its narrative still says unmerged. This pre-existing state was observed, not changed here. Existing dirty WP15 worktree was preserved; inspection and documentation use a separate detached checkout of the reviewed head. Existing [CI run 42501881704](https://github.com/Chain-Frost/ryan-tools/actions/runs/42501881704) is successful; no jobs were rerun.

HY-8 read-only verification: `C:/Program Files/HY-8 8.00/HY864.exe`, file/product **8.0.1.2**, SHA-256 `0b1c5e7fb6b48be9c675b5486ae9a84346f436c5c649aa5ed09a1552267ee806`. Disposable execution remains authorised, but the source-equivalence gate is unmet.

### Independent source/event trace

Read raw Git blobs at the pinned commit; copied only selected archive members and matching EG11 output files to unique scratch. No submodule checkout or source modification was needed.

Archive `tuflow/example_input_preflight/examples.zip`:
- `runs/EG11_~e1~_~e2~_004.tcf`: matrix ON, method RAM, default A; reads `model/EG11_004.ecf`, `model/Matrix_Blockages.csv`, `runs/EG11_004.tef`.
- ECF reads `model/gis/1d_nwk_EG11_004_L.shp`. Exactly three records; all `Ignore=0`, character `pBlockage` categories B/C/D, blank `Inlet_Type`.
- TEF: Q100 = ARI 100 years; QPMF = ARI 2000 years; 2hr ends at 3 hours. The TCF default names Q100, but retained QPMF TLF explicitly records `-e1 QPMF -e2 2hr`; both selected TLFs confirm RAM, correct ARI, default A, **Blockage Override = Not Set**, and the selected ECF/network.
- Retained EOF/TLF build is **2025.2.1-iSP-w64**, not 2026.3. Output root: `tuflow/TUFLOW_Example_Model_Dataset/EG11/`; logs under sibling `log/`; channel CSVs under `EG11/plot/csv/`.

| DBF record (1-based) / channel | Category / barrels | Nominal dimensions (m) | Q100 matrix → output %; EOF effective (m) | QPMF matrix → output %; EOF effective (m) |
| --- | --- | --- | --- | --- |
| 1 / FC04.1_C | B / 3 | diameter 1.2 | 20 → 20; D 1.073 | 50 → 50; D 0.849 |
| 2 / FC01.1_R | C / 3 | width 1.8, height 1.8 | 50 → 50; W 0.900, H 1.800 | 70 → 70; W 0.540, H 1.800 |
| 3 / FC01.2_R | D / 5 | width 2.4, height 1.2 | 70 → 70; W 0.720, H 1.200 | **100 → 99**; W 0.024, H 1.200 |

Independent circular calculations: `1.2*sqrt(0.8)=1.0733126292` and `1.2*sqrt(0.5)=0.8485281374` m. EOF minus calculated values: -0.0003126292 and +0.0004718626 m, both within printed 0.001 m rounding (±0.0005 m). Rectangular calculations `W*(1-output_pct/100)` give 0.900/0.540 and 0.720/0.024 m. **6/6 dimension checks pass**; this rounding check is not an engineering hydraulic tolerance. EOF culvert rows are lines 135–137; channel-definition rows 59–61 and corresponding `_1d_Chan.csv` corroborate output percentages.

**Correction/qualification to earlier shorthand:** category D at ARI 2000 requests 100 in the matrix, while this retained build reports 99. The residual 1% width is observed evidence, not proof of complete closure or a universal cap rule. Category E also contains 100 but is not assigned to these three records. No executed 2026.3 complete-closure acceptance is established.

These EOF dimensions already contain the observed RAM reduction. Applying the percentage again would reduce area twice (remaining-area factor squared). Nominal circular HW/D denominator remains 1.2 m; rectangular nominal rises remain 1.8/1.2 m. The EOF circular `Diameter or Width` column holds D; its `Height` is `-----`, so do not treat that field as a nominal circular diameter.

### Physical configuration and missing evidence

Source lengths: 20/7/10 m and US/DS inverts: 43.35/42.75, 44.6/44.5, 37.6/37.5 m, respectively in DBF order above. All rows specify n=0.015, entry=0.5, exit=1.0, form=0; EOF confirms n and entrance/exit/fixed coefficients and marks entrance/exit losses **Adjusted**. Inlet-contraction parameters are height=0.6, width=0.9. None establishes a physical inlet or material or an equivalent HY-8 loss treatment.

Selected TCF/TEF/ECF/TBC/TGC, boundary database, active network and referenced boundary/material DBF attributes were inspected. No explicit culvert material or physical inlet mapping exists there. `Materials.csv` describes 2D land cover (pasture/roads/buildings/water/vegetation), not pipe construction. Network `Inlet_Type` is blank for all three records, with no material field. Boundary links are SX/CN into the 2D domain; an external HQ boundary is not a fixed culvert tailwater. Time-dependent node heads are retained, but no source-equivalent steady HY-8 boundary/loss/roadway specification is established. No matching EG11 Maximums XLSX was found in the pinned TUFLOW tree or archive.

To unblock: supply an authoritative channel-ID mapping to actual construction material and physical inlet details; reconcile TUFLOW adjusted loss/contraction treatment with that geometry; supply the matching Maximums export and selected Q/headwater/tailwater times plus roadway/boundary geometry; agree engineering comparison tolerances before parity decisions. For #103 also supply genuine 2026.3 ELM evidence covering outlet-control ELM versus inlet-control RAM fallback, numeric/blank defaults/overrides and complete blockage. This RAM-only three-channel network does not supply the required pit/ignored-feature cases.

Task B forward/inverse Maximums scenarios, network HW/D 1.5/2.0, and HY-8 Q/HW/HW-rise/barrel-road split/outlet velocity/control/warning comparisons: **NOT RUN / BLOCKED**, not passing skips. No raw HY-8 project/results exist from this inspection. #102/#103 remain incomplete.

### SHA-256 provenance

Values below hash **unmodified raw Git blob/archive-member bytes**, not text rewritten by a Windows checkout.

| Source | SHA-256 |
| --- | --- |
| examples.zip | `e374c8e072679fc6c4dc11e63b270caa1101b6aebb84d70060872cdd1b0aa7d6` |
| TCF | `4580565687282e45204e631110c4f6cfab425e15941971379046b8c5b3d257f4` |
| TEF | `c9dca9344c2b942ac7b18aaa94d6792f1ca79ac65493300dd9071b346543fe81` |
| ECF | `70b4d7b669cc9a6c2dec7a67588ba938ef685f8bf2b7f109471718ed9caf9cf5` |
| Matrix_Blockages.csv | `4aca443eb35f7ccc24f7e76963984c5b9eaef1b7d485fd92617626160e662638` |
| network DBF | `cdb344c9e6e36324e1d55a4a9027efba852856492d53c6fb59ae707cee9bec13` |
| Q100 EOF | `5d277223a726ef51b856e33e86a3a82b57ce819a4f2a450c9260ba0ca810568e` |
| QPMF EOF | `d0f6cf31bb9a4dc8b8602dc509a59f192c2b05fba35b73c851a772e78d7f24c0` |
| Q100 TLF | `ef9732faf8540c60b1d67164e2f6dd37b59a41bd0b29bfd6efa96a7d54a32302` |
| QPMF TLF | `2015548aa533986ee3c462c0ee0da58c7935ce29c1d798b2fa0519f3ba9c0dcb` |
| Q100 channel CSV | `27e158e72fab7ec877072a29b718aff8113d19978b735cc0b931d3beec128bab` |
| QPMF channel CSV | `8fa6ae89c8c63244b9f1c2f6b8948215d1f9f4a843e0e92885413fd7012ea5da` |

Earlier EOF hashes `f789b265…` / `b74ebc9f…` are reproduced exactly by LF→CRLF conversion of these blobs; this explains the difference without asserting changed hydraulic content.

Local evidence retains selected source/output bytes, complete SHA-256 manifest, network/ancillary attribute JSON, environment inventory and six-row numerical CSV in a unique scratch directory. Documentation update is kept on an isolated detached checkout for coordination; no branch push into the already-merged child or moving parent is made.



Next review: 2026-11-15 or when the maintainer supplies physical material/inlet and matched boundary/loss evidence. The earlier permission and pending-CI statements are historical; execution is authorised, existing exact-head CI is green, and the current blocker is source equivalence.

Delivery checkpoint: [acceptance report posted to PR #282](https://github.com/Chain-Frost/ryan-tools/pull/282#issuecomment-6724523011). Default documentation checker (7 files and central-index coverage), explicit links-only checks (2 files), and git diff --check pass. All 28 retained output blobs rechecked against pinned Git source. Initial verification-manifest lookup used Windows separators and was corrected to POSIX keys; source bytes were unaffected. Documentation-only local commit retained in E:/Github/ryan-tools-wp15-source-acceptance; no branch push. Raw local evidence: C:/Users/Ryan/AppData/Local/Temp/wp15-independent-eg11-_ano6do8.


## WP15 conditional EG11 RCP/headwall comparison — 2026-11-09

**PASS WITH LIMITATIONS for authorised disposable execution; full #102/#103 source-equivalence acceptance remains BLOCKED.** The [explicit hypothesis](https://github.com/Chain-Frost/ryan-tools/pull/282#issuecomment-6724651602) is adopted only for circular FC04.1_C. Construction material/inlet remain unverified by source. No rectangular-to-circular substitution, Maximums workbook invention, 2026.3 ELM claim or engineering tolerance was introduced.

### Exact sources and operating assumptions

Parent source `63c7da77676b9ad44e20e7f1729b738985d400cc`; matching installed verification wheel ryan-tools **26.11.9.1**, SHA-256 `4a296287add1fd972507ea49153a2b91e6a7bab5376ac9f5597cde781fec5021`. Normal Windows 10/Python **3.14.6**, run-hy8 **2026.10.18.2**, ryan-culverts **26.10.19.1**. The wheel is installed into a separate target, with physical import origin recorded, without a source checkout override or new virtual environment.

Read-only reverified executable `C:/Program Files/HY-8 8.00/HY864.exe`, file/product **8.0.1.2**, SHA-256 `0b1c5e7fb6b48be9c675b5486ae9a84346f436c5c649aa5ed09a1552267ee806`. All executions use a fresh unique temp workspace.

Pinned EG11 source remains `d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58`, separate from WP04's newer fixture pin. The [independent 6/6 dimension/source-hash report](https://github.com/Chain-Frost/ryan-tools/pull/282#issuecomment-6724523011) is retained. All **46** selected source-copy hashes were rechecked unchanged. Matrix SHA-256 `4aca443eb35f7ccc24f7e76963984c5b9eaef1b7d485fd92617626160e662638`; network DBF `cdb344c9e6e36324e1d55a4a9027efba852856492d53c6fb59ae707cee9bec13`.

- Explicit assumed material/inlet: concrete RCP, square-edge headwall.
- Retained source n=**0.015**, entry=**0.5**, exit=**1.0**, form=**0**; 3 barrels, length 20 m, inverts **43.35/42.75 m**.
- Nominal diameter **1.2 m**, used for HW/D. Hydraulic diameters computed once: Q100 20% RAM → **1.0733126292 m**; QPMF 50% → **0.8485281374 m**. Printed EOF 1.073/0.849 m is audited, not reduced again.
- **The actual source width-contraction factor is 0.9 and inlet-contraction height is 0.6. Both adapters explicitly reject width factor 0.9.** Four fail-closed probes (two events × two engines) confirm this before any solver run. Executed scenarios explicitly assume width factor **1.0**, standard physical-inlet losses and no separate 0.6 height-contraction effect. EOF `Adjusted` entrance/exit treatment is not reproduced. These unsupported source effects are recorded, not silently dropped.
- Assumed forward Q=**1 m³/s** with constant TW=**42.75** or **43.25 m**. Inverse targets **45.15/45.75 m** correspond to nominal HW/D **1.5/2.0**, with assumed TW=43.25 m. The internal scenario label “submerged” means elevated TW by 0.5 m; it does **not** mean the whole outlet is submerged.
- HY-8's adapter suppresses roadway overtopping with an **assumed crest 93.35 m**, roadway width/stations 10 m, matching the native absence of roadway flow. This is not recovered source roadway geometry.

### Numerical results

Twenty successful public-API solves form **10 paired scenarios**. Units: Q m³/s, headwater elevation m, outlet velocity m/s. Values below are native / HY-8.

| Event / case | Q | Headwater | Outlet velocity |
| --- | --- | --- | --- |
| Q100 forward, TW 42.75 | 1 / 1 | 43.768785 / 43.78 | 2.778556 / 2.76 |
| Q100 forward, TW 43.25 | 1 / 1 | 43.768785 / 43.78 | **0.806953 / 2.76** |
| Q100 inverse nominal HW/D 1.5 | 7.868701 / 7.88 | 45.150000 / 45.14 | 4.461215 / 4.46 |
| Q100 inverse nominal HW/D 2.0 | 9.786829 / 9.82 | 45.750000 / 45.75 | 4.733694 / 4.74 |
| QPMF forward, TW 42.75 | 1 / 1 | 43.814824 / 43.82 | 2.845965 / 2.84 |
| QPMF forward, TW 43.25 | 1 / 1 | 43.814824 / 43.82 | **0.961545 / 2.84** |
| QPMF inverse nominal HW/D 1.5 | 5.237586 / 5.24 | 45.150000 / 45.14 | 4.182898 / 4.18 |
| QPMF inverse nominal HW/D 2.0 | 6.376602 / 6.40 | 45.750000 / 45.76 | 4.404832 / 4.40 |
| QPMF coincident sample treated as steady: forward | 0.199 / 0.20 | 43.573909 / 43.57 | 0.119691 / 0.12 |
| QPMF coincident sample treated as steady: inverse | **0.149541 / 0.05** | 43.563600 / 43.56 | 0.089943 / 0.03 |

QPMF sampled inputs come from the **same 1.283333 h row** in retained Q/H CSVs: Q=**0.199**, upstream H=**43.5636**, downstream H=**43.5542**. They are an instantaneous transient snapshot, not a proven steady boundary. Q100 sampled circular flow is zero throughout its retained Q series; no positive source flow was invented. Its 1 m³/s cases are openly assumed.

Maximum paired |ΔHW| = **0.011214738 m**. Nominal HW/D-target inverse discharge differences are **+0.0461% to +0.3669%** (HY-8 minus native). These are observations, not pass thresholds. Elevated-tailwater velocity differences reach **1.953047 m/s** and remain unresolved. The sampled inverse differs by **−66.5643% Q**; run-hy8's existing internal **0.01 m headwater search stop** plus printed result precision is not an agreed discharge/parity tolerance. No tighter retry, coefficient tuning or excluded failure was used to manufacture agreement.

All roadway flows are zero under the stated artificial roadway assumptions. Total barrel-group Q equals reported total Q; per-barrel Q is explicitly derived as group Q/3 and retained in CSV. Native regimes: inlet-control unsubmerged for assumed forward cases, inlet-control submerged for HW/D targets, outlet-control free-surface for the sampled pair. HY-8 raw flow codes: **1-S2n**, **5-S2n**, **1-S1t**, respectively. Native warning `representative_barrel_equal_flow` occurs in all ten native solves; Python warning capture is empty. Raw report qualifiers/profile data are retained, without pretending the two control vocabularies are identical.

Readback of emitted HY-8 projects confirms concrete/headwall, **3 barrels and n_top=n_bottom=0.015**. SI→stored-unit→SI diameter values are 1.0733126616/0.8485281480 m; these are conversion precision differences, not catalogue snapping. Inverts/length and constant tailwaters were also audited.

### Retained evidence and remaining gates

Raw **29 each** `.hy8/.rst/.rsql/.plt` files (inverse searches produce multiple trials), all source/executable/package hashes, warning captures, serialized-input audit, parsed RST, normalized 20-row results and 10-row differences are retained. Public sanitized numerical CSVs and relative artifact SHA-256 manifests accompany the canonical WP15 record in the follow-up branch. Local workspace: `wp15-assumed-eg11-bn23q1oo`.

**Not run / still blocked:** genuine Maximums workbook scenarios, true physical provenance, equivalent source contraction/adjusted-loss/roadway and steady-boundary treatment, agreed hydraulic tolerances, 2026.3 ELM outlet-control/inlet-control-RAM fallback and full blockage. The category-D QPMF source matrix still requests 100 while historical outputs apply 99; no general cap rule or full-closure acceptance is claimed.

The prior local evidence commit `9fcefd6c` was preserved and cherry-picked onto a new scoped branch from #251, without pushing the merged #282 branch. No production edits, issue closure, parent/main merge, auto-merge or release. #101/#102/#103 remain open.



Numerical artifacts: [20 normalized results](../validation_data/wp15-eg11-assumed-20261109/results.csv), [10 paired differences](../validation_data/wp15-eg11-assumed-20261109/differences.csv), [source hashes](../validation_data/wp15-eg11-assumed-20261109/source-sha256.json), [raw artifact hashes](../validation_data/wp15-eg11-assumed-20261109/artifact-sha256.json), and [HY-8 input readback](../validation_data/wp15-eg11-assumed-20261109/serialized-input-audit.json). Next review 2026-11-15 or on source/loss/tolerance resolution.
