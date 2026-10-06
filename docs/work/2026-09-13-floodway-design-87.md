# Floodway design and reporting workflow

| Field | Value |
| --- | --- |
| Status | Needs review: floodway validation passed; repository-wide failures remain |
| Owner | Unassigned |
| Created | 2026-09-13 |
| Updated | 2026-10-06 |
| Next review | 2026-10-13 |
| Branch | `feature/floodway-design-87` / PR #88 |
| Baseline | Local merge `2d978b6` incorporates `origin/main` (`beb1011`); 0 commits behind main |

## Outcome and scope

Deliver issue #87: a maintained floodway overtopping design-assessment and reporting workflow using `ryan-culverts` as
the authoritative crossing-hydraulics engine and `ryan-tools` for formation response, protection/design checks,
event envelopes and reporting.

The prerequisite roadway API is no longer blocked:

- `ryan-culverts` PR #13 is merged;
- upstream issues #4 and #14 are closed;
- the vendored `ryan_culverts` revision on this branch points to the merged roadway-overtopping implementation;
- `ryan-tools` PR #86 / issue #80 is merged and provides the shared culvert workflow boundary.

Debris impact/loading and debris blockage remain future scope.

Hydrograph/overtopping-duration and road-closure-duration analysis is **not part of this floodway formation-design
workflow**. It is tracked separately in issue #93 because it requires a different time-series workflow, scripts and
reporting contract.

## Current implementation

PR #88 now contains substantive Python implementation rather than research-only material:

- typed floodway A-F formation, hydraulic-state, applicability, demand, envelope and protection result models;
- adapter from authoritative `ScenarioResult` / roadway segment results without re-deriving roadway-weir hydraulics;
- irregular roadway profile support through the merged `ryan-culverts` public API;
- MRWA Equation 4/6 surface-velocity primitives;
- MRWA Equation 7 limiting-velocity calculation;
- source-bounded Figure 4.5 plunging/surface-flow transition relation;
- Figure 4.6 `K` relation reconstructed from the guide's own Equation 3 + Equation 6 energy relation rather than
  hand-digitised;
- event-envelope selection retaining independent governors for unit discharge, velocity, dynamic pressure and momentum flux;
- configurable peak-discharge sweeps from overtopping onset through the selected maximum, with solver submergence detection;
- explicit project-configured 2D-verification escalation that preserves supported scalar results while flagging spatial limitations;
- explicit preservation of the MRWA `D/H < 0.76` and Appendix C/D `D/H = 0.8` source thresholds;
- MRWA Table 5.1 dumped-rock class/thickness selection kept distinct from HEC-23 enhanced protection;
- current MRWA 2023 floodway requirements retained as source-labelled project-level report guidance;
- HEC-23 DG5 overtopping-riprap equations and worked-example regression tests;
- strict versioned floodway formation JSON/TOML configuration;
- JSON, CSV and Markdown scenario/envelope outputs with retained governing hydraulic evidence;
- maintained `ryan-scripts/floodway.py` human-facing wrapper and MCP workflow catalogue entry;
- focused test coverage across calculations, configuration, sweep, reporting, wrapper and the culvert/floodway boundary.

Unsupported zone mechanisms continue to fail closed rather than receiving invented scalar methods.

## MRWA source verification — 2026-10-06

The authoritative 2006 MRWA Floodway Design Guide was visually re-checked for Figures 4.5 and 4.6 and Appendix D.

### Figure 4.6

Figure 4.6 does not require hand digitisation.

Using the guide's simplified free-flow relation

```text
q = 1.69 H^(3/2)
```

and Equation 6 no-loss energy relation with

```text
V = K sqrt(H)
delta = delta_p / H
```

gives the dimensionless equation

```text
1 + delta = K^2/(2g) + 1.69/K
```

The larger positive root is the Figure 4.6 high-velocity branch. The reconstructed values reproduce the published
Appendix D graph reads within graph-reading precision:

| delta_p/H | Published graph read | Reconstructed |
| ---: | ---: | ---: |
| 0.104 | 3.50 | ~3.48 |
| 0.150 | 3.70 | ~3.68 |
| 0.307 | 4.20 | ~4.22 |
| 0.318 | 4.25 | ~4.25 |

The implementation enforces the displayed Figure 4.6 domain `0 <= delta_p/H <= 1.8` and refuses extrapolation.

### Figure 4.5

Figure 4.5 remains a graphical source and has been visually digitised over its displayed `H/l` domain. The
implementation uses bounded linear interpolation and refuses extrapolation.

Independent Appendix D checks support the digitisation:

- Seven Mile Creek transition: `H/l = 0.90/9.0 = 0.10`, with `(D/H)trans ~= 0.60`;
- Majors Creek transition: `H/l ~= 1.48/9.0 = 0.164`, with `(D/H)trans ~= 0.67-0.68`.

### Figure 4.2

Figure 4.2 is not required for the production crossing flow split because `ryan-culverts` remains authoritative for
roadway/culvert hydraulics. It should only be automated if an explicit MRWA legacy-capacity reproduction path is later
required. The floodway design workflow must not create a competing production crossing solver.

## Architectural boundary

### `ryan-culverts` owns

- common headwater/tailwater solution;
- culvert/roadway flow split;
- irregular roadway crest integration;
- roadway submergence correction;
- local roadway unit discharge and segment-state provenance.

### `ryan-tools` owns

- floodway formation geometry and material/protection metadata;
- MRWA formation-response calculations;
- A-F zone demand and applicability assessment;
- enhanced sourced protection checks such as HEC-23 DG5;
- event-envelope and governing-state selection;
- specialist/2D escalation;
- floodway reporting and exports.

## Remaining first-increment work

Implementation and focused validation for the supported first increment are complete locally. PR #88 remains draft
and unmerged. The validation repairs, documentation updates and dated package version are now committed on the PR branch.

Next action: reconcile the branch with the latest `main`, review the five repository-wide test failures described below,
and then decide review readiness. No enabled GitHub CI workflow exists; local validation is the delivery evidence and
absence of CI is not a blocker.

## Explicit fail-closed boundaries

The first increment does not invent numerical methods for:

- downstream-shoulder suction/uplift without a validated pressure relationship;
- complete seepage/piping/internal-erosion analysis;
- complex downstream toe scour/impingement where a bounded analytical method has not been adopted;
- debris impact/loading or debris blockage;
- spatial hydraulic effects that require 2D verification.

These states remain `SOURCE_DATA_REQUIRED`, `SPECIALIST_REVIEW_REQUIRED`,
`OUTSIDE_SOURCE_RANGE` or `TWO_D_VERIFICATION_RECOMMENDED` as appropriate.

## Validation and delivery record

### 2026-10-06

- Confirmed PR #88 is open, mergeable and 0 commits behind `main`.
- Confirmed `ryan-culverts` PR #13 is merged and issue #14 is closed.
- Posted a current-status comment to PR #88.
- Raised issue #93 for the separate hydrograph/overtopping-duration workflow.
- Visually verified MRWA Figures 4.5 and 4.6 against the authoritative guide.
- Added bounded Figure 4.5 interpolation with Appendix D anchors.
- Reconstructed Figure 4.6 analytically from MRWA Equations 3 and 6 and added Appendix D regression anchors.
- Routed free-flow pavement/downstream-batter assessment through the source-backed Equation 4/7 velocity limit where
  sufficient geometry is available.
- Added `crest_flow_length` to formation geometry for Figure 4.5 `H/l` classification.
- Added focused tests for Figure 4.5/4.6 relations and plunging-versus-surface routing.
- Preserved the Section 4.4.3 `D/H < 0.76` and Appendix C/D `D/H = 0.8` thresholds as separate source rules.
- Visually verified MRWA Table 5.1 and implemented dumped-rock class/thickness selection with source-vector tests.
- Added source-labelled current MRWA 2023 pavement/protection extent, trafficability, containment and relief-culvert guidance.
- Added depth, Froude number, velocity head and an independent unit-discharge envelope governor.
- Added a configurable peak-discharge sweep with overtopping-onset and solver-submergence searches; this is explicitly
  separate from hydrograph/time-series analysis in issue #93.
- Added strict floodway formation JSON/TOML configuration and the maintained `ryan-scripts/floodway.py` wrapper.
- Added JSON/CSV/Markdown hydraulic/protection provenance, MCP workflow discovery and focused wrapper/config/sweep tests.
- Added explicit `two_d_verification_reason` formation configuration and propagated the resulting
  `TWO_D_VERIFICATION_RECOMMENDED` status/message through scenario, envelope, JSON/CSV and Markdown outputs.
- Full repository Ruff/Pyright/pytest/docs/build validation has **not yet been run** and is explicitly delegated to a
  separate validation agent; local focused validation is now recorded above.

### 2026-09-14

- Refreshed against post-#86 `main`.
- Added the calculation specification and validation-vector pack.
- Reviewed the shared culvert workflow boundary and package placement.

### 2026-09-13

- Opened the research-first branch/PR and established the source baseline and architectural boundary.

### Independent validation follow-up - 2026-10-06

Validated `ff77c9bd947ddc5590ab775d8ec7d9c3636aee3e` plus local repairs using normal Python 3.14.6.

- Fixed the near-overtopping-onset sweep exception: Figure 4.6 out-of-range inputs now produce
  `OUTSIDE_SOURCE_RANGE` zone results, preserving other assessments and separate HEC-23 evidence.
- Added a focused regression for both pavement and downstream-batter source-range handling.
- Corrected test type narrowing and an invalid assertion that differently dimensioned quantities must have different
  numerical values; retained independent formula checks.
- Applied Ruff safe import/export sorting and formatting, used pairwise interpolation, and split pavement input
  validation to meet the existing complexity limit. No unsafe automatic fixes were used.
- Added five missing central-index links and formatted source URLs as Markdown autolinks.
- Changed Python from `git diff --name-only origin/main...HEAD -- '*.py'`: `python -m ruff check` and
  `python -m ruff format --check` passed; `python -m pyright` passed with zero errors/warnings in strict project mode.
- `python -m pytest tests/floodway tests/culvert tests/mcp -q`: **124 passed** after final repairs.
- `python -m pytest tests -q`: **969 passed, 4 skipped, 5 failed**, with two CRS warnings. This is not a full-suite pass.
  Failures are three footprint mocks in `tests/orchestrators/gdal/test_raster_maintenance_coverage.py` expecting
  positional arguments where production passes keywords; the missing-utility log assertion in
  `tests/orchestrators/tuflow/test_project_setup.py`; and the string-versus-Path raster script test in
  `tests/scripts/raster/test_raster_scripts.py`. Those test and production files are unchanged against `origin/main`.
  Isolated rerun reproduced the three GDAL and raster failures; the TUFLOW log test passed alone, indicating
  suite-order interaction. No baseline checkout reproduction was performed; unrelated source/test files were preserved.
- `python repo-scripts/check_documentation.py` and explicit changed-document `--links-only` checks passed.
- Markdown lint: `python -m pymarkdown -d MD013 scan` on changed Markdown passed. The default 80-column MD013 rule
  is excluded explicitly for this check because repository prose/tables use longer lines; default lint fails on that
  formatting convention. No repository lint configuration was changed.
- `python repo-scripts/check_loguru_formatting.py`, wrapper compilation and copied-wrapper `--help` passed.
- `python repo-scripts/build_library.py --no-bump --skip-pip`: passed and retained version `26.9.14.1` without
  modifying `pyproject.toml`. Candidate verification preceded wheel promotion.
- After the user queried the old date, ran `python repo-scripts/build_library.py --skip-pip` successfully:
  declared version is now `26.10.6.1`; the verification rebuild above was superseded by this dated build.
- Final wheel: `dist/ryan_functions-26.10.6.1-py3-none-any.whl`, 659876 bytes,
  SHA-256 `952e018b7539894476226eb8aa914dac4b9ff6e678067b4d974a0c9920a894cb`.
- Temporary `pip --target --no-deps` install, isolated interpreter import-root checks via
  `repo-scripts/smoke_test_installed_wheel.py --expected-root`, and a copied wrapper outside the checkout passed.
  Existing user-site runtime dependencies were explicitly made available; bundled packages resolved under the
  temporary target. Copied wrapper checked help, a five-point synthetic sweep/export, and missing-directory exit 1.
- The ordinary installed package is stale (`python ryan-scripts/floodway.py --help` cannot import the culvert models).
  It was not replaced; the rebuilt wheel was verified separately in temporary storage.
- GitHub checks confirmed draft/open/unmerged state, no review threads and no workflow runs on the validated head.
  The checkout has no enabled CI workflow. The validation repairs and documentation/package updates were subsequently
  committed and pushed to PR #88; the PR remains draft and unmerged.
- The five full-suite failures are outside the floodway change surface and are tracked separately in issue #95. Four
  deterministic failures are in test/production files byte-identical to `main`; the TUFLOW logging test passes in
  isolation and appears order-dependent. These are not currently demonstrated regressions caused by PR #88.
- The repository improvement backlog review date remains overdue (2026-09-20); unrelated work was not started.

### Merge reconciliation - 2026-10-06

- User authorized resolving the in-progress merge while retaining the reviewed current implementation.
- Incoming `beb1011` has exactly the same tree as previously integrated `1d8aa17`:
  `26aa3a58e4cfe2fe32e14c7d9b88e35634e1a138`. The conflicts resulted from changed commit ancestry, not new source work.
- Retained current versions of every conflicted file and the `0213eac` culvert pin; removed the incoming old wheel and
  duplicate stale culvert index entry. No submodule worktree was modified.
- Resolved index tree exactly matched pre-merge `fa4e784`:
  `272f382b66ad6d582debbdca6fc37e04296a3627`. The local merge commit preserves that tree unchanged.
- Ruff lint/format, strict Pyright on conflicted Python, documentation index checks and dated-wheel verification passed.
- `python -m pytest tests/floodway tests/culvert tests/mcp -q`: **124 passed** (13.83 seconds).
- No source changes warranted rerunning the full suite; the five previously recorded failures remain unresolved.
- Local merge commit: `2d978b6` (`Merge main while preserving validated floodway implementation`), parents
  `fa4e784` and `beb1011`. `git rev-list --count HEAD..origin/main`: **0**.
- Merge complete, no unresolved paths, version remains `26.10.6.1`. Nothing pushed; PR #88 remains draft and unmerged.
- This status-record/register update remains unstaged and uncommitted; the merge itself is committed locally.
