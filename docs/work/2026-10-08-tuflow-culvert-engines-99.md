# TUFLOW culvert engine migration: final validation

| Field | Value |
| --- | --- |
| Status | Blocked |
| Owner | Unassigned |
| Created | 2026-10-08 |
| Updated | 2026-10-17 |
| Next review | 2026-10-24 |
| Baseline | `feature/issue-99-tuflow-culvert-engines`; repairs committed and pushed in `5ec678b` |

## Outcome and scope

Finish validation of [PR #100](https://github.com/Chain-Frost/ryan-tools/pull/100), following the
[mapping contract](../TUFLOW_CULVERT_ENGINES.md). Keep merge manual. External numerical verification is tracked in
[issue #102](https://github.com/Chain-Frost/ryan-tools/issues/102); source blockage verification is tracked in
[issue #103](https://github.com/Chain-Frost/ryan-tools/issues/103).

## Current state

- Integration repairs remove duplicate project metadata and parenthesize exception tuples in the attribute reader
  and `1d_nwk` wrapper for the declared Python 3.13 minimum. These defects are absent from current main `037cff3`.
- Retained wheel `26.10.17.1` replaces the two conflicting retained versions. Build, source verification and isolated
  installed-wheel smoke pass. SHA-256: `df815600f722ddac66ba3df2921130d7c9f3bd33808d25722d6dd2981cef24ef`.
- Full local suite: **1226 passed, 12 skipped** in 146.01 seconds. Skips require Windows symlink privileges.
  Focused attribute/configuration/engine/wrapper tests: **98 passed**; wrapper tests repeated after its repair:
  **59 passed**. Ruff lint/format and strict Pyright pass across all 15 PR Python files.
- Generated a Maximums workbook using the maintained merge orchestrator and Module 03 fixture outputs. Crossing
  `FC04.1_C` completed all four scenarios in both native and executable HY-8 engines with positive results.
  Explicit concrete/square-edge-headwall settings are validation assumptions, not verified physical source metadata.
- Real mixed-network inputs and active blockage provenance remain unavailable. A read-only scan of the supplied
  fixture's 162 GeoPackages / 950 layers found no `nwk` layer or `pBlockage` field. No named network source or
  blockage file, or blockage control command in the available TCF/TBC/TEF/TRD files, was found.
- Hosted checks at old head `23cd600` all failed: four on duplicate TOML keys and one on Python 3.13 syntax.
  Repairs are committed and pushed in `5ec678b`. New hosted verification is underway; no final-head review is claimed.
  Keep merge manual.

### Historical 2026-10-08 state

- The supplied handoff reports implementation threads resolved and the deferred HDPE thread closed.
- Hosted run [150](https://github.com/Chain-Frost/ryan-tools/actions/runs/37754684194) at the baseline:
  Policy passed; Package failed at retained-wheel verification; Hosted tests were still running at the latest check.
  The 1037 passing hosted tests from run 149 apply to the older `4839e8b` head.
- Local fixes replace the Maximums finite-positive-flow lambda with vector comparisons and type the two test reader
  stubs. Strict Pyright now reports zero errors. Ruff formatting was applied to the reader stubs.
- Head `45e972c` advances `vendor/run_hy8` to `28e7909afd5ae53c4357380ef70c5f2e482c917d` (elliptical support,
  upstream PR #7), making retained wheel `26.10.8.3` stale. Rebuilt version `26.10.8.4` verifies against current source
  and passes isolated installed-wheel smoke validation. The replacement wheel and metadata remain uncommitted.
- User confirms reduced-area blockage with unadjusted nominal diameters as the intended design-acceptance assumption.
  This decision does not establish the provenance of any particular EOF dimensions or active blockage matrix.
- User requires Windows HY-8 numerical verification before merge; it is not deferred.
- All 15 synthetic executable comparison pairs were repeated against `28e7909`; the 30 results are unchanged for
  discharge, headwater, velocity, flow type and status. Representative Maximums/EOF and mixed-network verification is
  blocked on missing source inputs. No numerical equivalence or general engineering acceptance is claimed.

## Next action

Confirm all five hosted checks on the published repaired head. Obtain the mixed `1d_nwk` source and matching
model/blockage configuration for issues #102/#103;
the Maximums workbook can now be generated from supplied results and is no longer a missing-input blocker.
Complete remaining inlet/material/loss and workspace checks, agree numerical acceptance tolerances and request final
review on the published repair. Do not merge this PR or dependent run-hy8 PR #6 automatically.

## Completion criteria

- Retained wheel and metadata delivered, with verification and isolated installed smoke passing.
- Ruff, strict Pyright, relevant tests and every hosted CI job pass on the final published head.
- Windows HY-8 checks in issue #102 completed with raw evidence and accepted comparison tolerances.
- Source blockage method and nominal dimensions verified for the selected engineering inputs.
- PR remains open until separately authorized manual merge.

## Validation and delivery

Local environment: Windows, normal Python 3.14.6, 2026-10-08.

- Changed-file Ruff lint and format checks cover the 15 first-party Python files in `origin/main...HEAD`.
- `python -m pyright` on those files uses the repository strict configuration: zero errors, warnings or informations.
- Focused pytest across TUFLOW attributes/configuration/engines, uncertainty workflow, wrappers and MCP registry:
  **134 passed**, zero skips or deselections (5.12 seconds), repeated after the dependency update.
- `python repo-scripts/build_library.py --skip-pip`: transactional build passed, version `26.10.8.3` to `26.10.8.4`.
- `python repo-scripts/verify_wheel.py`: passed; SHA-256
  `56d2573ae626267de8433b19fa25dfa050f5e5362d5a5cf850b40b0f08626756` (699341 bytes).
- Isolated `pip install --no-deps --target` and `smoke_test_installed_wheel.py --expected-root`: passed outside the
  source tree for `26.10.8.4`. Both wrappers' `--help` previously passed against the `26.10.8.3` isolated install.
  Normal installed package remains stale;
  a direct wrapper invocation without the isolated installation failed to import `tuflow_attributes`.
- Loguru policy passed. No unrelated full local suite or application-environment checks were run.
- Final documentation/link and diff checks accompany this record.
- The previous fixes/handoff and wheel were committed in `dd25469`; the dependency update is committed in `45e972c`.
  Current replacement wheel, version metadata and handoff/register updates remain unstaged/uncommitted. No push or
  merge performed. PR description refreshed under the user's instruction with the current head and dependency refs,
  local verification, pending publication and representative-data blockers.
- Dependency refs: `vendor/run_hy8` at `28e7909afd5ae53c4357380ef70c5f2e482c917d`, `vendor/ryan_culverts` at
  `2adea6abdb715993ba06c2487a12da9a7207f335`. The parent pins fixture commit
  `277b08e8821f60dad740635f1c7bbe730c418880`, but its independent checkout is now at
  `b604b20ea62a61f1aa8e4837dc7e4ebd155430a4` (only `.gitignore` differs; fixture contents unchanged).
  This pre-existing submodule pointer difference was preserved and must not be staged with the wheel repair.

## Windows numerical evidence

Executable: `C:/Program Files/HY-8 8.00/HY864.exe`, file/product version **8.0.1.2**.
Raw HY-8 projects/reports, per-engine JSON, comparison script and aggregate JSON are retained locally under
`C:/Users/Ryan/AppData/Local/Temp/pr100-hy8-28e7909-c0c9026bbb9c4965975004ea8486a5b7`
(temporary storage; archive before cleanup). The earlier comparison used `0baaa2a` and remains under
`C:/Users/Ryan/AppData/Local/Temp/pr100-hy8-numerical-cblw4r5h` as historical evidence.

The 15 scenario pairs use concrete square-edge headwall, CSP thin-edge projecting and CSP square-edge headwall.
Each uses three barrels, diameter 1.2 m, length 40 m, inverts 10/9.5 m, source Manning n 0.013 for concrete and
0.025 for CSP, and no arbitrary loss overrides. Scenarios: forward Q=6 m3/s at tailwater 10 and 9.5 m;
inverse headwater 11.8 m at tailwater 10 m; inverse HW/D 1.5 and 2.0 at tailwater 9.5 m.

All 30 engine evaluations produced positive finite discharge, headwater and outlet velocity and valid/success status.
Maximum observed absolute differences: discharge 0.0925411 m3/s, headwater 0.0100001 m, velocity 0.0081513 m/s.
Maximum discharge relative difference was 0.9742%; velocity 0.2271%. These are descriptive results, not pre-agreed
acceptance tolerances. HY-8 tabular rounding and inverse interpolation may contribute; that attribution is not proven.
Flow-type strings were retained but not semantically reconciled. Wrapper-level real-data, explicit loss overrides,
blockage and retained-workspace safeguards remain outside this synthetic numerical check.

## Progress

### 2026-10-17: integration repair and fixture-derived Maximums verification

Normal Windows Python 3.14.6; parent dependencies are run-hy8 `8f2e5b5070204df7c6c23984ed75d5e3107124e5`,
ryan-culverts `403f2ed5883daa518e52d92b01b33887c7d2e172` and test data
`b604b20ea62a61f1aa8e4837dc7e4ebd155430a4`. No submodule edits made.

Validation commands: `python -m pytest tests -q -ra` (1226 passed, 12 symlink-privilege skips);
`python -m pytest tests/culvert/test_tuflow_attributes.py tests/culvert/test_tuflow_configuration.py
tests/culvert/test_tuflow_engines.py tests/scripts/test_tuflow_culvert_wrappers.py -q -ra` (98 passed), then wrapper-only
repeat (59 passed). Changed-PR-file Ruff lint/format and strict Pyright pass; Loguru policy and default documentation
checks pass. `ast.parse(..., feature_version=(3, 13))` passes for 192 library files and the repaired wrapper;
native Python 3.13 is unavailable locally. Wrapper help passes with source PYTHONPATH; normal installed package
without that override lacks the attribute module. Isolated new-wheel smoke passes outside the repository.

`python repo-scripts/build_library.py --skip-pip` advances 26.10.16.2 to 26.10.17.1 and removes both old retained wheels
(26.10.13.3 and 26.10.16.2). `python repo-scripts/verify_wheel.py` verifies 722342 bytes and the hash above.
`pip install --no-deps --target <temporary-directory>` followed by `smoke_test_installed_wheel.py --expected-root`
passes. All package/source/documentation changes remain unstaged and uncommitted; no push or merge performed.
PR #100 description updated on GitHub with the current published head, local repair evidence, current dependency
pins and remaining engineering blockers. Its implementation summary and dependent merge order are preserved.

Artifacts: `C:/Users/Ryan/AppData/Local/Temp/pr100-verification-20261017/` contains generation scripts/logs, the Module 03
Maximums workbook, `module03-native.csv`, `module03-hy8.csv` and retained `hy8/` workspaces. The generation script calls
`tuflow_culverts_merge.main_processing` with Module_03/results and an external output directory; fixture files remain
untouched. An earlier Module 05 export demonstrated missing node-head joins and was replaced by the Module 03 workbook.
Full-suite output is `C:/Users/Ryan/AppData/Local/Temp/pr100-full-tests.log`. Archive temporary evidence before cleanup.

Module 03 `FC04.1_C`: three barrels, diameter 1.2 m, length 23.9 m, inverts 43.35/42.75 m, Manning n 0.012,
entry/exit/fixed losses 0.5/1.0/0.0; pBlockage 0. Explicit physical selection: concrete/square-edge-headwall.
Both wrappers' engine paths complete Q at DS_h, Q at invert, inverse at US_h and inverse HW/D 1.5.
All eight results are positive with valid/success status. Maximum differences: flow 0.01715851335 m3/s,
headwater 0.00367772681 m and velocity 0.00750987776 m/s. Flow-type labels are retained, not semantically reconciled.
Relative to HY-8, maximum inverse discharge difference is 0.18138%, velocity 0.16326% and headwater depth above
upstream invert 0.22930%. Prescribed forward discharge agreement is by construction, not independent flow evidence.
Per-scenario differences are saved as `module03-comparison.csv` beside the raw outputs.
These are descriptive observations, not agreed acceptance tolerances or confirmation of the model's physical inlet.
Mixed-network inverse HW/D 1.5/2.0 and blockage-model evidence remain incomplete; no issue closed.

The unrelated floodway work record is overdue (2026-10-13); reported without resuming that work.

The user authorized committing and pushing these repairs on 2026-10-17. Delivery proceeds on the existing PR branch;
merge remains unauthorized. Final hosted verification must use the resulting published head.
Repairs and wheel pushed in `5ec678b`; initial Python CI run `39035931139` and fast checks run `39035931188` started.
This delivery-state documentation follow-up also requires final-head hosted verification.

### 2026-10-08: run-hy8 reference update

Rebuilt and verified `26.10.8.4` against `28e7909`, isolated-smoked the installed wheel, repeated all 15 synthetic
comparison pairs with zero engine failures, and confirmed results unchanged across the reference update. Repeated
changed-file Ruff/strict Pyright and focused tests successfully. CI run 150 has Policy passing and Package failing;
Hosted tests remain pending terminal confirmation. Representative input and processed-dimension provenance blockers
remain. Preserve the independent fixture pointer change and keep PR #100 open.

### 2026-10-08

Reconciled terminal hosted CI, repaired seven strict typing errors and rebuilt/isolated-smoked the retained artifact.
Ran the installed HY-8 executable for 15 synthetic pairs. Recorded the user's blockage assumption and requirement to
complete numerical verification before merge. Inspected the supplied fixture location read-only and identified the
missing representative source inputs. The older culvert-workflow register entry is stale (last update 2026-09-14);
reported without resuming that independent work front.
