# TUFLOW culvert engine migration: final validation

| Field | Value |
| --- | --- |
| Status | Blocked |
| Owner | Unassigned |
| Created | 2026-10-08 |
| Updated | 2026-10-08 |
| Next review | 2026-10-15 |
| Baseline | `feature/issue-99-tuflow-culvert-engines` at `4839e8bd3fe7dc1d9b05557624211aa32a8be92f` |

## Outcome and scope

Finish validation of [PR #100](https://github.com/Chain-Frost/ryan-tools/pull/100), following the
[mapping contract](../TUFLOW_CULVERT_ENGINES.md). Keep merge manual. External numerical verification is tracked in
[issue #102](https://github.com/Chain-Frost/ryan-tools/issues/102); source blockage verification is tracked in
[issue #103](https://github.com/Chain-Frost/ryan-tools/issues/103).

## Current state

- The supplied handoff reports implementation threads resolved and the deferred HDPE thread closed.
- Hosted run [149](https://github.com/Chain-Frost/ryan-tools/actions/runs/37745967656) is terminal at the baseline:
  Hosted tests passed (1037 passed, 5 deselected, 63 warnings); Policy and Package failed.
- Local fixes replace the Maximums finite-positive-flow lambda with vector comparisons and type the two test reader
  stubs. Strict Pyright now reports zero errors. Ruff formatting was applied to the reader stubs.
- The stale retained wheel differed from pinned `culvert_solver/__init__.py`. Rebuilt version `26.10.8.3` verifies
  against the source and passes isolated installed-wheel smoke validation.
- User confirms reduced-area blockage with unadjusted nominal diameters as the intended design-acceptance assumption.
  This decision does not establish the provenance of any particular EOF dimensions or active blockage matrix.
- User requires Windows HY-8 numerical verification before merge; it is not deferred.
- Synthetic executable comparisons completed, but representative Maximums/EOF and mixed-network verification is
  blocked on missing source inputs. No numerical equivalence or general engineering acceptance is claimed.

## Next action

Locate a representative Maximums workbook, mixed `1d_nwk` source layer and corresponding active event/blockage matrix.
The user supplied `tests/test_data/tuflow`; it contains EOF outputs, no XLSX workbooks, and no `1d_nwk` layer in any of
its 162 GeoPackages. No named `1d_nwk` source file or blockage matrix was found. Preserve the fixture submodule.

Complete issue #102's wrapper-level scenarios and workspace safeguards on real inputs, verify nominal/effective
blockage evidence under issue #103, and agree numerical tolerances for the engineering use. Then obtain explicit
commit/push instructions, publish the local fixes and retained wheel, and confirm every hosted job passes on that head.

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
  **134 passed**, zero skips or deselections (5.90 seconds).
- `python repo-scripts/build_library.py --skip-pip`: transactional build passed, version `26.10.8.2` to `26.10.8.3`.
- `python repo-scripts/verify_wheel.py`: passed; SHA-256
  `9e60cda4e8e9b768fa21e8eb4dcf2c79cbcffc90187a562804d99a1e69d725cf` (691666 bytes).
- Isolated `pip install --no-deps --target` and `smoke_test_installed_wheel.py --expected-root`: passed outside the
  source tree. Both wrappers' `--help` passed against that isolated install. Normal installed package remains stale;
  a direct wrapper invocation without the isolated installation failed to import `tuflow_attributes`.
- Loguru policy passed. No unrelated full local suite or application-environment checks were run.
- Final documentation/link and diff checks accompany this record.
- Parent changes are unstaged/uncommitted; no push, external comment or merge performed. Version metadata, replacement
  wheel, two Python files and this indexed handoff are the delivery set.
- Dependency refs: `vendor/run_hy8` at `0baaa2ac83e38aaad1a0419dc6e39c40445cb98d`, `vendor/ryan_culverts` at
  `2adea6abdb715993ba06c2487a12da9a7207f335`, fixture submodule at
  `277b08e8821f60dad740635f1c7bbe730c418880`. Fixture worktree was clean; submodule contents were not changed.

## Windows numerical evidence

Executable: `C:/Program Files/HY-8 8.00/HY864.exe`, file/product version **8.0.1.2**.
Raw HY-8 projects/reports, per-engine JSON, comparison script and aggregate JSON are retained locally under
`C:/Users/Ryan/AppData/Local/Temp/pr100-hy8-numerical-cblw4r5h` (temporary storage; archive before cleanup).

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

### 2026-10-08

Reconciled terminal hosted CI, repaired seven strict typing errors and rebuilt/isolated-smoked the retained artifact.
Ran the installed HY-8 executable for 15 synthetic pairs. Recorded the user's blockage assumption and requirement to
complete numerical verification before merge. Inspected the supplied fixture location read-only and identified the
missing representative source inputs. The older culvert-workflow register entry is stale (last update 2026-09-14);
reported without resuming that independent work front.
