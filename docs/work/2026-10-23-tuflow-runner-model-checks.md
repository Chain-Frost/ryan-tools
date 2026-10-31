# TUFLOW input checking and runner integration

| Field | Value |
| --- | --- |
| Status | Review candidate: complete example regression passes with eight exact pinned-PyTUFLOW allowances; upstream fix tracked in #181 |
| Owner | ChatGPT |
| Created / Updated | 2026-10-23 / 2026-10-31 |
| Next review | Windows acceptance / 2026-11-07 |
| Pull request | [ryan-tools #159](https://github.com/Chain-Frost/ryan-tools/pull/159), open and non-draft when checked |
| Branch / delivery base | `feat/tuflow-runner-model-checks`; regression delivery based on `f780d1d243867f2a867bd2511164ba37ed809f09`; prior cleanup baseline retained below |
| Delivery | Regression and CI publication on #159, upstream fix in PyTUFLOW #60; no main merge, parser repin or release |
| Next action | Review final-head CI, merge PR #159 independently of upstream when approved; issue #181 removes the allowance and requires 161/161 clean after adopting the reviewed fix. |

## Current scope and decisions

## Post-review correction — 1 November 2026

- CSV Resume keys use path-independent `tuflow-logical-v2`: TCF filename
  (case-insensitive for Windows) plus effective event/scenario, model and engine
  flags. TCF directory, child cwd and solver executable location remain readable
  diagnostics only. Old pre-release v1 completion keys remain in the CSV but do
  not match new v2 runs; review planned reruns after upgrading.
- A Windows Python-TCF consistency comparison can recognize the current
  logon session's mapped drive/UNC alias using `WNetGetConnectionW`. This
  lookup does not open the TCF or change its execution path. Unknown mappings
  remain strict mismatches. No network mapping is needed for CSV identity.
- Reject distinct TCF files with the same name and flags in a single batch,
  rather than silently sharing a completion record. Preserve engine flag
  distinctions (including test-only `-t`).
- TXT BAT quoting allows safely quoted CMD metacharacters such as `&`,
  parentheses and pipe in input values, but still rejects unsafe expansion
  syntax. Current BAT files are assumed to be modern Windows-created text;
  do not add a legacy codepage compatibility setting.
- The input-preflight guide now documents the exact eight-finding temporary
  CI allowance rather than incorrectly saying the Windows job is red.

## Pre-upstream-merge known-failure policy (31 October 2026)

PR #159 is intended to merge **before** PyTUFLOW PR #60 is adopted. The full
native Windows archive replay continues to inspect all 161 TCFs / 174 contexts.
A temporary, exact-signature allowlist in
`tests/orchestrators/tuflow/test_example_models_real.py` recognises only
the seven documented 1D BC-schema findings and the one RF-triangle Name finding
from pinned PyTUFLOW `05e35476`. It checks the model, finding count,
attribution, status, kind, origin, layer, source filename and diagnostic text.
Any extra/changed failure, incomplete run or unexpectedly passing previously
broken model fails CI; no model is skipped, no assertion is bypassed, and
complete JSON/CSV evidence is still uploaded. See [issue #181](https://github.com/Chain-Frost/ryan-tools/issues/181)
for removing the allowance and requiring 161/161 clean after adopting the
reviewed upstream fix. No parser repin or TUFLOW execution occurs here.


**31 October 2026 follow-up:** The owner clarified that imported BAT/TXT is
never executable control and that duplicate model selections must be discarded.
This supersedes older scope-pack statements preserving source launch directories,
source executables or duplicate occurrences. Windows is the primary target;
the POSIX absolute-executable START parsing correction is compatibility-only.
The normal installers still pin PyTUFLOW at `05e35476`. Generic PyPI
dependency resolution cannot guarantee the required API until upstream releases
it; no package release is authorised by this PR.

Scopes 01-04 are integrated together in #159. The earlier component PRs and
interim benchmark records do not define the current implementation. The
[scope index](2026-10-31-tuflow-integration-scopes/00_scope_index.md) records the
shared contracts; the owner's later decisions below take precedence.

- Consume the accepted public PyTUFLOW API from immutable
  `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`. Both normal installation routes use
  the same source requirement, GIS bootstrap and post-install verification.
- Input checker findings are terminal-only by default. An explicit report path
  enables an exclusively created issue CSV.
- The Python runner is the sole launcher. BAT/TXT files are inert input data
  supplying model selections and resolved TCFs; their executable, CD/START
  directory, engine, GPU and scheduling commands cannot control process launch.
  Only Python settings determine the solver, working directory and runtime policy.
  Before writing a CSV RESET/START, actual execution checks the configured solver.
  Identical model selections are discarded, keeping first source provenance. The earlier abbreviated-input P1 classification is withdrawn;
  the owner clarified the authoritative Python setting instead.
- Both command readers use a captured inherited environment and independent
  state per input file. TCF conflicts fail before writes or execution.
- Resume regenerates the full plan and filters only the current launch queue.
  Successful CSV history is retained. Sparse original run indices are valid for
  preflight and presentation.
- Refresh one `<wrapper-stem>.txt` command export beside the supplying Python
  file. TXT is intentional protection against accidental double-click execution.
  Refresh the full plan even when Resume is complete. No timestamped exports,
  backup files or session directories. Never overwrite imported sources or model,
  wrapper or history files, including aliases.
- The runner and command export target Windows. GPU arguments use individual
  `-puN` tokens; comma-separated tokens are rejected. External GPU inspection
  remains once at actual startup, warning-only, with no later activity scans.
- Install Loguru and Colorama explicitly in the focused Windows CSV workflow,
  matching the maintained runner/dashboard imports.
- Use maintained Loguru and live-dashboard helpers; keep subprocess ownership,
  durable completion callbacks and CSV history in their current domain-specific
  boundaries. Share pure argument interpretation without changing valid history
  keys as a side effect of extraction.
- Compose each grouped finding with its original `PreflightIssue` rather than
  copying a reduced set of fields. Preserve public findings and source provenance.
- Remove the unused flags-only parser and 55 retired investigation/evidence files
  added by this workstream. Current user guidance and the scope pack remain.

The new import policy intentionally normalizes engine switches before history
identity is computed. An older imported command can therefore match differently
even when normalization only moves an engine switch. Existing CSV rows remain;
an affected run is pending again. Once normalized, changes to ignored source
engine/GPU switches do not affect identity. No old-parser compatibility shim
or CSV migration is introduced.

Background advisory validation uses one upstream I/O worker to avoid an executor
pool extending interpreter shutdown. Explicit checker/check-only operations
retain their normal parallel settings.

## Ownership

| Boundary | Maintained implementation |
| --- | --- |
| Editable settings, CLI, banners and pause | User-facing checker and runner under `ryan-scripts/TUFLOW-python/model_management` |
| Run parsing and pure argument interpretation | `ryan_library/functions/tuflow/command_plan.py` |
| Public PyTUFLOW validation | `ryan_library/orchestrators/tuflow/input_preflight.py` |
| Multi-TCF grouping and command provenance | `ryan_library/orchestrators/tuflow/command_preflight.py` |
| Scheduling, export and advisory lifecycle | `ryan_library/orchestrators/tuflow/simulation_runner.py` |
| Durable continuation history | `ryan_library/functions/tuflow/execution_history.py` |
| Startup process/GPU snapshot | `ryan_library/functions/tuflow/gpu_occupancy.py` |

The standalone activity tool remains independent. Raster-header/CRS work in
[PyTUFLOW PR #59](https://github.com/Chain-Frost/PyTuflow/pull/59) remains outside
this ryan-tools release and does not restore a local raster validator.

## Validation and delivery

Verified on 2026-10-31 using Linux Python 3.14.8, the real accepted PyTUFLOW
archive at `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`, and temporary validation
dependencies. No TUFLOW solver or production model was launched.

| Check | Result |
| --- | --- |
| Combined focused pytest on Linux | 298 passed, 6 native-Windows cases skipped |
| Windows 3.14 CSV and runner acceptance on 8a65a53 | 69 passed, no skips; installed copied-wrapper help and generated-path export passed |
| Strict Pyright 1.1.414, Python 3.14 / Windows target | 21 changed/new Python files; 0 errors, 0 warnings |
| Ruff check / format check | Passed; all 21 files already formatted |
| Repository Loguru formatting policy / Python compilation | Passed for the same 21 files |
| Wheel verification build | Passed; version 26.10.31.1 unchanged |
| Installed wheel and copied wrapper smoke | Passed from a neutral directory without checkout imports |
| Documentation | Seven default documents and eight touched status/guide files checked; remaining-file index coverage passed |
| Whitespace / deletion references | Passed; all 55 retired files absent and no remaining Markdown references found |

The focused test command was:

```bash
PYTHONPATH=/tmp/ryan-tools-pr159-py314-deps /root/.local/bin/python3.14 -m pytest -q -rs \
  tests/functions/tuflow/test_command_plan.py \
  tests/functions/tuflow/test_execution_history.py \
  tests/functions/tuflow/test_gpu_occupancy.py \
  tests/functions/tuflow/test_pytuflow_compatibility.py \
  tests/orchestrators/tuflow/test_input_preflight.py \
  tests/orchestrators/tuflow/test_command_preflight.py \
  tests/orchestrators/tuflow/test_command_preflight_real.py \
  tests/orchestrators/tuflow/test_simulation_runner.py \
  tests/scripts/tuflow/test_command_preflight_wrapper.py \
  tests/scripts/test_run_tuflow_simulations.py \
  tests/scripts/test_tuflow_advisory_runner.py \
  tests/scripts/test_tuflow_csv_continuation.py \
  tests/scripts/test_tuflow_preflight_opt_in.py \
  tests/repo_scripts/test_install_latest_wheel.py \
  tests/repo_scripts/test_install_from_pypi.py \
  tests/scripts/test_codex_mcp_setup.py \
  tests/mcp/test_registry.py
```

Modified-only static checks derive the file set from tracked changes plus new
Python files, excluding deleted files and submodules. The temporary strict
configuration is `/tmp/ryan-tools-pr159-modified-pyright.json`; its 21-file list
is `/tmp/ryan-tools-pr159-modified-python.json`. Includes are relative to the
configuration directory; import paths cover the repository, vendor, installer
helpers and temporary dependencies. No diagnostic rule was relaxed. An earlier
combined typing invocation with invalid absolute include paths was interrupted
and is not counted; the corrected bounded check above completed successfully.

Build command:

```bash
PYTHONPATH=/tmp/ryan-tools-pr159-py314-deps /root/.local/bin/python3.14 repo-scripts/build_library.py --no-bump --skip-pip
```

The verified wheel is `dist/ryan_tools-26.10.31.1-py3-none-any.whl`, SHA-256
`be47b71fd2ce81721a99003573d21d0561462eee78099e1e6fdbb5da6c969368`.
The copied wrappers came from a fresh installation of that wheel. The import
origin was the temporary installed target, and the public capability guard
confirmed actual PyTUFLOW 1.2.7 from the pinned archive. Wheel installation used
`--no-deps` only to isolate this verification target; the normal end-user
installation remains dependency-aware.

The real two-run checker reported the deliberately missing dependency, returned
the expected failure status, and created no default CSV. An explicit report
created only its requested CSV. Export-only returned success twice, refreshed
the same wrapper-named TXT, used Python `BCMD=-b`, discarded imported
`-t/-x/-nlc/-pu7`, and retained the model option `-et1`. No solver, history CSV,
BAT export, timestamped output or leftover temporary output was created.
Temporary evidence is at
`/tmp/ryan-tools-final-wheel-smoke-i39xzrim/smoke-results.json`.

The six Linux-skipped cases concern Windows command-path export and associated
CSV failure handling. They ran successfully in the 69-test
[Windows CSV acceptance job](https://github.com/Chain-Frost/ryan-tools/actions/runs/41203093209)
on delivered implementation commit `8a65a53842a74eb527fd07c6e95a719498cecfb3`.
That job also passed strict typing, built and installed the wheel, and copied the
runner outside the checkout. Help and generated-path export succeeded with an
absent solver, producing the fixed TXT and no history CSV. The Linux smoke used
a portable bare executable token. No real solver or licensed production model
was launched. The complete normal Windows installer was not run end to end;
installer sequencing/failure tests, offline pip resolution and the real installed
PyTUFLOW acceptance cover its changed components.

Documentation checks use the normal seven-document checks and explicit
`--links-only` checks for touched guides/status files. During the original unstaged validation, the stock central-index check still
saw nine deleted Markdown files in the Git index. Checking its findings against
the exact deletion set confirmed every remaining document was indexed. The
publication tree includes those deletions; the normal index check is required
before delivery and passed after staging the exact publication tree. No checker
rule was relaxed.

Only the required `tests/test_data` and `qgis-resources` submodules were
initialized for testing and packaging. Their pinned revisions are unchanged.
The new installer regression test is included in this delivery. After the
initial local-only handoff, the owner explicitly clarified that the work must
be delivered to GitHub. The complete tested cleanup is published together on
#159, using the latest remote commit as its parent and a checked fast-forward
branch update. No PR merge or release is included.

## Published CI and typing follow-up

The first cleanup commit is
[`8a65a53`](https://github.com/Chain-Frost/ryan-tools/commit/8a65a53842a74eb527fd07c6e95a719498cecfb3).
Its GitHub tree exactly matched the verified local publication tree. The Windows
CSV checks passed as recorded above. The
[public PyTUFLOW workflow](https://github.com/Chain-Frost/ryan-tools/actions/runs/41203093223)
also passed 25 public-validation tests against the installed accepted archive,
plus its separate same-version incompatible-package rejection job.

The separate command-plan job failed collection because its minimal environment
did not install Loguru, now imported by shared execution history. The follow-up
adds only that required dependency to its install command. The runner-focused
workflow already installs Loguru and Colorama.

The initial Hosted suite also reported seven empty-log-capture assertions
(1,563 passed; seven deselected). An earlier ASC workflow leaves the
`ryan_library` Loguru namespace disabled across tests. Reproducing that state
with the preceding logger tests produced the same seven failures. The two
runner capture fixtures now explicitly enable the namespace before attaching
their sink; all behavioral assertions remain. The identical reproduction then
passed all 16 selected cases. This correction is limited to test capture setup.
Check the latest PR checks for the follow-up result; do not substitute
earlier-head results for a new head.

The owner queried deletion of the old PyTUFLOW stub. The
[retired stub](https://github.com/Chain-Frost/ryan-tools/blob/820358bb399296879c736887e0893249025a1de2/typings/pytuflow/__init__.pyi)
only describes the former TCF context and deferred-reference consumer interface.
It does not describe public batch validation and was removed before this cleanup.
Pinned `05e35476` and current PR17 head `76486e82` expose the same annotated
validation modules, but neither ships a `py.typed` marker or stub files. The
consumer retains narrow Protocols and casts; strict checks validate that local
contract, not complete upstream typing conformance. Keep the obsolete stub
deleted. Upstream packaging/type-surface verification is a separate improvement,
with no consumer pin change in this task.

## Concurrent integration checkpoint

While this cleanup was being verified, remote #159 advanced to documentation-only
commits `624189c3f97a44f690a2c4cd210a07e99e4d7d7a` and
`5810fba8def32aa3570c0684cfa7d99c18caae25`. They change only this status
record and the work-register row; no implementation changed from local baseline
`5edc2f5371da3a55faec4cbab2115e43574f1c43`.

The [remote checkpoint](https://github.com/Chain-Frost/ryan-tools/blob/5810fba8def32aa3570c0684cfa7d99c18caae25/docs/work/2026-10-23-tuflow-runner-model-checks.md)
preserves the restored native merge history: task PRs #170, #172, #173, #174,
#176 and #177 are merged into #159, and all nine checks passed on implementation
head `5edc2f5`. #159 remained open against main. This concise record preserves
that delivery evidence without carrying forward retired investigation logs.
Those earlier checks do not validate this cleanup. Both concurrent documentation
edits have been reconciled into this current record and its register row. New
checks must be assessed on the delivered commit; no main merge or release is
included.

### Complete example-model regression — 2026-10-31

Added locally on `test/pr159-example-model-regression` in
`E:\Github\ryan-tools-pr159-example-regression`, based on #159 head
`f780d1d243867f2a867bd2511164ba37ed809f09`. Changes are unstaged and uncommitted;
the existing main checkout and test-data pin `d2dae3ec` are unchanged.

The new Windows regression verifies the archive and every one of its 1,464 members,
extracts once outside the submodule, and derives contexts from all 21 simulation BAT
files through the production planner. The creation utility is excluded; duplicate
aggregate contexts are deduplicated and unlisted TCFs get no-flag coverage.
The per-TCF counts exactly match the 161-TCF / 174-run historical inventory.
The fixture-only reader removes the two aggregate batches' fixed executable guard;
it does not extend production shell support.

Normal Windows Python 3.14.6 used the real immutable parser `05e35476` installed
with `--no-deps` into the external target `E:\Github\pr159-example-validation-deps`.
Its direct-url provenance and physical import origin were confirmed. Native GDAL
3.13.3 was available. No solver or project BAT was launched.

Full replay: **all 174 runs completed; 153/161 TCFs and 166/174 runs passed**.
Eight one-run cases fail, intentionally retained as ordinary test failures:

- EG02_013, EG04_004/005/006 and EG14_001/002/003: seven unresolved attribute
  findings, `Unsupported GIS BC/SA/RF layer schema`. The pinned parser's
  `validation_links.py` recognises `2d_bc/sa/rf` layer names in this path and rejects
  these `1d_bc` sources. OGR confirms their fields are Type, Flags, Name and Description
  (the Shapefile truncates the latter to Descriptio). These are coverage findings,
  not demonstrated missing source files or proven obsolete TUFLOW syntax.
- EG03_009: rainfall GIS source `2d_rf_EG03_009_R.shp` contains a blank Name;
  the public API reports `Missing or unexpanded BC Name`. OGR confirms the blank
  first-row Name; no input was repaired or waived.

The local process also emitted h5py native DLL exception diagnostics (`0xc0000139`)
and projection-reader warnings but continued and completed the replay. This is a
local environment limitation, not a clean native-environment acceptance claim.

Evidence: `E:\Github\pr159-example-reports-20261031\results.json` and 161 issue CSVs.
The exact replay command used the external parser target via process-local
`PYTHONPATH` and `TUFLOW_EXAMPLE_REPORT_DIR`, then
`python -m pytest tests/orchestrators/tuflow/test_example_models_real.py -q --tb=short`:
**1 passed, 1 failed**, 29.50 seconds. No xfail, finding suppression or baseline change.
The dedicated public-validation workflow now installs native GIS readers, runs this
test explicitly and uploads reports even on failure. Hosted execution has not run
for these uncommitted changes. See the [regression instructions](../TUFLOW_INPUT_PREFLIGHT.md#regression-coverage).

The BAT inventory plus existing command-plan suite passes **106 tests**.
Ruff lint/format and modified-file strict Pyright pass (0 errors, 0 warnings).
Default documentation/index checks, explicit links for all three touched guides/records,
workflow YAML loading/replay-artifact assertions and `git diff --check` pass.
Final-source replay repeats the same eight failures: **1 passed, 1 failed** in
27.80 seconds; all 174 runs completed, 153 TCFs / 166 runs passed. Final JSON and
161 issue CSVs are at `E:\Github\pr159-example-reports-20261031-final`;
the captured output is `E:\Github\pr159-example-replay-final.log`.
Actionlint is unavailable locally, so full Actions expression validation remains
for CI. No library or package metadata changed; no wheel rebuild is required.

Next action: review the eight findings against the pinned upstream parser and
obtain a clean Windows CI replay. Parser changes or a pin update require separate
implementation and fresh validation; this task adds regression coverage only.

### Investigation of the eight findings — 2026-10-31

Read-only reruns through the same pinned `05e35476` consumer reproduce all eight
findings. Inspection of the original control commands, DBF rows and selected
database records identifies two validator coverage defects, rather than evidence
of obsolete syntax or missing project data. No parser or model data was changed.

**Seven 1D boundary cases:** `validation_links.py` only recognises layer names
matching `^2d_(bc|sa|rf)_` for BC commands; `validation_gis.py` also lacks 1d_bc
positional schema support. The affected ECFs use ordinary `Read GIS BC` with
Type/Flags/Name/Description fields, matching the current
[TUFLOW 1D boundary specification, Table 8.4](https://docs.tuflow.com/classic-hpc/manual/2026.3/07-Boundaries-and-Initial-Conditions.html).
The file and DBF exist, but the validator rejects the schema before discovering
these selected BC source files. Direct consumer calls show each source below is
absent from `checked_paths`; manual selection of its active database rows and the
same pinned parser's bounded CSV header inspectors confirms all selectors pass:

| TCF | Selected 1D BC keys | Expected source CSV absent from actual checked paths |
| --- | --- | --- |
| EG02_013, EG14_001 | DS_BC, FC01 | bc_dbase/EG14_001.csv |
| EG04_004 | FC01, FC02 | bc_dbase/EG04_004.csv |
| EG04_005 | DS_BC, FC01 | bc_dbase/EG04_005.csv |
| EG04_006 | DS_BC, FC01, FC02 | bc_dbase/EG04_006.csv |
| EG14_002 | SB1, SB3 | bc_dbase/EG03_003.csv |
| EG14_003 | Loc_001, Loc_002, Loc_003, Loc_004 | bc_dbase/EG14_003.csv |

EG04_004 additionally uses HQ `Name=slope=0.01`, a documented internally generated
rating curve; this is not a missing BC database key. A fix must recognise this
1D-specific rule rather than simply adding `1d_` to a regex or applying the 2D
eight-field schema. EG03_003.csv has its header on row three and EG14_003.csv uses
`Time` for selector `time`; the existing bounded, case-insensitive header reader
already accepts both. A first-line-only/exact-case diagnostic would misclassify
these valid sources, so it is not used as validation evidence.

**EG03_009:** its rainfall control file uses `RF Interpolation Method == TIN`
and `Read GIS RF Triangle == ..\model\gis\2d_rf_EG03_009_R.shp` on line 7.
All 12 triangle rows have empty Name values. That is valid: TUFLOW explicitly
ignores triangle attributes, as specified in the
[official 2016-03 release notes, rainfall interpolation](https://downloads.tuflow.com/_archive/TUFLOW/Releases/2016-03/TUFLOW%20Release%20Notes.2016-03.pdf)
and illustrated by the
[official rainfall control examples](https://wiki.tuflow.com/TUFLOW_Rainfall_Control_File_Examples).
The validator's `_gis_read_role` broadly classifies every `Read GIS RF ...`
command as a BC lookup, then wrongly demands a Name on the triangle geometry.
Its separate rainfall point layer supplies the actual rainfall identifiers.
The targeted fix is command-aware geometry-only handling of RF Triangle, retaining
file/companion validation and ordinary rainfall-point database selection.

The smallest repair belongs upstream in the pinned public PyTUFLOW validation API:
support 1D BC field/Type/Name semantics and distinguish rainfall triangulation
geometry from BC-name-bearing layers. Do not edit example inputs, suppress these
findings, add a second consumer inspector, or accept an unreviewed pin change.
These findings do not establish solver/hydraulic validity, and the unmodified
full regression remains **153/161 TCFs, 166/174 runs passed** pending repairs.

Detailed actual checked-path lists, expected selected sources, original GIS rows,
BC keys and header results are retained outside the repository in
`E:\Github\pr159-example-failure-investigation.json`; its read-only reproduction
is `E:\Github\pr159-example-failure-investigation.py` and log of reruns is the
matching `.log` file. The parser target and source fixture remain unchanged.

### Published regression and upstream repair — 2026-10-31

The owner authorised publishing this regression now and creating a fix PR based
on Chain-Frost/PyTUFLOW #59. The repair is
[PyTUFLOW #60](https://github.com/Chain-Frost/PyTuflow/pull/60), targeting
`feat/optional-raster-header-validation` at `cd18e35f58ac05e0355bf198c63fa42b0102c685`.
Fix head `388f42f7833e60fca322b1172e202c273b6fecf1` is committed and pushed;
neither #59 nor official upstream PR17 was changed or merged.

The unchanged #59 base reproduces **153/161 TCFs, 166/174 contexts passing**.
The fixed source and its non-editable installed wheel both pass **161/161 TCFs,
174/174 contexts**, with BC CSV header checks enabled and both raster metadata
and CRS options explicitly disabled for this dependency-focused comparison.
Both modified installed modules match the committed source byte-for-byte.
Affected installed-wheel boundary/batch/GIS/input-safety/raster/CRS tests pass
**177 tests, 9 existing platform/privilege skips**; no new boundary-test skips.
Normal Windows Python 3.14.6 retains the inherited native DLL/projection logs;
this is not full clean-QGIS acceptance or native Python 3.11 runtime evidence.
The fixed wheel SHA-256 is
`6186ddf3a97958aa00fee01e7d39badba1159d237cf3d2db6530b44f2fad7b5f`.
Public-data diagnostic receipts remain external to the repositories, including
`E:\Github\pr159-upstream-example-pr159-upstream-installed.json`.

At its initial publication, the separate `windows-example-regression` CI job
remained visibly red on the eight pinned-PyTUFLOW findings. The owner's later
decision allows only those exact source/model/diagnostic signatures until the
upstream fix is reviewed and adopted (issue #181). All 161 models are still
inspected with full run accounting and JSON/CSV evidence; extra/changed
findings or unexpectedly recovered models fail the test. The public-API smoke
remains independent. The test verifies its installed source against
`repo-scripts/pytuflow-requirement.txt`, so adoption does not require another
hardcoded hash in the test.

Final publication-source local checks: **106 inventory/planner tests pass**;
Ruff lint/format, modified-file strict Pyright, documentation/index/link checks,
whitespace and checksum-verified Actionlint **1.7.12** pass. The final full archive
test deliberately returns **1 passed, 1 failed** in 30.04 seconds, with exactly
the eight known cases and all 174 contexts completed. Reports are at
`E:\Github\pr159-published-example-reports-20261031`; no fixture or pin changed.
The concurrent documentation-only update `5c4d073a` is retained on publication.

[Action #181](https://github.com/Chain-Frost/ryan-tools/issues/181) is the durable
adoption checklist: wait for reviewed upstream delivery, verify exact revision,
version and installed origin, reconcile installer/capability guidance, preserve
the consumer's agreed raster/CRS scope despite #59's changed defaults, rerun the
complete strict native archive and affected consumer checks, then obtain final-head
hosted evidence. Remove the temporary exact-signature allowance when adopting
the upstream fix; do not modify example inputs to make them pass.
Next review is 2026-11-07 or upstream delivery, whichever comes first.

#### Delivery and hosted receipt

The regression was committed and pushed to #159 as
`8c22ee7f779e5ca38039d0a34946c0fe8e3eb29f`. Publication retained all seven
concurrent commits between the original baseline and `5c4d073a`, including the
new deduplication/Python-owned launch implementation, not just the final docs.
After that reconciliation, **109 inventory/planner tests pass**, with one inherited
invalid-escape warning in `test_command_plan.py`; the new test's Ruff and strict
Pyright checks remain clean. The full archive replay repeats the same eight
findings in 25.56 seconds; evidence is at
`E:\Github\pr159-rebased-example-reports-20261031`.

[Hosted Windows run 41208998343](https://github.com/Chain-Frost/ryan-tools/actions/runs/41208998343)
is terminal on PR head `8c22ee7f`. Both existing public-validation and incompatible-
same-version jobs **pass**. The new archive job **fails as expected**, completes
all 161 TCFs / 174 contexts, and uploads its issue CSVs, JSON and JUnit artifact.
The downloaded JSON reproduces precisely the same eight findings and **153 TCFs /
166 contexts passing**, with native Python 3.14.10 and installed parser 1.2.7 from
the exact accepted `05e35476` archive. Its actual PR merge checkout is
`be9efa8ed8d93ae02b986ae9433f796d09cc1364`; distinguish that from the PR head.
The artifact is `tuflow-example-public-validation`; local receipt copy is
`E:\Github\pr159-hosted-example-artifact-8c22`.

Other checks on that head expose unrelated incoming runner changes: Fast PR checks
report three exception-message lint errors in `simulation_runner.py` and two
invalid Windows-path escapes in `test_command_plan.py`; the command-plan job
reports an old duplicate-preserving wrapper assertion/formatting; CSV strict typing
reports a ModuleType attribute assignment in `test_run_tuflow_simulations.py`.
These do not originate in the regression patch and are not fixed or suppressed
by this task. Subsequent concurrent commit `c301275b` changes pending-Resume solver
checks and is preserved; earlier checks are not relabelled as its acceptance.
Final-head overall #159 CI remains a separate review requirement. PyTUFLOW #60 has
no hosted checks configured at the checked fix SHA; its evidence is local and
installed-wheel validation only. Neither PR is merged and the parser pin is unchanged.

### Process-interface typing and pre-upstream Windows acceptance — 2026-10-31

- The runner now uses a structural process-handle `Protocol` covering only
  actual lifecycle operations. Deterministic process doubles implement it
  directly, without pretending they are `subprocess.Popen` instances.
- Windows acceptance launches three harmless real **Python** children: one
  successful, one nonzero exit and one terminated after interruption. The
  tests inspect TUFLOW argv without executing it, and verify real PID,
  working-directory effects, observed returns and callback behaviour.
  No BAT/CMD file, imported command or TUFLOW solver is executed.
- The complete native example archive is checked against the **unchanged**
  PyTUFLOW `05e35476` pin. A temporary eight-finding allowlist matches exact
  model, origin, layer, status, kind, source filename and diagnostic detail.
  No model is skipped. New findings and unexpected recovery fail CI.
- [Windows example-regression run 41215311419](https://github.com/Chain-Frost/ryan-tools/actions/runs/41215311419)
  reports **2 pytest tests passing**, covering **161 TCFs / 174 model
  contexts**, with JSON/CSV issue artifacts. The same candidate passed
  [Windows runner acceptance](https://github.com/Chain-Frost/ryan-tools/actions/runs/41215311500),
  Ruff, strict policy, packaged wheel, public API smoke and command-plan checks.
- [Issue #181](https://github.com/Chain-Frost/ryan-tools/issues/181)
  explicitly requires removing the temporary allowance and achieving 161/161
  clean validation when the reviewed upstream fix is adopted. No repin,
  production solver execution, PR merge or release has occurred.

## Retired history

The previous investigation material and detailed interim status are available
in [the reviewed Git revision](https://github.com/Chain-Frost/ryan-tools/tree/5edc2f5371da3a55faec4cbab2115e43574f1c43/docs/work).
They are removed from the final working tree at the owner's request; they are
not outstanding benchmark tasks or additional acceptance gates.
