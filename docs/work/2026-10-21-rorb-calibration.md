# Generalised RORB calibration

| Field | Value |
| --- | --- |
| Status | Bounded native acceptance passed locally; isolation correction awaits final-head CI and independent review |
| Owner | ChatGPT / local maintainer agent |
| Created | 2026-10-21 |
| Updated | 2026-11-28 |
| Next review | 2026-12-05 |
| Baseline | main `5eda4f06cafbbdc5f528bb108909438302a13ce5`; existing branch `feature/rorb-parameter-calibration-20261021` |

## 2026-11-28 other-PC native acceptance (#345)

This receipt supersedes the unavailable-input and pre-start execution blockers below for this machine and case.
Baseline software head `2a66697c3cc8e1f97becb1253d2fd3d843ee333e`; continue the existing PR #145 branch.
Private receipt **RT345-NATIVE-20261128** retains exact original/staged paths, SHA-256/size, executable identity,
generated PAR/report, complete comparisons and before/after source checks. No proprietary data or binary is committed.

Authenticated the original model, storm, 15/10/5 PAR/report, historical script and CSV from read-only custody.
Report site headings identify calculated versus actual discharge; all 289 observed ordinates agree with the storm
source at printed precision. Archive inspection/scoring reproduces RMSE **95.1206399347255**, MSE
**9047.936141591696**, NSE **-20.074156571084288**, calculated peak **467.856 m3/s at 5.75 h** and observed
peak **101.09 m3/s at 11.5 h**. These times use the original one-based printed table clock, 0.25 to 72.25 h;
the report summary clocks are one 0.25 h step earlier. Both conventions are retained without shifting series.
The archived CSV's rounded 9048 entry is MSE, not RMSE. This is workflow reproduction, not acceptable calibration fit.

Windows / normal **Python 3.14.5**, all Python commands/scripts executed from an isolated W-drive worktree;
generated evidence and model copies are local scratch. Original project files remained read-only. Used the installed
Program Files executable in place; its fresh output identifies **CMD 6.44 (12 March 2019)**, separately from the
older documentation's 6.31 basis and bundled release-note versions. Original settings verified: Kc 15, m 0.8,
IL/CL mode 1, one ISA/burst, IL 10 mm, CL 5 mm/h, verbosity 3 and 0.25 h / 288 calculation increments.

**Reproducible isolation defect and correction.** The first single-combination/one-worker process returned exit 0
and generated the correct report beside its absolute-path catchment input in local scratch, outside the per-run
folder. The CLI correctly refused to score an absent per-run report and returned 1. Nothing was written to original
custody or Program Files. Fix: stage byte-preserved catchment/storm copies in each fresh run directory, with short
relative PAR locators. A focused synthetic native-placement check now follows the catchment path, verifies staged
bytes and source preservation, and would expose the original defect.

The second and final native process repeated **only the same single case**, one worker, after correction:
native and CLI exits **0**, exactly one report inside its run directory, all **seven Q series / 2,023 ordinates**
plus **289 times and increments exactly equal** to the archive (zero absolute difference), identical metrics and
byte-identical comparison PNG to archive scoring. No sweep, parallel run or application-control workaround.
Input and executable hashes remained unchanged. The first diagnostic's unexpected scratch report is retained and
documented; the corrected report, logs, PAR, inputs and plot are isolated within its run directory.

**Local checks:** 23 focused calibration tests passed; strict Pyright on both changed Python files returned zero
errors. Three input-specific audited preflights (default; explicit execute plus dry-run; configured execute plus
dry-run) returned 0 with zero external launch events, no CLI-created output directory and unchanged inputs, both
before and after correction. Archive inspect and score CLI modes passed. Local Ruff lint/format were attempted but
the installed Ruff executable was denied before startup (`WinError 5`); neither is claimed passed locally. The prior
accepted software head's CI/review remains historical; final pushed-head hosted checks and independent review are
required for this correction. No package/library/dependency change; bare `pyrorb` retained.

**Limits / next:** direct mapped/UNC native inputs, parallel licensing and PyHMA scoring are not tested. Original
source authentication and local staged execution are distinct. Keep [#345](https://github.com/Chain-Frost/ryan-tools/issues/345)
open until this native receipt and final correction are reviewed. Refresh exact-head CI and independent review,
then obtain owner integration approval; no merge, release or proprietary publication is authorised.

Earlier current-state and next-action sections below describe their dated stages, not a reason to repeat the run.

## 2026-11-28 integration and dependency follow-up

The owner selected this existing PR for removal of the PyRORB exact version pin.
Reconcile current main with the prior calibration head `f404642bddf3b7bf8f5d71988cfa0d247d8a0e19`, preserving the
calibration source/tests and all current main changes. Runtime dependency is now
`pyrorb` with no minimum, maximum or exact constraint in the proposed metadata.
Routine validation resolves a compatible public wheel and checks its identity,
provenance and reader behaviour, without changing the numerical method.

The original integrated head `eb5486728c5f6280d56778bd09652e380073647b` passed
[reader acceptance](https://github.com/Chain-Frost/ryan-tools/actions/runs/46039211889)
(10 tests, no skips), with the unconstrained dependency resolving the authenticated
26.11.24.1 wheel. [General CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/46039211929)
stopped on a single formatting difference; that difference is corrected.

The follow-up addresses both P1 calibration review findings: editable top-level
settings for operational options, including explicit false/zero CLI overrides;
read-only preflight by default, with explicit `--execute` or `EXECUTE = True` needed
for native launches. Dry-run takes precedence, and archive inspection/scoring
never invokes RORB. Added mocked/no-write regression checks and updated the
guide. No change to the calibration metrics, parameter rendering or parser.
Independent review of 974f03ec676f42766262260c722c52688b58594b found no major issues.
The permitted Windows single-run and archive acceptance remain required. The current
owner request authorises the bounded single run subject to input verification and
Windows application controls; merge and publication remain unauthorised.

## Outcome and scope

Provide a reusable, non-project-specific RORB_CMD parameter sweep for Kc, IL and CL/RoC. Retain optional PyHMA, correct the historic squared-error-as-RMSE defect, and require positively identified observed/modelled flow sources.

Use a maintained standalone script under ryan-scripts/RORB-python, without a premature library/API split or distribution change. Historic Pilgangoora project files and executable are intentionally excluded from source control.

## Current state

- Standalone calibration CLI and RORB parameter generation added.
- Dedicated per-run directories, isolated logs, ranked CSV, opt-in plots and PyHMA.
- Read-only output inspection, side-effect-free dry run, observed CSV alternative, input checks and run-count safeguards.
- Synthetic validation completed. The archived installed-format output parses successfully, and its hydrograph summary
  plus the storm-file source data establish `Hyd001` as calculated and `Hyd002` as actual Turner River flow.
- Real RORB_CMD execution remains blocked in the current Codex execution context (`WinError 5: Access is denied`)
  before the installed executable starts; no RORB output is produced by that attempt.
- Local RORB files, RORB_CMD manual/version and old outputs are available to the maintainer's local agent.
- An offline `--score-output` mode now evaluates archived RORB outputs without launching RORB_CMD, using
  the same parser/score logic as active runs. Supports output PNG/CSV, external measured data, `--dry-run`, and
  validation before any new output directory is created. The active run CSV also includes Kc, m, loss model,
  measured-flow source, calculated-flow column and exact discovered .out path.
- Five additional synthetic test cases were committed for archived-mode scoring, measured CSV inputs, no-write
  invalid input and dry-run. These newly committed changes **have not yet been rerun** under Python 3.14,
  Ruff or Pyright. Earlier local validation covered the prior head only.

## 2026-11-02 review correction

At PR review, an incomplete or nonnumeric hydrograph row could previously end table parsing and score only an apparently valid prefix. The parser now distinguishes textual report footers from numeric data rows and rejects incomplete, nonnumeric and non-finite flow data, with synthetic tests for archived scoring and isolated fresh-run failure reporting. The strict Pyright policy failure on standalone-script import and its test double was also addressed by a repository Pyright extra path and corrected mock typing.

**Awaiting exact-head CI results and permitted Windows RORB_CMD acceptance.** The previous archive provenance applies only to the verified Pilgangoora 15/10/5 report. No fresh licensed executable validation or hydrograph-format compatibility is claimed from these changes.

## Next action

Local maintainer: first use `--score-output` on the verified archived 15/10/5 `.out` to confirm
RMSE 95.12064, peaks and plot using the new shared scoring path; run the focused Python 3.14 tests, Ruff and Pyright
on the current PR head. Then launch the installed unsigned `C:\Program Files\RORBWin\RORB_CMD.exe` from a permitted
interactive context, rerun one actual 15/10/5 simulation with one worker using `--execute` only after preflight, and compare the new `.out` and metrics
with the archive before widening the parameter sweep.

## Completion criteria

- A known input pair runs on installed RORB_CMD with reproducible per-run outputs and a correct hydrograph table.
- Independently prove observed discharge versus simulated discharge mapping or supply a measured-flow CSV.
- Compare representative modelled/observed hydrographs, peak timing and error values with archived results.
- Run Python 3.14 Ruff, strict Pyright on changed files, focused tests and --help/--version/--dry-run.
- Obtain local maintainer approval before merging.

## Validation and delivery

2026-10-21: Six focused synthetic tests passed on Python 3.13.5. Compiled source; checked --help, --version and missing-executable preflight (nonzero). Python 3.14, Ruff, strict Pyright and **RORB_CMD execution were not available** in the current environment. Changes committed/pushed to feature/rorb-parameter-calibration-20261021. No PyPI release or merge authorised.

2026-10-21 local validation: Python 3.14.5 compilation, `--help`, `--version`, no-write dry run and archived-output
inspection passed. The archived 15/10/5 report parsed as 289 rows with `Inc`, `Time` and `Hyd001` through `Hyd007`.
Its hydrograph summary explicitly identifies `Hyd001` as calculated and `Hyd002` as actual at TurnerRvr-Flow; the
storm file contains the corresponding `TurnerRvr-Flow` hydrograph data. Inputs were copied byte-for-byte to
`C:\Temp\rorb-pr145-20261021`; nothing was written to the historic `P:` project. The generated local parameter file
matches the historic 15/10/5 values and uses absolute local input paths. Both normal subprocess launch and hidden
`Start-Process` launch of the installed unsigned RORB_CMD were denied before execution (`WinError 5`), so the real
output, plot and metric comparison remain blocked. Strict Pyright exposed typing defects in the draft and
they were corrected locally. Strict Pyright then passed with zero findings, Python 3.14 compilation passed, and all six
focused tests passed using a local pytest runtime directory (the repository-root cache folders remain inaccessible).
The archived case produces RMSE 95.12064, MSE 9047.93614, peaks 467.856 at 5.75 h calculated and 101.09 at 11.5 h
actual; this confirms the historic CSV's rounded `RMSE` value 9048 was MSE. A 35,888-byte local comparison PNG was
generated from the archived output. Ruff could not launch in the current application-control context. The existing
`qgis-resources` submodule worktree change was unrelated and preserved.
The installed bundle includes RORB v6.52 release notes and a 2025 RORB_CMD binary without embedded version metadata,
while its command-line documentation still states that it is based on v6.31; exact RORB_CMD version therefore remains
unconfirmed.

## Progress

### 2026-10-21

Generalised the original fixed-path Calibrate_v5.py approach, replaced fragile automatic Hyd001/Hyd002 assumptions, added a documented local test workflow and tests. Awaiting the local RORB_CMD implementation/output check.

### 2026-10-21 — follow-up coding

Extended the draft to support offline scoring of historic RORB outputs, sharing the exact scoring pipeline with
live runs so numerical comparisons can proceed even when Windows application controls block RORB_CMD. Added
synthetic regression tests and included RORB exponent, loss model, reference provenance and generated report path
in the sweep CSV for traceability. Changes committed to PR #145. No fresh executable-level run, Python 3.14,
Ruff, strict Pyright or focused pytest validation has been performed on these follow-up commits; this remains
a local-acceptance blocker rather than an assumed pass.

## 2026-11-28 acceptance refresh on a different Windows computer

Validated source revision 974f03ec676f42766262260c722c52688b58594b in an isolated
worktree, E:/Github/ryan-tools-pr145-acceptance, preserving the main checkout and
concurrent PyRORB work. Windows / normal Python 3.14.6: all 23 calibration tests
passed, requested Ruff lint/format and strict Pyright passed; help/version passed.
Only the required `tests/test_data` submodule was initialised.

Reused [original-reader acceptance](https://github.com/Chain-Frost/ryan-tools/actions/runs/46040244171)
and the [independent exact-head review](https://github.com/Chain-Frost/ryan-tools/pull/145#issuecomment-7224217939).
[General CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/46040244162)
passed Ruff/whitespace, POSIX security and Package; Policy failed on four typing
diagnostics at the reader dependency test's PackageMetadata.get call. Hosted
tests were still in progress at inspection. Replaced that call with the supported
indexed metadata lookup, preserving the optional-field check and bare pyrorb
dependency. After installing the bare public dependency (resolved 26.11.24.1) in
the normal user environment, all 25 calibration/reader compatibility tests passed;
Ruff lint/format and strict Pyright passed on all three Python files. This does
not replace the already successful authenticated original-reader workflow.

**Native acceptance is blocked by unavailable verified inputs.** The owner confirmed
that the historical model/report are on a different computer. The old local
staging folder and historical project drive are unavailable here. The installed
RORB_CMD exists, but no native process was launched and no project data was
modified or committed. Archive scoring, input-specific default no-write preflight,
new output location, complete native hydrograph, metrics and input integrity are
**not_run**, not passing. Synthetic no-launch/no-write tests passed. No broader
sweep, application-control workaround, merge or publication was performed.

The owner request says observed peak 101.09 at 1.5 h; the retained historical
record and script guide say **11.5 h**. Do not change the reference or claim either
time freshly verified without the original report/storm series. Calculated peak
467.856 at 5.75 h and RMSE 95.12064 likewise remain historical values only.

Next action: provide the verified archive on this computer or resume on its host;
authenticate source series and input hashes, score the archive, confirm default
preflight causes no launch/write, then run exactly one 15/10/5 case with one worker
and explicit --execute --no-pause. Reconcile the observed peak clock against the
original series. Final pushed-head CI/review and owner acceptance remain required.
The historical P1 threads have implementation fixes and a clean independent review;
no native or engineering acceptance follows from resolving software review.
