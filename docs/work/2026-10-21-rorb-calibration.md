# Generalised RORB calibration

| Field | Value |
| --- | --- |
| Status | In local validation |
| Owner | ChatGPT / local maintainer agent |
| Created | 2026-10-21 |
| Updated | 2026-10-21 (local validation) |
| Next review | 2026-10-28 |
| Baseline | main; feature/rorb-parameter-calibration-20261021 |

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

## Next action

Local maintainer: first use `--score-output` on the verified archived 15/10/5 `.out` to confirm
RMSE 95.12064, peaks and plot using the new shared scoring path; run the focused Python 3.14 tests, Ruff and Pyright
on the current PR head. Then launch the installed unsigned `C:\Program Files\RORBWin\RORB_CMD.exe` from a permitted
interactive context, rerun one actual 15/10/5 simulation with one worker and compare the new `.out` and metrics
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
