# Generalised RORB calibration

| Field | Value |
| --- | --- |
| Status | In local validation |
| Owner | ChatGPT / local maintainer agent |
| Created | 2026-10-21 |
| Updated | 2026-10-21 |
| Next review | 2026-10-28 |
| Baseline | main; feature/rorb-parameter-calibration-20261021 |

## Outcome and scope

Provide a reusable, non-project-specific RORB_CMD parameter sweep for Kc, IL and CL/RoC. Retain optional PyHMA, correct the historic squared-error-as-RMSE defect, and require positively identified observed/modelled flow sources.

Use a maintained standalone script under ryan-scripts/RORB-python, without a premature library/API split or distribution change. Historic Pilgangoora project files and executable are intentionally excluded from source control.

## Current state

- Standalone calibration CLI and RORB parameter generation added.
- Dedicated per-run directories, isolated logs, ranked CSV, opt-in plots and PyHMA.
- Read-only output inspection, side-effect-free dry run, observed CSV alternative, input checks and run-count safeguards.
- Synthetic validation completed; **real RORB_CMD compatibility and observed hydrograph provenance are not yet validated**.
- Local RORB files, RORB_CMD manual/version and old outputs are available to the maintainer's local agent.

## Next action

Local agent: execute the local validation procedure in ryan-scripts/RORB-python/README.md, identify RORB_CMD and the correct hydrograph provenance, run a single combination with one worker, and report any .par/.out/parser differences as PR comments (prefer anonymised excerpts over uploading sensitive files). Fix the adapter and rerun targeted tests before widening the parameter sweep.

## Completion criteria

- A known input pair runs on installed RORB_CMD with reproducible per-run outputs and a correct hydrograph table.
- Independently prove observed discharge versus simulated discharge mapping or supply a measured-flow CSV.
- Compare representative modelled/observed hydrographs, peak timing and error values with archived results.
- Run Python 3.14 Ruff, strict Pyright on changed files, focused tests and --help/--version/--dry-run.
- Obtain local maintainer approval before merging.

## Validation and delivery

2026-10-21: Six focused synthetic tests passed on Python 3.13.5. Compiled source; checked --help, --version and missing-executable preflight (nonzero). Python 3.14, Ruff, strict Pyright and **RORB_CMD execution were not available** in the current environment. Changes committed/pushed to feature/rorb-parameter-calibration-20261021. No PyPI release or merge authorised.

## Progress

### 2026-10-21

Generalised the original fixed-path Calibrate_v5.py approach, replaced fragile automatic Hyd001/Hyd002 assumptions, added a documented local test workflow and tests. Awaiting the local RORB_CMD implementation/output check.
