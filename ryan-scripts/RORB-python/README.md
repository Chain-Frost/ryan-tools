# RORB Python scripts

The RORB folder contains standalone or library-backed utilities for RORB storm data, hydrographs, calibration, and model results. Scripts retain their individual input/output contracts. See the [script selection guide](../README.md).

## RORB command-line calibration

[rorb_calibrate.py](rorb_calibrate.py) is a maintained **standalone, locally validated prototype** generalised from an older Pilgangoora parameter-sweep script. It requires a locally installed and licensed version of RORB_CMD.exe; RORB_CMD is **not** distributed with ryan-tools. The original project .catg/.stm files and results are not committed.

The installed-format parser and historic Pilgangoora output were validated locally on 2026-10-21. The retained report
explicitly labels `Hyd001` as calculated and `Hyd002` as actual at the Turner River gauging station, and the retained
storm file contains the corresponding measured `TurnerRvr-Flow` hydrograph. This provenance is specific to that input;
other projects must still verify their own labels and source data. On 2026-11-28, the installed Windows CMD 6.44
successfully reproduced all seven archived hydrographs and all 289 printed time/increment rows exactly. The first
single-case attempt exposed report placement beside the catchment input; a corrected single-case validation passed
after staging inputs per run. See the [acceptance record](../../docs/work/2026-10-21-rorb-calibration.md).
Final-head hosted checks, independent review and owner integration approval remain separate requirements.

When explicitly enabled, it generates a Cartesian product of **Kc × IL × CL/RoC**, writes an isolated RORB .par per combination, launches RORB_CMD, parses the returned labelled hydrograph table, and ranks successful simulations by *correctly calculated* RMSE. It also reports NSE, signed mean bias, modelled/observed peak flows and peak times, and optional PyHMA scores and hydrograph PNGs.

### Editable settings and execution permission

Every operational input and default is exposed near the top of the script. Leave
`INSPECT_OUTPUT` and `SCORE_OUTPUT` unset for calibration; set one for the corresponding
offline mode. Choose either `OBS_COLUMN` or `OBSERVED_CSV` for comparison. Explicit CLI
mode and observation-source selections replace the corresponding editable choice.
Use `--calibrate` to override an editable report mode.

Calibration defaults to read-only preflight, even with valid input paths and
hydrograph labels. Launching RORB_CMD requires `--execute` or an intentional
edit of `EXECUTE = True`. `--no-execute` overrides that edit; `--dry-run` always
takes precedence over execution and prevents result writes. `--no-dry-run`,
`--no-plots` and `--no-hma` explicitly override their editable defaults.
Archived scoring never launches RORB and may write new results unless dry-run is
selected. The script is always noninteractive; `--no-pause` is retained for
CLI compatibility.

### Score archived output without running RORB_CMD

When command-line execution is blocked (for example by Windows application control), the original `.out` can
still be analysed using the **same scoring function** as a newly generated run. This requires no executable and
no `.catg` or `.stm` input. For the independently verified *Pilgangoora* 15/10/5 report only, the local
hydrograph summary identifies `Hyd001` as calculated and `Hyd002` as observed:

```powershell
py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py `
    --score-output "Q:\model\historic_case.out" `
    --sim-column Hyd001 --obs-column Hyd002 `
    --output "Q:\model\calibration_review" --plots --no-pause
```

Replace the example output and input paths with local paths, and **do not reuse those column assumptions on
unverified models**. This creates a separate timestamped `archived_*` folder containing `metrics.csv` and
`hydrograph.png` (with `--plots`). Invalid columns or mismatched observation timestamps are rejected before
creating output folders. The archived single-case values reported by local validation are RMSE **95.12064 m³/s**,
MSE **9047.93614 (m³/s)²**, modelled peak **467.856 m³/s** at **5.75 h** and observed peak
**101.09 m³/s** at **11.5 h**. Use these as regression references, allowing for the archived report's numerical
precision. RORB_CMD executable output and working-directory behaviour are **not** validated by offline scoring.

For an external reference instead of a hydrograph column, replace `--obs-column` with
`--observed-csv "Q:\model\gauge.csv"` (columns `Time,Flow` by default), and explicitly confirm the time
reference and flow units. No automatic interpolation, resampling or unit conversion occurs.

### Local verification sequence

From the repository root, using the normal Python 3.14 installation:

1. Inspect the retained historic RORB output. This is read-only and does not invoke RORB:

   ```powershell
   py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py --inspect-output "Q:\model\old_run.out"
   ```

2. Examine the original .par, .out, RORB_CMD version/manual and .catg/.stm to confirm the calculated/observed hydrograph mapping. **Hyd001 and Hyd002 are not automatically treated as modelled and observed.** The user or local agent must independently establish which series represents observed discharge (or export measured flow to a separate CSV).

3. Check the executable and inputs without running any simulation or writing files:

   ```powershell
   py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py --catg "Q:\model\catchment.catg" --stm "Q:\model\event.stm" --rorb "Q:\tools\RORB_CMD.exe" --dry-run --no-pause
   ```

4. After verifying the columns, start with **one** combination and **one** worker:

   ```powershell
   py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py --catg "Q:\model\catchment.catg" --stm "Q:\model\event.stm" --rorb "Q:\tools\RORB_CMD.exe" --kc 15 --il 10 --continuing 5 --sim-column Hyd001 --obs-column Hyd002 --workers 1 --execute --plots --no-pause
   ```

   **Column names above are illustrative, not verified as modelled versus observed.** Substitute the labels established in step 2. If the real observed flow is not a RORB output column, use an independently validated `--observed-csv` with matching Time and Flow values instead of `--obs-column`.

5. Review the generated parameter file, standard-output/error logs, discovered .out, extracted times and hydrograph plots against the archived results before expanding the sweep. The script expects exactly **one** .out file in each isolated run folder. If the installed RORB_CMD writes elsewhere or produces a different table layout, capture those results and amend the adapter rather than silently picking a file/column.

The input files are read from their existing paths, using `Path.absolute()` rather than dereferencing network-drive/junction paths. After execution opt-in, each fresh per-run directory receives byte-preserved `catchment.catg` and `storm.stm` copies. Its PAR uses those short relative names: the observed CMD build writes reports beside the catchment, so referencing shared original inputs would break output isolation. Dry-run and default preflight create no copies or result folders. RORB runs execute from the per-run directories under the configured `--output` parent (default: `rorb_calibration`); a timestamped batch folder prevents overwriting existing results. Each run contains the staged inputs, .par, RORB .out and log files. The batch contains `results.csv`; failures are included with errors, and the process exits nonzero if any run fails.

### Larger sweep

```powershell
py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py --catg "Q:\model\catchment.catg" --stm "Q:\model\event.stm" --rorb "Q:\tools\RORB_CMD.exe" --kc 15:20:5 --il 10:20:5 --continuing 5:10:5 --sim-column Hyd001 --observed-csv "Q:\model\gauged_flow.csv" --workers 1 --execute --plots --no-pause
```

Ranges are inclusive `start:end:step` or comma-separated values; use decimal numbers freely. The example explicitly authorises 12 combinations. The default maximum is 1,000 combinations. Multi-process RORB operation is **opt-in** via `--workers`; use one process until concurrency, licensing and file isolation are proven. When using external observed CSV, `--obs-time` and `--obs-flow` select the exact named columns and times must match RORB's numeric time values without interpolation. No automatic conversion of units or time references is performed.

Live trials generate their CMD parameter files with PyRORB's published serializer. Install `pyrorb` if running
a copied script outside a ryan-tools installation. Archived inspection and scoring use the local report adapter.
Set `--loss-model 2` for IL/RoC and supply a dimensionless runoff coefficient between 0 and 1 with `--continuing`.
For loss model 1, `--continuing` is a continuing loss in mm/h. Optional `--hma` invokes the installed `vendor.PyHMA` from ryan-tools; `--hma-window` (time increments) and `--hma-weight` control scoring. PyHMA is not required for the default RMSE/NSE workflow.

### Limitations and local acceptance

- **Bounded native interface acceptance.** Windows/Python 3.14.5, installed CMD 6.44 and one archived 15/10/5 case
  passed with local copied inputs, one worker, complete report/metric equality and an identical archive-scoring PNG.
  Direct mapped/UNC native inputs, parallel licensing and optional PyHMA scoring remain untested. This is a workflow
  reproduction, not a claim that the parameter combination is a satisfactory observed-flood calibration.
- A labelled RORB column cannot on its own establish provenance. The Pilgangoora source was independently established;
  do not transfer its `Hyd001`/`Hyd002` mapping to another model without equivalent source evidence.
- The parser rejects absent or multiple hydrograph tables, incomplete or nonnumeric hydrograph records, non-finite values and non-increasing times. Only textual report footers terminate a table; malformed numeric rows fail rather than silently returning partial calibration scores. If the installed RORB version produces an alternative layout, the local agent should supply a de-identified sample and update the parser with tests.
- A separate observed CSV is supported only when its series and numerical time steps exactly align with the model. No interpolation, lag correction or unit conversion occurs.
- PyHMA scoring is optional and may need numerical and version validation for the local Python/RORB versions.
- Success in synthetic tests does **not** establish hydraulic validity or agreement with the historic calibrated outputs.

Tests: `py -3.14 -m pytest tests/test_rorb_calibrate.py -q` (from the repository root). The calibration script remains repository-only; PR #145 also contains the separate unconstrained PyRORB dependency follow-up. Keep the PR unmerged until the local single-run comparison and final review are accepted.
