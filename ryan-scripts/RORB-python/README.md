# RORB Python scripts

The RORB folder contains standalone or library-backed utilities for RORB storm data, hydrographs, calibration, and model results. Scripts retain their individual input/output contracts. See the [script selection guide](../README.md).

## RORB command-line calibration

[rorb_calibrate.py](rorb_calibrate.py) is a maintained **standalone, locally validated prototype** generalised from an older Pilgangoora parameter-sweep script. It requires a locally installed and licensed version of RORB_CMD.exe; RORB_CMD is **not** distributed with ryan-tools. The original project .catg/.stm files and results are not committed.

The installed-format parser and historic Pilgangoora output were validated locally on 2026-10-21. The retained report
explicitly labels `Hyd001` as calculated and `Hyd002` as actual at the Turner River gauging station, and the retained
storm file contains the corresponding measured `TurnerRvr-Flow` hydrograph. This provenance is specific to that input;
other projects must still verify their own labels and source data. The local Codex execution context denied launch
of the installed unsigned RORB_CMD before it started, so a newly generated executable-level comparison remains
outstanding.

It generates a Cartesian product of **Kc × IL × CL/RoC**, writes an isolated RORB .par per combination, launches RORB_CMD, parses the returned labelled hydrograph table, and ranks successful simulations by *correctly calculated* RMSE. It also reports NSE, signed mean bias, modelled/observed peak flows and peak times, and optional PyHMA scores and hydrograph PNGs.

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
   py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py --catg "Q:\model\catchment.catg" --stm "Q:\model\event.stm" --rorb "Q:\tools\RORB_CMD.exe" --kc 15 --il 10 --continuing 5 --sim-column Hyd001 --obs-column Hyd002 --workers 1 --plots --no-pause
   ```

   **Column names above are illustrative, not verified as modelled versus observed.** Substitute the labels established in step 2. If the real observed flow is not a RORB output column, use an independently validated `--observed-csv` with matching Time and Flow values instead of `--obs-column`.

5. Review the generated parameter file, standard-output/error logs, discovered .out, extracted times and hydrograph plots against the archived results before expanding the sweep. The script expects exactly **one** .out file in each isolated run folder. If the installed RORB_CMD writes elsewhere or produces a different table layout, capture those results and amend the adapter rather than silently picking a file/column.

The input files are read from their existing paths, using `Path.absolute()` rather than dereferencing network-drive/junction paths. RORB runs execute from fresh per-run directories under the configured `--output` parent (default: `rorb_calibration`); a timestamped batch folder prevents overwriting existing results. Each run contains .par, RORB .out and log files. The batch contains `results.csv`; failures are included with errors, and the process exits nonzero if any run fails.

### Larger sweep

```powershell
py -3.14 .\ryan-scripts\RORB-python\rorb_calibrate.py --catg "Q:\model\catchment.catg" --stm "Q:\model\event.stm" --rorb "Q:\tools\RORB_CMD.exe" --kc 15:20:5 --il 10:20:5 --continuing 5:10:5 --sim-column Hyd001 --observed-csv "Q:\model\gauged_flow.csv" --workers 1 --plots --no-pause
```

Ranges are inclusive `start:end:step` or comma-separated values; use decimal numbers freely. The example runs 12 combinations. The default maximum is 1,000 combinations. Multi-process RORB operation is **opt-in** via `--workers`; use one process until concurrency, licensing and file isolation are proven. When using external observed CSV, `--obs-time` and `--obs-flow` select the exact named columns and times must match RORB's numeric time values without interpolation. No automatic conversion of units or time references is performed.

Set `--loss-model 2` for IL/RoC and adjust continuing-loss values accordingly. Optional `--hma` invokes the installed `vendor.PyHMA` from ryan-tools; `--hma-window` (time increments) and `--hma-weight` control scoring. PyHMA is not required for the default RMSE/NSE workflow.

### Limitations and local acceptance

- **Partially verified RORB interface.** The installed command manual, historic parameter/output filenames and real
  output table were checked, and the real output parses successfully. The current execution context blocked a fresh
  RORB_CMD process before startup, so absolute-path execution and new output placement still require executable-level
  proof.
- A labelled RORB column cannot on its own establish provenance. The Pilgangoora source was independently established;
  do not transfer its `Hyd001`/`Hyd002` mapping to another model without equivalent source evidence.
- The parser rejects absent or multiple hydrograph tables, incomplete or nonnumeric hydrograph records, non-finite values and non-increasing times. Only textual report footers terminate a table; malformed numeric rows fail rather than silently returning partial calibration scores. If the installed RORB version produces an alternative layout, the local agent should supply a de-identified sample and update the parser with tests.
- A separate observed CSV is supported only when its series and numerical time steps exactly align with the model. No interpolation, lag correction or unit conversion occurs.
- PyHMA scoring is optional and may need numerical and version validation for the local Python/RORB versions.
- Success in synthetic tests does **not** establish hydraulic validity or agreement with the historic calibrated outputs.

Tests: `py -3.14 -m pytest tests/test_rorb_calibrate.py -q` (from the repository root). This script does not require installation as an entry point, does not update package metadata, and should remain a draft PR until the local single-run comparison is accepted.
