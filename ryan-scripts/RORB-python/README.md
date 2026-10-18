# TUFLOW rainfall CSV → RORB STM

Use [`tuflow_bc_to_rorb_stm.py`](tuflow_bc_to_rorb_stm.py) to convert **TUFLOW
STEPPED rainfall** (simulation hours and **mm per previous interval**) into a
single-pluviograph RORB `.stm` DESIGN storm. It supports one file or a
critical-storm-table batch; the maintained entry point delegates processing to
`ryan_library.functions.RORB.storm_files` and
`ryan_library.orchestrators.rorb.storm_conversion`.

## Usage

```powershell
py -3.14 tuflow_bc_to_rorb_stm.py "D:\TUFLOW_MLGD\bc_dbase\GD08\rf_inflow\GD08_RF_01.00p00120m.CSV" --column "TP07_2030_SSP3-7.0" --output-dir "D:\RORB\stm" --dry-run --no-pause

py -3.14 tuflow_bc_to_rorb_stm.py --critical-storms "D:\critical_storms.csv" --bc-database "D:\TUFLOW_MLGD\bc_dbase" --output-dir "D:\RORB\stm" --dry-run --no-pause
```

For the batch table, the required column headings are `Model`,
`TUFLOW_AEP`, `TUFLOW_Duration` and `TP`, for example
`GD08,01.00p,00120m,TP07`. **Incomplete rows fail batch validation by default**,
to avoid silently omitting design storms. Pass `--allow-incomplete` to skip
incomplete rows with explicit warnings; even with this option, a batch with
no valid jobs fails. Invalid complete rows also fail the entire batch.
Malformed tables (missing/duplicate headings or extra cells) are rejected.
No files are written if preflight validation fails. A filesystem failure during
writing may still leave earlier files; the tool does not provide transaction
rollback. By default existing STMs are not overwritten.

Default paths are relative to the supplied `--bc-database` and
`--output-dir` respectively:

- Input: `{model}/rf_inflow/{model}_RF_{aep}{duration}.CSV`
- Output: `{model}/Storm_{model}_{aep_normalised}_{duration_minutes}m_{tp_lower}.stm`
- Rainfall column: `{tp}_{climate_suffix}`

Use `--input-template`, `--output-template`, `--column-template` and
`--climate-suffix` to support other directory layouts and climate columns.
Template outputs must stay inside the output root.

## Rainfall increment interpretation

The CSV's `t=0` row is a boundary placeholder: its rainfall must be zero and
it is **not** written into the RORB interval depths. For a TUFLOW input with
times `0,1,2` and depths `0,5,10`, the RORB block is:

```text
0,2
Pluviograph 1
5
10
-99
```

The `0,2` line means two rainfall increments from the burst start (not three
timestamp records). Dry intervals inside the storm are retained. The default
`--terminal-zero auto` policy strips one **extra zero timestep** only where
the rainfall filename's nominated duration is exactly one timestep shorter
than the CSV time range. If the storm intentionally extends beyond the nominal
duration, use `--terminal-zero keep`. Use `drop` to explicitly exclude one
final zero for non-standard filenames, only after reviewing the CSV. No other
rainfall values are silently removed. Inputs with negative depths, a nonzero
time-zero value, non-uniform steps or mismatched named durations are rejected.

The default `--run-increments` is **200**, matching RORBwin 6.52's documented
simulation default. This **does not guarantee** the computed runoff hydrograph
recedes within that period. Increase it after checking the model runoff tail.
RORBwin 6.52 documents an **upper limit of 2,000 increments** for reading an
existing storm. The converter rejects run lengths beyond that limit and run
lengths shorter than the rainfall burst.

## Verification and limitations

- The generator supports **one rainfall burst, one pluviograph and uniform
  spatial rainfall**. More complex patterns and ARR data are not added
  implicitly.
- Regression tests assert the complete rainfall interval sequence and full
  generated STM text, not just total depth, to catch the historic off-by-one
  issue. The included TUFLOW-format fixture is synthetic; a verified real
  project export is still needed for final engineering acceptance.
- Test a generated storm against a known RORBwin 6.52 reference model before
  relying on the output for design. Native execution of RORB is not part of
  the Python tests.
- Documentation sources: *RORB Version 6 User Manual* (Laurenson, Mein and
  Nathan, 2010), §3.2 p.28 and Table 5-2 pp.56–57; *RORBwin v6.52 release
  notes* §1.8 pp.29–30; *RORB_CMD documentation* (based on 6.31);
  [TUFLOW Classic/HPC 2026.0 §8.4.3.1.1](https://docs.tuflow.com/classic-hpc/manual/2026.0/TwoDBoundaries-2.html).
