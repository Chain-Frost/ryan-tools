# Outlet rock-protection lookup data

These CSV files transcribe the Excel tables used by `260709_Skyway Options.xlsx`:

- `agrd05b_figure_3_17.csv` comes from the `Working` sheet table `AGRD05B_Fig3.17` (`K3:P45`). The source figure is
  **Figure 3.17: Minimum rock size and length of apron for a multi-pipe outlet** in *AGRD 05B (2023)*. The figure is
  attributed to Catchments and Creeks (2011).
- `rock_classes.csv` comes from the `Working` sheet table `Rock_Class` (`S4:V12`).

For a multi-pipe outlet, `culvert_diameter_m` is the diameter of one pipe, not the combined outlet width. For example,
use `1.2 m` for an outlet comprising two 1200 mm pipes. The figure describes the apron length as a minimum recommended
length and notes that it does not account for outlet jetting effects.

## Scope limitation

The CSV and `multi_pipe_outlet_rock_protection()` implement only the multi-pipe outlet figure used by the workbook. AGRD
05B (2023) contains a separate, curved design chart for a single pipe in a bank. That single-pipe chart has not been
digitised here and must not be evaluated using the multi-pipe lookup.

Velocity bands preserve the workbook formula's boundary convention: the lower bound is exclusive and the upper bound
is inclusive. For example, a velocity of exactly `1.3 m/s` remains in the band ending at `1.3 m/s`.

The source workbook has an apparent typo in `Working!K15`: it records diameter `1.5 m`, although the row continues the
`1.2 m` sequence from rows 10–14 and duplicates the `1.5 m`, `4.5 < v <= 4.9 m/s` band at row 21. The CSV corrects
`Working!K15` to `1.2 m`, completing the otherwise regular `1.2 m` sequence.
