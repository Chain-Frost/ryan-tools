# TUFLOW culvert evaluation engines

TUFLOW-specific culvert ingestion and batch orchestration are owned by `ryan-tools`.
Hydraulic calculations are delegated to one of two independent engines:

- `hy8` uses the public `run_hy8` package and the external HY-8 executable.
- `ryan-culverts` uses the public `culvert_solver` API vendored from
  `ryan-culverts` and does not require HY-8.

This replaces the TUFLOW-specific computation demos formerly kept in `run-hy8`.
Generic HY-8 examples remain upstream.

## Maximums workbook

```powershell
python ryan-scripts/tuflow/tuflow_culvert_from_maximums.py maximums.xlsx --engine ryan-culverts
python ryan-scripts/tuflow/tuflow_culvert_from_maximums.py maximums.xlsx --engine hy8 --hy8-exe C:\Path\HY864.exe
```

The workflow retains the useful scenarios from the former
`culvert_demo-from-tuflow.py` script:

- prescribed TUFLOW discharge with `DS_h` tailwater;
- prescribed TUFLOW discharge with downstream-invert tailwater;
- inverse discharge at the TUFLOW `US_h` level when available;
- inverse discharge at a configurable HW/D target, default 1.5.

For each crossing/AEP pair the highest positive `Q` row is retained.

## TUFLOW 1d_nwk

```powershell
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --engine ryan-culverts
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --layer 1d_nwk --engine hy8
```

The default inverse checks are HW/D 1.5 and 2.0. Use `--headwater-ratios`
to supply another set.

Both migrated wrappers refuse to replace an existing result CSV unless
`--overwrite` is supplied. The `1d_nwk` wrapper also honours the TUFLOW
`Ignore` field and resolves negative `Len_or_ANA` values from the digitized
feature length. When Maximums HY-8 workspaces are retained, their paths include
the crossing, AEP and scenario; an existing run directory is not reused unless
`--overwrite` is explicitly supplied.

The wrappers are discoverable through the MCP workflow catalogue as
`tuflow_culvert_evaluate_maximums` and `tuflow_culvert_evaluate_1d_nwk`,
with separate `ryan-culverts` and `hy8` scenarios.

## Shared mapping contract

Both engines receive the same engine-neutral circular-culvert definition:
diameter, length, inlet/outlet invert, Manning roughness, barrel count and material.
TUFLOW parsing therefore happens before engine dispatch.

The migrated source demos are circular-only:

- the Maximums workflow maps circular corrugated-steel pipe assumptions retained
  from its former `run-hy8` demo;
- the `1d_nwk` workflow accepts TUFLOW `Type = C` and maps circular concrete pipe
  assumptions retained from its former demo.

The migration deliberately does not infer missing box width, material or inlet
configuration. Unsupported source combinations fail closed. Broader project-export
mapping in `tuflow_to_hy8.py` remains a separate workflow.

## Engine differences

The engines are not asserted to be numerically identical. Inlet configuration and
coefficient libraries, solver formulations, convergence behaviour and supported
hydraulic states can differ.

For the HY-8 backend the migration preserves the old demo inlet assumptions:
square-edge headwall for circular concrete and thin-edge projecting for circular
corrugated steel. A high artificial roadway crest keeps roadway overtopping outside
the intended migrated demo calculations.

The `ryan-culverts` backend uses the corresponding public material and geometry
contracts and the authoritative solver coefficient selection. Compare the engines
explicitly before treating them as interchangeable for design acceptance.

## Output

Both wrappers write long-form CSV. Every scenario row records the source, crossing,
selected engine, requested and computed hydraulic values, HW/D, outlet velocity,
flow type, roadway discharge/overtopping, status, warnings, failure text and any
retained HY-8 workspace. Failed inputs and scenarios remain visible rather than
being silently dropped.

## Repository boundary

`ryan-tools` owns TUFLOW field interpretation, engine selection and batch output.
`run-hy8` owns HY-8 serialization, execution and parsing. `ryan-culverts` owns
native culvert hydraulic equations. Missing hydraulic capability belongs in the
appropriate engine rather than being reimplemented in this integration layer.
