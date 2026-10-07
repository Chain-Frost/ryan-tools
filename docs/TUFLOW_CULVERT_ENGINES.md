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

For each crossing/base-run/AEP combination the highest positive `Q` row is retained.
The base run uses `trim_runcode` when available, falling back to `internalName`,
so alternatives such as EXG and DEV are not collapsed into one governing row.
The selected run identity is also written to the result CSV. Maximums rows must
contain the actual culvert length, upstream/downstream inverts and a positive
Manning roughness; the maintained wrapper does not invent fallback geometry or
roughness when these merged source attributes are missing.

## TUFLOW 1d_nwk

```powershell
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --engine ryan-culverts
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --layer 1d_nwk --engine hy8
```

The default inverse checks are HW/D 1.5 and 2.0. Use `--headwater-ratios`
to supply another non-empty set of unique, finite, positive ratios. The wrapper
expects an SI TUFLOW model. When negative `Len_or_ANA` requests digitised line
length, the GIS layer must have a projected CRS whose horizontal axes are in metres.

Both migrated wrappers refuse to replace an existing result CSV unless
`--overwrite` is supplied. Numeric `pBlockage` is applied to circular pipes in
both Maximums and `1d_nwk` inputs by scaling the hydraulic diameter by the square
root of the unblocked area fraction. The original nominal diameter is retained
separately and remains the denominator for TUFLOW HW/D targets and reported HW/D.
Category-based blockage is rejected until its event-specific percentage has been
resolved. A blank character `pBlockage` is also rejected because a TUFLOW
`Blockage Default` may apply; a blank numeric blockage remains the ordinary 0%
case. Fully blocked (100%) culverts are rejected rather than converted to a
zero-diameter solver object.

The `1d_nwk` wrapper also honours the TUFLOW `Ignore` field, requires a
traceable non-blank culvert ID, treats `Number_of = 0` as one barrel, requires a
positive source Manning roughness, and rejects the TUFLOW `-99999` invert
sentinel until effective inverts have been resolved from processed TUFLOW data.

For the circular concrete mapping, the currently supported source-loss contract is
zero additional `Form_Loss`, an effective width-contraction factor of 1.0,
entry loss 0.5 and exit loss 1.0. Source values requesting another treatment fail
closed because that TUFLOW loss model is not yet represented consistently by both
backends.

When HY-8 workspaces are retained, an existing run directory is not reused unless
`--overwrite` is explicitly supplied. Workspace directory names include a stable
hash of the unsanitized run key so distinct TUFLOW identifiers cannot collide after
filesystem sanitization. Maximums keys include crossing, base run, AEP and scenario;
`1d_nwk` keys include crossing and scenario. HY-8 also receives a deterministic
filesystem-safe internal crossing name; normalized output continues to report the
original TUFLOW crossing identifier.

The native wrappers are discoverable through the MCP workflow catalogue as
`tuflow_culvert_evaluate_maximums` and `tuflow_culvert_evaluate_1d_nwk` under
the `create` profile. External HY-8 execution is exposed separately as
`tuflow_culvert_evaluate_maximums_hy8` and `tuflow_culvert_evaluate_1d_nwk_hy8`
under the `privileged` profile with explicit approval required.

## Shared mapping contract

Both engines receive the same engine-neutral circular-culvert definition:
hydraulic diameter, nominal HW/D diameter, length, inlet/outlet invert, Manning
roughness, barrel count and material.
TUFLOW parsing therefore happens before engine dispatch. Adverse slopes are retained
in the shared definition and passed to HY-8; the `ryan-culverts` backend rejects
these slopes explicitly because its current geometry contract does not support them.

The migrated source demos are circular-only:

- the Maximums workflow maps circular corrugated-steel pipe assumptions retained
  from its former `run-hy8` demo;
- the `1d_nwk` workflow accepts TUFLOW `Type = C` and maps circular concrete pipe
  assumptions retained from its former demo.

The migration deliberately does not infer missing box width, material or inlet
configuration. Unsupported source combinations fail closed. Broader project-export
mapping in [`tuflow_to_hy8.py`](../ryan-scripts/tuflow/tuflow_to_hy8.py) remains a separate workflow.

## Engine differences

The engines are not asserted to be numerically identical. Inlet configuration and
coefficient libraries, solver formulations, convergence behaviour and supported
hydraulic states can differ.

For the HY-8 backend the migration preserves the old demo inlet assumptions:
square-edge headwall for circular concrete and thin-edge projecting for circular
corrugated steel. A high artificial roadway crest keeps roadway overtopping outside
the intended migrated demo calculations.

The `ryan-culverts` backend now applies the matching public coefficient selection:
its circular concrete default corresponds to the square-edge assumption, while the
corrugated-steel Maximums path explicitly selects the projecting CMP inlet and
entrance-loss coefficients rather than the library's headwall default. The engines
can still differ in formulations and convergence behaviour, so compare them
explicitly before treating them as interchangeable for design acceptance.

## Output

Both wrappers print their embedded wrapper revision and installed library version
at startup and completion, including processing failures. Native runs do not create
HY-8 workspace directories even when workspace options are supplied.

Both wrappers write long-form CSV. Every scenario row records the source, crossing,
selected engine, requested and computed hydraulic values, HW/D, outlet velocity,
flow type, roadway discharge/overtopping, status, warnings, failure text and any
retained HY-8 workspace. The Maximums output also records the selected base run,
and the `1d_nwk` output preserves the original source-row number after filtering.
Failed inputs and scenarios remain visible rather than being silently dropped.
A native result whose solver status is `unresolved` is retained in the CSV but
causes a non-zero wrapper exit status.

## Repository boundary

`ryan-tools` owns TUFLOW field interpretation, engine selection and batch output.
`run-hy8` owns HY-8 serialization, execution and parsing. `ryan-culverts` owns
native culvert hydraulic equations. Missing hydraulic capability belongs in the
appropriate engine rather than being reimplemented in this integration layer.
