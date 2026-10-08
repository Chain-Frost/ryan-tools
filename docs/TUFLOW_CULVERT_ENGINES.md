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
python ryan-scripts/tuflow/tuflow_culvert_from_maximums.py maximums.xlsx --culvert-attributes culvert-attributes.csv --engine hy8
python ryan-scripts/tuflow/tuflow_culvert_from_maximums.py maximums.xlsx --culvert-attributes model.gpkg --attributes-layer 1d_nwk --engine ryan-culverts
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

Physical configuration is explicit and is **not inferred from Manning roughness
or TUFLOW loss coefficients**. A mixed network can supply per-crossing
`Material` and `Inlet Configuration` through `--culvert-attributes`, using a
CSV or vector layer keyed by `ID`, `Chan ID` or `Crossing`. Global
`--material` and `--inlet-configuration` overrides are available only when the
whole selection genuinely shares those properties.

Internally, TUFLOW `Type = C` maps to the circular physical configuration
family. Material and inlet treatment are stored together in a typed
`CircularCulvertConfiguration`; numeric TUFLOW loss coefficients are stored
separately in `TuflowLossParameters`.

## TUFLOW 1d_nwk

```powershell
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --engine ryan-culverts
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --layer 1d_nwk --engine hy8
python ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py model.gpkg --layer 1d_nwk --culvert-attributes material-map.csv --engine hy8
```

The default inverse checks are HW/D 1.5 and 2.0. Use `--headwater-ratios`
to supply another non-empty set of unique, finite, positive ratios. The wrapper
expects an SI TUFLOW model. When negative `Len_or_ANA` requests digitised line
length, the GIS layer must have a projected CRS whose horizontal axes are in metres.
Unselected non-circular channel types are excluded from bulk `1d_nwk` runs;
explicitly requested non-circular channels report an input mapping error. Point-based
`Type = C` pit inlets are not circular culvert barrels and are rejected based on
geometry type. Malformed length and CRS errors are reported for the affected feature
rather than aborting an otherwise usable batch.

Both migrated wrappers refuse to replace an existing result CSV unless
`--overwrite` is supplied. Numeric `pBlockage` is applied to circular pipes in
both Maximums and `1d_nwk` inputs by scaling the hydraulic diameter by the square
root of the unblocked area fraction. The original nominal diameter is retained
separately and remains the denominator for TUFLOW HW/D targets and reported HW/D.
Category-based blockage is rejected until its event-specific percentage has been
resolved. A blank character `pBlockage` is also rejected because a TUFLOW
`Blockage Default` may apply; a blank numeric blockage remains the ordinary 0%
case. Fully blocked (100%) culverts are rejected rather than converted to a
zero-diameter solver object. This is the reduced-area blockage approximation,
not TUFLOW's alternative increased-energy-loss blockage method. Confirm that
Maximums `Height` represents nominal diameter and that processed input dimensions
have not already been adjusted for blockage, otherwise applying `pBlockage` again
would double-count the adjustment. Event-based category blockage and energy-loss
blockage require a separate processed-data mapping.

The `1d_nwk` wrapper also honours the TUFLOW `Ignore` field, requires a
traceable non-blank culvert ID, treats `Number_of = 0` as one barrel, requires a
positive source Manning roughness, and rejects the TUFLOW `-99999` invert
sentinel until effective inverts have been resolved from processed TUFLOW data.
The Maximums wrapper also rejects unresolved `-99999` inlet/outlet inverts and
nonfinite discharge values when selecting the governing event.

Loss attributes may come from the input row or the optional per-crossing attribute
source. CSV/EOF-style names `Entry Loss`, `Exit Loss` and `Fixed Loss` are
recognised, as are the canonical `1d_nwk` names `EntryC_or_WSa`,
`ExitC_or_WSb`, `Form_Loss` and `WConF_or_WEx`. The external source takes
precedence for fields it supplies. It does not replace geometry, inverts, diameter
or barrel count.

Loss coefficients do not select the physical inlet enum. They are retained as
numeric TUFLOW model parameters. The native engine can honour an explicit
`EntryC` independently of the physical inlet-control coefficient set; unsupported
exit/form/contraction overrides fail closed. Source `EntryC` above 1.0 is retained
for auditing while the effective TUFLOW inlet loss is clipped to 1.0 for hydraulic
application. HY-8 currently obtains its outlet-loss
behaviour from the selected physical inlet configuration, so a supplied `EntryC`
is accepted only when it matches that physical configuration's standard value.
Broader arbitrary TUFLOW loss overrides require engine support rather than being
silently converted into another inlet type.

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
a typed physical configuration (shape/material/inlet), hydraulic diameter,
nominal HW/D diameter, length, inlet/outlet invert, Manning roughness, barrel
count, and separate TUFLOW loss parameters.
TUFLOW parsing therefore happens before engine dispatch. Adverse slopes are retained
in the shared definition and passed to HY-8; the `ryan-culverts` backend rejects
these slopes explicitly because its current geometry contract does not support them.

The migrated workflows are circular-only: TUFLOW `Type = C` is accepted and
`Type = R` remains outside this adapter. Material and inlet treatment are independent explicit physical inputs, not a
consequence of `Type`, Manning roughness, `EntryC`, or other loss coefficients.
Supported explicit materials are reinforced concrete pipe, corrugated steel pipe
and smooth HDPE.

HY-8 supports all three explicit circular materials. The native `ryan-culverts`
path currently fails closed for HDPE because the TUFLOW adapter does not yet supply
an explicit native HDPE inlet-coefficient set; it does not borrow concrete or CSP
coefficients. The migration deliberately does not infer missing box width, material
or inlet configuration. Unsupported source combinations fail closed. Broader
project-export mapping in [`tuflow_to_hy8.py`](../ryan-scripts/tuflow/tuflow_to_hy8.py)
remains a separate workflow.

**TODO (deferred, out of scope of this PR):** Complete native HDPE support before
advertising `SMOOTH_HDPE` as solvable through the generic `culvert_solver`
adapter. The generic `CircularBarrelDefinition` schema currently accepts HDPE,
but `build_solver_crossing()` does not supply verified HDPE inlet-control and
entrance-loss coefficients, so native evaluation cannot succeed. Add material-
specific coefficient mappings, explicit validation, and regression tests when
HDPE support is prioritised. Until then, use the supported HY-8 path for HDPE.

## Engine differences

The engines are not asserted to be numerically identical. Inlet configuration and
coefficient libraries, solver formulations, convergence behaviour and supported
hydraulic states can differ.

For the HY-8 backend the selected physical `CircularInletConfiguration` is
mapped to the corresponding material-specific HY-8 inlet enum. The adapter does
not use `EntryC` or Manning roughness to choose that enum. A high artificial
roadway crest keeps roadway overtopping outside the intended migrated demo
calculations.

The `ryan-culverts` backend maps the same physical configuration to its public
inlet-control and outlet-control coefficient sets where a verified mapping exists.
For example, square-edge concrete headwall and projecting/mitered CSP treatments
use their matching native coefficient records. Unsupported physical combinations
fail closed instead of borrowing another inlet type. A numeric TUFLOW `EntryC`
override is attached separately to the native barrel for outlet-control loss
resolution and does not alter the inlet-control selection. The engines can still
differ in formulations and convergence behaviour, so compare them explicitly
before treating them as interchangeable for design acceptance.

### Updated ryan-culverts CSP defaults (2026-10-08)

The vendored `ryan-culverts` reference includes its projecting-end CSP default
(`Ke = 0.9`) and a sourced MRWA diameter/corrugation roughness resolver. The generic
Austroads CSP roughness fallback is opt-in. The TUFLOW adapter **does not** infer
physical inlet geometry from those library defaults, and does not replace the
Manning `n` read from the source TUFLOW network/EOF. Its explicit per-crossing
physical inlet selection overrides the solver's default; missing source roughness
is a mapping failure. The native solver also has no HDPE default inlet coefficients.

## Output

Both wrappers print their embedded wrapper revision and installed library version
at startup and completion, including processing failures. Native runs do not create
HY-8 workspace directories even when workspace options are supplied.

Both wrappers write long-form CSV. Every scenario row records the source, crossing,
selected engine, requested and computed hydraulic values, HW/D, outlet velocity,
flow type, roadway discharge/overtopping, status, warnings, failure text and any
retained HY-8 workspace. The Maximums output also records the selected base run,
and the `1d_nwk` output preserves the original source-row number after filtering.
Both outputs include physical input audit columns for material, inlet arrangement,
nominal/effective diameter, length, inverts, Manning roughness, barrel count,
raw/effective EntryC, remaining losses and scenario tailwater elevation.
Failed inputs and scenarios remain visible rather than being silently dropped.
A native result whose solver status is `unresolved` is retained in the CSV but
causes a non-zero wrapper exit status.

## Repository boundary

`ryan-tools` owns TUFLOW field interpretation, engine selection and batch output.
`run-hy8` owns HY-8 serialization, execution and parsing. `ryan-culverts` owns
native culvert hydraulic equations. Missing hydraulic capability belongs in the
appropriate engine rather than being reimplemented in this integration layer.
