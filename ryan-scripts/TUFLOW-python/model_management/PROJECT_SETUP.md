# TUFLOW standard project setup

Status: implemented and validated on 29 August 2026.

## Goal

Create a reusable script and template set so that a new TUFLOW project can be initialised with one command, producing a compact folder structure, boilerplate control files, projection files, TUFLOW-generated canonical empty files, and empty scenario GIS layers.

## Implemented workflow

The standard wrapper and `tuflow-initialiser init` generate the same starter project.
`tuflow-initialiser build` starts with that project, then populates model GIS and adds terrain and hydrology data.
Both include four working layers, an active boundary read, the location style and default utilities.
The boundary layer remains empty until project-specific boundary features are supplied.

The maintained wrapper is [`init_tuflow_project.py`](init_tuflow_project.py). Reusable setup logic is in [`project_setup.py`](../../../ryan_library/orchestrators/tuflow/project_setup.py), and text templates are under [`tuflow_templates/`](../../../ryan_library/resources/tuflow_templates/).

The GeoPackage workflow deliberately follows TUFLOW's own QGIS plugin contract:

1. Create `model/gis/projection_<source-prj-stem>.gpkg`, `.prj`, and `.tif` from the supplied `.prj` file so the
   projection source remains visible in every filename.
2. Run `Create_Empties.tcf` with the selected TUFLOW executable using `-b -nmb`.
3. Let TUFLOW populate the project `empty/` directory with its authoritative `*_empty.gpkg` files.
4. Select the required geometry layer from each canonical empty GeoPackage.
5. Verify that the canonical source layer has zero features, then create a new zero-feature scenario GeoPackage with the source CRS and full field schema.

The initial working layers are:

| Empty source layer | Working layer |
| --- | --- |
| `2d_bc_empty.gpkg:2d_bc_empty_L` | `2d_bc_<scenario>_01_L.gpkg` |
| `2d_code_empty.gpkg:2d_code_empty_R` | `2d_code_<scenario>_01_R.gpkg` |
| `2d_loc_empty.gpkg:2d_loc_empty_R` | `2d_loc_<scenario>_01_R.gpkg` |
| `2d_rf_empty.gpkg:2d_rf_empty_R` | `2d_rf_<scenario>_01_R.gpkg` |

No working GeoPackage binaries are bundled as templates. The four earlier binaries were removed because they contained 44 project features and had a fixed CRS.
The generated `2d_loc` GeoPackage also receives a same-stem `.qml` sidecar copied from the maintained
`qgis-resources/styles/TUFLOW/2d_loc_qa_check.qml`. The wheel includes that same file for installed use.

The generated main control file follows the example project flag convention:

```text
<project>_01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~e5~_~s4~.tcf
```

It reads `runs/run_times_01.trd` for duration-dependent end times and `runs/model_cell_sizes_01.trd` for `VarCellSize`.
The starter `EVENTS_01.tef` defines the matching 22 durations with four- and five-digit event flags, including both `~DUR5~` and `~DUR4~`
aliases, plus starter AEP, temporal-pattern, rainfall-method and climate events.
Both event spellings supply a four-digit `~DUR4~` and a five-digit `~DUR5~` substitution. Review the BC database naming before choosing flags. The 9-hour event
(`00540m` or `0540m`) has an editable starter end time of 18 hours. All end times require project-specific review.
Climate selection uses `~e5~` independently of the rainfall method (`~e4~`); `CCNear` supplies the starter `~CC~`
substitution, which must be reviewed against the rainfall database. `EnsembleILCL` no longer implicitly selects climate.
Existing generated projects and launch commands are not migrated automatically: new projects must supply `-e5`.
`OutputBase` defaults to `..`, while `OutputRoot` contains the relative results hierarchy including the climate flag;
output, log and check folders share that base. Map output explicitly stores maximums only, with the global,
TIF and HRTIF map intervals all set to zero. Time-series output retains its separate `OutInterval` setting.
Grid writing defaults to TIF; the paired GDAL/COG commands remain commented options for a verified application environment.
An unrecognised rainfall method stops through a TUFLOW `Pause` diagnostic. The TCF contains a example project rainfall
method block for selecting the BC database and `InitialLoss`/`ContLoss`; its database paths and loss values are
explicitly project-specific and must be reviewed before running a model.

The initializer does not create a root `check` directory or `model/xf`. TUFLOW can create result-specific check-file
folders later when a model is run.

The starter loss setup is complete across the control files: the TCF reads `model/Soil_01.tsoilf`, its rainfall-method
block sets `InitialLoss` and `ContLoss` directly for PMP/nested inputs or reads `model/soils_losses_01.trd` for
`EnsembleILCL`, and `GEOM_01.tgc` selects soil 99 (no infiltration) or soil 100 (variable IL/CL)
from the active method event. These project-specific loss values must still be reviewed before simulation.
The loss file starts at 0 mm and 0 mm/h; these disable infiltration losses until reviewed and are not calibrated defaults.
Add assessed climate/AEP loss branches there as needed, setting both variables in every branch. Check how preburst
is included in the rainfall inputs before applying additional losses.

The event file also includes `00.2p` and `00.5p` AEP substitutions and separate `DummyTP` and `DummyDUR` aliases,
while retaining `Dummy` for existing temporal-pattern inputs. `DummyDUR` substitutes only `~DUR4~`; it does not
select an end time or set `~DUR5~`. Supply matching boundary data and an explicit project end-time branch
in `runs/run_times_01.trd` before using it. Local/regional inflow selection, ESTRY connections, spatial loss zones
and catchment-specific end times require project design and are not enabled by the generic templates.

## Usage

```powershell
python .\ryan-scripts\TUFLOW-python\model_management\init_tuflow_project.py `
  --output "E:\Projects\MyProject\tuflow" `
  --name "MyProject" `
  --scenario "bigModel" `
  --prj "E:\Projects\MyProject\MGA2020_50.prj" `
  --tuflow-exe "C:\TUFLOW\tuflow-2026.3\TUFLOW_iSP_w64.exe" `
  --no-pause
```

The TUFLOW executable has an editable wrapper default. The `--prj` argument is mandatory because the canonical empty
files must be generated in the model CRS; omitting it produces an argument error before any project files are written.
Existing project files are protected unless the explicit `--overwrite` option is used. The workflow stops if a
selected `*_empty` source layer contains any feature; it will never derive working GIS from a populated source.

By default, the wrapper also copies a small set of maintained utilities from the repository's
`ryan-scripts/TUFLOW-python` folder into the generated project. The orchestrator resolves this default relative to its
own location in the ryan-tools checkout, so a copied wrapper does not resolve utility paths relative to itself. The same
utilities are included in the `ryan-tools` wheel and are selected automatically when the repository folder is not
available. Use `--no-copy-utilities` to omit them or change the editable `DEFAULT_COPY_UTILITIES` value.

Utility copying is best-effort. A missing source, an existing destination when overwrite is disabled, or another copy
error is logged and skipped without stopping the required project setup workflow.

| Destination | Utilities |
| --- | --- |
| `model/gis` | `rename_geopackage_layer_to_filename.py` |
| `results` | `combine_culvert_maximums.py`, `apply_qgis_styles_to_results.py`, `check_timeseries_stability.py`, `combine_pomm_results.py`, `create_pomm_mean_peak_report.py`, `run_asc_to_asc_mean_then_maximum.py` |
| Project root | `run_tuflow_simulations.py`, `create_log_summary_report.py` |

## Editor configuration and model packaging

Each generated project includes `.vscode/settings.json` with TUFLOW log icon associations,
TUFLOW Validator hints, project-folder watcher exclusions and the system Python environment,
plus `.vscode/extensions.json` with recommended VS Code extensions. These recommendations
are not required for project generation.

The root `package_model.bat` runs the configured TUFLOW executable using
`-pmAllini pm.ini -xf0` against the generated main TCF. It runs from the project root
regardless of the caller's working directory, checks for the executable/TCF/INI and
reports the tool's exit code. Edit `pm.ini` before running: the starter
`Copy Destination` is `C:\Temp\<project name>`, substituted from the chosen project
name. Change the destination for local disk space and storage policy as appropriate.

TUFLOW's package-model mode does not validate the inputs, and `-pmAll` can package
additional related file extensions. `-xf0` excludes processed XF files. Review whether
the particular project needs explicit `Model Scenario` and `Model Event` entries in
`pm.ini` to resolve every scenario/event filename before relying on the archive.
No packaging execution is part of the initializer or automated tests.

## Validation evidence

- Focused project-setup tests verify schema-derived output, zero output features, expected names, projection files, and unsafe scenario-name rejection.
- A live smoke test used the current `C:\TUFLOW\tuflow-2026.3` release and a local-grid `.prj`. TUFLOW generated the canonical empty files successfully.
- All four working GeoPackages had zero rows, the expected geometry and complete TUFLOW fields, and the generated local-grid CRS (`srs_id 99999` in that smoke test).
- The wrapper compiles, renders `--help`, and passes strict Pyright and the Loguru formatting check.

## Reference findings

`F:\Temp\TUFLOW_MLGD` uses the same concise `Create_Empties.tcf` pattern. Its broader project complexity was not
copied, but the useful control-file separation was retained. The QGIS plugin references were `utils/project.py` for
projection/empty generation and `utils/tf_empty.py` for deriving a new geometry-specific working layer from an empty
source schema.

The templates remain a starting point. Project terrain, boundary databases, event logic, output selections, and any ESTRY configuration still require project-specific editing.

MCP discovery assessment: this workflow is not catalogued. It is a mutating project-bootstrap operation with an external TUFLOW executable and several human-edited modelling choices, so the explicit wrapper remains the safer discovery and execution boundary.

## Important implementation detail

Fiona 1.10 does not expose every numeric attribute from the observed TUFLOW-generated `2d_bc_empty` schema. The workflow therefore reads the GeoPackage table definition for the complete field list and uses Fiona for standards-compliant spatial output. This retains `f`, `d`, `td`, `a`, and `b`. Canonical empty inputs and the derived working layers contain no source features.

## Remaining optional work

- Add an ESTRY ECF template only when a concrete project requires ESTRY.
- Expand the starter working-layer list only when another layer is genuinely part of the standard project baseline.
- Refine project-specific BC database paths, loss values, and event coverage as each model requires.
