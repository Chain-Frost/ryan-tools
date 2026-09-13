# Culvert analysis and design workflow

The maintained culvert workflow provides project-level solve, comparison, rating-curve and design-search coordination in
`ryan-tools` while keeping `culvert_solver` authoritative for all culvert hydraulics.

## Architecture

The workflow is split by repository responsibility:

- `ryan_library/classes/culvert/` owns typed project, crossing, scenario, alternative, criteria, candidate and workflow
  result models.
- `ryan_library/functions/culvert/` owns the bounded `culvert_solver` adapter, candidate generation, design assessment,
  strict versioned JSON/TOML loading and machine-readable export.
- `ryan_library/orchestrators/culvert/` owns solve, batch analysis, design search, rating-curve coordination and Markdown
  reporting.
- `ryan-scripts/culvert.py` is the maintained human-facing process boundary.

No HDS-5 or other culvert hydraulic equations are implemented in these workflow modules. Crossing definitions are
converted to public `culvert_solver` objects and the public solver APIs perform the hydraulic calculations.

## Supported first increment

The initial workflow supports:

- rectangular concrete box barrels;
- circular concrete-pipe and corrugated-steel barrels;
- multiple identical barrels per group;
- multiple groups per crossing for normal solve/analysis;
- optional constant-elevation roadway-weir data;
- fixed-elevation and rectangular/trapezoidal Manning-channel tailwater in project files;
- any public `culvert_solver.TailwaterBoundary` when constructing `Scenario` from Python;
- base crossings, scenarios and named alternatives;
- explicit circular or rectangular size/quantity candidate generation for a single-group template crossing;
- design constraints for maximum headwater elevation, headwater depth, outlet velocity and roadway discharge;
- preservation of complete `CrossingHydraulicResult` objects, structured warning messages, applicability notices,
  source references and tailwater-resolution provenance;
- CSV, JSON and Markdown scenario summaries;
- JSON and Markdown design-search summaries;
- crossing rating curves delegated to `culvert_solver`.
- longitudinal profile and rating plots generated from already-computed structured results.

GUI work is future aspirational issue #89 and is outside PR #86. Plotting remains tracked by #84.

## Plotting

`plot_longitudinal_profile` consumes a `ScenarioResult` containing a solver-computed profile and plots invert, crown,
water surface, energy grade, headwater, tailwater and any reported hydraulic-jump station.
`plot_rating_curve` consumes a `CrossingRatingResult` and plots headwater elevation and outlet velocity against
discharge. Neither function solves or reinterprets hydraulics. Both return a Matplotlib `Figure`; callers can display it
interactively or use `save_figure` for PNG, SVG, PDF or another Matplotlib-supported headless format.

## Project files

Project files require `schema_version = 1`. Unknown fields are rejected, unsupported schema versions fail explicitly,
and all dimensional configuration keys include units. JSON and TOML use the same schema; JSON is also the deterministic
interchange format. See the [complete TOML example](../examples/culvert_project.toml) for mixed groups, roadway
overtopping, provenance metadata, alternatives and Manning-channel tailwater.

A minimal fixed-tailwater JSON project is:

```json
{
  "schema_version": 1,
  "name": "Demo",
  "crossings": [
    {
      "name": "Crossing A",
      "groups": [
        {
          "name": "Pipes",
          "quantity": 2,
          "barrel": {
            "shape": "circular",
            "diameter_mm": 1200,
            "length_m": 40,
            "inlet_invert_elevation_m": 10.0,
            "outlet_invert_elevation_m": 9.5,
            "roughness_manning_n": 0.013,
            "material": "concrete_pipe"
          }
        }
      ]
    }
  ],
  "scenarios": [
    {
      "name": "Design",
      "discharge_m3s": 4.0,
      "tailwater": {
        "type": "fixed",
        "elevation_m": 10.0
      }
    }
  ]
}
```

Rectangular barrels use `shape: "rectangular"`, `span_mm`, `rise_mm` and `material: "concrete_box"`. Circular barrels
use `diameter_mm` with `material: "concrete_pipe"` or `"corrugated_steel"`.

An optional crossing `roadway` object accepts `crest_elevation_m`, `crest_length_m`, `discharge_coefficient` and
optional `label`. An optional top-level `alternatives` array contains objects with `name` and a complete nested
`crossing`. Project, crossing, scenario and alternative objects accept optional `source` and `notes`; scenarios also
accept `aep_percent`.

There are no implicit override layers in schema version 1: every crossing and alternative contains its complete
hydraulic definition. Parser defaults are limited to group `quantity = 1`, empty alternatives and omitted optional
metadata. Migration is deliberately explicit: a file with any other schema version is rejected instead of guessed.

## Wrapper use

Typical commands are:

```powershell
python ryan-scripts/culvert.py analyse --project C:\Project\culvert_project.json --no-pause
python ryan-scripts/culvert.py compare --project C:\Project\culvert_project.toml --no-pause
python ryan-scripts/culvert.py solve --project C:\Project\culvert_project.json --crossing "Crossing A" --scenario Design
python ryan-scripts/culvert.py design --project C:\Project\culvert_project.json `
    --diameters-mm 900 1200 1500 --quantities 1 2 3 --max-headwater-elevation 11.5
python ryan-scripts/culvert.py rating --project C:\Project\culvert_project.json `
    --min-discharge 0.5 --max-discharge 8 --points 16
python ryan-scripts/culvert.py report --directory C:\Project --no-pause
python ryan-scripts/culvert.py analyse --project C:\Project\culvert_project.toml `
    --events-csv C:\Project\design_events.csv --no-pause
```

Outputs default to a `culvert_results` directory under the wrapper working directory. `solve` and `analyse` write
`scenario_results.json`, `scenario_results.csv` and `scenario_results.md`. `design` writes `design_results.json` and
`design_results.md`; the JSON retains the applied criteria and complete candidate definitions. `rating` writes
`rating_curve.csv` and `rating_curve.json`, with the JSON retaining warnings and tailwater provenance.

## Design-search behaviour

Automatic candidate generation is deliberately bounded in the first increment. It accepts explicit candidate dimensions
and quantities and varies one single-group template crossing. Mixed-group candidate generation is not inferred because
there is no unambiguous policy for which groups should be resized or duplicated.

Every candidate is solved independently for every requested scenario. Rejected candidates retain scenario-specific
failure reasons. Passing candidates are ranked first by total full-flow area and then by total barrel count. This is a
simple hydraulic-size ranking, not a cost or constructability optimisation.

## Imported events

`load_event_csv` imports externally supplied event rows with a name, AEP, or both. Each row must contain exactly one
of `discharge_m3s` or `target_headwater_elevation_m` and may override `tailwater_elevation_m`.
`materialize_event_scenarios` passes supplied discharges through unchanged and resolves headwater targets with the
public `culvert_solver.solve_crossing_discharge_for_headwater` API. Source, notes, AEP, target headwater and the resolved
discharge remain in structured scenario results and exports. This workflow performs no hydrology.

The maintained wrapper accepts the table through `--events-csv`. A `compare` run evaluates the same complete
alternative/scenario matrix as `analyse` and writes deterministic comparison outputs. A later `report` command reads
the saved `scenario_results.json` and regenerates Markdown without loading a project or rerunning hydraulics.

## Hydraulic boundary

Workflow code may select candidates, apply project criteria and format results, but it must not reproduce hydraulic
relationships from `ryan-culverts`. Missing hydraulic behaviour belongs upstream in `ryan-culverts` and should be added
there before being exposed through this workflow.
