# Culvert analysis and design workflow

The maintained culvert workflow provides project-level solve, comparison, rating-curve and design-search coordination in
`ryan-tools` while keeping `culvert_solver` authoritative for all culvert hydraulics.

## Architecture

The workflow is split by repository responsibility:

- `ryan_library/classes/culvert/` owns typed project, crossing, scenario, alternative, criteria, candidate and workflow
  result models.
- `ryan_library/functions/culvert/` owns the bounded `culvert_solver` adapter, candidate generation, design assessment,
  project JSON loading and machine-readable export.
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
- fixed tailwater elevation in JSON project files;
- any public `culvert_solver.TailwaterBoundary` when constructing `Scenario` from Python;
- base crossings, scenarios and named alternatives;
- explicit circular or rectangular size/quantity candidate generation for a single-group template crossing;
- design constraints for maximum headwater elevation, headwater depth, outlet velocity and roadway discharge;
- preservation of complete `CrossingHydraulicResult` objects, structured warning messages, applicability notices,
  source references and tailwater-resolution provenance;
- CSV, JSON and Markdown scenario summaries;
- JSON and Markdown design-search summaries;
- crossing rating curves delegated to `culvert_solver`.

GUI, plotting, PDF and elaborate Excel reporting are intentionally deferred.

## Project JSON

A minimal fixed-tailwater project is:

```json
{
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
            "length": 40,
            "inlet_invert": 10.0,
            "outlet_invert": 9.5,
            "roughness": 0.013,
            "material": "concrete_pipe"
          }
        }
      ]
    }
  ],
  "scenarios": [
    {
      "name": "Design",
      "discharge": 4.0,
      "tailwater_elevation": 10.0
    }
  ]
}
```

Rectangular barrels use `shape: "rectangular"`, `span_mm`, `rise_mm` and `material: "concrete_box"`. Circular barrels
use `diameter_mm` with `material: "concrete_pipe"` or `"corrugated_steel"`.

An optional crossing `roadway` object accepts `crest_elevation`, `crest_length`, `discharge_coefficient` and optional
`label`. An optional top-level `alternatives` array contains objects with `name` and a complete nested `crossing`.

## Wrapper use

Typical commands are:

```powershell
python ryan-scripts/culvert.py analyse --project C:\Project\culvert_project.json --no-pause
python ryan-scripts/culvert.py solve --project C:\Project\culvert_project.json --crossing "Crossing A" --scenario Design
python ryan-scripts/culvert.py design --project C:\Project\culvert_project.json `
    --diameters-mm 900 1200 1500 --quantities 1 2 3 --max-headwater-elevation 11.5
python ryan-scripts/culvert.py rating --project C:\Project\culvert_project.json `
    --min-discharge 0.5 --max-discharge 8 --points 16
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

## Hydraulic boundary

Workflow code may select candidates, apply project criteria and format results, but it must not reproduce hydraulic
relationships from `ryan-culverts`. Missing hydraulic behaviour belongs upstream in `ryan-culverts` and should be added
there before being exposed through this workflow.
