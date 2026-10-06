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
- summary CSV plus detailed JSON and Markdown scenario outputs;
- JSON and Markdown design-search summaries;
- crossing rating curves delegated to `culvert_solver`;
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
`scenario_results.json`, `scenario_results.csv` and `scenario_results.md`. The CSV is a compact summary; the JSON retains
per-group hydraulic state, critical/normal depth, adopted coefficient and roughness provenance, loss components,
convergence evidence, warnings, applicability notices and tailwater-resolution provenance. `design` writes
`design_results.json` and `design_results.md`; the JSON retains the applied criteria and complete candidate definitions.
`rating` writes `rating_curve.csv` and `rating_curve.json`, with the JSON retaining warnings and tailwater provenance.

## Design-search behaviour

Automatic candidate generation is deliberately bounded in the first increment. It accepts explicit candidate dimensions
and quantities and varies one single-group template crossing. Mixed-group candidate generation is not inferred because
there is no unambiguous policy for which groups should be resized or duplicated.

Every candidate is solved independently for every requested scenario. Rejected candidates retain scenario-specific
failure reasons. Passing candidates are ranked first by total full-flow area and then by total barrel count. This is a
simple hydraulic-size ranking, not a cost or constructability optimisation.

## Imported events

`load_event_csv` imports externally supplied event rows with a name, AEP, or both. Each row must contain exactly one
of `discharge_m3s` or `target_headwater_elevation_m` and may override `tailwater_elevation_m`. The equivalent Python/list
API is a `Sequence[EventDefinition]` passed directly to `materialize_event_scenarios`; no CSV round trip is required.

`materialize_event_scenarios` passes supplied discharges through unchanged and resolves headwater targets with the
public `culvert_solver.solve_crossing_discharge_for_headwater` API. Source, notes, AEP, target headwater, resolved
discharge and any event-level tailwater override remain in structured scenario results and exports. A subsequent forward
solve reports the target-headwater residual (`solved HW - requested HW`) so the inverse result is auditable. This workflow
performs no hydrology.

The maintained wrapper accepts the table through `--events-csv`. A `compare` run evaluates the same complete
alternative/scenario matrix as `analyse` and writes deterministic comparison outputs. A later `report` command reads
the saved `scenario_results.json` and regenerates Markdown without loading a project or rerunning hydraulics.

## Hydraulic boundary

Workflow code may select candidates, apply project criteria and format results, but it must not reproduce hydraulic
relationships from `ryan-culverts`. Missing hydraulic behaviour belongs upstream in `ryan-culverts` and should be added
there before being exposed through this workflow.

## Uncertainty and sensitivity studies

Project schema version 1 accepts an optional `uncertainty_studies` array. The
[complete TOML example](../examples/culvert_project.toml) includes a synthetic bounded roughness study.
Use project evidence for bounds and source references before applying it to engineering work.

```powershell
python ryan-scripts/culvert.py uncertainty --project C:\Project\culvert_project.toml `
    --study "Roughness sensitivity" --console-log-level SUCCESS --no-pause
```

The Python entry point is `ryan_library.orchestrators.culvert.run_uncertainty_study(project, study)`.
`UncertaintyStudy` and study results belong to the application layer; bounds, distributions, parameter identities,
samples and expected failure records use the public `culvert_solver` contracts directly. Hydraulic calculations use
`solve_crossing_hydraulics`, including upstream group allocation, roadway flow and tailwater resolution.

Every study requires `name`, `sampling_mode`, `sample_count` and a nonempty `parameters` array. Each parameter requires
`parameter`, `bounds` (`lower`, `upper`, `unit`) and `source` (`source_id`, `publication`, `edition`, `locator`,
`applicability`; optional `url` and `notes`). Supported identities and canonical units are:

| Parameter | Unit | Application |
| --- | --- | --- |
| `manning_roughness` | `s/m^(1/3)` | Replace roughness in every barrel group; retain its source as a user override. |
| `entrance_loss_coefficient` | `1` | Attach a sourced coefficient to every barrel group. |
| `discharge` | `m3/s` | Replace total crossing discharge; let the solver allocate flows. |
| `tailwater_elevation` | `m` | Replace downstream boundary with an absolute elevation. |

Values are absolute SI inputs, not multipliers or relative perturbations. Unvaried inputs retain their base definitions;
unvaried flow-dependent tailwater is resolved at the sampled discharge. Group-specific variation and correlated
distributions are not represented by this study policy.

`bounded_sweep` forms a deterministic Cartesian grid with `sample_count` points per parameter, inclusive of both bounds
when count is at least two; a single point uses the midpoint. It does not accept a seed. With `k` parameters the sample
count is `sample_count ** k`. `monte_carlo` uses uniform bounds and requires an integer `seed`; it creates `sample_count`
multi-parameter samples using the public sampler with `seed + parameter_index` for each parameter stream. Parameter
order therefore forms part of reproducibility. Reproducibility assumes the same input definitions and solver version.

Optional `crossing_names`, `scenario_names` and `alternative_names` select exact project names. Omitted or empty lists
select all available members within an enabled target class; unknown or duplicate names fail explicitly.
`include_base_crossings = false` disables base crossings, while `include_alternatives = false` disables alternatives.
Both flags default to `true`, preserving the existing all-target behavior. A disabled alternative class requires
`alternative_names` to be empty. Setting `crossing_names` filters base crossings independently of the alternative
selection; it does not imply a relationship between a crossing and an alternative. The entire matrix is checked against
`maximum_evaluations` (default 10000) before samples are allocated or hydraulic evaluation starts. CLI `--crossing` and
`--scenario` are rejected for uncertainty; put selections in the study. `--study` defaults to the editable wrapper
setting or the first configured study. Imported `--events-csv` flows can supply the scenario matrix through the existing
event boundary; they remain externally supplied hydrology. Discharge rows are geometry-independent. A
`target_headwater_elevation_m` row is an inverse hydraulic solve and therefore requires the selected study to resolve
to exactly one base crossing or alternative; the wrapper uses that target geometry and rejects ambiguous multi-target
studies instead of deriving one discharge from an unrelated crossing.

Every evaluation retains its sample, crossing, scenario, alternative, sources and full hydraulic result or expected
failure. Input-domain and convergence exceptions become public `HydraulicEvaluationFailure` records with category,
exception type, message, bracket and iteration evidence where supplied. Unexpected programming errors propagate.
Valid, advisory, approximate and unresolved results retain their authoritative status and warnings; failed evaluations
have a distinct `failed` outcome and no invented hydraulic result.

Statistics are computed separately for each crossing/scenario/alternative. `aggregation_statuses` defaults to
`["valid", "valid_with_advisory"]`; `"approximate"` can be explicitly included. Unresolved and failed outcomes always
remain outside the statistics but visible in status counts and detailed output. Each metric includes its finite-value
count, minimum, maximum, mean, configured percentiles and governing maximum sample ID. Defaults are P5/P50/P95;
percentiles use linear interpolation at `(n-1)*p/100`. Empty populations have null statistics and no percentiles.
These are conditional sample descriptions, not confidence limits; bounded-grid percentiles imply no probability model.

Outputs include headwater elevation/depth, outlet velocity, total discharge, culvert/roadway discharge and each group
discharge. Sampled discharge is an imposed flow, not a solved capacity. The separate design-search API remains the
boundary for project acceptance criteria and candidate ranking; uncertainty summaries do not choose a preferred
alternative across unrelated crossing sites.

The wrapper writes `uncertainty_results.json` with study policy, input project, every evaluation and detailed solver
evidence. Its project field is an audit snapshot, not a project-file interchange document. Public fixed, channel and rating
tailwater boundaries retain their typed input definitions. Other Python boundary implementations are identified by type
with an explicit unavailable-definition marker; retain their external configuration separately. Imported event overrides
remain in the input snapshot even when a sample replaces the applied tailwater. The result-level event-override flag
then correctly describes the applied boundary.

It also writes `uncertainty_results.csv` (one row per outcome, including failures, with sourced parameter JSON and
warning/failure cells), `uncertainty_summary.csv` (one row per metric with denominators, percentiles and governing sample
IDs) and `uncertainty_results.md` (concise review summary). Exit code 0 means all outcomes belong to the declared statistics
population; 2 means outputs were written with failed or excluded outcomes requiring review; 1 means a configuration,
execution or export failure. Existing files at these output names are overwritten, as with the other wrapper commands.
