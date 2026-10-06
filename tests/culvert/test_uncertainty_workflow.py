"""Study matrix, failure propagation, provenance and conditional aggregation contracts."""

import csv
import json
import runpy
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from culvert_solver import (
    BoundedParameterSpec,
    ConvergenceError,
    CrossingHydraulicResult,
    CulvertCrossing,
    HydraulicResultStatus,
    HydraulicUncertaintyParameter,
    HydraulicWarning,
    HydraulicWarningCode,
    InvalidInputError,
    ManningChannelTailwater,
    ParameterBounds,
    RectangularChannel,
    SourceReference,
    TailwaterInput,
    TailwaterRatingCurve,
    TailwaterRatingPoint,
    UniformParameterSpec,
)

from ryan_library.classes.culvert import (
    Alternative,
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertProject,
    RoadwayDefinition,
    Scenario,
    UncertaintySamplingMode,
    UncertaintyStudy,
)
from ryan_library.functions.culvert.config import export_project_json, load_project, project_record
from ryan_library.functions.culvert.uncertainty_export import (
    export_uncertainty_csv,
    export_uncertainty_json,
    export_uncertainty_summary_csv,
    uncertainty_result_record,
)
from ryan_library.orchestrators.culvert import run_uncertainty_study, solve_crossing_scenario
from ryan_library.orchestrators.culvert.uncertainty_report import render_uncertainty_markdown


def _source() -> SourceReference:
    return SourceReference(
        source_id="SYNTHETIC",
        publication="Synthetic test values",
        edition="1",
        locator="fixture",
        url=None,
        applicability="For workflow tests only.",
    )


def _spec(parameter: HydraulicUncertaintyParameter, lower: float, upper: float) -> BoundedParameterSpec:
    return BoundedParameterSpec(
        parameter=parameter, bounds=ParameterBounds(lower=lower, upper=upper, unit=parameter.unit), source=_source()
    )


def _crossing(name: str = "Existing", *, mixed: bool = False) -> CrossingDefinition:
    barrel = CircularBarrelDefinition(diameter_mm=1200, length=40, inlet_invert=10, outlet_invert=9.5, roughness=0.013)
    groups = (CulvertGroupDefinition(name="Pipes", barrel=barrel, quantity=2),)
    if mixed:
        groups += (CulvertGroupDefinition(name="Larger pipes", barrel=replace(barrel, diameter_mm=1500)),)
    return CrossingDefinition(
        name=name,
        groups=groups,
        roadway=RoadwayDefinition(crest_elevation=12, crest_length=8, discharge_coefficient=1.7) if mixed else None,
        source="Synthetic crossing",
        notes="Retain crossing provenance.",
    )


def _study(*, count: int = 2) -> UncertaintyStudy:
    return UncertaintyStudy(
        name="Sensitivity",
        sampling_mode=UncertaintySamplingMode.BOUNDED_SWEEP,
        parameters=(_spec(HydraulicUncertaintyParameter.DISCHARGE, 2, 4),),
        sample_count=count,
    )


def _project() -> CulvertProject:
    return CulvertProject(
        name="Synthetic matrix",
        crossings=(_crossing(), _crossing("Mixed", mixed=True)),
        scenarios=(
            Scenario(name="Fixed", discharge=3, tailwater=10, source="Test flow", aep_percent=1),
            Scenario(
                name="Channel",
                discharge=3,
                tailwater=ManningChannelTailwater(
                    section=RectangularChannel(bottom_width=5),
                    channel_invert_elevation=9.5,
                    roughness=0.03,
                    friction_slope=0.01,
                ),
            ),
        ),
        alternatives=(Alternative(name="Upgrade", crossing=_crossing("Proposed"), source="Test alternative"),),
        uncertainty_studies=(_study(),),
        source="Test project",
    )


def test_real_project_matrix_retains_sources_and_flow_splits(tmp_path: Path) -> None:
    study = replace(
        _study(),
        parameters=(
            _spec(HydraulicUncertaintyParameter.MANNING_ROUGHNESS, 0.012, 0.016),
            _spec(HydraulicUncertaintyParameter.ENTRANCE_LOSS_COEFFICIENT, 0.3, 0.7),
        ),
    )
    project = _project()
    result = run_uncertainty_study(project, study)
    assert len(result.evaluations) == 24
    assert len(result.summaries) == 6
    assert {item.alternative_name for item in result.evaluations} == {None, "Upgrade"}
    assert project == _project()
    for evaluation in result.evaluations:
        assert evaluation.result is not None
        hydraulic = evaluation.result.hydraulic_result
        assert hydraulic.total_discharge == 3
        assert hydraulic.culvert_discharge + hydraulic.roadway_discharge == pytest.approx(3)
        for group in hydraulic.group_results:
            assert group.barrel_result.roughness_source == _source()
            assert group.barrel_result.entrance_loss_selection is not None
            assert group.barrel_result.entrance_loss_selection.source == _source()
            assert group.barrel_result.adopted_roughness in {0.012, 0.016}
            assert group.barrel_result.entrance_loss_selection.ke in {0.3, 0.7}
    payload = cast(
        "dict[str, object]", json.loads(export_uncertainty_json(result, tmp_path / "study.json").read_text())
    )
    assert payload["study"] is not None
    assert "group_results" in json.dumps(payload)
    assert "SYNTHETIC" in json.dumps(payload)
    csv_path = export_uncertainty_csv(result, tmp_path / "study.csv")
    with csv_path.open(newline="", encoding="utf-8") as stream:
        assert len(list(csv.DictReader(stream))) == 24
    summary_path = export_uncertainty_summary_csv(result, tmp_path / "summary.csv")
    assert "maximum_sample_id" in summary_path.read_text()
    markdown = render_uncertainty_markdown(result)
    assert "Eligible / total" in markdown
    assert "confidence limits" in markdown


def test_sampled_discharge_resolves_channel_tailwater_at_sample_flow() -> None:
    result = run_uncertainty_study(
        _project(), replace(_study(), crossing_names=("Existing",), scenario_names=("Channel",))
    )
    for evaluation in result.evaluations:
        assert evaluation.result is not None
        resolution = evaluation.result.hydraulic_result.tailwater_resolution
        assert resolution is not None
        assert resolution.discharge == evaluation.sample.parameters[0].value


def test_real_unsupported_rating_curve_flow_retains_failure_and_boundary(tmp_path: Path) -> None:
    boundary = TailwaterRatingCurve(
        points=(TailwaterRatingPoint(discharge=1, elevation=9.7), TailwaterRatingPoint(discharge=3, elevation=10)),
        rating_curve_source=_source(),
    )
    project = CulvertProject(
        name="Rated tailwater",
        crossings=(_crossing(),),
        scenarios=(Scenario(name="Event", discharge=2, tailwater=boundary),),
    )
    result = run_uncertainty_study(project, _study())
    assert result.evaluations[0].result is not None
    failure = result.evaluations[1].failure
    assert failure is not None
    assert failure.kind.value == "invalid_input"
    assert "outside the tailwater rating-curve range" in failure.message
    path = export_uncertainty_json(result, tmp_path / "rated.json")
    payload = cast("dict[str, object]", json.loads(path.read_text(encoding="utf-8")))
    assert "TailwaterRatingCurve" in json.dumps(payload["project"])
    assert "SYNTHETIC" in json.dumps(payload["project"])


def test_sampled_tailwater_replaces_event_override_but_retains_original_provenance() -> None:
    scenario = Scenario(
        name="Imported",
        discharge=2,
        tailwater=10,
        tailwater_override_elevation=10,
        target_headwater_elevation=11,
        source="Imported event",
    )
    project = CulvertProject(name="Event override", crossings=(_crossing(),), scenarios=(scenario,))
    study = replace(_study(), parameters=(_spec(HydraulicUncertaintyParameter.TAILWATER_ELEVATION, 9.8, 10.2),))
    result = run_uncertainty_study(project, study)
    for evaluation in result.evaluations:
        assert evaluation.result is not None
        assert evaluation.result.hydraulic_result.tailwater_elevation == evaluation.sample.parameters[0].value
        assert not evaluation.result.tailwater_was_event_override
        assert evaluation.scenario.tailwater_override_elevation == 10
    assert "base_event_tailwater_override_elevation_m" in json.dumps(uncertainty_result_record(result))


def test_statuses_failures_and_percentiles_keep_population_explicit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    crossing = _crossing()
    scenario = Scenario(name="Event", discharge=2, tailwater=10)
    reference = solve_crossing_scenario(crossing, scenario).hydraulic_result
    statuses = tuple(HydraulicResultStatus)

    def solve(
        crossing: CulvertCrossing, *, total_discharge: float, tailwater: TailwaterInput
    ) -> CrossingHydraulicResult:
        del crossing, tailwater
        if total_discharge == 6:
            msg = "Synthetic convergence failure"
            raise ConvergenceError(msg, bracket=(10, 20), iterations=7)
        codes = (
            None,
            HydraulicWarningCode.INLET_CONTROL_HIGH_HEAD_EXTENSION,
            HydraulicWarningCode.INLET_OUTLET_DEPTH_APPROXIMATION,
            HydraulicWarningCode.MIXED_FLOW_NOT_RESOLVED,
        )
        code = codes[int(total_discharge) - 2]
        warnings = () if code is None else (HydraulicWarning(code=code, message="Synthetic status evidence"),)
        groups = tuple(
            replace(group, barrel_result=replace(group.barrel_result, warnings=warnings))
            for group in reference.group_results
        )
        return replace(reference, group_results=groups, headwater_elevation=total_discharge + 10)

    monkeypatch.setattr("ryan_library.functions.culvert.uncertainty_evaluation.solve_crossing_hydraulics", solve)
    study = replace(_study(count=5), parameters=(_spec(HydraulicUncertaintyParameter.DISCHARGE, 2, 6),))
    project = CulvertProject(name="Statuses", crossings=(crossing,), scenarios=(scenario,))
    result = run_uncertainty_study(project, study)
    assert len(result.evaluations) == 5
    summary = result.summaries[0]
    assert dict(summary.status_counts) == dict.fromkeys([status.value for status in statuses] + ["failed"], 1)
    assert summary.eligible_count == 2
    metric = next(metric for metric in summary.metrics if metric.metric == "headwater_elevation_m")
    assert metric.minimum == 12
    assert metric.maximum == 13
    assert metric.mean == 12.5
    assert dict(metric.percentiles) == {5: 12.05, 50: 12.5, 95: 12.95}
    assert metric.maximum_sample_id == "Sensitivity:bounded:1"
    failure = result.evaluations[-1].failure
    assert failure is not None
    assert failure.bracket == (10, 20)
    assert failure.iterations == 7
    assert "Synthetic convergence failure" in render_uncertainty_markdown(result)
    exported = export_uncertainty_csv(result, tmp_path / "failures.csv")
    with exported.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 5
    assert rows[-1]["status"] == "failed"
    assert rows[-1]["headwater_elevation_m"] == ""
    assert json.loads(rows[-1]["failure_json"])["iterations"] == 7
    included = run_uncertainty_study(
        project, replace(study, aggregation_statuses=(*study.aggregation_statuses, HydraulicResultStatus.APPROXIMATE))
    )
    assert included.summaries[0].eligible_count == 3


def test_all_failed_summary_has_no_fabricated_statistics(monkeypatch: pytest.MonkeyPatch) -> None:
    def solve(
        crossing: CulvertCrossing, *, total_discharge: float, tailwater: TailwaterInput
    ) -> CrossingHydraulicResult:
        del crossing, total_discharge, tailwater
        msg = "Synthetic unsupported input"
        raise InvalidInputError(msg)

    monkeypatch.setattr("ryan_library.functions.culvert.uncertainty_evaluation.solve_crossing_hydraulics", solve)
    result = run_uncertainty_study(_project(), _study())
    assert all(evaluation.failure is not None for evaluation in result.evaluations)
    assert all(summary.eligible_count == 0 for summary in result.summaries)
    assert all(
        metric.maximum is None and metric.percentiles == ()
        for summary in result.summaries
        for metric in summary.metrics
    )
    assert all(
        evaluation["result"] is None
        for evaluation in cast("list[dict[str, object]]", uncertainty_result_record(result)["evaluations"])
    )


def test_wrapper_retains_failed_output_and_returns_review_exit_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def solve(
        crossing: CulvertCrossing, *, total_discharge: float, tailwater: TailwaterInput
    ) -> CrossingHydraulicResult:
        del crossing, total_discharge, tailwater
        msg = "Synthetic wrapper failure"
        raise InvalidInputError(msg)

    monkeypatch.setattr("ryan_library.functions.culvert.uncertainty_evaluation.solve_crossing_hydraulics", solve)
    wrapper = runpy.run_path(str(Path(__file__).resolve().parents[2] / "ryan-scripts" / "culvert.py"))
    run = cast("Callable[[CulvertProject, str | None, Path], int]", wrapper["_run_uncertainty"])
    assert run(_project(), "Sensitivity", tmp_path) == 2
    payload = cast("dict[str, object]", json.loads((tmp_path / "uncertainty_results.json").read_text(encoding="utf-8")))
    assert "Synthetic wrapper failure" in json.dumps(payload)
    assert (tmp_path / "uncertainty_results.md").is_file()


def test_unexpected_programming_error_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    def solve(
        crossing: CulvertCrossing, *, total_discharge: float, tailwater: TailwaterInput
    ) -> CrossingHydraulicResult:
        del crossing, total_discharge, tailwater
        msg = "Programming defect"
        raise TypeError(msg)

    monkeypatch.setattr("ryan_library.functions.culvert.uncertainty_evaluation.solve_crossing_hydraulics", solve)
    with pytest.raises(TypeError, match="Programming defect"):
        run_uncertainty_study(_project(), _study())


def test_seeded_complete_study_is_reproducible() -> None:
    bounded = _study().parameters[0]
    study = replace(
        _study(),
        sampling_mode=UncertaintySamplingMode.MONTE_CARLO,
        seed=42,
        parameters=(UniformParameterSpec(parameter=bounded.parameter, bounds=bounded.bounds, source=bounded.source),),
    )
    assert run_uncertainty_study(_project(), study) == run_uncertainty_study(_project(), study)


def test_selection_limits_checked_before_sampling_or_solving(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(_study: UncertaintyStudy) -> None:
        pytest.fail("Sampling must not start for an invalid study matrix")

    monkeypatch.setattr("ryan_library.orchestrators.culvert.uncertainty.generate_study_samples", unexpected)
    with pytest.raises(ValueError, match="Unknown study selection"):
        run_uncertainty_study(_project(), replace(_study(), scenario_names=("missing",)))
    with pytest.raises(ValueError, match="exceeding maximum_evaluations"):
        run_uncertainty_study(_project(), replace(_study(), maximum_evaluations=1))
    project = replace(_project(), alternatives=())
    with pytest.raises(ValueError, match="no crossings or alternatives"):
        run_uncertainty_study(project, replace(_study(), include_base_crossings=False))


def test_alternative_only_study_has_no_base_results() -> None:
    result = run_uncertainty_study(
        _project(), replace(_study(), include_base_crossings=False, scenario_names=("Fixed",))
    )
    assert len(result.evaluations) == 2
    assert all(evaluation.alternative_name == "Upgrade" for evaluation in result.evaluations)


def test_base_only_study_can_exclude_all_alternatives() -> None:
    result = run_uncertainty_study(
        _project(),
        replace(
            _study(),
            crossing_names=("Existing",),
            scenario_names=("Fixed",),
            include_alternatives=False,
        ),
    )
    assert len(result.evaluations) == 2
    assert all(evaluation.crossing.name == "Existing" for evaluation in result.evaluations)
    assert all(evaluation.alternative is None for evaluation in result.evaluations)


def test_disabling_all_target_classes_is_rejected_before_sampling(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(_study: UncertaintyStudy) -> None:
        pytest.fail("Sampling must not start when both target classes are disabled")

    monkeypatch.setattr("ryan_library.orchestrators.culvert.uncertainty.generate_study_samples", unexpected)
    with pytest.raises(ValueError, match="no crossings or alternatives"):
        run_uncertainty_study(
            _project(),
            replace(_study(), include_base_crossings=False, include_alternatives=False),
        )


def test_project_studies_roundtrip_and_reject_unknown_fields(tmp_path: Path) -> None:
    project = _project()
    exported = export_project_json(project, tmp_path / "project.json")
    assert project_record(load_project(exported)) == project_record(project)
    assert load_project(exported).uncertainty_studies == project.uncertainty_studies
    raw = project_record(project)
    studies = cast("list[dict[str, object]]", raw["uncertainty_studies"])
    studies[0]["unknown"] = 1
    exported.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown field"):
        load_project(exported)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sample_count", True),
        ("maximum_evaluations", 0),
        ("include_alternatives", "no"),
        ("percentiles", (float("nan"),)),
        ("aggregation_statuses", (HydraulicResultStatus.UNRESOLVED,)),
    ],
)
def test_invalid_study_policy_rejected(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(_study(), **{field: value})

def test_disabled_alternatives_reject_named_alternative_selection() -> None:
    with pytest.raises(ValueError, match="alternative_names must be empty"):
        replace(_study(), include_alternatives=False, alternative_names=("Upgrade",))
