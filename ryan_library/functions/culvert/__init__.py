"""Reusable helpers for culvert project workflows."""

from .adapter import build_solver_barrel, build_solver_crossing, build_solver_group, build_solver_roadway
from .assessment import assess_scenario
from .candidate_generation import generate_circular_candidates, generate_rectangular_candidates
from .config import (
    SCHEMA_VERSION,
    export_project_json,
    load_project,
    load_project_json,
    project_record,
    uncertainty_study_record,
)
from .event_import import load_event_csv
from .export import (
    candidate_assessment_record,
    crossing_definition_record,
    design_criteria_record,
    export_crossing_rating_csv,
    export_crossing_rating_json,
    export_design_result_json,
    export_scenario_results_csv,
    export_scenario_results_json,
    scenario_result_record,
)
from .plotting import plot_longitudinal_profile, plot_rating_curve, save_figure
from .tuflow_attributes import (
    TuflowCulvertAttributes,
    load_tuflow_culvert_attributes,
    material_from_mapping,
    parse_culvert_material,
)
from .tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    CulvertShapeName,
    TuflowLossParameters,
    parse_circular_inlet_configuration,
    parse_tuflow_shape,
)
from .tuflow_engines import (
    CulvertEngine,
    CulvertEngineResult,
    TuflowCircularCulvert,
    solve_tuflow_culvert_forward,
    solve_tuflow_culvert_inverse,
)
from .uncertainty import generate_study_samples
from .uncertainty_evaluation import evaluate_crossing_sample, sampled_crossing_inputs
from .uncertainty_export import (
    export_uncertainty_csv,
    export_uncertainty_json,
    export_uncertainty_summary_csv,
    study_evaluation_record,
    study_summary_record,
    uncertainty_result_record,
)
from .uncertainty_statistics import aggregate_study_evaluations

__all__: list[str] = [
    "CircularCulvertConfiguration",
    "CircularInletConfiguration",
    "CulvertEngine",
    "CulvertEngineResult",
    "CulvertShapeName",
    "SCHEMA_VERSION",
    "TuflowCircularCulvert",
    "TuflowCulvertAttributes",
    "TuflowLossParameters",
    "aggregate_study_evaluations",
    "assess_scenario",
    "build_solver_barrel",
    "build_solver_crossing",
    "build_solver_group",
    "build_solver_roadway",
    "candidate_assessment_record",
    "crossing_definition_record",
    "design_criteria_record",
    "evaluate_crossing_sample",
    "export_crossing_rating_csv",
    "export_crossing_rating_json",
    "export_design_result_json",
    "export_project_json",
    "export_scenario_results_csv",
    "export_scenario_results_json",
    "export_uncertainty_csv",
    "export_uncertainty_json",
    "export_uncertainty_summary_csv",
    "generate_circular_candidates",
    "generate_rectangular_candidates",
    "generate_study_samples",
    "load_event_csv",
    "load_project",
    "load_project_json",
    "load_tuflow_culvert_attributes",
    "material_from_mapping",
    "parse_circular_inlet_configuration",
    "parse_culvert_material",
    "parse_tuflow_shape",
    "plot_longitudinal_profile",
    "plot_rating_curve",
    "project_record",
    "sampled_crossing_inputs",
    "save_figure",
    "scenario_result_record",
    "solve_tuflow_culvert_forward",
    "solve_tuflow_culvert_inverse",
    "study_evaluation_record",
    "study_summary_record",
    "uncertainty_result_record",
    "uncertainty_study_record",
]
