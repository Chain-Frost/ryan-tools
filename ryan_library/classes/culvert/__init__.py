"""Typed domain models for culvert project workflows."""

from .alternative import Alternative
from .candidate import DesignCandidate
from .criteria import DesignCriteria
from .crossing import (
    BarrelDefinition,
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    RectangularBarrelDefinition,
    RoadwayCrestPointDefinition,
    RoadwayDefinition,
    RoadwayOvertoppingDefinition,
    RoadwayProfileDefinition,
    RoadwaySurfaceName,
)
from .event import EventDefinition
from .project import CulvertProject
from .results import (
    CandidateAssessment,
    CrossingRatingResult,
    DesignFailure,
    DesignFailureCode,
    DesignResult,
    ScenarioResult,
)
from .scenario import Scenario
from .uncertainty import UncertaintyParameterSpec, UncertaintySamplingMode, UncertaintyStudy
from .uncertainty_results import StudyEvaluation, StudyMetricSummary, StudySummary, UncertaintyStudyResult

__all__: list[str] = [
    "Alternative",
    "BarrelDefinition",
    "CandidateAssessment",
    "CircularBarrelDefinition",
    "CrossingDefinition",
    "CrossingRatingResult",
    "CulvertGroupDefinition",
    "CulvertMaterialName",
    "CulvertProject",
    "DesignCandidate",
    "DesignCriteria",
    "DesignFailure",
    "DesignFailureCode",
    "DesignResult",
    "EventDefinition",
    "RectangularBarrelDefinition",
    "RoadwayCrestPointDefinition",
    "RoadwayDefinition",
    "RoadwayOvertoppingDefinition",
    "RoadwayProfileDefinition",
    "RoadwaySurfaceName",
    "Scenario",
    "ScenarioResult",
    "StudyEvaluation",
    "StudyMetricSummary",
    "StudySummary",
    "UncertaintyParameterSpec",
    "UncertaintySamplingMode",
    "UncertaintyStudy",
    "UncertaintyStudyResult",
]
