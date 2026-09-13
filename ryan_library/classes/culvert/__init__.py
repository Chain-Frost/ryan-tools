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
    RoadwayDefinition,
)
from .project import CulvertProject
from .results import CandidateAssessment, CrossingRatingResult, DesignResult, ScenarioResult
from .scenario import Scenario

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
    "DesignResult",
    "RectangularBarrelDefinition",
    "RoadwayDefinition",
    "Scenario",
    "ScenarioResult",
]
