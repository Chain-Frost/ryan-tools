"""Typed workflow results retaining authoritative solver output."""

from dataclasses import dataclass
from enum import StrEnum

from culvert_solver import CrossingHydraulicResult, HydraulicResultStatus, RatingCurveResult

from .candidate import DesignCandidate


class DesignFailureCode(StrEnum):
    """Stable categories for rejected candidate evidence."""

    CRITERION = "criterion"
    SOLVER_FAILURE = "solver_failure"


@dataclass(frozen=True, slots=True)
class DesignFailure:
    """One typed candidate failure associated with a scenario."""

    code: DesignFailureCode
    scenario_name: str
    message: str


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """One crossing/scenario evaluation with the complete solver result retained."""

    crossing_name: str
    scenario_name: str
    hydraulic_result: CrossingHydraulicResult
    alternative_name: str | None = None
    aep_percent: float | None = None
    source: str | None = None
    notes: str = ""
    target_headwater_elevation: float | None = None
    tailwater_override_elevation: float | None = None

    @property
    def maximum_outlet_velocity(self) -> float:
        """Return the maximum active barrel outlet velocity in metres per second."""
        return max(
            (
                group_result.barrel_result.velocity_outlet
                for group_result in self.hydraulic_result.group_results
                if group_result.barrel_discharge > 0.0
            ),
            default=0.0,
        )

    @property
    def target_headwater_residual(self) -> float | None:
        """Return solved minus requested headwater for inverse-target scenarios."""
        if self.target_headwater_elevation is None:
            return None
        return self.hydraulic_result.headwater_elevation - self.target_headwater_elevation

    @property
    def tailwater_was_event_override(self) -> bool:
        """Return whether an imported event supplied its own tailwater elevation."""
        return self.tailwater_override_elevation is not None

    @property
    def warning_codes(self) -> tuple[str, ...]:
        """Return unique structured hydraulic warning codes."""
        return tuple(
            dict.fromkeys(
                warning.code.value
                for group_result in self.hydraulic_result.group_results
                for warning in group_result.barrel_result.warnings
            )
        )

    @property
    def applicability_codes(self) -> tuple[str, ...]:
        """Return structured crossing applicability-notice codes."""
        return tuple(notice.code.value for notice in self.hydraulic_result.applicability_notices)


@dataclass(frozen=True, slots=True)
class CandidateAssessment:
    """Pass/fail assessment for one candidate across all requested scenarios."""

    candidate: DesignCandidate
    scenario_results: tuple[ScenarioResult, ...]
    passed: bool
    failure_reasons: tuple[str, ...] = ()
    failures: tuple[DesignFailure, ...] = ()

    def __post_init__(self) -> None:
        if self.passed and self.failure_reasons:
            msg = "A passing candidate cannot contain failure reasons."
            raise ValueError(msg)
        if self.passed and self.failures:
            msg = "A passing candidate cannot contain typed failures."
            raise ValueError(msg)
        if not self.passed and not self.failure_reasons:
            msg = "A rejected candidate must retain at least one failure reason."
            raise ValueError(msg)

    @property
    def worst_status(self) -> HydraulicResultStatus:
        """Return the most conservative solver status across candidate scenarios."""
        priority = {
            HydraulicResultStatus.VALID: 0,
            HydraulicResultStatus.VALID_WITH_ADVISORY: 1,
            HydraulicResultStatus.APPROXIMATE: 2,
            HydraulicResultStatus.UNRESOLVED: 3,
        }
        return max(
            (result.hydraulic_result.status for result in self.scenario_results),
            key=priority.__getitem__,
            default=HydraulicResultStatus.VALID,
        )


@dataclass(frozen=True, slots=True)
class DesignResult:
    """Ranked design-candidate assessments."""

    assessments: tuple[CandidateAssessment, ...]

    @property
    def feasible(self) -> tuple[CandidateAssessment, ...]:
        """Return passing candidates in ranking order."""
        return tuple(assessment for assessment in self.assessments if assessment.passed)

    @property
    def recommended(self) -> CandidateAssessment | None:
        """Return the highest-ranked feasible candidate, if one exists."""
        return next(iter(self.feasible), None)


@dataclass(frozen=True, slots=True)
class CrossingRatingResult:
    """A named crossing rating curve returned by the solver."""

    crossing_name: str
    rating_curve: RatingCurveResult
