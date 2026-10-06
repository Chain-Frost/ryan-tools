"""Application-level study outcomes retaining public hydraulic contracts."""

from dataclasses import dataclass

from culvert_solver import HydraulicEvaluationFailure, HydraulicResultStatus, HydraulicSample

from .alternative import Alternative
from .crossing import CrossingDefinition
from .project import CulvertProject
from .results import ScenarioResult
from .scenario import Scenario
from .uncertainty import UncertaintyStudy


@dataclass(frozen=True, slots=True)
class StudyEvaluation:
    """One selected crossing/scenario/sample, including expected solver failure evidence."""

    sample: HydraulicSample
    crossing: CrossingDefinition
    scenario: Scenario
    alternative: Alternative | None = None
    result: ScenarioResult | None = None
    failure: HydraulicEvaluationFailure | None = None

    def __post_init__(self) -> None:
        if (self.result is None) == (self.failure is None):
            msg = "Exactly one of result or failure must be supplied."
            raise ValueError(msg)

    @property
    def status(self) -> HydraulicResultStatus | None:
        """Return the authoritative hydraulic status; failures have no hydraulic result."""
        return None if self.result is None else self.result.hydraulic_result.status

    @property
    def alternative_name(self) -> str | None:
        """Return the selected alternative name, or None for a base crossing."""
        return None if self.alternative is None else self.alternative.name


@dataclass(frozen=True, slots=True)
class StudyMetricSummary:
    """Descriptive statistics for one explicitly eligible output population."""

    metric: str
    unit: str
    count: int
    minimum: float | None
    maximum: float | None
    mean: float | None
    percentiles: tuple[tuple[float, float], ...]
    maximum_sample_id: str | None


@dataclass(frozen=True, slots=True)
class StudySummary:
    """Status counts and output envelopes for one crossing/scenario/alternative."""

    crossing_name: str
    scenario_name: str
    alternative_name: str | None
    evaluation_count: int
    eligible_count: int
    status_counts: tuple[tuple[str, int], ...]
    warning_codes: tuple[str, ...]
    applicability_codes: tuple[str, ...]
    metrics: tuple[StudyMetricSummary, ...]


@dataclass(frozen=True, slots=True)
class UncertaintyStudyResult:
    """Complete reproducible study outcomes and conditional descriptive summaries."""

    project: CulvertProject
    study: UncertaintyStudy
    evaluations: tuple[StudyEvaluation, ...]
    summaries: tuple[StudySummary, ...]
