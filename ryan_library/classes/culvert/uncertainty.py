"""Application-level configuration for culvert uncertainty studies."""

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite

from culvert_solver import BoundedParameterSpec, HydraulicResultStatus, UniformParameterSpec

type UncertaintyParameterSpec = BoundedParameterSpec | UniformParameterSpec


class UncertaintySamplingMode(StrEnum):
    """Supported project-level uncertainty study sampling policies."""

    BOUNDED_SWEEP = "bounded_sweep"
    MONTE_CARLO = "monte_carlo"


def _normalise_names(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    result = tuple(value.strip() for value in values)
    if any(not value for value in result):
        msg = f"{name} must contain only nonempty names."
        raise ValueError(msg)
    if len(result) != len(set(result)):
        msg = f"{name} must not contain duplicate names."
        raise ValueError(msg)
    return result


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"{name} must be a strictly positive integer."
        raise ValueError(msg)
    return value


def _validate_sampling(
    mode: UncertaintySamplingMode, parameters: tuple[UncertaintyParameterSpec, ...], seed: object
) -> None:
    expected = BoundedParameterSpec if mode is UncertaintySamplingMode.BOUNDED_SWEEP else UniformParameterSpec
    if not all(isinstance(spec, expected) for spec in parameters):  # pyright: ignore[reportUnnecessaryIsInstance]
        msg = f"{mode.value} studies require only {expected.__name__} values."
        raise ValueError(msg)
    if mode is UncertaintySamplingMode.BOUNDED_SWEEP:
        if seed is not None:
            msg = "bounded_sweep studies do not use a random seed."
            raise ValueError(msg)
    elif isinstance(seed, bool) or not isinstance(seed, int):
        msg = "monte_carlo studies require an explicit integer seed."
        raise ValueError(msg)


def _validate_aggregation(
    percentiles: tuple[float, ...], statuses: tuple[HydraulicResultStatus, ...]
) -> tuple[tuple[float, ...], tuple[HydraulicResultStatus, ...]]:
    values = tuple(float(value) for value in percentiles)
    if not values or any(not isfinite(value) or not 0 <= value <= 100 for value in values):
        msg = "percentiles must contain finite values between 0 and 100."
        raise ValueError(msg)
    if len(set(values)) != len(values):
        msg = "percentiles must be unique."
        raise ValueError(msg)
    accepted = tuple(HydraulicResultStatus(status) for status in statuses)
    if not accepted or HydraulicResultStatus.UNRESOLVED in accepted or len(set(accepted)) != len(accepted):
        msg = "aggregation_statuses must contain unique resolved statuses; unresolved results cannot be aggregated."
        raise ValueError(msg)
    return values, accepted


@dataclass(frozen=True, slots=True)
class UncertaintyStudy:
    """Project policy describing one bounded or stochastic hydraulic uncertainty study."""

    name: str
    sampling_mode: UncertaintySamplingMode
    parameters: tuple[UncertaintyParameterSpec, ...]
    sample_count: int
    seed: int | None = None
    crossing_names: tuple[str, ...] = ()
    scenario_names: tuple[str, ...] = ()
    alternative_names: tuple[str, ...] = ()
    source: str | None = None
    notes: str = ""
    include_base_crossings: bool = True
    include_alternatives: bool = True
    maximum_evaluations: int = 10000
    percentiles: tuple[float, ...] = (5.0, 50.0, 95.0)
    aggregation_statuses: tuple[HydraulicResultStatus, ...] = (
        HydraulicResultStatus.VALID,
        HydraulicResultStatus.VALID_WITH_ADVISORY,
    )

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty text."
            raise ValueError(msg)
        try:
            sampling_mode = UncertaintySamplingMode(self.sampling_mode)
        except (TypeError, ValueError) as exc:
            msg = "sampling_mode must be an UncertaintySamplingMode value."
            raise ValueError(msg) from exc

        parameters = tuple(self.parameters)
        if not parameters:
            msg = "parameters must contain at least one public ryan-culverts uncertainty specification."
            raise ValueError(msg)
        _validate_sampling(sampling_mode, parameters, self.seed)
        identities = tuple(spec.parameter for spec in parameters)
        if len(identities) != len(set(identities)):
            msg = "parameters must not contain the same hydraulic uncertainty parameter more than once."
            raise ValueError(msg)

        _positive_integer(self.sample_count, "sample_count")
        _positive_integer(self.maximum_evaluations, "maximum_evaluations")
        if not isinstance(self.include_base_crossings, bool):  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = "include_base_crossings must be boolean."
            raise ValueError(msg)
        if not isinstance(self.include_alternatives, bool):  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = "include_alternatives must be boolean."
            raise ValueError(msg)
        crossing_names = _normalise_names(tuple(self.crossing_names), "crossing_names")
        alternative_names = _normalise_names(tuple(self.alternative_names), "alternative_names")
        if not self.include_base_crossings and crossing_names:
            msg = "crossing_names must be empty when include_base_crossings is false."
            raise ValueError(msg)
        if not self.include_alternatives and alternative_names:
            msg = "alternative_names must be empty when include_alternatives is false."
            raise ValueError(msg)
        percentiles, statuses = _validate_aggregation(self.percentiles, self.aggregation_statuses)
        object.__setattr__(self, "percentiles", percentiles)
        object.__setattr__(self, "aggregation_statuses", statuses)

        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "sampling_mode", sampling_mode)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "crossing_names", crossing_names)
        object.__setattr__(self, "scenario_names", _normalise_names(tuple(self.scenario_names), "scenario_names"))
        object.__setattr__(self, "alternative_names", alternative_names)
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
