"""Application-level configuration for culvert uncertainty studies."""

from dataclasses import dataclass
from enum import StrEnum

from culvert_solver import BoundedParameterSpec, UniformParameterSpec

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
        identities = tuple(spec.parameter for spec in parameters)
        if len(identities) != len(set(identities)):
            msg = "parameters must not contain the same hydraulic uncertainty parameter more than once."
            raise ValueError(msg)

        sample_count: object = self.sample_count
        if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count <= 0:
            msg = "sample_count must be a strictly positive integer."
            raise ValueError(msg)

        if sampling_mode is UncertaintySamplingMode.BOUNDED_SWEEP:
            if not all(isinstance(spec, BoundedParameterSpec) for spec in parameters):
                msg = "bounded_sweep studies require only BoundedParameterSpec values."
                raise ValueError(msg)
            if self.seed is not None:
                msg = "bounded_sweep studies do not use a random seed."
                raise ValueError(msg)
        else:
            if not all(isinstance(spec, UniformParameterSpec) for spec in parameters):
                msg = "monte_carlo studies require only UniformParameterSpec values."
                raise ValueError(msg)
            seed: object = self.seed
            if isinstance(seed, bool) or not isinstance(seed, int):
                msg = "monte_carlo studies require an explicit integer seed."
                raise ValueError(msg)

        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "sampling_mode", sampling_mode)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "crossing_names", _normalise_names(tuple(self.crossing_names), "crossing_names"))
        object.__setattr__(self, "scenario_names", _normalise_names(tuple(self.scenario_names), "scenario_names"))
        object.__setattr__(
            self,
            "alternative_names",
            _normalise_names(tuple(self.alternative_names), "alternative_names"),
        )
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
