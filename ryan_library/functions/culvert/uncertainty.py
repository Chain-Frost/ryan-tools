"""Sampling orchestration for project-level culvert uncertainty studies."""

from itertools import product

from culvert_solver import (
    BoundedParameterSpec,
    HydraulicSample,
    SampledParameter,
    UniformParameterSpec,
    sample_bounded_parameter,
    sample_uniform_parameter,
)

from ...classes.culvert.uncertainty import UncertaintySamplingMode, UncertaintyStudy


def _single_parameter(sample: HydraulicSample) -> SampledParameter:
    if len(sample.parameters) != 1:
        msg = "ryan-culverts single-parameter sampling primitive returned an unexpected sample shape."
        raise RuntimeError(msg)
    return sample.parameters[0]


def generate_study_samples(study: UncertaintyStudy) -> tuple[HydraulicSample, ...]:
    """Generate deterministic project-level samples using public ryan-culverts primitives.

    Bounded studies form the Cartesian product of each parameter's inclusive bounded
    sweep. Monte Carlo studies generate sample_count aligned multi-parameter samples,
    using the study seed plus each parameter index as the explicit low-level seed.
    """
    if study.sampling_mode is UncertaintySamplingMode.BOUNDED_SWEEP:
        sampled_parameters: list[tuple[HydraulicSample, ...]] = []
        for spec in study.parameters:
            if not isinstance(spec, BoundedParameterSpec):
                msg = "bounded_sweep study contains a non-bounded parameter specification."
                raise TypeError(msg)
            sampled_parameters.append(sample_bounded_parameter(spec, count=study.sample_count))

        return tuple(
            HydraulicSample(
                parameters=tuple(_single_parameter(sample) for sample in combination),
                sample_id=f"{study.name}:bounded:{index}",
            )
            for index, combination in enumerate(product(*sampled_parameters))
        )

    if study.seed is None:
        msg = "monte_carlo study is missing its required seed."
        raise ValueError(msg)

    sampled_parameters: list[tuple[HydraulicSample, ...]] = []
    for parameter_index, spec in enumerate(study.parameters):
        if not isinstance(spec, UniformParameterSpec):
            msg = "monte_carlo study contains a non-uniform parameter specification."
            raise TypeError(msg)
        sampled_parameters.append(
            sample_uniform_parameter(
                spec,
                count=study.sample_count,
                seed=study.seed + parameter_index,
            )
        )

    return tuple(
        HydraulicSample(
            parameters=tuple(_single_parameter(samples[index]) for samples in sampled_parameters),
            sample_id=f"{study.name}:monte_carlo:{index}",
        )
        for index in range(study.sample_count)
    )
