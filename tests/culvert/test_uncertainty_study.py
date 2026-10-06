"""Focused tests for project-level uncertainty study sampling."""

from culvert_solver import (
    BoundedParameterSpec,
    HydraulicUncertaintyParameter,
    ParameterBounds,
    SourceReference,
    UniformParameterSpec,
)

from ryan_library.classes.culvert import UncertaintySamplingMode, UncertaintyStudy
from ryan_library.functions.culvert import generate_study_samples


def _source() -> SourceReference:
    return SourceReference(
        source_id="TEST-UNCERTAINTY",
        publication="Synthetic uncertainty test basis",
        edition="1",
        locator="test fixture",
        url=None,
        applicability="Synthetic values used only for deterministic unit tests.",
    )


def test_bounded_study_uses_cartesian_product_and_includes_bounds() -> None:
    source = _source()
    study = UncertaintyStudy(
        name="Bounds",
        sampling_mode=UncertaintySamplingMode.BOUNDED_SWEEP,
        parameters=(
            BoundedParameterSpec(
                parameter=HydraulicUncertaintyParameter.MANNING_ROUGHNESS,
                bounds=ParameterBounds(
                    lower=0.012,
                    upper=0.016,
                    unit=HydraulicUncertaintyParameter.MANNING_ROUGHNESS.unit,
                ),
                source=source,
            ),
            BoundedParameterSpec(
                parameter=HydraulicUncertaintyParameter.DISCHARGE,
                bounds=ParameterBounds(
                    lower=2.0,
                    upper=4.0,
                    unit=HydraulicUncertaintyParameter.DISCHARGE.unit,
                ),
                source=source,
            ),
        ),
        sample_count=2,
    )

    samples = generate_study_samples(study)

    assert len(samples) == 4
    values = {tuple(parameter.value for parameter in sample.parameters) for sample in samples}
    assert values == {(0.012, 2.0), (0.012, 4.0), (0.016, 2.0), (0.016, 4.0)}


def test_seeded_monte_carlo_study_is_reproducible() -> None:
    source = _source()
    parameters = (
        UniformParameterSpec(
            parameter=HydraulicUncertaintyParameter.MANNING_ROUGHNESS,
            bounds=ParameterBounds(
                lower=0.012,
                upper=0.016,
                unit=HydraulicUncertaintyParameter.MANNING_ROUGHNESS.unit,
            ),
            source=source,
        ),
        UniformParameterSpec(
            parameter=HydraulicUncertaintyParameter.DISCHARGE,
            bounds=ParameterBounds(
                lower=2.0,
                upper=4.0,
                unit=HydraulicUncertaintyParameter.DISCHARGE.unit,
            ),
            source=source,
        ),
    )
    study = UncertaintyStudy(
        name="Monte Carlo",
        sampling_mode=UncertaintySamplingMode.MONTE_CARLO,
        parameters=parameters,
        sample_count=5,
        seed=42,
    )

    assert generate_study_samples(study) == generate_study_samples(study)

    changed_seed = UncertaintyStudy(
        name="Monte Carlo",
        sampling_mode=UncertaintySamplingMode.MONTE_CARLO,
        parameters=parameters,
        sample_count=5,
        seed=43,
    )
    assert generate_study_samples(study) != generate_study_samples(changed_seed)
