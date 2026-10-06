"""Regression against published MRWA 2006 Appendix D worked-example values."""

import pytest

from ryan_library.classes.floodway import FloodwayZone
from ryan_library.functions.floodway import calculate_mrwa_surface_velocity, mrwa_steady_state_velocity


@pytest.mark.parametrize(
    ("zone", "q", "slope", "roughness", "head", "delta_p", "published_velocity"),
    [
        (FloodwayZone.PAVEMENT, 0.50, 0.03, 0.015, 0.44, 0.135, 2.79),
        (FloodwayZone.DOWNSTREAM_BATTER, 0.50, 1.0 / 3.0, 0.040, 0.44, 0.140, 2.82),
        (FloodwayZone.PAVEMENT, 0.44, 0.03, 0.015, 0.50, 0.150, 2.97),
        (FloodwayZone.DOWNSTREAM_BATTER, 0.60, 1.0 / 3.0, 0.030, 0.50, 0.160, 3.01),
    ],
)
def test_practical_event_velocities_match_appendix_d(
    zone: FloodwayZone,
    q: float,
    slope: float,
    roughness: float,
    head: float,
    delta_p: float,
    published_velocity: float,
) -> None:
    result = calculate_mrwa_surface_velocity(
        zone=zone,
        unit_discharge=q,
        slope=slope,
        roughness=roughness,
        total_head=head,
        delta_p=delta_p,
    )

    assert result.adopted_velocity == pytest.approx(published_velocity, abs=0.02)


@pytest.mark.parametrize(
    ("q", "roughness", "published_velocity"),
    [
        (0.35, 0.040, 3.26),
        (0.28, 0.030, 3.53),
    ],
)
def test_low_tailwater_intersection_velocities_match_appendix_d(
    q: float,
    roughness: float,
    published_velocity: float,
) -> None:
    velocity = mrwa_steady_state_velocity(q, 1.0 / 3.0, roughness)

    assert velocity == pytest.approx(published_velocity, abs=0.02)


@pytest.mark.parametrize(
    ("q", "roughness", "head", "delta_p", "published_velocity"),
    [
        (1.443, 0.040, 0.90, 0.135, 3.51),
        (3.043, 0.030, 1.48, 0.150, 4.26),
    ],
)
def test_transition_batter_velocities_match_appendix_d_graph_precision(
    q: float,
    roughness: float,
    head: float,
    delta_p: float,
    published_velocity: float,
) -> None:
    result = calculate_mrwa_surface_velocity(
        zone=FloodwayZone.DOWNSTREAM_BATTER,
        unit_discharge=q,
        slope=1.0 / 3.0,
        roughness=roughness,
        total_head=head,
        delta_p=delta_p,
    )

    # The source result includes a Figure 4.6 graph read rounded to about 0.05 m/s.
    assert result.adopted_velocity == pytest.approx(published_velocity, abs=0.05)
