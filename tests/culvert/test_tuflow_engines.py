"""Tests for selectable TUFLOW culvert hydraulic engines."""

from pathlib import Path

import pytest

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert.tuflow_engines import (
    CulvertEngine,
    TuflowCircularCulvert,
    solve_tuflow_culvert_forward,
    solve_tuflow_culvert_inverse,
)


@pytest.fixture
def concrete_crossing() -> TuflowCircularCulvert:
    return TuflowCircularCulvert(
        name="C01",
        diameter_m=1.2,
        length_m=40.0,
        inlet_invert_m=10.0,
        outlet_invert_m=9.5,
        roughness_manning_n=0.013,
        barrels=3,
        material=CulvertMaterialName.CONCRETE_PIPE,
    )


def test_ryan_culverts_forward_inverse_round_trip(concrete_crossing: TuflowCircularCulvert) -> None:
    forward = solve_tuflow_culvert_forward(
        concrete_crossing,
        scenario="design flow",
        discharge_m3s=6.0,
        tailwater_elevation_m=10.0,
        engine=CulvertEngine.RYAN_CULVERTS,
    )

    assert forward.engine is CulvertEngine.RYAN_CULVERTS
    assert forward.computed_discharge_m3s == pytest.approx(6.0)
    assert forward.headwater_elevation_m > 10.0
    assert forward.outlet_velocity_mps > 0.0
    assert forward.headwater_ratio > 0.0

    inverse = solve_tuflow_culvert_inverse(
        concrete_crossing,
        scenario="round trip",
        headwater_elevation_m=forward.headwater_elevation_m,
        tailwater_elevation_m=10.0,
        engine=CulvertEngine.RYAN_CULVERTS,
    )

    assert inverse.requested_headwater_m == pytest.approx(forward.headwater_elevation_m)
    assert inverse.computed_discharge_m3s == pytest.approx(6.0, rel=1e-4)
    assert inverse.headwater_elevation_m == pytest.approx(forward.headwater_elevation_m, abs=1e-4)


def test_ryan_culverts_does_not_resolve_hy8_executable(concrete_crossing: TuflowCircularCulvert) -> None:
    result = solve_tuflow_culvert_forward(
        concrete_crossing,
        scenario="native solver",
        discharge_m3s=2.0,
        tailwater_elevation_m=9.5,
        engine="ryan-culverts",
        hy8=Path("Z:/definitely-not-installed/HY864.exe"),
    )

    assert result.engine is CulvertEngine.RYAN_CULVERTS
    assert result.computed_discharge_m3s == pytest.approx(2.0)


def test_unsupported_material_fails_closed() -> None:
    with pytest.raises(ValueError, match="circular concrete or corrugated-steel"):
        TuflowCircularCulvert(
            name="BOX",
            diameter_m=1.2,
            length_m=40.0,
            inlet_invert_m=10.0,
            outlet_invert_m=9.5,
            roughness_manning_n=0.013,
            material=CulvertMaterialName.CONCRETE_BOX,
        )


def test_unknown_engine_fails_closed(concrete_crossing: TuflowCircularCulvert) -> None:
    with pytest.raises(ValueError):
        solve_tuflow_culvert_forward(
            concrete_crossing,
            scenario="bad engine",
            discharge_m3s=2.0,
            tailwater_elevation_m=9.5,
            engine="not-an-engine",
        )


def test_hy8_forward_dispatch_uses_run_hy8_boundary(
    concrete_crossing: TuflowCircularCulvert,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crossing = MagicMock()
    crossing.hw_from_q.return_value = HydraulicsResult(
        crossing_name="C01",
        requested_flow=2.0,
        computed_flow=2.0,
        computed_headwater=10.8,
        row=Hy8ResultRow(
            flow=2.0,
            headwater_elevation=10.8,
            velocity=2.2,
            roadway_discharge=0.0,
            flow_type="Outlet Control",
            overtopping=False,
        ),
    )
    monkeypatch.setattr(engine_module, "_hy8_crossing", lambda *args, **kwargs: (MagicMock(), crossing))

    result = solve_tuflow_culvert_forward(
        concrete_crossing,
        scenario="HY-8 mock",
        discharge_m3s=2.0,
        tailwater_elevation_m=9.5,
        engine=CulvertEngine.HY8,
        hy8=Path("C:/HY8/HY864.exe"),
    )

    assert result.engine is CulvertEngine.HY8
    assert result.computed_discharge_m3s == pytest.approx(2.0)
    assert result.headwater_elevation_m == pytest.approx(10.8)
    crossing.hw_from_q.assert_called_once()
