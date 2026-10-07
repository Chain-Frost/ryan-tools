"""Tests for selectable TUFLOW culvert hydraulic engines."""

from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from culvert_solver import CIRCULAR_CMP_PROJECTING, PIPE_CMP_LOSS_PROJECTING
from run_hy8 import CulvertMaterial as Hy8Material
from run_hy8 import Hy8ResultRow, HydraulicsResult

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert import tuflow_engines as engine_module
from ryan_library.functions.culvert.tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    TuflowLossParameters,
)
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
        configuration=CircularCulvertConfiguration(
            material=CulvertMaterialName.CONCRETE_PIPE,
            inlet=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        ),
        diameter_m=1.2,
        length_m=40.0,
        inlet_invert_m=10.0,
        outlet_invert_m=9.5,
        roughness_manning_n=0.013,
        barrels=3,
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


def test_physical_configuration_rejects_box_material_for_circular_pipe() -> None:
    with pytest.raises(ValueError, match="Unsupported circular culvert material"):
        CircularCulvertConfiguration(
            material=CulvertMaterialName.CONCRETE_BOX,
            inlet=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        )


def test_hdpe_is_supported_by_hy8_but_fails_closed_for_native(
    concrete_crossing: TuflowCircularCulvert,
) -> None:
    definition = replace(
        concrete_crossing,
        configuration=CircularCulvertConfiguration(
            material=CulvertMaterialName.SMOOTH_HDPE,
            inlet=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        ),
        roughness_manning_n=0.012,
    )

    _project, crossing = engine_module._hy8_crossing(  # pyright: ignore[reportPrivateUsage]
        definition,
        tailwater_elevation_m=9.5,
        seed_discharge_m3s=2.0,
    )
    assert crossing.culverts[0].material is Hy8Material.HDPE

    with pytest.raises(ValueError, match="requires explicit HDPE inlet coefficients"):
        solve_tuflow_culvert_forward(
            definition,
            scenario="native HDPE",
            discharge_m3s=2.0,
            tailwater_elevation_m=9.5,
            engine=CulvertEngine.RYAN_CULVERTS,
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


def test_native_cmp_configuration_matches_projecting_hy8_assumption(
    concrete_crossing: TuflowCircularCulvert,
) -> None:
    definition = replace(
        concrete_crossing,
        configuration=CircularCulvertConfiguration(
            material=CulvertMaterialName.CORRUGATED_STEEL,
            inlet=CircularInletConfiguration.THIN_EDGE_PROJECTING,
        ),
    )

    configuration = engine_module._solver_configuration(  # pyright: ignore[reportPrivateUsage]
        definition
    )

    assert configuration.default_circular_cmp_inlet is CIRCULAR_CMP_PROJECTING
    assert configuration.default_circular_cmp_loss is PIPE_CMP_LOSS_PROJECTING


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
    builder = MagicMock(return_value=(MagicMock(), crossing))
    monkeypatch.setattr(engine_module, "_hy8_crossing", builder)

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


def test_hy8_tailwater_preserves_downstream_invert(
    concrete_crossing: TuflowCircularCulvert,
) -> None:
    _project, crossing = engine_module._hy8_crossing(  # pyright: ignore[reportPrivateUsage]
        concrete_crossing,
        tailwater_elevation_m=10.25,
        seed_discharge_m3s=2.0,
    )

    assert crossing.tailwater.constant_elevation == pytest.approx(10.25)
    assert crossing.tailwater.invert_elevation == pytest.approx(concrete_crossing.outlet_invert_m)


def test_hy8_crossing_uses_filesystem_safe_internal_name() -> None:
    definition = TuflowCircularCulvert(
        name="A/B:C*?<>|",
        configuration=CircularCulvertConfiguration(
            material=CulvertMaterialName.CONCRETE_PIPE,
            inlet=CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        ),
        diameter_m=1.2,
        length_m=40.0,
        inlet_invert_m=10.0,
        outlet_invert_m=9.5,
        roughness_manning_n=0.013,
    )

    project, crossing = engine_module._hy8_crossing(  # pyright: ignore[reportPrivateUsage]
        definition,
        tailwater_elevation_m=9.5,
        seed_discharge_m3s=2.0,
    )

    forbidden = set('<>:"/\\|?*')
    assert not forbidden.intersection(crossing.name)
    assert crossing.name != definition.name
    assert project.title == crossing.name
    assert crossing.culverts[0].name.startswith(crossing.name)


def test_hy8_reported_hw_d_uses_nominal_diameter(
    concrete_crossing: TuflowCircularCulvert,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blocked = replace(
        concrete_crossing,
        diameter_m=1.2 * (0.5**0.5),
        nominal_diameter_m=1.2,
    )
    crossing = MagicMock()
    crossing.hw_from_q.return_value = HydraulicsResult(
        crossing_name=blocked.name,
        requested_flow=2.0,
        computed_flow=2.0,
        computed_headwater=11.8,
        row=Hy8ResultRow(
            flow=2.0,
            headwater_elevation=11.8,
            velocity=2.0,
            roadway_discharge=0.0,
            flow_type="Outlet Control",
            overtopping=False,
        ),
    )
    builder = MagicMock(return_value=(MagicMock(), crossing))
    monkeypatch.setattr(engine_module, "_hy8_crossing", builder)

    result = solve_tuflow_culvert_forward(
        blocked,
        scenario="blocked",
        discharge_m3s=2.0,
        tailwater_elevation_m=9.5,
        engine=CulvertEngine.HY8,
    )

    assert blocked.diameter_m < blocked.hw_diameter_m
    assert result.headwater_ratio == pytest.approx(1.5)


@pytest.mark.parametrize("inverse", [False, True])
def test_adverse_slope_dispatch_is_engine_specific(
    concrete_crossing: TuflowCircularCulvert,
    monkeypatch: pytest.MonkeyPatch,
    inverse: bool,
) -> None:
    definition = replace(concrete_crossing, outlet_invert_m=10.5)
    result = HydraulicsResult(
        crossing_name=definition.name,
        computed_flow=2.0,
        computed_headwater=11.5,
        row=Hy8ResultRow(flow=2.0, headwater_elevation=11.5, velocity=2.0),
    )
    method = "q_from_hw" if inverse else "hw_from_q"
    dispatch = MagicMock(return_value=result)
    monkeypatch.setattr(engine_module.Hy8Crossing, method, dispatch)
    for engine in (CulvertEngine.HY8, CulvertEngine.RYAN_CULVERTS):

        def solve(engine: CulvertEngine = engine) -> engine_module.CulvertEngineResult:
            if inverse:
                return solve_tuflow_culvert_inverse(
                    definition,
                    scenario="adverse",
                    headwater_elevation_m=11.5,
                    tailwater_elevation_m=10.5,
                    engine=engine,
                )
            return solve_tuflow_culvert_forward(
                definition,
                scenario="adverse",
                discharge_m3s=2.0,
                tailwater_elevation_m=10.5,
                engine=engine,
            )

        if engine is CulvertEngine.HY8:
            assert solve().engine is CulvertEngine.HY8
            dispatched_barrel = dispatch.call_args.kwargs["project"].crossings[0].culverts[0]
            assert dispatched_barrel.inlet_invert_elevation == pytest.approx(10.0)
            assert dispatched_barrel.outlet_invert_elevation == pytest.approx(10.5)
        else:
            with pytest.raises(ValueError, match="ryan-culverts does not support adverse slopes"):
                solve()
    dispatch.assert_called_once()


def test_native_entry_loss_override_is_numeric_not_an_inlet_selector(
    concrete_crossing: TuflowCircularCulvert,
) -> None:
    definition = replace(
        concrete_crossing,
        losses=TuflowLossParameters(entry_loss_coefficient=0.65),
    )

    configuration = engine_module._solver_configuration(  # pyright: ignore[reportPrivateUsage]
        definition
    )

    assert configuration.default_circular_concrete_inlet is not None
    assert configuration.default_circular_concrete_loss.ke == pytest.approx(0.65)


def test_hy8_rejects_entry_loss_that_conflicts_with_physical_inlet(
    concrete_crossing: TuflowCircularCulvert,
) -> None:
    definition = replace(
        concrete_crossing,
        losses=TuflowLossParameters(entry_loss_coefficient=0.7),
    )

    with pytest.raises(ValueError, match="does not yet expose an arbitrary TUFLOW EntryC override"):
        engine_module._hy8_crossing(  # pyright: ignore[reportPrivateUsage]
            definition,
            tailwater_elevation_m=9.5,
            seed_discharge_m3s=2.0,
        )


@pytest.mark.parametrize(
    ("material", "inlet", "hy8_material"),
    [
        (
            CulvertMaterialName.CONCRETE_PIPE,
            CircularInletConfiguration.GROOVED_END_HEADWALL,
            Hy8Material.CONCRETE,
        ),
        (
            CulvertMaterialName.CORRUGATED_STEEL,
            CircularInletConfiguration.MITERED_TO_SLOPE,
            Hy8Material.CORRUGATED_STEEL,
        ),
        (
            CulvertMaterialName.SMOOTH_HDPE,
            CircularInletConfiguration.THIN_EDGE_PROJECTING,
            Hy8Material.HDPE,
        ),
    ],
)
def test_hy8_mapping_uses_explicit_physical_configuration(
    concrete_crossing: TuflowCircularCulvert,
    material: CulvertMaterialName,
    inlet: CircularInletConfiguration,
    hy8_material: Hy8Material,
) -> None:
    definition = replace(
        concrete_crossing,
        configuration=CircularCulvertConfiguration(material=material, inlet=inlet),
    )

    _project, crossing = engine_module._hy8_crossing(  # pyright: ignore[reportPrivateUsage]
        definition,
        tailwater_elevation_m=9.5,
        seed_discharge_m3s=2.0,
    )

    assert crossing.culverts[0].material is hy8_material
