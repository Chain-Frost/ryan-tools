"""Selectable hydraulic backends for TUFLOW culvert integration."""

import hashlib
from dataclasses import dataclass, replace
from enum import StrEnum
from math import isfinite, isnan
from pathlib import Path

from culvert_solver import (
    CIRCULAR_CMP_PROJECTING,
    DEFAULT_SOLVER_CONFIGURATION,
    PIPE_CMP_LOSS_PROJECTING,
    SolverConfiguration,
)
from culvert_solver import CulvertCrossing as SolverCrossing
from culvert_solver import solve_crossing_discharge_for_headwater, solve_crossing_hydraulics
from run_hy8 import (
    CircularConcreteInlet,
    CircularCorrugatedSteelInlet,
    FlowDefinition,
    FlowMethod,
    Hy8Project,
    HydraulicsResult,
    InletType,
    UnitSystem,
)
from run_hy8 import CulvertBarrel as Hy8Barrel
from run_hy8 import CulvertCrossing as Hy8Crossing
from run_hy8 import CulvertMaterial as Hy8Material
from run_hy8 import CulvertShape as Hy8Shape

from ...classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
)
from .adapter import build_solver_crossing


class CulvertEngine(StrEnum):
    """Hydraulic engine available to TUFLOW culvert workflows."""

    HY8 = "hy8"
    RYAN_CULVERTS = "ryan-culverts"


@dataclass(frozen=True, slots=True)
class TuflowCircularCulvert:
    """Engine-neutral circular culvert definition in SI units."""

    name: str
    diameter_m: float
    length_m: float
    inlet_invert_m: float
    outlet_invert_m: float
    roughness_manning_n: float
    barrels: int = 1
    material: CulvertMaterialName = CulvertMaterialName.CONCRETE_PIPE
    nominal_diameter_m: float | None = None

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty."
            raise ValueError(msg)
        for field_name in ("diameter_m", "length_m", "roughness_manning_n"):
            value = float(getattr(self, field_name))
            if not isfinite(value) or value <= 0.0:
                msg = f"{field_name} must be finite and strictly positive."
                raise ValueError(msg)
            object.__setattr__(self, field_name, value)
        nominal_diameter = self.diameter_m if self.nominal_diameter_m is None else float(self.nominal_diameter_m)
        if not isfinite(nominal_diameter) or nominal_diameter <= 0.0:
            msg = "nominal_diameter_m must be finite and strictly positive."
            raise ValueError(msg)
        object.__setattr__(self, "nominal_diameter_m", nominal_diameter)
        for field_name in ("inlet_invert_m", "outlet_invert_m"):
            value = float(getattr(self, field_name))
            if not isfinite(value):
                msg = f"{field_name} must be finite."
                raise ValueError(msg)
            object.__setattr__(self, field_name, value)
        barrels: object = self.barrels
        # Keep runtime validation for callers supplying values outside the annotation.
        if isinstance(barrels, bool) or not isinstance(barrels, int) or barrels <= 0:  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = "barrels must be a strictly positive integer."
            raise ValueError(msg)
        if self.material not in {
            CulvertMaterialName.CONCRETE_PIPE,
            CulvertMaterialName.CORRUGATED_STEEL,
        }:
            msg = "TUFLOW engine integration currently supports circular concrete or corrugated-steel pipes."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)

    @property
    def hw_diameter_m(self) -> float:
        """Nominal diameter used for TUFLOW HW/D targets and reporting."""
        return self.diameter_m if self.nominal_diameter_m is None else self.nominal_diameter_m


@dataclass(frozen=True, slots=True)
class CulvertEngineResult:
    """Normalized result shared by both hydraulic engines."""

    engine: CulvertEngine
    crossing: str
    scenario: str
    requested_discharge_m3s: float | None
    requested_headwater_m: float | None
    computed_discharge_m3s: float
    headwater_elevation_m: float
    headwater_ratio: float
    outlet_velocity_mps: float
    flow_type: str
    roadway_discharge_m3s: float
    overtopping: bool
    status: str
    warnings: tuple[str, ...] = ()
    workspace: Path | None = None


def _engine(value: CulvertEngine | str) -> CulvertEngine:
    return value if isinstance(value, CulvertEngine) else CulvertEngine(value)


def _solver_configuration(definition: TuflowCircularCulvert) -> SolverConfiguration:
    """Match native coefficient assumptions to the corresponding HY-8 inlet."""
    if definition.material is CulvertMaterialName.CORRUGATED_STEEL:
        return replace(
            DEFAULT_SOLVER_CONFIGURATION,
            default_circular_cmp_inlet=CIRCULAR_CMP_PROJECTING,
            default_circular_cmp_loss=PIPE_CMP_LOSS_PROJECTING,
        )
    return DEFAULT_SOLVER_CONFIGURATION


def _solver_crossing(definition: TuflowCircularCulvert) -> SolverCrossing:
    if definition.outlet_invert_m > definition.inlet_invert_m:
        msg = "ryan-culverts does not support adverse slopes: outlet_invert_m must not exceed inlet_invert_m."
        raise ValueError(msg)
    barrel = CircularBarrelDefinition(
        diameter_mm=definition.diameter_m * 1000.0,
        length=definition.length_m,
        inlet_invert=definition.inlet_invert_m,
        outlet_invert=definition.outlet_invert_m,
        roughness=definition.roughness_manning_n,
        material=definition.material,
        label=definition.name,
    )
    crossing = CrossingDefinition(
        name=definition.name,
        groups=(CulvertGroupDefinition(name=definition.name, barrel=barrel, quantity=definition.barrels),),
    )
    return build_solver_crossing(crossing)


def _solver_result(
    definition: TuflowCircularCulvert,
    *,
    scenario: str,
    discharge_m3s: float,
    tailwater_elevation_m: float,
    requested_headwater_m: float | None,
) -> CulvertEngineResult:
    crossing = _solver_crossing(definition)
    result = solve_crossing_hydraulics(
        crossing=crossing,
        total_discharge=discharge_m3s,
        tailwater=tailwater_elevation_m,
        configuration=_solver_configuration(definition),
    )
    active = tuple(item for item in result.group_results if item.barrel_discharge > 0.0)
    velocity = max((item.barrel_result.velocity_outlet for item in active), default=0.0)
    regimes = tuple(dict.fromkeys(item.barrel_result.regime.value for item in active))
    warning_codes = [warning.code.value for item in active for warning in item.barrel_result.warnings]
    warning_codes.extend(notice.code.value for notice in result.applicability_notices)
    warnings = tuple(dict.fromkeys(warning_codes))
    return CulvertEngineResult(
        engine=CulvertEngine.RYAN_CULVERTS,
        crossing=definition.name,
        scenario=scenario,
        requested_discharge_m3s=None if requested_headwater_m is not None else discharge_m3s,
        requested_headwater_m=requested_headwater_m,
        computed_discharge_m3s=result.total_discharge,
        headwater_elevation_m=result.headwater_elevation,
        headwater_ratio=(result.headwater_elevation - definition.inlet_invert_m) / definition.hw_diameter_m,
        outlet_velocity_mps=velocity,
        flow_type=";".join(regimes),
        roadway_discharge_m3s=result.roadway_discharge,
        overtopping=result.roadway_discharge > 0.0,
        status=result.status.value,
        warnings=warnings,
    )


def _hy8_safe_name(name: str) -> str:
    """Return a deterministic Windows-filename-safe HY-8 internal name."""
    safe = "".join(ch if ch.isascii() and (ch.isalnum() or ch in "-_.") else "_" for ch in name).strip(" .")
    if not safe:
        safe = "culvert"
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    return f"{safe[:80]}__{digest}"


def _hy8_crossing(
    definition: TuflowCircularCulvert,
    *,
    tailwater_elevation_m: float,
    seed_discharge_m3s: float,
) -> tuple[Hy8Project, Hy8Crossing]:
    hy8_name = _hy8_safe_name(definition.name)
    project = Hy8Project(title=hy8_name, units=UnitSystem.SI, exit_loss_option=0)
    crossing = Hy8Crossing(name=hy8_name)
    project.crossings.append(crossing)
    crossing.flow = FlowDefinition(
        method=FlowMethod.USER_DEFINED,
        user_values=[max(seed_discharge_m3s, 0.05)],
    )
    crossing.tailwater.set_constant(
        elevation=tailwater_elevation_m,
        invert=definition.outlet_invert_m,
    )
    crossing.roadway.width = 10.0
    crossing.roadway.stations = [0.0, 10.0]
    crest = definition.inlet_invert_m + 50.0
    crossing.roadway.elevations = [crest, crest]

    if definition.material is CulvertMaterialName.CONCRETE_PIPE:
        material = Hy8Material.CONCRETE
        inlet_configuration = CircularConcreteInlet.SQUARE_EDGE_WITH_HEADWALL
    elif definition.material is CulvertMaterialName.CORRUGATED_STEEL:
        material = Hy8Material.CORRUGATED_STEEL
        inlet_configuration = CircularCorrugatedSteelInlet.THIN_EDGE_PROJECTING
    else:
        msg = f"Unsupported HY-8 material: {definition.material.value}"
        raise ValueError(msg)

    barrel = Hy8Barrel(
        name=f"{hy8_name} Barrel",
        span=definition.diameter_m,
        rise=definition.diameter_m,
        shape=Hy8Shape.CIRCLE,
        material=material,
        number_of_barrels=definition.barrels,
        inlet_invert_station=0.0,
        inlet_invert_elevation=definition.inlet_invert_m,
        outlet_invert_station=definition.length_m,
        outlet_invert_elevation=definition.outlet_invert_m,
        inlet_type=InletType.STRAIGHT,
        inlet_configuration=inlet_configuration,
    )
    barrel.manning_n_top = definition.roughness_manning_n
    barrel.manning_n_bottom = definition.roughness_manning_n
    crossing.culverts = [barrel]
    errors = crossing.validate()
    if errors:
        msg = "; ".join(errors)
        raise ValueError(msg)
    return project, crossing


def _hy8_result(
    definition: TuflowCircularCulvert,
    *,
    scenario: str,
    result: HydraulicsResult,
) -> CulvertEngineResult:
    row = result.row
    if row is None:
        msg = "HY-8 returned no result row."
        raise ValueError(msg)
    roadway = 0.0 if isnan(row.roadway_discharge) else row.roadway_discharge
    return CulvertEngineResult(
        engine=CulvertEngine.HY8,
        crossing=definition.name,
        scenario=scenario,
        requested_discharge_m3s=result.requested_flow,
        requested_headwater_m=result.requested_headwater,
        computed_discharge_m3s=result.computed_flow,
        headwater_elevation_m=result.computed_headwater,
        headwater_ratio=(result.computed_headwater - definition.inlet_invert_m) / definition.hw_diameter_m,
        outlet_velocity_mps=row.velocity,
        flow_type=row.flow_type,
        roadway_discharge_m3s=roadway,
        overtopping=row.overtopping or roadway > 0.0,
        status="success",
        workspace=result.workspace,
    )


def solve_tuflow_culvert_forward(
    definition: TuflowCircularCulvert,
    *,
    scenario: str,
    discharge_m3s: float,
    tailwater_elevation_m: float,
    engine: CulvertEngine | str,
    hy8: Path | str | None = None,
    workspace: Path | None = None,
    keep_workspace: bool = False,
) -> CulvertEngineResult:
    """Solve headwater for a prescribed crossing discharge."""
    selected = _engine(engine)
    if selected is CulvertEngine.RYAN_CULVERTS:
        return _solver_result(
            definition,
            scenario=scenario,
            discharge_m3s=discharge_m3s,
            tailwater_elevation_m=tailwater_elevation_m,
            requested_headwater_m=None,
        )
    project, crossing = _hy8_crossing(
        definition,
        tailwater_elevation_m=tailwater_elevation_m,
        seed_discharge_m3s=discharge_m3s,
    )
    result = crossing.hw_from_q(
        q=discharge_m3s,
        hy8=Path(hy8) if isinstance(hy8, str) else hy8,
        project=project,
        workspace=workspace,
        keep_files=keep_workspace,
    )
    return _hy8_result(definition, scenario=scenario, result=result)


def solve_tuflow_culvert_inverse(
    definition: TuflowCircularCulvert,
    *,
    scenario: str,
    headwater_elevation_m: float,
    tailwater_elevation_m: float,
    engine: CulvertEngine | str,
    q_hint_m3s: float | None = None,
    hy8: Path | str | None = None,
    workspace: Path | None = None,
    keep_workspace: bool = False,
) -> CulvertEngineResult:
    """Solve discharge for a prescribed headwater elevation."""
    selected = _engine(engine)
    if selected is CulvertEngine.RYAN_CULVERTS:
        crossing = _solver_crossing(definition)
        discharge = solve_crossing_discharge_for_headwater(
            crossing=crossing,
            headwater_elevation=headwater_elevation_m,
            tailwater=tailwater_elevation_m,
            configuration=_solver_configuration(definition),
        )
        return _solver_result(
            definition,
            scenario=scenario,
            discharge_m3s=discharge,
            tailwater_elevation_m=tailwater_elevation_m,
            requested_headwater_m=headwater_elevation_m,
        )
    project, crossing = _hy8_crossing(
        definition,
        tailwater_elevation_m=tailwater_elevation_m,
        seed_discharge_m3s=q_hint_m3s or 0.05,
    )
    result = crossing.q_from_hw(
        hw=headwater_elevation_m,
        q_hint=q_hint_m3s,
        hy8=Path(hy8) if isinstance(hy8, str) else hy8,
        project=project,
        workspace=workspace,
        keep_files=keep_workspace,
    )
    return _hy8_result(definition, scenario=scenario, result=result)


__all__ = [
    "CulvertEngine",
    "CulvertEngineResult",
    "TuflowCircularCulvert",
    "solve_tuflow_culvert_forward",
    "solve_tuflow_culvert_inverse",
]
