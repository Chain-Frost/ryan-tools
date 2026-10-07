"""Selectable hydraulic backends for TUFLOW culvert integration."""

import hashlib
from dataclasses import dataclass, field, replace
from enum import StrEnum
from math import isfinite, isnan
from pathlib import Path

from culvert_solver import (
    CIRCULAR_CMP_HEADWALL,
    CIRCULAR_CMP_MITERED,
    CIRCULAR_CMP_PROJECTING,
    CIRCULAR_CONCRETE_GROOVE_END,
    CIRCULAR_CONCRETE_SQUARE_EDGE,
    DEFAULT_SOLVER_CONFIGURATION,
    PIPE_CMP_LOSS_HEADWALL,
    PIPE_CMP_LOSS_PROJECTING,
    PIPE_LOSS_SOCKET_END,
    PIPE_LOSS_SQUARE_EDGE,
    EntranceLossCoefficient,
    InletCoefficients,
    SolverConfiguration,
    solve_crossing_discharge_for_headwater,
    solve_crossing_hydraulics,
)
from culvert_solver import CulvertCrossing as SolverCrossing
from culvert_solver.outlet_control import PIPE_CMP_MITERED
from run_hy8 import (
    CircularConcreteInlet,
    CircularCorrugatedSteelInlet,
    CircularHdpeInlet,
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
from .tuflow_configuration import (
    CircularCulvertConfiguration,
    CircularInletConfiguration,
    TuflowLossParameters,
)


class CulvertEngine(StrEnum):
    """Hydraulic engine available to TUFLOW culvert workflows."""

    HY8 = "hy8"
    RYAN_CULVERTS = "ryan-culverts"


@dataclass(frozen=True, slots=True)
class TuflowCircularCulvert:
    """Engine-neutral circular culvert definition in SI units."""

    name: str
    configuration: CircularCulvertConfiguration
    diameter_m: float
    length_m: float
    inlet_invert_m: float
    outlet_invert_m: float
    roughness_manning_n: float
    barrels: int = 1
    nominal_diameter_m: float | None = None
    losses: TuflowLossParameters = field(default_factory=TuflowLossParameters)

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty."
            raise ValueError(msg)
        if not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            self.configuration, CircularCulvertConfiguration
        ):
            msg = "configuration must be CircularCulvertConfiguration."
            raise TypeError(msg)
        if not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            self.losses, TuflowLossParameters
        ):
            msg = "losses must be TuflowLossParameters."
            raise TypeError(msg)
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
        if isinstance(barrels, bool) or not isinstance(barrels, int) or barrels <= 0:  # pyright: ignore[reportUnnecessaryIsInstance]
            msg = "barrels must be a strictly positive integer."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)

    @property
    def material(self) -> CulvertMaterialName:
        return self.configuration.material

    @property
    def inlet_configuration(self) -> CircularInletConfiguration:
        return self.configuration.inlet

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


def _native_physical_parameters(
    configuration: CircularCulvertConfiguration,
) -> tuple[InletCoefficients, EntranceLossCoefficient]:
    key = (configuration.material, configuration.inlet)
    supported: dict[
        tuple[CulvertMaterialName, CircularInletConfiguration],
        tuple[InletCoefficients, EntranceLossCoefficient],
    ] = {
        (
            CulvertMaterialName.CONCRETE_PIPE,
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        ): (CIRCULAR_CONCRETE_SQUARE_EDGE, PIPE_LOSS_SQUARE_EDGE),
        (
            CulvertMaterialName.CONCRETE_PIPE,
            CircularInletConfiguration.GROOVED_END_HEADWALL,
        ): (CIRCULAR_CONCRETE_GROOVE_END, PIPE_LOSS_SOCKET_END),
        (
            CulvertMaterialName.CORRUGATED_STEEL,
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL,
        ): (CIRCULAR_CMP_HEADWALL, PIPE_CMP_LOSS_HEADWALL),
        (
            CulvertMaterialName.CORRUGATED_STEEL,
            CircularInletConfiguration.THIN_EDGE_PROJECTING,
        ): (CIRCULAR_CMP_PROJECTING, PIPE_CMP_LOSS_PROJECTING),
        (
            CulvertMaterialName.CORRUGATED_STEEL,
            CircularInletConfiguration.MITERED_TO_SLOPE,
        ): (CIRCULAR_CMP_MITERED, PIPE_CMP_MITERED),
    }
    try:
        return supported[key]
    except KeyError as exc:
        msg = (
            "ryan-culverts does not yet provide a mapped inlet coefficient set for "
            f"{configuration.material.value}/{configuration.inlet.value}. "
            "Use HY-8 or extend the native adapter."
        )
        raise ValueError(msg) from exc


def _effective_width_contraction(value: float) -> float:
    return 1.0 if value <= 0.0 or value > 1.0 else value


def _validate_common_loss_support(losses: TuflowLossParameters) -> None:
    if losses.exit_loss_coefficient is not None and abs(losses.exit_loss_coefficient - 1.0) > 1e-12:
        msg = "The current adapters only represent TUFLOW exit loss coefficient 1.0."
        raise ValueError(msg)
    if losses.form_loss_coefficient is not None and abs(losses.form_loss_coefficient) > 1e-12:
        msg = "The current adapters do not yet represent non-zero TUFLOW Form_Loss."
        raise ValueError(msg)
    if losses.width_contraction_coefficient is not None:
        effective = _effective_width_contraction(losses.width_contraction_coefficient)
        if abs(effective - 1.0) > 1e-12:
            msg = "The current adapters only represent effective circular width-contraction factor 1.0."
            raise ValueError(msg)


def _solver_configuration(definition: TuflowCircularCulvert) -> SolverConfiguration:
    """Map physical inlet configuration to native coefficient defaults."""
    _validate_common_loss_support(definition.losses)
    if definition.material is CulvertMaterialName.SMOOTH_HDPE:
        msg = "ryan-culverts requires explicit HDPE inlet coefficients; use HY-8 or extend the native adapter."
        raise ValueError(msg)
    inlet, standard_loss = _native_physical_parameters(definition.configuration)
    if definition.material is CulvertMaterialName.CONCRETE_PIPE:
        return replace(
            DEFAULT_SOLVER_CONFIGURATION,
            default_circular_concrete_inlet=inlet,
            default_circular_concrete_loss=standard_loss,
        )
    if definition.material is CulvertMaterialName.CORRUGATED_STEEL:
        return replace(
            DEFAULT_SOLVER_CONFIGURATION,
            default_circular_cmp_inlet=inlet,
            default_circular_cmp_loss=standard_loss,
        )
    msg = "ryan-culverts requires explicit HDPE inlet coefficients; use HY-8 or extend the native adapter."
    raise ValueError(msg)


def _solver_crossing(definition: TuflowCircularCulvert) -> SolverCrossing:
    if definition.material is CulvertMaterialName.SMOOTH_HDPE:
        msg = "ryan-culverts requires explicit HDPE inlet coefficients; use HY-8 or extend the native adapter."
        raise ValueError(msg)
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
    solver_crossing = build_solver_crossing(crossing)
    entry_loss = definition.losses.entry_loss_coefficient
    if entry_loss is None:
        return solver_crossing
    groups = tuple(
        replace(
            group,
            barrel=replace(group.barrel, entrance_loss_coefficient=entry_loss),
        )
        for group in solver_crossing.groups
    )
    return replace(solver_crossing, groups=groups)


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


def _hy8_material_and_inlet(
    configuration: CircularCulvertConfiguration,
) -> tuple[
    Hy8Material,
    CircularConcreteInlet | CircularCorrugatedSteelInlet | CircularHdpeInlet,
]:
    material = configuration.material
    inlet = configuration.inlet
    if material is CulvertMaterialName.CONCRETE_PIPE:
        mapping = {
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL: CircularConcreteInlet.SQUARE_EDGE_WITH_HEADWALL,
            CircularInletConfiguration.GROOVED_END_PROJECTING: CircularConcreteInlet.GROOVED_END_PROJECTING,
            CircularInletConfiguration.GROOVED_END_HEADWALL: CircularConcreteInlet.GROOVED_END_IN_HEADWALL,
            CircularInletConfiguration.MITERED_TO_SLOPE: CircularConcreteInlet.MITERED_TO_CONFORM_TO_SLOPE,
            CircularInletConfiguration.BEVELED_EDGE_1_TO_1: CircularConcreteInlet.BEVELED_EDGE_1_TO_1,
            CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1: CircularConcreteInlet.BEVELED_EDGE_1_5_TO_1,
        }
        return Hy8Material.CONCRETE, mapping[inlet]
    if material is CulvertMaterialName.CORRUGATED_STEEL:
        mapping = {
            CircularInletConfiguration.THIN_EDGE_PROJECTING: CircularCorrugatedSteelInlet.THIN_EDGE_PROJECTING,
            CircularInletConfiguration.MITERED_TO_SLOPE: CircularCorrugatedSteelInlet.MITERED_TO_CONFORM_TO_SLOPE,
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL: CircularCorrugatedSteelInlet.SQUARE_EDGE_WITH_HEADWALL,
            CircularInletConfiguration.BEVELED_EDGE_1_TO_1: CircularCorrugatedSteelInlet.BEVELED_EDGE_1_TO_1,
            CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1: CircularCorrugatedSteelInlet.BEVELED_EDGE_1_5_TO_1,
        }
        return Hy8Material.CORRUGATED_STEEL, mapping[inlet]
    if material is CulvertMaterialName.SMOOTH_HDPE:
        mapping = {
            CircularInletConfiguration.SQUARE_EDGE_HEADWALL: CircularHdpeInlet.SQUARE_EDGE_WITH_HEADWALL,
            CircularInletConfiguration.THIN_EDGE_PROJECTING: CircularHdpeInlet.THIN_EDGE_PROJECTING,
            CircularInletConfiguration.MITERED_TO_SLOPE: CircularHdpeInlet.MITERED_TO_CONFORM_TO_SLOPE,
            CircularInletConfiguration.BEVELED_EDGE_1_TO_1: CircularHdpeInlet.BEVELED_EDGE_1_TO_1,
            CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1: CircularHdpeInlet.BEVELED_EDGE_1_5_TO_1,
        }
        return Hy8Material.HDPE, mapping[inlet]
    msg = f"Unsupported HY-8 material: {material.value}"
    raise ValueError(msg)


_STANDARD_ENTRY_LOSS: dict[
    tuple[CulvertMaterialName, CircularInletConfiguration],
    float,
] = {
    (CulvertMaterialName.CONCRETE_PIPE, CircularInletConfiguration.SQUARE_EDGE_HEADWALL): 0.5,
    (CulvertMaterialName.CONCRETE_PIPE, CircularInletConfiguration.GROOVED_END_PROJECTING): 0.2,
    (CulvertMaterialName.CONCRETE_PIPE, CircularInletConfiguration.GROOVED_END_HEADWALL): 0.2,
    (CulvertMaterialName.CONCRETE_PIPE, CircularInletConfiguration.MITERED_TO_SLOPE): 0.7,
    (CulvertMaterialName.CONCRETE_PIPE, CircularInletConfiguration.BEVELED_EDGE_1_TO_1): 0.2,
    (CulvertMaterialName.CONCRETE_PIPE, CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1): 0.2,
    (CulvertMaterialName.CORRUGATED_STEEL, CircularInletConfiguration.THIN_EDGE_PROJECTING): 0.9,
    (CulvertMaterialName.CORRUGATED_STEEL, CircularInletConfiguration.SQUARE_EDGE_HEADWALL): 0.5,
    (CulvertMaterialName.CORRUGATED_STEEL, CircularInletConfiguration.MITERED_TO_SLOPE): 0.7,
    (CulvertMaterialName.CORRUGATED_STEEL, CircularInletConfiguration.BEVELED_EDGE_1_TO_1): 0.2,
    (CulvertMaterialName.CORRUGATED_STEEL, CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1): 0.2,
    (CulvertMaterialName.SMOOTH_HDPE, CircularInletConfiguration.SQUARE_EDGE_HEADWALL): 0.5,
    (CulvertMaterialName.SMOOTH_HDPE, CircularInletConfiguration.THIN_EDGE_PROJECTING): 0.9,
    (CulvertMaterialName.SMOOTH_HDPE, CircularInletConfiguration.MITERED_TO_SLOPE): 0.7,
    (CulvertMaterialName.SMOOTH_HDPE, CircularInletConfiguration.BEVELED_EDGE_1_TO_1): 0.2,
    (CulvertMaterialName.SMOOTH_HDPE, CircularInletConfiguration.BEVELED_EDGE_1_5_TO_1): 0.2,
}


def _validate_hy8_losses(definition: TuflowCircularCulvert) -> None:
    _validate_common_loss_support(definition.losses)
    entry_loss = definition.losses.entry_loss_coefficient
    if entry_loss is None:
        return
    expected = _STANDARD_ENTRY_LOSS[(definition.material, definition.inlet_configuration)]
    if abs(entry_loss - expected) > 1e-12:
        msg = (
            "run-hy8 does not yet expose an arbitrary TUFLOW EntryC override. "
            f"The selected physical inlet implies standard Ke={expected:g}, but the "
            f"TUFLOW value is {entry_loss:g}."
        )
        raise ValueError(msg)


def _hy8_crossing(
    definition: TuflowCircularCulvert,
    *,
    tailwater_elevation_m: float,
    seed_discharge_m3s: float,
) -> tuple[Hy8Project, Hy8Crossing]:
    _validate_hy8_losses(definition)
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

    material, inlet_configuration = _hy8_material_and_inlet(definition.configuration)
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
