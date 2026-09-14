"""Bounded conversion from workflow definitions to public ``culvert_solver`` objects."""

from culvert_solver import (
    CONCRETE_BOX,
    CONCRETE_PIPE,
    CORRUGATED_STEEL,
    CircularGeometry,
    CulvertBarrel,
    CulvertCrossing,
    CulvertGroup,
    CulvertMaterial,
    RectangularGeometry,
    RoadwayCrestPoint,
    RoadwayCrestProfile,
    RoadwayOvertoppingInput,
    RoadwayProfileWeir,
    RoadwaySurface,
    RoadwayWeir,
)

from ...classes.culvert.crossing import (
    BarrelDefinition,
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    RectangularBarrelDefinition,
    RoadwayDefinition,
    RoadwayOvertoppingDefinition,
    RoadwayProfileDefinition,
    RoadwaySurfaceName,
)

_MATERIALS: dict[CulvertMaterialName, CulvertMaterial] = {
    CulvertMaterialName.CONCRETE_BOX: CONCRETE_BOX,
    CulvertMaterialName.CONCRETE_PIPE: CONCRETE_PIPE,
    CulvertMaterialName.CORRUGATED_STEEL: CORRUGATED_STEEL,
}

_ROADWAY_SURFACES: dict[RoadwaySurfaceName, RoadwaySurface] = {
    RoadwaySurfaceName.PAVED: RoadwaySurface.PAVED,
    RoadwaySurfaceName.GRAVEL: RoadwaySurface.GRAVEL,
}


def build_solver_barrel(definition: BarrelDefinition) -> CulvertBarrel:
    """Convert a workflow barrel definition to the authoritative solver model."""
    definition_value: object = definition
    if isinstance(definition_value, RectangularBarrelDefinition):
        typed_definition: BarrelDefinition = definition_value
        geometry = RectangularGeometry.from_mm(definition_value.span_mm, definition_value.rise_mm)
    elif isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
        definition_value, CircularBarrelDefinition
    ):
        typed_definition = definition_value
        geometry = CircularGeometry.from_mm(definition_value.diameter_mm)
    else:
        msg = f"Unsupported barrel definition type: {type(definition_value).__name__}"
        raise TypeError(msg)
    return CulvertBarrel(
        geometry=geometry,
        length=typed_definition.length,
        inlet_invert=typed_definition.inlet_invert,
        outlet_invert=typed_definition.outlet_invert,
        roughness=typed_definition.roughness,
        material=_MATERIALS[typed_definition.material],
        label=typed_definition.label,
    )


def build_solver_group(definition: CulvertGroupDefinition) -> CulvertGroup:
    """Convert one workflow group definition to a solver group."""
    return CulvertGroup(
        barrel=build_solver_barrel(definition.barrel),
        quantity=definition.quantity,
    )


def _solver_surface(surface: RoadwaySurfaceName | None) -> RoadwaySurface | None:
    return None if surface is None else _ROADWAY_SURFACES[surface]


def build_solver_roadway(definition: RoadwayOvertoppingDefinition) -> RoadwayOvertoppingInput:
    """Convert a workflow roadway definition to the authoritative public solver model."""
    if isinstance(definition, RoadwayDefinition):
        return RoadwayWeir(
            crest_elevation=definition.crest_elevation,
            crest_length=definition.crest_length,
            discharge_coefficient=definition.discharge_coefficient,
            label=definition.label,
            surface=_solver_surface(definition.surface),
        )
    if isinstance(definition, RoadwayProfileDefinition):
        profile = RoadwayCrestProfile(
            tuple(RoadwayCrestPoint(station=point.station, elevation=point.elevation) for point in definition.points)
        )
        return RoadwayProfileWeir(
            profile=profile,
            discharge_coefficient=definition.discharge_coefficient,
            label=definition.label,
            surface=_solver_surface(definition.surface),
        )
    msg = f"Unsupported roadway definition type: {type(definition).__name__}"
    raise TypeError(msg)


def build_solver_crossing(definition: CrossingDefinition) -> CulvertCrossing:
    """Convert a complete crossing through the public ``culvert_solver`` boundary."""
    roadway = None if definition.roadway is None else build_solver_roadway(definition.roadway)
    return CulvertCrossing(
        groups=tuple(build_solver_group(group) for group in definition.groups),
        roadway=roadway,
    )
