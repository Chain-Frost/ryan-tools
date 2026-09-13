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
)

_MATERIALS: dict[CulvertMaterialName, CulvertMaterial] = {
    CulvertMaterialName.CONCRETE_BOX: CONCRETE_BOX,
    CulvertMaterialName.CONCRETE_PIPE: CONCRETE_PIPE,
    CulvertMaterialName.CORRUGATED_STEEL: CORRUGATED_STEEL,
}


def build_solver_barrel(definition: BarrelDefinition) -> CulvertBarrel:
    """Convert a workflow barrel definition to the authoritative solver model."""
    if isinstance(definition, RectangularBarrelDefinition):
        geometry = RectangularGeometry.from_mm(definition.span_mm, definition.rise_mm)
    elif isinstance(definition, CircularBarrelDefinition):
        geometry = CircularGeometry.from_mm(definition.diameter_mm)
    else:
        raise TypeError(f"Unsupported barrel definition type: {type(definition).__name__}")
    return CulvertBarrel(
        geometry=geometry,
        length=definition.length,
        inlet_invert=definition.inlet_invert,
        outlet_invert=definition.outlet_invert,
        roughness=definition.roughness,
        material=_MATERIALS[definition.material],
        label=definition.label,
    )


def build_solver_group(definition: CulvertGroupDefinition) -> CulvertGroup:
    """Convert one workflow group definition to a solver group."""
    return CulvertGroup(
        barrel=build_solver_barrel(definition.barrel),
        quantity=definition.quantity,
    )


def build_solver_roadway(definition: RoadwayDefinition) -> RoadwayWeir:
    """Convert a workflow roadway definition to a solver roadway weir."""
    return RoadwayWeir(
        crest_elevation=definition.crest_elevation,
        crest_length=definition.crest_length,
        discharge_coefficient=definition.discharge_coefficient,
        label=definition.label,
    )


def build_solver_crossing(definition: CrossingDefinition) -> CulvertCrossing:
    """Convert a complete crossing through the public ``culvert_solver`` boundary."""
    roadway = None if definition.roadway is None else build_solver_roadway(definition.roadway)
    return CulvertCrossing(
        groups=tuple(build_solver_group(group) for group in definition.groups),
        roadway=roadway,
    )
