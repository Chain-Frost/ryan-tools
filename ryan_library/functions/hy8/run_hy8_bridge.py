"""Convert TUFLOW culvert maximum exports into run_hy8 models."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence, cast

from loguru import logger
from pandas import DataFrame

from run_hy8 import (
    CulvertBarrel,
    CulvertCrossing,
    CulvertMaterial,
    CulvertShape,
    FlowDefinition,
    FlowMethod,
    Hy8Project,
    RoadwaySurface,
    TailwaterDefinition,
    UnitSystem,
)

_SLUG_PATTERN: re.Pattern[str] = re.compile(pattern=r"[^A-Za-z0-9]+")


def _empty_raw_mapping() -> dict[str, Any]:
    return {}


def _default_shape_map() -> dict[str, CulvertShape]:
    return {"C": CulvertShape.CIRCLE, "R": CulvertShape.BOX}


def _default_material_map() -> dict[str, CulvertMaterial]:
    return {
        "R": CulvertMaterial.CONCRETE,
        "C": CulvertMaterial.CORRUGATED_STEEL,
    }


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def _coerce_string(value: Any) -> str:
    if value is None:
        return ""
    text: str = str(value).strip()
    return text


def _slugify(value: str, *, fallback: str = "Crossing") -> str:
    candidate: str = _SLUG_PATTERN.sub("_", value).strip("_")
    return candidate or fallback


def _compose_label(internal_name: str, chan_id: str, row_index: int | str | None) -> str:
    parts: list[str] = []
    if internal_name:
        parts.append(internal_name)
    if chan_id:
        parts.append(chan_id)
    if row_index is not None:
        parts.append(f"row={row_index}")
    if not parts:
        return "unknown culvert"
    return " / ".join(parts)


@dataclass(slots=True)
class Hy8CulvertOptions:
    """Configuration controlling how TUFLOW rows are mapped to HY-8 objects."""

    units: UnitSystem = UnitSystem.SI
    default_material: CulvertMaterial = CulvertMaterial.CONCRETE
    material_by_flag: dict[str, CulvertMaterial] = field(default_factory=_default_material_map)
    shape_by_flag: dict[str, CulvertShape] = field(default_factory=_default_shape_map)
    default_number_of_barrels: int = 1
    roadway_width_multiplier: float = 6.0
    minimum_roadway_width: float = 5.0
    roadway_crest_offset: float = 0.5
    tailwater_minimum_gap: float = 0.05
    flow_minimum_factor: float = 0.9
    flow_maximum_factor: float = 1.1
    minimum_flow_cms: float = 0.005
    crossing_name_template: str = "{internal}_{chan}"
    flow_builder: Callable[["CulvertMaximumRecord", "Hy8CulvertOptions"], FlowDefinition] | None = None
    roadway_width_builder: Callable[["CulvertMaximumRecord", "Hy8CulvertOptions"], float] | None = None
    roadway_crest_builder: Callable[["CulvertMaximumRecord", "Hy8CulvertOptions"], float] | None = None
    barrel_count_builder: Callable[["CulvertMaximumRecord", "Hy8CulvertOptions"], int] | None = None
    default_inlet_type: int = 1
    default_inlet_edge_type: int = 0
    default_inlet_edge_type71: int = 0
    default_improved_inlet_edge_type: int = 0


@dataclass(slots=True)
class CulvertMaximumRecord:
    """Strongly-typed view over the culvert maximum export inputs."""

    row_index: int | str | None
    trim_runcode: str
    internal_name: str
    chan_id: str
    flow_q: float
    ds_headwater: float
    ds_invert: float
    us_invert: float
    height: float
    length: float
    flag: str
    velocity: float | None = None
    us_headwater: float | None = None
    us_obvert: float | None = None
    ds_obvert: float | None = None
    mannings_n: float | None = None
    slope_percent: float | None = None
    blockage_percent: float | None = None
    area_culv: float | None = None
    raw: Mapping[str, Any] = field(default_factory=_empty_raw_mapping)

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any], *, row_index: int | str | None) -> "CulvertMaximumRecord | None":
        trim_runcode: str = _coerce_string(row.get("trim_runcode"))
        internal_name: str = _coerce_string(row.get("internalName"))
        chan_id: str = _coerce_string(row.get("Chan ID"))
        q: float | None = _coerce_float(row.get("Q"))
        ds_headwater: float | None = _coerce_float(row.get("DS_h"))
        ds_invert: float | None = _coerce_float(row.get("DS Invert"))
        us_invert: float | None = _coerce_float(row.get("US Invert"))
        height: float | None = _coerce_float(row.get("Height"))
        length: float = _coerce_float(row.get("Length")) or 0.0
        flag: str = _coerce_string(row.get("Flags")).upper()
        label: str = _compose_label(internal_name, chan_id, row_index)

        missing: list[str] = []
        if q is None or q <= 0:
            missing.append("Q")
        if ds_headwater is None:
            missing.append("DS_h")
        if ds_invert is None:
            missing.append("DS Invert")
        if us_invert is None:
            missing.append("US Invert")
        if height is None or height <= 0:
            missing.append("Height")
        if missing:
            logger.warning("Skipping %s because %s are missing.", label, ", ".join(missing))
            return None

        assert q is not None
        assert ds_headwater is not None
        assert ds_invert is not None
        assert us_invert is not None
        assert height is not None

        ds_obvert: float | None = _coerce_float(row.get("DS Obvert"))
        us_obvert: float | None = _coerce_float(row.get("US Obvert"))
        us_headwater: float | None = _coerce_float(row.get("US_h"))
        velocity: float | None = _coerce_float(row.get("V"))
        slope_percent: float | None = _coerce_float(row.get("pSlope"))
        blockage_percent: float | None = _coerce_float(row.get("pBlockage"))
        mannings_n: float | None = _coerce_float(row.get("n or Cd"))
        area_culv: float | None = _coerce_float(row.get("Area_Culv")) or _coerce_float(row.get("area_culv"))
        normalized_length: float = length if length > 0 else height
        raw_mapping: dict[str, Any] = {str(key): value for key, value in row.items()}

        return cls(
            row_index=row_index,
            trim_runcode=trim_runcode,
            internal_name=internal_name,
            chan_id=chan_id,
            flow_q=q,
            ds_headwater=ds_headwater,
            ds_invert=ds_invert,
            us_invert=us_invert,
            height=height,
            length=normalized_length,
            flag=flag,
            velocity=velocity,
            us_headwater=us_headwater,
            us_obvert=us_obvert,
            ds_obvert=ds_obvert,
            mannings_n=mannings_n,
            slope_percent=slope_percent,
            blockage_percent=blockage_percent,
            area_culv=area_culv,
            raw=raw_mapping,
        )

    @property
    def identifier(self) -> str:
        label: str = _compose_label(self.internal_name, self.chan_id, self.row_index)
        if self.trim_runcode:
            return f"{label} ({self.trim_runcode})"
        return label


def maximums_dataframe_to_crossings(
    maximums: DataFrame,
    *,
    options: Hy8CulvertOptions | None = None,
) -> list[CulvertCrossing]:
    """Convert the Maximums sheet into HY-8 crossings."""

    cfg: Hy8CulvertOptions = options or Hy8CulvertOptions()
    if maximums.empty:
        return []

    raw_rows = cast(
        list[dict[str, Any]],
        maximums.to_dict(orient="records"),  # pyright: ignore[reportUnknownMemberType]
    )
    indexes: Sequence[int | str] = list(maximums.index)
    crossings: list[CulvertCrossing] = []
    for idx, raw_row in zip(indexes, raw_rows):
        record: CulvertMaximumRecord | None = CulvertMaximumRecord.from_mapping(raw_row, row_index=idx)
        if record is None:
            continue
        crossing: CulvertCrossing = build_crossing_from_record(record, options=cfg)
        crossings.append(crossing)
    logger.info("Built %d HY-8 crossings from %d rows.", len(crossings), len(raw_rows))
    return crossings


def maximums_dataframe_to_project(
    maximums: DataFrame,
    *,
    project_title: str,
    designer: str = "",
    project_notes: str | None = None,
    options: Hy8CulvertOptions | None = None,
) -> Hy8Project:
    """Create a Hy8Project populated with crossings from the provided DataFrame."""

    cfg: Hy8CulvertOptions = options or Hy8CulvertOptions()
    project = Hy8Project(title=project_title, designer=designer, units=cfg.units, notes=project_notes or "")
    for crossing in maximums_dataframe_to_crossings(maximums, options=cfg):
        project.crossings.append(crossing)
    return project


def build_crossing_from_record(
    record: CulvertMaximumRecord,
    *,
    options: Hy8CulvertOptions | None = None,
) -> CulvertCrossing:
    """Generate a single CulvertCrossing from one culvert maximum row."""

    cfg: Hy8CulvertOptions = options or Hy8CulvertOptions()
    name: str = _resolve_crossing_name(record, cfg)
    crossing = CulvertCrossing(name=name)
    crossing.notes = _build_notes(record)
    crossing.flow = _build_flow(record, cfg)
    crossing.tailwater = _build_tailwater(record, cfg)
    _configure_roadway(crossing, record, cfg)
    crossing.culverts = [_build_barrel(record, cfg)]
    return crossing


def _resolve_crossing_name(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> str:
    internal_slug: str = _slugify(record.internal_name or record.trim_runcode or "Scenario")
    chan_slug: str = _slugify(record.chan_id or "CHAN")
    template: str = options.crossing_name_template or "{internal}_{chan}"
    try:
        raw_name = template.format(internal=internal_slug, chan=chan_slug, record=record)
    except (KeyError, ValueError):
        raw_name = f"{internal_slug}_{chan_slug}"
    return _slugify(raw_name, fallback=internal_slug)


def _build_flow(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> FlowDefinition:
    if options.flow_builder is not None:
        return options.flow_builder(record, options)

    flow = FlowDefinition(method=FlowMethod.MIN_DESIGN_MAX)
    design: float = max(options.minimum_flow_cms, record.flow_q)
    minimum: float = max(options.minimum_flow_cms, design * options.flow_minimum_factor)
    maximum: float = max(design + options.minimum_flow_cms, design * options.flow_maximum_factor)
    if minimum >= design:
        design = minimum + options.minimum_flow_cms
    if design >= maximum:
        maximum = design + options.minimum_flow_cms
    flow.minimum = minimum
    flow.design = design
    flow.maximum = maximum
    return flow


def _build_tailwater(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> TailwaterDefinition:
    tailwater = TailwaterDefinition()
    tailwater.constant_elevation = record.ds_headwater
    tailwater.invert_elevation = min(record.ds_invert, tailwater.constant_elevation - options.tailwater_minimum_gap)
    if tailwater.constant_elevation <= tailwater.invert_elevation:
        tailwater.constant_elevation = record.ds_invert + options.tailwater_minimum_gap
        tailwater.invert_elevation = record.ds_invert
    return tailwater


def _configure_roadway(crossing: CulvertCrossing, record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> None:
    width: float = (
        options.roadway_width_builder(record, options)
        if options.roadway_width_builder
        else _default_roadway_width(record, options)
    )
    crest: float = (
        options.roadway_crest_builder(record, options)
        if options.roadway_crest_builder
        else _default_roadway_crest(record, options)
    )
    width = max(options.minimum_roadway_width, width)
    crossing.roadway.width = width
    crossing.roadway.surface = RoadwaySurface.PAVED
    half_width: float = width / 2.0
    crossing.roadway.stations = [-half_width, 0.0, half_width]
    crossing.roadway.elevations = [crest, crest, crest]


def _default_roadway_width(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> float:
    return max(options.minimum_roadway_width, record.height * options.roadway_width_multiplier)


def _default_roadway_crest(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> float:
    candidates: list[float] = []
    for candidate in (
        record.us_headwater,
        record.ds_headwater,
        record.us_obvert or (record.us_invert + record.height),
        record.ds_obvert or (record.ds_invert + record.height),
    ):
        if candidate is not None:
            candidates.append(candidate)
    base: float = max(candidates) if candidates else record.ds_headwater
    return base + max(0.0, options.roadway_crest_offset)


def _build_barrel(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> CulvertBarrel:
    barrel = CulvertBarrel(name=record.chan_id or "Barrel")
    barrel.material = _resolve_material(record, options)
    barrel.shape = _infer_shape(record, options)
    span, rise = _resolve_span_rise(record, barrel.shape)
    barrel.span = span
    barrel.rise = rise
    barrel.number_of_barrels = _resolve_barrel_count(record, options)
    barrel.inlet_invert_station = 0.0
    barrel.outlet_invert_station = record.length
    barrel.inlet_invert_elevation = record.us_invert
    barrel.outlet_invert_elevation = record.ds_invert
    if record.mannings_n and record.mannings_n > 0:
        barrel.manning_n_top = record.mannings_n
        barrel.manning_n_bottom = record.mannings_n
    barrel.inlet_type = options.default_inlet_type
    barrel.inlet_edge_type = options.default_inlet_edge_type
    barrel.inlet_edge_type71 = options.default_inlet_edge_type71
    barrel.improved_inlet_edge_type = options.default_improved_inlet_edge_type
    return barrel


def _infer_shape(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> CulvertShape:
    flag: str = record.flag
    if flag and flag in options.shape_by_flag:
        return options.shape_by_flag[flag]
    chan_hint: str = record.chan_id.split("_")[-1].upper() if record.chan_id else ""
    if chan_hint in options.shape_by_flag:
        return options.shape_by_flag[chan_hint]
    return CulvertShape.CIRCLE


def _resolve_span_rise(record: CulvertMaximumRecord, shape: CulvertShape) -> tuple[float, float]:
    if shape is CulvertShape.CIRCLE:
        return record.height, record.height
    # Without a width column we assume a square box and flag for follow-up in notes.
    return record.height, record.height


def _resolve_barrel_count(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> int:
    if options.barrel_count_builder is not None:
        count: int = options.barrel_count_builder(record, options)
        return max(1, count)
    if record.area_culv and record.area_culv > 0:
        single_area: float = _culvert_cross_section_area(record, options)
        if single_area > 0:
            derived: int = max(1, round(record.area_culv / single_area))
            logger.debug(
                "Derived %d barrels from Area_Culv %.3f m2 (single %.3f m2).",
                derived,
                record.area_culv,
                single_area,
            )
            return derived
        logger.debug(
            "Area-based barrel estimate not implemented for flag '%s'; falling back to defaults.",
            record.flag,
        )
    return max(1, options.default_number_of_barrels)


def _culvert_cross_section_area(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> float:
    shape: CulvertShape = _infer_shape(record, options)
    if shape is CulvertShape.CIRCLE:
        return math.pi * (record.height / 2.0) ** 2
    # TODO: support rectangular culverts once Area_Culv width inference is implemented.
    return 0.0


def _resolve_material(record: CulvertMaximumRecord, options: Hy8CulvertOptions) -> CulvertMaterial:
    flag: str = record.flag
    if flag and flag in options.material_by_flag:
        return options.material_by_flag[flag]
    return options.default_material


def _build_notes(record: CulvertMaximumRecord) -> str:
    entries: list[str] = []
    if record.trim_runcode:
        entries.append(f"run={record.trim_runcode}")
    if record.internal_name:
        entries.append(f"internalName={record.internal_name}")
    if record.chan_id:
        entries.append(f"chan={record.chan_id}")
    if record.slope_percent is not None:
        entries.append(f"pSlope={record.slope_percent}")
    if record.blockage_percent is not None:
        entries.append(f"pBlockage={record.blockage_percent}")
    if not entries:
        return record.identifier
    return "; ".join(entries)


__all__ = [
    "CulvertMaximumRecord",
    "Hy8CulvertOptions",
    "build_crossing_from_record",
    "maximums_dataframe_to_crossings",
    "maximums_dataframe_to_project",
]
