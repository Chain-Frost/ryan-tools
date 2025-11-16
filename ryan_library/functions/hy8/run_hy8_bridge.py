"""Convert TUFLOW culvert maximum exports into run_hy8 models."""

from __future__ import annotations

import copy
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from subprocess import CompletedProcess
from tempfile import TemporaryDirectory
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
    Hy8Executable,
    Hy8FileWriter,
    Hy8Project,
    Hy8Results,
    RoadwayProfile,
    RoadwaySurface,
    TailwaterDefinition,
    UnitSystem,
    parse_rsql,
    parse_rst,
)
from run_hy8.results import FlowProfile, Hy8ResultRow, Hy8Series

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


DEFAULT_UNITS: UnitSystem = UnitSystem.SI
DEFAULT_MATERIAL: CulvertMaterial = CulvertMaterial.CORRUGATED_STEEL
DEFAULT_NUMBER_OF_BARRELS: int = 1
TAILWATER_MINIMUM_GAP: float = 0.05
FLOW_MINIMUM_FACTOR: float = 0.9
FLOW_MAXIMUM_FACTOR: float = 1.1
MINIMUM_FLOW_CMS: float = 0.005
CROSSING_NAME_TEMPLATE: str = "{internal}_{chan}"
MATERIAL_BY_FLAG: dict[str, CulvertMaterial] = _default_material_map()
SHAPE_BY_FLAG: dict[str, CulvertShape] = _default_shape_map()
DEFAULT_INLET_TYPE: int = 1
DEFAULT_INLET_EDGE_TYPE: int = 0
DEFAULT_INLET_EDGE_TYPE71: int = 0
DEFAULT_IMPROVED_INLET_EDGE_TYPE: int = 0


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
    num_barrels: int | None = None
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
        num_barrels: float | None = _coerce_float(row.get("num_barrels")) or _coerce_float(row.get("number_interp"))
        barrels_int: int | None = None
        if num_barrels and num_barrels > 0:
            barrels_int = max(1, int(round(num_barrels)))
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
            num_barrels=barrels_int,
            raw=raw_mapping,
        )

    @property
    def identifier(self) -> str:
        label: str = _compose_label(self.internal_name, self.chan_id, self.row_index)
        if self.trim_runcode:
            return f"{label} ({self.trim_runcode})"
        return label


@dataclass(slots=True)
class Hy8SingleFlowResult:
    """Summary of a HY-8 run evaluated at a single requested discharge."""

    crossing_name: str
    requested_flow: float
    flow_used: float = math.nan
    headwater_elevation: float = math.nan
    headwater_depth: float = math.nan
    outlet_velocity: float = math.nan
    flow_type: str = ""
    roadway_discharge: float = math.nan
    overtopping: bool = False
    iterations: str = ""


def calculate_headwater_for_flow(
    record: CulvertMaximumRecord,
    flow_cms: float,
    *,
    hy8_executable: Hy8Executable | Path | str | None = None,
    workdir: Path | str | None = None,
    project_title: str | None = None,
    project_units: UnitSystem = DEFAULT_UNITS,
) -> Hy8SingleFlowResult:
    """Build a crossing from ``record`` and run HY-8 for a single discharge."""

    crossing: CulvertCrossing = build_crossing_from_record(record=record)
    title: str = project_title or record.identifier
    return run_crossing_for_flow(
        crossing=crossing,
        flow_cms=flow_cms,
        hy8_executable=hy8_executable,
        workdir=workdir,
        project_title=title,
        project_units=project_units,
    )


def run_crossing_for_flow(
    crossing: CulvertCrossing,
    flow_cms: float,
    *,
    hy8_executable: Hy8Executable | Path | str | None = None,
    workdir: Path | str | None = None,
    project_title: str | None = None,
    project_units: UnitSystem = DEFAULT_UNITS,
) -> Hy8SingleFlowResult:
    """Execute HY-8 for ``crossing`` to retrieve headwater, velocity, and flow type."""

    requested_flow: float = _validate_target_flow(flow_cms=flow_cms)
    crossing_copy: CulvertCrossing = copy.deepcopy(crossing)
    crossing_copy.flow = _build_single_flow_definition(flow_cms=requested_flow)
    project_name: str = project_title or crossing_copy.name or "Crossing"
    return _execute_single_flow_analysis(
        crossing=crossing_copy,
        requested_flow=requested_flow,
        unit_system=project_units,
        hy8_executable=hy8_executable,
        workdir=workdir,
        project_title=project_name,
    )


def maximums_dataframe_to_crossings(
    maximums: DataFrame,
) -> list[CulvertCrossing]:
    """Convert the Maximums sheet into HY-8 crossings."""

    if maximums.empty:
        return []

    raw_rows: list[dict[str, Any]] = cast(
        list[dict[str, Any]],
        maximums.to_dict(orient="records"),  # pyright: ignore[reportUnknownMemberType]
    )
    indexes: Sequence[int | str] = list(maximums.index)
    crossings: list[CulvertCrossing] = []
    for idx, raw_row in zip(indexes, raw_rows):
        record: CulvertMaximumRecord | None = CulvertMaximumRecord.from_mapping(raw_row, row_index=idx)
        if record is None:
            continue
        if record.flag and record.flag.upper() != "C":
            logger.debug(
                "Skipping %s because flag '%s' is not yet supported for HY-8 bridging.", record.identifier, record.flag
            )
            continue
        crossing: CulvertCrossing = build_crossing_from_record(record)
        crossings.append(crossing)
    logger.info("Built %d HY-8 crossings from %d rows.", len(crossings), len(raw_rows))
    return crossings


def maximums_dataframe_to_project(
    maximums: DataFrame,
    *,
    project_title: str,
    designer: str = "",
    project_notes: str | None = None,
    units: UnitSystem = DEFAULT_UNITS,
) -> Hy8Project:
    """Create a Hy8Project populated with crossings from the provided DataFrame."""

    project = Hy8Project(title=project_title, designer=designer, units=units, notes=project_notes or "")
    for crossing in maximums_dataframe_to_crossings(maximums):
        project.crossings.append(crossing)
    return project


def build_crossing_from_record(
    record: CulvertMaximumRecord,
) -> CulvertCrossing:
    """Generate a single CulvertCrossing from one culvert maximum row."""

    name: str = _resolve_crossing_name(record)
    crossing = CulvertCrossing(name=name)
    crossing.notes = _build_notes(record)
    crossing.flow = _build_flow(record)
    crossing.tailwater = _build_tailwater(record)
    _configure_roadway(crossing, record)
    crossing.culverts = [_build_barrel(record)]
    return crossing


def _resolve_crossing_name(record: CulvertMaximumRecord) -> str:
    internal_slug: str = _slugify(record.internal_name or record.trim_runcode or "Scenario")
    chan_slug: str = _slugify(record.chan_id or "CHAN")
    template: str = CROSSING_NAME_TEMPLATE
    try:
        raw_name = template.format(internal=internal_slug, chan=chan_slug, record=record)
    except (KeyError, ValueError):
        raw_name = f"{internal_slug}_{chan_slug}"
    return _slugify(raw_name, fallback=internal_slug)


def _build_flow(record: CulvertMaximumRecord) -> FlowDefinition:
    flow = FlowDefinition(method=FlowMethod.MIN_DESIGN_MAX)
    design: float = max(MINIMUM_FLOW_CMS, record.flow_q)
    minimum: float = max(MINIMUM_FLOW_CMS, design * FLOW_MINIMUM_FACTOR)
    maximum: float = max(design + MINIMUM_FLOW_CMS, design * FLOW_MAXIMUM_FACTOR)
    if minimum >= design:
        design = minimum + MINIMUM_FLOW_CMS
    if design >= maximum:
        maximum = design + MINIMUM_FLOW_CMS
    flow.minimum = minimum
    flow.design = design
    flow.maximum = maximum
    return flow


def _build_tailwater(record: CulvertMaximumRecord) -> TailwaterDefinition:
    tailwater = TailwaterDefinition()
    tailwater.constant_elevation = record.ds_headwater
    tailwater.invert_elevation = record.ds_invert
    if tailwater.constant_elevation <= tailwater.invert_elevation:
        tailwater.constant_elevation = record.ds_invert
    return tailwater


def _configure_roadway(crossing: CulvertCrossing, record: CulvertMaximumRecord) -> None:
    barrel: CulvertBarrel | None = crossing.culverts[0] if crossing.culverts else None
    length: float = _roadway_length_from_barrel(barrel, record.length)
    width: float = _roadway_top_width(length)
    crest: float = _roadway_crest_from_barrel(barrel, record)
    width = max(0.5, width)
    crest_length: float = 10.0
    flat_start: float = max(0.0, (length - width) / 2.0)
    flat_end: float = min(length, flat_start + crest_length)
    if flat_end <= flat_start:
        flat_start = 0.0
        flat_end = min(length, crest_length)
    stations: list[float] = _unique_stations([0.0, flat_start, flat_end, length])
    if len(stations) < 2:
        stations = [0.0, max(length, 1.0)]
    elevations: list[float] = [crest for _ in stations]
    roadway = RoadwayProfile()
    roadway.width = width
    roadway.surface = RoadwaySurface.PAVED
    roadway.stations = stations
    roadway.elevations = elevations
    crossing.roadway = roadway


def _roadway_length_from_barrel(barrel: CulvertBarrel | None, fallback_length: float) -> float:
    if barrel is not None:
        length = abs(barrel.outlet_invert_station - barrel.inlet_invert_station)
        if length > 0:
            return length
        if barrel.span > 0:
            return barrel.span
    return max(fallback_length, 1.0)


def _roadway_top_width(length: float) -> float:
    usable: float = max(length - 0.1, 0.5)
    return min(5.0, usable)


def _roadway_crest_from_barrel(barrel: CulvertBarrel | None, record: CulvertMaximumRecord) -> float:
    if barrel is not None:
        height: float = _barrel_height(barrel, fallback=record.height)
        return barrel.inlet_invert_elevation + max(height, 0.1) * 10.0
    return record.us_invert + record.height * 10.0


def _barrel_height(barrel: CulvertBarrel, fallback: float) -> float:
    if barrel.rise > 0:
        return barrel.rise
    if barrel.span > 0:
        return barrel.span
    return max(fallback, 0.5)


def _unique_stations(values: Sequence[float]) -> list[float]:
    ordered: list[float] = []
    for value in values:
        if not ordered or not math.isclose(value, ordered[-1], rel_tol=0.0, abs_tol=1e-6):
            ordered.append(value)
    return ordered


def _build_barrel(record: CulvertMaximumRecord) -> CulvertBarrel:
    barrel = CulvertBarrel(name=record.chan_id or "Barrel")
    barrel.material = _resolve_material(record)
    barrel.shape = _infer_shape(record)
    span, rise = _resolve_span_rise(record, barrel.shape)
    barrel.span = span
    barrel.rise = rise
    barrel.number_of_barrels = _resolve_barrel_count(record)
    barrel.inlet_invert_station = 0.0
    barrel.outlet_invert_station = record.length
    barrel.inlet_invert_elevation = record.us_invert
    barrel.outlet_invert_elevation = record.ds_invert
    if record.mannings_n and record.mannings_n > 0:
        barrel.manning_n_top = record.mannings_n
        barrel.manning_n_bottom = record.mannings_n
    barrel.inlet_type = DEFAULT_INLET_TYPE
    barrel.inlet_edge_type = DEFAULT_INLET_EDGE_TYPE
    barrel.inlet_edge_type71 = DEFAULT_INLET_EDGE_TYPE71
    barrel.improved_inlet_edge_type = DEFAULT_IMPROVED_INLET_EDGE_TYPE
    return barrel


def _infer_shape(record: CulvertMaximumRecord) -> CulvertShape:
    flag: str = record.flag
    if flag and flag in SHAPE_BY_FLAG:
        return SHAPE_BY_FLAG[flag]
    chan_hint: str = record.chan_id.split("_")[-1].upper() if record.chan_id else ""
    if chan_hint in SHAPE_BY_FLAG:
        return SHAPE_BY_FLAG[chan_hint]
    return CulvertShape.CIRCLE


def _resolve_span_rise(record: CulvertMaximumRecord, shape: CulvertShape) -> tuple[float, float]:
    if shape is CulvertShape.CIRCLE:
        return record.height, record.height
    # Without a width column we assume a square box and flag for follow-up in notes.
    return record.height, record.height


def _resolve_barrel_count(record: CulvertMaximumRecord) -> int:
    if record.num_barrels and record.num_barrels > 0:
        return record.num_barrels
    logger.warning(
        "Missing num_barrels for %s; defaulting to %d. Provide num_barrels for circular culverts.",
        record.identifier,
        DEFAULT_NUMBER_OF_BARRELS,
    )
    return max(1, DEFAULT_NUMBER_OF_BARRELS)


def _resolve_material(record: CulvertMaximumRecord) -> CulvertMaterial:
    flag: str = record.flag
    if flag and flag in MATERIAL_BY_FLAG:
        return MATERIAL_BY_FLAG[flag]
    return DEFAULT_MATERIAL


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


def _validate_target_flow(flow_cms: float) -> float:
    try:
        value = float(flow_cms)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Flow must be numeric: {flow_cms!r}") from exc
    if math.isnan(value) or value <= 0:
        raise ValueError(f"Flow must be greater than zero (received {flow_cms!r}).")
    return value


def _build_single_flow_definition(flow_cms: float) -> FlowDefinition:
    target: float = max(MINIMUM_FLOW_CMS, flow_cms)
    flow = FlowDefinition(method=FlowMethod.USER_DEFINED)
    flow.user_values = [target]
    flow.minimum = target
    flow.design = target
    flow.maximum = target
    return flow


def _execute_single_flow_analysis(
    crossing: CulvertCrossing,
    *,
    requested_flow: float,
    unit_system: UnitSystem,
    hy8_executable: Hy8Executable | Path | str | None,
    workdir: Path | str | None,
    project_title: str,
) -> Hy8SingleFlowResult:
    executor: Hy8Executable = _coerce_executable(executable=hy8_executable)
    project = Hy8Project(title=project_title, designer="ryan-tools", units=unit_system)
    project.crossings.append(crossing)
    workdir_path, cleanup = _prepare_workdir(path=workdir)
    slug: str = _slugify(value=project_title, fallback="Crossing")
    hy8_path: Path = workdir_path / f"{slug}.hy8"
    try:
        Hy8FileWriter(project).write(output_path=hy8_path, overwrite=True)
        logger.debug("Running HY-8 for %s at %.3f cms (workdir=%s)", crossing.name, requested_flow, workdir_path)
        process: CompletedProcess[str] = executor.open_run_save(hy8_file=hy8_path, check=False)
        if process.returncode != 0:
            summary: str = _summarize_process_output(output=process.stderr) or _summarize_process_output(
                output=process.stdout
            )
            message: str = f"HY-8 returned exit code {process.returncode} for {hy8_path}"
            if summary:
                message = f"{message}:\n{summary}"
            raise RuntimeError(message)
        result: Hy8SingleFlowResult = _parse_single_crossing_results(
            crossing=crossing,
            hy8_path=hy8_path,
            requested_flow=requested_flow,
        )
        logger.debug(
            "HY-8 complete for %s: flow %.3f cms -> HW %.3f, velocity %.3f",
            crossing.name,
            requested_flow,
            result.headwater_elevation,
            result.outlet_velocity,
        )
        return result
    finally:
        cleanup()


def _prepare_workdir(path: Path | str | None) -> tuple[Path, Callable[[], None]]:
    if path is not None:
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        return target, lambda: None
    temp = TemporaryDirectory(prefix="hy8_single_flow_")
    return Path(temp.name), temp.cleanup


def _coerce_executable(executable: Hy8Executable | Path | str | None) -> Hy8Executable:
    if executable is None:
        return Hy8Executable()
    if isinstance(executable, Hy8Executable):
        return executable
    return Hy8Executable(Path(executable))


def _parse_single_crossing_results(
    crossing: CulvertCrossing,
    *,
    hy8_path: Path,
    requested_flow: float,
) -> Hy8SingleFlowResult:
    rst_path: Path = hy8_path.with_suffix(".rst")
    if not rst_path.exists():
        raise FileNotFoundError(f"HY-8 RST output not found for {hy8_path}.")
    rst_entries: dict[str, Hy8Series] = parse_rst(rst_path)
    entry: Hy8Series | None = rst_entries.get(crossing.name)
    if not entry:
        raise RuntimeError(f"Crossing '{crossing.name}' not found in {rst_path.name}.")
    rsql_path: Path = hy8_path.with_suffix(".rsql")
    profile_data: dict[str, list[FlowProfile]] = parse_rsql(rsql_path) if rsql_path.exists() else {}
    profiles: list[FlowProfile] | None = profile_data.get(crossing.name)
    results = Hy8Results(entry, profiles)
    row: Hy8ResultRow | None = results.nearest(requested_flow)
    if row is None:
        raise RuntimeError(f"HY-8 output did not include discharge values for {crossing.name}.")
    headwater_elevation, headwater_depth = _resolve_headwater_elevation(crossing, row)
    return Hy8SingleFlowResult(
        crossing_name=crossing.name,
        requested_flow=requested_flow,
        flow_used=row.flow,
        headwater_elevation=headwater_elevation,
        headwater_depth=headwater_depth,
        outlet_velocity=row.velocity,
        flow_type=row.flow_type,
        roadway_discharge=row.roadway_discharge,
        overtopping=row.overtopping,
        iterations=row.iterations,
    )


def _resolve_headwater_elevation(crossing: CulvertCrossing, row: Hy8ResultRow) -> tuple[float, float]:
    headwater_elevation: float = row.headwater_elevation
    headwater_depth: float = row.headwater_depth
    invert: float | None = _upstream_invert_reference(crossing)
    if invert is not None and not math.isnan(headwater_depth):
        headwater_elevation = invert + headwater_depth
    return headwater_elevation, headwater_depth


def _upstream_invert_reference(crossing: CulvertCrossing) -> float | None:
    if crossing.culverts:
        return crossing.culverts[0].inlet_invert_elevation
    return None


def _summarize_process_output(output: str, limit: int = 12) -> str:
    if not output:
        return ""
    lines: list[str] = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return ""
    if len(lines) > limit:
        omitted = len(lines) - limit
        lines = [f"... ({omitted} lines omitted) ...", *lines[-limit:]]
    return "\n".join(lines)


__all__ = [
    "CulvertMaximumRecord",
    "Hy8SingleFlowResult",
    "build_crossing_from_record",
    "calculate_headwater_for_flow",
    "maximums_dataframe_to_crossings",
    "maximums_dataframe_to_project",
    "run_crossing_for_flow",
]
