"""Group only observed co-located TUFLOW material/soil loss configurations.

Input pairs must be derived from aligned active-cell check grids or another
explicitly verified co-location source. Never manufacture a Cartesian product
from database inventories. This is an input-configuration audit, not a
prediction of actual infiltration or material loss depth.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal, Never, Protocol

from .soil_loss_validation import validated_soil_parameters


class MaterialWithLoss(Protocol):
    material_id: int
    initial_loss_mm: float | None
    continuing_loss_mm_h: float | None
    fraction_impervious: float | None
    status: str


class SoilWithLoss(Protocol):
    soil_id: int
    method: str
    parameters: tuple[tuple[str, str], ...]
    status: str


@dataclass(frozen=True, slots=True)
class ObservedPair:
    material_id: int
    soil_id: int
    cell_count: int


@dataclass(frozen=True, slots=True)
class EffectiveLossGroup:
    category: Literal["no-loss", "material-only", "soil-only", "coupled"]
    signature: tuple[str, ...]
    material_ids: tuple[int, ...]
    soil_ids: tuple[int, ...]
    observed_pairs: tuple[ObservedPair, ...]
    cell_count: int


class UnresolvedLossCombinationError(ValueError):
    """Spatial IDs or loss parameters cannot be interpreted without guessing."""


def _fail(message: str) -> Never:
    raise UnresolvedLossCombinationError(message)


def _number(value: str | float | int | None) -> str:
    if value is None:
        _fail("Missing loss parameter")
    try:
        number = Decimal(str(value).strip())
    except InvalidOperation:
        _fail(f"Invalid numeric loss parameter {value!r}")
    if not number.is_finite():
        _fail(f"Nonfinite loss parameter {value!r}")
    if number.is_zero():
        return "0"
    return format(number.normalize(), "f")


def _soil_signature(soil: SoilWithLoss) -> tuple[str, ...]:
    method = soil.method.strip().upper()
    if soil.status != "defined":
        _fail(f"Soil {soil.soil_id} is not resolved")
    if method == "NONE":
        return ()
    try:
        values = validated_soil_parameters(method, dict(soil.parameters))
    except ValueError as exc:
        _fail(f"Unresolved soil {soil.soil_id}: {exc}")
    by_name = dict(values)
    if method == "ILCL" and by_name.get("il") == by_name.get("cl") == "0":
        return ()
    return (f"method={method}", *(f"{key}={value}" for key, value in values))


def _material_signature(material: MaterialWithLoss, *, soil_active: bool) -> tuple[str, ...]:
    if material.status != "defined":
        _fail(f"Material {material.material_id} is not resolved")
    il = _number(material.initial_loss_mm)
    cl = _number(material.continuing_loss_mm_h)
    if Decimal(il) < 0 or Decimal(cl) < 0:
        _fail(f"Negative material loss for ID {material.material_id}")
    parts: list[str] = []
    if il != "0" or cl != "0":
        parts.extend([f"il={il}", f"cl={cl}"])
    if soil_active:
        # A material without rainfall IL/CL can still change infiltration by
        # its impervious fraction, so the fraction cannot be ignored.
        if material.fraction_impervious is None:
            _fail(f"Material {material.material_id} has unspecified impervious fraction")
        fraction = _number(material.fraction_impervious)
        if Decimal(fraction) < 0 or Decimal(fraction) > 1:
            _fail(f"Invalid impervious fraction for material {material.material_id}")
        if fraction != "0":
            parts.append(f"impervious={fraction}")
    return tuple(parts)


def effective_loss_signature(
    material: MaterialWithLoss,
    soil: SoilWithLoss,
) -> tuple[Literal["no-loss", "material-only", "soil-only", "coupled"], tuple[str, ...]]:
    """Normalize loss-relevant properties; ignore IDs and hydraulic roughness."""
    soil_part = _soil_signature(soil)
    mat_part = _material_signature(material, soil_active=bool(soil_part))
    kind: Literal["no-loss", "material-only", "soil-only", "coupled"]
    if mat_part and soil_part:
        kind = "coupled"
    elif mat_part:
        kind = "material-only"
    elif soil_part:
        kind = "soil-only"
    else:
        kind = "no-loss"
    return kind, ("material", *mat_part, "soil", *soil_part)


def group_observed_pairs(
    pairs: Sequence[ObservedPair],
    materials: Mapping[int, MaterialWithLoss],
    soils: Mapping[int, SoilWithLoss],
) -> tuple[EffectiveLossGroup, ...]:
    """Group only provided observed cells, collapsing provably neutral dimensions."""
    indexed: dict[tuple[str, ...], list[ObservedPair]] = defaultdict(list)
    types: dict[tuple[str, ...], Literal["no-loss", "material-only", "soil-only", "coupled"]] = {}
    for item in pairs:
        if item.cell_count <= 0:
            _fail("Observed pair cell_count must be strictly positive")
        material = materials.get(item.material_id)
        soil = soils.get(item.soil_id)
        if material is None or soil is None:
            _fail(f"Undefined active material/soil pair {item.material_id}/{item.soil_id}")
        category, key = effective_loss_signature(material, soil)
        indexed[key].append(item)
        types[key] = category
    groups: list[EffectiveLossGroup] = []
    for signature, members in sorted(indexed.items()):
        groups.append(
            EffectiveLossGroup(
                types[signature],
                signature,
                tuple(sorted({v.material_id for v in members})),
                tuple(sorted({v.soil_id for v in members})),
                tuple(sorted(members, key=lambda v: (v.material_id, v.soil_id))),
                sum(v.cell_count for v in members),
            )
        )
    return tuple(groups)


def observed_pairs_from_check_grids(
    material_grid: Path, soil_grid: Path, *, active_mask_grid: Path | None = None
) -> tuple[ObservedPair, ...]:
    """Stream aligned check-grid cell pairs, not GIS polygon combinations.

    The caller must verify the grids belong to the audited run; the function
    only proves aligned raster co-location. No implicit model/grid discovery.
    With an explicit active mask, a missing material/soil assignment inside an
    active cell is a fatal error rather than a silently excluded cell.
    """
    import numpy as np
    import rasterio

    with rasterio.open(material_grid) as mat, rasterio.open(soil_grid) as soil:
        with ExitStack() as stack:
            active = stack.enter_context(rasterio.open(active_mask_grid)) if active_mask_grid is not None else None
            datasets = [mat, soil]
            if active is not None:
                datasets.append(active)
            _check_raster_alignment(datasets)
            counts: Counter[tuple[int, int]] = Counter()
            for _, window in mat.block_windows(1):
                m = mat.read(1, window=window, masked=True)
                s = soil.read(1, window=window, masked=True)
                missing = np.ma.getmaskarray(m) | np.ma.getmaskarray(s)
                if active is not None:
                    a = active.read(1, window=window, masked=True)
                    selected = ~np.ma.getmaskarray(a) & (np.asarray(a.data) != 0)
                    if bool(np.any(selected & missing)):
                        _fail("Explicitly active cells have missing material/soil assignments")
                    valid = selected & ~missing
                else:
                    valid = ~missing
                _add_raster_pairs(counts, np.asarray(m.data)[valid], np.asarray(s.data)[valid])
    return tuple(ObservedPair(a, b, count) for (a, b), count in sorted(counts.items()))


def _check_raster_alignment(datasets: Sequence[object]) -> None:
    first = datasets[0]
    for dataset in datasets:
        if any(
            getattr(dataset, attribute) != getattr(first, attribute)
            for attribute in ("crs", "transform", "width", "height")
        ) or dataset.count != 1:
            _fail("Check grids must be aligned single-band rasters with identical CRS and transforms")
    if first.crs is None:
        _fail("Check grid CRS is unavailable")


def _add_raster_pairs(counts: Counter[tuple[int, int]], left: object, right: object) -> None:
    import numpy as np

    a, b = np.asarray(left), np.asarray(right)
    if not np.isfinite(a).all() or not np.isfinite(b).all():
        _fail("Active material/soil grids contain nonfinite IDs")
    if not np.equal(a, np.floor(a)).all() or not np.equal(b, np.floor(b)).all():
        _fail("Active material/soil IDs must be integers")
    if (a < 0).any() or (b < 0).any():
        _fail("Active material/soil IDs must be nonnegative")
    if a.size == 0:
        return
    combined = np.stack([a.astype("int64"), b.astype("int64")], axis=1)
    ids, frequency = np.unique(combined, axis=0, return_counts=True)
    for pair, count in zip(ids, frequency, strict=True):
        counts[int(pair[0]), int(pair[1])] += int(count)


def observed_pairs_from_grd_check(path: Path, *, layer: str | None = None) -> tuple[ObservedPair, ...]:
    """Read the actual TUFLOW ``_grd_check`` cell attributes, without GIS intersection.

    The TUFLOW check file contains one feature per grid cell, ``Material``,
    ``Code`` and (if soil infiltration was selected) ``Soil_ID``. Only cells
    with Code=1 are counted; Code=-1 is an inactive mesh cell. Without a
    Soil_ID column, a synthetic soil ID -1 denotes no *active* soil method.

    The caller is responsible for matching the check file to the run and
    verifying whether its domain/SGS/Quadtree representation is complete.
    """
    import fiona

    if path.suffix.casefold() not in {".gpkg", ".shp"}:
        _fail("A TUFLOW grd_check source must be a .gpkg or .shp GIS file")
    if path.suffix.casefold() == ".gpkg":
        layer = _grd_check_layer(path, layer, fiona.listlayers(path))

    with fiona.open(path, layer=layer) as source:
        fields = {name.casefold(): name for name in source.schema["properties"]}
        if "material" not in fields or "code" not in fields:
            message = f"TUFLOW grd_check requires Material and Code attributes; found {sorted(fields.values())}"
            _fail(message)
        soil_column = fields.get("soil_id")
        counts: Counter[tuple[int, int]] = Counter()
        for feature in source:
            properties = feature["properties"]
            code = _integer_id(properties[fields["code"]], "Code", path, nonnegative=False)
            if code == -1:
                continue
            if code != 1:
                _fail(f"Unsupported _grd_check cell Code {code} in {path}")
            material = _integer_id(properties[fields["material"]], "Material", path)
            soil = _integer_id(properties[soil_column], "Soil_ID", path) if soil_column is not None else -1
            counts[material, soil] += 1
    return tuple(ObservedPair(material, soil, count) for (material, soil), count in sorted(counts.items()))


def _grd_check_layer(path: Path, layer: str | None, names: Sequence[str]) -> str:
    if layer is not None:
        if layer not in names:
            _fail(f"Layer {layer!r} absent from {path}; available: {list(names)}")
        return layer
    candidates = [name for name in names if name.casefold().endswith("_grd_check")]
    if len(candidates) == 1:
        return candidates[0]
    if len(names) == 1:
        return names[0]
    _fail(f"Expected unique _grd_check layer in {path}; available: {list(names)}. Supply --grd-check-layer")


def _integer_id(raw: object, name: str, path: Path, *, nonnegative: bool = True) -> int:
    try:
        parsed = Decimal(str(raw))
    except InvalidOperation:
        _fail(f"Invalid {name} {raw!r} in {path}")
    if not parsed.is_finite() or parsed != parsed.to_integral_value():
        _fail(f"Noninteger {name} {raw!r} in {path}")
    value = int(parsed)
    if nonnegative and value < 0:
        _fail(f"Negative {name} {raw!r} in {path}")
    return value
