"""Group only observed co-located TUFLOW material/soil loss configurations.

Input pairs must be derived from aligned active-cell check grids or another
explicitly verified co-location source. Never manufacture a Cartesian product
from database inventories. This is an input-configuration audit, not a
prediction of actual infiltration or material loss depth.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal, Protocol


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


class UnresolvedLossCombination(ValueError):
    """Spatial IDs or loss parameters cannot be interpreted without guessing."""


def _number(value: str | float | int | None) -> str:
    if value is None:
        raise UnresolvedLossCombination("Missing loss parameter")
    try:
        number = Decimal(str(value).strip())
    except InvalidOperation as exc:
        raise UnresolvedLossCombination(f"Invalid numeric loss parameter {value!r}") from exc
    if not number.is_finite():
        raise UnresolvedLossCombination(f"Nonfinite loss parameter {value!r}")
    if number.is_zero():
        return "0"
    return format(number.normalize(), "f")


def _soil_signature(soil: SoilWithLoss) -> tuple[str, ...]:
    method = soil.method.strip().upper()
    if soil.status != "defined":
        raise UnresolvedLossCombination(f"Soil {soil.soil_id} is not resolved")
    if method == "NONE":
        return ()
    values = {key.casefold(): val for key, val in soil.parameters}
    if method == "ILCL":
        il = _number(values.get("il"))
        cl = _number(values.get("cl"))
        if Decimal(il) < 0 or Decimal(cl) < 0:
            raise UnresolvedLossCombination(f"Negative ILCL losses in soil {soil.soil_id}")
        if il == "0" and cl == "0":
            return ()
    elif method not in {"GA", "HO", "CO", "SCS"}:
        raise UnresolvedLossCombination(f"Unsupported soil method {method!r} for ID {soil.soil_id}")
    # Preserve *all* supplied method-specific state/parameters for non-neutral
    # soils. Unrecognised blanks cannot be dismissed as a zero-loss method.
    details: list[str] = [f"method={method}"]
    for key, val in sorted(soil.parameters, key=lambda item: item[0].casefold()):
        if val:
            try:
                normal = _number(val)
            except UnresolvedLossCombination:
                normal = val.strip()
        else:
            normal = "<unspecified>"
        details.append(f"{key.casefold()}={normal}")
    return tuple(details)


def _material_signature(material: MaterialWithLoss, *, soil_active: bool) -> tuple[str, ...]:
    if material.status != "defined":
        raise UnresolvedLossCombination(f"Material {material.material_id} is not resolved")
    il = _number(material.initial_loss_mm)
    cl = _number(material.continuing_loss_mm_h)
    if Decimal(il) < 0 or Decimal(cl) < 0:
        raise UnresolvedLossCombination(f"Negative material loss for ID {material.material_id}")
    parts: list[str] = []
    if il != "0" or cl != "0":
        parts.extend([f"il={il}", f"cl={cl}"])
    if soil_active:
        # A material without rainfall IL/CL can still change infiltration by
        # its impervious fraction, so the fraction cannot be ignored.
        if material.fraction_impervious is None:
            raise UnresolvedLossCombination(f"Material {material.material_id} has unspecified impervious fraction")
        fraction = _number(material.fraction_impervious)
        if Decimal(fraction) < 0 or Decimal(fraction) > 1:
            raise UnresolvedLossCombination(f"Invalid impervious fraction for material {material.material_id}")
        if fraction != "0":
            parts.append(f"impervious={fraction}")
    return tuple(parts)


def effective_loss_signature(
    material: MaterialWithLoss, soil: SoilWithLoss
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
            raise ValueError("Observed pair cell_count must be strictly positive")
        material = materials.get(item.material_id)
        soil = soils.get(item.soil_id)
        if material is None or soil is None:
            raise UnresolvedLossCombination(f"Undefined active material/soil pair {item.material_id}/{item.soil_id}")
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
    Pixels with any invalid mask or optional active_mask==0 are excluded.
    """
    import numpy as np
    import rasterio

    with rasterio.open(material_grid) as mat, rasterio.open(soil_grid) as soil:
        from contextlib import ExitStack

        with ExitStack() as stack:
            active = stack.enter_context(rasterio.open(active_mask_grid)) if active_mask_grid is not None else None
            datasets = [mat, soil] + ([active] if active is not None else [])
            if any(
                data.count != 1 or data.crs != mat.crs or data.transform != mat.transform or
                data.width != mat.width or data.height != mat.height
                for data in datasets
            ):
                raise ValueError("Check grids must be aligned single-band rasters with identical CRS and transforms")
            if mat.crs is None:
                raise ValueError("Check grid CRS is unavailable")
            counts: Counter[tuple[int, int]] = Counter()
            for _, window in mat.block_windows(1):
                m = mat.read(1, window=window, masked=True)
                s = soil.read(1, window=window, masked=True)
                valid = ~np.ma.getmaskarray(m) & ~np.ma.getmaskarray(s)
                if active is not None:
                    a = active.read(1, window=window, masked=True)
                    valid &= ~np.ma.getmaskarray(a) & (np.asarray(a.data) != 0)
                left = np.asarray(m.data)[valid]
                right = np.asarray(s.data)[valid]
                if len(left) == 0:
                    continue
                if not np.isfinite(left).all() or not np.isfinite(right).all():
                    raise ValueError("Active material/soil grids contain nonfinite IDs")
                if not np.equal(left, np.floor(left)).all() or not np.equal(right, np.floor(right)).all():
                    raise ValueError("Active material/soil IDs must be integers")
                if (left < 0).any() or (right < 0).any():
                    raise ValueError("Active material/soil IDs must be nonnegative")
                ids, frequency = np.unique(np.stack([left.astype('int64'), right.astype('int64')], axis=1), axis=0, return_counts=True)
                for pair, count in zip(ids, frequency, strict=True):
                    counts[int(pair[0]), int(pair[1])] += int(count)
    return tuple(ObservedPair(a, b, count) for (a, b), count in sorted(counts.items()))
