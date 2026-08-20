"""Select multi-pipe culvert outlet rock protection from inspectable data."""

# started, not tested or finished


from csv import DictReader
from dataclasses import dataclass
from functools import cache
from importlib.resources import files
from importlib.resources.abc import Traversable
from math import isfinite

_VELOCITY_BANDS_FILE = "agrd05b_figure_3_17.csv"
_ROCK_CLASSES_FILE = "rock_classes.csv"


@dataclass(frozen=True, slots=True)
class MultiPipeOutletRockProtection:
    """Rock-protection properties selected for a multi-pipe culvert outlet."""

    rock_class: str
    nominal_d50_mm: int
    rock_thickness_m: float
    rounded_rock_d50_mm: int
    rock_d50_mm: int
    apron_length_diameters: int


@dataclass(frozen=True, slots=True)
class _VelocityBand:
    culvert_diameter_m: float
    velocity_lower_exclusive_m_per_s: float
    velocity_upper_inclusive_m_per_s: float
    nominal_d50_mm: int
    apron_length_diameters: int


@dataclass(frozen=True, slots=True)
class _RockClass:
    name: str
    thickness_m: float
    rounded_d50_mm: int
    d50_mm: int


def _data_file(filename: str) -> Traversable:
    return files("ryan_library.functions").joinpath("data", filename)


def _required(row: dict[str, str | None], column: str, filename: str) -> str:
    value = row.get(column)
    if value is None or not value.strip():
        raise ValueError(f"Missing {column!r} in {filename}")
    return value


@cache
def _velocity_bands() -> tuple[_VelocityBand, ...]:
    with _data_file(_VELOCITY_BANDS_FILE).open("r", encoding="utf-8") as csv_file:
        rows = tuple(DictReader(csv_file))

    return tuple(
        _VelocityBand(
            culvert_diameter_m=float(_required(row, "culvert_diameter_m", _VELOCITY_BANDS_FILE)),
            velocity_lower_exclusive_m_per_s=float(
                _required(row, "velocity_lower_exclusive_m_per_s", _VELOCITY_BANDS_FILE)
            ),
            velocity_upper_inclusive_m_per_s=float(
                _required(row, "velocity_upper_inclusive_m_per_s", _VELOCITY_BANDS_FILE)
            ),
            nominal_d50_mm=int(_required(row, "nominal_d50_mm", _VELOCITY_BANDS_FILE)),
            apron_length_diameters=int(_required(row, "apron_length_diameters", _VELOCITY_BANDS_FILE)),
        )
        for row in rows
    )


@cache
def _rock_classes() -> tuple[_RockClass, ...]:
    with _data_file(_ROCK_CLASSES_FILE).open("r", encoding="utf-8") as csv_file:
        rows = tuple(DictReader(csv_file))

    classes: list[_RockClass] = []
    for row in rows:
        rounded_d50_text = row.get("d50_rounded_mm")
        if rounded_d50_text is None or not rounded_d50_text.strip():
            continue
        classes.append(
            _RockClass(
                name=_required(row, "rock_class", _ROCK_CLASSES_FILE),
                thickness_m=float(_required(row, "thickness_m", _ROCK_CLASSES_FILE)),
                rounded_d50_mm=int(rounded_d50_text),
                d50_mm=int(_required(row, "d50_mm", _ROCK_CLASSES_FILE)),
            )
        )
    return tuple(classes)


def multi_pipe_outlet_rock_protection(
    culvert_diameter_m: float,
    outlet_velocity_m_per_s: float,
) -> MultiPipeOutletRockProtection:
    """Return multi-pipe outlet rock protection selected by the workbook lookup.

    ``culvert_diameter_m`` is the diameter of one pipe, not the combined width
    of the multi-pipe outlet. It must match one of the tabulated diameters.
    Velocity bands use ``lower < velocity <= upper``, matching the formula in
    cell X3 and its peers in ``260709_Skyway Options.xlsx``.

    This lookup must not be used for a single pipe in a bank. AGRD 05B (2023)
    provides a separate chart for that outlet configuration.

    Raises:
        ValueError: If an input is non-finite or has no tabulated result.
    """
    if not isfinite(culvert_diameter_m) or not isfinite(outlet_velocity_m_per_s):
        raise ValueError("Culvert diameter and outlet velocity must be finite")

    matching_band = next(
        (
            band
            for band in _velocity_bands()
            if band.culvert_diameter_m == culvert_diameter_m
            and band.velocity_lower_exclusive_m_per_s < outlet_velocity_m_per_s <= band.velocity_upper_inclusive_m_per_s
        ),
        None,
    )
    if matching_band is None:
        supported_diameters = sorted({band.culvert_diameter_m for band in _velocity_bands()})
        raise ValueError(
            f"No outlet rock-protection band for diameter {culvert_diameter_m:g} m and "
            f"velocity {outlet_velocity_m_per_s:g} m/s; supported diameters are {supported_diameters}"
        )

    rock_class = next(
        (candidate for candidate in _rock_classes() if candidate.rounded_d50_mm >= matching_band.nominal_d50_mm),
        None,
    )
    if rock_class is None:
        raise ValueError(f"No rock class for nominal d50 {matching_band.nominal_d50_mm} mm")

    return MultiPipeOutletRockProtection(
        rock_class=rock_class.name,
        nominal_d50_mm=matching_band.nominal_d50_mm,
        rock_thickness_m=rock_class.thickness_m,
        rounded_rock_d50_mm=rock_class.rounded_d50_mm,
        rock_d50_mm=rock_class.d50_mm,
        apron_length_diameters=matching_band.apron_length_diameters,
    )


def multi_pipe_outlet_rock_class(culvert_diameter_m: float, outlet_velocity_m_per_s: float) -> str:
    """Return only the workbook-equivalent multi-pipe rock class (column X)."""
    return multi_pipe_outlet_rock_protection(culvert_diameter_m, outlet_velocity_m_per_s).rock_class
