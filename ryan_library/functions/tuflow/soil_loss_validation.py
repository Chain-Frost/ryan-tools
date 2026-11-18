"""Conservative validation of PyTUFLOW soil-loss parameters for input auditing.

Do not infer a valid infiltration configuration from a method name alone.
Missing, nonfinite, or unsupported parameters have to remain unresolved.  This
helper evaluates *input definitions*, not realized infiltration during a run.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation

_COMMON = frozenset(
    {
        "porosity", "initial moisture", "max ponding depth", "horizontal conductivity",
        "residual water content", "saturated water content", "alpha", "n", "l",
    }
)
_REQUIRED: dict[str, frozenset[str]] = {
    "ILCL": frozenset({"il", "cl"}),
    "HO": frozenset({"il", "il rate", "final loss rate", "ex decay rate"}),
    "CO": frozenset({"hydraulic conductivity", "porosity"}),
    "SCS": frozenset({"curve number", "initial abstraction ratio"}),
}
_ALLOWED: dict[str, frozenset[str]] = {
    "ILCL": _REQUIRED["ILCL"] | _COMMON,
    "GA": frozenset({"usda soil type", "suction", "hydraulic conductivity"}) | _COMMON,
    "HO": _REQUIRED["HO"] | _COMMON,
    "CO": _REQUIRED["CO"] | _COMMON,
    "SCS": _REQUIRED["SCS"] | _COMMON,
    "NONE": frozenset(),
}
_FRACTIONS = frozenset({"porosity", "initial moisture", "residual water content", "saturated water content", "initial abstraction ratio"})


def _normalize_value(method: str, key: str, raw: object) -> str:
    if key not in _ALLOWED[method]:
        message = f"Unexpected parameter {key!r} for soil method {method}"
        raise ValueError(message)
    if key == "usda soil type":
        if not isinstance(raw, str) or not raw.strip().strip('"\''):
            message = "USDA Soil Type must be a nonempty named type"
            raise ValueError(message)
        return raw.strip().strip('"\'').upper()
    try:
        number = Decimal(str(raw).strip())
    except InvalidOperation as exc:
        message = f"Soil {method} parameter {key!r} is not numeric: {raw!r}"
        raise ValueError(message) from exc
    if not number.is_finite():
        message = f"Soil {method} parameter {key!r} is not finite"
        raise ValueError(message)
    if number < 0:
        message = f"Soil {method} parameter {key!r} must be nonnegative"
        raise ValueError(message)
    if key in _FRACTIONS and number > 1:
        message = f"Soil {method} parameter {key!r} must not exceed 1"
        raise ValueError(message)
    if key == "curve number" and not (0 < number <= 100):
        message = "SCS Curve Number must be in (0, 100]"
        raise ValueError(message)
    if key == "porosity" and number == 0:
        message = "Soil porosity must be positive"
        raise ValueError(message)
    return "0" if number == 0 else format(number.normalize(), "f")


def _validate_required(method: str, normal: Mapping[str, str]) -> None:
    if method == "NONE":
        if normal:
            message = "NONE soil must not have infiltration parameters"
            raise ValueError(message)
        return
    if method == "GA":
        has_usda = "usda soil type" in normal
        numeric = {"suction", "hydraulic conductivity", "porosity"}
        if has_usda and numeric.intersection(normal):
            message = "GA soil mixes USDA and numeric soil definitions"
            raise ValueError(message)
        if not has_usda and not numeric.issubset(normal):
            message = "GA requires USDA Soil Type or suction, conductivity and porosity"
            raise ValueError(message)
        return
    missing = _REQUIRED[method].difference(normal)
    if missing:
        message = f"{method} soil is missing required parameters: {', '.join(sorted(missing))}"
        raise ValueError(message)


def validated_soil_parameters(method: str, parameters: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
    """Return a canonical, finite signature, or reject uncertain soil parameters.

    Recognises the method vocabulary returned by PyTUFLOW SoilDatabase.value.
    Optional null values are omitted, not silently treated as zero.  Unknown
    populated keys, missing mandatory keys, and mixed GA input modes fail.
    """
    method = method.strip().upper()
    if method not in _ALLOWED:
        message = f"Unsupported soil loss method: {method or '<missing>'}"
        raise ValueError(message)
    params = {str(key).strip().casefold(): raw for key, raw in parameters.items() if str(key).casefold() != "method"}
    if len(params) != len(parameters) - int(any(str(key).casefold() == "method" for key in parameters)):
        message = "Duplicate soil parameter names"
        raise ValueError(message)
    normal: dict[str, str] = {}
    for key, raw in params.items():
        absent = raw is None or (isinstance(raw, str) and not raw.strip())
        if absent:
            continue
        normal[key] = _normalize_value(method, key, raw)
    _validate_required(method, normal)
    return tuple(sorted(normal.items()))
