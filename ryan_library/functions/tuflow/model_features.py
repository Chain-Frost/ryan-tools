"""Conservative, read-only advisory for SGS and Quadtree TCF settings.

This is not TUFLOW's effective-input parser. Conditional commands, unresolved
includes, and calculated SGS sampling are explicitly reported as unknown or
solver-determined. The solver's TLF remains authoritative.
"""

from dataclasses import dataclass
from pathlib import Path

_UNKNOWN = "UNKNOWN"
_MAX_INCLUDE_DEPTH = 16
_MAX_FILE_BYTES = 4_000_000


@dataclass(frozen=True, slots=True)
class ModelFeatures:
    """Advisory settings from the TCF and its plain Read File includes."""

    sgs: str
    target_distance: str
    quadtree: str


@dataclass(slots=True)
class _Values:
    sgs: str | None = None
    distance: str | None = None
    frequency: str | None = None
    quadtree: str | None = None
    uncertain: bool = False

    def unreadable(self) -> None:
        """An unknown include may override any previously declared setting."""
        self.sgs = _UNKNOWN
        self.distance = _UNKNOWN
        self.frequency = _UNKNOWN
        self.quadtree = _UNKNOWN
        self.uncertain = True


def _visit(path: Path, values: _Values, *, stack: frozenset[Path], conditional: bool) -> None:
    """Process ordered TCF/Read File declarations without following other control files."""
    if path in stack or len(stack) >= _MAX_INCLUDE_DEPTH:
        values.unreadable()
        return
    try:
        if path.stat().st_size > _MAX_FILE_BYTES:
            values.unreadable()
            return
        content = path.read_text(encoding="utf-8-sig", errors="replace")
    except (OSError, UnicodeError):
        values.unreadable()
        return

    active_stack = stack | {path}
    depth = 0
    for raw in content.splitlines():
        statement = raw.partition("!")[0].strip()
        if "==" not in statement:
            # End If and Else also occur without an == separator.
            key = " ".join(statement.casefold().split())
            if key in {"end if", "endif"}:
                depth = max(depth - 1, 0)
            continue
        lhs, rhs = statement.split("==", 1)
        key = " ".join(lhs.casefold().split())
        value = rhs.strip().strip('"')
        if key in {"if scenario", "if event"}:
            depth += 1
            continue
        if key.startswith("else if"):
            continue
        uncertain_command = conditional or depth > 0
        if key == "read file":
            if not value or "<<" in value or ">>" in value or "|" in value:
                values.unreadable()
                continue
            included = Path(value.replace("\\", "/"))
            if not included.is_absolute():
                included = path.parent / included
            _visit(included.absolute(), values, stack=active_stack, conditional=uncertain_command)
        elif key == "sgs":
            values.sgs = _UNKNOWN if uncertain_command or value.casefold() not in {"on", "off"} else value.upper()
        elif key == "sgs sample target distance":
            values.distance = _UNKNOWN if uncertain_command or not value or "<<" in value else value
        elif key == "sgs sample frequency":
            values.frequency = _UNKNOWN if uncertain_command or not value or "<<" in value else value
        elif key == "quadtree control file":
            values.quadtree = _UNKNOWN if uncertain_command or not value or "<<" in value else value
    if depth:
        values.unreadable()


def inspect_model_features(tcf: Path) -> ModelFeatures:
    """Return non-blocking TCF declarations, never guessed solver-resolved settings.

    Reads only the TCF and its ordinary Read File includes; no model data,
    raster sampling, TUFLOW launch or filesystem mutation occurs.
    """
    values = _Values()
    _visit(tcf.absolute(), values, stack=frozenset(), conditional=False)

    if values.sgs in {"ON", "OFF"}:
        sgs = values.sgs
    else:
        sgs = "UNKNOWN (conditional, unavailable or not declared)"

    if values.sgs == "OFF":
        target = "N/A (SGS OFF)"
    elif values.frequency == _UNKNOWN or values.distance == _UNKNOWN:
        target = "UNKNOWN (conditional or unavailable)"
    elif values.frequency is not None:
        target = f"not applicable (SGS frequency {values.frequency} takes precedence)"
    elif values.distance is not None:
        target = f"{values.distance} (model distance units; explicit)"
    elif values.uncertain:
        target = "UNKNOWN (unresolved include)"
    else:
        target = "automatic / not specified (check solver TLF)"

    if values.quadtree == _UNKNOWN:
        quadtree = "UNKNOWN (conditional or unavailable)"
    elif values.quadtree is None:
        quadtree = "UNKNOWN (unresolved include)" if values.uncertain else "OFF (no QCF declared)"
    elif values.quadtree.casefold() == "single level":
        quadtree = "ON (Single Level)"
    else:
        quadtree = "ON (QCF declared)"
    return ModelFeatures(sgs, target, quadtree)
