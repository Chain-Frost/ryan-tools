"""Source-preserving result types and strict primitives for read-only RORB inspection."""

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

type Value = str | int | float | bool | None
type Record = dict[str, Value]
NUMBER = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[EeDd][-+]?\d+)?")


def number(token: str) -> float:
    """Read a complete finite numeric token, including Fortran D exponents."""
    if NUMBER.fullmatch(token) is None:
        msg = f"Not a finite numeric token: {token!r}"
        raise ValueError(msg)
    result = float(token.replace("D", "E").replace("d", "e"))
    if not math.isfinite(result):
        msg = f"Non-finite number: {token!r}"
        raise ValueError(msg)
    return result


def integer(token: str, minimum: int = 0) -> int:
    """Read a decimal integer with a lower bound."""
    if re.fullmatch(r"\d+", token) is None or int(token) < minimum:
        msg = f"Expected integer >= {minimum}: {token!r}"
        raise ValueError(msg)
    return int(token)


@dataclass(frozen=True)
class Diagnostic:
    """A machine-readable issue with source line provenance."""

    code: str
    message: str
    severity: Literal["warning", "error"] = "warning"
    line: int | None = None


@dataclass
class Section:
    """A named, independently validated table; rows include their source line."""

    name: str
    columns: list[str]
    rows: list[Record] = field(default_factory=list[Record])
    valid: bool = True
    completeness: str = "unverified"


@dataclass
class RorbDocument:
    """Parsed sections plus the original decoded lines, without inferred defaults."""

    path: Path
    kind: str
    encoding: str
    sha256: str
    lines: tuple[str, ...]
    metadata: Record = field(default_factory=dict[str, Value])
    sections: list[Section] = field(default_factory=list[Section])
    diagnostics: list[Diagnostic] = field(default_factory=list[Diagnostic])
    fully_interpreted: bool = False

    @property
    def status(self) -> str:
        """Separate unsupported, invalid, partial and fully interpreted documents."""
        if self.kind == "unsupported":
            return "unsupported"
        if any(d.severity == "error" for d in self.diagnostics):
            return "invalid"
        return "ok" if self.fully_interpreted else "partial"

    def section(self, name: str) -> Section:
        """Return a uniquely named section, raising if absent."""
        return next(s for s in self.sections if s.name == name)

    def issue(self, code: str, message: str, line: int | None = None, *, error: bool = False) -> None:
        """Record a diagnostic without logging or discarding source evidence."""
        self.diagnostics.append(Diagnostic(code, message, "error" if error else "warning", line))


def validate_series(document: RorbDocument, section: Section, time_column: str) -> None:
    """Require ordered time and contiguous integer increments; never drop bad rows."""
    if not section.rows:
        document.issue("empty_table", f"No records in {section.name}.", error=True)
        section.valid = False
        return
    previous_time: float | None = None
    previous_inc: int | None = None
    for row in section.rows:
        time = float(str(row[time_column]))
        inc = int(str(row["Inc"])) if "Inc" in row else None
        if (previous_time is not None and time <= previous_time) or (
            inc is not None and previous_inc is not None and inc != previous_inc + 1
        ):
            document.issue(
                "invalid_sequence",
                f"Unordered time or missing increment in {section.name}.",
                int(str(row["line"])),
                error=True,
            )
            section.valid = False
        previous_time, previous_inc = time, inc
