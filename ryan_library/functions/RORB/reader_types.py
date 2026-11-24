"""Source-preserving result types and strict primitives for read-only RORB inspection."""

import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Literal, TypeAlias

Value: TypeAlias = str | int | float | bool | None
Record: TypeAlias = dict[str, Value]
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


def exact_number(token: str) -> Decimal:
    """Parse a complete finite source token without binary rounding or default values."""
    if NUMBER.fullmatch(token.strip()) is None:
        msg = f"Not a finite numeric token: {token!r}"
        raise ValueError(msg)
    result = Decimal(token.strip().replace("D", "E").replace("d", "e"))
    if not result.is_finite():
        msg = f"Non-finite number: {token!r}"
        raise ValueError(msg)
    return result


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
    # Index-aligned with rows. Original text (including exponents and padding)
    # is authoritative; rounded floats in rows are convenience views only.
    source_tokens: list[dict[str, str]] = field(default_factory=list[dict[str, str]])
    increment_origin: int | None = None

    def source_decimal(self, row_index: int, column: str) -> Decimal:
        """Return a finite exact value from the original printed column token."""
        return exact_number(self.source_tokens[row_index][column])


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
    # This is not placed in metadata or JSON previews. Preserve CRLF/BOM/encoding
    # and original bytes for independently validated file/in-memory callers.
    original_bytes: bytes | None = field(default=None, repr=False)

    @property
    def size_bytes(self) -> int | None:
        """The exact retained source byte count, if available."""
        return None if self.original_bytes is None else len(self.original_bytes)

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
    previous_time: Decimal | None = None
    previous_inc: int | None = None
    for row_index, row in enumerate(section.rows):
        time = (
            section.source_decimal(row_index, time_column) if section.source_tokens else Decimal(str(row[time_column]))
        )
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
