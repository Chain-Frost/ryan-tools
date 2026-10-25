"""Bounded CSV header inspection without parsing time-series data rows."""

import csv
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class CsvHeader:
    """One immutable header snapshot, or an inspection error."""

    columns: tuple[str, ...] = ()
    error: str = ""

    def problem(self, required: tuple[str, ...] | None) -> str:
        """Describe missing/ambiguous named selectors, preserving their spelling."""
        if required is None:
            return "Parser does not expose BC CSV column requirements; install the matching PyTUFLOW build"
        if len(required) != 2 or any(not name.strip() or "<<" in name for name in required):
            return "BC CSV requires two resolved named columns; blank selectors or this source format are unsupported"
        if self.error:
            return self.error
        counts = Counter(name.strip().casefold() for name in self.columns)
        missing = [name for name in dict.fromkeys(required) if not counts[name.strip().casefold()]]
        ambiguous = [name for name in dict.fromkeys(required) if counts[name.strip().casefold()] > 1]
        problems: list[str] = []
        if missing:
            problems.append("CSV header is missing required column(s): " + ", ".join(repr(name) for name in missing))
        if ambiguous:
            problems.append(
                "CSV header has ambiguous duplicate column(s): " + ", ".join(repr(name) for name in ambiguous)
            )
        return "; ".join(problems)


def read_csv_header(path: Path, *, maximum_bytes: int = 1048576) -> CsvHeader:
    """Read the first nonblank/noncomment CSV record, bounded including preamble.

    Accept UTF-8 (with optional BOM), quoted fields and multiline headers. Stop
    after that record; never search data rows for a desired column name. Leading
    blank lines and TUFLOW ! comments are allowed. Other preambles fail coverage.
    """
    if maximum_bytes < 1:
        message = "maximum_bytes must be positive"
        raise ValueError(message)
    try:
        with path.open("rb") as stream:

            def lines() -> Iterator[str]:
                remaining = maximum_bytes
                first = True
                started = False
                while True:
                    line = stream.readline(remaining + 1)
                    if len(line) > remaining:
                        message = "CSV header exceeds the bounded byte limit"
                        raise ValueError(message)
                    if not line:
                        return
                    remaining -= len(line)
                    text = line.decode("utf-8-sig" if first else "utf-8")
                    first = False
                    if not started and (not text.strip() or text.lstrip().startswith("!")):
                        continue
                    started = True
                    yield text

            row = next(csv.reader(lines(), strict=True), None)
            if not row or not any(name.strip() for name in row):
                return CsvHeader(error="CSV has no header record")
            return CsvHeader(tuple(name.strip() for name in row))
    except (OSError, UnicodeError, csv.Error, ValueError) as exc:
        return CsvHeader(error=f"Cannot inspect CSV header: {exc}")
