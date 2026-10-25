"""Bounded CSV header inspection without parsing time-series data rows."""

import csv
from collections import Counter
from collections.abc import Collection, Iterator
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


def read_csv_header(path: Path, *, required_columns: Collection[str] = (), maximum_bytes: int = 1048576) -> CsvHeader:
    """Find a header within five physical lines, including comments/preamble.

    With selectors, stop at the first CSV record containing any requested name.
    Missing names are diagnosed against that record, never sought in later rows.
    Without selectors, return the first nonblank/noncomment record. No numeric
    rainfall values are converted or validated. Reads stop at the header or limit.
    """
    if maximum_bytes < 1:
        message = "maximum_bytes must be positive"
        raise ValueError(message)
    requested = {name.strip().casefold() for name in required_columns if name.strip()}
    try:
        with path.open("rb") as stream:
            record_started = False

            def lines() -> Iterator[str]:
                nonlocal record_started
                remaining = maximum_bytes
                for line_number in range(5):
                    line = stream.readline(remaining + 1)
                    if len(line) > remaining:
                        message = "CSV header exceeds the bounded byte limit"
                        raise ValueError(message)
                    if not line:
                        return
                    remaining -= len(line)
                    text = line.decode("utf-8-sig" if line_number == 0 else "utf-8")
                    if not record_started and (not text.strip() or text.lstrip().startswith("!")):
                        continue
                    record_started = True
                    yield text

            for row in csv.reader(lines(), strict=True):
                columns = tuple(name.strip() for name in row)
                if any(columns) and (not requested or requested.intersection(name.casefold() for name in columns)):
                    return CsvHeader(columns)
                record_started = False
            return CsvHeader(error="No CSV header containing a requested column within the first 5 physical lines")
    except (OSError, UnicodeError, csv.Error, ValueError) as exc:
        return CsvHeader(error=f"Cannot inspect CSV header: {exc}")
