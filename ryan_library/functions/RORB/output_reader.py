"""Section-aware reading of observed standard RORB output, without rewriting time or rainfall."""

import re

from .reader_types import Record, RorbDocument, Section, integer, number, validate_series
from .result_readers import read_sites, report_metadata


def _numeric_table(doc: RorbDocument, start: int, columns: list[str], name: str) -> Section:
    table = Section(name, ["line", *columns])
    doc.sections.append(table)
    for i in range(start, len(doc.lines)):
        line = doc.lines[i]
        tokens = line.split()
        if not tokens:
            if table.rows:
                break
            continue
        if tokens[0] in {"Tot.", "Total"} or tokens[0].startswith("Tot."):
            table.completeness = "terminated"
            break
        # A textual footer may end a table, but numeric-looking bad records must fail.
        if not re.match(r"^[\d+.*-]", tokens[0]) and table.rows:
            doc.issue("table_boundary", f"Unrecognized end of {name}; retained in raw text.", i + 1, error=True)
            table.valid = False
            break
        try:
            if len(tokens) != len(columns):
                msg = "Numeric record width differs from header."
                raise ValueError(msg)
            row: Record = {"line": i + 1, columns[0]: integer(tokens[0])}
            row.update({c: number(v) for c, v in zip(columns[1:], tokens[1:], strict=True)})
            table.rows.append(row)
        except ValueError as exc:
            table.valid = False
            doc.issue("output_record", f"{name}: {exc}", i + 1, error=True)
    return table


def read_output(doc: RorbDocument) -> None:  # noqa: C901 - preserve ordered format state and diagnostic scope
    """Read named hydrographs and supported rainfall tables; retain all other source lines."""
    report_metadata(doc)
    sites = read_sites(doc, "Site", "Hyd")
    messages = Section("reported_messages", ["line", "text"])
    doc.sections.append(messages)
    timestep: float | None = None
    expected_last: int | None = None
    hydrograph_count = 0
    rainfall_count = 0
    for i, line in enumerate(doc.lines):
        if re.search(r"\b(?:warning|error)\b", line, re.IGNORECASE):
            messages.rows.append({"line": i + 1, "text": line})
        match = re.fullmatch(r"\s*Time increment \(hours\)\s*=\s*(\S+)\s*", line)
        if match:
            try:
                value = number(match[1])
                if value <= 0 or (timestep is not None and timestep != value):
                    msg = "Invalid or conflicting time increment."
                    raise ValueError(msg)
                timestep = value
                doc.metadata["time_increment_hours"] = timestep
            except ValueError as exc:
                doc.issue("time_increment", str(exc), i + 1, error=True)
        match = re.fullmatch(r"\s*Duration of calculations:\s*(\d+)\s*", line)
        if match:
            value = int(match[1])
            if expected_last is not None and value != expected_last:
                doc.issue("calculation_duration", "Conflicting calculation durations.", i + 1, error=True)
            expected_last = value
        tokens = line.split()
        if tokens[:2] == ["Inc", "Time"]:
            hydrograph_count += 1
            if (
                len(tokens) < 3
                or len(set(tokens)) != len(tokens)
                or any(re.fullmatch(r"Hyd\d+", t) is None for t in tokens[2:])
            ):
                doc.issue("output_header", "Unsupported hydrograph summary header.", i + 1, error=True)
                continue
            table = _numeric_table(doc, i + 1, tokens, f"hydrographs_{hydrograph_count}")
            validate_series(doc, table, "Time")
            normalized = {f"Hyd{int(t[3:]):04d}" for t in tokens[2:]}
            if normalized != {r["id"] for r in sites.rows}:
                doc.issue("site_mapping", "Hydrograph IDs lack an unambiguous site mapping.", i + 1, error=True)
                table.valid = False
            if expected_last is not None and table.rows:
                if (
                    table.rows[0]["Inc"] != 0
                    or table.rows[-1]["Inc"] != expected_last
                    or len(table.rows) != expected_last + 1
                ):
                    doc.issue(
                        "incomplete_hydrograph",
                        "Rows do not cover the reported calculation increments.",
                        i + 1,
                        error=True,
                    )
                    table.valid = False
                else:
                    table.completeness = "reported_increment_count"
            else:
                doc.issue("completion_unverified", "No calculation count to verify hydrograph completion.", i + 1)
        if line.strip().startswith(("Rainfall, mm, in time inc.", "Rainfall-excess, mm, in time inc.")):
            rainfall_count += 1
            candidates = [
                (j, doc.lines[j].split())
                for j in range(i + 1, min(i + 7, len(doc.lines)))
                if doc.lines[j].split()[:2] == ["Incs", "ment"]
            ]
            if len(candidates) == 1:
                j, labels = candidates[0]
                areas = labels[2:]
                if areas and all(re.fullmatch(r"[A-Za-z]+", a) for a in areas) and len(set(areas)) == len(areas):
                    columns = ["Inc", "Catchment_mm", *[f"Subarea_{a}_mm" for a in areas]]
                    table = _numeric_table(doc, j + 1, columns, f"rainfall_{rainfall_count}")
                    if table.rows:
                        validate_series(doc, table, "Inc")
                    doc.metadata[f"rainfall_{rainfall_count}_meaning"] = line.strip()
                    continue
            doc.issue("unsupported_rainfall", "Rainfall header layout unsupported; raw block preserved.", i + 1)
    if hydrograph_count == 0:
        doc.issue(
            "no_hydrograph_summary", "No supported numeric hydrograph summary; output detail or run mode may differ."
        )
    if "program_version" not in doc.metadata or (
        hydrograph_count == 0 and "Vector file" not in doc.metadata and "Storm file" not in doc.metadata
    ):
        doc.issue(
            "output_structure",
            "Output signature lacks corroborating version and report sections.",
            error=True,
        )
    if hydrograph_count > 1:
        doc.issue(
            "multiple_summaries",
            "Multiple summaries retained separately; site/parameter scope requires review.",
            error=True,
        )
    doc.metadata["hydrograph_time_unit"] = "hours"
    doc.issue(
        "uninterpreted_sections",
        "Routing instructions, other input/output sections and unsupported tables remain in raw text.",
    )
