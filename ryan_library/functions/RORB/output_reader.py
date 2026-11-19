"""Section-aware reading of observed standard RORB output, without rewriting time or rainfall."""

import re
from dataclasses import dataclass
from decimal import Decimal

from .reader_types import Record, RorbDocument, Section, exact_number, integer, number, validate_series
from .result_readers import read_sites, report_metadata


@dataclass(frozen=True, slots=True)
class HydrographSiteColumn:
    """Unambiguous physical site bound to one *original* printed numeric column."""

    site_id: str
    column: str
    physical_name: str
    description: str
    site_record_line: int


def _hyd_ordinal(token: str) -> int:
    match = re.fullmatch(r"Hyd(\d+)", token)
    if match is None or int(match[1]) == 0:
        msg = f"Invalid printed hydrograph identifier {token!r}"
        raise ValueError(msg)
    return int(match[1])


def bind_output_site_columns(  # noqa: C901 -- strict physical identity and missing-data guards
    document: RorbDocument, *, table_name: str = "hydrographs_1"
) -> tuple[HydrographSiteColumn, ...]:
    """Match Hyd ordinals to named sites, never by order or guessed zero padding.

    This binding does not establish native executable identity or original
    catchment geometry. Printed source tokens remain unchanged.
    """
    # Scalar interpretation is independent of a complete numeric table and
    # physical-site legend. Preserve scalar-specific failures for consumers to
    # diagnose instead of hiding them behind an unrelated site-binding error.
    scalar_errors = {"reported_scalar_value", "reported_scalar_values", "reported_scalar_layout"}
    if document.kind != "standard_output" or any(
        issue.severity == "error" and issue.code not in scalar_errors for issue in document.diagnostics
    ):
        msg = "Only a structurally valid parsed standard output can bind physical sites"
        raise ValueError(msg)
    if document.metadata.get("hydrograph_summary_count") != 1:
        msg = "Exactly one hydrograph summary is required for physical binding"
        raise ValueError(msg)
    try:
        sites = document.section("sites")
        table = document.section(table_name)
    except StopIteration as exc:
        msg = "Missing physical site descriptions or hydrograph table"
        raise ValueError(msg) from exc
    if not sites.valid or not sites.rows or not table.valid or not table.rows:
        msg = "Incomplete or invalid physical sites or hydrograph samples"
        raise ValueError(msg)

    identified: dict[int, HydrographSiteColumn] = {}
    physical_names: set[str] = set()
    for site in sites.rows:
        site_id = site["id"]
        description = site["description"]
        name = site["name"]
        role = site["role"]
        if not isinstance(site_id, str) or not isinstance(description, str):
            msg = "Invalid physical site description"
            raise ValueError(msg)
        physical_name = name if role is not None else description
        if (
            not isinstance(physical_name, str)
            or not physical_name.strip()
            or re.fullmatch(r"(?:calculated|observed)?\s*hydrograph\s*[:,]?", physical_name, re.IGNORECASE)
        ):
            msg = "Native physical print location is unnamed"
            raise ValueError(msg)
        ordinal = _hyd_ordinal(site_id)
        if ordinal in identified or physical_name.casefold() in physical_names:
            msg = "Ambiguous physical site names or identifiers"
            raise ValueError(msg)
        identified[ordinal] = HydrographSiteColumn(site_id, "", physical_name, description, int(str(site["line"])))
        physical_names.add(physical_name.casefold())

    matched: list[HydrographSiteColumn] = []
    printed_ordinals: set[int] = set()
    for column in table.columns[3:]:
        ordinal = _hyd_ordinal(column)
        if ordinal in printed_ordinals:
            msg = "Duplicate hydrograph column ordinal with alternate zero padding"
            raise ValueError(msg)
        printed_ordinals.add(ordinal)
        site = identified.get(ordinal)
        if site is None:
            msg = f"Hydrograph column {column} has no matching physical site"
            raise ValueError(msg)
        matched.append(
            HydrographSiteColumn(site.site_id, column, site.physical_name, site.description, site.site_record_line)
        )
    if len(matched) != len(identified):
        msg = "Physical site list differs from the hydrograph table"
        raise ValueError(msg)
    return tuple(matched)


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
        if tokens[0] in {"Tot.", "Total", "End"} or tokens[0].startswith("Tot.") or line.strip().startswith("End of "):
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
            table.source_tokens.append(dict(zip(columns, tokens, strict=True)))
        except ValueError as exc:
            table.valid = False
            doc.issue("output_record", f"{name}: {exc}", i + 1, error=True)
    return table


def _source_decimal(token: str) -> Decimal:
    """Read an already-validated numeric source token without binary rounding."""
    return Decimal(token.replace("D", "E").replace("d", "e"))


def _printed_resolution(value: Decimal) -> Decimal:
    """Return the resolution implied by the source's printed decimal places."""
    return Decimal(1).scaleb(int(value.as_tuple().exponent))


def _validate_reported_time_increment(doc: RorbDocument, table: Section, step_token: str) -> None:
    """Check the reported step using exact source decimals and printed rounding bounds."""
    step = _source_decimal(step_token)
    step_resolution = _printed_resolution(step)
    for previous, current in zip(table.rows, table.rows[1:], strict=False):
        previous_line = int(str(previous["line"]))
        current_line = int(str(current["line"]))
        previous_time = _source_decimal(doc.lines[previous_line - 1].split()[1])
        current_time = _source_decimal(doc.lines[current_line - 1].split()[1])
        tolerance = min(
            step / 2,
            (_printed_resolution(previous_time) + _printed_resolution(current_time) + step_resolution) / 2,
        )
        observed = current_time - previous_time
        if abs(observed - step) > tolerance:
            doc.issue(
                "time_increment_mismatch",
                f"{table.name}: observed {observed:g} h between rows, expected {step:g} h.",
                current_line,
                error=True,
            )
            table.valid = False


def _reported_scalars(doc: RorbDocument) -> None:
    """Collect generic reported scalar lexemes, not interpreted print-site bindings."""
    section = Section("reported_scalars", ["line", "family", "unit", "token", "character", "ordinal"])
    doc.sections.append(section)
    families = ("Peak discharge,", "Time to peak,", "Volume,")
    for index, line in enumerate(doc.lines):
        if not line.lstrip().startswith(families):
            continue
        match = re.match(r"^\s*(Peak discharge|Time to peak|Volume),(\S+)\s+", line)
        if match is None:
            doc.issue("reported_scalar_layout", "Scalar unit/value layout is unsupported.", index + 1, error=True)
            section.valid = False
            continue
        family, unit = match[1], match[2]
        values = list(re.finditer(r"\S+", line[match.end() :]))
        if not values:
            doc.issue("reported_scalar_values", "No reported scalar values.", index + 1, error=True)
            section.valid = False
        for ordinal, item in enumerate(values, 1):
            token = item[0]
            try:
                exact_number(token)
            except ValueError as exc:
                # Keep the invalid original token: generic source evidence must
                # never silently lose a failed scalar row.
                doc.issue("reported_scalar_value", str(exc), index + 1, error=True)
                section.valid = False
            section.rows.append(
                {
                    "line": index + 1,
                    "family": family,
                    "unit": unit,
                    "token": token,
                    "character": match.end() + item.start() + 1,
                    "ordinal": ordinal,
                }
            )
            section.source_tokens.append({"value": token})
    section.completeness = "observed_scalar_lines"


def read_output(doc: RorbDocument) -> None:  # noqa: C901 - preserve ordered format state and diagnostic scope
    """Read named hydrographs and supported rainfall tables; retain all other source lines."""
    report_metadata(doc)
    _reported_scalars(doc)
    doc.metadata["hydrograph_summary_count"] = sum(line.strip() == "Hydrograph summary" for line in doc.lines)
    doc.metadata["engine_identity"] = "unverified_from_report_banner"
    sites = read_sites(doc, "Site", "Hyd")
    messages = Section("reported_messages", ["line", "text"])
    doc.sections.append(messages)
    timestep: float | None = None
    timestep_text: str | None = None
    expected_last: int | None = None
    hydrograph_count = 0
    hydrograph_summaries: list[Section] = []
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
                timestep_text = match[1]
                doc.metadata["time_increment_hours"] = timestep
                doc.metadata["time_increment_raw"] = match[1]
            except ValueError as exc:
                doc.issue("time_increment", str(exc), i + 1, error=True)
        match = re.fullmatch(r"\s*Duration of calculations:\s*(\d+)\s*", line)
        if match:
            value = int(match[1])
            if expected_last is not None and value != expected_last:
                doc.issue("calculation_duration", "Conflicting calculation durations.", i + 1, error=True)
            expected_last = value
            doc.metadata["reported_duration_increments"] = value
            doc.metadata["reported_duration_raw"] = match[1]
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
            hydrograph_summaries.append(table)
            validate_series(doc, table, "Time")
            ordinals = [int(token[3:]) for token in tokens[2:]]
            if 0 in ordinals or len(set(ordinals)) != len(ordinals):
                doc.issue(
                    "hydrograph_column_ambiguous",
                    "Repeated or zero Hyd ordinal with alternate zero padding.",
                    i + 1,
                    error=True,
                )
                table.valid = False
            normalized = {f"Hyd{value:04d}" for value in ordinals}
            if normalized != {r["id"] for r in sites.rows}:
                # A table's numeric validity is independent of physical
                # print-site interpretation. The latter remains unresolved.
                doc.issue(
                    "site_mapping",
                    "Printed Hyd labels lack a complete independently verified site mapping.",
                    i + 1,
                )
            if table.rows:
                # Observed RORBWin summaries use 0..N; retained CMD 6.44
                # summaries use 1..N+1. Neither source clock is shifted.
                origin = int(str(table.rows[0]["Inc"]))
                table.increment_origin = origin
                doc.metadata[f"{table.name}_increment_origin"] = origin
                if origin not in (0, 1):
                    doc.issue(
                        "increment_origin",
                        "Only characterised increment origins 0 and 1 are supported.",
                        i + 1,
                        error=True,
                    )
                    table.valid = False
            if expected_last is not None and table.rows:
                origin = table.increment_origin
                if (
                    origin is None
                    or origin not in (0, 1)
                    or table.rows[-1]["Inc"] != expected_last + origin
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
    if timestep_text is not None:
        for table in hydrograph_summaries:
            _validate_reported_time_increment(doc, table, timestep_text)
    if hydrograph_count == 0:
        doc.issue(
            "no_hydrograph_summary", "No supported numeric hydrograph summary; output detail or run mode may differ."
        )
    if hydrograph_count == 0 and "Vector file" not in doc.metadata and "Storm file" not in doc.metadata:
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
