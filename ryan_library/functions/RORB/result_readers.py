"""Strict section readers for observed RORBWin batch and hydrograph CSV layouts."""

import csv
import re

from .reader_types import Record, RorbDocument, Section, integer, number, validate_series


def report_metadata(doc: RorbDocument) -> None:  # noqa: C901 - preserve ordered format state and diagnostic scope
    """Retain labelled report metadata and each parameter occurrence with provenance."""
    parameters = Section("parameters", ["line", "kc", "m", "IL", "CL", "ROC", "loss_model"])
    doc.sections.append(parameters)
    for i, line in enumerate(doc.lines):
        stripped = line.strip()
        # Version text is metadata, not a claim that every section is supported.
        if stripped.startswith("Program version "):
            doc.metadata["program_version"] = stripped.split()[2]
        if ":" in line and not stripped.startswith(("Parameters:", "Loss parameters")):
            key, value = line.split(":", 1)
            if key.strip() in {
                "Date run",
                "Catchment file",
                "Vector file",
                "Storm file",
                "Rainfall location",
                "Temporal pattern",
                "Spatial pattern",
                "Areal Red. Fact.",
                "Loss factors",
                "Output information",
            }:
                doc.metadata[key.strip()] = value.strip()
                doc.metadata[key.strip() + "_line"] = i + 1
        if stripped.startswith("Parameters:"):
            match = re.fullmatch(r"Parameters:\s+kc\s*=\s*(\S+)\s+m\s*=\s*(\S+)", stripped)
            try:
                if match is None:
                    msg = "Unsupported routing-parameter layout."
                    raise ValueError(msg)
                parameters.rows.append({"line": i + 1, "kc": number(match[1]), "m": number(match[2])})
            except ValueError as exc:
                parameters.valid = False
                doc.issue("routing_parameters", str(exc), i + 1, error=True)
        if stripped.startswith("Loss parameters"):
            model = (
                "CL"
                if "Cont. loss (mm/h)" in line
                else "ROC" if re.search(r"runoff coeff", line, re.IGNORECASE) else None
            )
            if model is None or "Initial loss (mm)" not in line:
                doc.issue("unsupported_losses", "Loss layout retained as raw text; no values inferred.", i + 1)
                continue
            j = i + 1
            while j < len(doc.lines) and not doc.lines[j].strip():
                j += 1
            try:
                values = doc.lines[j].split() if j < len(doc.lines) else []
                if len(values) != 2:
                    msg = "Expected two loss values."
                    raise ValueError(msg)
                parameters.rows.append(
                    {"line": j + 1, "IL": number(values[0]), model: number(values[1]), "loss_model": "IL/" + model}
                )
            except ValueError as exc:
                parameters.valid = False
                doc.issue("loss_parameters", str(exc), j + 1, error=True)
    if "program_version" not in doc.metadata:
        doc.issue("missing_version", "Report has no program-version record.")


def read_sites(doc: RorbDocument, marker: str, prefix: str) -> Section:
    """Read explicit site descriptions, preserving role and unnamed sites."""
    section = Section("sites", ["line", "id", "description", "role", "name"])
    doc.sections.append(section)
    active = False
    seen: set[str] = set()
    for i, line in enumerate(doc.lines):
        if line.split() == [marker, "Description"]:
            active = True
            continue
        if not active:
            continue
        if not line.strip():
            active = False
            continue
        match = re.fullmatch(r"\s*(\d+)\s+(.+?)\s*", line)
        if match is None:
            active = False
            doc.issue("site_record", "Unrecognized site-description record.", i + 1, error=True)
            section.valid = False
            continue
        identifier = f"{prefix}{int(match[1]):04d}"
        description = match[2]
        role_match = re.fullmatch(r"(.+?) hydrograph[:,]\s*(.*)", description, re.IGNORECASE)
        role, name = (role_match[1], role_match[2]) if role_match else (None, None)
        if identifier in seen:
            doc.issue(
                "duplicate_site", f"Repeated site {identifier}; multiple blocks are not conflated.", i + 1, error=True
            )
            section.valid = False
        seen.add(identifier)
        section.rows.append({"line": i + 1, "id": identifier, "description": description, "role": role, "name": name})
    return section


def read_batch(doc: RorbDocument) -> None:  # noqa: C901 - preserve ordered format state and diagnostic scope
    """Parse run rows with explicit frequency kinds, durations and table boundaries."""
    report_metadata(doc)
    sites = read_sites(doc, "Peak", "Peak")
    runs = Section("runs", [])
    representatives = Section("representatives", ["line", "Run", "reference"])
    doc.sections.extend([runs, representatives])
    header: list[str] = []
    active = False
    expect_representative = False
    ended = False
    keys: set[tuple[int, str, float, int]] = set()
    for i, line in enumerate(doc.lines):
        tokens = line.split()
        if not tokens:
            continue
        if tokens[:2] == ["Run", "Duration"]:
            if header:
                doc.issue("repeated_header", "Multiple run tables are not safely supported.", i + 1, error=True)
                runs.valid = False
            header = [s for s in tokens if s != "Unit"]
            required = header[:4] == ["Run", "Duration", "AEP", "TPat"]
            if not required or len(set(header)) != len(header) or not any(s.startswith("Peak") for s in header):
                doc.issue("run_header", "Unsupported run-table columns.", i + 1, error=True)
                runs.valid = False
            runs.columns = ["line", *header, "duration_raw", "duration_unit", "aep_raw", "aep_kind"]
            active = True
            continue
        if re.fullmatch(r"\s*Run,\s+Representative hydrograph\s*", line):
            if expect_representative:
                doc.issue("representative_missing", "Missing representative record.", i + 1, error=True)
                representatives.valid = False
            active, ended, expect_representative = False, True, True
            continue
        if line.strip().startswith("Elapsed Run Time"):
            doc.metadata["elapsed_time_raw"] = line.strip()
            active, ended = False, True
            continue
        if expect_representative:
            match = re.fullmatch(r"\s*(\d+)\s+(.+\.out)\s*", line, re.IGNORECASE)
            if match:
                representatives.rows.append({"line": i + 1, "Run": int(match[1]), "reference": match[2].rstrip()})
            else:
                doc.issue("representative_record", "Malformed representative record.", i + 1, error=True)
                representatives.valid = False
            expect_representative = False
            continue
        if not active:
            continue
        try:
            if len(tokens) != len(header) + 1:
                msg = "Run row width differs from header."
                raise ValueError(msg)
            run, tp = integer(tokens[0], 1), integer(tokens[4], 1)
            duration = number(tokens[1])
            unit = tokens[2].casefold()
            if unit not in {"hour", "minute"} or duration <= 0:
                msg = "Duration must be positive with hour or minute units."
                raise ValueError(msg)
            duration_hours = duration if unit == "hour" else duration / 60
            freq = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(%|EY)", tokens[3])
            if freq is None or number(freq[1]) <= 0 or (freq[2] == "%" and number(freq[1]) > 100):
                msg = "Unsupported or invalid frequency label."
                raise ValueError(msg)
            key = (run, tokens[3], duration_hours, tp)
            if key in keys:
                msg = "Duplicate run/duration/frequency/pattern record."
                raise ValueError(msg)
            keys.add(key)
            row: Record = {
                "line": i + 1,
                "Run": run,
                "Duration": duration_hours,
                "AEP": tokens[3].removesuffix("%"),
                "TPat": tp,
                "duration_raw": tokens[1],
                "duration_unit": tokens[2],
                "aep_raw": tokens[3],
                "aep_kind": "percent" if freq[2] == "%" else "EY",
            }
            for column, token in zip(header[4:], tokens[5:], strict=True):
                row[column] = number(token)
            runs.rows.append(row)
        except ValueError as exc:
            doc.issue("run_record", str(exc), i + 1, error=True)
            runs.valid = False
    if not runs.rows:
        doc.issue("missing_runs", "No supported run records.", error=True)
        runs.valid = False
    if not ended:
        doc.issue("incomplete_runs", "No recognized run-table terminator; file may be truncated.", error=True)
        runs.valid = False
    if expect_representative:
        doc.issue("representative_missing", "File ends before representative record.", error=True)
        representatives.valid = False
    if "elapsed_time_raw" not in doc.metadata:
        doc.issue("completion_unverified", "Elapsed-time footer absent; whole-report completion is unverified.")
    runs.completeness = "terminated" if ended else "unverified"
    site_ids = {r["id"] for r in sites.rows}
    if {h for h in header if h.startswith("Peak")} != site_ids:
        doc.issue("site_mapping", "Run peak columns do not match the site-description table.", error=True)
        runs.valid = False


def read_csv(doc: RorbDocument) -> None:
    """Read a signed CSV, retaining increment, source report and unmodified time axis."""
    if len(doc.lines) < 3 or not doc.lines[1].startswith(
        "Refer output file for complete listing of inputs and outputs"
    ):
        doc.issue("csv_preamble", "Missing RORB CSV reference preamble.", 2, error=True)
        return
    preamble = doc.lines[1]
    marker = "inputs and outputs:"
    if marker in preamble:
        doc.metadata["output_reference"] = preamble.split(marker, 1)[1].removeprefix(" ").rstrip()
    section = Section("hydrographs", [])
    doc.sections.append(section)
    reader = csv.reader(doc.lines[2:], strict=True)
    try:
        columns = [s.strip() for s in next(reader)]
        section.columns = ["line", *columns]
        if columns[:2] != ["Inc", "Time (hrs)"] or len(columns) < 3 or len(set(columns)) != len(columns):
            msg = "Expected unique Inc, Time (hrs), and hydrograph columns."
            raise ValueError(msg)
        if any(re.fullmatch(r".+ hydrograph:\s*.*", c, re.IGNORECASE) is None for c in columns[2:]):
            msg = "Unrecognized hydrograph column role."
            raise ValueError(msg)
        for values in reader:
            line_number = reader.line_num + 2
            if not values:
                continue
            try:
                if len(values) != len(columns):
                    msg = "CSV row width differs from header."
                    raise ValueError(msg)
                row: Record = {"line": line_number, "Inc": integer(values[0].strip())}
                row.update({c: number(v.strip()) for c, v in zip(columns[1:], values[1:], strict=True)})
                section.rows.append(row)
            except ValueError as exc:
                section.valid = False
                doc.issue("csv_record", str(exc), line_number, error=True)
        validate_series(doc, section, "Time (hrs)")
    except (ValueError, csv.Error, StopIteration) as exc:
        section.valid = False
        doc.issue("csv_structure", str(exc), reader.line_num + 2, error=True)
    doc.metadata["time_unit"] = "hours"
    doc.issue(
        "completion_unverified",
        "CSV has no expected record count or end marker; EOF cannot prove a complete simulation.",
    )
