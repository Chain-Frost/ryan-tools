"""Conservative RORB parameter inspection: documented CMD subset and observed saved settings.

The installed RORB_CMD-documentation_V2 describes column-11 data and a specific
command-line layout. Recognition never certifies executable feature support.
"""

from .reader_types import RorbDocument, Section, integer, number


def read_cmd_parameters(doc: RorbDocument) -> None:  # noqa: C901 - preserve ordered format state and diagnostic scope
    """Parse the V2 single-burst layout; retain other variants without guessing."""
    doc.metadata["format_contract"] = "RORB_CMD-documentation_V2, single-burst subset"
    doc.metadata["engine_compatibility"] = "not_checked"
    doc.metadata["engine_execution"] = "not_performed"
    doc.issue(
        "engine_capabilities_unverified",
        "Parsing does not verify that any RORB_CMD executable supports these settings.",
    )
    section = Section("configuration", ["line", "field", "value"])
    routing = Section("routing", ["line", "isa", "kc", "m"])
    losses = Section("losses", ["line", "isa", "burst", "IL", "CL", "ROC"])
    doc.sections.extend([section, routing, losses])
    if (
        doc.lines[0] != "# BEGIN"
        or doc.lines[-1] != "# END"
        or doc.lines.count("# BEGIN") != 1
        or doc.lines.count("# END") != 1
    ):
        doc.issue(
            "cmd_boundaries", "Expected exactly one column-1 BEGIN/END pair without trailing records.", error=True
        )
        section.valid = False
        return
    position = 1

    def take(*, strip: bool = True) -> tuple[int, str]:
        nonlocal position
        if position >= len(doc.lines) - 1 or len(doc.lines[position]) < 11:
            msg = "Missing column-11 data record."
            raise ValueError(msg)
        line_no = position + 1
        value = doc.lines[position][10:].strip() if strip else doc.lines[position][10:].rstrip()
        position += 1
        return line_no, value

    def pair() -> tuple[int, float, float]:
        line_no, value = take()
        parts = value.split(",")
        if len(parts) != 2:
            msg = "Expected exactly two comma-separated parameter values."
            raise ValueError(msg)
        return line_no, number(parts[0].strip()), number(parts[1].strip())

    try:
        for field in ("catchment_reference", "storm_reference"):
            line_no, value = take(strip=False)
            if not value:
                msg = "Empty input reference."
                raise ValueError(msg)
            section.rows.append({"line": line_no, "field": field, "value": value})
        line_no, value = take()
        if value.upper() not in {"T", "F"}:
            msg = "Expected T/F lumped-kc flag."
            raise ValueError(msg)
        lumped = value.upper() == "T"
        section.rows.append({"line": line_no, "field": "lumped_kc", "value": lumped})
        line_no, value = take()
        verbosity = integer(value, 1)
        if verbosity not in {1, 2, 3}:
            msg = "Output-detail flag is outside the documented V2 values."
            raise ValueError(msg)
        section.rows.append({"line": line_no, "field": "verbosity", "value": verbosity})
        line_no, value = take()
        loss_model = integer(value, 1)
        if loss_model not in {1, 2}:
            doc.issue(
                "cmd_variant",
                "Loss-model flag outside the documented subset; remaining records uninterpreted.",
                line_no,
            )
            return
        section.rows.append({"line": line_no, "field": "loss_model", "value": "IL/CL" if loss_model == 1 else "IL/ROC"})
        line_no, value = take()
        count = integer(value, 1)
        if count > 10000:
            msg = "Interstation count exceeds reader limit."
            raise ValueError(msg)
        section.rows.append({"line": line_no, "field": "interstation_count", "value": count})
        for isa in range(1, (1 if lumped else count) + 1):
            line_no, kc, m = pair()
            routing.rows.append({"line": line_no, "isa": isa, "kc": kc, "m": m})
        line_no, value = take()
        bursts = integer(value, 1)
        section.rows.append({"line": line_no, "field": "burst_count", "value": bursts})
        if bursts != 1:
            doc.issue(
                "cmd_variant",
                "Multiple-burst record ordering is not validated; remaining records uninterpreted.",
                line_no,
            )
            return
        for isa in range(1, count + 1):
            line_no, il, continuing = pair()
            losses.rows.append(
                {"line": line_no, "isa": isa, "burst": 1, "IL": il, "CL" if loss_model == 1 else "ROC": continuing}
            )
        if position != len(doc.lines) - 1:
            doc.issue(
                "cmd_variant",
                "Additional records outside the documented subset; interpretation is partial.",
                position + 1,
            )
            return
        doc.fully_interpreted = True
        for table in doc.sections:
            table.completeness = "documented_record_count"
    except ValueError as exc:
        for table in doc.sections:
            table.valid = False
        doc.issue("cmd_record", str(exc), position + 1, error=True)


def resembles_saved_settings(lines: tuple[str, ...]) -> bool:
    """Require several independent structural observations, not just a .par suffix."""
    return (
        len(lines) >= 536
        and lines[3].rstrip().lower().endswith((".catg", ".cat"))
        and lines[4].rstrip().lower().endswith(".stm")
        and lines[9].strip() in {"T", "F"}
        and lines[14].split() == ["ARR2016", "EnsARR16"]
        and lines[529].strip() == "ARR2016"
        and lines[535].split()[:1] == ["STANDARDAREA"]
    )


def read_saved_settings(doc: RorbDocument) -> None:
    """Expose corroborated records without claiming unknown flags are active settings."""
    doc.metadata["format_contract"] = "observed ARR2016 ensemble saved-settings layout (6.45/6.52 examples)"
    doc.metadata["engine_compatibility"] = "not_checked"
    references = Section("reference_candidates", ["line", "role", "reference", "active"])
    doc.sections.append(references)
    for index, role in [
        (0, "working_directory"),
        (3, "catchment"),
        (4, "storm"),
        (16, "ifd_map"),
        (18, "output_directory"),
        (530, "design_depths"),
        (531, "data_hub"),
        (534, "temporal_patterns"),
    ]:
        references.rows.append(
            {"line": index + 1, "role": role, "reference": doc.lines[index].rstrip(), "active": None}
        )
    records = Section("parameter_candidates", ["line", "record", "first", "second", "active"])
    doc.sections.append(records)
    for index, label in [(20, "routing_pair"), (21, "loss_pair")]:
        tokens = doc.lines[index].split()
        try:
            if len(tokens) != 2:
                msg = "Pair does not match the observed saved-settings layout."
                raise ValueError(msg)
            records.rows.append(
                {
                    "line": index + 1,
                    "record": label,
                    "first": number(tokens[0]),
                    "second": number(tokens[1]),
                    "active": None,
                }
            )
        except ValueError as exc:
            records.valid = False
            doc.issue("saved_record", str(exc), index + 1, error=True)
    doc.issue(
        "saved_settings_partial",
        "Conditional flags, activity, loss-model interpretation and other positional records are unverified; raw records retained. Do not use candidate pairs as active model parameters.",
    )
