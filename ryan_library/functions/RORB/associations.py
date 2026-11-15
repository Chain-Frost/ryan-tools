"""Bounded, explicit associations and comparisons; never search recursively or repair source times."""

import csv
import math
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .reader_types import Record, RorbDocument, Section
from .readers import read_rorb_file

type BatchEventKey = tuple[int, str, str, str, int]


def batch_event_key(row: Record) -> BatchEventKey:
    """Original run/duration/unit/frequency/TP identity, never Run alone."""
    return (
        int(str(row["Run"])),
        str(row["duration_raw"]),
        str(row["duration_unit"]),
        str(row["aep_raw"]),
        int(str(row["TPat"])),
    )


def _basename(reference: str) -> str:
    return reference.replace("\\", "/").rsplit("/", 1)[-1]


def _printed_peak(token: str) -> tuple[Decimal, Decimal]:
    """Return a finite source value and the decimal resolution of its printed token."""
    value = Decimal(token.strip().replace("D", "E").replace("d", "e"))
    if not value.is_finite():
        msg = "A printed peak must be finite."
        raise ValueError(msg)
    resolution = Decimal(1).scaleb(int(value.as_tuple().exponent))
    return value, resolution


def _matches_peaks(batch: RorbDocument, row: Record, csv_doc: RorbDocument) -> bool:
    table = csv_doc.section("hydrographs")
    if not table.valid or not table.rows:
        return False

    batch_columns = batch.section("runs").columns
    batch_tokens = batch.lines[int(str(row["line"])) - 1].split()
    for site in batch.section("sites").rows:
        if site["role"] is None or site["name"] is None:
            return False
        label = f"{site['role']} hydrograph: {site['name']}".strip()
        matches = [c for c in table.columns if " ".join(c.split()) == " ".join(label.split())]
        if len(matches) != 1:
            return False
        site_id = str(site["id"])
        peak_row = max(table.rows, key=lambda record: float(str(record[matches[0]])))
        try:
            # Run rows include a printed Unit token, so the source peak index
            # equals its column index in the validated table (which adds "line").
            batch_index = batch_columns.index(site_id)
            expected, batch_resolution = _printed_peak(batch_tokens[batch_index])

            # CSV source rows omit the provenance "line" column.
            csv_index = table.columns.index(matches[0]) - 1
            csv_line = csv_doc.lines[int(str(peak_row["line"])) - 1]
            csv_tokens = next(csv.reader([csv_line], strict=True))
            observed, csv_resolution = _printed_peak(csv_tokens[csv_index])
        except (IndexError, ValueError, InvalidOperation, csv.Error, StopIteration):
            return False

        # Reject an unexpected mapping between source tokens and parsed records.
        if float(expected) != float(str(row[site_id])) or float(observed) != float(str(peak_row[matches[0]])):
            return False
        if abs(observed - expected) > (batch_resolution + csv_resolution) / 2:
            return False
    return bool(batch.section("sites").rows)


def associate_batch_csvs(  # noqa: C901 - bounded event association keeps explicit missing/ambiguous/mismatch cases
    batch: RorbDocument,
    *,
    max_runs: int = 500,
    overrides: Mapping[BatchEventKey, Path] | None = None,
) -> Section:
    """Check only two observed filename conventions against signatures, references, sites and peaks.

    Multiple existing candidates remain ambiguous unless the caller supplies an
    exact event-keyed path override. Overrides are checked against source signature,
    report filename, site labels and peaks just like default candidates. No recursive
    scan or silent filename fallback occurs. Association is not proof of simulation
    validity, physical file identity or source time origin.
    """
    if batch.kind != "batch_output" or batch.status == "invalid":
        msg = "A valid batch run table is required for association"
        raise ValueError(msg)
    runs = batch.section("runs")
    if max_runs < 1 or len(runs.rows) > max_runs:
        msg = f"Association exceeds the requested {max_runs}-run limit"
        raise ValueError(msg)
    bindings = overrides if overrides is not None else {}
    keys = {batch_event_key(row) for row in runs.rows}
    if set(bindings) - keys:
        msg = "CSV overrides contain an unknown run/duration/unit/frequency/TP identity"
        raise ValueError(msg)
    result = Section("csv_associations", ["line", "Run", "TPat", "status", "path", "reason"])
    for row in runs.rows:
        aep = str(row["AEP"]).replace(".", "p")
        duration = str(row["duration_raw"]).replace(".", "_") + str(row["duration_unit"])
        suffix = f" aep{aep}_du{duration}tp{row['TPat']}.csv"
        prefix = batch.path.name.removesuffix("batch.out")
        key = batch_event_key(row)
        explicit = key in bindings
        if explicit:
            selected = bindings[key]
            if not isinstance(selected, Path):
                msg = "CSV override must be a pathlib.Path"
                raise ValueError(msg)
            candidates = [selected if selected.is_absolute() else batch.path.parent / selected]
        else:
            candidates = list(dict.fromkeys([batch.path.parent / (prefix + suffix), batch.path.parent / suffix]))
        existing = [p for p in candidates if p.is_file()]
        record: Record = {
            "line": row["line"],
            "Run": row["Run"],
            "TPat": row["TPat"],
            "path": None,
            "status": "missing",
            "reason": (
                "Explicit CSV override does not exist."
                if explicit
                else "Neither observed filename convention exists."
            ),
        }
        if len(existing) > 1:
            record.update(status="ambiguous", reason="Multiple candidate files exist; no file selected.")
        elif existing:
            candidate = existing[0]
            try:
                doc = read_rorb_file(candidate)
                reference = str(doc.metadata.get("output_reference", ""))
                if doc.kind != "hydrograph_csv" or doc.status == "invalid":
                    msg = "Candidate is not a valid RORB hydrograph CSV."
                    raise ValueError(msg)
                if _basename(reference) != candidate.with_suffix(".out").name:
                    msg = "CSV preamble does not reference the corresponding output filename."
                    raise ValueError(msg)
                if not _matches_peaks(batch, row, doc):
                    msg = "Hydrograph site labels or rounded peaks do not agree with this batch row."
                    raise ValueError(msg)
                record.update(
                    status="corroborated",
                    path=str(candidate),
                    reason=(
                        "Explicit override: CSV signature, output reference, site labels and rounded peaks agree;"
                        " time origin unverified."
                        if explicit
                        else "Event filename, CSV signature, output reference, site labels and rounded peaks agree;"
                        " time origin unverified."
                    ),
                )
            except (OSError, ValueError, UnicodeError, StopIteration) as exc:
                record.update(status="mismatch", reason=str(exc))
        result.rows.append(record)
    result.valid = all(r["status"] == "corroborated" for r in result.rows)
    result.completeness = "all_batch_rows_checked"
    return result


def compare_output_csv(output: RorbDocument, csv_doc: RorbDocument) -> Record:
    """Compare explicitly selected reports by increment and site; preserve both source time axes."""
    if output.kind != "standard_output" or csv_doc.kind != "hydrograph_csv":
        msg = "Select a standard output and a hydrograph CSV"
        raise ValueError(msg)
    if output.status == "invalid" or csv_doc.status == "invalid":
        msg = "Cannot compare invalid documents"
        raise ValueError(msg)
    tables = [s for s in output.sections if s.name.startswith("hydrographs_")]
    if len(tables) != 1:
        msg = "Comparison requires one unambiguous output hydrograph summary"
        raise ValueError(msg)
    left, right = tables[0], csv_doc.section("hydrographs")
    if not left.valid or not right.valid or not left.rows or len(left.rows) != len(right.rows):
        return {"status": "mismatch", "reason": "Invalid, empty or unequal-length series."}
    if [r["Inc"] for r in left.rows] != [r["Inc"] for r in right.rows]:
        return {"status": "mismatch", "reason": "Increment sequences differ."}
    for site in output.section("sites").rows:
        label = " ".join(f"{site['role']} hydrograph: {site['name']}".split())
        columns = [c for c in right.columns if " ".join(c.split()) == label]
        ids = [c for c in left.columns if c.startswith("Hyd") and int(c[3:]) == int(str(site["id"])[3:])]
        if len(columns) != 1 or len(ids) != 1:
            return {"status": "mismatch", "reason": "Site identity is ambiguous or absent."}
        if any(
            not math.isclose(float(str(a[ids[0]])), float(str(b[columns[0]])), rel_tol=0, abs_tol=0.00051)
            for a, b in zip(left.rows, right.rows, strict=True)
        ):
            return {"status": "mismatch", "reason": "Flow series differ."}
    offsets = [float(str(b["Time (hrs)"])) - float(str(a["Time"])) for a, b in zip(left.rows, right.rows, strict=True)]
    constant = all(math.isclose(x, offsets[0], rel_tol=0, abs_tol=1e-9) for x in offsets)
    same_time = all(math.isclose(x, 0, rel_tol=0, abs_tol=1e-9) for x in offsets)
    reference_matches = _basename(str(csv_doc.metadata.get("output_reference", ""))) == output.path.name
    return {
        "status": "match" if same_time and reference_matches else "mismatch",
        "rows": len(offsets),
        "flows_match": True,
        "time_matches": same_time,
        "reference_filename_matches": reference_matches,
        "csv_minus_output_hours": offsets[0] if constant else None,
        "reason": "Compared source values without time correction; matching filenames do not establish physical file identity.",
    }
