"""Bounded read-only MCP views of the source-preserving RORB readers."""

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..functions.RORB.associations import associate_batch_csvs, compare_output_csv
from ..functions.RORB.readers import MAX_FILE_BYTES, read_rorb_file

RORB_GUIDANCE_URI = "ryan-tools://guidance/rorb-readers"
MAX_RESPONSE_CHARS = 60000


def _bounded_text(value: object) -> object:
    if not isinstance(value, str) or len(json.dumps(value, ensure_ascii=True)) <= 300:
        return value
    preview = value[:280]
    while len(json.dumps(preview + "... [truncated]", ensure_ascii=True)) > 300:
        preview = preview[: len(preview) // 2]
    return preview + "... [truncated]"


def inspect_rorb_file(
    file_path: str,
    section: str | None = None,
    offset: int = 0,
    limit: int = 10,
    encoding: str | None = None,
    resolve_csv: bool = False,
    comparison_path: str | None = None,
) -> dict[str, Any]:
    """Inspect RORB batch, CSV, standard output or supported parameter layouts without writes.

    Default returns metadata, diagnostics and section counts. Select a section
    (or 'raw') for a page of rows. Strings/columns are bounded with truncation
    flags. resolve_csv explicitly checks at most 500 batch-row associations in
    the same directory. comparison_path explicitly selects an output/CSV pair;
    it never corrects their times. Parsing does not certify engine capability.
    """
    if isinstance(limit, bool) or not 0 <= limit <= 20 or isinstance(offset, bool) or offset < 0:
        return {"error": "limit must be 0..20 and offset must be nonnegative"}
    try:
        document = read_rorb_file(Path(file_path), encoding=encoding)
        if resolve_csv:
            document.sections.append(associate_batch_csvs(document))
        result: dict[str, Any] = {
            "path": _bounded_text(str(document.path)),
            "path_truncated": _bounded_text(str(document.path)) != str(document.path),
            "format": document.kind,
            "status": document.status,
            "encoding": document.encoding,
            "sha256": document.sha256,
            "line_count": len(document.lines),
            "fully_interpreted": document.fully_interpreted,
            "engine_execution": "not_performed",
            "metadata": {k: _bounded_text(v) for k, v in list(document.metadata.items())[:40]},
            "metadata_count": len(document.metadata),
            "diagnostic_count": len(document.diagnostics),
            "diagnostics": [{k: _bounded_text(v) for k, v in asdict(d).items()} for d in document.diagnostics[:20]],
            "diagnostics_truncated": len(document.diagnostics) > 20,
            "section_count": len(document.sections),
            "sections": [
                {"name": s.name, "row_count": len(s.rows), "valid": s.valid, "completeness": s.completeness}
                for s in document.sections[:20]
            ],
            "sections_truncated": len(document.sections) > 20,
            "text_limit": 300,
            "column_limit": 30,
            "input_byte_limit": MAX_FILE_BYTES,
        }
        if section is not None:
            if section == "raw":
                rows = [
                    {"line": i + 1, "text": document.lines[i]}
                    for i in range(offset, min(offset + limit, len(document.lines)))
                ]
                count = len(document.lines)
                columns = ["line", "text"]
            else:
                selected = document.section(section)
                rows = selected.rows[offset : offset + limit]
                count, columns = len(selected.rows), selected.columns
                result["section_valid"] = selected.valid
            result.update(
                section=section,
                offset=offset,
                row_count=count,
                columns=[
                    str(c) if _bounded_text(c) == c else f"[{i}] {_bounded_text(c)}" for i, c in enumerate(columns[:30])
                ],
                columns_truncated=len(columns) > 30,
            )
            # Long labels are displayed only as bounded previews; Python retains exact keys.
            labels = dict(zip(columns[:30], result["columns"], strict=True))
            sample = [{labels[k]: _bounded_text(v) for k, v in row.items() if k in labels} for row in rows]
            result["sample"] = sample
            result["values_truncated"] = any(_bounded_text(c) != c for c in columns[:30]) or any(
                _bounded_text(v) != v for row in rows for v in row.values()
            )
            while len(json.dumps(result, ensure_ascii=True, allow_nan=False)) > MAX_RESPONSE_CHARS - 1000 and sample:
                sample.pop()
            result["returned_count"] = len(sample)
            result["has_more"] = offset + len(sample) < count
            result["next_offset"] = offset + len(sample) if result["has_more"] and sample else None
        if comparison_path is not None:
            other = read_rorb_file(Path(comparison_path), encoding=encoding)
            output, csv_doc = (document, other) if document.kind == "standard_output" else (other, document)
            result["comparison"] = compare_output_csv(output, csv_doc)
        result["metadata_truncated"] = len(document.metadata) > 40 or any(
            _bounded_text(v) != v for v in document.metadata.values()
        )
        return result
    except StopIteration:
        return {"error": f"No section {section!r}; inspect without a section to list available sections"}
    except (OSError, ValueError, LookupError) as exc:
        return {"error": str(exc)[:1000]}


def rorb_reader_guidance_resource() -> str:
    """Describe supported inspection and the distinction between parsing and engine support."""
    return json.dumps(
        {
            "tool": "inspect_rorb_file",
            "python_import": "from ryan_library.functions.RORB.readers import read_rorb_file",
            "python_usage": "document = read_rorb_file(Path(path)); inspect document.status, diagnostics and section.valid before using rows",
            "formats": {
                "batch_output": "Observed 6.45/6.52 run tables and labelled metadata",
                "hydrograph_csv": "RORBWin signature, reference preamble, Inc/time/labelled hydrographs",
                "standard_output": "Supported hydrograph summaries, sites and simple subarea rainfall tables; other sections raw",
                "cmd_parameters": "Installed V2 documented single-burst subset; other variants explicit",
                "saved_settings": "Observed ARR2016 ensemble layout: reference and parameter candidates only; activity unknown",
            },
            "limits": {
                "file_bytes": MAX_FILE_BYTES,
                "rows": 20,
                "columns": 30,
                "text_characters": 300,
                "association_runs": 500,
            },
            "raw": "section='raw' returns numbered source lines, paginated and text-bounded; Python retains full decoded lines",
            "semantics": [
                "partial does not mean fully interpreted or complete simulation",
                "invalid sections may retain rows for diagnosis; never use them as valid engineering results",
                "unknown parameters are not zero; loss-model applicability is explicit",
                "CSV and standard output time axes are preserved, not reconciled automatically",
                "engine compatibility is not verified by parsing a parameter file",
                "No engine launch, reference traversal, source modification or export occurs by default",
            ],
            "associations": "Explicit resolve_csv checks two observed same-directory names plus signature, report reference, sites and peaks; ambiguous candidates are rejected",
            "comparison": "comparison_path explicitly selects the corresponding output/CSV; reports flow and time differences",
            "source_guide": "ryan_library/functions/RORB/README.md",
        },
        indent=2,
    )
