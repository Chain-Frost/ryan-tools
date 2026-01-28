#!/usr/bin/env python3
"""
break_pdf_signatures.py

Rewrites a PDF using pypdf, invalidates all digital signatures,
and removes signature fields (/Sig) from the AcroForm.

Priority order for input:
1. If HARD_CODED_INPUT is set, it is always used.
2. Otherwise, the first command-line argument is used.

Output defaults to "<input>_clean.pdf" unless HARD_CODED_OUTPUT is set.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from pypdf import PdfReader, PdfWriter
from pypdf.generic import ArrayObject, NameObject
from pypdf.generic._data_structures import DictionaryObject

# --- User configuration -----------------------------------------------------

HARD_CODED_INPUT: Path | str | None = None  # e.g. Path(r"C:\path\to\in.pdf")
HARD_CODED_OUTPUT: Path | str | None = None  # e.g. Path(r"C:\path\to\out.pdf")

# ---------------------------------------------------------------------------


def _normalize_path(value: Path | str) -> Path:
    return value if isinstance(value, Path) else Path(value)


def _resolve_indirect(value: Any) -> Any:
    return value.get_object() if hasattr(value, "get_object") else value


def _strip_signature_fields(writer: PdfWriter) -> None:
    root: DictionaryObject = writer._root_object
    acro_entry = root.get("/AcroForm")
    if acro_entry is None:
        return

    acro_form = _resolve_indirect(acro_entry)
    if not isinstance(acro_form, dict):
        root.pop("/AcroForm", None)
        return

    fields_entry = acro_form.get("/Fields")
    if fields_entry is None:
        root.pop("/AcroForm", None)
        return

    fields_array = _resolve_indirect(fields_entry)
    if not isinstance(fields_array, ArrayObject):
        root.pop("/AcroForm", None)
        return

    kept_fields = ArrayObject()
    for field_ref in fields_array:
        field_dict = _resolve_indirect(field_ref)
        field_type = field_dict.get("/FT")
        if field_type == NameObject("/Sig"):
            continue
        kept_fields.append(field_ref)

    if kept_fields:
        acro_form[NameObject("/Fields")] = kept_fields
    else:
        acro_form.pop("/Fields", None)

    remaining_fields = acro_form.get("/Fields")
    if remaining_fields is None:
        root.pop("/AcroForm", None)
        return

    remaining_array = _resolve_indirect(remaining_fields)
    if isinstance(remaining_array, ArrayObject) and len(remaining_array) == 0:
        root.pop("/AcroForm", None)


def break_and_remove_signature_fields(src: Path, dst: Path) -> None:
    reader = PdfReader(str(src))
    writer = PdfWriter()
    writer.clone_document_from_reader(reader)
    _strip_signature_fields(writer)

    with dst.open("wb") as output_stream:
        writer.write(output_stream)


def resolve_paths() -> tuple[Path, Path]:
    if HARD_CODED_INPUT is not None:
        src: Path = _normalize_path(value=HARD_CODED_INPUT)
    else:
        if len(sys.argv) < 2:
            raise SystemExit("Usage: python break_pdf_signatures.py <input.pdf> [output.pdf]")
        src = Path(sys.argv[1])

    if HARD_CODED_OUTPUT is not None:
        dst: Path = _normalize_path(HARD_CODED_OUTPUT)
    else:
        if len(sys.argv) >= 3:
            dst = Path(sys.argv[2])
        else:
            dst = src.with_name(f"{src.stem}_clean{src.suffix}")

    return src, dst


def main() -> None:
    src, dst = resolve_paths()

    if not src.exists():
        raise SystemExit(f"Input file does not exist: {src}")

    break_and_remove_signature_fields(src=src, dst=dst)
    print(f"Wrote: {dst}")


if __name__ == "__main__":
    main()
