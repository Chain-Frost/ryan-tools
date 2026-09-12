# ryan-scripts\misc-python\pypdf_decrypt.py

"""Decrypt a PDF (remove permission flags) using pypdf.

Usage:
  python3 pypdf_decrypt.py input.pdf [output.pdf]
  or set the DEFAULT_INPUT_PDF
  python3 pypdf_decrypt.py
"""

# Standalone install: python -m pip install pypdf "cryptography>=3.1"

from __future__ import annotations

import sys
from pathlib import Path

from pypdf import PdfReader, PdfWriter

DEFAULT_INPUT_PDF = Path("folder/file.pdf")
DEFAULT_OUTPUT_PDF: Path | None = None
DEFAULT_DECRYPT_PASSWORD = ""


def _default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_decrypted{input_path.suffix or '.pdf'}")


def main() -> int:
    if len(sys.argv) > 1:
        pdf_path = Path(sys.argv[1])
    else:
        pdf_path = DEFAULT_INPUT_PDF
    if len(sys.argv) > 2:
        output_path = Path(sys.argv[2])
    else:
        output_path = DEFAULT_OUTPUT_PDF or _default_output_path(pdf_path)

    if not pdf_path:
        msg = "Set DEFAULT_INPUT_PDF or pass a PDF path on the command line."
        raise ValueError(msg)

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()

    decrypt_status = "not_encrypted"
    if reader.is_encrypted:
        try:
            result = reader.decrypt(DEFAULT_DECRYPT_PASSWORD)
        except Exception as exc:
            msg = f"Decrypt failed: {type(exc).__name__}: {exc}"
            raise RuntimeError(msg) from exc
        if result == 0:
            msg = "Decrypt failed: invalid or missing password."
            raise RuntimeError(msg)
        decrypt_status = f"decrypted (result={result})"

    writer.clone_document_from_reader(reader)
    with output_path.open("wb") as handle:
        writer.write(handle)

    print(f"Decrypt status: {decrypt_status}")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
