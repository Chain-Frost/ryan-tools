# ryan-scripts\misc-python\pypdf_decrypt.py

"""
Decrypt a PDF (remove permission flags) using pypdf.

Usage:
  python3 pypdf_decrypt.py input.pdf [output.pdf]
  or set the DEFAULT_INPUT_PDF
  python3 pypdf_decrypt.py
"""

from __future__ import annotations

import os
import sys

from pypdf import PdfReader, PdfWriter

DEFAULT_INPUT_PDF = r"folder/file.pdf"
DEFAULT_OUTPUT_PDF = None
DEFAULT_DECRYPT_PASSWORD = ""


def _default_output_path(input_path: str) -> str:
    root, ext = os.path.splitext(input_path)
    return f"{root}_decrypted{ext or '.pdf'}"


def main() -> int:
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    else:
        pdf_path = DEFAULT_INPUT_PDF
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    else:
        output_path = DEFAULT_OUTPUT_PDF or _default_output_path(pdf_path)

    if not pdf_path:
        raise ValueError("Set DEFAULT_INPUT_PDF or pass a PDF path on the command line.")

    reader = PdfReader(pdf_path)
    writer = PdfWriter()

    decrypt_status = "not_encrypted"
    if reader.is_encrypted:
        try:
            result = reader.decrypt(DEFAULT_DECRYPT_PASSWORD)
        except Exception as exc:
            raise RuntimeError(f"Decrypt failed: {type(exc).__name__}: {exc}") from exc
        if result == 0:
            raise RuntimeError("Decrypt failed: invalid or missing password.")
        decrypt_status = f"decrypted (result={result})"

    writer.clone_document_from_reader(reader)
    with open(output_path, "wb") as handle:
        writer.write(handle)

    print(f"Decrypt status: {decrypt_status}")
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
