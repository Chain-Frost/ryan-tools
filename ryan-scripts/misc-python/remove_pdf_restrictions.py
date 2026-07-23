#!/usr/bin/env python3
"""
Remove PDF permission restrictions by rewriting PDFs with pypdf.

This script is intentionally narrow. It opens each input PDF, decrypts it when
possible, then writes a fresh unencrypted copy. That removes common PDF
permission flags such as copy/edit/print restrictions when the file can be
opened with the supplied password, often an empty password.

It does not remove layers, OCG content, annotations, JavaScript, signatures,
form fields, or page content. Use the other specialist scripts for those tasks.

Inputs can be supplied in two ways:
- Hard-coded defaults in the configuration section below.
- Command-line arguments, which override the hard-coded input/output settings.

The reusable remove_pdf_restrictions() function accepts a single Path/string or
a list/tuple of Paths and strings.

Usage:
  python remove_pdf_restrictions.py
  python remove_pdf_restrictions.py input.pdf another.pdf
  python remove_pdf_restrictions.py input.pdf --output output.pdf
  python remove_pdf_restrictions.py input.pdf another.pdf --output-dir C:\\Temp\\clean
  python remove_pdf_restrictions.py input.pdf --password secret
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from pypdf import PdfReader, PdfWriter

# --- User configuration -----------------------------------------------------

DEFAULT_INPUT_PDFS: Path | str | Sequence[Path | str] = Path(
    r"C:\folder\file.pdf"
)
# DEFAULT_INPUT_PDFS = Path(r"C:\Temp\input.pdf")
# DEFAULT_INPUT_PDFS = r"C:\Temp\input.pdf"
# DEFAULT_INPUT_PDFS = [r"C:\Temp\first.pdf", Path(r"C:\Temp\second.pdf")]

DEFAULT_OUTPUT: Path | str | None = None
# DEFAULT_OUTPUT = Path(r"C:\Temp\input_unrestricted.pdf")  # single input only
# DEFAULT_OUTPUT = Path(r"C:\Temp\unrestricted_pdfs")  # output folder for multiple inputs

DEFAULT_OUTPUT_DIR: Path | str | None = None
# DEFAULT_OUTPUT_DIR = Path(r"C:\Temp\unrestricted_pdfs")

DEFAULT_DECRYPT_PASSWORD = ""

# ---------------------------------------------------------------------------

PathInput = Path | str
PathInputs = PathInput | Sequence[PathInput]


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_unrestricted{input_path.suffix or '.pdf'}")


def normalize_inputs(inputs: PathInputs | None) -> list[Path]:
    if inputs is None:
        return []

    if isinstance(inputs, Path | str):
        return [Path(inputs)]

    return [Path(input_path) for input_path in inputs]


def resolve_output_path(src: Path, output: Path | None, output_dir: Path | None, input_count: int) -> Path:
    if output_dir is not None:
        return output_dir / default_output_path(src).name

    if output is None:
        return default_output_path(src)

    if input_count == 1:
        return output

    return output / default_output_path(src).name


def remove_pdf_restrictions_from_file(
    src: PathInput, dst: PathInput | None = None, password: str = ""
) -> tuple[Path, str]:
    source_path = Path(src)
    destination_path = Path(dst) if dst is not None else default_output_path(source_path)

    if not source_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {source_path}")

    reader = PdfReader(str(source_path))
    decrypt_status = "not encrypted"

    if reader.is_encrypted:
        result = reader.decrypt(password)
        if result == 0:
            raise ValueError("PDF is encrypted and could not be decrypted with the supplied password.")
        decrypt_status = f"decrypted with result {result}"

    writer = PdfWriter()
    writer.clone_document_from_reader(reader)

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with destination_path.open("wb") as output_stream:
        writer.write(output_stream)

    return destination_path, decrypt_status


def remove_pdf_restrictions(
    inputs: PathInputs,
    output: PathInput | None = None,
    output_dir: PathInput | None = None,
    password: str = DEFAULT_DECRYPT_PASSWORD,
) -> list[tuple[Path, str]]:
    input_paths = normalize_inputs(inputs)
    if not input_paths:
        raise ValueError("No input PDFs were supplied.")

    output_path = Path(output) if output is not None else None
    output_dir_path = Path(output_dir) if output_dir is not None else None

    if output_path is not None and output_dir_path is not None:
        raise ValueError("Use either output or output_dir, not both.")

    if output_path is not None and len(input_paths) > 1 and output_path.suffix:
        raise ValueError("For multiple inputs, output must be a folder. Use output_dir for clarity.")

    results: list[tuple[Path, str]] = []
    for src in input_paths:
        dst = resolve_output_path(
            src=src,
            output=output_path,
            output_dir=output_dir_path,
            input_count=len(input_paths),
        )
        results.append(remove_pdf_restrictions_from_file(src=src, dst=dst, password=password))

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rewrite a PDF without PDF permission restrictions.")
    parser.add_argument("input_pdfs", nargs="*", type=Path, help="Input PDF path(s). Overrides DEFAULT_INPUT_PDFS.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output PDF for one input, or output folder for multiple inputs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output folder. Each PDF is written with the default unrestricted filename.",
    )
    parser.add_argument("--password", default=DEFAULT_DECRYPT_PASSWORD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inputs = args.input_pdfs if args.input_pdfs else DEFAULT_INPUT_PDFS
    output = args.output if args.output is not None else DEFAULT_OUTPUT
    output_dir = args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR

    if args.output is not None and args.output_dir is not None:
        raise SystemExit("Use either --output or --output-dir, not both.")

    try:
        results = remove_pdf_restrictions(
            inputs=inputs,
            output=output,
            output_dir=output_dir,
            password=args.password,
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    for dst, decrypt_status in results:
        print(f"Decrypt status: {decrypt_status}")
        print(f"Wrote: {dst}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
