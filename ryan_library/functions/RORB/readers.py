"""Public read-only RORB format dispatcher with bounded input and explicit diagnostics."""

from hashlib import sha256
from pathlib import Path

from .output_reader import read_output
from .parameter_reader import read_cmd_parameters, read_saved_settings, resembles_saved_settings
from .reader_types import RorbDocument
from .result_readers import read_batch, read_csv

MAX_FILE_BYTES = 32 * 1024 * 1024


def _decode(content: bytes, encoding: str | None) -> tuple[str, str, bool]:
    """Decode without replacement and report whether an encoding fallback was used."""
    fallback = False
    selected = encoding or "utf-8-sig"
    try:
        decoded = content.decode(selected, errors="strict")
    except UnicodeDecodeError:
        if encoding is not None:
            raise
        selected, fallback = "cp1252", True
        decoded = content.decode(selected, errors="strict")
    return decoded, selected, fallback


def read_rorb_bytes(
    content: bytes,
    *,
    path: Path | None = None,
    encoding: str | None = None,
    max_bytes: int = MAX_FILE_BYTES,
) -> RorbDocument:
    """Inspect an exact in-memory source snapshot with the same grammar as file reads.

    Returns retained bytes, decoded lines, original source tokens, typed sections
    and diagnostics. A banner never proves the executable which produced a report.
    No files are opened or written, and references are never followed.
    """
    if max_bytes < 1:
        msg = "max_bytes must be positive"
        raise ValueError(msg)
    if len(content) > max_bytes:
        msg = f"File exceeds the {max_bytes}-byte reader limit"
        raise ValueError(msg)
    decoded, selected, fallback = _decode(content, encoding)
    lines = tuple(decoded.splitlines())
    document_path = Path("<memory>") if path is None else path
    doc = RorbDocument(
        document_path.absolute(),
        "unsupported",
        selected,
        sha256(content).hexdigest(),
        lines,
        original_bytes=bytes(content),
    )
    if fallback:
        doc.issue(
            "encoding_fallback", "UTF-8 decoding failed; decoded losslessly as CP1252. Override encoding if needed."
        )
    if not lines:
        doc.issue("empty_file", "Empty file; no format evidence.", error=True)
        return doc
    first = lines[0].strip()
    if first == "RORBWin Batch Run Summary":
        doc.kind = "batch_output"
        read_batch(doc)
    elif first == "RORBWin CSV Output File":
        doc.kind = "hydrograph_csv"
        read_csv(doc)
    elif first == "RORBWin Output File":
        doc.kind = "standard_output"
        read_output(doc)
    elif any(line.split()[:2] == ["Inc", "Time"] for line in lines) and (
        any(line.lstrip().startswith("Time increment (hours)") for line in lines)
        or any(line.strip() == "Hydrograph summary" for line in lines)
    ):
        # Some retained standard reports have lost their identifying banner.
        # Content is parseable, but the executable and full format remain unknown.
        doc.kind = "standard_output"
        doc.issue("missing_report_banner", "Content-based standard report; executable identity unverified.")
        read_output(doc)
    elif first == "# BEGIN":
        doc.kind = "cmd_parameters"
        read_cmd_parameters(doc)
    elif resembles_saved_settings(lines):
        doc.kind = "saved_settings"
        read_saved_settings(doc)
    else:
        doc.issue("unsupported_format", "No validated format structure; extension alone is not sufficient.")
    return doc



def read_rorb_table_bytes(
    content: bytes,
    *,
    path: Path | None = None,
    encoding: str | None = None,
    max_bytes: int = MAX_FILE_BYTES,
) -> RorbDocument:
    """Explicitly inspect an unversioned Inc/Time table as *unverified* evidence.

    Callers must opt in to loose source admission. The same canonical numeric
    grammar validates the table; executable identity, physical sites, clock
    conventions and full-report completeness are never inferred from a bare
    table. No fallback reader or duplicated numeric grammar is maintained.
    """
    document = read_rorb_bytes(content, path=path, encoding=encoding, max_bytes=max_bytes)
    if document.kind != "unsupported":
        return document
    if not any(line.split()[:2] == ["Inc", "Time"] for line in document.lines):
        return document
    document.kind = "isolated_hydrograph_table"
    document.diagnostics = [item for item in document.diagnostics if item.code != "unsupported_format"]
    document.issue(
        "unverified_table_source",
        "Bare Inc/Time table has no version or executable identity; source association is caller-owned.",
    )
    read_output(document)
    return document


def read_rorb_table_file(
    path: Path, *, encoding: str | None = None, max_bytes: int = MAX_FILE_BYTES
) -> RorbDocument:
    """Read an opt-in unverified bare table using a bounded file snapshot."""
    if max_bytes < 1:
        msg = "max_bytes must be positive"
        raise ValueError(msg)
    with path.open("rb") as stream:
        content = stream.read(max_bytes + 1)
    return read_rorb_table_bytes(content, path=path, encoding=encoding, max_bytes=max_bytes)


def read_rorb_file(path: Path, *, encoding: str | None = None, max_bytes: int = MAX_FILE_BYTES) -> RorbDocument:
    """Inspect a bounded file by delegating to the canonical bytes reader."""
    if max_bytes < 1:
        msg = "max_bytes must be positive"
        raise ValueError(msg)
    with path.open("rb") as stream:
        content = stream.read(max_bytes + 1)
    return read_rorb_bytes(content, path=path, encoding=encoding, max_bytes=max_bytes)
