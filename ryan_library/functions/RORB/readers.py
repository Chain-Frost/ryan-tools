"""Public read-only RORB format dispatcher with bounded input and explicit diagnostics."""

from hashlib import sha256
from pathlib import Path

from .output_reader import read_output
from .parameter_reader import read_cmd_parameters, read_saved_settings, resembles_saved_settings
from .reader_types import RorbDocument
from .result_readers import read_batch, read_csv

MAX_FILE_BYTES = 32 * 1024 * 1024


def read_rorb_file(path: Path, *, encoding: str | None = None, max_bytes: int = MAX_FILE_BYTES) -> RorbDocument:
    """Inspect one file without following references, executing RORB or writing outputs.

    UTF-8 is tried first; a strict CP1252 fallback is explicitly diagnosed.
    All decoded lines are retained. Unknown and partially supported variants
    are returned as such; callers must check status and section validity.
    """
    if max_bytes < 1:
        msg = "max_bytes must be positive"
        raise ValueError(msg)
    with path.open("rb") as stream:
        content = stream.read(max_bytes + 1)
    if len(content) > max_bytes:
        msg = f"File exceeds the {max_bytes}-byte reader limit"
        raise ValueError(msg)
    fallback = False
    selected = encoding or "utf-8-sig"
    try:
        decoded = content.decode(selected, errors="strict")
    except UnicodeDecodeError:
        if encoding is not None:
            raise
        selected, fallback = "cp1252", True
        decoded = content.decode(selected, errors="strict")
    lines = tuple(decoded.splitlines())
    doc = RorbDocument(path.absolute(), "unsupported", selected, sha256(content).hexdigest(), lines)
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
    elif first == "# BEGIN":
        doc.kind = "cmd_parameters"
        read_cmd_parameters(doc)
    elif resembles_saved_settings(lines):
        doc.kind = "saved_settings"
        read_saved_settings(doc)
    else:
        doc.issue("unsupported_format", "No validated format structure; extension alone is not sufficient.")
    return doc
