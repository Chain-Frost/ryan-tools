"""Non-executing executable-location checks for engineering acceptance.

Finding a file is not equivalent to testing its imports, licences, version,
process behaviour or outputs. MCP discovery does not call this module implicitly.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

type ProbeStatus = Literal["path_found_unverified", "not_found"]


@dataclass(frozen=True, slots=True)
class ExecutableProbe:
    """Location evidence only; never an application-readiness certification."""

    program: str
    status: ProbeStatus
    path: Path | None
    source: Literal["configured_path", "path_lookup"]
    note: str


def locate_external_program(
    program: str,
    *,
    configured_path: Path | None = None,
    find_on_path: Callable[[str], str | None] | None = None,
) -> ExecutableProbe:
    """Find an explicitly configured executable or perform a PATH lookup, without launching it.

    The finder is injected for tests; production defaults to shutil.which.
    A configured path is authoritative: an invalid override must not silently
    fall back to a different, potentially untrusted executable on PATH.
    """
    if not program.strip():
        msg = "External program name must be non-empty."
        raise ValueError(msg)

    source: Literal["configured_path", "path_lookup"]
    candidate: Path | None
    if configured_path is not None:
        source = "configured_path"
        candidate = configured_path.expanduser().absolute()
    else:
        source = "path_lookup"
        found = (find_on_path or shutil.which)(program)
        candidate = Path(found).absolute() if found else None

    if candidate is None or not candidate.is_file():
        return ExecutableProbe(
            program=program,
            status="not_found",
            path=candidate,
            source=source,
            note="No executable file was found; check the configured path, PATH and target application environment.",
        )
    return ExecutableProbe(
        program=program,
        status="path_found_unverified",
        path=candidate,
        source=source,
        note="File exists only. Application launch, version, licence, DLLs and engineering outputs are not verified.",
    )
