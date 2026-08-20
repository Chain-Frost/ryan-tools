"""Single-file handoff for a web agent using the TUFLOW string parser.

Agent instructions
------------------
This file is an integration adapter, not a second parser implementation. The
``ryan_functions`` distribution will be installed in the Python environment;
use the authoritative classes imported from ``ryan_library`` below. Do not
copy their regexes or suffix registry into web application code.

Call ``parse_tuflow_path`` at the Python backend boundary and return its result
as JSON. The parser only inspects the supplied path and filename; it does not
require the file to exist and it does not read file contents. File data types
come from the configuration bundled with the installed distribution.

Runtime contract:

* Python 3.14 or newer.
* The ``ryan_functions`` distribution is installed and version-aligned with
  the application.
* Import paths remain ``ryan_library...`` even though the distribution is
  named ``ryan_functions``.
* Treat ``None`` as "not detected". AEP values ``PMP`` and ``PMPF`` have no
  finite numeric value, so this adapter emits ``null`` for ``numeric_value``.
* Preserve ``raw_run_code`` for display/auditing and use ``trim_run_code`` when
  the AEP, duration and temporal-pattern tokens must be removed.

Backend example::

    result = parse_tuflow_path("R01_TP12_2.0p_120m_POMM.csv")

The module can also be smoke-tested directly::

    python web_agent_tuflow_string_parser.py R01_TP12_2.0p_120m_POMM.csv
"""

import argparse
import json
import math
from pathlib import Path
from typing import TypedDict

from ryan_library.classes.tuflow_string_classes import RunCodeComponent, TuflowStringParser


class ComponentResult(TypedDict):
    """JSON-safe representation of one parsed run-code component."""

    raw_value: str
    component_type: str
    numeric_value: float | int | None
    text_repr: str
    original_text: str | None


class TuflowParseResult(TypedDict):
    """JSON-safe result returned to the web application."""

    file_path: str
    file_name: str
    data_type: str | None
    raw_run_code: str
    clean_run_code: str
    run_code_parts: dict[str, str]
    tp: ComponentResult | None
    duration: ComponentResult | None
    aep: ComponentResult | None
    trim_run_code: str


def _json_numeric(value: float | int | None) -> float | int | None:
    """Convert non-finite floats to ``None`` for standards-compliant JSON."""

    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _component_result(component: RunCodeComponent | None) -> ComponentResult | None:
    """Serialize a parsed component without exposing implementation details."""

    if component is None:
        return None
    return ComponentResult(
        raw_value=component.raw_value,
        component_type=component.component_type,
        numeric_value=_json_numeric(component.numeric_value),
        text_repr=component.text_repr,
        original_text=component.original_text,
    )


def parse_tuflow_path(file_path: str | Path) -> TuflowParseResult:
    """Parse a TUFLOW result path into a stable, JSON-safe web payload."""

    parser = TuflowStringParser(file_path=file_path)
    return TuflowParseResult(
        file_path=str(parser.file_path),
        file_name=parser.file_name,
        data_type=parser.data_type,
        raw_run_code=parser.raw_run_code,
        clean_run_code=parser.clean_run_code,
        run_code_parts=parser.run_code_parts,
        tp=_component_result(parser.tp),
        duration=_component_result(parser.duration),
        aep=_component_result(parser.aep),
        trim_run_code=parser.trim_run_code,
    )


def main() -> None:
    """Print one parsed path as JSON for a direct integration smoke test."""

    argument_parser = argparse.ArgumentParser(description="Parse a TUFLOW filename into JSON.")
    argument_parser.add_argument("file_path", type=Path, help="TUFLOW result path or filename")
    args = argument_parser.parse_args()
    print(json.dumps(parse_tuflow_path(args.file_path), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
