"""Regressions for hyphenated user-defined TUFLOW log variables (issue #239)."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

from ryan_library.functions.parse_tlf import search_from_top
from ryan_library.orchestrators.tuflow.tuflow_logsummary import process_log_file
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor


def test_defined_variables_keep_hyphenated_names_and_existing_forms() -> None:
    assignments = {
        "IL-PEBBLE": "36.75",
        "CL-PEBBLE": "5.35",
        "IL-WEELI": "63",
        "CL-WEELI": "5.564",
        "IL-COONDINER": "53.865",
        "CL-COONDINER": "4.601",
        "IL-MINING": "21",
        "ordinary": "12",
        "with_underscore": "0.125",
    }
    data: dict[str, Any] = {}
    success = 0
    events = scenarios = variables = False

    with patch("ryan_library.functions.parse_tlf.logger.warning") as warning:
        for line in [
            "Number of defined variables:",
            *(f"Set Variable {name} == {value}" for name, value in assignments.items()),
            "Set Variable ~E1~ == rainfall.csv",
            "Set Variable ~S9~ == existing",
            "Set Variable ~E~ == event",
            "",
        ]:
            data, success, events, scenarios, variables = search_from_top(
                line, data, success, events, scenarios, variables
            )

    assert {name: data[name] for name in assignments} == assignments
    assert "~E1~" not in data
    assert "~S9~" not in data
    assert "~E~" not in data
    assert variables is False
    warning.assert_not_called()


def test_defined_variables_still_warn_for_malformed_assignments() -> None:
    data: dict[str, Any] = {}
    data, success, events, scenarios, variables = search_from_top(
        "Number of defined variables:", data, 0, False, False, False
    )
    invalid_lines = [
        "Set Variable IL--PEBBLE == 1",
        "Set Variable IL-PEBBLE == ",
        "Set Variable IL-PEBBLE = 1",
    ]
    with patch("ryan_library.functions.parse_tlf.logger.warning") as warning:
        for line in invalid_lines:
            data, success, events, scenarios, variables = search_from_top(
                line, data, success, events, scenarios, variables
            )

    assert data == {}
    assert warning.call_count == len(invalid_lines)
    assert all(call.args[0] == "Unexpected variable format: {}" for call in warning.call_args_list)


def test_hyphenated_variables_reach_processor_and_log_summary(tmp_path: Path) -> None:
    log = tmp_path / "M11_5m_001.tlf"
    log_lines = [
        "Build: 2023-03-AF",
        "Computer Name: TUFLOW-TEST",
        "Simulation Started : 2026-Nov-07 10:00",
        r"Input File: C:\Models\M11_5m_001.tcf",
        "Specified Events:",
        "E1 05p",
        "",
        "Specified Scenarios:",
        "S1 EXIST",
        "",
        "Number of defined variables:",
        "Set Variable IL-PEBBLE == 36.75",
        "Set Variable IL-WEELI == 63",
        "Set Variable CL-COONDINER == 2.889",
        "",
        "Final Times",
        "Processor Time: [0.125 h]",
        "Clock Time: [0.250 h]",
        "Simulation FINISHED",
        "Final Cumulative ME: -0.25%",
    ]
    log.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    with patch("ryan_library.functions.parse_tlf.logger.warning") as warning:
        processor = TLFProcessor(file_path=log)
        processor.process()
        summary = process_log_file(log)

    assert processor.processed
    assert not summary.empty
    for field, expected in {
        "IL-PEBBLE": "36.75",
        "IL-WEELI": "63",
        "CL-COONDINER": "2.889",
    }.items():
        assert processor.df.iloc[0][field] == expected
        assert summary.iloc[0][field] == expected
    warning.assert_not_called()
