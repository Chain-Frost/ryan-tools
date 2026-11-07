"""Signed numeric, control-path and streaming encoding regressions for TLFs (#224)."""

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from ryan_library.functions.parse_tlf import get_log_lines, search_for_completion, search_from_top
from ryan_library.orchestrators.tuflow.tuflow_logsummary import process_log_file
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor


@pytest.mark.parametrize(
    ("line", "expected_field", "expected_value"),
    [
        ("Start Time (h): -2.0", "Model_Start_Time", -2.0),
        ("Start Time (h): +.5e1", "Model_Start_Time", 5.0),
        ("End Time (h): 1.2E+02", "Model_End_Time", 120.0),
        ("End Time (h): -1.5e-2", "Model_End_Time", -0.015),
        ("Final Cumulative ME: +0.25%", "Final_Cumulative_ME_pct", 0.25),
        ("Final Cumulative ME: -1.2E-02 %", "Final_Cumulative_ME_pct", -0.012),
    ],
)
def test_complete_signed_number_tokens(line: str, expected_field: str, expected_value: float) -> None:
    parsed, _, _ = search_for_completion(line, {}, 0)
    assert parsed[expected_field] == pytest.approx(expected_value)


@pytest.mark.parametrize(
    ("line", "field"),
    [
        ("Start Time (h): NaN", "Model_Start_Time"),
        ("End Time (h): 1.2E+broken", "Model_End_Time"),
        ("End Time (h): 1e9999", "Model_End_Time"),
        ("Final Cumulative ME: -Inf%", "Final_Cumulative_ME_pct"),
        ("Final Cumulative ME: 1.2e+03invalid%", "Final_Cumulative_ME_pct"),
        ("Clock Time: [NaN h]", "Final_RunTime"),
        ("Processor Time: [1e9999 h]", "Final_CPU_Time"),
    ],
)
def test_invalid_numeric_tokens_are_not_partially_accepted(line: str, field: str) -> None:
    with patch("ryan_library.functions.parse_tlf.logger.warning") as warning:
        parsed, _, _ = search_for_completion(line, {}, 0, "final")
    assert field not in parsed
    warning.assert_called()


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (r"Geometry Control File == ../model/My.Model.tgc", {"TGC": "My.Model.tgc"}),
        ("Geometry Control File == local.tgc", {"TGC": "local.tgc"}),
        (r"BC Control File == C:\Project A\BOUND.01.tbc", {"TBC": "BOUND.01.tbc"}),
        ("BC Database == ../model/BC Master.csv ! boundary condition database", {"BC_dbase": "BC Master.csv"}),
        ("ESTRY Control File == ../model/My.Model.v2.ecf -> redirected", {"ECF": "My.Model.v2"}),
        (r"ESTRY Control File == C:\Modèle\My.Model.v2.ecf", {"ECF": "My.Model.v2"}),
        ("BC Event File == ../model/Storm.events.tef ! event mapping", {"TEF": "Storm.events"}),
        ("BC Event File == local.tef", {"TEF": "local"}),
        (
            r"Trying to open (I) file ..\model\Storm.events.tef...OK.  File Unit: 30",
            {"TEF": "Storm.events"},
        ),
    ],
)
def test_control_file_metadata_preserves_full_names(line: str, expected: dict[str, str]) -> None:
    parsed, _, _, _, _ = search_from_top(line, {}, 0, False, False, False)
    assert parsed == expected


@pytest.mark.parametrize(
    ("input_date", "expected"),
    [
        ("2026-Nov-07 10:00", datetime(2026, 11, 7, 10)),
        ("2026-Nov-07 10:00:30", datetime(2026, 11, 7, 10, 0, 30)),
        ("2026-11-07 10:00", datetime(2026, 11, 7, 10)),
        ("2026-11-07 10:00:30", datetime(2026, 11, 7, 10, 0, 30)),
    ],
)
def test_supported_start_timestamp_formats(input_date: str, expected: datetime) -> None:
    data: dict[str, Any] = {}
    data, success, _, _, _ = search_from_top(f"Simulation Started : {input_date}", data, 0, False, False, False)
    assert data["StartDate"] == expected
    assert success == 1


def _write_synthetic_tlf(path: Path, *, encoding: str, large: bool) -> None:
    header = [
        "Build: 2023-03-AF",
        "Computer Name: TEST-PC",
        "Simulation Started : 2026-Nov-07 10:00",
        r"Simulations Log Folder == C:\TUFLOW\log\René ! Source log folder",
        r"Input File: C:\Modèle\My.Model.v2.tcf",
        "Specified Events:",
        "E1 05p",
        "",
        "Specified Scenarios:",
        "S1 EXIST",
        "",
        "Geometry Control File == ../model/My.Model.tgc",
        "ESTRY Control File == ../model/My.Model.v2.ecf -> redirected",
        "BC Event File == Storm.events.tef",
        "Number of defined variables:",
        "Set Variable IL-PEBBLE == 36.75",
        "",
        "Start Time (h): -2.0",
        "End Time (h): 1.2E+02",
    ]
    footer = [
        "Final Times",
        "Processor Time: [+1.5E+00 h]",
        "Clock Time: [2.0E+00 h]",
        "Simulation FINISHED",
        "Final Cumulative ME: +2.5e-01%",
    ]
    with path.open("wb") as output:
        output.write(("\n".join(header) + "\n").encode(encoding))
        if large:
            padding = b"ignore " + b"." * 8180 + b"\n"
            for _ in range(1290):
                output.write(padding)
        output.write(("\n".join(footer) + "\n").encode(encoding))


@pytest.mark.parametrize("encoding", ["cp1252", "utf-8"])
@pytest.mark.parametrize("large", [False, True])
def test_tlf_processor_and_log_summary_match_across_size_and_encoding(
    tmp_path: Path, encoding: str, large: bool
) -> None:
    log_path = tmp_path / "M11_5m_001.tlf"
    _write_synthetic_tlf(log_path, encoding=encoding, large=large)
    assert (log_path.stat().st_size > 10 * 1024 * 1024) is large

    processor = TLFProcessor(file_path=log_path)
    processor.process()
    report = process_log_file(log_path)
    assert processor.processed
    assert not report.empty
    for frame in (processor.df, report):
        row = frame.iloc[0]
        assert row["username"] == "René"
        assert row["orig_TCF_path"] == r"C:\Modèle\My.Model.v2.tcf"
        assert row["TCF"] == "My.Model.v2.tcf"
        assert row["IL-PEBBLE"] == "36.75"
        assert row["TGC"] == "My.Model.tgc"
        assert row["ECF"] == "My.Model.v2"
        assert row["TEF"] == "Storm.events"
        assert row["Model_Start_Time"] == -2.0
        assert row["Model_End_Time"] == 120.0
        assert row["Final_CPU_Time"] == 1.5
        assert row["Final_RunTime"] == 2.0
        assert row["Final_Cumulative_ME_pct"] == 0.25


def test_linewise_encoding_handles_mixed_complete_lines(tmp_path: Path) -> None:
    path = tmp_path / "mixed.tlf"
    path.write_bytes("Model: 流域\n".encode() + "User: René\n".encode("cp1252"))
    lines, last_lines = get_log_lines(path, is_large_file=False)
    assert lines == ["Model: 流域", "User: René"]
    assert last_lines == lines


def test_large_tlf_tail_starts_after_partial_multibyte_sequence(tmp_path: Path) -> None:
    path = tmp_path / "large_utf8.tlf"
    footer = "Final Times\nClock Time: [0.75 h]\nSimulation FINISHED\n"
    with path.open("wb") as output:
        padding = b"ignore " + b"." * 8180 + b"\n"
        for _ in range(1290):
            output.write(padding)
        output.write(("界" * 10015 + "\n").encode("utf-8"))
        output.write(footer.encode("utf-8"))

    assert path.stat().st_size > 10 * 1024 * 1024
    lines, tail = get_log_lines(path, is_large_file=True)
    assert lines == []
    assert tail == footer.splitlines()
    assert all("\ufffd" not in line for line in tail)
