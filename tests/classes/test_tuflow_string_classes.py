# tests/classes/test_tuflow_string_classes.py
import pytest
from pathlib import Path
import json
from ryan_library.classes.tuflow_string_classes import (
    TuflowStringParser,
    RunCodeComponent,
)


# Fixture to load example file names and expected outputs
@pytest.fixture(scope="module")
def example_file_names():
    with open(
        Path(__file__).parent.parent
        / "test_data"
        / "tuflow"
        / "example_file_names.json",
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)
    return data["file_names"]


@pytest.fixture(scope="module")
def suffixes():
    """
    Load the real suffixes.json for testing.
    """
    with open(
        Path(__file__).parent.parent.parent
        / "ryan_library"
        / "classes"
        / "suffixes.json",
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)
    return data["suffixes"]


def test_suffix_loading(suffixes):
    """
    Verify that the suffixes are loaded correctly.
    """
    assert isinstance(suffixes, dict), "Suffixes should be a dictionary."
    assert "_1d_Cmx.csv" in suffixes, "Missing expected key in suffixes."


def test_file_name_parsing(example_file_names):
    for test_case in example_file_names:
        file_name = test_case["file_name"]
        expected = test_case["expected"]

        parser = TuflowStringParser(
            file_path=Path("test_data/tuflow/tutorial") / file_name
        )

        assert parser.file_name == file_name, f"File name mismatch for {file_name}"
        assert parser.data_type == expected.get(
            "data_type"
        ), f"Data type mismatch for {file_name}"
        assert parser.raw_run_code == expected.get(
            "raw_run_code"
        ), f"Raw run code mismatch for {file_name}"
        assert parser.clean_run_code == expected.get(
            "clean_run_code"
        ), f"Clean run code mismatch for {file_name}"
        assert parser.run_code_parts == expected.get(
            "run_code_parts"
        ), f"Run code parts mismatch for {file_name}"

        # Test RunCodeComponents
        if "tp" in expected:
            assert parser.tp is not None, f"TP component missing for {file_name}"
            assert (
                parser.tp.text_repr == expected["tp"]["text_repr"]
            ), f"TP text_repr mismatch for {file_name}"
            assert (
                parser.tp.numeric_value == expected["tp"]["numeric_value"]
            ), f"TP numeric_value mismatch for {file_name}"
        else:
            assert parser.tp is None, f"Unexpected TP component for {file_name}"

        if "duration" in expected:
            assert (
                parser.duration is not None
            ), f"Duration component missing for {file_name}"
            assert (
                parser.duration.text_repr == expected["duration"]["text_repr"]
            ), f"Duration text_repr mismatch for {file_name}"
            assert (
                parser.duration.numeric_value == expected["duration"]["numeric_value"]
            ), f"Duration numeric_value mismatch for {file_name}"
        else:
            assert (
                parser.duration is None
            ), f"Unexpected Duration component for {file_name}"

        if "aep" in expected:
            assert parser.aep is not None, f"AEP component missing for {file_name}"
            assert (
                parser.aep.text_repr == expected["aep"]["text_repr"]
            ), f"AEP text_repr mismatch for {file_name}"
            assert (
                parser.aep.numeric_value == expected["aep"]["numeric_value"]
            ), f"AEP numeric_value mismatch for {file_name}"
        else:
            assert parser.aep is None, f"Unexpected AEP component for {file_name}"

        # Test trimmed run code
        assert parser.trim_run_code == expected.get(
            "trim_run_code"
        ), f"Trimmed run code mismatch for {file_name}"


def test_run_code_component():
    # Test numeric parsing
    component = RunCodeComponent(raw_value="12", component_type="TP")
    assert component.numeric_value == 12
    assert component.text_repr == "TP12"

    component = RunCodeComponent(raw_value="3.5", component_type="AEP")
    assert component.numeric_value == 3.5
    assert component.text_repr == "3.5p"

    component = RunCodeComponent(raw_value="120", component_type="Duration")
    assert component.numeric_value == 120
    assert component.text_repr == "120m"

    # Test invalid numeric value
    component = RunCodeComponent(raw_value="abc", component_type="TP")
    assert component.numeric_value is None
    assert component.text_repr == "abc"


def test_trim_runcode_function():
    from ryan_library.classes.tuflow_string_classes import trim_runcode

    # Test case 1: All components present
    run_code = "R01_R02_TP12_Duration300_AEP1.5"
    aep = "AEP1.5"
    duration = "Duration300"
    tp = "TP12"
    expected = "R01_R02"
    result = trim_runcode(run_code, aep, duration, tp)
    assert (
        result == expected
    ), f"Trim run code failed: expected {expected}, got {result}"

    # Test case 2: Some components missing
    run_code = "R01_TP12_AEP2.0"
    aep = "AEP2.0"
    duration = None
    tp = "TP12"
    expected = "R01"
    result = trim_runcode(run_code, aep, duration, tp)
    assert (
        result == expected
    ), f"Trim run code failed: expected {expected}, got {result}"

    # Test case 3: No components to remove
    run_code = "R01_R02"
    aep = None
    duration = None
    tp = None
    expected = "R01_R02"
    result = trim_runcode(run_code, aep, duration, tp)
    assert (
        result == expected
    ), f"Trim run code failed: expected {expected}, got {result}"

    # Test case 4: Run code with different delimiters
    run_code = "R01+TP12+Duration300+AEP1.5"
    aep = "AEP1.5"
    duration = "Duration300"
    tp = "TP12"
    expected = "R01"
    result = trim_runcode(run_code, aep, duration, tp)
    assert (
        result == expected
    ), f"Trim run code failed with delimiters: expected {expected}, got {result}"