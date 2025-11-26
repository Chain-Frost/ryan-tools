import json
from pathlib import Path
import pytest

# import sys
# REPO_ROOT = Path(__file__).resolve().parents[2]
# if str(REPO_ROOT) not in sys.path:
#     sys.path.insert(0, str(REPO_ROOT))

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.classes.tuflow_string_classes import RunCodeComponent, TuflowStringParser

DATA_DIR: Path = Path(__file__).absolute().parent.parent / "test_data" / "tuflow"


def locate_file(name: str) -> Path:
    matches: list[Path] = list(DATA_DIR.glob(f"tutorials/*/results/{name}"))
    if not matches:
        raise FileNotFoundError(name)
    return matches[0]


@pytest.fixture(scope="module")
def run_code_examples() -> list[dict]:
    with open(DATA_DIR / "pomm_run_codes.json", "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def suffixes() -> dict[str, str]:
    return SuffixesConfig.get_instance().suffix_to_type


def test_suffix_loading(suffixes: dict[str, str]) -> None:
    assert isinstance(suffixes, dict), "Suffixes should be a dictionary."
    assert "_POMM.csv" in suffixes, "Missing expected key in suffixes."


def test_file_name_parsing(run_code_examples: list[dict]) -> None:
    for test_case in run_code_examples:
        file_name: str = test_case["file_name"]
        expected: dict = test_case["expected"]

        parser = TuflowStringParser(file_path=locate_file(file_name))

        assert parser.file_name == file_name, f"File name mismatch for {file_name}"
        assert parser.run_code_parts == expected.get("run_code_parts"), f"Run code parts mismatch for {file_name}"
        assert (parser.aep.text_repr if parser.aep else None) == expected.get("aep"), f"AEP mismatch for {file_name}"
        assert (parser.duration.text_repr if parser.duration else None) == expected.get(
            "duration"
        ), f"Duration mismatch for {file_name}"
        assert (parser.tp.text_repr if parser.tp else None) == expected.get("tp"), f"TP mismatch for {file_name}"
        assert parser.trim_run_code == expected.get("trim_run_code"), f"Trimmed run code mismatch for {file_name}"


def test_run_code_component_numeric_and_text_repr() -> None:
    tp_component = RunCodeComponent(raw_value="12", component_type="TP")
    assert tp_component.numeric_value == 12
    assert tp_component.text_repr == "TP12"

    aep_component = RunCodeComponent(raw_value="3.5", component_type="AEP")
    assert aep_component.numeric_value == 3.5
    assert aep_component.text_repr == "3.5p"

    duration_component = RunCodeComponent(raw_value="120", component_type="Duration")
    assert duration_component.numeric_value == 120
    assert duration_component.text_repr == "120m"


def test_trim_run_code_removes_parsed_components() -> None:
    parser = TuflowStringParser(file_path=Path("R01_TP12_2.0p_120m_POMM.csv"))

    assert parser.data_type == "POMM"
    assert parser.tp and parser.tp.text_repr == "TP12"
    assert parser.aep and parser.aep.text_repr == "2.0p"
    assert parser.duration and parser.duration.text_repr == "120m"
    assert parser.trim_run_code == "R01"
