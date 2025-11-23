import json
from pathlib import Path
from typing import Any, Hashable

# import sys
# REPO_ROOT: Path = Path(__file__).resolve().parents[2]
# if str(REPO_ROOT) not in sys.path:
#     sys.path.insert(0, str(REPO_ROOT))

from ...ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ...ryan_library.processors.tuflow.other_processors.POMMProcessor import POMMProcessor

DATA_DIR: Path = Path(__file__).absolute().parent.parent / "test_data" / "tuflow"

with open(DATA_DIR / "pomm_run_codes.json", "r", encoding="utf-8") as fp:
    RUN_CODES = json.load(fp)
with open(DATA_DIR / "pomm_processed_results.json", "r", encoding="utf-8") as fp:
    PROCESSED_RESULTS = json.load(fp)


def locate_file(name: str) -> Path:
    matches: list[Path] = list(DATA_DIR.glob(f"tutorials/*/results/{name}"))
    if not matches:
        raise FileNotFoundError(name)
    return matches[0]


def test_tuflow_string_parser_examples() -> None:
    for entry in RUN_CODES:
        file_name = entry["file_name"]
        expected = entry["expected"]
        file_path = locate_file(file_name)
        parser = TuflowStringParser(file_path)
        assert parser.run_code_parts == expected["run_code_parts"]
        assert (parser.aep.text_repr if parser.aep else None) == expected["aep"]
        assert (parser.duration.text_repr if parser.duration else None) == expected["duration"]
        assert (parser.tp.text_repr if parser.tp else None) == expected["tp"]
        assert parser.trim_run_code == expected["trim_run_code"]


def test_pomm_processor_outputs() -> None:
    for file_name, expected_df in PROCESSED_RESULTS.items():
        file_path: Path = locate_file(file_name)
        processor = POMMProcessor(file_path)
        processor.process()
        result: dict[Hashable, Any] = processor.df.to_dict(orient="list")  # pyright: ignore[reportUnknownMemberType]
        assert result == expected_df
