import json
from pathlib import Path
import pandas as pd
from ryan_library.processors.tuflow.POMMProcessor import POMMProcessor
from ryan_library.classes.tuflow_string_classes import TuflowStringParser

DATA_DIR = Path(__file__).resolve().parent.parent / "test_data" / "tuflow"

with open(DATA_DIR / "pomm_run_codes.json", "r", encoding="utf-8") as fp:
    RUN_CODES = json.load(fp)
with open(DATA_DIR / "pomm_processed_results.json", "r", encoding="utf-8") as fp:
    PROCESSED_RESULTS = json.load(fp)


def locate_file(name: str) -> Path:
    matches = list(DATA_DIR.glob(f"tutorials/*/results/{name}"))
    if not matches:
        raise FileNotFoundError(name)
    return matches[0]


def test_tuflow_string_parser_examples():
    for entry in RUN_CODES:
        file_name = entry["file_name"]
        expected = entry["expected"]
        file_path = locate_file(file_name)
        parser = TuflowStringParser(file_path)
        assert parser.run_code_parts == expected["run_code_parts"]
        assert (parser.aep.text_repr if parser.aep else None) == expected["aep"]
        assert (parser.duration.text_repr if parser.duration else None) == expected[
            "duration"
        ]
        assert (parser.tp.text_repr if parser.tp else None) == expected["tp"]
        assert parser.trim_run_code == expected["trim_run_code"]


def test_pomm_processor_outputs():
    for file_name, expected_df in PROCESSED_RESULTS.items():
        file_path = locate_file(file_name)
        processor = POMMProcessor(file_path)
        processor.process()
        result = processor.df.to_dict(orient="list")
        assert result == expected_df
