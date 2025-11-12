from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas import DataFrame
import pytest

import ryan_library.functions.misc_functions as misc_functions
from ryan_library.functions.tuflow import pomm_combine as pomm_module


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        return cls(2024, 1, 2, 3, 4)


class _DummyResults:
    def __init__(self, df: pd.DataFrame, processors: list[object] | None = None) -> None:
        self._df: DataFrame = df
        self.processors: list[object] = processors if processors is not None else [object()]

    def pomm_combine(self) -> pd.DataFrame:
        return self._df


@pytest.fixture(autouse=True)
def _fixed_time(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setattr(misc_functions, "datetime", _FixedDateTime)
    return "20240102-0304"


@pytest.fixture(autouse=True)
def _stub_parquet(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_to_parquet(self, path, *args, **kwargs):
        Path(path).write_text("parquet", encoding="utf-8")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _fake_to_parquet, raising=False)


@pytest.fixture()
def temp_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _list_outputs(directory: Path) -> list[Path]:
    return sorted(directory.iterdir())


def test_export_results_excel_only(temp_cwd: Path) -> None:
    df = pd.DataFrame({"A": [1]})
    pomm_module.export_results(results=_DummyResults(df), export_mode="excel")

    outputs: list[Path] = _list_outputs(temp_cwd)
    assert len(outputs) == 1
    assert outputs[0].suffix == ".xlsx"
    assert outputs[0].name == "20240102-0304_combined_POMM.xlsx"


def test_export_results_parquet_only(temp_cwd: Path) -> None:
    df = pd.DataFrame({"A": [1]})
    pomm_module.export_results(results=_DummyResults(df), export_mode="parquet")

    outputs: list[Path] = _list_outputs(temp_cwd)
    assert len(outputs) == 1
    assert outputs[0].suffixes == [".parquet", ".gzip"]
    assert outputs[0].name == "20240102-0304_combined_POMM_combined_POMM.parquet.gzip"


def test_export_results_both_formats(temp_cwd: Path) -> None:
    df = pd.DataFrame({"B": [1]})
    pomm_module.export_results(results=_DummyResults(df), export_mode="both")

    outputs = _list_outputs(temp_cwd)
    assert len(outputs) == 2
    assert {p.suffix for p in outputs} == {".xlsx", ".gzip"}


def test_export_results_no_processors_produces_no_files(temp_cwd: Path) -> None:
    df = pd.DataFrame({"B": [1]})
    results = _DummyResults(df, processors=[])
    pomm_module.export_results(results=results, export_mode="excel")

    assert list(temp_cwd.iterdir()) == []
