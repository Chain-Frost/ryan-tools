"""Safety regressions for the standalone PO label exporter."""

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest


def _load() -> ModuleType:
    source = (
        Path(__file__).absolute().parents[3]
        / "ryan-scripts/TUFLOW-python/po_and_timeseries/generate_2d_po_label_list.py"
    )
    spec = importlib.util.spec_from_file_location("po_label_safety", source)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_existing_label_output_is_preserved(tmp_path: Path) -> None:
    module = _load()
    write = cast("Callable[..., None]", module.write_output_file)
    output = tmp_path / "labels.txt"
    output.write_text("user-owned content", encoding="utf-8")
    with pytest.raises(FileExistsError):
        write(output_path=output, contents="replacement")
    assert output.read_text(encoding="utf-8") == "user-owned content"


def test_missing_database_is_not_created_by_reader(tmp_path: Path) -> None:
    import sqlite3

    module = _load()
    read = cast("Callable[..., object]", module.load_label_entries)
    missing = tmp_path / "missing.gpkg"
    with pytest.raises(sqlite3.OperationalError):
        read(gpkg_path=missing)
    assert not missing.exists()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows UNC path spelling")
def test_readonly_database_uri_preserves_unc_path(monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlite3
    from unittest.mock import Mock

    module = _load()
    connect = Mock(side_effect=sqlite3.OperationalError("synthetic stop before network access"))
    monkeypatch.setattr(sqlite3, "connect", connect)
    read = cast("Callable[..., object]", module.load_label_entries)
    with pytest.raises(sqlite3.OperationalError, match="synthetic stop"):
        read(gpkg_path=Path("//server/share/labels #1.gpkg"))
    connect.assert_called_once_with(database="file:////server/share/labels%20%231.gpkg?mode=ro", uri=True)
