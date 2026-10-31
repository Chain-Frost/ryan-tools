"""Capability diagnostics do not trust PyTUFLOW's version alone."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

from ryan_library.functions.tuflow import pytuflow_compatibility as compatibility


def _installed(tmp_path: Path, *, url: str | None = None) -> SimpleNamespace:
    root = tmp_path / "site-packages"
    return SimpleNamespace(
        version="1.2.7",
        locate_file=lambda _name: root / "pytuflow",
        read_text=lambda _name: json.dumps({"url": url}) if url else None,
    )


def test_official_same_version_without_batch_api_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "site-packages" / "pytuflow" / "__init__.py"
    monkeypatch.setattr(compatibility.metadata, "distribution", lambda _name: _installed(tmp_path))

    def importer(name: str) -> object:
        if name == "pytuflow":
            return SimpleNamespace(__file__=str(root), __version__="1.2.7")
        message = "No module named 'pytuflow.batch_validation'"
        raise ModuleNotFoundError(message)

    monkeypatch.setattr(compatibility.importlib, "import_module", importer)
    status = compatibility.check_validation_capabilities()
    assert not status.available
    assert status.version == "1.2.7"
    assert "public validation import failed" in status.problem
    with pytest.raises(compatibility.PyTuflowCompatibilityError, match="05e35476"):
        compatibility.require_validation_api()


def test_shadowing_checkout_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compatibility.metadata, "distribution", lambda _name: _installed(tmp_path))
    monkeypatch.setattr(
        compatibility.importlib, "import_module",
        lambda _name: SimpleNamespace(__file__=str(tmp_path / "checkout" / "pytuflow" / "__init__.py")),
    )
    status = compatibility.check_validation_capabilities()
    assert not status.available
    assert "shadowed" in status.problem
    assert "checkout" in status.module_path


def test_missing_distribution_does_not_validate(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(_name: str) -> object:
        message = "pytuflow"
        raise compatibility.metadata.PackageNotFoundError(message)

    def import_failed(_name: str) -> object:
        message = "No module named pytuflow"
        raise ModuleNotFoundError(message)

    monkeypatch.setattr(compatibility.metadata, "distribution", missing)
    monkeypatch.setattr(compatibility.importlib, "import_module", import_failed)
    result = compatibility.check_validation_capabilities()
    assert not result.available
    assert result.version == "not installed"
    assert "Python:" in result.message


def test_broken_transitive_import_is_diagnosed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compatibility.metadata, "distribution", lambda _name: _installed(tmp_path))

    def importer(_name: str) -> object:
        message = "h5py DLL is missing"
        raise ImportError(message)

    monkeypatch.setattr(compatibility.importlib, "import_module", importer)
    result = compatibility.check_validation_capabilities()
    assert "h5py DLL" in result.message
    assert result.module_path == "not importable"


def test_correct_build_without_direct_url_metadata_is_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(compatibility.metadata, "distribution", lambda _name: _installed(tmp_path))
    class Options:
        __dataclass_fields__: ClassVar[dict[str, object]] = {
        "check_files": None, "check_gpkg_layers": None, "check_bc_csv_headers": None,
        "io_workers": None, "maximum_directory_entries": None,
        }

    options = Options

    class BatchResult:
        @property
        def passed(self) -> bool:
            return True

        @property
        def issues(self) -> tuple[()]:
            return ()

    def importer(name: str) -> object:
        if name == "pytuflow":
            return SimpleNamespace(__file__=str(tmp_path / "site-packages" / "pytuflow" / "__init__.py"))
        if name == "pytuflow.batch_validation":
            return SimpleNamespace(validate_batch_inputs=lambda *_a, **_k: object(), BatchValidationResult=BatchResult)
        return SimpleNamespace(ValidationOptions=options, ValidationFinding=type("ValidationFinding", (), {}))

    monkeypatch.setattr(compatibility.importlib, "import_module", importer)
    status = compatibility.check_validation_capabilities()
    assert status.available, status.message
    assert status.source == "not recorded"


def test_real_build_capability_smoke() -> None:
    pytest.importorskip("pytuflow.batch_validation")
    api = compatibility.require_validation_api()
    assert api.diagnostic.available
    assert callable(api.validate_batch_inputs)


def test_validation_helper_import_is_lightweight() -> None:
    result = subprocess.run(
        [
            sys.executable, "-c",
            ("import sys; from ryan_library.functions.tuflow.pytuflow_compatibility "
            "import check_validation_capabilities; assert 'pytuflow' not in sys.modules"),
        ],
        check=False, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
