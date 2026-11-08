"""Fail-closed acceptance checks for the temporary wheel distribution boundary."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest


def _vendor() -> ModuleType:
    path = Path(__file__).absolute().parents[2] / "repo-scripts" / "vendor_pytuflow_wheel.py"
    spec = importlib.util.spec_from_file_location("vendor_pytuflow_for_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_committed_wheel_and_manifest_have_accepted_bytes() -> None:
    vendor = _vendor()
    vendor.verify_wheel()
    assert vendor.WHEEL_PATH.is_file()
    assert vendor.WHEEL_PATH.stat().st_size == 976_447
    manifest = json.loads(vendor.MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest["source_commit"] == vendor.SOURCE_COMMIT
    assert manifest["wheel_sha256"] == vendor.WHEEL_SHA256


def test_fail_closed_on_missing_or_corrupt_wheel(tmp_path: Path) -> None:
    vendor = _vendor()
    with pytest.raises(FileNotFoundError):
        vendor.verify_wheel(tmp_path / "absent.whl", manifest=False)
    invalid = tmp_path / "pytuflow-1.3.0-py3-none-any.whl"
    invalid.write_bytes(b"not an accepted wheel")
    with pytest.raises(ValueError, match="corrupt"):
        vendor.verify_wheel(invalid, manifest=False)


def test_accepted_bytes_can_be_verified_in_a_path_with_spaces(tmp_path: Path) -> None:
    vendor = _vendor()
    alternate = tmp_path / "folder with spaces" / vendor.WHEEL_NAME
    alternate.parent.mkdir()
    alternate.write_bytes(vendor.WHEEL_PATH.read_bytes())
    vendor.verify_wheel(alternate, manifest=False)


def test_installed_provenance_requires_matching_archive_url_and_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    vendor = _vendor()

    class FakeDistribution:
        version = "1.3.0"

        def read_text(self, _name: str) -> str:
            return json.dumps(
                {
                    "url": vendor.WHEEL_PATH.as_uri(),
                    "archive_info": {"hash": f"sha256={vendor.WHEEL_SHA256}"},
                }
            )

    def accepted_distribution(_name: str) -> FakeDistribution:
        return FakeDistribution()

    monkeypatch.setattr(vendor.metadata, "distribution", accepted_distribution)
    vendor.verify_installed()

    class WrongDistribution(FakeDistribution):
        def read_text(self, _name: str) -> str:
            return json.dumps(
                {
                    "url": vendor.WHEEL_PATH.as_uri(),
                    "archive_info": {"hash": "sha256=00"},
                }
            )

    def incorrect_distribution(_name: str) -> WrongDistribution:
        return WrongDistribution()

    monkeypatch.setattr(vendor.metadata, "distribution", incorrect_distribution)
    with pytest.raises(ValueError, match="digest"):
        vendor.verify_installed()
