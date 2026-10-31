"""Installer dry-run contract: exact parser, GIS bootstrap and recovery modes."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _installer() -> ModuleType:
    path = Path(__file__).absolute().parents[2] / "repo-scripts" / "install_latest_wheel.py"
    spec = importlib.util.spec_from_file_location("install_latest_wheel_for_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_normal_commands_pin_accepted_parser_and_preserve_gis_wheels() -> None:
    installer = _installer()
    wheel = Path("dist/ryan_tools-26.10.31.1-py3-none-any.whl")
    commands = installer._pip_commands(wheel, False, skip_qgis_mcp=True)
    pin = "pytuflow @ https://github.com/Chain-Frost/PyTuflow/archive/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b.zip"
    assert commands[0] == installer._gis_dependency_command()
    assert installer.GIS_WHEEL_INDEX == "https://gisidx.github.io/gwi"
    assert commands[1][-1] == pin
    assert "--force-reinstall" in commands[1] and "--no-deps" in commands[1]
    assert commands[-1][-2:] == [f"{wheel}[mcp]", pin]
    assert "--only-binary=fiona" in commands[-1]
    assert "--only-binary=rasterio" in commands[-1]
    assert "--only-binary=gdal" in commands[-1]


def test_no_dependency_recovery_does_not_claim_parser_installation() -> None:
    installer = _installer()
    commands = installer._pip_commands(Path("dist/local.whl"), True)
    assert len(commands) == 1
    assert "--no-deps" in commands[0]
    assert "pytuflow" not in " ".join(commands[0]).casefold()


def test_dry_run_has_public_guard_and_pip_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    installer = _installer()
    monkeypatch.setattr(installer, "_latest_wheel", lambda _dist_dir: tmp_path / "ryan_tools.whl")
    assert installer.main(["--dry-run", "--skip-qgis-mcp"]) == 0
    output = capsys.readouterr().out
    assert "gisidx.github.io/gwi" in output
    assert "05e35476c02d6d8ed17c83caf1cb0d185ce51d1b" in output
    assert "pip check" in output
    assert "require_validation_api" in output
    assert "no packages or validation capabilities were checked" in output


def test_dependency_only_dry_run_does_not_require_wheel(
    capsys: pytest.CaptureFixture[str],
) -> None:
    installer = _installer()
    assert installer.main(["--dependencies-only", "--dry-run"]) == 0
    printed = capsys.readouterr().out
    assert "gisidx.github.io/gwi" in printed
    assert "require_validation_api" not in printed


def test_force_reinstall_dry_run_skips_false_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    installer = _installer()
    monkeypatch.setattr(installer, "_latest_wheel", lambda dist_dir: tmp_path / "ryan_tools.whl")
    assert installer.main(["--dry-run", "--force-reinstall", "--skip-qgis-mcp"]) == 0
    printed = capsys.readouterr().out
    assert "--no-deps" in printed
    assert "require_validation_api" not in printed
