"""Installer dry-run contract: exact parser, GIS bootstrap and recovery modes."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest


def _installer() -> ModuleType:
    path = Path(__file__).absolute().parents[2] / "repo-scripts" / "install_latest_wheel.py"
    spec = importlib.util.spec_from_file_location("install_latest_wheel_for_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Direct file loading does not automatically add repo-scripts to sys.path.
    sys.path.insert(0, str(path.parent))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(path.parent))
    return module


def test_normal_commands_pin_accepted_parser_and_preserve_gis_wheels() -> None:
    installer = _installer()
    wheel = Path("dist/ryan_tools-26.10.31.1-py3-none-any.whl")
    commands = installer._pip_commands(wheel, False, skip_qgis_mcp=True)
    pin = str(installer.WHEEL_PATH)
    assert Path(pin).is_absolute()
    assert Path(pin).name == "pytuflow-1.3.0-py3-none-any.whl"
    assert Path(pin).is_file()
    assert commands[0] == installer._gis_dependency_command()
    assert all("--ignore-requires-python" in command for command in commands)
    assert installer.GIS_WHEEL_INDEX == "https://gisidx.github.io/gwi"
    assert len(commands) == 3
    assert commands[-2][-3:] == ["--force-reinstall", "--no-deps", pin]
    assert all(
        "--no-deps" not in command and "--force-reinstall" not in command for command in (commands[0], commands[-1])
    )
    assert commands[-1][-2:] == [f"{wheel}[mcp]", pin]
    assert "--only-binary=fiona" in commands[-1]
    assert "--only-binary=rasterio" in commands[-1]
    assert "--only-binary=gdal" in commands[-1]


def test_legacy_migration_still_reinstalls_the_package_after_parser_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installer = _installer()

    def installed_legacy(_name: str) -> object:
        return object()

    monkeypatch.setattr(installer, "distribution", installed_legacy)
    commands = installer.normal_install_commands("ryan-tools[mcp]", skip_qgis_mcp=True)
    verification = ["verify-replacement"]
    migrated = installer.migration_commands(commands, verification_command=verification)
    assert migrated[0] == verification
    assert migrated[2] == commands[-2]
    assert migrated[-2][-3:] == ["uninstall", "--yes", "ryan_functions"]
    assert "ryan-tools[mcp]" in migrated[-1]
    assert "--force-reinstall" in migrated[-1]
    assert "--no-deps" not in migrated[-1]


def test_no_dependency_recovery_does_not_claim_parser_installation() -> None:
    installer = _installer()
    commands = installer._pip_commands(Path("dist/local.whl"), True)
    assert len(commands) == 1
    assert "--ignore-requires-python" in commands[0]
    assert "--no-deps" in commands[0]
    assert "pytuflow" not in " ".join(commands[0]).casefold()


def test_dry_run_has_public_guard_and_pip_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    installer = _installer()

    def latest(*, dist_dir: Path) -> Path:
        assert dist_dir.name == "dist"
        return tmp_path / "ryan_tools.whl"

    monkeypatch.setattr(installer, "_latest_wheel", latest)
    assert installer.main(["--dry-run", "--skip-qgis-mcp"]) == 0
    output = capsys.readouterr().out
    assert "gisidx.github.io/gwi" in output
    assert "887534ea8c15b80d185a38a7ead1d42ab42a7167" in output
    assert "pip check" in output
    assert "require_validation_api" in output
    assert "vendor_pytuflow_wheel.py" in output
    assert "--installed" in output
    assert "no packages or validation capabilities were checked" in output


def test_dependency_only_dry_run_does_not_require_wheel(
    capsys: pytest.CaptureFixture[str],
) -> None:
    installer = _installer()
    assert installer.main(["--dependencies-only", "--dry-run"]) == 0
    printed = capsys.readouterr().out
    assert "gisidx.github.io/gwi" in printed
    assert "--ignore-requires-python" in printed
    assert "require_validation_api" not in printed


def test_force_reinstall_dry_run_skips_false_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    installer = _installer()

    def latest(*, dist_dir: Path) -> Path:
        assert dist_dir.name == "dist"
        return tmp_path / "ryan_tools.whl"

    monkeypatch.setattr(installer, "_latest_wheel", latest)
    assert installer.main(["--dry-run", "--force-reinstall", "--skip-qgis-mcp"]) == 0
    printed = capsys.readouterr().out
    assert "--no-deps" in printed
    assert "require_validation_api" not in printed


def test_rejects_nonaccepted_same_version_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    installer = _installer()
    candidate = tmp_path / "pytuflow-requirement.txt"
    candidate.write_text(
        "pytuflow @ https://github.com/Chain-Frost/PyTuflow/archive/8012b8a530616a8e10dfe96a53189f8585ec2805.zip\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(installer, "PYTUFLOW_REQUIREMENT_FILE", candidate)
    with pytest.raises(ValueError, match="Invalid PyTUFLOW source"):
        installer._pytuflow_requirement()


def test_batch_installers_allow_older_python_3_and_interpreter_override() -> None:
    root = Path(__file__).absolute().parents[2]
    for name in ("install-from-pypi.bat", "install-local-wheel.bat"):
        batch = (root / name).read_text(encoding="utf-8")
        assert "RYAN_TOOLS_PYTHON" in batch
        assert "sys.version_info.major == 3" in batch
        assert "sys.version_info >= (3, 14)" not in batch
        assert "sys.version_info[1] >= 14" not in batch


def test_accepted_wheel_is_verified_and_missing_or_corrupt_files_fail(tmp_path: Path) -> None:
    installer = _installer()
    installer.verify_wheel()
    missing = tmp_path / "missing.whl"
    with pytest.raises(FileNotFoundError, match="missing"):
        installer.verify_wheel(missing, manifest=False)
    corrupt = tmp_path / "pytuflow-1.3.0-py3-none-any.whl"
    corrupt.write_bytes(b"bad")
    with pytest.raises(ValueError, match="corrupt"):
        installer.verify_wheel(corrupt, manifest=False)


def test_parser_install_is_independent_of_cwd_and_supports_spaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installer = _installer()
    outside = tmp_path / "outside working folder"
    outside.mkdir()
    monkeypatch.chdir(outside)
    commands = installer.normal_install_commands("ryan-tools[mcp]", skip_qgis_mcp=True)
    assert commands[1][-1] == str(installer.WHEEL_PATH)
    assert commands[2][-1] == str(installer.WHEEL_PATH)
    assert Path(commands[1][-1]).is_file()
    copied = tmp_path / "folder with spaces" / installer.WHEEL_PATH.name
    copied.parent.mkdir(parents=True)
    copied.write_bytes(installer.WHEEL_PATH.read_bytes())
    installer.verify_wheel(copied, manifest=False)
