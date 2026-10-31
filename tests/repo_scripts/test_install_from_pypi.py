"""Verify the end-user installation route and same-version archive replacement."""

from __future__ import annotations

import base64
import importlib.util
import io
import os
import subprocess
import sys
import sysconfig
import zipfile
from argparse import Namespace
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import ModuleType

import pytest


def _installer(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    path = Path(__file__).absolute().parents[2] / "repo-scripts" / "install_from_pypi.py"
    monkeypatch.setattr(sys, "path", [str(path.parent), *sys.path])
    spec = importlib.util.spec_from_file_location("install_from_pypi_for_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def absent_distribution(_name: str) -> object:
        raise PackageNotFoundError

    monkeypatch.setitem(module.migration_commands.__globals__, "distribution", absent_distribution)
    monkeypatch.setattr(module, "disable_bytecode_for_environment", lambda: False)
    return module


def _stage(command: list[str]) -> str:
    if "require_validation_api" in command[-1]:
        return "capabilities"
    if command[-2:] == ["pip", "check"]:
        return "dependencies"
    if "ryan-tools[mcp]" in command:
        return "package"
    return "bootstrap"


def test_pypi_route_verifies_before_codex_setup(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    installer = _installer(monkeypatch)
    stages: list[str] = []
    calls: list[list[str]] = []

    def successful_command(command: list[str], *, check: bool) -> subprocess.CompletedProcess[bytes]:
        assert check is False
        stages.append(_stage(command))
        calls.append(command)
        return subprocess.CompletedProcess(command, 0)

    def configure_codex(_args: Namespace, *, disable_bytecode: bool) -> int:
        assert disable_bytecode is False
        stages.append("codex")
        return 0

    monkeypatch.setattr(subprocess, "run", successful_command)
    monkeypatch.setattr(installer, "_configure_codex", configure_codex)
    assert installer.main(["--skip-qgis-mcp"]) == 0
    assert stages == ["bootstrap", "package", "dependencies", "capabilities", "codex"]
    assert all(command[0] == sys.executable for command in calls)
    assert all("--no-deps" not in command for command in calls)
    assert installer.__file__ is not None
    requirement = Path(installer.__file__).with_name("pytuflow-requirement.txt").read_text(encoding="utf-8").strip()
    assert calls[1][-2:] == ["ryan-tools[mcp]", requirement]
    assert "public PyTUFLOW capabilities verified" in capsys.readouterr().out


@pytest.mark.parametrize("failed_stage", ["bootstrap", "package", "dependencies", "capabilities"])
def test_pypi_route_stops_on_install_or_verification_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], failed_stage: str
) -> None:
    installer = _installer(monkeypatch)
    stages: list[str] = []

    def command_result(command: list[str], *, check: bool) -> subprocess.CompletedProcess[bytes]:
        assert check is False
        stage = _stage(command)
        stages.append(stage)
        return subprocess.CompletedProcess(command, 17 if stage == failed_stage else 0)

    def unexpected_codex(_args: Namespace, *, disable_bytecode: bool) -> int:
        del disable_bytecode
        pytest.fail("Codex setup must not run after an installation or verification failure")

    monkeypatch.setattr(subprocess, "run", command_result)
    monkeypatch.setattr(installer, "_configure_codex", unexpected_codex)
    assert installer.main(["--skip-qgis-mcp"]) == 17
    assert stages[-1] == failed_stage
    output = capsys.readouterr()
    assert "status 17" in output.err
    assert "installation complete" not in output.out.casefold()


def test_pypi_dry_run_prints_checks_without_running_them(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    installer = _installer(monkeypatch)

    def unexpected_command(command: list[str], *, check: bool) -> subprocess.CompletedProcess[bytes]:
        del command, check
        pytest.fail("A dry run must not execute installation or verification commands")

    monkeypatch.setattr(subprocess, "run", unexpected_command)
    assert installer.main(["--dry-run", "--skip-codex", "--skip-qgis-mcp", "--disable-bytecode"]) == 0
    output = capsys.readouterr().out
    assert "05e35476c02d6d8ed17c83caf1cb0d185ce51d1b" in output
    assert "-B -m pip check" in output
    assert "-B -c" in output
    assert "require_validation_api" in output
    assert "no packages or validation capabilities were checked" in output


def _wheel_bytes(name: str, version: str, source: str, *, dependency: str = "") -> bytes:
    metadata = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
    if dependency:
        metadata += f"Requires-Dist: {dependency}\n"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as wheel:
        wheel.writestr(f"{name}.py", source)
        wheel.writestr(f"{name}-{version}.dist-info/METADATA", metadata)
        wheel.writestr(
            f"{name}-{version}.dist-info/WHEEL",
            "Wheel-Version: 1.0\nGenerator: installer-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        wheel.writestr(f"{name}-{version}.dist-info/RECORD", "")
    return buffer.getvalue()


def test_explicit_archive_replaces_same_version_with_dependency_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise pip offline using only disposable packages under a temporary prefix."""
    installer = _installer(monkeypatch)
    name = "ryan_installer_pin_probe"
    version = "1.2.7"
    prefix = tmp_path / "prefix"
    site = Path(sysconfig.get_path("purelib", vars={"base": str(prefix), "platbase": str(prefix)}))
    metadata = site / f"{name}-{version}.dist-info"
    metadata.mkdir(parents=True)
    (site / f"{name}.py").write_text("ACCEPTED = False\n", encoding="utf-8")
    (metadata / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n", encoding="utf-8")
    (metadata / "RECORD").write_text(
        f"{name}.py,,\n{name}-{version}.dist-info/METADATA,,\n{name}-{version}.dist-info/RECORD,,\n",
        encoding="utf-8",
    )

    archive = tmp_path / "accepted.zip"
    encoded_wheel = base64.b64encode(_wheel_bytes(name, version, "ACCEPTED = True\n")).decode("ascii")
    backend = (
        "import base64\nfrom pathlib import Path\n"
        "def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):\n"
        f"    filename = {f'{name}-{version}-py3-none-any.whl'!r}\n"
        f"    Path(wheel_directory, filename).write_bytes(base64.b64decode({encoded_wheel!r}))\n"
        "    return filename\n"
    )
    with zipfile.ZipFile(archive, "w") as source:
        source.writestr(
            "accepted/pyproject.toml",
            '[build-system]\nrequires = []\nbuild-backend = "fixture_backend"\nbackend-path = ["."]\n',
        )
        source.writestr("accepted/fixture_backend.py", backend)
    consumer_name = "ryan_installer_consumer_probe"
    consumer = tmp_path / f"{consumer_name}-1.0-py3-none-any.whl"
    consumer.write_bytes(_wheel_bytes(consumer_name, "1.0", "", dependency=f"{name}>=1,<2"))

    # Keep the real installer options; replace only its two package inputs with
    # local fixtures. The consumer deliberately permits the incompatible version.
    command = installer.pip_commands(skip_qgis_mcp=True)[-1]
    command[-2:] = [str(consumer), f"{name} @ {archive.as_uri()}"]
    assert "--no-deps" not in command
    assert "--force-reinstall" not in command
    command.extend(
        ["--prefix", str(prefix), "--no-index", "--no-build-isolation", "--no-cache-dir", "--root-user-action=ignore"]
    )
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(site), os.environ.get("PYTHONPATH", "")]),
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
    }
    result = subprocess.run(command, env=environment, capture_output=True, text=True, check=False, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (site / f"{name}.py").read_text(encoding="utf-8") == "ACCEPTED = True\n"
    assert (site / f"{consumer_name}.py").is_file()
