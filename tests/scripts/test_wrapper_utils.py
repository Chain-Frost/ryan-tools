"""Tests for the active wrapper utility functions."""

import json
import os
import sys
from argparse import ArgumentParser, Namespace
from collections.abc import Callable
from io import StringIO
from pathlib import Path

import colorama
from typing import cast
from unittest.mock import patch

import pytest

from ryan_library.functions import wrapper_utils


def test_change_working_directory_success(tmp_path: Path) -> None:
    original_directory = Path.cwd()
    try:
        assert wrapper_utils.change_working_directory(tmp_path) is True
        assert Path.cwd() == tmp_path
    finally:
        os.chdir(original_directory)


def test_change_working_directory_failure():
    with patch("os.chdir", side_effect=OSError("Error")):
        assert wrapper_utils.change_working_directory(Path("invalid")) is False


def test_print_library_version_found(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("ryan_library.functions.wrapper_utils.version", return_value="1.0.0"):
        wrapper_utils.print_library_version("pkg")
        captured = capsys.readouterr()
        assert "pkg version: 1.0.0" in captured.out


def test_print_library_version_not_found(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("ryan_library.functions.wrapper_utils.version", side_effect=ImportError):
        # wrapper_utils catches PackageNotFoundError which inherits from ImportError in recent python?
        # Actually PackageNotFoundError is from importlib.metadata
        from importlib.metadata import PackageNotFoundError

        with patch("ryan_library.functions.wrapper_utils.version", side_effect=PackageNotFoundError):
            wrapper_utils.print_library_version("pkg")
            captured = capsys.readouterr()
            assert "pkg version: unknown" in captured.out


def test_add_common_cli_arguments():
    parser = ArgumentParser()
    wrapper_utils.add_common_cli_arguments(parser)

    # Check if arguments were added
    args = parser.parse_args(["--console-log-level", "DEBUG", "--locations", "L1", "L2", "--working-directory", "."])
    assert args.console_log_level == "DEBUG"
    assert args.locations == ["L1", "L2"]
    assert args.working_directory == Path()


def test_parse_common_cli_arguments():
    args = Namespace(console_log_level="INFO", locations=[" L1 ", "L2"], working_directory=Path())
    options = wrapper_utils.parse_common_cli_arguments(args)

    assert options.console_log_level == "INFO"
    assert options.locations_to_include == ("L1", "L2")
    assert options.working_directory == Path()


def test_coerce_locations_argument() -> None:
    coerce_locations = cast(
        Callable[[list[str] | None], tuple[str, ...] | None],
        getattr(wrapper_utils, "_coerce_locations_argument"),  # noqa: B009 - intentional private-helper test
    )
    assert coerce_locations(None) is None
    assert coerce_locations([]) is None
    assert coerce_locations([""]) is None
    assert coerce_locations([" a ", "b"]) == ("a", "b")


def test_windows_console_pause_reads_key_without_cmd(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The Windows keypress prompt must not spawn cmd.exe from UNC directories."""
    from types import SimpleNamespace

    received: list[str] = []

    def fake_getwch() -> str:
        received.append("key")
        return "x"

    monkeypatch.setitem(sys.modules, "msvcrt", SimpleNamespace(getwch=fake_getwch))
    pause_windows_console = cast(
        Callable[[], None],
        getattr(wrapper_utils, "_pause_windows_console"),  # noqa: B009 - intentional private-helper test
    )
    pause_windows_console()
    assert received == ["key"]
    assert capsys.readouterr().out == "Press any key to continue . . .\n"


def test_change_working_directory_prints_configured_spelling(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Preserve the caller's path form even when Windows exposes a UNC alias."""
    requested = tmp_path / "mapped_alias"

    def ignore_chdir(_path: str | os.PathLike[str]) -> None:
        pass

    monkeypatch.setattr(wrapper_utils.os, "chdir", ignore_chdir)
    assert wrapper_utils.change_working_directory(requested)
    assert capsys.readouterr().out == f"Current Working Directory: {requested.absolute()}\n"


@pytest.mark.parametrize("version_name", ["WRAPPER_VERSION", "SCRIPT_VERSION"])
@pytest.mark.parametrize(
    ("current", "bundled", "filename", "source", "warn"),
    [
        ("2026-11-07.2", "2026-11-07.10", "runner.py", None, True),
        ("2026-11-07.10", "2026-11-07.2", "runner.py", None, False),
        ("2026-11-07.2", "2026-11-07.2", "runner.py", None, False),
        ("2026-10-31.9", "2026-11-01.1", "runner.py", None, True),
        ("custom", "2026-11-07.2", "runner.py", None, False),
        ("2026-02-30.1", "2026-11-07.2", "runner.py", None, False),
        ("2026-11-07.1", "custom", "runner.py", None, False),
        ("2026-11-07.1", "2026-11-07.2", "renamed.py", "tools/runner.py", True),
        ("2026-11-07.1", "2026-11-07.2", "renamed.py", None, False),
        ("2026-11-07.1", "2026-11-07.2", "runner.py", "../runner.py", False),
    ],
)
def test_wrapper_banner_compares_installed_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    current: str,
    bundled: str,
    filename: str,
    source: str | None,
    warn: bool,
    version_name: str,
) -> None:
    resource = tmp_path / "resources"
    resource.mkdir()
    (resource / "installed_scripts.json").write_text(
        json.dumps({"schema_version": 1, "installed_files": ["tools/runner.py"]}), encoding="utf-8"
    )
    original = tmp_path / "ryan-scripts/tools/runner.py"
    original.parent.mkdir(parents=True)
    original.write_text(
        f'{version_name}: str = "{bundled}"\nraise RuntimeError("Must not execute bundled scripts")\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(wrapper_utils, "__file__", str(tmp_path / "ryan_library/functions/wrapper_utils.py"))

    def resources(_package: str) -> Path:
        return resource

    def package_version(**_kwargs: str) -> str:
        return "26.11.7.5"

    monkeypatch.setattr(wrapper_utils, "files", resources)
    monkeypatch.setattr(wrapper_utils, "version", package_version)
    wrapper_utils.print_wrapper_banner(
        wrapper_file=tmp_path / filename, wrapper_version=current, wrapper_source=source, leading_blank_line=True
    )
    output = capsys.readouterr().out
    assert output.startswith("\nWrapper:")
    assert "ryan-tools version: 26.11.7.5" in output
    assert ("is older than" in output) is warn
    if warn:
        assert f"tools/runner.py ({bundled})" in output
        assert "transfer your settings" in output


@pytest.mark.parametrize("problem", ["missing", "invalid_manifest", "ambiguous", "invalid_source"])
def test_wrapper_banner_unavailable_comparison_does_not_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], problem: str
) -> None:
    manifest = tmp_path / "installed_scripts.json"
    if problem == "invalid_manifest":
        manifest.write_text("broken json", encoding="utf-8")
    elif problem != "missing":
        entries = ["tools/runner.py", "other/runner.py"] if problem == "ambiguous" else ["tools/runner.py"]
        manifest.write_text(json.dumps({"schema_version": 1, "installed_files": entries}), encoding="utf-8")
    original = tmp_path / "ryan-scripts/tools/runner.py"
    original.parent.mkdir(parents=True)
    original.write_text("invalid Python!", encoding="utf-8")
    monkeypatch.setattr(wrapper_utils, "__file__", str(tmp_path / "ryan_library/functions/wrapper_utils.py"))

    def resources(_package: str) -> Path:
        return tmp_path

    monkeypatch.setattr(wrapper_utils, "files", resources)
    wrapper_utils.print_wrapper_banner(wrapper_file=Path("runner.py"), wrapper_version="2026-11-07.1")
    assert "is older than" not in capsys.readouterr().out


def test_wrapper_warning_is_yellow_on_tty_and_plain_when_redirected(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeTerminal(StringIO):
        def isatty(self) -> bool:
            return True

    plain = StringIO()
    monkeypatch.setattr(wrapper_utils.sys, "stdout", plain)
    wrapper_utils._print_wrapper_warning("WARNING: upgrade this wrapper")  # pyright: ignore[reportPrivateUsage]
    assert plain.getvalue() == "WARNING: upgrade this wrapper\n"

    terminal = FakeTerminal()
    monkeypatch.setattr(wrapper_utils.sys, "stdout", terminal)
    monkeypatch.setattr(colorama, "just_fix_windows_console", lambda: None)
    wrapper_utils._print_wrapper_warning("WARNING: upgrade this wrapper")  # pyright: ignore[reportPrivateUsage]
    assert terminal.getvalue() == f"{colorama.Fore.YELLOW}WARNING:{colorama.Style.RESET_ALL} upgrade this wrapper\n"


def test_legacy_banner_calls_warn_and_continue(capsys: pytest.CaptureFixture[str]) -> None:
    wrapper_utils.print_wrapper_banner()
    output = capsys.readouterr().out
    assert "version unknown" in output
    assert "outdated banner interface" in output
    assert "Execution will continue" in output

    wrapper_utils.print_wrapper_banner(wrapper_file=Path("old-copy.py"), wrapper_version="2026-11-07.1")
    assert "outdated banner interface" in capsys.readouterr().out


def test_versioned_installed_scripts_keep_identity_after_renaming() -> None:
    """Every versioned installed entry point passes its original script identity."""
    import ast

    from ryan_library.mcp.installed_scripts import available_wrappers

    root = Path(__file__).absolute().parents[2] / "ryan-scripts"
    for source in available_wrappers():
        if not source.endswith(".py"):
            continue
        tree = ast.parse((root / source).read_text(encoding="utf-8"))
        versioned = any(
            isinstance(node, ast.Name) and node.id in {"WRAPPER_VERSION", "SCRIPT_VERSION"} for node in ast.walk(tree)
        )
        if not versioned:
            continue
        banners = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print_wrapper_banner"
        ]
        assert banners, source
        for banner in banners:
            identities = [keyword.value for keyword in banner.keywords if keyword.arg == "wrapper_source"]
            assert len(identities) == 1, source
            identity = identities[0]
            assert isinstance(identity, ast.Constant), source
            assert identity.value == source, source


@pytest.mark.parametrize(
    "source",
    [
        "TUFLOW-python/gis_processing/rename_geopackage_layer_to_filename.py",
        "TUFLOW-python/po_and_timeseries/generate_2d_po_label_list.py",
        "docx-python/word_style_audit.py",
        "RORB-python/hydrograph_extract_rorb_csv.py",
    ],
)
def test_standalone_scripts_tolerate_an_older_installed_banner(source: str, monkeypatch: pytest.MonkeyPatch) -> None:
    import runpy
    from types import ModuleType

    older = ModuleType("ryan_library.functions.wrapper_utils")

    def old_banner(*, wrapper_file: Path, wrapper_version: str) -> None:
        pytest.fail(f"Standalone script called an incompatible older banner: {wrapper_file}, {wrapper_version}")

    vars(older)["print_wrapper_banner"] = old_banner
    monkeypatch.setitem(sys.modules, older.__name__, older)
    script = Path(__file__).absolute().parents[2] / "ryan-scripts" / source
    namespace = runpy.run_path(str(script))
    assert namespace["print_wrapper_banner"] is None
