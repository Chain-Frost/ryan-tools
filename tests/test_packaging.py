"""Focused tests for transactional wheel versioning and promotion."""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]


def _load_script(name: str) -> ModuleType:
    """Load one repository script whose parent directory is not importable."""
    path: Path = PROJECT_ROOT / "repo-scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        msg = f"Could not load {path}."
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build_library = _load_script("build_library")
verify_wheel = _load_script("verify_wheel")


def test_calendar_version_increments_same_day() -> None:
    assert build_library.next_calendar_version("26.9.13.1", dt.date(2026, 9, 13)) == "26.9.13.2"


def test_calendar_version_resets_revision_on_new_day() -> None:
    assert build_library.next_calendar_version("26.9.12.7", dt.date(2026, 9, 13)) == "26.9.13.1"


def test_calendar_version_rejects_clock_before_current_release() -> None:
    with pytest.raises(ValueError, match="refusing a version regression"):
        build_library.next_calendar_version("26.9.13.1", dt.date(2026, 9, 12))


@pytest.mark.parametrize("version", ["26.09.13.1", "2026.9.13.1", "26.2.30.1", "26.9.13.0"])
def test_calendar_version_rejects_invalid_or_non_normalized_values(version: str) -> None:
    with pytest.raises(ValueError, match=r"normalized|must be in range"):
        build_library.parse_calendar_version(version)


def test_explicit_version_must_move_forward() -> None:
    assert build_library.validate_explicit_version("26.9.13.1", "26.9.13.4") == "26.9.13.4"
    with pytest.raises(ValueError, match="must be newer"):
        build_library.validate_explicit_version("26.9.13.1", "26.9.13.1")


def test_replace_project_version_changes_only_project_section(tmp_path: Path) -> None:
    project = tmp_path / "pyproject.toml"
    project.write_text(
        '[project]\nname = "example"\nversion = "26.9.13.1"\n\n[tool.example]\nversion = "keep"\n',
        encoding="utf-8",
    )

    previous = build_library.replace_project_version(project, "26.9.13.2")

    assert previous == "26.9.13.1"
    assert 'version = "26.9.13.2"' in project.read_text(encoding="utf-8")
    assert 'version = "keep"' in project.read_text(encoding="utf-8")


def _prepare_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, str]:
    """Create and configure a minimal package workspace for a builder test."""
    project = tmp_path / "pyproject.toml"
    original = '[project]\nname = "ryan_functions"\nversion = "26.9.13.1"\n'
    project.write_text(original, encoding="utf-8")
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    monkeypatch.setattr(build_library, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(build_library, "PROJECT_PATH", project)
    monkeypatch.setattr(build_library, "DIST_DIR", dist_dir)
    return project, dist_dir, original


def test_failed_build_restores_version_and_retains_wheel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, dist_dir, original = _prepare_project(tmp_path, monkeypatch)
    previous_wheel = dist_dir / "ryan_functions-26.9.13.1-py3-none-any.whl"
    previous_wheel.write_bytes(b"previous wheel")

    def failed_build(_output_dir: Path) -> int:
        return 17

    monkeypatch.setattr(build_library, "_run_build", failed_build)

    status = build_library.main(["--skip-pip"], today=dt.date(2026, 9, 13))

    assert status == 17
    assert project.read_text(encoding="utf-8") == original
    assert previous_wheel.read_bytes() == b"previous wheel"


def test_failed_verification_restores_version_and_retains_wheel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, dist_dir, original = _prepare_project(tmp_path, monkeypatch)
    previous_wheel = dist_dir / "ryan_functions-26.9.13.1-py3-none-any.whl"
    previous_wheel.write_bytes(b"previous wheel")

    def successful_build(output_dir: Path) -> int:
        (output_dir / "ryan_functions-26.9.13.2-py3-none-any.whl").write_bytes(b"candidate")
        return 0

    def failed_verification(_wheel: Path) -> int:
        return 19

    monkeypatch.setattr(build_library, "_run_build", successful_build)
    monkeypatch.setattr(build_library, "_run_verification", failed_verification)

    status = build_library.main(["--skip-pip"], today=dt.date(2026, 9, 13))

    assert status == 19
    assert project.read_text(encoding="utf-8") == original
    assert previous_wheel.read_bytes() == b"previous wheel"


def test_no_bump_builds_current_version_without_changing_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, dist_dir, original = _prepare_project(tmp_path, monkeypatch)

    def successful_build(output_dir: Path) -> int:
        (output_dir / "ryan_functions-26.9.13.1-py3-none-any.whl").write_bytes(b"candidate")
        return 0

    def successful_verification(_wheel: Path) -> int:
        return 0

    monkeypatch.setattr(build_library, "_run_build", successful_build)
    monkeypatch.setattr(build_library, "_run_verification", successful_verification)

    status = build_library.main(["--no-bump", "--skip-pip"])

    assert status == 0
    assert project.read_text(encoding="utf-8") == original
    assert (dist_dir / "ryan_functions-26.9.13.1-py3-none-any.whl").read_bytes() == b"candidate"


def test_promote_wheel_keeps_only_new_project_distribution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "ryan_functions-26.9.13.1-py3-none-any.whl").write_bytes(b"old")
    (dist_dir / "ryan_functions-26.9.13.1.tar.gz").write_bytes(b"old source")
    unrelated = dist_dir / "other_package-1-py3-none-any.whl"
    unrelated.write_bytes(b"unrelated")
    staged = tmp_path / "ryan_functions-26.9.13.2-py3-none-any.whl"
    staged.write_bytes(b"new")
    monkeypatch.setattr(build_library, "DIST_DIR", dist_dir)

    promoted = build_library.promote_wheel(staged)

    assert promoted.read_bytes() == b"new"
    assert [path.name for path in dist_dir.glob("ryan_functions-*")] == [promoted.name]
    assert unrelated.read_bytes() == b"unrelated"


def test_retained_wheel_must_match_project_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "ryan_functions-26.9.13.1-py3-none-any.whl").write_bytes(b"stale")
    monkeypatch.setattr(verify_wheel, "DIST_DIR", dist_dir)

    with pytest.raises(ValueError, match=r"does not match project version 26\.9\.13\.2"):
        verify_wheel.retained_wheel("26.9.13.2")


def test_exactly_one_retained_project_wheel_is_required(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "ryan_functions-26.9.13.1-py3-none-any.whl").write_bytes(b"old")
    (dist_dir / "ryan_functions-26.9.13.2-py3-none-any.whl").write_bytes(b"current")
    monkeypatch.setattr(verify_wheel, "DIST_DIR", dist_dir)

    with pytest.raises(ValueError, match="exactly one retained project wheel under dist/, found 2"):
        verify_wheel.retained_wheel("26.9.13.2")
