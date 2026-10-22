from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from batch_vector_clip import _promote_dataset, build_output_jobs, clip_vector
from split_vector_by_attribute import process_single_value


def test_output_jobs_reject_duplicate_stems(tmp_path: Path) -> None:
    first = tmp_path / "one" / "input.shp"
    second = tmp_path / "two" / "input.shp"
    extent = tmp_path / "extent.shp"

    with pytest.raises(ValueError, match="duplicate"):
        build_output_jobs([first, second], [extent], tmp_path / "outputs")


def test_clip_vector_promotes_temporary_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    input_path = tmp_path / "input.gpkg"
    extent_path = tmp_path / "extent.gpkg"
    output_path = tmp_path / "output.gpkg"

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        Path(command[1]).write_text("completed", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert clip_vector(
        input_path,
        extent_path,
        output_path,
        executable="ogr2ogr",
        overwrite=False,
        dry_run=False,
    )
    assert output_path.read_text(encoding="utf-8") == "completed"
    assert not list(tmp_path.glob(".*.tmp.gpkg"))


def test_split_vector_quotes_attribute_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured_where = ""

    def fake_translate(**kwargs: object) -> None:
        nonlocal captured_where
        captured_where = str(kwargs["where"])

    monkeypatch.setattr("split_vector_by_attribute.translate_vector_dataset", fake_translate)

    assert process_single_value(
        "O'Brien",
        tmp_path / "input.gpkg",
        tmp_path,
        "gpkg",
        ".gpkg",
        'Owner"Name',
        "assets",
    )
    assert captured_where == "\"Owner\"\"Name\" = 'O''Brien'"



def test_geopackage_overwrite_replaces_complete_staged_output(tmp_path: Path) -> None:
    """Single-file replacements should never require removing the old output first."""
    original = tmp_path / "output café.gpkg"
    original.write_bytes(b"old geopackage")
    staged = tmp_path / ".output cafe.stage.gpkg"
    staged.write_bytes(b"new geopackage")

    _promote_dataset(staged, original, overwrite=True)

    assert original.read_bytes() == b"new geopackage"
    assert not staged.exists()


def test_failed_geopackage_promotion_keeps_old_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A Windows lock or filesystem failure must not delete an existing GeoPackage."""
    original = tmp_path / "output.gpkg"
    original.write_bytes(b"old geopackage")
    staged = tmp_path / ".output.stage.gpkg"
    staged.write_bytes(b"new geopackage")

    def fail_replace(self: Path, destination: str | Path) -> Path:
        assert self == staged
        assert destination == original
        message = "injected promotion error"
        raise PermissionError(message)

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(PermissionError, match="injected promotion error"):
        _promote_dataset(staged, original, overwrite=True)

    assert original.read_bytes() == b"old geopackage"
    assert staged.read_bytes() == b"new geopackage"


def test_shapefile_success_replaces_sidecars_without_stale_originals(tmp_path: Path) -> None:
    """Sidecars absent from the new dataset must not survive from the old one."""
    original = tmp_path / "existing.shp"
    old_data = {".shp": b"old shapes", ".dbf": b"old table", ".shx": b"old index", ".prj": b"old projection"}
    for suffix, data in old_data.items():
        original.with_suffix(suffix).write_bytes(data)
    staged = tmp_path / ".existing.staged.shp"
    staged.with_suffix(".shp").write_bytes(b"new shapes")
    staged.with_suffix(".dbf").write_bytes(b"new table")
    staged.with_suffix(".shx").write_bytes(b"new index")

    _promote_dataset(staged, original, overwrite=True)

    assert original.read_bytes() == b"new shapes"
    assert original.with_suffix(".dbf").read_bytes() == b"new table"
    assert original.with_suffix(".shx").read_bytes() == b"new index"
    assert not original.with_suffix(".prj").exists()
    assert not list(tmp_path.glob(".existing.backup.*"))


def test_shapefile_mid_promotion_error_restores_every_original(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failure after one new sidecar was installed must roll back the full set."""
    original = tmp_path / "existing.shp"
    old_data = {".shp": b"old shapes", ".dbf": b"old table", ".shx": b"old index", ".prj": b"old projection"}
    for suffix, data in old_data.items():
        original.with_suffix(suffix).write_bytes(data)
    staged = tmp_path / ".existing.staged.shp"
    staged.with_suffix(".shp").write_bytes(b"new shapes")
    staged.with_suffix(".dbf").write_bytes(b"new table")
    staged.with_suffix(".shx").write_bytes(b"new index")

    original_replace = Path.replace

    def fail_after_partial_promotion(self: Path, destination: str | Path) -> Path:
        if self == staged.with_suffix(".shx") and destination == original.with_suffix(".shx"):
            message = "injected third-sidecar failure"
            raise PermissionError(message)
        return original_replace(self, destination)

    monkeypatch.setattr(Path, "replace", fail_after_partial_promotion)
    with pytest.raises(PermissionError, match="third-sidecar"):
        _promote_dataset(staged, original, overwrite=True)

    for suffix, data in old_data.items():
        assert original.with_suffix(suffix).read_bytes() == data
    assert not list(tmp_path.glob(".existing.backup.*"))


def test_shapefile_rollback_failure_retains_recovery_backups(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If restoration itself is blocked, never discard the last copy of old data."""
    original = tmp_path / "existing.shp"
    original.write_bytes(b"old shapes")
    staged = tmp_path / ".existing.staged.shp"
    staged.write_bytes(b"new shapes")
    real_replace = Path.replace

    def fail_promotion_and_recovery(self: Path, destination: str | Path) -> Path:
        if self == staged or (self.parent != tmp_path and destination == original):
            message = "injected locked output"
            raise PermissionError(message)
        return real_replace(self, destination)

    monkeypatch.setattr(Path, "replace", fail_promotion_and_recovery)
    with pytest.raises(RuntimeError, match="rollback was incomplete"):
        _promote_dataset(staged, original, overwrite=True)

    backups = list(tmp_path.glob(".existing.backup.*"))
    assert len(backups) == 1
    assert (backups[0] / original.name).read_bytes() == b"old shapes"
    assert staged.read_bytes() == b"new shapes"


def test_shapefile_no_overwrite_preserves_destination_and_stage(tmp_path: Path) -> None:
    output = tmp_path / "existing.shp"
    output.write_bytes(b"old shapes")
    staged = tmp_path / ".existing.staged.shp"
    staged.write_bytes(b"new shapes")

    with pytest.raises(FileExistsError, match="already exists"):
        _promote_dataset(staged, output, overwrite=False)

    assert output.read_bytes() == b"old shapes"
    assert staged.read_bytes() == b"new shapes"
