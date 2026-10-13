"""Regression tests for the standalone TUFLOW check-folder deduplicator."""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "ryan-scripts"
    / "TUFLOW-python"
    / "model_management"
    / "delete_duplicate_check_files.py"
)


class PlaceholderRecord(Protocol):
    @property
    def keep_file(self) -> Path: ...

    @property
    def check_folder(self) -> Path: ...

    @property
    def deleted_files(self) -> tuple[Path, ...]: ...


def respond_delete(_prompt: str) -> str:
    return "DELETE"


def respond_restore(_prompt: str) -> str:
    return "RESTORE"


@pytest.fixture
def utility(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    spec = importlib.util.spec_from_file_location("test_duplicate_check_utility", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    monkeypatch.setattr(module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(module, "REPORT_FILE", tmp_path / "deletions.txt")
    monkeypatch.setattr(module, "MANIFEST_FILE", tmp_path / "deletions.csv")
    monkeypatch.setattr(module, "PROPOSAL_MANIFEST_FILE", tmp_path / "proposal.csv")
    return module


def check_folder(root: Path, name: str) -> Path:
    folder = root / name / "01.0p" / "check"
    folder.mkdir(parents=True)
    return folder


def write_duplicates(folder: Path, *names: str, content: bytes = b"identical-style") -> list[Path]:
    paths = [folder / name for name in names]
    for path in paths:
        path.write_bytes(content)
    return paths


def planned_actions(utility: ModuleType, root: Path) -> tuple[list[object], list[PlaceholderRecord]]:
    records = utility.iter_files_in_check_folders(utility.find_check_folders(root))
    hashed_records = utility.add_hashes_to_candidates(records)
    groups = utility.find_duplicate_groups(hashed_records)
    actions = utility.build_delete_actions(groups, hashed_records)
    placeholders = utility.build_placeholder_actions(actions, records)
    return actions, cast(list[PlaceholderRecord], placeholders)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_safe_defaults(utility: ModuleType) -> None:
    assert utility.DRY_RUN is False
    assert utility.COMPARE_ACROSS_CHECK_FOLDERS is True
    assert utility.INCLUDE_RASTER_FILES is True
    assert utility.RESTORE_FROM_MANIFEST is False


def test_one_placeholder_per_hash_within_one_folder(utility: ModuleType, tmp_path: Path) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    actions, placeholders = planned_actions(utility, tmp_path)
    assert len(actions) == 2
    assert len(placeholders) == 1
    assert placeholders[0].keep_file == files[0]
    assert {path.name for path in placeholders[0].deleted_files} == {"b.qml", "c.qml"}
    assert "in this or another check folder" in utility.placeholder_text(placeholders[0])


def test_placeholder_for_each_affected_folder_and_content_group(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder_a = check_folder(tmp_path, "GD2")
    folder_b = check_folder(tmp_path, "MLTP")
    write_duplicates(folder_a, "a.qml", "b.qml")
    write_duplicates(folder_b, "c.qml", "d.qml")
    write_duplicates(folder_b, "e.qml", "f.qml", content=b"different-hash")
    monkeypatch.setattr(utility, "COMPARE_ACROSS_CHECK_FOLDERS", True)

    actions, placeholders = planned_actions(utility, tmp_path)
    assert len(actions) == 4
    assert len(placeholders) == 3
    assert sorted(p.check_folder for p in placeholders).count(folder_a) == 1
    assert sorted(p.check_folder for p in placeholders).count(folder_b) == 2


def test_raster_and_sidecar_are_opt_in(utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    folder = check_folder(tmp_path, "GD2")
    write_duplicates(folder, "a.qml", "b.qml")
    for name in ("a.tif", "b.tif", "a.tif.ovr", "b.tif.ovr", "a.tif.aux.xml", "b.tif.aux.xml"):
        (folder / name).write_bytes(b"some-raster-or-sidecar")
    monkeypatch.setattr(utility, "INCLUDE_RASTER_FILES", False)
    default_files = utility.iter_files_in_check_folders([folder])
    assert {record.path.name for record in default_files} == {"a.qml", "b.qml"}
    monkeypatch.setattr(utility, "INCLUDE_RASTER_FILES", True)
    all_files = utility.iter_files_in_check_folders([folder])
    assert len(all_files) == 8


def test_dry_run_writes_report_and_proposal_without_mutation(utility: ModuleType, tmp_path: Path) -> None:
    folder = check_folder(tmp_path, "GD2")
    paths = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    utility.DRY_RUN = True
    utility.main()
    assert all(path.is_file() for path in paths)
    assert (tmp_path / "deletions.txt").is_file()
    assert len(read_rows(tmp_path / "proposal.csv")) == 2
    assert not (tmp_path / "deletions.csv").exists()
    assert not list(folder.glob("*.DUPLICATE_REMOVED.txt"))


def test_delete_creates_manifest_and_same_folder_placeholder(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    paths = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    monkeypatch.setattr(utility, "DRY_RUN", False)
    monkeypatch.setattr("builtins.input", respond_delete)
    utility.main()
    assert paths[0].is_file()
    assert not paths[1].exists()
    assert not paths[2].exists()
    placeholders = list(folder.glob("*.DUPLICATE_REMOVED.txt"))
    assert len(placeholders) == 1
    content = placeholders[0].read_text(encoding="utf-8")
    assert "b.qml" in content
    assert "c.qml" in content
    rows = read_rows(tmp_path / "deletions.csv")
    assert len(rows) == 2
    assert all(row["status"] == "deleted" for row in rows)
    assert all(Path(row["kept_path"]) == paths[0] for row in rows)


def test_changed_keep_or_duplicate_is_not_deleted(utility: ModuleType, tmp_path: Path) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    actions, _ = planned_actions(utility, tmp_path)
    files[0].write_bytes(b"changed-kept-file")
    assert utility.delete_duplicate_files(actions) == []
    assert all(path.exists() for path in files)


def test_recovery_restores_original_paths(utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    monkeypatch.setattr(utility, "DRY_RUN", False)
    monkeypatch.setattr("builtins.input", respond_delete)
    utility.main()
    monkeypatch.setattr(utility, "RESTORE_FROM_MANIFEST", True)
    monkeypatch.setattr("builtins.input", respond_restore)
    utility.main()
    assert all(path.read_bytes() == b"identical-style" for path in files)
    assert all(row["status"] == "restored" for row in read_rows(tmp_path / "deletions.csv"))


def test_restoration_preserves_existing_different_file(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml")
    monkeypatch.setattr(utility, "DRY_RUN", False)
    monkeypatch.setattr("builtins.input", respond_delete)
    utility.main()
    files[1].write_bytes(b"new-user-content")
    monkeypatch.setattr("builtins.input", respond_restore)
    assert utility.restore_files_from_manifest(tmp_path / "deletions.csv") == (0, 0)
    assert files[1].read_bytes() == b"new-user-content"


def test_restore_pending_manifest_after_interruption(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml")
    actions, _ = planned_actions(utility, tmp_path)
    utility.write_manifest(tmp_path / "deletions.csv", utility.manifest_rows(actions, status="pending"))
    files[1].unlink()
    monkeypatch.setattr(utility, "DRY_RUN", False)
    monkeypatch.setattr("builtins.input", respond_restore)
    assert utility.restore_files_from_manifest(tmp_path / "deletions.csv") == (1, 0)
    assert files[1].read_bytes() == files[0].read_bytes()


def test_existing_recovery_manifest_blocks_new_delete(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml")
    (tmp_path / "deletions.csv").write_text("old recovery state", encoding="utf-8")
    monkeypatch.setattr(utility, "DRY_RUN", False)
    utility.main()
    assert all(path.exists() for path in files)
    assert (tmp_path / "deletions.csv").read_text(encoding="utf-8") == "old recovery state"


def test_conflicting_placeholder_blocks_delete(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    files = write_duplicates(folder, "a.qml", "b.qml")
    (folder / "b.qml.DUPLICATE_REMOVED.txt").write_text("important unrelated data", encoding="utf-8")
    monkeypatch.setattr(utility, "DRY_RUN", False)
    utility.main()
    assert all(path.exists() for path in files)
    assert not (tmp_path / "deletions.csv").exists()


def test_two_folders_three_groups_of_100_leave_six_placeholders(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder_a = check_folder(tmp_path, "GD2")
    folder_b = check_folder(tmp_path, "MLTP")
    groups = [
        ("_Z.qml", b"style"),
        ("_DEM_Z.tif", b"terrain"),
        ("_DEM_Z_HR.tif", b"hires"),
    ]
    for folder in (folder_a, folder_b):
        for suffix, data in groups:
            for index in range(100):
                (folder / f"run_{index:03}{suffix}").write_bytes(data)

    hash_calls: list[Path] = []
    hash_once = utility.file_hash

    def tracking_hash(path: Path) -> str:
        hash_calls.append(path)
        return hash_once(path)

    monkeypatch.setattr(utility, "file_hash", tracking_hash)
    monkeypatch.setattr("builtins.input", respond_delete)
    utility.main()

    assert len(hash_calls) == 600  # One content read per candidate; none during deletion.
    assert len(list(folder_a.glob("*.DUPLICATE_REMOVED.txt"))) == 3
    assert len(list(folder_b.glob("*.DUPLICATE_REMOVED.txt"))) == 3
    assert len([path for path in folder_a.iterdir() if not utility.is_placeholder_file(path)]) == 3
    assert len([path for path in folder_b.iterdir() if not utility.is_placeholder_file(path)]) == 0
    rows = read_rows(tmp_path / "deletions.csv")
    assert len(rows) == 597
    assert all(row["status"] == "deleted" for row in rows)


def test_retained_file_checked_before_each_delete(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    paths = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    actions, _ = planned_actions(utility, tmp_path)
    original_delete = utility.delete_duplicate_file
    deletion_count = 0

    def change_retained_after_first(action: object) -> bool:
        nonlocal deletion_count
        deleted: bool = original_delete(action)
        if deleted:
            deletion_count += 1
            paths[0].write_bytes(b"modified-retained")
        return deleted

    monkeypatch.setattr(utility, "delete_duplicate_file", change_retained_after_first)
    deleted_actions = utility.delete_duplicate_files(actions)
    assert deletion_count == 1
    assert len(deleted_actions) == 1
    assert paths[2].exists()


def test_proposal_and_report_are_not_overwritten(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    paths = write_duplicates(folder, "a.qml", "b.qml")
    proposal = tmp_path / "proposal.csv"
    report = tmp_path / "deletions.txt"
    proposal.write_text("old proposal", encoding="utf-8")
    report.write_text("old report", encoding="utf-8")
    monkeypatch.setattr("builtins.input", lambda _prompt: "CANCEL")

    utility.main()

    assert all(path.exists() for path in paths)
    assert proposal.read_text(encoding="utf-8") == "old proposal"
    assert report.read_text(encoding="utf-8") == "old report"
    assert len(read_rows(tmp_path / "proposal_001.csv")) == 1
    assert (tmp_path / "deletions_001.txt").is_file()
    assert not (tmp_path / "deletions.csv").exists()
