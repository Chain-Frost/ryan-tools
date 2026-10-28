"""Regression tests for the standalone TUFLOW check-folder deduplicator."""

from __future__ import annotations

import csv
import importlib.util
import os
import sys
from datetime import date
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast

import pytest
from rich.progress import Progress

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "ryan-scripts"
    / "TUFLOW-python"
    / "model_management"
    / "delete_duplicate_check_files.py"
)


class DeleteRecord(Protocol):
    @property
    def delete_file(self) -> Path: ...


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


def respond_cancel(_prompt: str) -> str:
    return "CANCEL"


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


def test_default_output_names_include_local_run_date() -> None:
    before = date.today().isoformat()
    spec = importlib.util.spec_from_file_location("test_duplicate_default_paths", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    after = date.today().isoformat()

    assert module.RUN_DATE in {before, after}
    assert module.REPORT_FILE == Path(f"duplicate_files_to_delete_{module.RUN_DATE}.txt")
    assert module.MANIFEST_FILE == Path(f"duplicate_files_manifest_{module.RUN_DATE}.csv")
    assert module.PROPOSAL_MANIFEST_FILE == Path(f"duplicate_files_proposed_{module.RUN_DATE}.csv")


def check_folder(root: Path, name: str) -> Path:
    folder = root / name / "01.0p" / "check"
    folder.mkdir(parents=True)
    return folder


def write_duplicates(folder: Path, *names: str, content: bytes = b"identical-style") -> list[Path]:
    paths = [folder / name for name in names]
    for path in paths:
        path.write_bytes(content)
    return paths


def planned_actions(utility: ModuleType, root: Path) -> tuple[list[DeleteRecord], list[PlaceholderRecord]]:
    records = utility.iter_files_in_check_folders(utility.find_check_folders(root))
    hashed_records = utility.add_hashes_to_candidates(records)
    groups = utility.find_duplicate_groups(hashed_records)
    actions = utility.build_delete_actions(groups, hashed_records)
    placeholders = utility.build_placeholder_actions(actions, records)
    return cast(list[DeleteRecord], actions), cast(list[PlaceholderRecord], placeholders)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def test_find_check_folders_walks_nested_directories_only(utility: ModuleType, tmp_path: Path) -> None:
    root = tmp_path / "results"
    first = root / "GD2" / "01.0p" / "ChEcK"
    second = root / "MLTP" / "v01" / "checks"
    third = root / "misc" / "CHECKS"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    third.mkdir(parents=True)
    unrelated = root / "logs"
    unrelated.mkdir()
    (unrelated / "check").write_text("not a directory", encoding="utf-8")
    (unrelated / "checks").write_text("not a directory", encoding="utf-8")
    (root / "GD2" / "ordinary.txt").write_text("not a directory", encoding="utf-8")

    assert utility.find_check_folders(root) == sorted((first, second, third), key=lambda path: str(path).casefold())


def test_duplicates_compare_across_check_and_checks_folders(utility: ModuleType, tmp_path: Path) -> None:
    first = tmp_path / "GD2" / "check"
    second = tmp_path / "MLTP" / "checks"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    write_duplicates(first, "a.qml", "b.qml", content=b"same")
    write_duplicates(second, "c.qml", "d.qml", content=b"same")

    actions, placeholders = planned_actions(utility, tmp_path)

    assert len(actions) == 3
    assert len(placeholders) == 2
    assert {placeholder.check_folder for placeholder in placeholders} == {first, second}
    assert {action.delete_file.parent for action in actions} == {first, second}


def test_safe_defaults(utility: ModuleType) -> None:
    assert utility.DRY_RUN is False
    assert utility.COMPARE_ACROSS_CHECK_FOLDERS is True
    assert utility.INCLUDE_RASTER_FILES is True
    assert utility.RESTORE_FROM_MANIFEST is False
    assert utility.APPLY_PROPOSAL_MANIFEST is False


def test_storage_totals_include_delete_fraction_and_remaining_gb(utility: ModuleType) -> None:
    action = utility.DeleteAction(
        delete_file=Path("delete.tif"),
        keep_file=Path("keep.tif"),
        delete_mtime_ns=1,
        keep_mtime_ns=1,
        suffix=".tif",
        size_bytes=1_000_000_000,
        content_hash="0" * 64,
    )
    lines = utility.storage_summary_lines(utility.ScanTotals(file_count=4, total_bytes=4_000_000_000), [action])
    assert "4.000 GB" in lines[0]
    assert "1.000 GB" in lines[1]
    assert "25.0%" in lines[1]
    assert "3.000 GB" in lines[2]
    assert "75.0%" in lines[2]
    assert "Actually deleted" in utility.storage_summary_lines(
        utility.ScanTotals(file_count=4, total_bytes=4_000_000_000), [action], actual=True
    )[1]


def test_proposal_mode_reuses_previous_scan_without_rehash(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "model")
    duplicates = write_duplicates(folder, "a.tif", "b.tif", content=b"0123456789")
    (folder / "unique.tif").write_bytes(b"unique")
    monkeypatch.setattr(utility, "DRY_RUN", True)
    utility.main()

    rows = read_rows(tmp_path / "proposal.csv")
    assert len(rows) == 1
    assert rows[0]["scan_file_count"] == "3"
    assert rows[0]["scan_total_bytes"] == "26"
    assert rows[0]["delete_mtime_ns"]
    assert rows[0]["keep_mtime_ns"]
    assert "Estimated remaining" in (tmp_path / "deletions.txt").read_text(encoding="utf-8")

    monkeypatch.setattr(utility, "APPLY_PROPOSAL_MANIFEST", True)
    monkeypatch.setattr(utility, "DRY_RUN", False)
    monkeypatch.setattr("builtins.input", respond_delete)

    def forbid_hash(_path: Path, *, on_chunk: object = None) -> str:
        raise AssertionError("Apply mode must not rehash saved proposals")

    monkeypatch.setattr(utility, "file_hash", forbid_hash)
    utility.main()

    assert duplicates[0].exists()
    assert not duplicates[1].exists()
    assert (folder / "unique.tif").exists()
    assert len(read_rows(tmp_path / "deletions.csv")) == 1
    assert (tmp_path / "deletions_001.txt").is_file()


def test_apply_rejects_changed_mtime_even_if_file_size_matches(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "model")
    duplicates = write_duplicates(folder, "a.qml", "b.qml")
    monkeypatch.setattr(utility, "DRY_RUN", True)
    utility.main()
    stat = duplicates[1].stat()
    os.utime(duplicates[1], ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))
    monkeypatch.setattr(utility, "APPLY_PROPOSAL_MANIFEST", True)
    monkeypatch.setattr(utility, "DRY_RUN", False)

    def unexpected_confirmation(_prompt: str) -> str:
        raise AssertionError("Stale proposals must be rejected before prompting")

    monkeypatch.setattr("builtins.input", unexpected_confirmation)
    utility.main()
    assert all(path.exists() for path in duplicates)
    assert not (tmp_path / "deletions.csv").exists()


def test_apply_rejects_old_proposal_without_scan_timestamps(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "model")
    duplicates = write_duplicates(folder, "a.qml", "b.qml")
    actions, _ = planned_actions(utility, tmp_path)
    utility.write_manifest(tmp_path / "proposal.csv", utility.manifest_rows(actions))
    monkeypatch.setattr(utility, "APPLY_PROPOSAL_MANIFEST", True)
    monkeypatch.setattr(utility, "DRY_RUN", False)
    utility.main()
    assert all(path.exists() for path in duplicates)
    assert not (tmp_path / "deletions.csv").exists()


def test_apply_rejects_proposal_that_deletes_its_retained_file(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "model")
    duplicates = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    monkeypatch.setattr(utility, "DRY_RUN", True)
    utility.main()
    proposal = tmp_path / "proposal.csv"
    rows = read_rows(proposal)
    # A would-be retained copy becomes another row's deletion target.
    rows[1]["kept_path"] = str(duplicates[1])
    rows[1]["keep_mtime_ns"] = str(duplicates[1].stat().st_mtime_ns)
    with proposal.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=utility.PROPOSAL_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    monkeypatch.setattr(utility, "APPLY_PROPOSAL_MANIFEST", True)
    monkeypatch.setattr(utility, "DRY_RUN", False)
    utility.main()
    assert all(path.exists() for path in duplicates)
    assert not (tmp_path / "deletions.csv").exists()


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


def test_dry_run_writes_report_and_proposal_without_mutation(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    paths = write_duplicates(folder, "a.qml", "b.qml", "c.qml")
    monkeypatch.setattr(utility, "DRY_RUN", True)
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

    def tracking_hash(path: Path, *, on_chunk: Callable[[int], None] | None = None) -> str:
        hash_calls.append(path)
        return hash_once(path, on_chunk=on_chunk)

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
    monkeypatch.setattr("builtins.input", respond_cancel)

    utility.main()

    assert all(path.exists() for path in paths)
    assert proposal.read_text(encoding="utf-8") == "old proposal"
    assert report.read_text(encoding="utf-8") == "old report"
    assert len(read_rows(tmp_path / "proposal_001.csv")) == 1
    assert (tmp_path / "deletions_001.txt").is_file()
    assert not (tmp_path / "deletions.csv").exists()


def test_sha256_progress_tracks_bytes_during_large_file(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    content = b"0123456789ABCDEF"
    paths = write_duplicates(folder, "a.qml", "b.qml", content=content)
    monkeypatch.setattr(utility, "HASH_CHUNK_SIZE", 3)
    dashboard = Progress(disable=True)

    def make_dashboard() -> Progress:
        return dashboard

    monkeypatch.setattr(utility, "hash_progress_display", make_dashboard)
    result = utility.add_hashes_to_candidates(utility.iter_files_in_check_folders([folder]))

    assert all(record.content_hash is not None for record in result)
    assert len(result) == 2
    assert len(dashboard.tasks) == 1
    assert dashboard.tasks[0].completed == len(content) * 2
    assert dashboard.tasks[0].total == len(content) * 2
    assert dashboard.tasks[0].fields["files_done"] == 2
    assert dashboard.tasks[0].description == "SHA-256 complete"
    assert all(path.exists() for path in paths)


def test_hashing_progress_skips_non_candidates(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    (folder / "small.qml").write_bytes(b"a")
    (folder / "large.qml").write_bytes(b"abcdef")
    dashboard = Progress(disable=True)

    def make_dashboard() -> Progress:
        return dashboard

    monkeypatch.setattr(utility, "hash_progress_display", make_dashboard)
    records = utility.iter_files_in_check_folders([folder])
    output = utility.add_hashes_to_candidates(records)
    assert len(output) == 2
    assert all(record.content_hash is None for record in output)
    assert not dashboard.tasks


def test_hashing_progress_disabled_preserves_hashes(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    write_duplicates(folder, "a.qml", "b.qml", content=b"equal")
    records = utility.iter_files_in_check_folders([folder])
    monkeypatch.setattr(utility, "SHOW_HASH_PROGRESS", False)
    output = utility.add_hashes_to_candidates(records)
    assert output[0].content_hash is not None
    assert output[0].content_hash == output[1].content_hash


def test_sha256_progress_handles_zero_length_files(
    utility: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder = check_folder(tmp_path, "GD2")
    write_duplicates(folder, "a.qml", "b.qml", content=b"")
    dashboard = Progress(disable=True)

    def make_dashboard() -> Progress:
        return dashboard

    monkeypatch.setattr(utility, "hash_progress_display", make_dashboard)
    output = utility.add_hashes_to_candidates(utility.iter_files_in_check_folders([folder]))
    assert len(output) == 2
    assert output[0].content_hash == output[1].content_hash
    assert dashboard.tasks[0].total == 0
    assert dashboard.tasks[0].fields["files_done"] == 2


def test_symlinked_duplicate_is_not_deleted(utility: ModuleType, tmp_path: Path) -> None:
    """Metadata can match, but a linked candidate must not pass the final unlink guard."""
    folder = check_folder(tmp_path, "linked-model")
    retained = folder / "retained.txt"
    retained.write_bytes(b"identical")
    alias = folder / "linked.txt"
    try:
        alias.symlink_to(retained)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"File symlinks unavailable: {exc}")

    stat = alias.stat()
    assert not utility.matches_snapshot(alias, size_bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)
    assert utility.matches_snapshot(retained, size_bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)
    action = utility.DeleteAction(
        delete_file=alias,
        keep_file=retained,
        delete_mtime_ns=stat.st_mtime_ns,
        keep_mtime_ns=retained.stat().st_mtime_ns,
        suffix=".txt",
        size_bytes=stat.st_size,
        content_hash=utility.file_hash(retained),
    )
    assert utility.delete_duplicate_files([action]) == []
    assert alias.is_symlink()
    assert retained.read_bytes() == b"identical"
