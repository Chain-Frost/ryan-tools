"""Unique I/O work, worker bounds and deterministic result routing."""

import sqlite3
from collections import Counter
from pathlib import Path
from threading import Barrier, Event, Lock

import pytest

from ryan_library.functions.tuflow import dependency_index as inventory


def test_threaded_inventory_matches_serial_with_layers_and_missing_files(tmp_path: Path) -> None:
    database = tmp_path / "model.gpkg"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE gpkg_contents (table_name TEXT)")
        connection.execute("INSERT INTO gpkg_contents VALUES ('existing')")
    present = tmp_path / "present.csv"
    present.touch()
    requirements = {database: {"existing", "absent"}, present: set[str](), tmp_path / "missing.csv": set[str]()}
    serial = inventory.check_unique_dependencies(requirements)
    threaded = inventory.check_unique_dependencies(requirements, io_workers=3)
    assert threaded == serial
    assert threaded.layers[(database, "absent")].status == "missing"
    assert threaded.files[present].status == "present"


def test_sparse_checks_overlap_once_per_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = [tmp_path / f"source{i}.csv" for i in range(4)]
    barrier = Barrier(2, timeout=5)
    lock = Lock()
    calls: Counter[Path] = Counter()

    def observe(path: Path) -> inventory.DependencyResult:
        with lock:
            calls[path] += 1
        barrier.wait()
        return inventory.DependencyResult("present")

    monkeypatch.setattr(inventory, "_individual_file", observe)
    result = inventory.check_unique_dependencies(dict.fromkeys(paths, ()), io_workers=2)
    assert list(result.files) == paths
    assert calls == Counter(paths)
    assert result.individually_checked == 4


def test_dense_directory_still_uses_one_scan(tmp_path: Path) -> None:
    paths = [tmp_path / f"source{i}.csv" for i in range(10)]
    for path in paths:
        path.touch()
    result = inventory.check_unique_dependencies(dict.fromkeys(paths, ()), io_workers=4)
    assert result.directories_scanned == 1
    assert result.individually_checked == 0
    assert all(item.status == "present" for item in result.files.values())


def test_finished_worker_starts_next_job_while_first_is_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = [tmp_path / f"source{i}.csv" for i in range(3)]
    first_started = Event()
    third_started = Event()

    def observe(path: Path) -> inventory.DependencyResult:
        if path == paths[0]:
            first_started.set()
            assert third_started.wait(timeout=5), "Worker waited for the entire submission wave"
        elif path == paths[1]:
            assert first_started.wait(timeout=5)
        else:
            third_started.set()
        return inventory.DependencyResult("present")

    monkeypatch.setattr(inventory, "_individual_file", observe)
    result = inventory.check_unique_dependencies(dict.fromkeys(paths, ()), io_workers=2)
    assert list(result.files) == paths
    assert result.individually_checked == 3


@pytest.mark.parametrize("workers", [0, 33])
def test_invalid_worker_count_rejected(workers: int) -> None:
    with pytest.raises(ValueError, match="io_workers"):
        inventory.check_unique_dependencies({}, io_workers=workers)
