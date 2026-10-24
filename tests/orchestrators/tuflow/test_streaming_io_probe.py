"""Bounded read-ahead pilot: deterministic overlap, no acceptance shortcuts."""

from __future__ import annotations

import os
import threading
import types
from collections.abc import Callable
from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow import streaming_io_probe
from ryan_library.orchestrators.tuflow.batch_preflight_adapter import BatchModelView
from ryan_library.orchestrators.tuflow.streaming_io_probe import FileReadAheadProbe, probe_batch_io_overlap


def _ref(path: Path, **overrides: object) -> types.SimpleNamespace:
    attributes: dict[str, object] = {
        "path_pattern": path,
        "base_directory": path.parent,
        "layer_pattern": None,
        "unresolved_variables": (),
        "requires_attribute_scan": False,
        "requires_database_scan": False,
        "requires_table_link_scan": False,
        "requires_raster_metadata_scan": False,
    }
    attributes.update(overrides)
    return types.SimpleNamespace(**attributes)


def _run(index: int, flags: str, *refs: object, error: str | None = None) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        index=index,
        flags=flags,
        error=error,
        file_references=tuple(refs),
        database_sources=(),
        database_activations=(),
        input_references=(),
        ordered_inputs=(),
    )


def test_worker_can_inspect_first_projection_before_second_context_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_work_started = threading.Event()
    producer_resumed = threading.Event()
    paths = [tmp_path / "first.csv", tmp_path / "second.csv"]
    for path in paths:
        path.write_text("data", encoding="utf-8")

    def observed_stat(path: Path) -> tuple[str, float]:
        first_work_started.set()
        assert path in paths
        return "present", 0.02

    monkeypatch.setattr(streaming_io_probe, "_stat_literal", observed_stat)

    class Model:
        def collect_batch_dependencies(
            self, flags: tuple[str, ...], *, on_run: Callable[[object], None]
        ) -> types.SimpleNamespace:
            cases = []
            for index, flag in enumerate(flags):
                case = _run(index, flag, _ref(paths[index]))
                cases.append(case)
                on_run(case)
                if index == 0:
                    assert first_work_started.wait(timeout=3), "I/O worker never ran during context construction"
                    producer_resumed.set()
            return types.SimpleNamespace(runs=tuple(cases))

    view, summary = probe_batch_io_overlap(Model(), ["-s1 DEV", "-s1 EXG"], max_pending=2)
    assert producer_resumed.is_set()
    assert summary.unique_paths == 2
    assert summary.present == 2
    assert summary.peak_outstanding <= 2
    assert summary.overlapped_context_construction
    assert isinstance(view, BatchModelView)


def test_references_are_conservatively_filtered_and_deduplicated(tmp_path: Path) -> None:
    plain = tmp_path / "plain.csv"
    plain.write_text("data", encoding="utf-8")
    missing = tmp_path / "missing.csv"
    with FileReadAheadProbe(max_pending=1) as probe:
        probe.submit_run(_run(0, "-e1 A", _ref(plain), _ref(plain), _ref(missing)))
        probe.submit_run(
            _run(
                1,
                "-e1 B",
                _ref(tmp_path / "grid_*.tif"),
                _ref(tmp_path / "model.gpkg", layer_pattern="all"),
                _ref(tmp_path / "code.shp", requires_attribute_scan=True),
                _ref(tmp_path / "<<RUN>>.csv", unresolved_variables=("RUN",)),
            )
        )
        probe.submit_run(_run(2, "-e1 C", _ref(tmp_path / "ignored.csv"), error="invalid flags"))
        probe.mark_producer_finished()
        summary = probe.finish()
    assert summary.unique_paths == 2
    assert summary.duplicate_paths == 1
    assert summary.excluded_references == 4
    assert summary.present == 1
    assert summary.missing == 1
    assert summary.unresolved == 0
    assert summary.peak_outstanding == 1


def test_unhandled_consumer_failure_propagates_and_closes_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed_stat(_path: Path) -> tuple[str, float]:
        message = "worker failure"
        raise RuntimeError(message)

    monkeypatch.setattr(streaming_io_probe, "_stat_literal", failed_stat)
    probe = FileReadAheadProbe(max_pending=1)
    probe.submit_run(_run(0, "-s1 DEV", _ref(tmp_path / "code.shp")))
    probe.mark_producer_finished()
    with pytest.raises(RuntimeError, match="worker failure"):
        probe.finish()


def test_ordinary_batch_adapter_never_passes_new_hook() -> None:
    class LegacyModel:
        def collect_batch_dependencies(self, flags: tuple[str, ...]) -> types.SimpleNamespace:
            return types.SimpleNamespace(runs=tuple(_run(index, flag) for index, flag in enumerate(flags)))

    view = BatchModelView(LegacyModel(), ("-s1 DEV",))
    assert view.context("-s1 DEV", file_resolution="deferred").find_input() == []
    view.assert_consumed()


def test_probe_does_not_treat_failure_or_prechecks_as_authoritative() -> None:
    class FailedModel:
        def collect_batch_dependencies(self, flags: tuple[str, ...], *, on_run: object) -> types.SimpleNamespace:
            case = _run(0, flags[0], error="bad context")
            on_run(case)
            return types.SimpleNamespace(runs=(case,))

    view, summary = probe_batch_io_overlap(FailedModel(), ("-s1 DEV",))
    assert summary.unique_paths == 0
    with pytest.raises(ValueError, match="bad context"):
        view.context("-s1 DEV", file_resolution="deferred")


def test_real_pytuflow_completed_run_hook_retains_projection_parity(tmp_path: Path) -> None:
    """Dedicated Windows gate pins PyTUFLOW PR #28; no native model or solver."""
    if os.environ.get("RYAN_TEST_STREAM_IO_PYTUFLOW") != "1":
        pytest.skip("Requires the exact opted-in producer fork")

    from pytuflow import TCF

    file = tmp_path / "gis.shp"
    file.write_bytes(b"")
    tcf = tmp_path / "pilot.tcf"
    tcf.write_text("Read GIS Code == gis.shp\n", encoding="utf-8")
    model = TCF(tcf)
    flags = ("-s1 DEV", "-s1 EXG", "-s1 DEV")
    plain = model.collect_batch_dependencies(flags)
    view, summary = probe_batch_io_overlap(model, flags, max_pending=2)
    assert plain.passed
    assert summary.unique_paths == 1
    assert summary.present == 1
    assert summary.duplicate_paths == 2
    assert summary.peak_outstanding <= 2
    for flag, original in zip(flags, plain.runs, strict=True):
        inputs = view.context(flag, file_resolution="deferred").find_input()
        assert tuple(ref for inp in inputs for ref in inp.file_references) == original.file_references
    view.assert_consumed()
