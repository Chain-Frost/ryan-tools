"""Native graph diagnostic contracts, independent of private models and solver execution."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import types
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ryan_library.orchestrators.tuflow.input_preflight import PreflightIssue, PreflightReport, RunRequest
from tests.fixtures.tuflow_example_repro import run_checks, run_local_checks

GRAPH_MODES = ("eager", "deferred", "batch_graph_off", "batch_graph_on")


@dataclass(frozen=True)
class _Reference:
    path_pattern: Path
    base_directory: Path
    origin: str
    layer_pattern: str | None = None
    unresolved_variables: tuple[str, ...] = ()
    requires_attribute_scan: bool = False
    requires_database_scan: bool = False
    requires_table_link_scan: bool = False
    requires_raster_metadata_scan: bool = False
    database_kind: str | None = None


@dataclass
class _BatchResult:
    runs: tuple[types.SimpleNamespace, ...]
    contexts_constructed: int = 1
    input_runstates_constructed: int = 3
    input_group_clone_seconds: float = 17.0
    input_group_key_seconds: float = 19.0
    input_group_fallbacks: tuple[tuple[str, int], ...] = (("unsupported-command", 2),)
    input_group_cache_bypass_reason: str | None = "graph-disabled"


def _projection(flags: tuple[str, ...], tcf: Path, source: Path) -> _BatchResult:
    cases: list[types.SimpleNamespace] = []
    for index, flag in enumerate(flags):
        command = types.SimpleNamespace(
            command_index=0,
            input_uuid=f"command-{index}",
            command_name="Read GIS Code",
            command_text=f"Read GIS Code == {source.name}",
            owner_control_file=tcf,
            line_number=1,
            reference_count=1,
        )
        reference = _Reference(
            path_pattern=source,
            base_directory=source.parent,
            origin=command.command_text,
        )
        record = types.SimpleNamespace(
            command_index=0,
            input_uuid=command.input_uuid,
            owner_control_file=tcf,
            line_number=1,
            reference=reference,
        )
        cases.append(
            types.SimpleNamespace(
                index=index,
                flags=flag,
                error=None,
                file_references=(reference,),
                input_references=(record,),
                ordered_inputs=(command,),
                database_activations=(),
                database_sources=(),
            )
        )
    return _BatchResult(tuple(cases))


def _success_stats() -> dict[str, Any]:
    return {
        "collection_issues": 0,
        "inventory_issues": 0,
        "entries": 1,
        "files": 1,
        "provenance_complete": True,
        "reference_provenance": [(1, "reference-digest")],
        "command_provenance": [
            {
                "run_index": 1,
                "flags": "-e1 A",
                "commands": [(0, "model.tcf", 1, "Read GIS Code", "Read GIS Code == source.txt")],
            }
        ],
    }


@pytest.mark.parametrize(("mode", "graph"), [("batch_graph_off", False), ("batch_graph_on", True)])
def test_graph_modes_forward_one_public_batch_call_with_all_caches_and_exact_flags(
    tmp_path: Path, mode: str, graph: bool
) -> None:
    source = tmp_path / "source.txt"
    source.write_text("input", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    calls: list[tuple[tuple[str, ...], bool, bool]] = []
    loads: list[str] = []

    class Model:
        def __init__(self, path: str) -> None:
            loads.append(path)

        def collect_batch_dependencies(
            self, flags: tuple[str, ...], *, cache_scopes: bool = False, compile_tree: bool = False
        ) -> _BatchResult:
            calls.append((flags, cache_scopes, compile_tree))
            return _projection(flags, tcf, source)

    runs = [
        RunRequest(41, {"s2": "north", "e1": "01.0p"}),
        RunRequest(9, {"e1": "20.0p", "s2": "south"}),
        RunRequest(72, {"s2": "north", "e1": "01.0p"}),
    ]
    keys, stats = run_checks.collect(tcf, runs, mode, Model)
    assert loads == [str(tcf)]
    assert calls == [(tuple(run.arguments for run in runs), True, graph)]
    assert {key[0] for key in keys} == {41, 9, 72}
    assert stats["collection_issues"] == stats["inventory_issues"] == 0


@pytest.mark.parametrize("mode", ["batch_graph_off", "batch_graph_on"])
def test_graph_mode_refuses_an_api_without_graph_controls(tmp_path: Path, mode: str) -> None:
    calls: list[tuple[str, ...]] = []

    class LegacyModel:
        def __init__(self, _path: str) -> None:
            pass

        def collect_batch_dependencies(self, flags: tuple[str, ...]) -> _BatchResult:
            calls.append(flags)
            return _BatchResult(())

    with pytest.raises((TypeError, ValueError), match=r"compile_tree|graph|cache_scopes"):
        run_checks.collect(tmp_path / "model.tcf", [RunRequest(1, {})], mode, LegacyModel)
    assert not calls


def test_unknown_mode_is_rejected_before_loading_any_model(tmp_path: Path) -> None:
    def forbidden_factory(_path: str) -> object:
        pytest.fail("An unknown diagnostic mode must not load the model")

    with pytest.raises(ValueError, match=r"mode|Mode"):
        run_checks.collect(tmp_path / "model.tcf", [RunRequest(1, {})], "typo", forbidden_factory)


@pytest.mark.parametrize(
    ("batch", "graph", "expected"),
    [(False, False, ("eager", "deferred")), (True, False, ("eager", "deferred", "batch")), (True, True, GRAPH_MODES)],
)
def test_existing_modes_are_preserved_and_graph_modes_are_explicit(
    batch: bool, graph: bool, expected: tuple[str, ...]
) -> None:
    assert run_checks.comparison_modes(batch, graph) == expected


@pytest.mark.parametrize("mode", ["batch_graph_off", "batch_graph_on"])
@pytest.mark.parametrize("defect", ["run_error", "reference_count", "reference_owner", "reference_uuid"])
def test_graph_collection_does_not_certify_corrupt_or_failed_projections(
    tmp_path: Path, mode: str, defect: str
) -> None:
    source = tmp_path / "source.txt"
    source.write_text("input", encoding="utf-8")
    tcf = tmp_path / "model.tcf"

    class Model:
        def __init__(self, _path: str) -> None:
            pass

        def collect_batch_dependencies(self, flags: tuple[str, ...], **_options: bool) -> _BatchResult:
            result = _projection(flags, tcf, source)
            case = result.runs[0]
            if defect == "run_error":
                case.error = "synthetic failed context"
            elif defect == "reference_count":
                case.ordered_inputs[0].reference_count = 2
            elif defect == "reference_owner":
                case.input_references[0].line_number = 100
            else:
                case.input_references[0].input_uuid = "orphan"
            return result

    try:
        _keys, stats = run_checks.collect(tcf, [RunRequest(1, {})], mode, Model)
    except ValueError:
        # Some provenance checks can fail before per-run replay begins.
        return
    else:
        assert stats["collection_issues"] > 0


@pytest.mark.parametrize("defect", ["missing_mode", "mode_error", "collection_issue", "inventory_issue", "identity"])
def test_four_mode_comparison_fails_closed_for_incomplete_or_different_results(defect: str) -> None:
    identity: tuple[Any, ...] = (1, "-e1 A", "read gis code", "model.gpkg", "2d_code")
    identities = {mode: {identity} for mode in GRAPH_MODES}
    row: dict[str, Any] = {mode: _success_stats() for mode in GRAPH_MODES}
    if defect == "missing_mode":
        del identities["batch_graph_off"]
        del row["batch_graph_off"]
    elif defect == "mode_error":
        row["batch_graph_off"] = {"error": "synthetic failure"}
    elif defect == "collection_issue":
        row["batch_graph_on"]["collection_issues"] = 1
    elif defect == "inventory_issue":
        row["batch_graph_on"]["inventory_issues"] = 1
    else:
        identities["batch_graph_on"] = {(1, "-e1 A", "read gis code", "other.gpkg", "2d_code")}
    run_checks.compare_collections(row, identities, GRAPH_MODES)
    assert row["passed"] is False
    if defect == "identity":
        assert row["batch_graph_on_missing"] == row["batch_graph_on_extra"] == 1
        assert row["graph_missing"] == row["graph_extra"] == 1


def test_four_mode_comparison_accepts_complete_equal_materialisation() -> None:
    identities: dict[str, set[tuple[Any, ...]]] = {
        mode: {(1, "flags", "command", "file", "layer")} for mode in GRAPH_MODES
    }
    row: dict[str, Any] = {mode: _success_stats() for mode in GRAPH_MODES}
    run_checks.compare_collections(row, identities, GRAPH_MODES)
    assert row["passed"] is True
    assert all(
        row[key] == 0
        for key in (
            "missing",
            "extra",
            "batch_graph_off_missing",
            "batch_graph_off_extra",
            "batch_graph_on_missing",
            "batch_graph_on_extra",
            "graph_missing",
            "graph_extra",
        )
    )


@pytest.mark.parametrize("defect", ["omitted", "line_number", "owner", "order"])
def test_equal_file_sets_do_not_hide_missing_or_changed_command_provenance(defect: str) -> None:
    identities: dict[str, set[tuple[Any, ...]]] = {
        mode: {(1, "flags", "command", "file", "layer")} for mode in GRAPH_MODES
    }
    row: dict[str, Any] = {mode: _success_stats() for mode in GRAPH_MODES}
    if defect == "omitted":
        del row["batch_graph_on"]["command_provenance"]
    else:
        command: list[Any] = [0, "model.tcf", 1, "Read GIS Code", "Read GIS Code == source.txt"]
        if defect == "line_number":
            command[2] = 99
        elif defect == "owner":
            command[1] = "another.tcf"
        else:
            command[0] = 1
        row["batch_graph_on"]["command_provenance"][0]["commands"] = [tuple(command)]
    run_checks.compare_collections(row, identities, GRAPH_MODES)
    assert row["passed"] is False


@pytest.mark.parametrize("field", ["command_provenance", "reference_provenance"])
def test_four_mode_comparison_requires_provenance_fields_even_when_every_mode_omits_them(field: str) -> None:
    identities: dict[str, set[tuple[Any, ...]]] = {mode: set() for mode in GRAPH_MODES}
    row: dict[str, Any] = {mode: _success_stats() for mode in GRAPH_MODES}
    for mode in GRAPH_MODES:
        del row[mode][field]
    run_checks.compare_collections(row, identities, GRAPH_MODES)
    assert row["passed"] is False


@pytest.mark.parametrize("preflight_defect", [None, "paths", "issues", "exception"])
def test_public_preflight_is_separate_untimed_and_uses_only_the_default_batch_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, preflight_defect: str | None
) -> None:
    calls: list[str] = []
    run = RunRequest(1, {"s1": "EXG"})

    def preflight(_tcf: Path, _runs: list[RunRequest], *, context_mode: str = "individual") -> PreflightReport:
        calls.append(context_mode)
        report = PreflightReport(1, checked_paths={"source.txt"})
        if context_mode == "batch":
            if preflight_defect == "paths":
                report.checked_paths = {"different.txt"}
            elif preflight_defect == "issues":
                report.issues.append(
                    PreflightIssue(1, run.description, "unresolved", "Read GIS", "source.txt", "failed")
                )
            elif preflight_defect == "exception":
                msg = "synthetic public preflight failure"
                raise ValueError(msg)
        return report

    monkeypatch.setattr(run_local_checks, "check_simulation_inputs", preflight)
    try:
        passed = run_local_checks._public_preflight(tmp_path / "model.tcf", [run], tmp_path, "ANON", True)  # pyright: ignore[reportPrivateUsage]
    except ValueError:
        assert preflight_defect == "exception"
        return
    assert passed is (preflight_defect is None)
    assert calls == ["individual", "batch"]
    summary = json.loads((tmp_path / "preflight.json").read_text(encoding="utf-8"))
    assert summary["timed"] is False
    assert summary["batch_graph_policy"] == "parser_default"
    assert summary["graph_comparison"] is False


def test_run_list_preserves_duplicate_rows_and_flag_insertion_order(tmp_path: Path) -> None:
    entries = [{"s2": "north", "e1": "A"}, {"e1": "B", "s2": "south"}, {"s2": "north", "e1": "A"}]
    source = tmp_path / "runs.json"
    source.write_text(json.dumps(entries), encoding="utf-8")
    options = argparse.Namespace(commands_file=None, runs_json=source, expect_runs=3)
    loaded = run_local_checks._read_flags(options, argparse.ArgumentParser())  # pyright: ignore[reportPrivateUsage]
    assert loaded == entries
    assert [list(entry) for entry in loaded] == [list(entry) for entry in entries]


@pytest.mark.parametrize("expected", [0, 1, 4])
def test_run_count_mismatch_remains_an_argument_error(tmp_path: Path, expected: int) -> None:
    source = tmp_path / "runs.json"
    source.write_text("[{}, {}]", encoding="utf-8")
    options = argparse.Namespace(commands_file=None, runs_json=source, expect_runs=expected)
    with pytest.raises(SystemExit) as exc:
        run_local_checks._read_flags(options, argparse.ArgumentParser())  # pyright: ignore[reportPrivateUsage]
    assert exc.value.code == 2


@pytest.mark.parametrize(("mode", "graph"), [("batch_graph_off", False), ("batch_graph_on", True)])
def test_measured_batch_includes_real_call_and_does_not_sum_overlapping_parser_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, graph: bool
) -> None:
    source = tmp_path / "source.txt"
    source.write_text("input", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    clock = 0.0
    calls: list[tuple[tuple[str, ...], bool, bool]] = []

    class Model:
        def __init__(self, _path: str) -> None:
            nonlocal clock
            clock += 2.0

        def collect_batch_dependencies(
            self, flags: tuple[str, ...], *, cache_scopes: bool = False, compile_tree: bool = False
        ) -> _BatchResult:
            nonlocal clock
            clock += 5.0
            calls.append((flags, cache_scopes, compile_tree))
            return _projection(flags, tcf, source)

    def counter() -> float:
        return clock

    @contextmanager
    def memory_sample() -> Generator[dict[str, int]]:
        yield {"rss_start_bytes": 0, "rss_peak_sampled_bytes": 0, "rss_peak_delta_bytes": 0}

    monkeypatch.setattr(run_local_checks.time, "perf_counter", counter)
    monkeypatch.setattr(run_local_checks, "_sample_memory", memory_sample)
    measurements = run_local_checks.Measurements()
    upstream = types.SimpleNamespace(TCF=Model)
    runs = [RunRequest(1, {"e1": "A"})]
    _keys, stats = run_local_checks._measure_collection(  # pyright: ignore[reportPrivateUsage]
        tcf, runs, mode, upstream, measurements
    )
    assert calls == [((runs[0].arguments,), True, graph)]
    assert stats["seconds"] == 7.0
    assert stats["phase_seconds"]["parse"] == 2.0
    assert stats["phase_seconds"]["batch"] == 5.0
    assert stats["bulk_and_collection_seconds"] == 0.0
    assert stats["batch_metrics"]["input_group_clone_seconds"] == 17.0
    assert stats["batch_metrics"]["input_group_key_seconds"] == 19.0
    assert dict(stats["batch_metrics"]["input_group_fallbacks"]) == {"unsupported-command": 2}
    assert measurements.phase == ""


def _arguments(tmp_path: Path) -> list[str]:
    runs = tmp_path / "runs.json"
    runs.write_text("[{}]", encoding="utf-8")
    return [
        "run_local_checks",
        "--tcf",
        str(tmp_path / "model.tcf"),
        "--runs-json",
        str(runs),
        "--expect-runs",
        "1",
        "--label",
        "ANON",
        "--pytuflow-checkout",
        str(tmp_path / "parser"),
        "--output",
        str(tmp_path / "report"),
    ]


@pytest.mark.parametrize(
    "extra",
    [
        ["--graph-compare"],
        ["--batch", "--graph-compare"],
        ["--batch", "--graph-compare", "--storage-and-cache-conditions", ""],
        ["--batch", "--graph-compare", "--storage-and-cache-conditions", "SMB, warm", "--repeats", "3"],
        ["--batch", "--graph-compare", "--storage-and-cache-conditions", "SMB, warm", "--repeats", "5"],
        ["--batch", "--graph-compare", "--storage-and-cache-conditions", "SMB, warm", "--repeats", "0"],
    ],
)
def test_graph_cli_rejects_uncontrolled_or_unbalanced_comparisons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra: list[str]
) -> None:
    monkeypatch.setattr(sys, "argv", _arguments(tmp_path) + extra)
    with pytest.raises(SystemExit) as exc:
        run_local_checks.main()
    assert exc.value.code == 2


def _git(path: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *arguments],  # noqa: S607 - intentional developer-tool PATH lookup
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _native_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, dirty_repository: str | None = None
) -> tuple[list[str], str, list[tuple[str, tuple[int, ...]]]]:
    """Keep real source/HEAD provenance while controlling workspace state and collection."""
    arguments = _arguments(tmp_path)
    parser_checkout = tmp_path / "parser"
    sources = (
        "pytuflow/__init__.py",
        "pytuflow/_tmf/context.py",
        "pytuflow/_tmf/db/mat.py",
        "pytuflow/_tmf/cf/tcf_run_state.py",
        "pytuflow/_tmf/inp/inp_run_state.py",
        "pytuflow/_tmf/inp/inp_build_state.py",
        "pytuflow/_tmf/inp/db.py",
        "pytuflow/_tmf/deferred_database.py",
        "pytuflow/_tmf/batch_dependencies.py",
    )
    for source in sources:
        path = parser_checkout / source
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# Synthetic source provenance fixture.\n", encoding="utf-8")
    _git(parser_checkout, "init")
    _git(parser_checkout, "add", "pytuflow")
    _git(
        parser_checkout,
        "-c",
        "user.name=Diagnostic Test",
        "-c",
        "user.email=diagnostic@example.invalid",
        "commit",
        "--no-gpg-sign",
        "-m",
        "Add synthetic parser provenance",
    )
    parser_sha = _git(parser_checkout, "rev-parse", "HEAD")
    if dirty_repository == "parser":
        (parser_checkout / "pytuflow/_tmf/context.py").write_text(
            "# Changed after the recorded commit.\n", encoding="utf-8"
        )
    git_revision = run_local_checks._git_revision  # pyright: ignore[reportPrivateUsage]
    consumer_checkout = Path(__file__).absolute().parents[3]

    def revision(root: Path) -> dict[str, Any]:
        recorded = git_revision(root)
        if root == consumer_checkout:
            # An active developer's working copy must not determine the test outcome.
            recorded = {
                **recorded,
                "dirty": dirty_repository == "consumer",
                "status": " M synthetic_consumer_source.py" if dirty_repository == "consumer" else "",
            }
        return recorded

    monkeypatch.setattr(run_local_checks, "_git_revision", revision)
    module = types.SimpleNamespace(__file__=str(parser_checkout / "pytuflow/__init__.py"), __version__="1.2.0")
    monkeypatch.setitem(sys.modules, "pytuflow", module)
    monkeypatch.setattr(sys, "path", list(sys.path))
    flags = [{"e1": name} for name in ("A", "B", "C", "D", "A")]
    (tmp_path / "runs.json").write_text(json.dumps(flags), encoding="utf-8")
    arguments[arguments.index("--expect-runs") + 1] = str(len(flags))
    calls: list[tuple[str, tuple[int, ...]]] = []

    def measured(
        _tcf: Path,
        runs: list[RunRequest],
        mode: str,
        _upstream: Any,
        _measurements: run_local_checks.Measurements,
    ) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
        calls.append((mode, tuple(run.index for run in runs)))
        return {(run.index, run.description, "Read GIS", "source.txt", "") for run in runs}, _success_stats()

    def preflight(_tcf: Path, runs: list[RunRequest], *, context_mode: str = "individual") -> PreflightReport:
        assert context_mode in {"individual", "batch"}
        return PreflightReport(len(runs), checked_paths={"source.txt"})

    monkeypatch.setattr(run_local_checks, "_measure_collection", measured)
    monkeypatch.setattr(run_local_checks, "check_simulation_inputs", preflight)
    return (
        [*arguments, "--batch", "--graph-compare", "--storage-and-cache-conditions", "new SMB, warm cache"],
        parser_sha,
        calls,
    )


def test_native_graph_cli_rotates_four_full_trials_and_records_both_actual_revisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments, parser_sha, calls = _native_cli(tmp_path, monkeypatch)
    monkeypatch.setattr(sys, "argv", arguments)
    assert run_local_checks.main() == 0
    assert len(calls) == 20  # Four representative modes plus four complete four-mode trials.
    assert calls[:4] == [(mode, (1, 3, 5)) for mode in GRAPH_MODES]
    for trial in range(4):
        order = GRAPH_MODES[trial:] + GRAPH_MODES[:trial]
        assert calls[4 + trial * 4 : 8 + trial * 4] == [(mode, (1, 2, 3, 4, 5)) for mode in order]
    provenance = json.loads((tmp_path / "report/provenance.json").read_text(encoding="utf-8"))
    assert provenance["parser_git"]["head"] == parser_sha
    assert provenance["parser_git"]["dirty"] is False
    assert provenance["consumer_git"]["head"] == _git(Path(__file__).absolute().parents[3], "rev-parse", "HEAD")
    assert provenance["consumer_git"]["dirty"] is False
    assert provenance["consumer_git"]["status"] == ""
    assert provenance["storage_and_cache_conditions"] == "new SMB, warm cache"
    assert provenance["sources_clean"] is True
    assert provenance["source_findings"] == []
    summary = json.loads((tmp_path / "report/summary.json").read_text(encoding="utf-8"))
    assert summary["sources_clean"] is True
    assert summary["source_findings"] == []
    assert summary["passed"] is True
    assert summary["full_workload_validated"] is True
    assert summary["accepted_for_benchmark"] is True


@pytest.mark.parametrize("dirty_repository", ["parser", "consumer"])
@pytest.mark.parametrize("continue_on_findings", [False, True])
def test_dirty_source_graph_diagnostic_fails_acceptance_with_optional_continued_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, dirty_repository: str, continue_on_findings: bool
) -> None:
    arguments, parser_sha, calls = _native_cli(tmp_path, monkeypatch, dirty_repository=dirty_repository)
    report = tmp_path / "report"
    report.mkdir()
    (report / "summary.json").write_text(
        json.dumps({"passed": True, "full_workload_validated": True, "accepted_for_benchmark": True}), encoding="utf-8"
    )
    (report / "preflight.json").write_text(json.dumps({"passed": True, "completed": True}), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", arguments + (["--continue-on-findings"] if continue_on_findings else []))
    assert run_local_checks.main() == 1

    provenance = json.loads((report / "provenance.json").read_text(encoding="utf-8"))
    assert provenance["parser_git"]["head"] == parser_sha
    assert provenance[f"{dirty_repository}_git"]["dirty"] is True
    assert provenance["sources_clean"] is False
    assert len(provenance["source_findings"]) == 1
    assert dirty_repository in provenance["source_findings"][0]
    summary = json.loads((report / "summary.json").read_text(encoding="utf-8"))
    assert summary["sources_clean"] is False
    assert summary["source_findings"] == provenance["source_findings"]
    assert summary["passed"] is False
    assert summary["accepted_for_benchmark"] is False
    assert summary["completed"] is continue_on_findings
    assert summary["full_workload_validated"] is continue_on_findings
    preflight = json.loads((report / "preflight.json").read_text(encoding="utf-8"))
    assert preflight["passed"] is continue_on_findings
    assert preflight["timed"] is False
    assert preflight["batch_graph_policy"] == "parser_default"
    assert preflight["graph_comparison"] is False
    if continue_on_findings:
        assert len(calls) == 20
        rows = json.loads((report / "parity.json").read_text(encoding="utf-8"))
        assert len(rows) == 5
        assert all(row["passed"] is True for row in rows)
    else:
        assert calls == []
        assert preflight["completed"] is False
        assert not (report / "parity.json").exists()


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("dirty_repository", [None, "parser", "consumer"])
def test_legacy_diagnostics_keep_successful_exit_but_require_clean_sources_for_benchmark_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, batch: bool, dirty_repository: str | None
) -> None:
    arguments, _sha, calls = _native_cli(tmp_path, monkeypatch, dirty_repository=dirty_repository)
    arguments.remove("--graph-compare")
    if not batch:
        arguments.remove("--batch")
    monkeypatch.setattr(sys, "argv", arguments)
    assert run_local_checks.main() == 0
    modes = ("eager", "deferred", "batch") if batch else ("eager", "deferred")
    assert calls == [(mode, (1, 3, 5)) for mode in modes] + [(mode, (1, 2, 3, 4, 5)) for mode in modes]
    summary = json.loads((tmp_path / "report/summary.json").read_text(encoding="utf-8"))
    assert summary["passed"] is True
    assert summary["completed"] is True
    assert summary["full_workload_validated"] is True
    assert summary["sources_clean"] is (dirty_repository is None)
    assert summary["accepted_for_benchmark"] is (dirty_repository is None)
    if dirty_repository is None:
        assert summary["source_findings"] == []
    else:
        assert len(summary["source_findings"]) == 1
        assert dirty_repository in summary["source_findings"][0]


def test_representative_only_never_claims_full_workload_or_benchmark_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments, _sha, calls = _native_cli(tmp_path, monkeypatch)
    monkeypatch.setattr(sys, "argv", [*arguments, "--representative-only"])
    assert run_local_checks.main() == 0
    assert calls == [(mode, (1, 3, 5)) for mode in GRAPH_MODES]
    rows = json.loads((tmp_path / "report/parity.json").read_text(encoding="utf-8"))
    assert len(rows) == 1
    assert rows[0]["validation_scope"] == "representative_only"
    assert rows[0]["full_workload_validated"] is False
    assert rows[0]["accepted_for_benchmark"] is False
    summary = json.loads((tmp_path / "report/summary.json").read_text(encoding="utf-8"))
    assert summary["validation_scope"] == "representative_only"
    assert summary["full_workload_validated"] is False
    assert summary["accepted_for_benchmark"] is False


@pytest.mark.parametrize("continue_on_findings", [False, True])
@pytest.mark.parametrize("defect", ["exception", "collection_issue", "identity"])
def test_native_findings_never_return_success_even_when_coverage_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, continue_on_findings: bool, defect: str
) -> None:
    arguments, _sha, calls = _native_cli(tmp_path, monkeypatch)
    original = run_local_checks._measure_collection  # pyright: ignore[reportPrivateUsage]

    def measured(
        tcf: Path,
        runs: list[RunRequest],
        mode: str,
        upstream: Any,
        measurements: run_local_checks.Measurements,
    ) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
        identities, stats = original(tcf, runs, mode, upstream, measurements)
        if mode == "batch_graph_off":
            if defect == "exception":
                msg = "synthetic mode failure"
                raise ValueError(msg)
            if defect == "collection_issue":
                stats["collection_issues"] = 1
            else:
                identities = set[tuple[Any, ...]]()
        return identities, stats

    monkeypatch.setattr(run_local_checks, "_measure_collection", measured)
    monkeypatch.setattr(sys, "argv", arguments + (["--continue-on-findings"] if continue_on_findings else []))
    assert run_local_checks.main() == 1
    assert len(calls) == (20 if continue_on_findings else 4)
    rows = json.loads((tmp_path / "report/parity.json").read_text(encoding="utf-8"))
    assert len(rows) == (5 if continue_on_findings else 1)
    assert all(row["passed"] is False for row in rows)


def test_stopping_on_a_failed_gate_replaces_any_previous_success_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments, _sha, _calls = _native_cli(tmp_path, monkeypatch)
    report = tmp_path / "report"
    report.mkdir()
    (report / "summary.json").write_text(
        json.dumps({"passed": True, "full_workload_validated": True, "accepted_for_benchmark": True}), encoding="utf-8"
    )

    def failed(
        _tcf: Path,
        _runs: list[RunRequest],
        _mode: str,
        _upstream: Any,
        _measurements: run_local_checks.Measurements,
    ) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
        return set(), {"error": "synthetic failed collection"}

    monkeypatch.setattr(run_local_checks, "_measure_collection", failed)
    monkeypatch.setattr(sys, "argv", arguments)
    assert run_local_checks.main() == 1
    summary = json.loads((report / "summary.json").read_text(encoding="utf-8"))
    assert summary["passed"] is False
    assert summary["full_workload_validated"] is False
    assert summary["accepted_for_benchmark"] is False


@pytest.mark.parametrize("exception", [False, True])
def test_failed_public_preflight_invalidates_otherwise_complete_graph_comparison(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exception: bool
) -> None:
    arguments, _sha, calls = _native_cli(tmp_path, monkeypatch)

    def preflight(_tcf: Path, runs: list[RunRequest], *, context_mode: str = "individual") -> PreflightReport:
        if exception:
            msg = "synthetic public preflight error"
            raise ValueError(msg)
        report = PreflightReport(len(runs), checked_paths={"source.txt"})
        if context_mode == "batch":
            report.issues.append(
                PreflightIssue(1, runs[0].description, "missing", "Read GIS", "missing.txt", "missing")
            )
        return report

    monkeypatch.setattr(run_local_checks, "check_simulation_inputs", preflight)
    monkeypatch.setattr(sys, "argv", arguments)
    assert run_local_checks.main() == 1
    assert len(calls) == 20
    summary = json.loads((tmp_path / "report/summary.json").read_text(encoding="utf-8"))
    assert summary["passed"] is False
    assert summary["accepted_for_benchmark"] is False
    preflight_result = json.loads((tmp_path / "report/preflight.json").read_text(encoding="utf-8"))
    assert preflight_result["passed"] is False
    assert preflight_result["timed"] is False
    assert preflight_result["graph_comparison"] is False


def test_dirty_source_provenance_keeps_the_actual_git_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _arguments, sha, _calls = _native_cli(tmp_path, monkeypatch)
    checkout = tmp_path / "parser"
    (checkout / "pytuflow/_tmf/context.py").write_text("# Changed after the recorded commit.\n", encoding="utf-8")
    recorded = run_local_checks._git_revision(checkout)  # pyright: ignore[reportPrivateUsage]
    assert recorded["head"] == sha
    assert recorded["dirty"] is True
    assert "pytuflow/_tmf/context.py" in recorded["status"]


@pytest.mark.parametrize("mode", ["eager", "deferred"])
def test_failed_individual_context_does_not_shift_provenance_of_later_runs(tmp_path: Path, mode: str) -> None:
    tcf = tmp_path / "model.tcf"
    source = tmp_path / "source.txt"
    source.write_text("input", encoding="utf-8")

    class Input:
        lhs = "Read GIS Code"
        line_number = 1
        uuid = "source-command"
        has_missing_files = False
        file_references = (_Reference(source, source.parent, "Read GIS Code == source.txt"),)

        @property
        def files(self) -> list[Path]:
            return [source]

        def command(self) -> types.SimpleNamespace:
            return types.SimpleNamespace(parent=tcf, original_text="Read GIS Code == source.txt")

        def __str__(self) -> str:
            return "Read GIS Code == source.txt"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [Input()] if lhs is None or lhs.casefold() == "read gis code" else []

        def database_sources(self) -> tuple[object, ...]:
            return ()

        def deferred_database_sources(self) -> tuple[object, ...]:
            return ()

    class Model:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, flags: str, *, file_resolution: str = "eager") -> Context:
            assert file_resolution == mode
            if flags == "-e1 bad":
                msg = "synthetic first-context failure"
                raise ValueError(msg)
            return Context()

    runs = [RunRequest(17, {"e1": "bad"}), RunRequest(23, {"e1": "good"}), RunRequest(42, {"e1": "later"})]
    keys, stats = run_checks.collect(tcf, runs, mode, Model)
    assert stats["collection_issues"] == 1
    assert stats["inventory_issues"] == 0
    assert {key[0] for key in keys} == {23, 42}
    assert [entry[0] for entry in stats["command_provenance"]] == [23, 42]
    assert stats["provenance_complete"] is False
