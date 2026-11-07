"""Real culvert exports retain selected channel values and truthful empty/failure outcomes."""

from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest
from openpyxl import load_workbook

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard, WorkflowOutcome
from ryan_library.functions.tuflow import tuflow_common
from ryan_library.orchestrators.tuflow import tuflow_culverts_mean, tuflow_culverts_merge, tuflow_culverts_timeseries


@dataclass(frozen=True)
class WorkflowCase:
    name: Literal["maximums", "timeseries", "mean", "median"]
    data_type: str
    sheets: tuple[str, ...]
    metric: str
    expected_values: tuple[float, ...]


CASES = (
    WorkflowCase("maximums", "Cmx", ("Maximums", "raw_data"), "Q", (5.0, 10.0)),
    WorkflowCase("timeseries", "Q", ("1d_timeseries_data",), "Q", (2.0, 4.0, 4.0, 8.0)),
    WorkflowCase("mean", "Cmx", ("aep-dur-mean", "aep-mean-max", "culvert-maximums"), "mean_Q", (7.5,)),
    WorkflowCase("median", "Cmx", ("aep-dur-med", "aep-med-max", "culvert-maximums"), "median_Q", (10.0,)),
)


@pytest.fixture(autouse=True)
def serial_processing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the real file processors while avoiding unrelated worker startup."""

    def pool_size(*, num_files: int) -> int:
        return min(num_files, 1)

    monkeypatch.setattr(tuflow_common, "calculate_pool_size", pool_size)


@pytest.fixture
def workflow_outcomes(monkeypatch: pytest.MonkeyPatch) -> list[WorkflowOutcome]:
    observed: list[WorkflowOutcome] = []
    original = LiveWorkflowDashboard.finish

    def finish(dashboard: LiveWorkflowDashboard, *, outcome: WorkflowOutcome, detail: str = "") -> None:
        observed.append(outcome)
        original(dashboard, outcome=outcome, detail=detail)

    monkeypatch.setattr(LiveWorkflowDashboard, "finish", finish)
    return observed


def _write_inputs(folder: Path, case: WorkflowCase, *, malformed: bool = False) -> dict[Path, bytes]:
    folder.mkdir()
    for pattern, multiplier in ((1, 1), (2, 2)):
        path = folder / f"Synthetic_01.00p_00120m_TP{pattern:02d}_1d_{case.data_type}.csv"
        if malformed:
            content = "unexpected,columns\nnot,a valid result\n"
        elif case.data_type == "Cmx":
            content = (
                "Chan ID,Time Qmax,Qmax,Vmax,Time Vmax\n"
                f"C01,0.5,{5 * multiplier},1.5,0.5\n"
                f"C02,0.5,{50 * multiplier},3.0,0.5\n"
            )
        else:
            content = (
                "Descriptor,Time,Q C01,Q C02\n"
                f"row,0,{2 * multiplier},{20 * multiplier}\n"
                f"row,1,{4 * multiplier},{40 * multiplier}\n"
            )
        path.write_text(content, encoding="utf-8")
    return {path: path.read_bytes() for path in folder.iterdir()}


def _run(
    case: WorkflowCase,
    inputs: Path,
    output: Path,
    *,
    selection: Collection[str] | None,
    dashboard: bool,
) -> bool:
    output.mkdir()
    if case.name in {"maximums", "timeseries"}:
        workflow = (
            tuflow_culverts_merge.main_processing
            if case.name == "maximums"
            else tuflow_culverts_timeseries.main_processing
        )
        return workflow(
            paths_to_process=[inputs],
            include_data_types=[case.data_type],
            locations_to_include=selection,
            output_dir=output,
            use_live_dashboard=dashboard,
        )
    report = (
        tuflow_culverts_mean.run_culvert_mean_report
        if case.name == "mean"
        else tuflow_culverts_mean.run_culvert_median_report
    )
    return report(
        script_directory=output,
        paths_to_process=[inputs],
        include_data_types=[case.data_type],
        locations_to_include=selection,
        export_raw=True,
        use_live_dashboard=dashboard,
    )


def _read_sheets(output: Path, names: tuple[str, ...]) -> dict[str, tuple[tuple[object, ...], ...]]:
    books = list(output.glob("*.xlsx"))
    assert len(books) == 1, books
    workbook = load_workbook(books[0], read_only=True, data_only=True)
    try:
        assert set(names).issubset(workbook.sheetnames)
        result: dict[str, tuple[tuple[object, ...], ...]] = {}
        for name in names:
            rows = tuple(tuple(row) for row in workbook[name].values)
            assert len(rows) > 1, (name, rows)
            assert "Chan ID" in rows[0], (name, rows[0])
            result[name] = rows
        return result
    finally:
        workbook.close()


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
@pytest.mark.parametrize("dashboard", [False, True])
def test_filtered_exports_preserve_selected_values_and_all_channel_sheets(
    case: WorkflowCase,
    dashboard: bool,
    tmp_path: Path,
    workflow_outcomes: list[WorkflowOutcome],
) -> None:
    inputs = tmp_path / "inputs"
    before = _write_inputs(inputs, case)
    reference_directory = tmp_path / "reference"
    selected_directory = tmp_path / "selected"
    assert _run(case, inputs, reference_directory, selection=None, dashboard=False)
    assert _run(case, inputs, selected_directory, selection=["C01"], dashboard=dashboard)

    reference = _read_sheets(reference_directory, case.sheets)
    selected = _read_sheets(selected_directory, case.sheets)
    for name in case.sheets:
        header, *reference_rows = reference[name]
        channel_index = header.index("Chan ID")
        assert {row[channel_index] for row in reference_rows} == {"C01", "C02"}
        selected_header, *selected_rows = selected[name]
        assert selected_header == header
        assert {row[channel_index] for row in selected_rows} == {"C01"}
        expected_rows = [row for row in reference_rows if row[channel_index] == "C01"]
        assert sorted(selected_rows, key=repr) == sorted(expected_rows, key=repr)

    header, *rows = selected[case.sheets[0]]
    metric_index = header.index(case.metric)
    values: list[float] = []
    for row in rows:
        value = row[metric_index]
        assert isinstance(value, (int, float))
        assert not isinstance(value, bool)
        values.append(float(value))
    assert sorted(values) == list(case.expected_values)
    assert workflow_outcomes == ["SUCCESS", "SUCCESS"]
    assert all(path.read_bytes() == content for path, content in before.items())


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
@pytest.mark.parametrize("dashboard", [False, True])
def test_unmatched_channel_is_skipped_without_creating_reports(
    case: WorkflowCase,
    dashboard: bool,
    tmp_path: Path,
    workflow_outcomes: list[WorkflowOutcome],
) -> None:
    inputs = tmp_path / "inputs"
    _write_inputs(inputs, case)
    output = tmp_path / "reports"

    assert _run(case, inputs, output, selection=["missing-channel"], dashboard=dashboard)

    assert workflow_outcomes == ["SKIPPED"]
    assert list(output.iterdir()) == []


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
@pytest.mark.parametrize("dashboard", [False, True])
def test_malformed_input_still_fails_instead_of_becoming_a_filter_skip(
    case: WorkflowCase,
    dashboard: bool,
    tmp_path: Path,
    workflow_outcomes: list[WorkflowOutcome],
) -> None:
    inputs = tmp_path / "inputs"
    _write_inputs(inputs, case, malformed=True)
    output = tmp_path / "reports"

    assert not _run(case, inputs, output, selection=["C01"], dashboard=dashboard)

    assert workflow_outcomes == ["FAILED"]
    assert list(output.iterdir()) == []
