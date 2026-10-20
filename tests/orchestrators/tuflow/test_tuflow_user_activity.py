"""Focused tests for integrated TUFLOW user activity reporting."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from ryan_library.functions.tuflow.codemeter import CodeMeterAllocation, CodeMeterSnapshot
from ryan_library.orchestrators.tuflow.tuflow_user_activity import (
    TlfInspection,
    TuflowActivityReport,
    _deduplicate_starts,  # pyright: ignore[reportPrivateUsage]
    build_activity_report,
    inspect_tlf,
    parse_activity_line,
    read_user_activity,
    render_activity_report,
)


def test_concurrent_starts_remain_independent_when_one_finishes(tmp_path: Path) -> None:
    user_directory = tmp_path / "Engineer.Name"
    user_directory.mkdir()
    global_log = user_directory / "_ All TUFLOW Simulations.log"
    lines = (
        '2026-Oct-20 09:00  Don: 0001 LOC 1/2 HOST Build: test Started: Model_A x.tcf "Q:\\A.tlf"',
        '2026-Oct-20 09:01  Don: 0001 LOC 2/2 HOST Build: test Started: Model_B x.tcf "Q:\\B.tlf"',
        '2026-Oct-20 10:00  Don: 0001 LOC 1/2 HOST Build: test Tot: 1:00:00 CT Finished: Model_A "Q:\\A.tlf"',
    )
    global_log.write_text("\n".join(lines), encoding="utf-8")

    activity = read_user_activity(user_directory, activity_since=datetime(2026, 10, 20, 8, 0))

    assert [record.model for record in activity.recent_starts] == ["Model_B", "Model_A"]
    assert [record.model for record in activity.recent_completions] == ["Model_A"]


def test_inspect_tlf_uses_completion_and_gpu_parser() -> None:
    fixture = (
        Path("tests") / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "log" / "EG00_001.tlf"
    ).absolute()

    inspection = inspect_tlf(fixture)

    assert inspection.status == "COMPLETE"
    assert inspection.gpu_device_ids == "0"
    assert inspection.runtime_seconds is not None


def test_network_and_other_documented_licence_origins() -> None:
    source = Path("global.log")
    for marker, expected_origin, expected_seat in (
        ("NWK 01/20", "NWK", "01/20"),
        ("NWK03/10", "NWK", "03/10"),
        ("LOC3/4", "LOC", "3/4"),
        ("HalcS", "HALCS", "-"),
        ("HalcN", "HALCN", "-"),
        ("XP", "XP", "-"),
        ("TUT", "TUT", "-"),
    ):
        record = parse_activity_line(
            "2026-Oct-20 09:00  Don: 0001 " + marker + ' HOST Build: 2026.0 Started: Model_A model.tcf "Q:\\\\A.tlf"',
            source=source,
        )
        assert record is not None, marker
        assert record.event == "STARTED"
        assert record.licence_origin == expected_origin
        assert record.seat == expected_seat


def test_network_simulation_start_and_completion_are_reported(tmp_path: Path) -> None:
    user_directory = tmp_path / "Network.User"
    user_directory.mkdir()
    lines = (
        '2026-Oct-20 09:00 Don: 0001 NWK 01/20 HOST Build: test Started: Model_A x.tcf "Q:\\A.tlf"',
        ('2026-Oct-20 09:50 Don: 0001 NWK 01/20 HOST Build: test Tot: 0:50:00 CT Finished:Model_A x.tcf "Q:\\A.tlf"'),
    )
    (user_directory / "_ All TUFLOW Simulations.log").write_text("\n".join(lines), encoding="utf-8")

    activity = read_user_activity(user_directory, activity_since=datetime(2026, 10, 20, 8))
    assert len(activity.recent_starts) == 1
    assert len(activity.recent_completions) == 1
    assert activity.recent_completions[0].model == "Model_A"
    assert activity.recent_completions[0].runtime_seconds == 3000


def test_repeated_simulations_of_same_model_are_not_deduplicated() -> None:
    first = parse_activity_line(
        '2026-Oct-20 09:00 Don: 0001 LOC 1/2 HOST Build: test Started: Model_A x.tcf "Q:\\\\A.tlf"',
        source=Path("a.log"),
    )
    second = parse_activity_line(
        '2026-Oct-20 10:00 Don: 0001 LOC 1/2 HOST Build: test Started: Model_A x.tcf "Q:\\\\A.tlf"',
        source=Path("b.log"),
    )
    assert first is not None
    assert second is not None
    assert len(_deduplicate_starts([("USER", first), ("USER", second), ("USER", first)])) == 2


def test_compact_report_keeps_two_starts_before_oldest_active_model(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer.Name"
    user_dir.mkdir()
    models = [
        ("Oldest", 7),
        ("Older2", 6),
        ("Older3", 5),
        ("RunningOld", 4),
        ("CompletedNew", 3),
        ("RunningNew", 2),
        ("Newest", 1),
    ]
    event_lines: list[tuple[datetime, str]] = []
    for seat, (model, hours_ago) in enumerate(models, start=1):
        started = now - timedelta(hours=hours_ago)
        prefix = f"{started:%Y-%b-%d %H:%M} Don: 0001 NWK {seat}/8 HOST Build: test "
        path = f"Q:\\TUFLOW_Project\\results\\Log\\{model}.tlf"
        event_lines.append((started, f'{prefix}Started: {model} test.tcf "{path}"'))
        if not model.startswith("Running"):
            finished = started + timedelta(minutes=20)
            prefix = f"{finished:%Y-%b-%d %H:%M} Don: 0001 NWK {seat}/8 HOST Build: test "
            event_lines.append((finished, f'{prefix}Tot: 0:20:00 CT Finished: {model} "{path}"'))
    event_lines.sort(key=lambda item: item[0])
    (user_dir / "All TUFLOW Simulations.log").write_text("\n".join(line for _, line in event_lines), encoding="utf-8")

    snapshot = CodeMeterSnapshot(
        products=(),
        allocations=tuple(
            CodeMeterAllocation(
                handle_id=seat,
                user=r"BGER\Engineer.Name",
                process_id=1000 + seat,
                product_code=26,
                product_name="TUFLOW GPU",
                first_access=now - timedelta(hours=age),
                last_access=now,
            )
            for seat, age in ((4, 4), (6, 2))
        ),
        denials=(),
        warnings=(),
    )

    def inspect_mock(path: Path | None) -> TlfInspection:
        model = str(path)
        return TlfInspection(status="INCOMPLETE" if "Running" in model else "COMPLETE")

    with (
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_user_activity.fetch_codemeter_snapshot",
            return_value=snapshot,
        ),
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_user_activity.inspect_tlf",
            side_effect=inspect_mock,
        ),
    ):
        report = build_activity_report(log_root=tmp_path, recent_hours=2, codemeter_url="http://localhost:22352")
        full = build_activity_report(
            log_root=tmp_path,
            recent_hours=24,
            codemeter_url="http://localhost:22352",
            all_history=True,
        )

    assert [row.record.model for row in report.starts] == [
        "Newest",
        "RunningNew",
        "CompletedNew",
        "RunningOld",
        "Older3",
        "Older2",
    ]
    assert [row.record.model for row in report.completions] == ["Newest", "CompletedNew", "Older3", "Older2"]
    assert [row.record.model for row in full.starts] == [
        "Newest",
        "RunningNew",
        "CompletedNew",
        "RunningOld",
        "Older3",
        "Older2",
        "Oldest",
    ]
    assert len(report.codemeter_processes) == 2
    assert all(row.simulation is not None for row in report.codemeter_processes)


def test_no_active_codemeter_allocations_limits_to_latest_three(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer.Name"
    user_dir.mkdir()
    lines = [
        f"{now - timedelta(hours=i):%Y-%b-%d %H:%M} Don: 0001 LOC {i}/8 HOST "
        f'Build: test Started: Model_{i} test.tcf "Q:\\Model_{i}.tlf"'
        for i in range(1, 6)
    ]
    (user_dir / "All Simulations.log").write_text("\n".join(reversed(lines)), encoding="utf-8")
    snapshot = CodeMeterSnapshot(products=(), allocations=(), denials=(), warnings=())
    with (
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_user_activity.fetch_codemeter_snapshot",
            return_value=snapshot,
        ),
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_user_activity.inspect_tlf",
            return_value=TlfInspection(status="INCOMPLETE"),
        ),
    ):
        report = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url="localhost")
    assert [row.record.model for row in report.starts] == ["Model_1", "Model_2", "Model_3"]


def test_terminal_links_and_full_model_names() -> None:
    long_name = "Blacksmith_v14_WeelMLTP_EXG_20.00p_0720m_TP07_CC_32M"
    timestamp = datetime(2026, 10, 20, 15, 27)
    record = parse_activity_line(
        f"2026-Oct-20 15:27 Don: 0001 NWK 1/2 HOST Build: test Started: {long_name} "
        f'test.tcf "Q:\\TUFLOW_Blacksmith\\results\\Log\\{long_name}.tlf"',
        source=Path("global.log"),
    )
    assert record is not None
    from ryan_library.orchestrators.tuflow.tuflow_user_activity import SimulationReportRow

    start_row = SimulationReportRow(
        user="Engineer.Name",
        record=record,
        inspection=TlfInspection(status="INCOMPLETE"),
        status="POSSIBLY RUNNING",
    )
    report = TuflowActivityReport(
        checked_at=timestamp,
        log_root=Path("C:/ProgramData/TUFLOW/log"),
        recent_hours=24,
        starts=(start_row,),
        completions=(),
        codemeter=None,
        codemeter_processes=(),
        access_warnings=(),
        workflow_warnings=(),
    )
    plain = render_activity_report(report)
    linked = render_activity_report(report, hyperlinks=True)
    assert long_name in plain
    assert long_name in linked
    assert "\x1b]8;;" not in plain
    assert "\x1b]8;;file:///Q:/TUFLOW_Blacksmith/" in linked
    assert "\x1b]8;;" in linked
