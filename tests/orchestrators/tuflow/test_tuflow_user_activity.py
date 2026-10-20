"""Focused tests for integrated TUFLOW user activity reporting."""

from datetime import datetime, timedelta
from os import utime
from pathlib import Path
from unittest.mock import patch

from ryan_library.functions.tuflow.codemeter import CodeMeterAllocation, CodeMeterSnapshot
from ryan_library.orchestrators.tuflow.tuflow_user_activity import (
    SimulationReportRow,
    TlfInspection,
    TuflowActivityReport,
    _deduplicate_starts,  # pyright: ignore[reportPrivateUsage]
    build_activity_report,
    inspect_tlf,
    parse_activity_line,
    read_simulation_progress,
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


def test_completed_runtime_is_shown_alongside_age_in_starts_table(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer"
    user_dir.mkdir()
    first_start = now - timedelta(minutes=20)
    first_end = first_start + timedelta(minutes=3)
    second_start = now - timedelta(minutes=10)
    third_start = now - timedelta(minutes=5)
    lines = [
        (
            f"{first_start:%Y-%b-%d %H:%M} Don: 0001 LOC 1/2 HOST Build: test "
            'Started: CompletedFromLog x.tcf "Q:\\CompletedFromLog.tlf"'
        ),
        (
            f"{first_end:%Y-%b-%d %H:%M} Don: 0001 LOC 1/2 HOST Build: test "
            'Tot: 0:02:05 CT Finished: CompletedFromLog "Q:\\CompletedFromLog.tlf"'
        ),
        (
            f"{second_start:%Y-%b-%d %H:%M} Don: 0001 LOC 2/2 HOST Build: test "
            'Started: CompletedFromTLF x.tcf "Q:\\CompletedFromTLF.tlf"'
        ),
        (
            f"{third_start:%Y-%b-%d %H:%M} Don: 0001 LOC 1/2 HOST Build: test "
            'Started: StillRunning x.tcf "Q:\\StillRunning.tlf"'
        ),
    ]
    (user_dir / "All Simulations.log").write_text("\n".join(lines), encoding="utf-8")
    snapshot = CodeMeterSnapshot(products=(), allocations=(), denials=(), warnings=())

    def fake_tlf(path: Path | None) -> TlfInspection:
        if path is not None and "StillRunning" in str(path):
            return TlfInspection(status="INCOMPLETE", runtime_seconds=950)
        return TlfInspection(status="COMPLETE", runtime_seconds=180)

    with (
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_user_activity.fetch_codemeter_snapshot",
            return_value=snapshot,
        ),
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_user_activity.inspect_tlf",
            side_effect=fake_tlf,
        ),
    ):
        report = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url="http://localhost:22352")

    by_model = {row.record.model: row for row in report.starts}
    assert by_model["CompletedFromLog"].runtime_seconds == 125
    assert by_model["CompletedFromLog"].status == "COMPLETED"
    assert by_model["CompletedFromTLF"].runtime_seconds == 180
    assert by_model["StillRunning"].runtime_seconds is None
    console = render_activity_report(report)
    starts_section = console.split("SELECTED SIMULATION STARTS", maxsplit=1)[1].split(
        "MATCHING SIMULATION COMPLETIONS", maxsplit=1
    )[0]
    assert "RUNTIME" in starts_section.splitlines()[1]
    log_row = next(line for line in starts_section.splitlines() if "CompletedFromLog" in line)
    tlf_row = next(line for line in starts_section.splitlines() if "CompletedFromTLF" in line)
    running_row = next(line for line in starts_section.splitlines() if "StillRunning" in line)
    assert "2m 05s" in log_row
    assert "3m 00s" in tlf_row
    assert "950" not in running_row


def test_tlf_fallback_searches_seven_days_for_oldest_active(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer"
    user_dir.mkdir()
    models = [
        ("Oldest", 44),
        ("Predecessor", 42),
        ("RunningOld", 30),
        ("NewerA", 4),
        ("NewerB", 3),
        ("NewerC", 2),
        ("NewerD", 1),
    ]
    lines: list[tuple[datetime, str]] = []
    for seat, (model, age) in enumerate(models, start=1):
        started = now - timedelta(hours=age)
        lines.append(
            (
                started,
                (
                    f"{started:%Y-%b-%d %H:%M} Don: 0001 NWK {seat}/8 HOST Build: test "
                    f'Started: {model} x.tcf "Q:\\{model}.tlf"'
                ),
            )
        )
        if model != "RunningOld":
            finished = started + timedelta(minutes=12)
            lines.append(
                (
                    finished,
                    (
                        f"{finished:%Y-%b-%d %H:%M} Don: 0001 NWK {seat}/8 HOST Build: test "
                        f'Tot: 0:12:00 CT Finished: {model} "Q:\\{model}.tlf"'
                    ),
                )
            )
    lines.sort(key=lambda item: item[0])
    (user_dir / "All Simulations.log").write_text("\n".join(line for _, line in lines), encoding="utf-8")

    def fake_inspect(path: Path | None) -> TlfInspection:
        return TlfInspection(status="INCOMPLETE" if path is not None and "RunningOld" in str(path) else "COMPLETE")

    with patch(
        "ryan_library.orchestrators.tuflow.tuflow_user_activity.inspect_tlf",
        side_effect=fake_inspect,
    ):
        report = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url=None)
    assert [row.record.model for row in report.starts] == [
        "NewerD",
        "NewerC",
        "NewerB",
        "NewerA",
        "RunningOld",
        "Predecessor",
        "Oldest",
    ]
    assert report.starts[4].status == "POSSIBLY RUNNING"


def test_interrupted_and_unstable_logs_are_terminal_not_completed_or_active(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer"
    user_dir.mkdir()
    lines: list[tuple[datetime, str]] = []
    models = ["InterruptedOld", "UnstableOld", "InterruptedRecent", "UnstableRecent", "StillRunning"]
    for index, name in enumerate(models):
        started = now - timedelta(minutes=50 - 10 * index)
        prefix = f"{started:%Y-%b-%d %H:%M} Don: 0001 NWK 01/05 HOST Build: test "
        lines.append((started, f'{prefix}Started: {name} model.tcf "Q:\\{name}.tlf"'))
        if name.startswith("Interrupted"):
            ended = started + timedelta(minutes=1)
            end_prefix = f"{ended:%Y-%b-%d %H:%M} Don: 0001 NWK 01/05 HOST Build: test "
            lines.append(
                (
                    ended,
                    f'{end_prefix}Ini: 0:00:01 CT Tot: 0:00:58 CT Interrupted: {name} model.tcf "Q:\\{name}.tlf"',
                )
            )
        elif name.startswith("Unstable"):
            ended = started + timedelta(minutes=2)
            end_prefix = f"{ended:%Y-%b-%d %H:%M} Don: 0001 NWK 01/05 HOST Build: test "
            # Some logs omit timing and the colon after the status.
            lines.append((ended, f'{end_prefix}UNSTABLE model.tcf "Q:\\{name}.tlf"'))
    lines.sort(key=lambda item: item[0])
    (user_dir / "_ All TUFLOW Simulations.log").write_text("\n".join(line for _, line in lines), encoding="utf-8")

    with patch(
        "ryan_library.orchestrators.tuflow.tuflow_user_activity.inspect_tlf",
        return_value=TlfInspection(status="INCOMPLETE", runtime_seconds=9999),
    ):
        report = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url=None)
        full = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url=None, all_history=True)

    assert [row.record.model for row in report.starts] == [
        "StillRunning",
        "UnstableRecent",
        "InterruptedRecent",
    ]
    assert [row.status for row in report.starts] == ["POSSIBLY RUNNING", "UNSTABLE", "INTERRUPTED"]
    assert [row.record.model for row in report.terminations] == ["UnstableRecent", "InterruptedRecent"]
    assert [row.status for row in report.terminations] == ["UNSTABLE", "INTERRUPTED"]
    assert len(report.completions) == 0
    assert report.starts[1].runtime_seconds is None
    assert report.starts[2].runtime_seconds == 58
    assert len(full.terminations) == 4

    rendered = render_activity_report(report)
    assert "INTERRUPTED AND UNSTABLE SIMULATION TERMINATIONS" in rendered
    assert "58s" in rendered
    assert "COMPLETED" not in rendered
    assert "9999" not in rendered


def test_terminal_parser_does_not_relabel_timed_interruption_as_finished() -> None:
    interrupted = parse_activity_line(
        "2026-Oct-20 09:00 Don: 0001 LOC 1/2 HOST Build: test "
        'Ini: 0:00:10 CT Tot: 0:05:30 CT Interrupted: X "Q:\\X.tlf"',
        source=Path("global.log"),
    )
    unstable = parse_activity_line(
        '2026-Oct-20 09:01 Don: 0001 LOC 1/2 HOST Build: test UNSTABLE "Q:\\X.tlf"',
        source=Path("global.log"),
    )
    assert interrupted is not None
    assert interrupted.event == "INTERRUPTED"
    assert interrupted.runtime_seconds == 330
    assert unstable is not None
    assert unstable.event == "UNSTABLE"
    assert unstable.model.startswith("(not recorded")


def test_active_codemeter_process_does_not_rematch_to_terminated_start(tmp_path: Path) -> None:
    """A failed start closer to first access must not steal a live allocation."""
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer"
    user_dir.mkdir()
    running_at = now - timedelta(minutes=30)
    interrupted_at = now - timedelta(minutes=25)
    unstable_at = now - timedelta(minutes=23)
    events = [
        (running_at, 1, 'Started: RunningModel x.tcf "Q:\\RunningModel.tlf"'),
        (interrupted_at, 2, 'Started: InterruptedModel x.tcf "Q:\\InterruptedModel.tlf"'),
        (
            interrupted_at + timedelta(minutes=1),
            2,
            'Tot: 0:01:00 CT Interrupted: InterruptedModel "Q:\\InterruptedModel.tlf"',
        ),
        (unstable_at, 3, 'Started: UnstableModel x.tcf "Q:\\UnstableModel.tlf"'),
        (
            unstable_at + timedelta(minutes=1),
            3,
            'UNSTABLE "Q:\\UnstableModel.tlf"',
        ),
    ]
    lines = [
        f"{stamp:%Y-%b-%d %H:%M} Don: 0001 NWK {seat}/8 HOST Build: test {event}"
        for stamp, seat, event in sorted(events, key=lambda item: item[0])
    ]
    (user_dir / "All Simulations.log").write_text("\n".join(lines), encoding="utf-8")
    snapshot = CodeMeterSnapshot(
        products=(),
        allocations=(
            CodeMeterAllocation(
                handle_id=7,
                user=r"BGER\Engineer",
                process_id=6543,
                product_code=26,
                product_name="TUFLOW GPU",
                first_access=interrupted_at + timedelta(seconds=30),
                last_access=now,
            ),
        ),
        denials=(),
        warnings=(),
    )
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
        compact = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url="localhost")
        full = build_activity_report(log_root=tmp_path, recent_hours=24, codemeter_url="localhost", all_history=True)

    for report in (compact, full):
        assert {row.record.model: row.status for row in report.starts} == {
            "RunningModel": "POSSIBLY RUNNING",
            "InterruptedModel": "INTERRUPTED",
            "UnstableModel": "UNSTABLE",
        }
        assert len(report.codemeter_processes) == 1
        process = report.codemeter_processes[0]
        assert process.process_id == 6543
        assert process.simulation is not None
        assert process.simulation.record.model == "RunningModel"
        assert process.simulation.status == "POSSIBLY RUNNING"
        assert {row.status for row in report.terminations} == {"INTERRUPTED", "UNSTABLE"}


def test_tsf_live_time_reads_progress_and_ignores_invalid_or_finished_summaries(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    tlf = tmp_path / "Long_Model_Name.tlf"
    tsf = tlf.with_suffix(".tsf")
    record = parse_activity_line(
        f"{now - timedelta(minutes=15):%Y-%b-%d %H:%M} Don: 0001 NWK 1/2 HOST Build: test "
        f'Started: Long_Model_Name x.tcf "{tlf}"',
        source=tmp_path / "global.log",
    )
    assert record is not None
    tsf.write_text(
        f'TUFLOW Log File == "{tlf}"\n'
        "Simulation Status == Started\n"
        "Simulation Start Time (h) == 0.\n"
        "Simulation Time (h) == 12.375\n"
        "Simulation End Time (h) == 48.\n"
        "Percentage Complete (%) == 25.78125\n",
        encoding="utf-8",
    )
    progress = read_simulation_progress(record)
    assert progress is not None
    assert progress.model_time_hours == 12.375
    assert progress.end_time_hours == 48.0
    assert progress.percentage_complete == 25.78125

    # The summary may be present even after a run has finished or crashed.
    tsf.write_text(tsf.read_text().replace("Status == Started", "Status == UNSTABLE"), encoding="utf-8")
    assert read_simulation_progress(record) is None
    tsf.write_text(tsf.read_text().replace("Status == UNSTABLE", "Status == Finished"), encoding="utf-8")
    assert read_simulation_progress(record) is None
    tsf.write_text(
        'TUFLOW Log File == "Q:\\Wrong_Model.tlf"\n'
        "Simulation Status == Started\nSimulation Time (h) == 1\nSimulation End Time (h) == 48\n",
        encoding="utf-8",
    )
    assert read_simulation_progress(record) is None
    tsf.write_text(
        "Simulation Status == Started\nSimulation Time (h) == nan\nSimulation End Time (h) == 48\n",
        encoding="utf-8",
    )
    assert read_simulation_progress(record) is None
    tsf.unlink()
    assert read_simulation_progress(record) is None


def test_live_tsf_values_are_reported_only_for_active_start(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    user_dir = tmp_path / "Engineer"
    user_dir.mkdir()
    active_started = now - timedelta(minutes=15)
    completed_started = now - timedelta(minutes=35)
    running_tlf = tmp_path / "Running.tlf"
    finished_tlf = tmp_path / "Finished.tlf"
    log_lines = [
        (
            f"{completed_started:%Y-%b-%d %H:%M} Don: 0001 NWK 1/2 HOST Build: test "
            f'Started: Finished x.tcf "{finished_tlf}"'
        ),
        (
            f"{completed_started + timedelta(minutes=4):%Y-%b-%d %H:%M} Don: 0001 NWK 1/2 HOST Build: test "
            f'Tot: 0:04:00 CT Finished: Finished "{finished_tlf}"'
        ),
        (f'{active_started:%Y-%b-%d %H:%M} Don: 0001 NWK 1/2 HOST Build: test Started: Running x.tcf "{running_tlf}"'),
    ]
    (user_dir / "All Simulations.log").write_text("\n".join(log_lines), encoding="utf-8")
    for tlf in (running_tlf, finished_tlf):
        tlf.with_suffix(".tsf").write_text(
            f'TUFLOW Log File == "{tlf}"\n'
            "Simulation Status == Started\n"
            "Simulation Start Time (h) == 0\n"
            "Simulation Time (h) == 21.50\n"
            "Simulation End Time (h) == 48.00\n"
            "Percentage Complete (%) == 44.79\n",
            encoding="utf-8",
        )
    snapshot = CodeMeterSnapshot(
        products=(),
        allocations=(
            CodeMeterAllocation(
                handle_id=10,
                user=r"BGER\Engineer",
                process_id=9876,
                product_code=26,
                product_name="TUFLOW GPU",
                first_access=active_started,
                last_access=now,
            ),
        ),
        denials=(),
        warnings=(),
    )
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
    by_model = {row.record.model: row for row in report.starts}
    assert by_model["Running"].progress is not None
    assert by_model["Running"].progress.model_time_hours == 21.5
    assert by_model["Finished"].status == "COMPLETED"
    assert by_model["Finished"].progress is None
    assert len(report.codemeter_processes) == 1
    assert report.codemeter_processes[0].simulation is not None
    assert report.codemeter_processes[0].simulation.progress == by_model["Running"].progress
    rendered = render_activity_report(report)
    assert "SIM TIME" in rendered
    assert "SIM END" in rendered
    assert "PROGRESS" in rendered
    assert "21.500h" in rendered
    assert "48.000h" in rendered
    assert "44.8%" in rendered


def test_tsf_timestamp_predating_start_is_not_reported(tmp_path: Path) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    tlf = tmp_path / "Reused.tlf"
    tsf = tlf.with_suffix(".tsf")
    started = now - timedelta(minutes=5)
    record = parse_activity_line(
        f'{started:%Y-%b-%d %H:%M} Don: 0001 NWK 1/2 HOST Build: test Started: Reused x.tcf "{tlf}"',
        source=tmp_path / "global.log",
    )
    assert record is not None
    tsf.write_text(
        "Simulation Status == Started\nSimulation Time (h) == 22\nSimulation End Time (h) == 48\n",
        encoding="utf-8",
    )
    old = (started - timedelta(minutes=8)).timestamp()
    utime(tsf, (old, old))
    assert read_simulation_progress(record) is None
