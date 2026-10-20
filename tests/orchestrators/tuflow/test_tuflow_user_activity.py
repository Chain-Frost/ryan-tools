"""Focused tests for integrated TUFLOW user activity reporting."""

from datetime import datetime
from pathlib import Path

from ryan_library.orchestrators.tuflow.tuflow_user_activity import (
    _deduplicate_starts,
    inspect_tlf,
    parse_activity_line,
    read_user_activity,
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
        Path("tests")
        / "test_data"
        / "tuflow"
        / "TUFLOW_Example_Model_Dataset"
        / "log"
        / "EG00_001.tlf"
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
            '2026-Oct-20 09:00  Don: 0001 '
            + marker
            + ' HOST Build: 2026.0 Started: Model_A model.tcf "Q:\\\\A.tlf"',
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
        '2026-Oct-20 09:50 Don: 0001 NWK 01/20 HOST Build: test Tot: 0:50:00 CT '
        'Finished:Model_A x.tcf "Q:\\A.tlf"',
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
