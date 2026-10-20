"""Focused tests for integrated TUFLOW user activity reporting."""

from datetime import datetime
from pathlib import Path

from ryan_library.orchestrators.tuflow.tuflow_user_activity import inspect_tlf, read_user_activity


def test_concurrent_starts_remain_independent_when_one_finishes(tmp_path: Path) -> None:
    user_directory = tmp_path / "Engineer.Name"
    user_directory.mkdir()
    global_log = user_directory / "_ All TUFLOW Simulations.log"
    global_log.write_text(
        "\n".join(
            (
                '2026-Oct-20 09:00  Don: 0001 LOC 1/2 HOST Build: test Started: Model_A x.tcf "Q:\\A.tlf"',
                '2026-Oct-20 09:01  Don: 0001 LOC 2/2 HOST Build: test Started: Model_B x.tcf "Q:\\B.tlf"',
                '2026-Oct-20 10:00  Don: 0001 LOC 1/2 HOST Build: test Tot: 1:00:00 CT Finished: Model_A "Q:\\A.tlf"',
            )
        ),
        encoding="utf-8",
    )

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
