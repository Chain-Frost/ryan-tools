# pyright: reportPrivateUsage=false
"""Real exporter failures must reach workflow outcomes and wrapper exit codes."""

import importlib.util
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from typing import Literal, cast
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.orchestrators.tuflow import _combination_workflow as workflow
from ryan_library.orchestrators.tuflow import po_combine
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("mode", ["parquet", "both", "fallback"])
def test_real_write_failure_reaches_dashboard_and_wrapper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool, mode: str
) -> None:
    monkeypatch.chdir(tmp_path)
    dashboard = LiveWorkflowDashboard(title="Export failure", enabled=False)
    results = Mock(spec=ProcessorCollection)
    results.processors = [object()]
    results.po_combine.return_value = pd.DataFrame({"value": [1]})

    def fail(*_args: object, **_kwargs: object) -> None:
        message = "synthetic export denial"
        raise PermissionError(message)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail)
    selected: Literal["excel", "parquet", "both"] = (
        "excel" if mode == "fallback" else "parquet" if mode == "parquet" else "both"
    )
    if mode == "fallback":
        from ryan_library.functions.excel_export import ExcelExporter

        monkeypatch.setattr(ExcelExporter, "MAX_EXCEL_ROWS", 1)
    with (
        patch.object(dashboard, "finish", wraps=dashboard.finish) as finish,
        patch.object(workflow, "LiveWorkflowDashboard", return_value=dashboard),
        patch.object(workflow, "setup_logger", return_value=nullcontext(Mock())),
        patch.object(workflow, "collect_files", return_value=[tmp_path / "input_PO.csv"]),
        patch.object(workflow, "process_files_in_parallel", return_value=cast("ProcessorCollection", results)),
    ):
        with pytest.raises(PermissionError, match="synthetic export denial"):
            po_combine.main_processing([tmp_path], export_mode=selected, use_live_dashboard=enabled)
        finish.assert_called_with(outcome="FAILED", detail="processing or export failed")
        assert list(tmp_path.iterdir()) == []

        source = (
            Path(__file__).absolute().parents[3] / "ryan-scripts/TUFLOW-python/po_and_timeseries/combine_po_results.py"
        )
        spec = importlib.util.spec_from_file_location("export_failure_wrapper", source)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        main = cast("Callable[..., int]", module.main)
        assert (
            main(
                working_directory=tmp_path,
                paths_to_process=(tmp_path,),
                export_mode=selected,
                use_live_dashboard=enabled,
            )
            == 1
        )
        finish.assert_called_with(outcome="FAILED", detail="processing or export failed")
        assert list(tmp_path.iterdir()) == []
