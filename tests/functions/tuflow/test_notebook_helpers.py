import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.functions.tuflow.notebook_helpers import (
    _resolve_parallel,  # pyright: ignore[reportPrivateUsage]
    init_notebook_logging,
    is_notebook,
    load_tuflow_data,
    plot_hydrographs,
    run_closure_durations,
    run_culvert_maximums,
    run_culvert_mean_peaks,
    run_culvert_timeseries,
    run_log_summary,
    run_po_combine,
    run_pomm_combine,
    run_timeseries_peaks_check,
    run_timeseries_stability,
)
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def test_is_notebook_false() -> None:
    with patch.dict(sys.modules, {"IPython": None}):
        assert is_notebook() is False


def test_is_notebook_true() -> None:
    mock_ipython = MagicMock()
    mock_shell = MagicMock()
    mock_shell.__class__.__name__ = "ZMQInteractiveShell"
    mock_ipython.get_ipython.return_value = mock_shell
    with patch.dict(sys.modules, {"IPython": mock_ipython}):
        assert is_notebook() is True


def test_init_notebook_logging() -> None:
    with patch("ryan_library.functions.tuflow.notebook_helpers.configure_notebook_logging") as mock_configure:
        init_notebook_logging("SUCCESS", log_file="notebook.log", file_log_level="DEBUG")
        init_notebook_logging("INFO")

    assert mock_configure.call_count == 2
    mock_configure.assert_any_call(
        console_log_level="SUCCESS",
        log_file="notebook.log",
        file_log_level="DEBUG",
    )
    mock_configure.assert_any_call(
        console_log_level="INFO",
        log_file=None,
        file_log_level="DEBUG",
    )


def test_resolve_parallel() -> None:
    assert _resolve_parallel(True) is True  # pyright: ignore[reportPrivateUsage]
    assert _resolve_parallel(False) is False  # pyright: ignore[reportPrivateUsage]

    with patch("ryan_library.functions.tuflow.notebook_helpers.is_notebook", return_value=True):
        assert _resolve_parallel(None) is False  # pyright: ignore[reportPrivateUsage]

    with patch("ryan_library.functions.tuflow.notebook_helpers.is_notebook", return_value=False):
        assert _resolve_parallel(None) is True  # pyright: ignore[reportPrivateUsage]


class TestLoadTuflowData:
    @pytest.fixture
    def mock_data_setup(self):
        temp_dir = Path(tempfile.mkdtemp())
        (temp_dir / "sim_1_Q.csv").touch()
        (temp_dir / "sim_1_V.csv").touch()
        yield temp_dir
        shutil.rmtree(temp_dir)

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    @patch("ryan_library.functions.tuflow.notebook_helpers.process_files_in_parallel")
    def test_load_tuflow_data_parallel(
        self, mock_process_parallel: MagicMock, mock_collect: MagicMock, mock_data_setup: Path
    ) -> None:
        mock_collect.return_value = [mock_data_setup / "sim_1_Q.csv"]
        mock_collection = MagicMock(spec=ProcessorCollection)
        mock_collection.processors = []
        mock_process_parallel.return_value = mock_collection

        result = load_tuflow_data(paths=[str(mock_data_setup)], data_types=["Q"], parallel=True)

        mock_collect.assert_called_once()
        mock_process_parallel.assert_called_once()
        assert result == mock_collection

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    @patch("ryan_library.functions.tuflow.notebook_helpers.process_file")
    def test_load_tuflow_data_serial(
        self, mock_process_file: MagicMock, mock_collect: MagicMock, mock_data_setup: Path
    ) -> None:
        files = [mock_data_setup / "sim_1_Q.csv", mock_data_setup / "sim_1_V.csv"]
        mock_collect.return_value = files

        mock_proc = MagicMock()
        mock_proc.processed = True
        mock_proc.df = pd.DataFrame({"A": [1]})
        mock_proc.file_name = "test.csv"

        mock_process_file.return_value = mock_proc

        result = load_tuflow_data(paths=[mock_data_setup], data_types=["Q"], parallel=False)

        assert mock_collect.call_count == 1
        assert mock_process_file.call_count == 2
        assert isinstance(result, ProcessorCollection)
        assert len(result.processors) == 2

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    def test_load_tuflow_data_no_files(self, mock_collect: MagicMock, mock_data_setup: Path) -> None:
        mock_collect.return_value = []
        result = load_tuflow_data(paths=[mock_data_setup], data_types=["Q"])
        assert isinstance(result, ProcessorCollection)
        assert len(result.processors) == 0

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    @patch("ryan_library.functions.tuflow.notebook_helpers.process_files_in_parallel")
    def test_load_tuflow_data_with_location_filter(
        self, mock_process_parallel: MagicMock, mock_collect: MagicMock, mock_data_setup: Path
    ) -> None:
        mock_collect.return_value = [mock_data_setup / "sim_1_Q.csv"]
        mock_collection = MagicMock(spec=ProcessorCollection)
        mock_collection.processors = []
        mock_process_parallel.return_value = mock_collection

        load_tuflow_data(paths=[mock_data_setup], data_types=["Q"], locations=["LocA"])
        mock_collection.filter_locations.assert_called_with(["LocA"])


@patch("ryan_library.functions.tuflow.notebook_helpers.load_tuflow_data")
def test_run_pomm_combine(mock_load: MagicMock) -> None:
    mock_collection = MagicMock()
    mock_collection.pomm_combine.return_value = pd.DataFrame({"A": [1]})
    mock_load.return_value = mock_collection

    df = run_pomm_combine(["path"])
    mock_load.assert_called_once()
    mock_collection.pomm_combine.assert_called_once()
    assert not df.empty


@patch("ryan_library.functions.tuflow.notebook_helpers.load_tuflow_data")
def test_run_po_combine(mock_load: MagicMock) -> None:
    mock_collection = MagicMock()
    mock_collection.po_combine.return_value = pd.DataFrame({"A": [1]})
    mock_load.return_value = mock_collection

    df = run_po_combine(["path"])
    mock_load.assert_called_once()
    mock_collection.po_combine.assert_called_once()
    assert not df.empty


@patch("ryan_library.functions.tuflow.notebook_helpers.load_tuflow_data")
def test_run_culvert_maximums(mock_load: MagicMock) -> None:
    mock_collection = MagicMock()
    mock_collection.combine_1d_maximums.return_value = pd.DataFrame({"A": [1]})
    mock_collection.combine_raw.return_value = pd.DataFrame({"B": [2]})
    mock_load.return_value = mock_collection

    result = run_culvert_maximums(["path"])
    mock_load.assert_called_once()
    mock_collection.combine_1d_maximums.assert_called_once()
    mock_collection.combine_raw.assert_called_once()
    assert not result.maximums.empty and not result.raw_data.empty
    assert result.processor_collection is mock_collection


@patch("ryan_library.functions.tuflow.notebook_helpers.load_tuflow_data")
def test_run_culvert_timeseries(mock_load: MagicMock) -> None:
    mock_collection = MagicMock()
    mock_collection.combine_1d_timeseries.return_value = pd.DataFrame({"A": [1]})
    mock_load.return_value = mock_collection

    df = run_culvert_timeseries(["path"])
    mock_load.assert_called_once()
    mock_collection.combine_1d_timeseries.assert_called_once()
    assert not df.empty


@patch("ryan_library.functions.tuflow.notebook_helpers.load_tuflow_data")
@patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.find_culvert_aep_mean_max")
@patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.find_culvert_aep_dur_mean")
def test_run_culvert_mean_peaks(mock_find_mean: MagicMock, mock_find_max: MagicMock, mock_load: MagicMock) -> None:
    mock_collection = MagicMock()
    mock_collection.processors = [MagicMock()]
    mock_collection.combine_1d_maximums.return_value = pd.DataFrame({"A": [1]})
    mock_load.return_value = mock_collection

    mock_find_mean.return_value = pd.DataFrame({"M": [1]})
    mock_find_max.return_value = pd.DataFrame({"Max": [2]})

    df1, df2 = run_culvert_mean_peaks(["path"])
    mock_load.assert_called_once()
    mock_find_mean.assert_called_once()
    mock_find_max.assert_called_once()
    assert not df1.empty and not df2.empty


@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.build_log_summary_dataframe")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.process_log_file_for_dashboard")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.discover_log_files")
def test_run_log_summary(mock_discover: MagicMock, mock_process: MagicMock, mock_build: MagicMock) -> None:
    mock_discover.return_value = [Path("fake.tlf")]
    mock_process.return_value = MagicMock(status="OK")
    mock_build.return_value = pd.DataFrame({"A": [1]})

    with tempfile.TemporaryDirectory() as tmpdir:
        df = run_log_summary([tmpdir])

    mock_discover.assert_called_once()
    mock_process.assert_called_once()
    mock_build.assert_called_once()
    assert not df.empty


@patch("ryan_library.functions.tuflow.notebook_helpers.load_tuflow_data")
@patch("ryan_library.functions.tuflow.closure_durations_functions.collect_po_data")
@patch("ryan_library.functions.tuflow.closure_durations_functions.calculate_threshold_durations")
@patch("ryan_library.functions.tuflow.closure_durations_functions.summarise_results")
def test_run_closure_durations(
    mock_summarise: MagicMock, mock_calc: MagicMock, mock_collect: MagicMock, mock_load: MagicMock
) -> None:
    mock_collection = MagicMock()
    mock_collection.processors = [MagicMock()]
    mock_load.return_value = mock_collection

    mock_collect.return_value = pd.DataFrame({"A": [1]})
    mock_calc.return_value = pd.DataFrame({"B": [2]})
    mock_summarise.return_value = pd.DataFrame({"C": [3]})

    df1, df2 = run_closure_durations(["path"])
    mock_load.assert_called_once()
    mock_collect.assert_called_once_with(collection=mock_collection)
    mock_calc.assert_called_once()
    mock_summarise.assert_called_once()
    assert not df1.empty and not df2.empty


@patch("ryan_library.functions.tuflow.po_timeseries_checks.analyze_stability_csv")
@patch("ryan_library.functions.tuflow.po_timeseries_checks.flatten_stability_results")
def test_run_timeseries_stability(mock_flatten: MagicMock, mock_analyze: MagicMock) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "sim_1_PO.csv").touch()

        mock_analyze.return_value = []
        mock_flatten.return_value = [{"A": 1}]

        df = run_timeseries_stability([tmpdir])
        mock_analyze.assert_called_once()
        mock_flatten.assert_called_once()
        assert not df.empty


@patch("ryan_library.functions.tuflow.po_timeseries_checks.analyze_peak_csv")
@patch("ryan_library.functions.tuflow.po_timeseries_checks.flatten_peak_results")
def test_run_timeseries_peaks_check(mock_flatten: MagicMock, mock_analyze: MagicMock) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        (tmp_path / "sim_1_PO.csv").touch()

        mock_analyze.return_value = []
        mock_flatten.return_value = [{"A": 1}]

        df = run_timeseries_peaks_check([tmpdir])
        mock_analyze.assert_called_once()
        mock_flatten.assert_called_once()
        assert not df.empty


@patch("matplotlib.pyplot.subplots")
@patch("matplotlib.pyplot.show")
def test_plot_hydrographs(mock_show: MagicMock, mock_subplots: MagicMock) -> None:
    mock_ax = MagicMock()
    mock_fig = MagicMock()
    mock_subplots.return_value = (mock_fig, mock_ax)

    df = pd.DataFrame({"Time": [0, 1], "Q": [10, 20], "Chan ID": ["C1", "C1"]})
    plot_hydrographs(df, time_col="Time", value_col="Q", group_col="Chan ID")
    mock_show.assert_called_once()
    mock_ax.plot.assert_called_once()
