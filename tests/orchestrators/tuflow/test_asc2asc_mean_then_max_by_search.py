"""Tests for ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search."""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search import (
    _parse_raster,
    discover_rasters,
    discover_mean_jobs,
    discover_max_jobs,
    run_mean_then_max_workflow,
    ParsedRaster,
    MeanJobDetails,
)
from ryan_library.functions.tuflow.asc_to_asc_statistics import StatisticJob, StageExecutionSummary


class TestParseRaster:
    def test_parse_raster_success(self, tmp_path: Path):
        file_path = tmp_path / "model_EXG_1%AEP_2hr_001_d_Max.asc"
        file_path.touch()

        with patch(
            "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.TuflowStringParser"
        ) as MockParser:
            parser_instance = MockParser.return_value
            parser_instance.data_type = "d"
            parser_instance.aep = MagicMock(original_text="1%AEP")
            parser_instance.duration = MagicMock(original_text="2hr", raw_value="120")
            parser_instance.tp = MagicMock(original_text="001", raw_value="1")
            parser_instance.run_code_parts = {"scenario": "EXG"}
            parser_instance.trim_run_code = "model_EXG"

            with patch(
                "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.result_type_from_parser",
                return_value="d",
            ):
                parsed = _parse_raster(
                    input_file=file_path, grid_directory=tmp_path, scenarios=["EXG"], result_types=["d"]
                )

                assert parsed is not None
                assert parsed.scenario == "EXG"
                assert parsed.aep == "1%AEP"
                assert parsed.duration == "2hr"
                assert parsed.tp_number == 1
                assert parsed.result_type == "d"
                assert parsed.mean_name == "model_EXG_1%AEP_2hr_TPMean_d_Max.asc"
                assert parsed.max_name == "model_EXG_1%AEP_TPMean-DurMax_d_Max.asc"

    def test_parse_raster_missing_components(self, tmp_path: Path):
        file_path = tmp_path / "model_EXG_1%AEP_2hr_d_Max.asc"
        file_path.touch()

        with patch(
            "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.TuflowStringParser"
        ) as MockParser:
            parser_instance = MockParser.return_value
            parser_instance.data_type = "d"
            parser_instance.aep = None
            parser_instance.duration = None
            parser_instance.tp = None

            with patch(
                "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.result_type_from_parser",
                return_value="d",
            ):
                with pytest.raises(ValueError, match="Could not parse AEP, duration, and TP"):
                    _parse_raster(input_file=file_path, grid_directory=tmp_path, scenarios=["EXG"], result_types=["d"])

    def test_parse_raster_multiple_scenarios(self, tmp_path: Path):
        file_path = tmp_path / "model_EXG_DEV_1%AEP_2hr_001_d_Max.asc"

        with patch(
            "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.TuflowStringParser"
        ) as MockParser:
            parser_instance = MockParser.return_value
            parser_instance.data_type = "d"
            parser_instance.aep = MagicMock(original_text="1%AEP")
            parser_instance.duration = MagicMock(original_text="2hr", raw_value="120")
            parser_instance.tp = MagicMock(original_text="001", raw_value="1")
            parser_instance.run_code_parts = {"scen1": "EXG", "scen2": "DEV"}

            with patch(
                "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.result_type_from_parser",
                return_value="d",
            ):
                with pytest.raises(ValueError, match="Expected one scenario"):
                    _parse_raster(
                        input_file=file_path, grid_directory=tmp_path, scenarios=["EXG", "DEV"], result_types=["d"]
                    )


class TestDiscoverRasters:
    def test_discover_rasters_no_grids(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="No grids directories"):
            discover_rasters(search_root=tmp_path, input_glob="*.asc", scenarios=["EXG"], result_types=["d"])

    def test_discover_rasters_no_supported_rasters(self, tmp_path: Path):
        (tmp_path / "grids").mkdir()
        with patch(
            "ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search._parse_raster", return_value=None
        ):
            with pytest.raises(FileNotFoundError, match="No supported ensemble result rasters"):
                discover_rasters(search_root=tmp_path, input_glob="*.asc", scenarios=["EXG"], result_types=["d"])


class TestDiscoverMeanJobs:
    def test_duplicate_tp(self, tmp_path: Path):
        raster1 = ParsedRaster(Path("f1.asc"), tmp_path, "EXG", "1%", "2hr", 120.0, 1, "d", "model", "m.asc", "max.asc")
        raster2 = ParsedRaster(Path("f2.asc"), tmp_path, "EXG", "1%", "2hr", 120.0, 1, "d", "model", "m.asc", "max.asc")

        with pytest.raises(ValueError, match="Duplicate temporal patterns"):
            discover_mean_jobs(rasters=[raster1, raster2], output_root=tmp_path, expected_tps=frozenset([1, 2]))

    def test_missing_tp(self, tmp_path: Path):
        raster1 = ParsedRaster(Path("f1.asc"), tmp_path, "EXG", "1%", "2hr", 120.0, 1, "d", "model", "m.asc", "max.asc")

        jobs, incomplete = discover_mean_jobs(rasters=[raster1], output_root=tmp_path, expected_tps=frozenset([1, 2]))
        assert len(jobs) == 0
        assert len(incomplete) == 1
        assert "missing TP02" in incomplete[0]


class TestDiscoverMaxJobs:
    def test_duplicate_duration(self, tmp_path: Path):
        job = StatisticJob("lbl", "op", (), Path("out"))
        m1 = MeanJobDetails(job, tmp_path, "EXG", "1%", "2hr", 120.0, "d", "model", "max.asc")
        m2 = MeanJobDetails(job, tmp_path, "EXG", "1%", "2hr", 120.0, "d", "model", "max.asc")

        with pytest.raises(ValueError, match="Duplicate mean durations"):
            discover_max_jobs(mean_jobs=[m1, m2], output_root=tmp_path)


class TestWorkflow:
    def test_workflow_validation_errors(self, tmp_path: Path):
        # Invalid workers
        res = run_mean_then_max_workflow(
            executable=Path("dummy"),
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
            workers=0,
        )
        assert res == 1

        # Invalid refresh
        res = run_mean_then_max_workflow(
            executable=Path("dummy"),
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
            live_refresh_per_second=0,
        )
        assert res == 1

        # Missing executable
        res = run_mean_then_max_workflow(
            executable=tmp_path / "nonexistent.exe",
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
        )
        assert res == 1

    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_rasters")
    def test_workflow_discovery_error(self, mock_discover, tmp_path: Path):
        exe = tmp_path / "asc2asc.exe"
        exe.touch()
        mock_discover.side_effect = FileNotFoundError("No files")

        res = run_mean_then_max_workflow(
            executable=exe,
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
        )
        assert res == 1

    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_rasters")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_mean_jobs")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_max_jobs")
    def test_workflow_incomplete_strict(self, mock_max, mock_mean, mock_discover, tmp_path: Path):
        exe = tmp_path / "asc2asc.exe"
        exe.touch()
        mock_discover.return_value = []
        mock_mean.return_value = ([], ["incomplete group"])
        mock_max.return_value = []

        res = run_mean_then_max_workflow(
            executable=exe,
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
            strict=True,
        )
        assert res == 1

    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_rasters")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_mean_jobs")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_max_jobs")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.run_statistic_stage")
    def test_workflow_success(self, mock_stage, mock_max, mock_mean, mock_discover, tmp_path: Path):
        exe = tmp_path / "asc2asc.exe"
        exe.touch()

        dummy_job = MagicMock()
        dummy_job.label = "lbl"
        dummy_job.input_files = []

        dummy_mean = MagicMock()
        dummy_mean.job = dummy_job

        mock_discover.return_value = ["dummy_raster"]
        mock_mean.return_value = ([dummy_mean], [])
        mock_max.return_value = [dummy_job]

        # Mean stage ok, max stage ok
        mock_stage.side_effect = [StageExecutionSummary(1, 1, 0), StageExecutionSummary(1, 1, 0)]

        res = run_mean_then_max_workflow(
            executable=exe,
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
        )
        assert res == 0
        assert mock_stage.call_count == 2

    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_rasters")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_mean_jobs")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.discover_max_jobs")
    @patch("ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search.run_statistic_stage")
    def test_workflow_mean_failure(self, mock_stage, mock_max, mock_mean, mock_discover, tmp_path: Path):
        exe = tmp_path / "asc2asc.exe"
        exe.touch()

        dummy_job = MagicMock()
        dummy_job.label = "lbl"
        dummy_job.input_files = []

        dummy_mean = MagicMock()
        dummy_mean.job = dummy_job

        mock_discover.return_value = ["dummy_raster"]
        mock_mean.return_value = ([dummy_mean], [])
        mock_max.return_value = [dummy_job]

        # Mean stage fails
        mock_stage.return_value = StageExecutionSummary(1, 0, 1)

        res = run_mean_then_max_workflow(
            executable=exe,
            search_root=tmp_path,
            output_root=tmp_path,
            input_glob="*.asc",
            expected_tps=frozenset([1]),
            scenarios=["EXG"],
            result_types=["d"],
        )
        assert res == 1
        assert mock_stage.call_count == 1
