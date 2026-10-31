"""Verify representative packaged wrapper CLIs in a fully provisioned environment."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).absolute().parents[2]


# Explicitly safe to execute with --help (protected argparse entry points).
# Do not execute unguarded legacy scripts or assume all 84 wheel resources
# implement a CLI: README and __init__ files are resources, not executables.
SAFE_HELP_WRAPPERS: tuple[str, ...] = (
    "culvert.py",
    "floodway.py",
    "RORB-python/RORB-find-closure-durations.py",
    "RORB-python/plot_ensemble_results.py",
    "RORB-python/hydrograph_extract_rorb_csv.py",
    "TUFLOW-python/culvert_results/combine_culvert_maximums.py",
    "TUFLOW-python/log_processing/check_tuflow_user_activity.py",
    "TUFLOW-python/culvert_results/combine_culvert_timeseries.py",
    "TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py",
    "TUFLOW-python/culvert_results/create_median_culvert_peak_report.py",
    "TUFLOW-python/gis_processing/apply_qgis_styles_to_results.py",
    "TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py",
    "TUFLOW-python/log_processing/create_log_summary_report.py",
    "TUFLOW-python/po_and_timeseries/calculate_po_closure_durations.py",
    "TUFLOW-python/po_and_timeseries/check_po_timeseries_peaks.py",
    "TUFLOW-python/po_and_timeseries/check_timeseries_stability.py",
    "TUFLOW-python/po_and_timeseries/combine_po_results.py",
    "TUFLOW-python/model_management/verify_converted_tuflow_rasters.py",
    "TUFLOW-python/po_and_timeseries/create_pomm_mean_peak_report.py",
    "TUFLOW-python/raster_processing/run_asc_to_asc_mean_then_maximum.py",
    "file-management-python/archive_compress_folder.py",
    "file-management-python/archive_extract.py",
    "gdal-python/gdal_merge.py",
    "gdal-python/gdal_set_nodata.py",
    "gdal-python/recompress_geotiffs.py",
    "misc-python/remove_excel_protection.py",
    "tuflow/init_tuflow_project.py",
    "tuflow/tuflow_to_hy8.py",
)


@pytest.mark.parametrize("relative", SAFE_HELP_WRAPPERS)
def test_maintained_wrapper_help_runs_without_project_data(relative: str) -> None:
    script = ROOT / "ryan-scripts" / relative
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=45,
    )
    assert result.returncode == 0, f"{relative} help failed: {result.stderr[-1000:]}"
    assert "usage:" in result.stdout.lower()
