"""Tests for the GDAL flood-extent orchestrator.

These tests verify the public API of the orchestrator module. The underlying
``calculate_flood_extent`` and ``polygonize_flood_extent`` calls are mocked so
the suite does not require real GDAL rasters.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ryan_library.orchestrators.gdal.gdal_flood_extent import (
    format_cutoff_value,
    main_processing,
    process_file,
)


class TestFormatCutoff:
    """Test the filename-safe cutoff formatter."""

    def test_zero(self) -> None:
        assert format_cutoff_value(0.0) == "0"

    def test_half(self) -> None:
        assert format_cutoff_value(0.5) == "05"

    def test_one_point_two(self) -> None:
        assert format_cutoff_value(1.2) == "12"

    def test_integer(self) -> None:
        assert format_cutoff_value(10.0) == "10"


class TestProcessFile:
    """Validate process_file delegate calls."""

    @patch(
        "ryan_library.orchestrators.gdal.gdal_flood_extent.polygonize_flood_extent",
    )
    @patch(
        "ryan_library.orchestrators.gdal.gdal_flood_extent.calculate_flood_extent",
    )
    def test_process_file_calls_calc_and_poly(self, mock_calc: MagicMock, mock_poly: MagicMock, tmp_path: Path) -> None:
        """Both flood-extent and polygonize should be called."""
        source: Path = tmp_path / "test_d_HR_Max.tif"
        source.write_bytes(b"\x00")

        mock_calc.return_value = None
        mock_poly.return_value = None

        outputs: list[Path] = process_file(source, cutoff_values=(0.0,), overwrite=True)

        mock_calc.assert_called_once()
        mock_poly.assert_called_once()
        assert len(outputs) == 2  # raster + vector pair


class TestMainProcessing:
    """Smoke-test main_processing with mocked internals."""

    @patch(
        "ryan_library.orchestrators.gdal.gdal_flood_extent.polygonize_flood_extent",
    )
    @patch(
        "ryan_library.orchestrators.gdal.gdal_flood_extent.calculate_flood_extent",
    )
    def test_main_processing_no_files(self, mock_calc: MagicMock, mock_poly: MagicMock, tmp_path: Path) -> None:
        """When no files match, an empty list is returned and no processing occurs."""
        result: list[Path] = main_processing(
            paths_to_process=[tmp_path],
            file_patterns=("*.nonexistent",),
        )
        assert result == []
        mock_calc.assert_not_called()
        mock_poly.assert_not_called()

    @patch(
        "ryan_library.orchestrators.gdal.gdal_flood_extent.polygonize_flood_extent",
    )
    @patch(
        "ryan_library.orchestrators.gdal.gdal_flood_extent.calculate_flood_extent",
    )
    def test_main_processing_with_files(self, mock_calc: MagicMock, mock_poly: MagicMock, tmp_path: Path) -> None:
        """Matched files should be processed."""
        raster: Path = tmp_path / "test_d_HR_Max.tif"
        raster.write_bytes(b"\x00")

        result: list[Path] = main_processing(
            paths_to_process=[tmp_path],
            file_patterns=("*_d_HR_Max.tif",),
            overwrite=True,
        )
        mock_calc.assert_called_once()
        mock_poly.assert_called_once()
        assert len(result) == 2


@pytest.mark.gdal
@pytest.mark.slow
class TestIntegration:
    """Integration tests running against actual GDAL bindings and test files."""

    def test_gdal_flood_extent_integration(self, tmp_path: Path) -> None:
        """Run the full flood extent process on a real raster without mocks."""
        import shutil

        # Use surface.flt and surface.hdr from the test data
        test_data_dir = Path("tests/test_data/raster/conversion")
        if not test_data_dir.exists():
            pytest.skip("Raster test data not found.")

        src_flt = test_data_dir / "surface.flt"
        src_hdr = test_data_dir / "surface.hdr"

        # Copy to tmp_path to avoid modifying the test_data directory
        target_flt = tmp_path / "test_d_HR_Max.flt"
        target_hdr = tmp_path / "test_d_HR_Max.hdr"
        shutil.copy(src_flt, target_flt)
        shutil.copy(src_hdr, target_hdr)

        # Run actual processing
        # cutoff=0.5 will create test_d_HR_Max_fextent05.tif and .gpkg
        result: list[Path] = main_processing(
            paths_to_process=[tmp_path], file_patterns=("test_d_HR_Max.flt",), cutoff_values=(0.5,), overwrite=True
        )

        assert len(result) == 2
        tif_path = next(p for p in result if p.suffix == ".tif")
        gpkg_path = next(p for p in result if p.suffix == ".gpkg")

        assert tif_path.exists()
        assert gpkg_path.exists()
        assert "FE_05m" in tif_path.name
        assert "FE_05m" in gpkg_path.name
