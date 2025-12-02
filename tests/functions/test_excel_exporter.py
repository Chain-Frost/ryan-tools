"""Tests for ExcelExporter class in misc_functions."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from ryan_library.functions.misc_functions import ExcelExporter

class TestExcelExporterHelpers:
    """Test helper methods of ExcelExporter."""
    
    def test_exceeds_excel_limits_normal(self):
        """Test that normal DataFrames don't exceed limits."""
        exporter = ExcelExporter()
        df = pd.DataFrame({"A": range(100), "B": range(100)})
        assert not exporter._exceeds_excel_limits([df])
    
    def test_exceeds_excel_limits_too_many_rows(self):
        """Test detection of too many rows."""
        exporter = ExcelExporter()
        # Create a DF that exceeds row limit
        huge_index = range(exporter.MAX_EXCEL_ROWS + 1)
        df = pd.DataFrame({"A": huge_index})
        assert exporter._exceeds_excel_limits([df])
    
    def test_exceeds_excel_limits_too_many_columns(self):
        """Test detection of too many columns."""
        exporter = ExcelExporter()
        # Create a DF that exceeds column limit
        df = pd.DataFrame({f"Col{i}": [1] for i in range(exporter.MAX_EXCEL_COLUMNS + 1)})
        assert exporter._exceeds_excel_limits([df])
    
    def test_resolve_export_stem_with_file_name(self):
        """Test stem resolution with explicit filename."""
        exporter = ExcelExporter()
        stem = exporter._resolve_export_stem(
            datetime_string="20250101-1200",
            export_key="Report",
            file_name="custom.xlsx"
        )
        assert stem == "custom"
    
    def test_resolve_export_stem_without_extension(self):
        """Test stem resolution without xlsx extension."""
        exporter = ExcelExporter()
        stem = exporter._resolve_export_stem(
            datetime_string="20250101-1200",
            export_key="Report",
            file_name="custom"
        )
        assert stem == "custom"
    
    def test_resolve_export_stem_default(self):
        """Test default stem generation."""
        exporter = ExcelExporter()
        stem = exporter._resolve_export_stem(
            datetime_string="20250101-1200",
            export_key="Report",
            file_name=None
        )
        assert stem == "20250101-1200_Report"
    
    def test_build_parquet_filename_gzip(self):
        """Test parquet filename with gzip compression."""
        exporter = ExcelExporter()
        filename = exporter._build_parquet_filename("base", "gzip")
        assert filename == "base.parquet.gzip"
    
    def test_build_parquet_filename_other_compression(self):
        """Test parquet filename with other compression."""
        exporter = ExcelExporter()
        filename = exporter._build_parquet_filename("base", "snappy")
        assert filename == "base.parquet.snappy"
    
    def test_build_parquet_filename_no_compression(self):
        """Test parquet filename without compression."""
        exporter = ExcelExporter()
        filename = exporter._build_parquet_filename("base", None)
        assert filename == "base.parquet"
    
    def test_build_output_path_with_directory(self):
        """Test output path building with directory."""
        exporter = ExcelExporter()
        path = exporter._build_output_path("file.xlsx", Path("/tmp"))
        assert path == Path("/tmp/file.xlsx")
    
    def test_build_output_path_without_directory(self):
        """Test output path building without directory."""
        exporter = ExcelExporter()
        path = exporter._build_output_path("file.xlsx", None)
        assert path == Path("file.xlsx")
    
    def test_sanitize_name(self):
        """Test name sanitization."""
        exporter = ExcelExporter()
        assert exporter._sanitize_name("My-Sheet_1") == "My-Sheet_1"
        assert exporter._sanitize_name("Invalid@Name#Here!") == "Invalid_Name_Here"
        assert exporter._sanitize_name("___") == "Sheet"
        assert exporter._sanitize_name("") == "Sheet"

class TestExcelExporterExport:
    """Test export functionality."""
    
    def test_export_dataframes_validation_error_mismatch(self):
        """Test error when dataframes and sheets don't match."""
        exporter = ExcelExporter()
        export_dict = {
            "Report": {
                "dataframes": [pd.DataFrame({"A": [1]})],
                "sheets": ["Sheet1", "Sheet2"]  # Mismatch
            }
        }
        with pytest.raises(ValueError, match="number of dataframes"):
            exporter.export_dataframes(export_dict)
    
    def test_export_dataframes_invalid_mode(self):
        """Test error with invalid export mode."""
        exporter = ExcelExporter()
        export_dict = {
            "Report": {
                "dataframes": [pd.DataFrame({"A": [1]})],
                "sheets": ["Sheet1"]
            }
        }
        with pytest.raises(ValueError, match="Invalid export_mode"):
            exporter.export_dataframes(export_dict, export_mode="invalid")
    
    def test_export_dataframes_file_name_multiple_workbooks(self):
        """Test error when file_name provided for multiple workbooks."""
        exporter = ExcelExporter()
        export_dict = {
            "Report1": {"dataframes": [pd.DataFrame()], "sheets": ["S1"]},
            "Report2": {"dataframes": [pd.DataFrame()], "sheets": ["S2"]}
        }
        with pytest.raises(ValueError, match="single workbook"):
            exporter.export_dataframes(export_dict, file_name="custom.xlsx")
    
    @patch("pandas.DataFrame.to_parquet")
    def test_export_as_parquet_only(self, mock_to_parquet, tmp_path):
        """Test parquet-only export."""
        exporter = ExcelExporter()
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        
        exporter._export_as_parquet_only(
            export_stem="test",
            dataframes=[df],
            sheets=["Sheet1"],
            output_directory=tmp_path,
            compression="gzip"
        )
        
        mock_to_parquet.assert_called_once()
    
    @patch("pandas.DataFrame.to_csv")
    @patch("pandas.DataFrame.to_parquet")
    def test_export_as_parquet_and_csv(self, mock_to_parquet,mock_to_csv, tmp_path):
        """Test combined parquet and CSV export."""
        exporter = ExcelExporter()
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        
        exporter._export_as_parquet_and_csv(
            export_stem="test",
            dataframes=[df],
            sheets=["Sheet1"],
            output_directory=tmp_path,
            compression="gzip"
        )
        
        mock_to_parquet.assert_called_once()
        mock_to_csv.assert_called_once()
    
    def test_calculate_column_widths(self):
        """Test column width calculation."""
        exporter = ExcelExporter()
        df = pd.DataFrame({
            "Short": ["A", "B"],
            "LongerColumn": ["Value1", "Value2"]
        })
        
        widths = exporter.calculate_column_widths(df)
        assert "A" in widths  # Column letter for first column
        assert "B" in widths  # Column letter for second column
        assert widths["B"] > widths["A"]  # Longer column should be wider

class TestExcelExporterSaveToExcel:
    """Test save_to_excel convenience method."""
    
    @patch.object(ExcelExporter, "export_dataframes")
    def test_save_to_excel_delegates(self, mock_export):
        """Test that save_to_excel delegates to export_dataframes."""
        exporter = ExcelExporter()
        df = pd.DataFrame({"A": [1, 2]})
        
        exporter.save_to_excel(
            data_frame=df,
            file_name_prefix="Test",
            sheet_name="Data"
        )
        
        mock_export.assert_called_once()
        args = mock_export.call_args
        assert "Test" in args.kwargs["export_dict"]
        assert args.kwargs["export_dict"]["Test"]["dataframes"] == [df]
        assert args.kwargs["export_dict"]["Test"]["sheets"] == ["Data"]
