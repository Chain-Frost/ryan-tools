"""Tests for ryan_library.functions.process_12D_culverts."""

import pandas as pd
from unittest.mock import patch
from ryan_library.functions import process_12D_culverts


class TestHelpers:
    def test_get_encoding(self, tmp_path):
        f_utf8 = tmp_path / "utf8.txt"
        f_utf8.write_bytes(b"hello")
        assert process_12D_culverts.get_encoding(f_utf8) == "utf-8"

        f_utf8sig = tmp_path / "utf8sig.txt"
        f_utf8sig.write_bytes(b"\xef\xbb\xbfhello")
        assert process_12D_culverts.get_encoding(f_utf8sig) == "utf-8-sig"

        f_utf16 = tmp_path / "utf16.txt"
        f_utf16.write_bytes(b"\xff\xfehello")
        assert process_12D_culverts.get_encoding(f_utf16) == "utf-16"

    def test_dms_to_decimal(self):
        assert process_12D_culverts.dms_to_decimal("10° 30' 0\"") == 10.5
        assert process_12D_culverts.dms_to_decimal("0° 0' 0\"") == 0.0
        assert process_12D_culverts.dms_to_decimal("invalid") == 0.0

    def test_get_field(self):
        assert process_12D_culverts.get_field("up", "down") == "up"
        assert process_12D_culverts.get_field("", "down") == "down"
        assert process_12D_culverts.get_field(None, "down") == "down"
        assert process_12D_culverts.get_field(None, None, "default") == "default"

    def test_extract_numeric(self):
        assert process_12D_culverts.extract_numeric("1.5", "field", "name", float, 0.0) == 1.5
        assert process_12D_culverts.extract_numeric("bad", "field", "name", float, 0.0) == 0.0
        assert process_12D_culverts.extract_numeric("10", "field", "name", int, 0) == 10


class TestFileParsing:
    def test_parse_rpt_file(self, tmp_path):
        content = '   1.0 2.0 45°0\'0" 3.0 4.0 5.0 6.0 7.0 "Culvert A"'
        f = tmp_path / "test.rpt"
        f.write_text(content, encoding="utf-8")

        result = process_12D_culverts.parse_rpt_file(f)
        assert len(result) == 1
        assert result[0]["Name"] == "Culvert A"
        assert result[0]["Angle_Degrees"] == 45.0

    def test_parse_txt_file(self, tmp_path):
        # Create a mock TXT file with headers and 2 lines (upstream/downstream)
        content = (
            "header1\t header2\n"
            "String Name\tPit Centre X\tPit Centre Y\tInvert US\tInvert DS\tPipe Type\tDiameter\tWidth\tNumber of Pipes\tSeparation\t*Direction\n"
            "units\n"
            "Culvert A\t10.0\t20.0\t5.0\t\tType1\t0.5\t\t1\t\t\n"
            "Culvert A\t30.0\t40.0\t\t4.0\tType1\t0.5\t\t1\t\t\n"
        )
        f = tmp_path / "test.txt"
        f.write_text(content, encoding="utf-8")

        result = process_12D_culverts.parse_txt_file(f)
        assert len(result) == 1
        c = result[0]
        assert c["Name"] == "Culvert A"
        assert c["US_X"] == 10.0
        assert c["DS_X"] == 30.0
        assert c["Invert US"] == 5.0
        assert c["Invert DS"] == 4.0


class TestDataProcessing:
    def test_combine_data(self):
        rpt_data = [{"Name": "C1", "Angle": "0°0'0\"", "Angle_Degrees": 0.0}]
        txt_data = [{"Name": "C1", "US_X": 10.0}]

        df = process_12D_culverts.combine_data(rpt_data, txt_data)
        assert len(df) == 1
        assert df.iloc[0]["Name"] == "C1"
        assert df.iloc[0]["US_X"] == 10.0

    def test_clean_and_convert(self):
        df = pd.DataFrame({"US_X": ["1.5", "bad"], "Number of Pipes": ["2", "bad"]})
        print(f"Before: {df.dtypes}")
        cleaned = process_12D_culverts.clean_and_convert(df)
        print(f"After: {cleaned.dtypes}")
        print(cleaned)

        # Check US_X
        val = cleaned["US_X"].iloc[0]
        print(f"US_X[0]: {val} type: {type(val)}")
        assert float(val) == 1.5
        assert cleaned["US_X"].iloc[1] == 0.0

        # Check Number of Pipes
        pipes = cleaned["Number of Pipes"].iloc[0]
        print(f"Pipes[0]: {pipes} type: {type(pipes)}")
        assert pipes == 2
        assert pd.isna(cleaned["Number of Pipes"].iloc[1])


class TestIntegration:
    @patch("ryan_library.functions.process_12D_culverts.process_multiple_files")
    def test_get_combined_df_from_files(self, mock_process, tmp_path):
        mock_df = pd.DataFrame(
            {"Name": ["C1"], "US_X": [1.0], "DS_X": [2.0], "Angle": ["0°0'0\""], "Angle_Degrees": [0.0]}
        )
        mock_process.return_value = mock_df

        result = process_12D_culverts.get_combined_df_from_files(tmp_path)
        assert not result.empty
        assert result.iloc[0]["Name"] == "C1"

    def test_get_combined_df_from_csv(self, tmp_path):
        csv = tmp_path / "combined.csv"
        pd.DataFrame({"Name": ["C1"], "US_X": [1.0]}).to_csv(csv, index=False)

        result = process_12D_culverts.get_combined_df_from_csv(csv)
        assert not result.empty
        assert result.iloc[0]["Name"] == "C1"

    def test_get_combined_df_from_csv_error(self, tmp_path):
        # Pass a non-existent file to trigger exception
        res = process_12D_culverts.get_combined_df_from_csv(tmp_path / "does_not_exist.csv")
        assert res.empty

    @patch("ryan_library.functions.process_12D_culverts.process_multiple_files")
    def test_get_combined_df_from_files_empty(self, mock_process, tmp_path):
        mock_process.return_value = pd.DataFrame()
        res = process_12D_culverts.get_combined_df_from_files(tmp_path)
        assert res.empty


class TestExceptionsAndEdgeCases:
    def test_dms_to_decimal_exception(self):
        # Pass something that causes an exception, like an object without replace
        res = process_12D_culverts.dms_to_decimal(None)
        assert res == 0.0

    def test_parse_txt_file_edge_cases(self, tmp_path):
        # Missing columns
        f_missing = tmp_path / "missing.txt"
        f_missing.write_text("header1\nCol1\tCol2\nunits\nval1\tval2\n", encoding="utf-8")
        assert process_12D_culverts.parse_txt_file(f_missing) == []

        # Odd number of rows
        f_odd = tmp_path / "odd.txt"
        content_odd = (
            "header1\t header2\n"
            "String Name\tPit Centre X\tPit Centre Y\tInvert US\tInvert DS\tPipe Type\tDiameter\tWidth\tNumber of Pipes\tSeparation\t*Direction\n"
            "units\n"
            "Culvert A\t10.0\t20.0\t5.0\t\tType1\t0.5\t\t1\t\t\n"
        )
        f_odd.write_text(content_odd, encoding="utf-8")
        res_odd = process_12D_culverts.parse_txt_file(f_odd)
        assert res_odd == []  # last entry skipped

        # Mismatched names
        f_mismatch = tmp_path / "mismatch.txt"
        content_mismatch = (
            "header1\t header2\n"
            "String Name\tPit Centre X\tPit Centre Y\tInvert US\tInvert DS\tPipe Type\tDiameter\tWidth\tNumber of Pipes\tSeparation\t*Direction\n"
            "units\n"
            "Culvert A\t10.0\t20.0\t5.0\t\tType1\t0.5\t\t1\t\t\n"
            "Culvert B\t30.0\t40.0\t\t4.0\tType1\t0.5\t\t1\t\t\n"
        )
        f_mismatch.write_text(content_mismatch, encoding="utf-8")
        assert process_12D_culverts.parse_txt_file(f_mismatch) == []

    @patch("ryan_library.functions.process_12D_culverts.parse_rpt_file")
    @patch("ryan_library.functions.process_12D_culverts.parse_txt_file")
    def test_process_culvert_files_loops(self, mock_txt, mock_rpt, tmp_path):
        f_rpt1 = tmp_path / "1.rpt"
        f_rpt2 = tmp_path / "2.rpt"
        f_txt1 = tmp_path / "1.txt"

        # Valid data from rpt1, none from rpt2
        mock_rpt.side_effect = [[{"Name": "C1", "Angle": "0°0'0\"", "Angle_Degrees": 0.0}], []]
        # Valid data from txt1
        mock_txt.return_value = [{"Name": "C1", "US_X": 10.0}]

        # This will merge, but at least rpt_df won't be empty, so no KeyError!
        df = process_12D_culverts.process_culvert_files([f_rpt1, f_rpt2], [f_txt1])
        assert len(df) == 1
        assert df.iloc[0]["Name"] == "C1"
        assert process_12D_culverts.process_multiple_files(tmp_path).empty

    def test_report_missing_culverts(self):
        df = pd.DataFrame({"Name": ["C1", "C2"], "Angle": ["0°0'0\"", "10°"], "US_X": [0.0, 10.0], "DS_X": [0.0, 20.0]})
        # Just ensure it doesn't crash
        process_12D_culverts.report_missing_culverts(df)

    def test_clean_and_convert_exception(self):
        df = pd.DataFrame(
            {
                "US_X": ["good", "bad"],  # string that can't be converted
            }
        )
        # Actually pandas to_numeric with coerce just returns NaN which fillna makes 0.0.
        # But we mock logger.error if pd.to_numeric throws.
        # By passing a weird type that pd.to_numeric can't coerce?
        df["US_X"] = [{"a": 1}, {"b": 2}]
        res = process_12D_culverts.clean_and_convert(df)
        assert res["US_X"].iloc[0] == 0.0
