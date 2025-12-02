"""Tests for ryan_library.functions.RORB.read_rorb_files."""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from ryan_library.functions.RORB import read_rorb_files

class TestHelpers:
    def test_find_batch_files(self, tmp_path):
        d1 = tmp_path / "d1"
        d1.mkdir()
        (d1 / "batch.out").touch()
        (d1 / "other.txt").touch()
        
        d2 = tmp_path / "d1" / "d2"
        d2.mkdir()
        (d2 / "batch.out").touch()
        
        files = read_rorb_files.find_batch_files([tmp_path])
        assert len(files) == 2
        assert any(f.parent.name == "d1" for f in files)
        assert any(f.parent.name == "d2" for f in files)

    def test_construct_csv_path(self):
        batchout = Path("/path/to/batch.out")
        path = read_rorb_files._construct_csv_path(batchout, "aep1", "24h", 1)
        # Expect: /path/to/ aep1_du24htp1.csv (Note the space in the code)
        # Code: second_part = f" {aep}_du{du}tp{tpat}.csv"
        # base_name = "" (batch.out replaced)
        # Wait, batchout.name is "batch.out". replace("batch.out", "") -> ""?
        # If filename is "myrun_batch.out", then base is "myrun_".
        
        batchout2 = Path("/path/to/myrun_batch.out")
        path2 = read_rorb_files._construct_csv_path(batchout2, "aep1", "24h", 1)
        assert path2.name == "myrun_ aep1_du24htp1.csv"

class TestParsing:
    def test_parse_run_line_valid(self):
        # Line format based on code:
        # split() -> indices 0..7+
        # 0: Run (int)
        # 1: Duration (float/str)
        # 2: Unit (hour/min) -> popped
        # 3: AEP (str, stripped %)
        # 6: Print? (Y/N) -> converted to 1/0
        
        # Example line: "1  24  hour  1%  ... ... Y ..."
        line = "1  24  hour  1%  val4  val5  Y  val7"
        batchout = Path("batch.out")
        
        # _parse_run_line(line, batchout)
        # raw: ['1', '24', 'hour', '1%', 'val4', 'val5', 'Y', 'val7']
        # raw[3] -> '1'
        # duration_part -> '24hour'
        # aep_part -> 'aep1'
        # raw[6] -> '1'
        # raw[2] is 'hour', so raw[1] stays '24'.
        # pop(2) -> 'hour' removed.
        # raw is now: ['1', '24', '1', 'val4', 'val5', '1', 'val7']
        # Loop:
        # i=0 ('1') -> int(1)
        # i=1 ('24') -> float(24.0)
        # i=2 ('1') -> AEP label -> '1' (string)
        # i=3 ('val4') -> int('val4') -> ValueError?
        # Wait, code says:
        # if i in (0, 3): processed_line.append(int(el))
        # After pop(2), indices shift.
        # Original indices: 0, 1, 2(popped), 3, 4, 5, 6, 7
        # New indices: 0, 1, 2(was 3), 3(was 4), 4(was 5), 5(was 6), 6(was 7)
        
        # Let's trace carefully.
        # raw = ['1', '24', 'hour', '1%', 'val4', 'val5', 'Y', 'val7']
        # raw[3] = '1'
        # raw[6] = '1'
        # raw.pop(2) -> removes 'hour'.
        # raw = ['1', '24', '1', 'val4', 'val5', '1', 'val7']
        # i=0: '1' -> int -> 1
        # i=1: '24' -> match regex -> 24.0
        # i=2: '1' -> AEP label -> '1'
        # i=3: 'val4' -> int('val4') -> ValueError!
        
        # So 'val4' MUST be an integer string (TP number).
        # Let's try a valid line: "1 24 hour 1% 10 12.5 Y 100"
        # 0: 1
        # 1: 24
        # 2: hour
        # 3: 1% (AEP)
        # 4: 10 (TP?) - Wait, code uses raw[3] as TP in _construct_csv_path call: int(processed_line[3])
        # processed_line[3] corresponds to i=3 in the loop over NEW raw.
        # New raw: ['1', '24', '1', '10', '12.5', '1', '100']
        # i=0: 1
        # i=1: 24.0
        # i=2: '1' (AEP)
        # i=3: '10' -> int(10) -> TP
        
        line = "1 24 hour 1% 10 12.5 Y 100"
        res = read_rorb_files._parse_run_line(line, batchout)
        assert res is not None
        assert res[0] == 1
        assert res[1] == 24.0
        assert res[2] == "1"
        assert res[3] == 10
        assert res[4] == 12.5
        assert res[5] == 1
        assert res[6] == 100.0
        assert str(res[7]).endswith(".csv")

    def test_parse_batch_output(self, tmp_path, caplog):
        f = tmp_path / "batch.out"
        header_line = " Run        Duration   Unit   AEP   TP   Peak   Print   Other"
        content = f"Some Header\nPeak  Description\n{header_line}\n1 24 hour 1% 10 12.5 Y 100\n"
        f.write_text(content)
        
        # Clear logs
        caplog.clear()
        
        df = read_rorb_files.parse_batch_output(f)
        
        # Print logs if empty
        if df.empty:
            print("Logs:")
            for record in caplog.records:
                print(record.message)
                
        assert not df.empty
        assert len(df) == 1
        assert df.iloc[0]["Run"] == 1
        assert df.iloc[0]["TP"] == 10

class TestHydrograph:
    def test_read_hydrograph_csv(self, tmp_path):
        f = tmp_path / "hydro.csv"
        content = (
            "Header1\n"
            "Header2\n"
            "Time (hrs),Calculated hydrograph:  Loc1,Calculated hydrograph:  Loc2\n"
            "0.0,0.0,0.0\n"
            "1.0,10.0,5.0\n"
        )
        f.write_text(content)
        
        df = read_rorb_files.read_hydrograph_csv(f)
        assert not df.empty
        assert "Time (hrs)" in df.columns
        assert "Calculated hydrograph:  Loc1" in df.columns

    def test_analyze_hydrograph(self, tmp_path):
        # Create a CSV with standard RORB format (Time first)
        f = tmp_path / "hydro.csv"
        content = (
            "H1\n"
            "H2\n"
            "Time (hrs),Calculated hydrograph:  Loc1\n"
            "0.0,0.0\n"
            "1.0,10.0\n"
        )
        f.write_text(content)
        
        # Now that read_hydrograph_csv doesn't drop the first column,
        # we don't need the "Extra" column.
        res = read_rorb_files.analyze_hydrograph(
            "1%", "24h", 1, f, tmp_path, [5.0]
        )
        
        assert not res.empty
        assert res.iloc[0]["Location"] == "Loc1"
        assert res.iloc[0]["Duration_Exceeding"] == 1.0 # 1 timestep > 5.0
