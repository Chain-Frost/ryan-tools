import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.EOFProcessor import EOFProcessor


def test_eof_processor_synthetic(tmp_path, change_cwd):
    """Test EOFProcessor with synthetic text data."""
    with change_cwd(tmp_path):
        eof_path = tmp_path / "Test_Run.eof"

        # Construct a minimal EOF content
        content = """
        Some Header Info
        ...
        CULVERT AND PIPE DATA
        Channel     Type  No  U/S   D/S   U/S   D/S   Length  Slope  n     Diameter  Height  Inlet H  Inlet W  Ent  Exit  Fixed  Losses
        Chan1       C     1   10.0  9.0   12.0  11.0  100.0   0.01   0.013 1.5       1.5     1.5      1.5      0.5  1.0   0.0    0.0
        Chan2       R     1   10.0  9.0   12.0  11.0  100.0   0.01   0.013 2.0       -----   2.0      2.0      0.5  1.0   0.0    0.0
        
        VELOCITIES
        ...
        """
        eof_path.write_text(content, encoding="utf-8")

        processor = EOFProcessor(file_path=eof_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check columns
    assert "Chan ID" in processor.df.columns
    assert "Type" in processor.df.columns
    assert "Height" in processor.df.columns

    # Check values
    chan1 = processor.df[processor.df["Chan ID"] == "Chan1"]
    assert len(chan1) == 1
    assert float(chan1.iloc[0]["Height"]) == 1.5

    # Check coercion of -----
    chan2 = processor.df[processor.df["Chan ID"] == "Chan2"]
    assert len(chan2) == 1
    assert pd.isna(chan2.iloc[0]["Height"])


def test_eof_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run.eof"
        empty_path.touch()
        proc_empty = EOFProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing section
        bad_path = tmp_path / "Bad_Run.eof"
        bad_path.write_text("Just some random text", encoding="utf-8")
        proc_bad = EOFProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_eof_processor_real_file(find_test_file):
    """Test EOFProcessor with a real file if available."""
    file_path = find_test_file(".eof")
    if not file_path:
        pytest.skip("No .eof file found in test data")

    processor = EOFProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "Chan ID" in processor.df.columns
