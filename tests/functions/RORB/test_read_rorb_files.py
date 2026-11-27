"""Public compatibility-reader and discovery behavior; strict formats tested separately."""

from pathlib import Path

from ryan_library.functions.RORB import read_rorb_files


def test_find_batch_files(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (tmp_path / "one_batch.out").touch()
    (nested / "two_batch.out").touch()
    (nested / "other.txt").touch()
    assert len(read_rorb_files.find_batch_files([tmp_path])) == 2


def test_analyze_hydrograph(tmp_path: Path) -> None:
    path = tmp_path / "hydro.csv"
    path.write_text(
        "RORBWin CSV Output File\n"
        "Refer output file for complete listing of inputs and outputs\n"
        "Inc,Time (hrs),Calculated hydrograph:  Loc1\n"
        "0,0.0,0.0\n1,1.0,10.0\n"
    )
    result = read_rorb_files.analyze_hydrograph("1%", "24h", 1, path, tmp_path, [5.0])
    assert result.iloc[0]["Location"] == "Loc1"
    assert result.iloc[0]["Duration_Exceeding"] == 1.0
