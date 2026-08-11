import os
import subprocess
import sys
from pathlib import Path
import pytest

repo_root = Path(__file__).resolve().parents[2]
SCRIPT_DIR = repo_root / "ryan-scripts" / "unsorted-python"
COMPRESS_FOLDER_SCRIPT = SCRIPT_DIR / "archive_compress_folder.py"
COMPRESS_INDIVIDUAL_SCRIPT = SCRIPT_DIR / "archive_compress_individual.py"


def _find_7z() -> Path | None:
    import shutil
    if which_7z := shutil.which("7z"):
        return Path(which_7z)
    paths = [
        Path(os.environ.get("ProgramFiles", "C:\\Program Files")) / "7-Zip" / "7z.exe",
        Path(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")) / "7-Zip" / "7z.exe",
    ]
    for p in paths:
        if p.exists():
            return p
    return None


SEVEN_ZIP_EXE = _find_7z()


@pytest.fixture
def mock_project(tmp_path: Path) -> Path:
    """Sets up a mock folder structure with valid and excluded files."""
    project_dir = tmp_path / "project_a"
    project_dir.mkdir()
    
    # Valid files
    (project_dir / "valid_data.csv").write_text("1,2,3")
    (project_dir / "valid_image.tif").write_text("fake tif")
    
    # Files to be excluded by extension
    (project_dir / "excluded_file.dat").write_text("skip this")
    (project_dir / "excluded_model.xmdf").write_text("skip this too")
    
    # Subfolder that should be excluded
    xf_dir = project_dir / "xf"
    xf_dir.mkdir()
    (xf_dir / "hidden.csv").write_text("skip because of folder")
    (xf_dir / "valid_image.tif").write_text("skip this one too")
    
    # Valid subfolder
    sub_dir = project_dir / "subfolder"
    sub_dir.mkdir()
    (sub_dir / "sub_data.dbf").write_text("fake dbf")
    
    return project_dir


@pytest.mark.skipif(not SEVEN_ZIP_EXE, reason="7z.exe not found on system")
def test_compress_folder_with_exclusions(mock_project: Path):
    """Tests that archive_compress_folder bundles the entire directory and natively excludes folders/extensions."""
    # Run the script via CLI
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    
    result = subprocess.run(
        [
            sys.executable, str(COMPRESS_FOLDER_SCRIPT),
            "-i", str(mock_project),
            "--exclude_folders", "xf",
            "--exclude_extensions", ".dat", ".xmdf",
            "--no-pause"
        ],
        capture_output=True, text=True, env=env
    )
        
    assert result.returncode == 0, f"Script failed with output:\n{result.stderr}\n{result.stdout}"
    
    expected_archive = mock_project.with_suffix(".7z")
    assert expected_archive.exists(), f"Expected archive {expected_archive} was not created"
    
    # Verify contents using 7z.exe list command
    result = subprocess.run(
        [str(SEVEN_ZIP_EXE), "l", str(expected_archive)],
        capture_output=True, 
        text=True, 
        check=True
    )
    
    stdout = result.stdout
    # Should contain valid files
    assert "valid_data.csv" in stdout
    assert "valid_image.tif" in stdout
    assert "sub_data.dbf" in stdout
    
    # Should NOT contain excluded extensions
    assert "excluded_file.dat" not in stdout
    assert "excluded_model.xmdf" not in stdout
    
    # Should NOT contain excluded folder contents
    assert "hidden.csv" not in stdout
    assert "xf" not in stdout


@pytest.mark.skipif(not SEVEN_ZIP_EXE, reason="7z.exe not found on system")
def test_compress_individual_with_exclusions(mock_project: Path):
    """Tests that archive_compress_individual processes files recursively but skips excluded ones."""
    # Run the script via CLI
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)
    
    result = subprocess.run(
        [
            sys.executable, str(COMPRESS_INDIVIDUAL_SCRIPT),
            "-i", str(mock_project),
            "--exclude_folders", "xf",
            "--exclude_extensions", ".dat", ".xmdf",
            "--no-pause"
        ],
        capture_output=True, text=True, env=env
    )
        
    assert result.returncode == 0, f"Script failed with output:\n{result.stderr}\n{result.stdout}"
    
    # Valid files should be compressed
    assert (mock_project / "valid_data.csv.7z").exists()
    assert (mock_project / "valid_image.tif.7z").exists()
    assert (mock_project / "subfolder" / "sub_data.dbf.7z").exists()
    
    # Excluded files/folders should NOT have .7z equivalents
    assert not (mock_project / "excluded_file.dat.7z").exists()
    assert not (mock_project / "excluded_model.xmdf.7z").exists()
    assert not (mock_project / "xf" / "hidden.csv.7z").exists()
    assert not (mock_project / "xf" / "valid_image.tif.7z").exists()
