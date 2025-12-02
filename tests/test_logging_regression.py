import subprocess
import sys
import os
from pathlib import Path
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_SCRIPT = REPO_ROOT / "tests" / "processors" / "tuflow" / "module11_logging_harness.py"


def run_harness(level: str, extra_args: list[str] = None) -> str:
    """Run the harness script with the specified log level and return stdout."""
    cmd = [sys.executable, str(HARNESS_SCRIPT), "--level", level]
    if extra_args:
        cmd.extend(extra_args)

    # Ensure we use the local ryan_library, not an installed one
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT, env=env)
    if result.returncode != 0:
        print(f"Command failed: {cmd}")
        print(result.stderr)
    result.check_returncode()
    return result.stdout


def test_logging_info_level():
    """Verify that INFO level does not show DEBUG logs."""
    output = run_harness("INFO")
    assert "Completed processing of NMX file" in output
    assert "Starting NMX data extraction and transformation" not in output


def test_logging_debug_level():
    """Verify that DEBUG level shows DEBUG logs."""
    output = run_harness("DEBUG")
    assert "Completed processing of NMX file" in output
    assert "Starting NMX data extraction and transformation" in output


def test_logging_warning_level():
    """Verify that WARNING level suppresses INFO logs."""
    output = run_harness("WARNING")
    # INFO logs should be missing
    assert "Completed processing of NMX file" not in output
    assert "Starting NMX data extraction and transformation" not in output
    # But we don't have explicit warnings in the normal flow, so just ensuring absence of INFO is good.


def test_logging_threaded():
    """Verify that threaded execution works and logs correctly."""
    output = run_harness("INFO", extra_args=["--threaded", "--no-parallel"])
    assert "=== Multithreaded run ===" in output
    assert "Completed processing of NMX file" in output
    assert "rows processed=" in output


if __name__ == "__main__":
    try:
        test_logging_info_level()
        print("INFO level test passed.")
        test_logging_debug_level()
        print("DEBUG level test passed.")
        test_logging_warning_level()
        print("WARNING level test passed.")
        test_logging_threaded()
        print("Threaded run test passed.")
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)
