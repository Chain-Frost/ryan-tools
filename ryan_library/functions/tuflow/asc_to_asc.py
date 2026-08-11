import subprocess
from pathlib import Path
from typing import Any

from loguru import logger

DEFAULT_EXE_PATH = r"C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe"

def run_asc_to_asc_max(
    input_files: list[str],
    output_file: str,
    exe_path: str = DEFAULT_EXE_PATH,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run asc_to_asc with the -max flag to find the maximum across multiple grids.
    """
    cmd = [exe_path, "-b", "-max"]
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend(["-out", output_file, *input_files])
    logger.debug("Running asc_to_asc max command: {}", " ".join(cmd))
    
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return {"status": "success", "output_file": output_file, "stdout": result.stdout}
    except subprocess.CalledProcessError as e:
        logger.error("asc_to_asc failed with return code {}: {}", e.returncode, e.stderr or e.stdout)
        return {"status": "error", "message": f"asc_to_asc failed with code {e.returncode}", "details": e.stderr or e.stdout}
    except FileNotFoundError:
        return {"status": "error", "message": f"asc_to_asc executable not found at {exe_path}"}


def run_asc_to_asc_diff(
    file1: str,
    file2: str,
    output_file: str,
    diff_mode: str | None = None,
    exe_path: str = DEFAULT_EXE_PATH,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run asc_to_asc with the -diff flag. 
    Subtracts file2 from file1 (file1 - file2).
    """
    cmd = [exe_path, "-b", "-diff"]
    if diff_mode:
        if not diff_mode.startswith("-"):
            diff_mode = f"-{diff_mode}"
        cmd.append(diff_mode)
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend(["-out", output_file, file1, file2])
    logger.debug("Running asc_to_asc diff command: {}", " ".join(cmd))
    
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return {"status": "success", "output_file": output_file, "stdout": result.stdout}
    except subprocess.CalledProcessError as e:
        logger.error("asc_to_asc failed with return code {}: {}", e.returncode, e.stderr or e.stdout)
        return {"status": "error", "message": f"asc_to_asc failed with code {e.returncode}", "details": e.stderr or e.stdout}
    except FileNotFoundError:
        return {"status": "error", "message": f"asc_to_asc executable not found at {exe_path}"}


def run_asc_to_asc_stat(
    stat_type: str,
    input_files: list[str],
    output_file: str,
    exe_path: str = DEFAULT_EXE_PATH,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run asc_to_asc with a statistics flag (e.g., -statAll, -statMax, -statMean).
    """
    if not stat_type.startswith("-stat"):
        stat_type = f"-stat{stat_type}"
        
    cmd = [exe_path, "-b", stat_type]
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend(["-out", output_file, *input_files])
    logger.debug("Running asc_to_asc stat command: {}", " ".join(cmd))
    
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True)
        return {"status": "success", "output_file": output_file, "stdout": result.stdout}
    except subprocess.CalledProcessError as e:
        logger.error("asc_to_asc failed with return code {}: {}", e.returncode, e.stderr or e.stdout)
        return {"status": "error", "message": f"asc_to_asc failed with code {e.returncode}", "details": e.stderr or e.stdout}
    except FileNotFoundError:
        return {"status": "error", "message": f"asc_to_asc executable not found at {exe_path}"}
