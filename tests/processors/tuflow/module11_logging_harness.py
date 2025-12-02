"""Local harness for culvert maximums logging validation (Module_11 data).

Runs Nmx samples in both serial and multiprocessing modes using the bundled
Module_11 dataset to inspect log formatting and level handling without
touching globally installed packages.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator

from loguru import logger


def _repo_root() -> Path:
    """Locate repository root by walking up to find pyproject.toml."""

    current: Path = Path(__file__).resolve()
    for ancestor in current.parents:
        if (ancestor / "pyproject.toml").exists():
            return ancestor
    raise RuntimeError("Could not locate repository root from harness location.")


REPO_ROOT: Path = _repo_root()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Imports that rely on the repo being on sys.path
from ryan_library.processors.tuflow.maximums_1d.NmxProcessor import NmxProcessor  # noqa: E402


@contextmanager
def pushd(path: Path) -> Iterator[None]:
    """Temporarily change working directory."""

    original: Path = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)


def configure_logging(level: str = "INFO") -> None:
    """Configure loguru for concise, user-readable output."""

    # Force a reset of any existing sinks (including those from imports)
    try:
        from ryan_library.functions.loguru_helpers import configure_serial_logging

        configure_serial_logging(console_log_level=level)
    except ImportError:
        logger.remove()
        logger.add(sys.stdout, level=level)

    logger.debug(f"Logger initialised at level {level}")


def _process_file(path: Path) -> tuple[str, int]:
    """Process a single file; callable from worker processes."""

    processor = NmxProcessor(file_path=path)
    processor.process()
    return (path.name, len(processor.df))


def run_serial(files: list[Path]) -> None:
    """Run Nmx processing in a single process to inspect baseline logs."""

    logger.info("=== Serial run ===")
    for path in files:
        logger.info(f"Processing {path.name} (serial)")
        name, rows = _process_file(path)
        logger.info(f"{name}: rows processed={rows}")


def run_parallel(files: list[Path], level: str = "INFO") -> None:
    """Run Nmx processing via multiprocessing to inspect listener formatting."""

    from ryan_library.functions.loguru_helpers import setup_logger, worker_initializer

    logger.info("=== Multiprocessing run (spawn) ===")

    # Use the production logging setup
    with setup_logger(console_log_level=level) as queue:
        ctx = mp.get_context("spawn")
        with ctx.Pool(
            processes=min(len(files), max(ctx.cpu_count() - 1, 1)), initializer=worker_initializer, initargs=(queue,)
        ) as pool:
            for name, rows in pool.map(_process_file, files):
                # The worker logs will go to the queue -> listener -> console
                # This main process log will also go to the queue if we configured it?
                # setup_logger configures the main process too.
                logger.info(f"{name}: rows processed={rows}")


def run_threaded(files: list[Path], level: str = "INFO") -> None:
    """Run Nmx processing via multithreading to verify thread safety."""

    from concurrent.futures import ThreadPoolExecutor

    logger.info("=== Multithreaded run ===")

    # Threading shares the same process, so standard logging setup applies.
    # We just want to ensure no weird interleaving or errors.

    with ThreadPoolExecutor(max_workers=min(len(files), 4)) as executor:
        for name, rows in executor.map(_process_file, files):
            logger.info(f"{name}: rows processed={rows}")


def main(use_parallel: bool = True, use_threaded: bool = False, level: str = "INFO") -> None:
    """Run small Nmx samples through the processor to inspect logging."""

    data_root: Path = REPO_ROOT / "tests" / "test_data" / "tuflow" / "tutorials" / "Module_11"
    csv_dir: Path = data_root / "results" / "plot" / "csv"
    sample_files: list[Path] = [
        csv_dir / "M11_5m_001_1d_Nmx.csv",
        csv_dir / "M11_5m_002_1d_Nmx.csv",
    ]

    missing: list[Path] = [p for p in sample_files if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing sample CSV(s): {missing}")

    configure_logging(level=level)
    logger.info(f"Using repository root {REPO_ROOT}")

    import ryan_library

    logger.info(f"Using ryan_library from: {ryan_library.__file__}")

    logger.info(f"Data directory {data_root}")

    with pushd(data_root):
        run_serial(sample_files)
        if use_parallel:
            run_parallel(sample_files, level=level)
        if use_threaded:
            run_threaded(sample_files, level=level)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run logging harness.")
    parser.add_argument("--level", default="INFO", help="Logging level (default: INFO)")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel run")
    parser.add_argument("--threaded", action="store_true", help="Enable threaded run")
    args = parser.parse_args()

    main(use_parallel=not args.no_parallel, use_threaded=args.threaded, level=args.level)
