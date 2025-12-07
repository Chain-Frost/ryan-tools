from __future__ import annotations

import argparse
import statistics
import sys
import time
from pathlib import Path
from typing import Iterable, Tuple


PATTERNS: str = "*.tlf"
EXCLUDES: list[str] = ["*.hpc.tlf", "*.gpu.tlf"]
INCLUDE_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF")


def purge_module_cache(prefix: str) -> None:
    """Drop any already-imported modules under the given prefix so we can reload from a different path."""
    for name in list(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            sys.modules.pop(name, None)


def load_apis(mode: str, repo_root: Path):
    """Load the functions from either the installed package or the working tree."""
    if mode == "installed":
        repo_resolved: Path = repo_root.resolve()
        sys.path = [p for p in sys.path if p and Path(p).resolve() != repo_resolved]
    elif mode == "local":
        repo_str = str(repo_root)
        if repo_str not in sys.path:
            sys.path.insert(0, repo_str)
    else:
        raise ValueError(f"Unknown mode '{mode}'")

    purge_module_cache("ryan_library")

    from loguru import logger

    logger.remove()
    logger.add(sys.stdout, level="WARNING")

    from ryan_library.functions.file_utils import find_files_parallel
    from ryan_library.functions.tuflow.tuflow_common import collect_files
    from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig

    return find_files_parallel, collect_files, SuffixesConfig.get_instance(), logger


def benchmark_find(find_files_parallel, root: Path, runs: int) -> tuple[list[float], list[int]]:
    durations: list[float] = []
    counts: list[int] = []
    for _ in range(runs):
        start = time.perf_counter()
        files = list(find_files_parallel(root_dirs=[root], patterns=PATTERNS, excludes=EXCLUDES))
        durations.append(time.perf_counter() - start)
        counts.append(len(files))
    return durations, counts


def benchmark_collect(
    collect_files,
    suffixes_config,
    root: Path,
    runs: int,
) -> tuple[list[float], list[int]]:
    durations: list[float] = []
    counts: list[int] = []
    for _ in range(runs):
        start = time.perf_counter()
        files = collect_files(
            paths_to_process=[root],
            include_data_types=list(INCLUDE_DATA_TYPES),
            suffixes_config=suffixes_config,
        )
        durations.append(time.perf_counter() - start)
        counts.append(len(files))
    return durations, counts


def summarize(name: str, root: Path, durations: Iterable[float], counts: Iterable[int]) -> None:
    def fmt(ns: float) -> str:
        return f"{ns * 1000:.1f} ms"

    counts = list(counts)
    durations = list(durations)
    print(f"\n{name} @ {root}")
    print(f"  runs : {len(durations)}")
    print(f"  count: min={min(counts)}, max={max(counts)}, mean={statistics.mean(counts):.1f}")
    print(
        "  time : min={}, max={}, mean={}, median={}".format(
            fmt(min(durations)),
            fmt(max(durations)),
            fmt(statistics.mean(durations)),
            fmt(statistics.median(durations)),
        )
    )


def run_for_root(label: str, root: Path, runs: int) -> None:
    repo_root: Path = Path(__file__).resolve().parent
    modes: Tuple[str, ...] = ("installed", "local")

    print(f"\n=== {label} @ {root} ===")
    if not root.exists():
        print(f"Path not found; skipping.")
        return

    for mode in modes:
        try:
            find_files_parallel, collect_files, suffixes_config, _logger = load_apis(mode=mode, repo_root=repo_root)
        except Exception as exc:
            print(f"\n{mode}: unable to import ryan_library ({exc})")
            continue

        print(f"\n[{mode}]")
        find_durs, find_counts = benchmark_find(find_files_parallel, root=root, runs=runs)
        summarize("find_files_parallel", root, find_durs, find_counts)
        collect_durs, collect_counts = benchmark_collect(
            collect_files, suffixes_config=suffixes_config, root=root, runs=runs
        )
        summarize("collect_files", root, collect_durs, collect_counts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare installed vs working-tree performance.")
    parser.add_argument(
        "--local-root",
        default=r"E:\Library\Automation\ryan-tools\tests\test_data",
        help="Local data root to benchmark.",
    )
    parser.add_argument(
        "--network-root",
        default=r"\\StrandServer\Library\Automation\ryan-tools\tests\test_data",
        help="Network data root to benchmark.",
    )
    parser.add_argument("--runs", type=int, default=5, help="Number of iterations per mode.")
    args = parser.parse_args()

    run_for_root("local", Path(args.local_root), runs=args.runs)
    run_for_root("network", Path(args.network_root), runs=args.runs)


if __name__ == "__main__":
    main()
