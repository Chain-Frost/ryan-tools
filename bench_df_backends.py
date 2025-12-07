from __future__ import annotations

"""Benchmark DataFrame concatenation/grouping across backends (pandas, optional polars/pyarrow).

Usage examples (from repo root):
  python bench_df_backends.py --num-frames 200 --rows-per-frame 1000
  python bench_df_backends.py --num-frames 4000 --rows-per-frame 3000 --num-columns 20 --repeats 3

By default only pandas runs. Polars/pyarrow are attempted if installed; force tries with
--try-polars/--try-pyarrow or disable with --no-polars/--no-pyarrow.
"""

import argparse
import glob
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


def _now() -> float:
    return time.perf_counter()


@dataclass
class BenchmarkResult:
    backend: str
    action: str
    seconds: float
    rows: int
    cols: int

    def format(self) -> str:
        return (
            f"{self.backend:10s} {self.action:12s} "
            f"{self.seconds*1000:8.1f} ms  rows={self.rows:,} cols={self.cols}"
        )


def make_frames(
    num_frames: int,
    rows: int,
    cols: int,
    seed: int = 42,
    num_categorical: int = 0,
    cat_cardinality: int = 50,
    cat_length: int = 12,
) -> list[pd.DataFrame]:
    rng = np.random.default_rng(seed)
    numeric_cols: int = max(0, cols - 3 - num_categorical)
    columns = [f"val_{i}" for i in range(numeric_cols)]
    cat_cols = [f"cat_{i}" for i in range(num_categorical)]
    frames: list[pd.DataFrame] = []
    # simple grouping keys to emulate combine_1d_timeseries / maximums
    group_values = [f"run_{i}" for i in range(max(10, int(math.sqrt(num_frames))))]
    chan_values = [f"chan_{i}" for i in range(max(10, int(math.sqrt(num_frames))))]

    def random_string_array(size: int) -> np.ndarray:
        words = ["".join(rng.choice(list("abcdefghijklmnopqrstuvwxyz"), size=cat_length)) for _ in range(cat_cardinality)]
        return rng.choice(words, size=size)

    for idx in range(num_frames):
        data = {
            "internalName": rng.choice(group_values, size=rows),
            "Chan ID": rng.choice(chan_values, size=rows),
            "Time": rng.integers(0, 1000, size=rows),
        }
        for col in columns:
            data[col] = rng.normal(loc=0.0, scale=1.0, size=rows)
        for col in cat_cols:
            data[col] = random_string_array(size=rows)
        frames.append(pd.DataFrame(data))
    return frames


def load_frames_from_paths(paths: list[Path], limit: int | None = None, fmt: str | None = None) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for path in paths:
        if limit is not None and len(frames) >= limit:
            break
        try:
            use_fmt = fmt
            if use_fmt is None:
                suffix = path.suffix.lower()
                if suffix == ".parquet" or suffix.endswith("parquet.gzip"):
                    use_fmt = "parquet"
                else:
                    use_fmt = "csv"
            if use_fmt == "parquet":
                df = pd.read_parquet(path)  # type: ignore[arg-type]
            else:
                df = pd.read_csv(path)  # type: ignore[arg-type]
            frames.append(df)
        except Exception as exc:
            print(f"Failed to load {path}: {exc}", file=sys.stderr)
            continue
    return frames


def bench_pandas_concat(frames: list[pd.DataFrame]) -> BenchmarkResult:
    start = _now()
    combined = pd.concat(frames, ignore_index=True, copy=False, sort=False)
    elapsed = _now() - start
    return BenchmarkResult("pandas", "concat", elapsed, len(combined), combined.shape[1])


def bench_pandas_group(frames: list[pd.DataFrame]) -> BenchmarkResult:
    combined = pd.concat(frames, ignore_index=True, copy=False, sort=False)
    start = _now()
    grouped = combined.groupby(["internalName", "Chan ID"], observed=True).agg("max").reset_index()
    elapsed = _now() - start
    return BenchmarkResult("pandas", "groupby", elapsed, len(grouped), grouped.shape[1])


def bench_polars(frames: list[pd.DataFrame]) -> list[BenchmarkResult]:
    try:
        import polars as pl
    except Exception:
        return []

    results: list[BenchmarkResult] = []
    pl_frames = [pl.from_pandas(df, rechunk=False) for df in frames]

    start = _now()
    combined = pl.concat(pl_frames, how="vertical_relaxed")
    elapsed = _now() - start
    results.append(BenchmarkResult("polars", "concat", elapsed, combined.height, combined.width))

    start = _now()
    grouped = combined.group_by(["internalName", "Chan ID"]).agg(pl.all().max())  # type: ignore[arg-type]
    elapsed = _now() - start
    results.append(BenchmarkResult("polars", "groupby", elapsed, grouped.height, grouped.width))
    return results


def bench_pyarrow(frames: list[pd.DataFrame]) -> list[BenchmarkResult]:
    try:
        import pyarrow as pa
    except Exception:
        return []

    results: list[BenchmarkResult] = []
    tables = [pa.Table.from_pandas(df, preserve_index=False) for df in frames]

    start = _now()
    combined = pa.concat_tables(tables, promote=True)
    elapsed = _now() - start
    results.append(BenchmarkResult("pyarrow", "concat", elapsed, combined.num_rows, combined.num_columns))

    try:
        start = _now()
        pandas_df = combined.to_pandas()
        elapsed = _now() - start
        results.append(
            BenchmarkResult("pyarrow->pd", "to_pandas", elapsed, len(pandas_df), pandas_df.shape[1])
        )
    except Exception:
        pass

    return results


def run_once(frames: list[pd.DataFrame], include_polars: bool, include_pyarrow: bool) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    results.append(bench_pandas_concat(frames))
    results.append(bench_pandas_group(frames))

    if include_polars:
        results.extend(bench_polars(frames))
    if include_pyarrow:
        results.extend(bench_pyarrow(frames))
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark DataFrame concat/group across backends.")
    parser.add_argument("--num-frames", type=int, default=200, help="Number of DataFrames to generate.")
    parser.add_argument("--rows-per-frame", type=int, default=1000, help="Rows per synthetic DataFrame.")
    parser.add_argument("--num-columns", type=int, default=20, help="Total columns (incl. keys).")
    parser.add_argument("--repeats", type=int, default=3, help="Benchmark repetitions.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--categorical-cols", type=int, default=0, help="Number of synthetic categorical columns.")
    parser.add_argument("--categorical-cardinality", type=int, default=50, help="Distinct values per categorical column.")
    parser.add_argument("--categorical-length", type=int, default=200, help="Length of synthetic categorical strings.")
    parser.add_argument("--try-polars", action="store_true", help="Attempt polars benchmarks if installed.")
    parser.add_argument("--try-pyarrow", action="store_true", help="Attempt pyarrow benchmarks if installed.")
    parser.add_argument("--no-polars", action="store_true", help="Disable polars even if installed.")
    parser.add_argument("--no-pyarrow", action="store_true", help="Disable pyarrow even if installed.")
    parser.add_argument(
        "--input-glob",
        action="append",
        default=[],
        help="Glob(s) for existing CSV/Parquet files to use instead of synthetic frames.",
    )
    parser.add_argument(
        "--input-format",
        choices=["csv", "parquet"],
        help="Force input format when using --input-glob. Default: infer from file extension.",
    )
    parser.add_argument(
        "--limit-frames",
        type=int,
        default=None,
        help="Maximum number of frames to load from --input-glob (useful for sampling).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    include_polars = False
    include_pyarrow = False

    if not args.no_polars:
        if args.try_polars:
            include_polars = True
        else:
            try:
                import polars as _  # noqa: F401
                include_polars = True
            except Exception:
                include_polars = False

    if not args.no_pyarrow:
        if args.try_pyarrow:
            include_pyarrow = True
        else:
            try:
                import pyarrow as _  # noqa: F401
                include_pyarrow = True
            except Exception:
                include_pyarrow = False

    frames: list[pd.DataFrame]
    if args.input_glob:
        path_strings: list[str] = []
        for pattern in args.input_glob:
            path_strings.extend(glob.glob(pattern))
        paths: list[Path] = [Path(p) for p in path_strings]
        frames = load_frames_from_paths(paths=paths, limit=args.limit_frames, fmt=args.input_format)
        if not frames:
            print("No frames loaded from --input-glob patterns; exiting.", file=sys.stderr)
            return
        print(
            f"Loaded {len(frames)} frame(s) from input globs "
            f"(mean rows {int(sum(len(f) for f in frames)/len(frames)):,}). Repeats: {args.repeats}"
        )
    else:
        frames = make_frames(
            num_frames=args.num_frames,
            rows=args.rows_per_frame,
            cols=args.num_columns,
            seed=args.seed,
            num_categorical=args.categorical_cols,
            cat_cardinality=args.categorical_cardinality,
            cat_length=args.categorical_length,
        )
        print(
            f"Synthetic frames: {args.num_frames} x {args.rows_per_frame} rows, "
            f"{args.num_columns} cols (keys included, {args.categorical_cols} categorical). Repeats: {args.repeats}"
        )
    if include_polars:
        print("Polars: enabled")
    else:
        print("Polars: not available (use --try-polars to force attempt)")
    if include_pyarrow:
        print("PyArrow: enabled")
    else:
        print("PyArrow: not available (use --try-pyarrow to force attempt)")

    all_results: list[BenchmarkResult] = []
    for i in range(args.repeats):
        run_results = run_once(
            frames=frames,
            include_polars=include_polars,
            include_pyarrow=include_pyarrow,
        )
        print(f"\nRun {i+1}:")
        for result in run_results:
            print(f"  {result.format()}")
        all_results.extend(run_results)

    # Simple summary by backend/action
    if all_results:
        print("\nSummary (mean over repeats):")
        summary: dict[tuple[str, str], list[BenchmarkResult]] = {}
        for res in all_results:
            summary.setdefault((res.backend, res.action), []).append(res)
        for key, items in summary.items():
            mean_ms = 1000 * sum(r.seconds for r in items) / len(items)
            sample = items[0]
            print(f"  {key[0]:10s} {key[1]:12s} {mean_ms:8.1f} ms  rows={sample.rows:,} cols={sample.cols}")


if __name__ == "__main__":
    main()
