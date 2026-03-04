from __future__ import annotations

import argparse
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal


MatchMode = Literal["numeric", "string"]


@dataclass(frozen=True, slots=True)
class Config:
    root: Path
    pattern: str
    recursive: bool
    workers: int
    match: MatchMode
    value: float
    value_str: str
    drop_malformed: bool


def parse_args(argv: list[str] | None = None) -> Config:
    p = argparse.ArgumentParser(
        description="Remove rows where the Z (3rd column) matches a sentinel from every .xyz file."
    )
    p.add_argument(
        "--dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory containing .xyz files. Default: folder containing this script.",
    )
    p.add_argument(
        "--glob",
        default="*.xyz",
        help="Glob for files to process. Default: *.xyz",
    )
    p.add_argument(
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories (uses rglob).",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of processes for parallel processing. 0/1 = single-process. Default: 0.",
    )

    p.add_argument(
        "--match",
        choices=("numeric", "string"),
        default="numeric",
        help="How to compare the Z token to the sentinel. Default: numeric.",
    )
    p.add_argument(
        "--value",
        type=float,
        default=-999.0,
        help="Sentinel value (used for --match numeric). Default: -999.0",
    )
    p.add_argument(
        "--value-str",
        default="",
        help="Sentinel token string (used for --match string). Example: -9.99000e+02",
    )
    p.add_argument(
        "--drop-malformed",
        action="store_true",
        help="Drop lines without at least 3 fields or with non-numeric Z (numeric mode only).",
    )

    ns: argparse.Namespace = p.parse_args(argv)

    match: MatchMode = ns.match
    root: Path = Path(ns.dir).resolve()
    pattern: str = str(ns.glob)
    recursive: bool = bool(ns.recursive)
    workers: int = max(0, int(ns.workers))
    value: float = float(ns.value)
    value_str: str = str(ns.value_str)
    drop_malformed: bool = bool(ns.drop_malformed)

    if match == "string" and not value_str:
        raise SystemExit("--match string requires --value-str")

    return Config(
        root=root,
        pattern=pattern,
        recursive=recursive,
        workers=workers,
        match=match,
        value=value,
        value_str=value_str,
        drop_malformed=drop_malformed,
    )


def split_fields(line: str) -> list[str]:
    # Delimiters: whitespace and/or commas
    return line.replace(",", " ").split()


def z_matches(
    line: str,
    *,
    match: MatchMode,
    value: float,
    value_str: str,
) -> tuple[bool, bool]:
    """
    Returns (match_hit, malformed).

    malformed means:
      - fewer than 3 fields, OR
      - numeric mode: Z cannot be parsed as float
    """
    fields: list[str] = split_fields(line)
    if len(fields) < 3:
        return (False, True)

    ztok: str = fields[2]

    if match == "string":
        return (ztok == value_str, False)

    # match == "numeric"
    try:
        z: float = float(ztok)
    except ValueError:
        return (False, True)

    return (z == value, False)


def iter_files(root: Path, pattern: str, recursive: bool) -> list[Path]:
    it: Iterable[Path] = root.rglob(pattern) if recursive else root.glob(pattern)
    return sorted(p for p in it if p.is_file())


def clean_file(
    path: Path,
    *,
    match: MatchMode,
    value: float,
    value_str: str,
    drop_malformed: bool,
) -> tuple[str, int]:
    removed: int = 0
    tmp_path: Path = path.with_name(path.name + ".tmp")

    try:
        with path.open("r", encoding="utf-8", errors="replace") as src, tmp_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as dst:
            for line in src:
                hit, malformed = z_matches(line, match=match, value=value, value_str=value_str)

                if hit:
                    removed += 1
                    continue

                if match == "numeric" and drop_malformed and malformed and line.strip():
                    removed += 1
                    continue

                dst.write(line)

        if removed:
            tmp_path.replace(path)
        else:
            tmp_path.unlink(missing_ok=True)

        return (path.name, removed)

    finally:
        # Best-effort cleanup if an exception occurs before replace/unlink.
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def run_single(cfg: Config, files: list[Path]) -> int:
    total_removed: int = 0
    for p in files:
        name, removed = clean_file(
            p,
            match=cfg.match,
            value=cfg.value,
            value_str=cfg.value_str,
            drop_malformed=cfg.drop_malformed,
        )
        total_removed += removed
        print(f"{name}: removed {removed} rows")
    return total_removed


def run_parallel(cfg: Config, files: list[Path]) -> int:
    total_removed: int = 0
    workers: int = min(cfg.workers, len(files))

    futures: list[Future[tuple[str, int]]] = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        for p in files:
            fut: Future[tuple[str, int]] = ex.submit(
                clean_file,
                p,
                match=cfg.match,
                value=cfg.value,
                value_str=cfg.value_str,
                drop_malformed=cfg.drop_malformed,
            )
            futures.append(fut)

        for fut in as_completed(futures):
            name, removed = fut.result()
            total_removed += removed
            print(f"{name}: removed {removed} rows")

    return total_removed


def main(argv: list[str] | None = None) -> int:
    cfg: Config = parse_args(argv)

    files: list[Path] = iter_files(root=cfg.root, pattern=cfg.pattern, recursive=cfg.recursive)
    if not files:
        print(f"No files matched {cfg.pattern!r} under {cfg.root}")
        return 0

    if cfg.workers <= 1:
        total_removed: int = run_single(cfg=cfg, files=files)
    else:
        total_removed = run_parallel(cfg=cfg, files=files)

    print(f"Done. Removed {total_removed} rows across {len(files)} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())