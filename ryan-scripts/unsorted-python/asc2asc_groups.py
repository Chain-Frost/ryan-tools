from __future__ import annotations
from pathlib import Path

WRAPPER_VERSION = "1.0.0"

import argparse
import sys
import re
import subprocess
import concurrent.futures
from typing import NamedTuple

from loguru import logger
import pandas as pd

from ryan_library.functions.path_stuff import to_single_path, to_path_list


class TUFLOWRaster(NamedTuple):
    full_path: Path
    filename: str
    tp: str | None
    duration: str | None
    aep: str | None
    suffix: str


def check_string_TP(string: str) -> str | None:
    match = re.search(r"TP(\d{2})", string, re.IGNORECASE)
    return f"TP{match.group(1)}" if match else None


def check_string_duration(string: str) -> str | None:
    match = re.search(r"(?:[_+]|^)(\d{3,5}[mM])(?:[_+]|$)", string, re.IGNORECASE)
    return match.group(1).replace("_", "").replace("m", "").replace("M", "") + "m" if match else None


def check_string_aep(string: str) -> str | None:
    match = re.search(r"(?:[_+]|^)(\d{2}\.\d{1,2}p)(?:[_+]|$)", string, re.IGNORECASE)
    return match.group(1).replace("_", "") if match else None


def trim_filename(filename: str, parts_to_remove: list[str]) -> str:
    name_parts = re.split(r"_|\+", filename)
    lower_parts_to_remove = set([str(p).lower() for p in parts_to_remove if p])
    filtered_parts = [p for p in name_parts if p.lower() not in lower_parts_to_remove]
    return "_".join(filtered_parts)


def parse_tuflow_raster(file_path: Path, suffix: str) -> TUFLOWRaster:
    filename = file_path.name
    base_filename = file_path.stem
    return TUFLOWRaster(
        full_path=file_path,
        filename=base_filename,
        tp=check_string_TP(filename),
        duration=check_string_duration(filename),
        aep=check_string_aep(filename),
        suffix=suffix,
    )


def execute_asc_to_asc(cmd: list[str], dry_run: bool) -> bool:
    logger.debug(f"Command: {' '.join(cmd)}")
    if dry_run:
        return True
    try:
        result = subprocess.run(cmd, check=False, text=True, capture_output=True)
        if result.returncode != 0:
            logger.error(f"Command failed: {' '.join(cmd)}")
            logger.error(result.stderr)
            return False
        return True
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        return False


def cmd_max_median(args: argparse.Namespace) -> None:
    input_dir = to_single_path(args.input[0])
    out_dir = to_single_path(args.output_dir) if args.output_dir else input_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    
    suffix = args.suffix
    file_paths = list(input_dir.rglob(f"*{suffix}.tif"))
    
    if not file_paths:
        logger.warning(f"No files found matching *{suffix}.tif in {input_dir}")
        return
        
    logger.info(f"Found {len(file_paths)} files matching *{suffix}.tif")
    
    rasters = []
    for p in file_paths:
        r = parse_tuflow_raster(p, suffix)
        if r.tp and r.duration and r.aep:
            rasters.append(r)
        else:
            logger.warning(f"Skipping {p.name}: Missing TP, Duration, or AEP pattern.")

    df = pd.DataFrame(rasters)
    if df.empty:
        logger.error("No valid rasters found after parsing attributes.")
        return

    # Step 1: Max/Median across Temporal Patterns (TPs)
    # Group by everything except TP
    commands_step1 = []
    unique_groups = df.groupby(["aep", "duration", "suffix"])
    
    stat_text = "_Median_Val.tif"
    
    for (aep, duration, suf), group in unique_groups:
        example = group.iloc[0]
        # Replace the TP part with '*' in the filename
        pattern = re.compile(re.escape(example.tp), re.IGNORECASE)
        star_tp_name = pattern.sub("*", example.filename) + ".tif"
        input_pattern_path = str(example.full_path.parent / star_tp_name)
        
        # Output filename: remove TP
        out_name = trim_filename(example.filename, [example.tp]) + stat_text
        out_path = out_dir / out_name
        
        cmd = [
            args.executable, "-b", "-tif", "-out", str(out_path), "-statMedian", input_pattern_path
        ]
        commands_step1.append(cmd)

    logger.info(f"Generating {len(commands_step1)} median commands (across TPs)...")
    success_count = 0
    if args.dry_run:
        logger.info("Dry run enabled. Skipping execution.")
        for c in commands_step1:
            logger.info(" ".join(c))
    else:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(execute_asc_to_asc, cmd, False) for cmd in commands_step1]
            for f in concurrent.futures.as_completed(futures):
                if f.result(): success_count += 1
        logger.success(f"Step 1 Complete: {success_count}/{len(commands_step1)} successful.")
    
    # Step 2: Max across durations
    # Group by AEP only
    commands_step2 = []
    aep_groups = df.groupby(["aep", "suffix"])
    
    for (aep, suf), group in aep_groups:
        example = group.iloc[0]
        # output of step 1 is input for step 2. We use '*' for duration.
        # e.g. base file without TP, with duration replaced by *
        base_no_tp = trim_filename(example.filename, [example.tp])
        pattern = re.compile(re.escape(example.duration), re.IGNORECASE)
        star_dur_name = pattern.sub("*", base_no_tp) + stat_text
        input_pattern_path = str(out_dir / star_dur_name)
        
        # Final output name: no TP, no Duration
        out_name = trim_filename(example.filename, [example.tp, example.duration]) + ".tif"
        out_path = out_dir / out_name
        
        cmd = [
            args.executable, "-b", "-tif", "-out", str(out_path), "-StatMax", input_pattern_path
        ]
        commands_step2.append(cmd)

    logger.info(f"Generating {len(commands_step2)} max commands (across durations)...")
    success_count = 0
    if args.dry_run:
        for c in commands_step2:
            logger.info(" ".join(c))
    else:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(execute_asc_to_asc, cmd, False) for cmd in commands_step2]
            for f in concurrent.futures.as_completed(futures):
                if f.result(): success_count += 1
        logger.success(f"Step 2 Complete: {success_count}/{len(commands_step2)} successful.")


def cmd_diff(args: argparse.Namespace) -> None:
    if len(args.input) < 2:
        logger.error("--mode diff requires at least two inputs (current_dir, existing_dir)")
        return
        
    current_dir = to_single_path(args.input[0])
    existing_dir = to_single_path(args.input[1])
    out_dir = to_single_path(args.output_dir) if args.output_dir else current_dir / "diff_output"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    suffix = args.suffix
    current_files = list(current_dir.rglob(f"*{suffix}.tif"))
    existing_files = list(existing_dir.rglob(f"*{suffix}.tif"))
    
    logger.info(f"Found {len(current_files)} files in current, {len(existing_files)} in existing.")
    
    current_rasters = [parse_tuflow_raster(p, suffix) for p in current_files]
    existing_rasters = [parse_tuflow_raster(p, suffix) for p in existing_files]
    
    current_df = pd.DataFrame(current_rasters)
    existing_df = pd.DataFrame(existing_rasters)
    
    if current_df.empty or existing_df.empty:
        logger.error("No valid rasters found to compare.")
        return
        
    # Gather parts to remove for pairing (TP, AEP, Duration)
    parts_to_remove = set()
    for col in ["tp", "aep", "duration"]:
        parts_to_remove.update(current_df[col].dropna().unique())
        
    logger.debug(f"Trimming parts to find matching scenarios: {parts_to_remove}")
    
    current_df["trimmed"] = current_df["filename"].apply(lambda x: trim_filename(str(x), list(parts_to_remove)))  # type: ignore
    existing_df["trimmed"] = existing_df["filename"].apply(lambda x: trim_filename(str(x), list(parts_to_remove)))  # type: ignore
    
    pairs: list[tuple[Path, Path]] = []
    for _, current_row in current_df.iterrows():  # type: ignore
        matches = existing_df[existing_df["trimmed"] == current_row["trimmed"]]  # type: ignore
        if not matches.empty:  # type: ignore
            existing_row = matches.iloc[0]  # type: ignore
            pairs.append((current_row["full_path"], existing_row["full_path"]))  # type: ignore

    logger.info(f"Paired {len(pairs)} matching scenarios.")
    
    commands = []
    for current, existing in pairs:
        outname = current.name.replace(".tif", "_DIFF.tif")
        outpath = out_dir / outname
        cmd = [
            args.executable, "-b", "-out", str(outpath), "-dif", str(current), str(existing)
        ]
        commands.append(cmd)
        
    if args.dry_run:
        logger.info("Dry run enabled. Skipping execution.")
        for c in commands:
            logger.info(" ".join(c))
    else:
        success_count = 0
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(execute_asc_to_asc, cmd, False) for cmd in commands]
            for f in concurrent.futures.as_completed(futures):
                if f.result(): success_count += 1
        logger.success(f"Diff Complete: {success_count}/{len(commands)} successful.")


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Automate asc_to_asc operations across grouped rasters (v{WRAPPER_VERSION})."
    )
    parser.add_argument(
        "--mode",
        choices=["max-median", "diff"],
        required=True,
        help="Mode of operation: max-median (group TP/Duration) or diff (subtract matched pairs)."
    )
    parser.add_argument(
        "input",
        nargs="+",
        type=str,
        help="Input directories. max-median needs 1, diff needs 2 (current, existing).",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=None,
        help="Output directory.",
    )
    parser.add_argument(
        "--suffix",
        type=str,
        default="d_Max",
        help="Suffix to match (e.g. d_Max, d_HR_Max).",
    )
    parser.add_argument(
        "--executable",
        type=str,
        default="asc_to_asc_w64.exe",
        help="Path to asc_to_asc executable if not in PATH.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_cli_arguments()

    if args.mode == "max-median":
        cmd_max_median(args)
    elif args.mode == "diff":
        cmd_diff(args)


if __name__ == "__main__":
    main()
