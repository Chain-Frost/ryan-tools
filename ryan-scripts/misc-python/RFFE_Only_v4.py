# 2025-07-08 RFFE extractor update

import argparse
import json
import re
from pathlib import Path
from typing import Any
import pandas as pd
from pandas import DataFrame
import requests
from requests import Response
from bs4 import BeautifulSoup

from ryan_library.functions.loguru_helpers import logger, setup_logger

# ─── Hard-coded defaults (override via CLI if desired) ────────────────────

DEFAULT_INPUT_DIR: Path = Path(r"C:\Temp\tester")
DEFAULT_OUTPUT_DIR: Path = DEFAULT_INPUT_DIR

"""
Expected input file: input_catchments.csv

A comma-separated values file with a header row and the following columns:

    Catchment   (str)   Unique name or identifier for each catchment.
    AreaKm2     (float) Total catchment area in square kilometres.
    OutletX     (float) Longitude of the catchment's outlet, in decimal degrees.
    OutletY     (float) Latitude of the catchment's outlet, in decimal degrees.
    CentroidX   (float) Longitude of the catchment's centroid, in decimal degrees.
    CentroidY   (float) Latitude of the catchment's centroid, in decimal degrees.

Example row:

    Catchment,AreaKm2,OutletX,OutletY,CentroidX,CentroidY
    GoldenEagles,211.52,120.1088,-21.9645,120.1194,-22.0615
"""

# ─── Constants ───────────────────────────────────────────────────────────────

RFFE_URL = "http://rffe.arr-software.org"
REQUEST_TIMEOUT = 30  # seconds


# ─── Main Entrypoint ─────────────────────────────────────────────────────────


def main() -> None:
    """Run the command line interface for fetching RFFE results."""

    parser = argparse.ArgumentParser(description="Process RFFE catchments (CLI flags optional).")
    parser.add_argument(
        "--input-dir",
        type=Path,
        help="Folder with input_catchments.csv (overrides hard-coded path)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Folder to write CSVs (overrides hard-coded path)",
    )
    args = parser.parse_args()

    input_dir = (args.input_dir or DEFAULT_INPUT_DIR).resolve()
    output_dir = (args.output_dir or DEFAULT_OUTPUT_DIR).resolve()

    logger.info("Using input directory: {}", input_dir)
    logger.info("Using output directory: {}", output_dir)

    csv_path = input_dir / "input_catchments.csv"
    if not csv_path.exists():
        logger.critical("Missing input file: {}", csv_path)
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.critical("Cannot read CSV: {}", exc)
        return

    session = requests.Session()
    agg_r = pd.DataFrame()
    agg_a = pd.DataFrame()
    failures: list[dict[str, Any]] = []

    # ← use enumerate to get a true integer counter
    for count, (_, row) in enumerate(df.iterrows(), start=1):
        raw_name = row.get("Catchment")
        # ensure it's a non-empty string, otherwise fall back
        name: str = raw_name if isinstance(raw_name, str) and raw_name else f"Catchment_{count}"

        logger.info("Processing {}/{}: {}", count, len(df), name)

        r_df, a_df, err = process_catchment(
            session=session,
            catchment=name,
            lono=float(row["OutletX"]),
            lato=float(row["OutletY"]),
            lonc=float(row["CentroidX"]),
            latc=float(row["CentroidY"]),
            area=float(row["AreaKm2"]),
            out_folder=output_dir,
        )

        if err:
            failures.append({**row.to_dict(), "Error": err})
            logger.warning("Failed {}: {}", name, err)
        else:
            if not r_df.empty:
                agg_r = pd.concat([agg_r, r_df], ignore_index=True)
            if not a_df.empty:
                agg_a = pd.concat([agg_a, a_df], ignore_index=True)

    def _save(df: pd.DataFrame, fname: str) -> None:
        path: Path = output_dir / fname
        try:
            df.to_csv(path_or_buf=path, index=False)
            logger.info("Saved {} ({} rows)", fname, len(df))
        except OSError as exc:
            logger.error("Save failed {}: {}", fname, exc)

    _save(df=agg_r, fname="rffe_results.csv")
    _save(df=agg_a, fname="all_catchment_results.csv")
    if failures:
        _save(df=pd.DataFrame(data=failures), fname="failed_catchments.csv")
        logger.error("Completed with {} failures", len(failures))
    else:
        logger.info("All catchments processed successfully.")


# ─── HTTP + Parsing Helpers ─────────────────────────────────────────────────


def fetch_rffe(
    session: requests.Session, name: str, lono: float, lato: float, lonc: float, latc: float, area: float
) -> Response | None:
    """
    Fetches RFFE data from the specified URL with given parameters.

    Args:
        name (str): Name of the catchment.
        lono (float): Longitude of the outlet.
        lato (float): Latitude of the outlet.
        lonc (float): Longitude of the centroid.
        latc (float): Latitude of the centroid.
        area (float): Area of the catchment in Km².

    Returns:
        requests.Response object if successful, None otherwise.
    """
    payload: dict[str, str] = {
        "catchment_name": name,
        "lato": str(lato),
        "lono": str(lono),
        "latc": str(latc),
        "lonc": str(lonc),
        "area": str(area),
    }

    try:
        resp: Response = session.post(url=RFFE_URL, data=payload, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Error fetching {}: {}", name, exc)
        return None

    if "<script>" in resp.text and "results =" in resp.text:
        logger.info("Fetched RFFE for {}", name)
        return resp

    logger.warning("Unexpected format for {}", name)
    return None


def parse_json_from_scripts(text: str, var_name: str) -> list[dict[str, Any]]:
    """Return a JSON array assigned to ``var_name`` inside any <script> tag."""

    pattern: str = rf"{var_name}\s*=\s*(\[\{{.*?\}}\]);"
    soup = BeautifulSoup(markup=text, features="html.parser")

    for script in soup.find_all(name="script"):
        # ← use get_text() so Pylance knows it's a str
        content: str = script.get_text()
        match: re.Match[str] | None = re.search(pattern=pattern, string=content, flags=re.DOTALL)
        if not match:
            continue

        raw_js = match.group(1).replace("'", '"')
        try:
            return json.loads(raw_js)
        except json.JSONDecodeError as exc:
            logger.error("JSON decode error for {}: {}", var_name, exc)
            break

    logger.warning("Variable '{}' not found", var_name)
    return []


def clean_rffe(text: str) -> tuple[DataFrame, DataFrame]:
    """Parse the RFFE HTML response into DataFrames."""

    results = parse_json_from_scripts(text, "results")
    all_catch = parse_json_from_scripts(text, "allCatchmentResults")

    df_r = pd.DataFrame(results) if results else pd.DataFrame()
    df_a = pd.DataFrame(all_catch) if all_catch else pd.DataFrame()

    logger.info("Parsed 'results': {} rows; 'allCatchmentResults': {} rows", len(df_r), len(df_a))
    return df_r, df_a


def save_raw_response(catchment: str, folder: Path, resp: Response) -> None:
    """Persist the raw HTML response for debugging purposes."""

    out_file = folder / f"{catchment}_rffe.txt"
    try:
        out_file.write_text(resp.text, encoding="utf-8")
        logger.info("Saved raw response to {}", out_file)
    except OSError as exc:
        logger.error("Failed to save raw for {}: {}", catchment, exc)


def process_catchment(
    session: requests.Session,
    catchment: str,
    lono: float,
    lato: float,
    lonc: float,
    latc: float,
    area: float,
    out_folder: Path,
) -> tuple[DataFrame, DataFrame, str | None]:
    """Fetch and parse RFFE results for a single catchment."""

    resp: Response | None = fetch_rffe(
        session=session,
        name=catchment,
        lono=lono,
        lato=lato,
        lonc=lonc,
        latc=latc,
        area=area,
    )
    if resp is None:
        return pd.DataFrame(), pd.DataFrame(), "fetch failed"

    save_raw_response(catchment=catchment, folder=out_folder, resp=resp)
    df_r, df_a = clean_rffe(resp.text)

    if not df_r.empty:
        df_r["Catchment"] = catchment
    if not df_a.empty:
        df_a["Catchment"] = catchment

    return df_r, df_a, None


if __name__ == "__main__":
    # initialize Loguru (console + optional file) via your helper
    setup_logger(console_log_level="INFO", log_file=None)
    main()
