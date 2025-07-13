# ryan-scripts/misc-python/RFFE_Only_v5.py
# 2025-07-13 RFFE extractor update v5  ──────────────────────────────────────────────────────────────────────────────
#  RFFE batch extractor – single-file
#
#  Copy this file anywhere and run:
#        python RFFE_Only_v5.py
# OR     python RFFE_Only_v5.py [--input-dir DIR] [--output-dir DIR]
#
#  Dependencies: pandas  requests  ryan-tools
# ─────────────────────────────────────────────────────────────────────────────

import argparse
import ast
from collections.abc import Hashable
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final
import pandas as pd
from pandas import DataFrame
import requests
from ryan_library.functions.loguru_helpers import logger, setup_logger

# ─── Hard-coded defaults ────────────────────────────────────────────────────
DEFAULT_INPUT_DIR: Final[Path] = Path(r"C:\Temp\tester")
DEFAULT_OUTPUT_DIR: Final[Path] = DEFAULT_INPUT_DIR
INPUT_FILE_NAME: str = "input_catchments.csv"
RFFE_URL: Final[str] = "http://rffe.arr-software.org"
REQUEST_TIMEOUT: Final[int] = 30  # seconds

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


# ─── Data model ──────────────────────────────────────────────────────────────
@dataclass(slots=True, frozen=True)
class Catchment:
    """Row from the input CSV, normalised and typed.
    Args:
    name (str): Name of the catchment.
    outlet_x (float): Longitude of the outlet. Decimal degrees.
    lato (float): Latitude of the outlet.  Decimal degrees.
    lonc (float): Longitude of the centroid. Decimal degrees.
    latc (float): Latitude of the centroid. Decimal degrees.
    area (float): Area of the catchment in Km²."""

    name: str
    area_km2: float
    outlet_x: float
    outlet_y: float
    centroid_x: float
    centroid_y: float

    # ── Constructors ──
    @classmethod
    def from_record(cls, rec: dict[Hashable, Any], idx: int) -> "Catchment":
        raw_name: str | None = rec.get("Catchment")
        name: str = raw_name.strip() if isinstance(raw_name, str) and raw_name.strip() else f"Catchment_{idx}"
        return cls(
            name=name,
            area_km2=float(rec["AreaKm2"]),
            outlet_x=float(rec["OutletX"]),
            outlet_y=float(rec["OutletY"]),
            centroid_x=float(rec["CentroidX"]),
            centroid_y=float(rec["CentroidY"]),
        )

    # ── HTTP payload ──
    def payload(self) -> dict[str, str]:
        return {
            "catchment_name": self.name,
            "lono": str(self.outlet_x),
            "lato": str(self.outlet_y),
            "lonc": str(self.centroid_x),
            "latc": str(self.centroid_y),
            "area": str(self.area_km2),
        }


# ─── HTML / JSON helpers ─────────────────────────────────────────────────────
def _extract_js_array(text: str, var_name: str) -> list[dict[str, Any]]:
    """
    Locate `var_name = [ ... ]` in the HTML, use bracket counting to extract the complete
    array (handles nested braces), sanitize trailing commas, and parse to Python.
    """
    marker: str = f"{var_name} ="
    pos: int = text.find(marker)
    if pos < 0:
        logger.warning(f"Variable '{var_name}' not found in HTML")
        return []

    # find opening bracket
    start: int = text.find("[", pos)
    if start < 0:
        logger.warning(f"Opening '[' for '{var_name}' not found")
        return []

    # bracket depth scan
    depth = 0
    end: int | None = None
    for i, ch in enumerate(iterable=text[start:], start=start):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        logger.error(f"Could not find matching ']' for '{var_name}'")
        return []

    raw_js: str = text[start : end + 1]
    # remove trailing commas before } and ]
    sanitized: str = re.sub(pattern=r",\s*(\}|])", repl=r"\1", string=raw_js)
    try:
        return json.loads(sanitized)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(node_or_string=raw_js)
        except Exception as exc:
            logger.error(f"Failed to parse {var_name}: {exc}")
            return []


def _clean_rffe(text: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (results, allCatchmentResults) DataFrames from RFFE HTML."""
    df_results = pd.DataFrame(data=_extract_js_array(text=text, var_name="results"))
    df_all = pd.DataFrame(data=_extract_js_array(text=text, var_name="allCatchmentResults"))
    logger.info(f"Parsed results: {len(df_results)} rows; allCatchmentResults: {len(df_all)} rows")
    return df_results, df_all


# ─── Network helpers ─────────────────────────────────────────────────────────
def _fetch_rffe(session: requests.Session, ct: Catchment) -> requests.Response | None:
    """POST to the RFFE endpoint and return a Response, or None on error."""
    try:
        resp: requests.Response = session.post(RFFE_URL, data=ct.payload(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error(f"HTTP error for {ct.name}: {exc}")
        return None

    if "<script>" in resp.text and "results =" in resp.text:
        return resp

    logger.warning(f"Unexpected response format for {ct.name}")
    return None


def _save_raw_html(ct: Catchment, folder: Path, resp: requests.Response) -> None:
    try:
        file_path: Path = folder / f"{ct.name}_rffe.txt"
        file_path.write_text(data=resp.text, encoding="utf-8")
        logger.debug(f"Saved raw response {file_path}")
    except OSError as exc:
        logger.error(f"Failed to save raw HTML for {ct.name}: {exc}")


# ─── File I/O ────────────────────────────────────────────────────────────────
def _save_df(df: pd.DataFrame, path: Path) -> None:
    try:
        df.to_csv(path_or_buf=path, index=False)
        logger.info(f"Wrote {path.name} ({len(df)} rows)")
    except OSError as exc:
        logger.error(f"Failed to write {path.name}: {exc}")


# ─── Per-catchment workflow ──────────────────────────────────────────────────
def _process_catchment(
    session: requests.Session,
    ct: Catchment,
    out_folder: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    resp: requests.Response | None = _fetch_rffe(session=session, ct=ct)
    if resp is None:
        return pd.DataFrame(), pd.DataFrame(), "fetch failed"

    _save_raw_html(ct=ct, folder=out_folder, resp=resp)
    df_r, df_a = _clean_rffe(text=resp.text)
    if not df_r.empty:
        df_r["Catchment"] = ct.name
    if not df_a.empty:
        df_a["Catchment"] = ct.name

    return df_r, df_a, None


# ─── CLI entrypoint ──────────────────────────────────────────────────────────
def main() -> None:
    """Command-line interface."""
    parser = argparse.ArgumentParser("Batch-process RFFE catchments.")
    parser.add_argument("--input-dir", type=Path, help="Folder containing input_catchments.csv")
    parser.add_argument("--output-dir", type=Path, help="Folder to write CSV outputs")
    args: argparse.Namespace = parser.parse_args()
    input_dir: Path = (args.input_dir or DEFAULT_INPUT_DIR).resolve()
    output_dir: Path = (args.output_dir or DEFAULT_OUTPUT_DIR).resolve()
    csv_path: Path = input_dir / INPUT_FILE_NAME

    logger.info(f"Input  directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")

    if not csv_path.exists():
        logger.critical(f"Missing input file: {csv_path}")
        return

    try:
        df_in: DataFrame = pd.read_csv(filepath_or_buffer=csv_path)  # type: ignore
    except Exception as exc:
        logger.critical(f"Cannot read CSV: {exc}")
        return

    catchments: list[Catchment] = [
        Catchment.from_record(rec=rec, idx=idx)
        for idx, rec in enumerate(iterable=df_in.to_dict(orient="records"), start=1)
    ]

    agg_results: list[pd.DataFrame] = []
    agg_all: list[pd.DataFrame] = []
    failures: list[dict[str, Any]] = []

    with requests.Session() as session:
        for i, ct in enumerate(iterable=catchments, start=1):
            logger.info(f"Processing {i}/{len(catchments)}: {ct.name}")
            df_r, df_a, err = _process_catchment(session, ct, output_dir)
            if err:
                failures.append({"Catchment": ct.name, "Error": err})
                logger.warning(f"❌  {ct.name} – {err}")
                continue
            if not df_r.empty:
                agg_results.append(df_r)
            if not df_a.empty:
                agg_all.append(df_a)

    output_dir.mkdir(parents=True, exist_ok=True)
    if agg_results:
        _save_df(df=pd.concat(objs=agg_results, ignore_index=True), path=output_dir / "rffe_results.csv")
    if agg_all:
        _save_df(df=pd.concat(objs=agg_all, ignore_index=True), path=output_dir / "all_catchment_results.csv")
    if failures:
        _save_df(df=pd.DataFrame(data=failures), path=output_dir / "failed_catchments.csv")
        logger.error(f"Completed with {len(failures)} failure(s).")
    else:
        logger.info("All catchments processed successfully.")


if __name__ == "__main__":
    setup_logger(console_log_level="INFO", log_file=None)
    main()
