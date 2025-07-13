# ryan-scripts/misc-python/RFFE_Only_v5.py
# 2025-07-15 RFFE extractor update
# ──────────────────────────────────────────────────────────────────────────────
#  RFFE batch extractor – single-file wrapper
#
#  Copy this file anywhere and run:
#        python RFFE_Only_v5.py
# OR     python RFFE_Only_v5.py [--input-dir DIR] [--output-dir DIR]
#
#  Dependencies:  pandas  requests  beautifulsoup4  ryan-tools
# ──────────────────────────────────────────────────────────────────────────────


import argparse
import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final
import pandas as pd
import requests
from bs4 import BeautifulSoup
from ryan_library.functions.loguru_helpers import logger, setup_logger

# ─── Hard-coded defaults (override via CLI) ───────────────────────────────────
DEFAULT_INPUT_DIR: Final[Path] = Path(r"C:\Temp\tester")
DEFAULT_OUTPUT_DIR: Final[Path] = DEFAULT_INPUT_DIR
INPUT_FILE_NAME: str = "catchments.csv"


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
RFFE_URL: Final = "http://rffe.arr-software.org"
REQUEST_TIMEOUT: Final = 30  # s
# Regex compiled once and reused for every script tag
_RE_BASE: Final[str] = r"{var}\s*=\s*(\[\{{.*?\}}\]);?"

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
    def from_record(cls, rec: dict[str, Any], idx: int) -> "Catchment":
        raw_name = rec.get("Catchment")
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
    Return the JSON array assigned to `var_name` inside any <script> tag.

    The function first tries json.loads, falling back to ast.literal_eval to cope
    with single quotes or trailing semicolons that break strict JSON.
    """
    pattern: re.Pattern[str] = re.compile(pattern=_RE_BASE.format(var=var_name), flags=re.DOTALL)
    soup = BeautifulSoup(markup=text, features="html.parser")

    content: str | None
    for script in soup.find_all("script"):
        if (content := script.string or script.get_text()) is None:
            continue
        match: re.Match[str] | None = pattern.search(string=content)
        if not match:
            continue

        raw_js: str = match.group(1).rstrip(";")
        try:
            return json.loads(raw_js)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(raw_js)
            except Exception as exc:  # pragma: no cover – defensive
                logger.error("Failed to parse {}: {}", var_name, exc)
                break

    logger.warning("Variable '{}' not found in HTML", var_name)
    return []


def _clean_rffe(text: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (results, allCatchmentResults) DataFrames from RFFE HTML."""
    df_results = pd.DataFrame(data=_extract_js_array(text=text, var_name="results"))
    df_all = pd.DataFrame(data=_extract_js_array(text=text, var_name="allCatchmentResults"))
    logger.info("Parsed results: {} rows; allCatchmentResults: {} rows", len(df_results), len(df_all))
    return df_results, df_all


# ─── Network helpers ─────────────────────────────────────────────────────────


def _fetch_rffe(session: requests.Session, ct: Catchment) -> requests.Response | None:
    """POST to the RFFE endpoint and return a Response, or None on error."""
    try:
        resp: requests.Response = session.post(RFFE_URL, data=ct.payload(), timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("HTTP error for {}: {}", ct.name, exc)
        return None

    if "<script>" in resp.text and "results =" in resp.text:
        return resp

    logger.warning("Unexpected response format for {}", ct.name)
    return None


def _save_raw_html(ct: Catchment, folder: Path, resp: requests.Response) -> None:
    """Persist raw HTML for troubleshooting."""
    try:
        file_path: Path = folder / f"{ct.name}_rffe.txt"
        file_path.write_text(data=resp.text, encoding="utf-8")
        logger.debug("Saved raw response {}", file_path)
    except OSError as exc:
        logger.error("Failed to save raw HTML for {}: {}", ct.name, exc)


# ─── File I/O helper ─────────────────────────────────────────────────────────


def _save_df(df: pd.DataFrame, path: Path) -> None:
    try:
        df.to_csv(path_or_buf=path, index=False)
        logger.info("Wrote {} ({:,} rows)", path.name, len(df))
    except OSError as exc:
        logger.error(f"Failed to write {path.name}: {exc}")


# ─── Core per-catchment workflow ─────────────────────────────────────────────


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
    parser = argparse.ArgumentParser(description="Batch-process RFFE catchments.")
    parser.add_argument("--input-dir", type=Path, help="Folder containing input_catchments.csv")
    parser.add_argument("--output-dir", type=Path, help="Folder to write CSV outputs")

    args: argparse.Namespace = parser.parse_args()
    input_dir: Path = (args.input_dir or DEFAULT_INPUT_DIR).resolve()
    output_dir: Path = (args.output_dir or DEFAULT_OUTPUT_DIR).resolve()
    csv_path: Path = input_dir / INPUT_FILE_NAME

    logger.info("Input  directory: {}", input_dir)
    logger.info("Output directory: {}", output_dir)

    if not csv_path.exists():
        logger.critical("Missing input file: {}", csv_path)
        return

    try:
        df_in: pd.DataFrame = pd.read_csv(filepath_or_buffer=csv_path)  # type: ignore
    except Exception as exc:
        logger.critical("Cannot read CSV: {}", exc)
        return

    catchments: list[Catchment] = [
        Catchment.from_record(rec=rec, idx=idx)
        for idx, rec in enumerate(iterable=df_in.to_dict(orient="records"), start=1)
    ]

    agg_results: list[pd.DataFrame] | list[dict[str, Any]] = []
    agg_all: list[pd.DataFrame] | list[dict[str, Any]] = []
    failures: list[pd.DataFrame] | list[dict[str, Any]] = []

    with requests.Session() as session:
        for i, ct in enumerate(iterable=catchments, start=1):
            logger.info("Processing {}/{}: {}", i, len(catchments), ct.name)
            df_r, df_a, err = _process_catchment(session=session, ct=ct, out_folder=output_dir)

            if err:
                failures.append({"Catchment": ct.name, "Error": err})
                logger.warning("❌  {} – {}", ct.name, err)
                continue

            if not df_r.empty:
                agg_results.append(df_r)
            if not df_a.empty:
                agg_all.append(df_a)

    output_dir.mkdir(parents=True, exist_ok=True)

    if agg_results:
        _save_df(pd.concat(agg_results, ignore_index=True), output_dir / "rffe_results.csv")
    if agg_all:
        _save_df(pd.concat(agg_all, ignore_index=True), output_dir / "all_catchment_results.csv")
    if failures:
        _save_df(pd.DataFrame(failures), output_dir / "failed_catchments.csv")
        logger.error("Completed with {} failure(s).", len(failures))
    else:
        logger.info("All catchments processed successfully.")


# ─── Initialise logging & run ────────────────────────────────────────────────
if __name__ == "__main__":
    setup_logger(console_log_level="INFO", log_file=None)
    main()
