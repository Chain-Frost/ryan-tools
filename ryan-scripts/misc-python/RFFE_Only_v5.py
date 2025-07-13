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
from collections.abc import Hashable, Mapping, Sequence
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import pandas as pd
import requests
from pandas import DataFrame
from ryan_library.functions.loguru_helpers import logger, setup_logger

# ─── Constants ────────────────────────────────────────────────────────────────
DEFAULT_INPUT_DIR: Final[Path] = Path(r"C:\Temp\tester")
DEFAULT_OUTPUT_DIR: Final[Path] = DEFAULT_INPUT_DIR
INPUT_FILE_NAME: Final[str] = "input_catchments.csv"
RFFE_URL: Final[str] = "http://rffe.arr-software.org"
REQUEST_TIMEOUT: Final[int] = 30  # seconds

Record = Mapping[Hashable, Any]
_TRAILING_COMMA_RE: re.Pattern[str] = re.compile(pattern=r",\s*(\}|])")
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


# ─── Data + Processing Model ─────────────────────────────────────────────────
@dataclass(slots=True, frozen=True)
class Catchment:
    name: str
    area_km2: float
    outlet_x: float
    outlet_y: float
    centroid_x: float
    centroid_y: float

    @classmethod
    def from_record(cls, rec: Record, idx: int) -> "Catchment":
        raw: str | None = rec.get("Catchment", default="")
        name: str = raw.strip() if isinstance(raw, str) and raw.strip() else f"Catchment_{idx}"
        return cls(
            name=name,
            area_km2=float(rec["AreaKm2"]),
            outlet_x=float(rec["OutletX"]),
            outlet_y=float(rec["OutletY"]),
            centroid_x=float(rec["CentroidX"]),
            centroid_y=float(rec["CentroidY"]),
        )

    def payload(self) -> dict[str, str]:
        return {
            "catchment_name": self.name,
            "lono": str(self.outlet_x),
            "lato": str(self.outlet_y),
            "lonc": str(self.centroid_x),
            "latc": str(self.centroid_y),
            "area": str(self.area_km2),
        }

    @staticmethod
    def _extract_js_array(html: str, var_name: str) -> Sequence[Record]:
        """Locate `var_name = [ ... ]` in the HTML, use bracket counting to extract the complete
        array (handles nested braces), sanitize trailing commas, and parse to Python."""
        start_idx: int = html.find(f"{var_name} =")
        if start_idx < 0:
            logger.warning(f"'{var_name}' not found")
            return []
        # find the opening '['
        start: int = html.find("[", start_idx)
        if start < 0:
            logger.warning(f"No '[' for '{var_name}'")
            return []
        # bracket counting
        depth = 0
        for i, ch in enumerate(iterable=html[start:], start=start):
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    raw: str = html[start : i + 1]
                    break
        else:
            logger.error(f"No matching ']' for '{var_name}'")
            return []

        cleaned: str = re.sub(pattern=_TRAILING_COMMA_RE, repl=r"\1", string=raw)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(node_or_string=raw)
            except Exception as ex:
                logger.error(f"Failed to parse JS '{var_name}': {ex}")
                return []

    def parse(self, html: str) -> tuple[DataFrame, DataFrame]:
        """Return (results_df, allCatchmentResults_df)."""
        results: Sequence[Record] = self._extract_js_array(html=html, var_name="results")
        all_r: Sequence[Record] = self._extract_js_array(html=html, var_name="allCatchmentResults")
        df_r = pd.DataFrame(data=results)
        df_a = pd.DataFrame(data=all_r)
        logger.info(f"[{self.name}] Parsed {len(df_r)} results, {len(df_a)} allCatchmentResults")
        return df_r, df_a

    def fetch(self, session: requests.Session) -> requests.Response | None:
        """POST to RFFE and return Response or None on error."""
        try:
            resp: requests.Response = session.post(url=RFFE_URL, data=self.payload(), timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as ex:
            logger.error(f"[{self.name}] HTTP error: {ex}")
            return None

        text: str = resp.text
        if "<script>" in text and "results =" in text:
            return resp

        logger.warning(f"[{self.name}] Unexpected response format")
        return None

    def save_raw_html(self, html: str, folder: Path) -> None:
        path: Path = folder / f"{self.name}_rffe.txt"
        try:
            path.write_text(data=html, encoding="utf-8")
            logger.debug(f"[{self.name}] Saved raw HTML to {path.name}")
        except OSError as ex:
            logger.error(f"[{self.name}] Could not save HTML: {ex}")

    def process(self, session: requests.Session, out_dir: Path) -> tuple[DataFrame, DataFrame, str | None]:
        resp: requests.Response | None = self.fetch(session)
        if resp is None:
            return pd.DataFrame(), pd.DataFrame(), "fetch failed"

        self.save_raw_html(html=resp.text, folder=out_dir)
        df_r, df_a = self.parse(resp.text)

        if not df_r.empty:
            df_r["Catchment"] = self.name
        if not df_a.empty:
            df_a["Catchment"] = self.name

        return df_r, df_a, None


# ─── File I/O Helpers ────────────────────────────────────────────────────────
def save_df(df: DataFrame, path: Path) -> None:
    try:
        df.to_csv(path_or_buf=path, index=False)
        logger.info(f"Wrote {path.name} ({len(df)} rows)")
    except OSError as ex:
        logger.error(f"Failed to write {path.name}: {ex}")


# ─── Main Entrypoint ─────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser("Batch-process RFFE catchments.")
    p.add_argument("--input-dir", type=Path, help="Folder with input_catchments.csv")
    p.add_argument("--output-dir", type=Path, help="Folder for CSV outputs")
    p.add_argument(
        "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Console log level"
    )
    args: argparse.Namespace = p.parse_args()

    setup_logger(console_log_level=args.log_level, log_file=None)

    inp: Path = (args.input_dir or DEFAULT_INPUT_DIR).resolve()
    out: Path = (args.output_dir or DEFAULT_OUTPUT_DIR).resolve()
    csv_fp: Path = inp / INPUT_FILE_NAME

    logger.info(f"Input dir:  {inp}")
    logger.info(f"Output dir: {out}")

    if not csv_fp.exists():
        logger.critical(f"Missing input file: {csv_fp}")
        return 1

    try:
        df_in: DataFrame = pd.read_csv(filepath_or_buffer=csv_fp)
    except Exception as ex:
        logger.critical(f"Cannot read CSV: {ex}")
        return 1

    out.mkdir(parents=True, exist_ok=True)
    catchments: list[Catchment] = [
        Catchment.from_record(rec=rec, idx=idx)
        for idx, rec in enumerate(iterable=df_in.to_dict(orient="records"), start=1)
    ]

    all_res: list[DataFrame] = []
    all_all: list[DataFrame] = []
    fails: list[dict[str, Any]] = []

    with requests.Session() as sess:
        for idx, ct in enumerate(iterable=catchments, start=1):
            logger.info(f"[{idx}/{len(catchments)}] Processing '{ct.name}'")
            df_r, df_a, err = ct.process(session=sess, out_dir=out)
            if err:
                fails.append({"Catchment": ct.name, "Error": err})
                logger.warning(f"❌ {ct.name}: {err}")
                continue
            if not df_r.empty:
                all_res.append(df_r)
            if not df_a.empty:
                all_all.append(df_a)

    if all_res:
        save_df(df=pd.concat(objs=all_res, ignore_index=True), path=out / "rffe_results.csv")
    if all_all:
        save_df(df=pd.concat(objs=all_all, ignore_index=True), path=out / "all_catchment_results.csv")
    if fails:
        save_df(df=pd.DataFrame(data=fails), path=out / "failed_catchments.csv")
        logger.error(f"Completed with {len(fails)} failure(s).")
        return 1

    logger.info("All catchments processed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
