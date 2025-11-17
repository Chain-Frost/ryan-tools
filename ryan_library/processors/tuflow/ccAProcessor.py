# ryan_library/processors/tuflow/ccAProcessor.py
from typing import Any
from pandas import DataFrame
import fiona
import geopandas as gpd
import pandas as pd
import shapefile
import sqlite3
from loguru import logger
from pathlib import Path
import os
import urllib.parse

from .base_processor import BaseProcessor


class ccAProcessor(BaseProcessor):
    """Processor for CCA files ('_1d_ccA_L.dbf' and '_Results1D.gpkg')."""

    def process(self) -> None:
        """Process the CCA file (DBF or GPKG) and return a cleaned DataFrame.
        Returns:
            pd.DataFrame: Processed CCA data."""
        logger.debug(f"Starting processing of CCA file: {self.file_path}")
        file_ext = self.file_path.suffix.lower()

        try:
            if file_ext == ".dbf":
                cca_data: pd.DataFrame = self.process_dbf()
            elif file_ext == ".gpkg":
                cca_data = self.process_gpkg()
            else:
                logger.error(f"Unsupported CCA file type: {self.file_path}")
                self.df = pd.DataFrame()
                return

            if cca_data.empty:
                logger.error(f"No data read from CCA file: {self.file_path}")
                self.df = pd.DataFrame()
                return

            self.df = cca_data

            # Add common columns like run codes, paths, etc.
            self.add_common_columns()

            # Apply output transformations (data types) as per config
            self.apply_output_transformations()

            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of CCA file: {self.file_path}")
            return
        except Exception as e:
            logger.error(f"Failed to process CCA file {self.file_path}: {e}")
            self.df = pd.DataFrame()
            return

    # Mapping of shapefile-safe field names (max 10 chars) to canonical names
    _SHAPEFILE_COLUMN_RENAMES: dict[str, str] = {"Dur_10pFul": "Dur_10pFull"}

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename truncated shapefile fields so downstream config matches."""
        rename_map: dict[str, str] = {
            truncated: full
            for truncated, full in self._SHAPEFILE_COLUMN_RENAMES.items()
            if truncated in df.columns and full not in df.columns
        }
        if rename_map:
            logger.debug(f"{self.file_name}: Renaming truncated shapefile columns: {rename_map}")
            df = df.rename(columns=rename_map)
        return df

    def process_dbf(self) -> pd.DataFrame:
        """Process a DBF CCA file.
        Returns:
            pd.DataFrame: Processed DBF CCA data."""
        logger.debug(f"Processing DBF CCA file: {self.file_path}")
        try:
            with self.file_path.open("rb") as dbf_file:
                sf = shapefile.Reader(dbf=dbf_file)
                records = sf.records()
                fieldnames = [field[0] for field in sf.fields[1:]]  # Skip DeletionFlag
                cca_data = pd.DataFrame(records, columns=fieldnames)
                # Rename 'Channel' to 'Chan ID' if present
                if "Channel" in cca_data.columns:
                    cca_data.rename(columns={"Channel": "Chan ID"}, inplace=True)
                cca_data: DataFrame = self._normalize_column_names(df=cca_data)
            logger.debug(f"Processed DBF CCA DataFrame head:\n{cca_data.head()}")
            return cca_data
        except Exception as e:
            logger.error(f"Error processing DBF file {self.file_path}: {e}")
            return pd.DataFrame()

    def process_gpkg(self) -> pd.DataFrame:
        """Process a GeoPackage CCA file in read-only mode, using sqlite only (no Fiona)."""
        path = Path(self.file_path)

        # Start / file checks
        msg = (
            "process_gpkg: Starting. "
            f"file_path={str(path)!r}, "
            f"exists={path.exists()}, "
            f"is_file={path.is_file()}, "
            f"readable={os.access(path, os.R_OK)}"
        )
        logger.debug(msg)

        if not path.exists():
            logger.error(f"process_gpkg: File does not exist: {str(path)!r}")
            return pd.DataFrame()

        try:
            # Build SQLite URI in read-only mode
            path_posix = path.as_posix()  # e.g. q:/BGER/.../file.gpkg
            uri_path = urllib.parse.quote(path_posix, safe="/:")
            db_uri: str = f"file:{uri_path}?mode=ro"

            logger.debug(f"process_gpkg: Connecting to GeoPackage via sqlite3 with db_uri={db_uri!r} (uri=True)")

            with sqlite3.connect(database=db_uri, uri=True) as conn:
                logger.debug("process_gpkg: SQLite connection opened successfully")
                cur: sqlite3.Cursor = conn.cursor()

                # 1. Find our ccA layer in gpkg_contents
                logger.debug("process_gpkg: Executing layer discovery query against gpkg_contents")
                cur.execute(
                    """
                    SELECT table_name
                    FROM gpkg_contents
                    WHERE data_type = 'features';
                    """
                )
                rows = cur.fetchall()
                layers: list[str] = [row[0] for row in rows]

                logger.debug("process_gpkg: Feature layers in gpkg_contents: " f"{layers!r}")

                desired_suffix = "1d_ccA_L"
                layer_name = next(
                    (lyr for lyr in layers if isinstance(lyr, str) and lyr.endswith(desired_suffix)),
                    None,
                )

                logger.debug("process_gpkg: Desired suffix=" f"{desired_suffix!r}, selected layer_name={layer_name!r}")

                if layer_name is None:
                    logger.error(
                        "process_gpkg: No layer ending with "
                        f"{desired_suffix!r} found in {str(path)!r}; treating datasource as malformed"
                    )
                    return pd.DataFrame()

                # 2. Inspect actual table columns via PRAGMA
                cur.execute(f"PRAGMA table_info('{layer_name}')")
                table_info = cur.fetchall()
                cols_in_table = [row[1] for row in table_info]

                logger.debug("process_gpkg: PRAGMA table_info for layer " f"{layer_name!r}: {table_info!r}")
                logger.debug("process_gpkg: Columns in table " f"{layer_name!r}: {cols_in_table!r}")

                # Exclude fid + geometry BLOB column (here named 'geom')
                excluded = {"fid", "geom"}
                value_cols = [c for c in cols_in_table if c not in excluded]

                if not value_cols:
                    logger.error(
                        "process_gpkg: No non-geometry columns found in table "
                        f"{layer_name!r}; treating datasource as malformed"
                    )
                    return pd.DataFrame()

                col_list_sql = ", ".join(f'"{c}"' for c in value_cols)
                select_sql = f'SELECT {col_list_sql} FROM "{layer_name}"'

                logger.debug("process_gpkg: Running attribute SELECT on table " f"{layer_name!r}: {select_sql}")

                # 3. Load attributes into DataFrame
                cca_data: DataFrame = pd.read_sql_query(select_sql, conn)

            logger.debug(
                "process_gpkg: Raw attribute DataFrame loaded with "
                f"shape={cca_data.shape}, columns={list(cca_data.columns)!r}"
            )

            # 4. Rename Channel -> Chan ID
            if "Channel" in cca_data.columns:
                logger.debug("process_gpkg: Renaming column 'Channel' -> 'Chan ID'")
                cca_data.rename(columns={"Channel": "Chan ID"}, inplace=True)
            else:
                logger.debug("process_gpkg: Column 'Channel' not present; no renaming applied")

            # 5. Normalise column names
            logger.debug("process_gpkg: Normalising column names via _normalize_column_names()")
            cca_data = self._normalize_column_names(df=cca_data)

            logger.debug(
                "process_gpkg: After normalisation, shape=" f"{cca_data.shape}, columns={list(cca_data.columns)!r}"
            )

            # 6. Validate required CCA columns
            required_cols: list[str] = [
                "Chan ID",
                "pFull_Max",
                "pTime_Full",
                "Area_Max",
                "Area_Culv",
                "Dur_Full",
                "Dur_10pFull",
                "Sur_CD",
                "Dur_Sur",
                "pTime_Sur",
                "TFirst_Sur",
            ]

            missing = [c for c in required_cols if c not in cca_data.columns]
            if missing:
                logger.error(
                    "process_gpkg: CCA table "
                    f"{layer_name!r} in file {str(path)!r} is missing required columns: {missing!r}. "
                    "Treating datasource as malformed."
                )
                return pd.DataFrame()

            logger.debug(
                "process_gpkg: Final CCA DataFrame ready to return. "
                f"shape={cca_data.shape}, columns={list(cca_data.columns)!r}"
            )
            logger.debug(f"process_gpkg: Completed successfully for file_path={str(path)!r}")

            return cca_data

        except Exception as e:
            logger.exception(f"process_gpkg: Error while processing GeoPackage file {str(path)!r}: {e}")
            return pd.DataFrame()
