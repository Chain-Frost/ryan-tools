# ryan_library/processors/tuflow/maximums_1d/ccAProcessor.py
from pathlib import Path
from typing import Any
import os
import sqlite3
import urllib.parse

import pandas as pd
import shapefile  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger
from pandas import DataFrame

from ..base_processor import BaseProcessor


class ccAProcessor(BaseProcessor):
    """Processor for CCA files ('_1d_ccA_L.dbf' and '_Results1D.gpkg')."""

    # Common renames across both DBF and GPKG sources
    _COLUMN_RENAMES: dict[str, str] = {"Channel": "Chan ID"}
    # Mapping of shapefile-safe field names (max 10 chars) to canonical names
    _SHAPEFILE_COLUMN_RENAMES: dict[str, str] = {"Dur_10pFul": "Dur_10pFull"}

    def process(self) -> None:
        """Process the CCA file (DBF or GPKG) and return a cleaned DataFrame.

        Sets:
            pd.DataFrame: Processed CCA data.
        """
        logger.debug(f"Starting processing of CCA file: {self.log_path}")
        file_ext = self.file_path.suffix.lower()

        try:
            if file_ext == ".dbf":
                cca_data: pd.DataFrame = self.process_dbf()
            elif file_ext == ".gpkg":
                cca_data = self.process_gpkg()
            else:
                logger.error(f"Unsupported CCA file type: {self.log_path}")
                self.df = pd.DataFrame()
                return

            if cca_data.empty:
                logger.error(f"No data read from CCA file: {self.log_path}")
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
            logger.info(f"Completed processing of CCA file: {self.log_path}")
            return
        except Exception as e:
            logger.error(f"Failed to process CCA file {self.log_path}: {e}")
            self.df = pd.DataFrame()
            return

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename truncated or variant columns so downstream config matches."""
        rename_map: dict[str, str] = {}
        for source, target in self._COLUMN_RENAMES.items():
            if source in df.columns and target not in df.columns:
                rename_map[source] = target
        for truncated, full in self._SHAPEFILE_COLUMN_RENAMES.items():
            if truncated in df.columns and full not in df.columns:
                rename_map[truncated] = full
        if rename_map:
            logger.debug(f"{self.file_name}: Renaming columns for consistency: {rename_map}")
            df = df.rename(columns=rename_map)
        return df

    def process_dbf(self) -> pd.DataFrame:
        """Process a DBF CCA file.

        Returns:
            pd.DataFrame: Processed DBF CCA data.
        """
        logger.debug(f"Processing DBF CCA file: {self.log_path}")
        try:
            with self.file_path.open("rb") as dbf_file:
                sf = shapefile.Reader(dbf=dbf_file)
                records: list[str | float | int | object] = sf.records()  # type: ignore
                fieldnames: list[str] = [field[0] for field in sf.fields[1:]]  # type: ignore
                cca_data = pd.DataFrame(records, columns=fieldnames)
                # Normalise column names so downstream configuration and validation align
                cca_data: DataFrame = self._normalize_column_names(df=cca_data)
            logger.debug(f"Processed DBF CCA DataFrame head:\n{cca_data.head()}")
            return cca_data
        except Exception as e:
            logger.error(f"Error processing DBF file {self.log_path}: {e}")
            return pd.DataFrame()

    def process_gpkg(self) -> pd.DataFrame:
        """Process a GeoPackage CCA file in read-only mode, using sqlite only (no Fiona)."""
        path = Path(self.file_path)

        # Start / file checks
        msg: str = (
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
            path_posix: str = path.as_posix()  # e.g. q:/folder/.../file.gpkg
            uri_path: str = urllib.parse.quote(path_posix, safe="/:")
            # immutable=1 guarantees SQLite never attempts to write journal/WAL files
            db_uri: str = f"file:{uri_path}?mode=ro&immutable=1"
            logger.debug("process_gpkg: Connecting to GeoPackage via sqlite3 with db_uri={!r} (uri=True)", db_uri)

            conn = None
            try:
                conn = sqlite3.connect(database=db_uri, uri=True)
                logger.debug("process_gpkg: SQLite connection opened successfully")
                cur: sqlite3.Cursor = conn.cursor()

                try:
                    # 1. Find our ccA layer in gpkg_contents
                    logger.debug("process_gpkg: Executing layer discovery query against gpkg_contents")
                    cur.execute(
                        """
                        SELECT table_name
                        FROM gpkg_contents
                        WHERE data_type = 'features';
                        """
                    )
                    rows: list[Any] = cur.fetchall()
                    layers: list[str] = [row[0] for row in rows]
                    logger.debug("process_gpkg: Feature layers in gpkg_contents: {!r}", layers)

                    # 2. Find our CCA layer by suffix
                    desired_suffix = "1d_ccA_L"
                    layer_name: str | None = next(
                        (
                            lyr
                            for lyr in layers
                            if isinstance(lyr, str)  # pyright: ignore[reportUnnecessaryIsInstance]
                            and lyr.endswith(desired_suffix)
                        ),
                        None,
                    )
                    logger.debug(
                        "process_gpkg: Desired suffix={!r}, selected layer_name={!r}", desired_suffix, layer_name
                    )

                    if layer_name is None:
                        logger.error(
                            "process_gpkg: No layer ending with "
                            f"{desired_suffix!r} found in {str(path)!r}; treating datasource as malformed"
                        )
                        return pd.DataFrame()

                    # 2b. Inspect actual table columns via PRAGMA
                    cur.execute(f"PRAGMA table_info('{layer_name}')")
                    table_info = cur.fetchall()
                    cols_in_table = [row[1] for row in table_info]
                    logger.debug("process_gpkg: PRAGMA table_info for layer {!r}: {!r}", layer_name, table_info)
                    logger.debug("process_gpkg: Columns in table {!r}: {!r}", layer_name, cols_in_table)

                    # Exclude fid + geometry BLOB column (here named 'geom')
                    excluded: set[str] = {"fid", "geom"}
                    value_cols = [c for c in cols_in_table if c not in excluded]
                    if not value_cols:
                        logger.error(
                            "process_gpkg: No non-geometry columns found in table "
                            f"{layer_name!r}; treating datasource as malformed"
                        )
                        return pd.DataFrame()

                    col_list_sql: str = ", ".join(f'"{c}"' for c in value_cols)
                    select_sql: str = f'SELECT {col_list_sql} FROM "{layer_name}"'
                    logger.debug("process_gpkg: Running attribute SELECT on table {!r}: {}", layer_name, select_sql)

                    # 3. Load attributes into DataFrame
                    # We use manual fetchall to avoid pandas holding references to the connection
                    cur.execute(select_sql)
                    data_rows = cur.fetchall()
                    cols = [description[0] for description in cur.description]
                    cca_data = pd.DataFrame(data_rows, columns=cols)
                finally:
                    cur.close()
            except sqlite3.Error as e:
                logger.error(f"process_gpkg: SQLite error: {e}")
                return pd.DataFrame()
            finally:
                if conn:
                    conn.close()

            logger.debug(
                "process_gpkg: Raw attribute DataFrame loaded with "
                f"shape={cca_data.shape}, columns={list(cca_data.columns)!r}"
            )

            # 4. Normalise column names to align with CSV/DBF output
            logger.debug("process_gpkg: Normalising column names via _normalize_column_names()")
            cca_data = self._normalize_column_names(df=cca_data)
            logger.debug(
                "process_gpkg: After normalisation, shape={}, columns={!r}", cca_data.shape, list(cca_data.columns)
            )

            # TODO - it looks like shapefile processor does not do this.
            # could we make it a shared function as they both should be validating in some way
            # some other functions use apply_final_transformations, but it doesn't seem to be
            # regularly applied - likely many functions need updating or checking their workflow logic.
            #
            # 5. Validate required CCA columns against the configured schema
            required_cols: list[str] = list(self.output_columns.keys())
            missing: list[str] = [c for c in required_cols if c not in cca_data.columns]
            if missing:
                logger.error(
                    "process_gpkg: CCA table {!r} in file {!r} is missing required columns: {!r}. "
                    "Treating datasource as malformed.",
                    layer_name,
                    str(path),
                    missing,
                )
                return pd.DataFrame()

            logger.debug(
                "process_gpkg: Final CCA DataFrame ready to return. "
                f"shape={cca_data.shape}, columns={list(cca_data.columns)!r}"
            )
            logger.debug(f"process_gpkg: Completed successfully for file_path={str(path)!r}")

            return cca_data

        except Exception as e:
            logger.error(f"process_gpkg: Error while processing GeoPackage file {str(path)!r}: {e}")
            return pd.DataFrame()
