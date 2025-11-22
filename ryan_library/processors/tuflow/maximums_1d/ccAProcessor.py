# ryan_library/processors/tuflow/ccAProcessor.py
from typing import Any
from pandas import DataFrame
import fiona
import geopandas as gpd
import pandas as pd
import shapefile
import sqlite3
from loguru import logger
from ..base_processor import BaseProcessor


class ccAProcessor(BaseProcessor):
    """Processor for CCA files ('_1d_ccA_L.dbf' and '_Results1D.gpkg')."""

    def process(self) -> None:
        """Process the CCA file (DBF or GPKG) and modify self.df in place."""
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
        """Process a GeoPackage CCA file purely read-only, without creating WAL/SHM."""
        logger.debug(f"Processing GeoPackage CCA file (read-only): {self.file_path}")
        try:
            # 1. Use sqlite3 in URI-mode to list feature layers from gpkg_contents
            db_uri: str = f"file:{self.file_path}?mode=ro&uri=true"
            with sqlite3.connect(database=db_uri) as conn:
                cur: sqlite3.Cursor = conn.cursor()
                cur.execute(
                    """
                    SELECT table_name
                      FROM gpkg_contents
                     WHERE data_type = 'features';
                """
                )
                layers: list[Any] = [row[0] for row in cur.fetchall()]

            # 2. Find our CCA layer by suffix
            desired = "1d_ccA_L"
            layer_name = next((lyr for lyr in layers if lyr.endswith(desired)), "layer-not-found")

            if layer_name == "layer-not-found":
                logger.debug(f"Layer ending with '{desired}' not found in {self.file_path}. " "No data to process.")
                return pd.DataFrame()

            # 3. Read just that layer with Fiona, in read-only mode
            uri: str = f"file://{self.file_path}?mode=ro&uri=true"
            # Open that URI in read-only mode, specifying the layer
            with fiona.Env():  # clean GDAL/Fiona env
                with fiona.open(fp=uri, layer=layer_name) as src:  # pyright: ignore[reportUnknownMemberType]
                    gdf: gpd.GeoDataFrame = gpd.GeoDataFrame.from_features(features=src, crs=src.crs)  # type: ignore

            # 4. Strip geometry, rename if needed, and return
            cca_data: DataFrame = gdf.drop(columns="geometry").copy()

            # 5. Rename 'Channel' to 'Chan ID' if needed
            if "Channel" in cca_data.columns:
                cca_data.rename(columns={"Channel": "Chan ID"}, inplace=True)
            cca_data = self._normalize_column_names(df=cca_data)

            logger.debug(f"Processed GeoPackage CCA DataFrame head:\n{cca_data.head()}")
            return cca_data

        except Exception as e:
            logger.error(f"Error processing GeoPackage file {self.file_path}: {e}")
            return pd.DataFrame()
