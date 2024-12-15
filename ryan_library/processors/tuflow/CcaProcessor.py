import pandas as pd
import geopandas as gpd
import shapefile
import logging
from pathlib import Path
from .base_processor import BaseProcessor


class CcaProcessor(BaseProcessor):
    """
    Processor for CCA files ('_1d_ccA_L.dbf' and '_Results1D.gpkg').
    """

    SUPPORTED_EXTENSIONS = {'.dbf': 'dbf', '.gpkg': 'gpkg'}

    def process(self) -> pd.DataFrame:
        """
        Process the CCA file (DBF or GPKG) and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed CCA data.
        """
        logging.info(f"Processing CCA file: {self.file_path}")
        file_ext = self.file_path.suffix.lower()

        if file_ext not in self.SUPPORTED_EXTENSIONS:
            logging.error(f"Unsupported CCA file type: {self.file_path}")
            raise ValueError(f"Unsupported CCA file type: {self.file_path}")

        try:
            process_method = getattr(self, f"process_{self.SUPPORTED_EXTENSIONS[file_ext]}")
            cca_data = process_method()
            self.df = cca_data
            self.add_common_columns()
            logging.debug(f"Processed CCA DataFrame head:\n{self.df.head()}")
            return self.df
        except Exception as e:
            logging.error(f"Failed to process CCA file {self.file_path}: {e}")
            raise

    def process_dbf(self) -> pd.DataFrame:
        """
        Process a DBF CCA file.

        Returns:
            pd.DataFrame: Processed DBF CCA data.
        """
        try:
            with self.file_path.open('rb') as dbf_file:
                sf = shapefile.Reader(dbf=dbf_file)
                records = sf.records()
                fieldnames = [field[0] for field in sf.fields[1:]]  # Skip DeletionFlag
                cca_data = pd.DataFrame(records, columns=fieldnames)
                cca_data.rename(columns={'Channel': 'Chan ID'}, inplace=True)
            logging.debug(f"Processed DBF CCA DataFrame head:\n{cca_data.head()}")
            return cca_data
        except Exception as e:
            logging.error(f"Error processing DBF file {self.file_path}: {e}")
            raise

    def process_gpkg(self) -> pd.DataFrame:
        """
        Process a GeoPackage CCA file.

        Returns:
            pd.DataFrame: Processed GeoPackage CCA data.
        """
        try:
            layers = gpd.io.file.fiona.listlayers(str(self.file_path))
            layer_name = next((layer for layer in layers if layer.endswith('1d_ccA_L')), None)

            if layer_name is None:
                raise ValueError("No layer found with '1d_ccA_L' in the GeoPackage.")

            gdf = gpd.read_file(self.file_path, layer=layer_name)
            cca_data = gdf.drop(columns='geometry').copy()
            if 'Channel' in cca_data.columns:
                cca_data.rename(columns={'Channel': 'Chan ID'}, inplace=True)
            logging.debug(f"Processed GeoPackage CCA DataFrame head:\n{cca_data.head()}")
            return cca_data
        except Exception as e:
            logging.error(f"Error processing GeoPackage file {self.file_path}: {e}")
            raise
