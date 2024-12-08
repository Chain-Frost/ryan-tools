import pandas as pd
import logging
from .base_processor import BaseProcessor


class ChanProcessor(BaseProcessor):
    """
    Processor for '_1d_Chan.csv' files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Chan.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed Chan data.
        """
        logging.info(f"Processing Chan file: {self.file_path}")

        try:
            # Read CSV data
            data = pd.read_csv(self.file_path)

            # Drop unnecessary columns
            columns_to_drop = [
                "US Node",
                "DS Node",
                "US Channel",
                "DS Channel",
                "Form Loss",
                "RBUS Obvert",
                "RBDS Obvert",
            ]
            data.drop(columns=columns_to_drop, inplace=True, errors="ignore")
            logging.debug(f"Columns after dropping: {data.columns.tolist()}")

            # Calculate Height
            data["Height"] = data["LBUS Obvert"] - data["US Invert"]

            # Rename 'Channel' to 'Chan ID'
            data.rename(columns={"Channel": "Chan ID"}, inplace=True)

            self.df = data
            self.add_common_columns()
            logging.debug(f"Chan DataFrame head:\n{self.df.head()}")

            return self.df

        except Exception as e:
            logging.error(f"Failed to process Chan file {self.file_path}: {e}")
            raise
