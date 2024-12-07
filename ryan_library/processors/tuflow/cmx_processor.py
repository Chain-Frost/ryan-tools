import pandas as pd
import csv
import logging
from .base_processor import BaseProcessor


class CmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Cmx.csv' files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Cmx.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed CMX data.
        """
        logging.info(f"Processing CMX file: {self.file_path}")

        try:
            with self.file_path.open(mode='r', newline='') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)

            # Skip the title row, and skip the first column
            max_data = [row[1:] for row in data[1:] if len(row) > 1]

            cleaned_data = []
            for row in max_data:
                try:
                    cleaned_data.append([row[0], float(row[2]), float(row[1]), None])
                    cleaned_data.append([row[0], float(row[4]), None, float(row[3])])
                except (IndexError, ValueError) as e:
                    logging.warning(f"Skipping malformed row in {self.file_path}: {row} ({e})")
                    continue

            self.df = pd.DataFrame(cleaned_data, columns=['Chan ID', 'Time', 'Q', 'V'])
            self.add_common_columns()
            logging.debug(f"CMX DataFrame head:\n{self.df.head()}")

            return self.df

        except Exception as e:
            logging.error(f"Failed to process CMX file {self.file_path}: {e}")
            raise
