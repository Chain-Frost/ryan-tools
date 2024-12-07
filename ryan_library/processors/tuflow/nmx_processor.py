import pandas as pd
import csv
import logging
from .base_processor import BaseProcessor


class NmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Nmx.csv' files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Nmx.csv' file and return a cleaned DataFrame.

        Returns:
            pd.DataFrame: Processed NMX data.
        """
        logging.info(f"Processing NMX file: {self.file_path}")

        try:
            with self.file_path.open(mode='r', newline='') as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)

            # Skip the title row, and skip the first column
            max_data = [row[1:] for row in data[1:] if len(row) > 1]

            cleaned_data = []
            for row in max_data:
                try:
                    chan_id = row[0][:-2]
                    node_suffix = row[0][-2:]
                    if node_suffix == '.1':  # Upstream node
                        cleaned_data.append([chan_id, float(row[2]), float(row[1]), None])
                    elif node_suffix == '.2':  # Downstream node
                        cleaned_data.append([chan_id, float(row[2]), None, float(row[1])])
                    else:
                        logging.warning(f"Unhandled node index in file {self.file_path}: {row[0]}")
                        continue  # Skip or handle as needed
                except (IndexError, ValueError) as e:
                    logging.warning(f"Skipping malformed row in {self.file_path}: {row} ({e})")
                    continue

            self.df = pd.DataFrame(cleaned_data, columns=['Chan ID', 'Time', 'US_h', 'DS_h'])
            self.add_common_columns()
            logging.debug(f"NMX DataFrame head:\n{self.df.head()}")

            return self.df

        except Exception as e:
            logging.error(f"Failed to process NMX file {self.file_path}: {e}")
            raise
