# ryan_library\processors\tuflow\cmx_processor.py
import pandas as pd
import csv
from loguru import logger
from .base_processor import BaseProcessor


class CmxProcessor(BaseProcessor):
    """
    Processor for '_1d_Cmx.csv' files.
    """

    def process(self) -> pd.DataFrame:
        """
        Process the '_1d_Cmx.csv' file and populate the class.

        Returns:
            pd.DataFrame: Processed CMX data.
        """
        logger.info(f"Processing CMX file: {self.file_path}")

        try:
            with self.file_path.open(mode="r", newline="") as csvfile:
                reader = csv.reader(csvfile)
                data = list(reader)

            if not data:
                logger.error(f"No data in file: {self.file_path}")
                return self.df

            # The first row is expected to be a header row with metadata.
            # Format (example):
            # "Run_Info","Chan ID","Qmax","Time Qmax","Vmax","Time Vmax"
            header = data[0]
            logger.debug(f"Header row: {header}")

            # Subsequent rows contain channel data.
            # Each row format (example):
            # Row[0]: Row count, Row[1]: "Chan ID",
            # Row[2]: Qmax, Row[3]: Time Qmax, Row[4]: Vmax, Row[5]: Time Vmax

            # We skip the first column in rows (row[0]), keep from row[1:] onwards
            max_data = [row[1:] for row in data[1:] if len(row) > 1]

            cleaned_data = []
            for row in max_data:
                try:
                    chan_id = row[0].strip()
                    qmax = float(row[1])
                    time_qmax = float(row[2])
                    vmax = float(row[3])
                    time_vmax = float(row[4])

                    # Reshape data into two rows per channel:
                    # One row for Q at Qmax time, another row for V at Vmax time.
                    cleaned_data.append([chan_id, time_qmax, qmax, None])
                    cleaned_data.append([chan_id, time_vmax, None, vmax])
                except (IndexError, ValueError) as e:
                    logger.warning(
                        f"Skipping malformed row in {self.file_path}: {row} ({e})"
                    )
                    continue

            self.df = pd.DataFrame(cleaned_data, columns=["Chan ID", "Time", "Q", "V"])
            self.apply_datatypes_to_df()
            self.processed = True
            # Add common columns (run code details, categories, etc.)
            self.add_common_columns()

            return self.df

        except Exception as e:
            logger.error(f"Failed to process CMX file {self.file_path}: {e}")
            raise
