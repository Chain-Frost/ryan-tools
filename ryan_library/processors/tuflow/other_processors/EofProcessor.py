# ryan_library/processors/tuflow/other_processors/EofProcessor.py

from ..base_processor import BaseProcessor
import pandas as pd
from loguru import logger
import io


class EOFProcessor(BaseProcessor):
    """Processor for '.eof' files."""

    def process(self) -> None:
        """Process the '.eof' file and extract CULVERT AND PIPE DATA."""
        logger.info(f"Starting processing of EOF file: {self.log_path}")

        try:
            with open(self.file_path, "r", encoding="utf-8", errors="replace") as f:
                lines: list[str] = f.readlines()

            start_line_idx = -1
            header_line_idx = -1
            end_line_idx = -1

            # Find the start of the section
            for i, line in enumerate(lines):
                if "CULVERT AND PIPE DATA" in line:
                    start_line_idx = i
                if start_line_idx != -1 and line.strip().startswith("Channel"):
                    header_line_idx = i
                    break

            if start_line_idx == -1 or header_line_idx == -1:
                logger.warning(f"{self.file_path.name}: 'CULVERT AND PIPE DATA' section not found.")
                self.df = pd.DataFrame()
                return

            # Find the end of the data block (first blank line after header)
            for i in range(header_line_idx + 1, len(lines)):
                if not lines[i].strip():
                    end_line_idx = i
                    break

            if end_line_idx == -1:
                end_line_idx = len(lines)

            # Extract the data block lines
            data_lines = lines[header_line_idx:end_line_idx]

            if not data_lines:
                logger.warning(f"{self.file_path.name}: No data lines found.")
                self.df = pd.DataFrame()
                return

            # Create a string buffer, skipping the header line for better inference
            # The header line in EOF files is often complex and can confuse read_fwf inference
            data_content = "".join(data_lines[1:])

            # Define column names manually as the header is complex
            col_names: list[str] = [
                "Chan ID",
                "Type",
                "Num_barrels",
                "US_Invert",
                "DS_Invert",
                "US_Obvert",
                "DS_Obvert",
                "Length",
                "Slope",
                "Mannings_n",
                "Diam_Width",
                "Height",
                "Inlet_Height",
                "Inlet_Width",
                "Entry Loss",
                "Exit Loss",
                "Fixed Loss",
                "Ent/Exit Losses",
            ]

            # Parse with read_fwf
            self.df = pd.read_fwf(
                io.StringIO(data_content),
                names=col_names,
                header=None,  # We skipped the header line
                na_values=["-----", "Adjusted", "---"],
                keep_default_na=True,
                infer_nrows=100,
            )

            # Handle '-----' in Height column (if not caught by na_values)
            if "Height" in self.df.columns:
                self.df["Height"] = pd.to_numeric(self.df["Height"], errors="coerce")

            # Rename columns to match ChanProcessor conventions
            rename_map: dict[str, str] = {
                "US_Invert": "US Invert",
                "DS_Invert": "DS Invert",
                "US_Obvert": "US Obvert",
                "DS_Obvert": "DS Obvert",
                "Slope": "pSlope",
                "Mannings_n": "n or Cd",
            }
            self.df.rename(columns=rename_map, inplace=True)

            # Clean 'Ent/Exit Losses' column if present
            if "Ent/Exit Losses" in self.df.columns:
                self.df["Ent/Exit Losses"] = pd.to_numeric(self.df["Ent/Exit Losses"], errors="coerce")

            self.apply_entity_filter()
            # Proceed with common processing steps
            self.add_common_columns()
            self.apply_output_transformations()

            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of EOF file: {self.log_path}")

        except Exception as e:
            logger.error(f"Failed to process EOF file {self.log_path}: {e}")
            self.df = pd.DataFrame()
            return
