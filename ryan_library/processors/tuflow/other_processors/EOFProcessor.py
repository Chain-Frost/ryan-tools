# ryan_library/processors/tuflow/other_processors/EOFProcessor.py

from ..base_processor import BaseProcessor
import pandas as pd
from loguru import logger
import re


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

            missing_markers: set[str] = {"-----", "Adjusted", "---"}
            parsed_rows: list[list[object]] = []
            for line in data_lines[1:]:
                stripped_line = line.strip()
                if not stripped_line:
                    continue

                # EOF culvert rows are fixed-width-ish text, but the channel
                # label itself can contain a single space (for example
                # "MLETP 156.66"). Splitting only on runs of two or more spaces
                # keeps that printed label intact while still separating the
                # report columns.
                parts: list[str] = re.split(pattern=r"\s{2,}", string=stripped_line)
                if len(parts) < len(col_names):
                    logger.warning(f"{self.file_path.name}: Skipping malformed EOF culvert row: {stripped_line}")
                    continue
                if len(parts) > len(col_names):
                    # Keep a stable schema if the final text field contains
                    # unexpected spacing by folding the overflow into the last
                    # column rather than shifting earlier numeric fields.
                    parts = parts[: len(col_names) - 1] + [" ".join(parts[len(col_names) - 1 :])]

                parsed_rows.append([pd.NA if part in missing_markers else part for part in parts])

            if not parsed_rows:
                logger.warning(f"{self.file_path.name}: No parseable culvert rows found.")
                self.df = pd.DataFrame()
                return

            self.df = pd.DataFrame(data=parsed_rows, columns=col_names)
            numeric_columns: list[str] = [col for col in col_names if col not in {"Chan ID", "Type"}]
            for column in numeric_columns:
                self.df[column] = pd.to_numeric(self.df[column], errors="coerce")

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
