# ryan_library/processors/tuflow/other_processors/EofProcessor.py

from ..base_processor import BaseProcessor
import pandas as pd
from loguru import logger
import numpy as np
import io

class EofProcessor(BaseProcessor):
    """Processor for '.eof' files."""

    def process(self) -> None:
        """Process the '.eof' file and extract CULVERT AND PIPE DATA."""
        logger.info(f"Starting processing of EOF file: {self.file_path}")

        try:
            with open(self.file_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()

            # Find the start of the table
            start_index = -1
            for i, line in enumerate(lines):
                if "CULVERT AND PIPE DATA" in line:
                    start_index = i
                    break
            
            if start_index == -1:
                logger.warning(f"CULVERT AND PIPE DATA section not found in {self.file_path}")
                self.df = pd.DataFrame()
                return

            # Skip to the header line starting with "Channel"
            header_index = -1
            for i in range(start_index, len(lines)):
                if lines[i].strip().startswith("Channel"):
                    header_index = i
                    break
            
            if header_index == -1:
                logger.warning(f"Header line not found in CULVERT AND PIPE DATA section in {self.file_path}")
                self.df = pd.DataFrame()
                return

            # Collect data lines
            data_lines = []
            # Start reading from the line after the header
            for i in range(header_index + 1, len(lines)):
                line = lines[i] # Keep original line with spaces for fwf
                stripped = line.strip()
                if not stripped:
                    continue # Skip empty lines
                
                # Stop if we hit another section or end of data
                if "VELOCITIES" in stripped or stripped.startswith("-"):
                    break
                
                data_lines.append(line)

            if not data_lines:
                logger.warning(f"No data extracted from CULVERT AND PIPE DATA in {self.file_path}")
                self.df = pd.DataFrame()
                return

            # Use read_fwf to parse the data lines
            # We define column names manually as the header is complex
            # Based on the file structure:
            # Channel, Type, No, U/S, D/S, U/S, D/S, Length, Slope, n, Diameter, Height, Inlet H, Inlet W, Ent, Exit, Fixed, Losses
            col_names = [
                "Chan ID", "Type", "Num_barrels", 
                "US_Invert", "DS_Invert", "US_Obvert", "DS_Obvert", 
                "Length", "Slope", "Mannings_n", "Diam_Width", "Height", 
                "Inlet_Height", "Inlet_Width", 
                "Entry Loss", "Exit Loss", "Fixed Loss", "Ent/Exit Losses"
            ]
            
            # Create a string buffer
            data_str = "".join(data_lines)
            
            # Parse with read_fwf
            # infer_nrows should help determine widths from the data
            self.df = pd.read_fwf(
                io.StringIO(data_str), 
                names=col_names,
                header=None,
                infer_nrows=100
            )
            
            # Handle '-----' in Height column
            if "Height" in self.df.columns:
                self.df["Height"] = pd.to_numeric(self.df["Height"], errors='coerce')

            # Rename columns to match ChanProcessor conventions
            rename_map = {
                "US_Invert": "US Invert",
                "DS_Invert": "DS Invert",
                "US_Obvert": "US Obvert",
                "DS_Obvert": "DS Obvert",
                "Slope": "pSlope",
                "Mannings_n": "n or Cd"
            }
            self.df.rename(columns=rename_map, inplace=True)

            # Proceed with common processing steps
            self.add_common_columns()
            self.apply_output_transformations()
            
            # Validate data
            if not self.validate_data():
                logger.error(f"{self.file_name}: Data validation failed.")
                self.df = pd.DataFrame()
                return

            self.processed = True
            logger.info(f"Completed processing of EOF file: {self.file_path}")

        except Exception as e:
            logger.error(f"Failed to process EOF file {self.file_path}: {e}")
            self.df = pd.DataFrame()
            return
