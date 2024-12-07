import pandas as pd
import logging
from pathlib import Path
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.classes.suffixes import SUFFIX_DATA_TYPES
from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    """
    Base class for processing different types of TUFLOW CSV and CCA files.
    """

    def __init__(self, file_path: str):
        """
        Initialize the BaseProcessor with the given file path.

        Args:
            file_path (str): Path to the file to be processed.
        """
        self.file_path = Path(file_path)
        self.file_name = self.file_path.name
        self.name_parser = TuflowStringParser(self.file_path)
        self.data_type = self.name_parser.data_type
        self.datatype_mapping = self.load_datatype_mapping()
        self.df = pd.DataFrame()

    def load_datatype_mapping(self) -> dict[str, str]:
        """
        Load datatype mapping from SUFFIX_DATA_TYPES based on the data_type.

        Returns:
            dict[str, str]: Column to datatype mapping.
        """
        if not self.data_type:
            logging.error(f"No data type determined for file: {self.file_path}")
            return {}

        datatype = SUFFIX_DATA_TYPES.get(self.data_type, {})
        columns = datatype.get("columns", {})
        logging.debug(f"Loaded datatype mapping for '{self.data_type}': {columns}")
        return columns

    def add_common_columns(self):
        """
        Add all common columns by delegating to specific methods.
        """
        self.add_basic_info()
        self.apply_datatypes()
        self.extract_run_code_details()
        self.extract_additional_attributes()

    def add_basic_info(self):
        """
        Add basic information columns to the DataFrame.
        """
        self.df["internalName"] = self.name_parser.raw_run_code
        self.df["path"] = str(self.file_path.resolve())
        self.df["file"] = self.file_name

    def apply_datatypes(self):
        """
        Apply datatype mapping to the DataFrame.
        """
        if not self.datatype_mapping:
            logging.warning(
                f"No datatype mapping available for data type '{self.data_type}'"
            )
            return

        try:
            self.df = self.df.astype(self.datatype_mapping)
            logging.debug(f"Applied datatype mapping: {self.datatype_mapping}")
        except Exception as e:
            logging.error(f"Error applying datatype mapping: {e}")

    def extract_run_code_details(self):
        """
        Extract and add R01, R02, etc., based on the run code.
        """
        run_code_parts = self.name_parser.run_code_parts
        for key, value in run_code_parts.items():
            self.df[key] = value

    def extract_additional_attributes(self):
        """
        Extract TP, Duration, and AEP using the parser.
        """
        self.df["TP"] = self.name_parser.tp
        self.df["Duration"] = self.name_parser.duration
        self.df["AEP"] = self.name_parser.aep

    @abstractmethod
    def process(self) -> pd.DataFrame:
        """
        Process the file and return a DataFrame.
        Must be implemented by subclasses.

        Returns:
            pd.DataFrame: Processed data.
        """
        pass
