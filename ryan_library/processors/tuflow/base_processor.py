# ryan_library\processors\tuflow\base_processor.py
from _collections_abc import dict_keys
from dataclasses import dataclass, field
from pathlib import Path
from abc import ABC, abstractmethod
import pandas as pd
from loguru import logger
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.classes.suffixes_and_dtypes import data_types_config

print(data_types_config)
print(data_types_config.data_types)


@dataclass
class BaseProcessor(ABC):
    """
    Base class for processing different types of TUFLOW CSV and CCA files.
    """

    file_path: Path
    file_name: str = field(init=False)
    resolved_file_path: Path = field(init=False)
    name_parser: TuflowStringParser = field(init=False)
    data_type: str | None = field(init=False)
    _datatype_mapping: dict[str, str] = field(init=False, repr=False)
    _expected_headers: list[str] = field(init=False, repr=False)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    processed: bool = field(default=False)

    def __post_init__(self):
        self.file_name = self.file_path.name
        self.resolved_file_path = self.file_path.resolve()
        self.name_parser = TuflowStringParser(file_path=self.file_path)
        self.data_type = self.name_parser.data_type
        self._load_configuration()

    def _load_configuration(self) -> None:
        """
        Load configuration for expected headers and output columns from DataTypesConfig.
        """
        if self.data_type is None:
            raise ValueError("data_type is not set.")

        data_type_def = data_types_config.data_types.get(self.data_type)
        if data_type_def is None:
            raise KeyError(
                f"Data type '{self.data_type}' is not defined in the config."
            )

        self.expected_headers = data_type_def.expected_headers
        self.output_columns = data_type_def.columns

        logger.debug(
            f"{self.file_name}: Loaded expected_headers: {self.expected_headers}"
        )
        logger.debug(f"{self.file_name}: Loaded output_columns: {self.output_columns}")

    def add_common_columns(self) -> None:
        """
        Add all common columns by delegating to specific methods.
        """
        # print("try to add extras")
        self.add_basic_info_to_df()
        # print("basic info")
        self.run_code_parts_to_df()
        # print("run code parts")
        self.additional_attributes_to_df()
        print("additonal done")

    def apply_datatypes_to_df(self) -> None:
        """
        Apply datatype mapping to the DataFrame.
        """
        if not self._datatype_mapping:
            logger.warning(
                f"{self.file_name}: No datatype mapping for '{self.data_type}'."
            )
            return
        missing_columns = [
            col for col in self._datatype_mapping if col not in self.df.columns
        ]
        if missing_columns:
            logger.warning(
                f"{self.file_name}: Missing columns in DataFrame: {missing_columns}"
            )
        applicable_mapping = {
            col: dtype
            for col, dtype in self._datatype_mapping.items()
            if col in self.df.columns
        }
        try:
            self.df = self.df.astype(applicable_mapping)
            logger.debug(
                f"{self.file_name}: Applied datatype mapping: {applicable_mapping}"
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"{self.file_name}: Error applying datatypes: {e}")
            raise

    def add_basic_info_to_df(self) -> None:
        """
        Add basic information columns to the DataFrame.
        """
        data: dict[str, str] = {
            "internalName": self.name_parser.raw_run_code,
            "rel_path": str(self.resolved_file_path.relative_to(Path.cwd().resolve())),
            "path": str(self.resolved_file_path),
            "file": self.file_name,
        }
        print(data)
        self.df = self.df.assign(**data).astype({key: "string" for key in data})

        basic_info_columns = list(data.keys())
        print(basic_info_columns)
        self.df[basic_info_columns] = self.df[basic_info_columns].astype("category")

    def run_code_parts_to_df(self) -> None:
        """
        Extract and add R01, R02, etc., based on the run code.
        """
        # Assign run code parts with native types (assuming they are strings)
        print(1)
        run_code_keys = list(self.name_parser.run_code_parts.keys())
        print("Run code keys:", run_code_keys)

        for key, value in self.name_parser.run_code_parts.items():
            self.df[key] = value
        print(2)
        # Convert to categorical after assignment
        self.df[run_code_keys] = self.df[run_code_keys].astype("string")
        self.df[run_code_keys] = self.df[run_code_keys].astype("category")

    def additional_attributes_to_df(self) -> None:
        """
        Extract and add TP, Duration, and AEP using the parser.
        """
        # print(10)
        # print(self.__dict__)
        # print(self.name_parser.__dict__)

        # Build the attributes dictionary conditionally
        attributes = {
            "trim_runcode": self.name_parser.trim_run_code,
            **{
                f"{attr}_text": getattr(self.name_parser, attr).text_repr
                for attr in ["tp", "duration", "aep"]
                if getattr(self.name_parser, attr) is not None
            },
            **{
                f"{attr}_numeric": getattr(self.name_parser, attr).numeric_value
                for attr in ["tp", "duration", "aep"]
                if getattr(self.name_parser, attr) is not None
            },
        }

        print(attributes)

        # Assign attributes to DataFrame
        self.df = self.df.assign(**attributes)

        # Define the complete type mapping
        complete_type_mapping = {
            "trim_runcode": "string",
            "tp_text": "string",
            "duration_text": "string",
            "aep_text": "string",
            "tp_numeric": "Int32",
            "duration_numeric": "Int32",
            "aep_numeric": "float32",
        }

        # Create a dynamic dtype mapping based on present attributes
        dtype_mapping = {
            col: dtype
            for col, dtype in complete_type_mapping.items()
            if col in attributes
        }

        # Apply the dynamic dtype mapping
        if dtype_mapping:
            self.df = self.df.astype(dtype=dtype_mapping)

        # Convert the added columns to category type
        # Only include columns that were added in attributes
        category_columns = list(attributes.keys())
        # Ensure that category_columns are actually present in the DataFrame
        existing_category_columns = [
            col for col in category_columns if col in self.df.columns
        ]
        self.df[existing_category_columns] = self.df[existing_category_columns].astype(
            "category"
        )

    def validate_data(self) -> bool:
        """
        Validate the processed data.
        By default, just checks if DataFrame is non-empty, but can be overridden by subclasses.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        if self.df.empty:
            logger.warning(
                f"{self.file_name}: DataFrame is empty for file: {self.file_path}"
            )
            return False
        return True

    def check_headers_match(self, test_headers: list[str]) -> bool:
        logger.debug(f"Header row to test: {test_headers}")
        if test_headers != list(self.expected_headers.keys()):
            logger.error(
                f"Error reading {self.file_name}, headers did not match expected format {list(self.expected_headers.keys())}. Got {test_headers}"
            )
            return False
        logger.debug("Test headers matched expected headers")
        return True

    # logic here needs to be cleaned up. can we just use the info from the json file to determine the columns to load and have it error out if missing?
    def read_max_csv(self, usecols: list[int], dtype: dict) -> tuple[pd.DataFrame, int]:
        """
        Reads a CSV file using Pandas with specified columns and data types.
        Handles empty DataFrame and header mismatch.

        Args:
            usecols (list[int]): List of column indices to read.
            dtype (dict): Dictionary specifying data types for columns.

        Returns:
            tuple[pd.DataFrame, int]: A tuple containing the DataFrame and a status code.
                                      Status codes:
                                      0 - Success
                                      1 - Empty DataFrame
                                      2 - Header mismatch
                                      3 - Read error
        """
        usecols = list(self.expected_headers.keys())
        dtype = {col: self.expected_headers[col] for col in usecols}

        try:
            df: pd.DataFrame = pd.read_csv(
                filepath_or_buffer=self.file_path,
                usecols=usecols,
                header=0,
                dtype=dtype,
                skipinitialspace=True,
            )
            logger.debug(f"CSV file read successfully with {len(df)} rows.")
        except Exception as e:
            logger.error(f"Failed to read CSV file {self.file_path}: {e}")
            return pd.DataFrame(), 3  # 3 indicates read error

        if df.empty:
            logger.error(f"No data found in file: {self.file_path}")
            return df, 1

        # Validate headers using the existing method
        if not self.check_headers_match(df.columns.tolist()):
            return df, 2

        return df, 0  # 0 indicates success

    def apply_output_transformations(self) -> None:
        """
        Apply output column transformations:
        - Rename columns
        - Apply data types
        """
        if not self.output_columns:
            logger.warning(
                f"{self.file_name}: No output columns mapping for '{self.data_type}'."
            )
            return

        # Rename columns based on output_columns configuration
        rename_mapping = {
            original: specs["new_name"]
            for original, specs in self.output_columns.items()
            if original in self.df.columns
        }

        if rename_mapping:
            self.df.rename(columns=rename_mapping, inplace=True)
            logger.debug(f"{self.file_name}: Renamed columns: {rename_mapping}")

        # Apply data types based on output_columns configuration
        dtype_mapping = {
            specs["new_name"]: specs["dtype"]
            for original, specs in self.output_columns.items()
            if original in self.df.columns
        }

        # Check for missing columns
        missing_columns = [col for col in dtype_mapping if col not in self.df.columns]
        if missing_columns:
            logger.warning(
                f"{self.file_name}: Missing columns in DataFrame for dtype application: {missing_columns}"
            )

        # Apply data types
        applicable_mapping = {
            col: dtype for col, dtype in dtype_mapping.items() if col in self.df.columns
        }

        try:
            self.df = self.df.astype(applicable_mapping)
            logger.debug(
                f"{self.file_name}: Applied datatype mapping: {applicable_mapping}"
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"{self.file_name}: Error applying datatypes: {e}")
            raise

    @abstractmethod
    def process(self) -> pd.DataFrame:
        """
        Process the file and return a DataFrame.
        Must be implemented by subclasses.

        Returns:
            pd.DataFrame: Processed data.
        """
        pass
