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
        self._datatype_mapping = self._load_datatype_mapping()
        self._expected_headers = self._load_expected_headers()

    def _load_datatype_mapping(self) -> dict[str, str]:
        """
        Internal helper to load datatype mappings from DATA_TYPES_CONFIG.
        """
        if self.data_type is None:
            raise ValueError("data_type is not set.")

        data_type_def = data_types_config.data_types[self.data_type]
        if data_type_def is None:
            raise KeyError(
                f"Data type '{self.data_type}' is not defined in the config."
            )

        columns_mapping = data_type_def.columns
        logger.debug(f"{self.file_name}: Loaded columns mapping: {columns_mapping}")
        return columns_mapping

    def _load_expected_headers(self) -> list[str]:
        """
        Internal helper to load expected headers from DATA_TYPES_CONFIG.

        Returns:
            list[str]: A list of expected header names.
        """
        if self.data_type is None:
            raise ValueError("data_type is not set.")

        data_type_def = data_types_config.data_types[self.data_type]
        if data_type_def is None:
            raise KeyError(
                f"Data type '{self.data_type}' is not defined in the config."
            )

        expected_headers = data_type_def.expected_headers
        logger.debug(f"{self.file_name}: Loaded headers: {expected_headers}")
        return expected_headers

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

    @abstractmethod
    def process(self) -> pd.DataFrame:
        """
        Process the file and return a DataFrame.
        Must be implemented by subclasses.

        Returns:
            pd.DataFrame: Processed data.
        """
        pass


class ProcessorCollection:
    """
    A collection of BaseProcessor instances, allowing combined export.

    This class can be used to hold one or more processed BaseProcessor instances.
    It provides methods to combine their DataFrames and export the consolidated result.
    """

    def __init__(self) -> None:
        """
        Initialize an empty ProcessorCollection.
        """
        self.processors: list[BaseProcessor] = []

    def add_processor(self, processor: BaseProcessor) -> None:
        """
        Add a processed BaseProcessor instance to the collection.

        Args:
            processor (BaseProcessor): A processed BaseProcessor instance.
        """
        self.processors.append(processor)

    def export_to_csv(self, output_path: Path) -> None:
        """
        Export the combined DataFrame of all processors to a single CSV file.

        Args:
            output_path (Path): The path where the combined CSV should be saved.
        """
        if not self.processors:
            logger.warning("No processors to export.")
            return
        print("some processors")
        # Combine all DataFrames vertically
        combined_df = self.combine_data()
        combined_df.to_csv(output_path / "export.csv", index=False)
        logger.info(f"Exported combined data to {output_path}")

    def combine_data(self) -> pd.DataFrame:
        """
        Combine the DataFrames from all processors into a single DataFrame.

        Returns:
            pd.DataFrame: Combined DataFrame.
        """
        if not self.processors:
            return pd.DataFrame()

        combined_df = pd.concat([p.df for p in self.processors], ignore_index=True)
        return combined_df

    def get_processors_by_data_type(
        self, data_types: list[str] | str
    ) -> "ProcessorCollection":
        """
        Retrieve processors matching a specific data_type or list of data_types.

        Args:
            data_types (list[str] | str): A data_type or list of data_types to match.

        Returns:
            ProcessorCollection: A new collection of processors with matching data_type(s).
        """
        # Ensure it's always a list for uniform processing
        if isinstance(data_types, str):
            data_types = [data_types]

        filtered_collection = ProcessorCollection()
        for processor in self.processors:
            if processor.data_type in data_types:
                filtered_collection.add_processor(processor)

        return filtered_collection
