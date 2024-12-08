# ryan_library\processors\tuflow\base_processor.py
import pandas as pd
from loguru import logger
from pathlib import Path
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.classes.suffixes_and_dtypes import DATA_TYPES_CONFIG
from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    """
    Base class for processing different types of TUFLOW CSV and CCA files.
    """

    def __init__(self, file_path: Path) -> None:
        """
        Initialize the BaseProcessor with the given file path.

        Args:
            file_path (str): Path to the file to be processed.
        """
        self.file_path: Path = Path(file_path)
        self.file_name: str = self.file_path.name
        self.resolved_file_path: Path = self.file_path.resolve()

        self.name_parser = TuflowStringParser(file_path=self.file_path)
        self.data_type: str | None = self.name_parser.data_type

        self._datatype_mapping: dict[str, str] | None = None
        self._expected_headers: dict[str, str] | None = None

        self.df: pd.DataFrame = pd.DataFrame()
        self.processed: bool = False

    def log_message(self, level: str, message: str) -> None:
        """
        Helper method to log messages with file context.
        """
        getattr(logger, level)(f"{self.file_name}: {message}")

    def _load_mapping(
        self, attribute_name: str, validation_type: str
    ) -> dict[str, str]:
        """
        Generic lazy loader for datatype and header mappings.
        """
        if getattr(self, attribute_name) is None:
            setattr(self, attribute_name, self._load_datatype_mapping(validation_type))
        return getattr(self, attribute_name)

    @property
    def datatype_mapping(self) -> dict[str, str]:
        return self._load_mapping("_datatype_mapping", "columns")

    @property
    def expected_headers(self) -> dict[str, str]:
        return self._load_mapping("_expected_headers", "expected_headers")

    def _load_datatype_mapping(self, validation_type: str) -> dict[str, str]:
        """
        Internal helper to load datatype mappings from DATA_TYPES_CONFIG.
        """
        if not self.data_type:
            self.log_message("error", "No data type determined.")
            return {}

        mapping = DATA_TYPES_CONFIG.get(self.data_type, {}).get(validation_type, {})
        self.log_message("debug", f"Loaded {validation_type} mapping: {mapping}")
        return mapping

    def add_common_columns(self) -> None:
        """
        Add all common columns by delegating to specific methods.
        """
        self.add_basic_info_to_df()
        self.run_code_parts_to_df()
        self.additional_attributes_to_df()

    def apply_datatypes_to_df(self) -> None:
        """
        Apply datatype mapping to the DataFrame.
        """
        if not self.datatype_mapping:
            self.log_message("warning", f"No datatype mapping for '{self.data_type}'.")
            return
        # Validate that all columns in mapping exist in the DataFrame
        missing_columns = [
            col for col in self.datatype_mapping if col not in self.df.columns
        ]
        if missing_columns:
            self.log_message(
                "warning", f"Missing columns in DataFrame: {missing_columns}"
            )
        # Apply mapping only to existing columns
        applicable_mapping = {
            col: dtype
            for col, dtype in self.datatype_mapping.items()
            if col in self.df.columns
        }
        try:
            self.df = self.df.astype(applicable_mapping)
            self.log_message("debug", f"Applied datatype mapping: {applicable_mapping}")
        except (KeyError, ValueError, TypeError) as e:
            self.log_message("error", f"Error applying datatypes: {e}")
            raise

    def add_basic_info_to_df(self) -> None:
        """
        Add basic information columns to the DataFrame.
        """
        data = {
            "internalName": self.name_parser.raw_run_code,
            "rel_path": str(self.resolved_file_path.relative_to(Path.cwd().resolve())),
            "path": str(self.resolved_file_path),
            "file": self.file_name,
        }
        self.df = self.df.assign(**data).astype({key: "category" for key in data})

    def run_code_parts_to_df(self) -> None:
        """
        Extract and add R01, R02, etc., based on the run code.
        """
        self.df = self.df.assign(
            **{
                key: pd.Categorical(value)
                for key, value in self.name_parser.run_code_parts.items()
            }
        )

    def additional_attributes_to_df(self) -> None:
        """
        Extract and add TP, Duration, and AEP using the parser.
        """
        attributes = {
            "trim_runcode": self.name_parser.trim_runcode,
            **{
                f"{attr}_text": getattr(self.name_parser, attr).text_repr
                for attr in ["tp", "duration", "aep"]
                if getattr(self.name_parser, attr)
            },
            **{
                f"{attr}_numeric": getattr(self.name_parser, attr).numeric_value
                for attr in ["tp", "duration", "aep"]
                if getattr(self.name_parser, attr)
            },
        }
        # Assign attributes to DataFrame and set explicit datatypes
        self.df = self.df.assign(**attributes).astype(
            dtype={
                "trim_runcode": "category",
                "tp_text": "category",
                "duration_text": "category",
                "aep_text": "category",
                "tp_numeric": "int32",
                "duration_numeric": "int32",
                "aep_numeric": "float32",
            }
        )

    def validate_data(self) -> bool:
        """
        Validate the processed data.
        By default, just checks if DataFrame is non-empty, but can be overridden by subclasses.

        Returns:
            bool: True if data is valid, False otherwise.
        """
        if self.df.empty:
            logger.warning(f"DataFrame is empty for file: {self.file_path}")
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

    def export_to_csv(self, output_path: Path = Path.cwd()) -> None:
        """
        Export the combined DataFrame of all processors to a single CSV file.

        Args:
            output_path (Path): The path where the combined CSV should be saved.
        """
        if not self.processors:
            logger.warning("No processors to export.")
            return

        # Combine all DataFrames vertically
        combined_df = self.combine_data()
        combined_df.to_csv(output_path, index=False)
        logger.info(f"Exported combined data to {output_path}")

    def combine_data(self) -> "pd.DataFrame":
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
