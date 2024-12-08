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
        self.file_path = Path(file_path)
        self.file_name = self.file_path.name

        self.name_parser = TuflowStringParser(file_path=self.file_path)
        self.data_type = self.name_parser.data_type

        self._datatype_mapping = None
        self._expected_headers = None

        self.df: pd.DataFrame = pd.DataFrame()
        self.processed = False

    @property
    def datatype_mapping(self) -> dict[str, str]:
        """
        Lazily load the datatype mapping from DATA_TYPES_CONFIG based on the data_type.
        """
        if self._datatype_mapping is None:
            self._datatype_mapping = self._load_datatype_mapping("columns")
        return self._datatype_mapping

    @property
    def expected_headers(self) -> dict[str, str]:
        """
        Lazily load the expected headers mapping from DATA_TYPES_CONFIG based on the data_type.
        """
        if self._expected_headers is None:
            self._expected_headers = self._load_datatype_mapping("expected_headers")
        return self._expected_headers

    def _load_datatype_mapping(self, validation_type: str) -> dict[str, str]:
        """
        Internal helper to load datatype mappings from DATA_TYPES_CONFIG.
        """
        if not self.data_type:
            logger.error(f"No data type determined for file: {self.file_path}")
            return {}

        mapping = DATA_TYPES_CONFIG.get(self.data_type, {}).get(validation_type, {})
        logger.debug(
            f"Loaded {validation_type} mapping for '{self.data_type}': {mapping}"
        )
        return mapping

    def add_common_columns(self):
        """
        Add all common columns by delegating to specific methods.
        """
        self.add_basic_info_to_df()
        self.run_code_parts_to_df()
        self.additional_attributes_to_df()

    def apply_datatypes_to_df(self):
        """
        Apply datatype mapping to the DataFrame.
        """
        if not self.datatype_mapping:
            logger.warning(
                f"No datatype mapping available for data type '{self.data_type}'"
            )
            return
        # Validate that all columns in mapping exist in the DataFrame
        missing_columns = [
            col for col in self.datatype_mapping if col not in self.df.columns
        ]
        if missing_columns:
            logger.warning(
                f"Datatype mapping contains columns not in DataFrame: {missing_columns}"
            )
        # Apply mapping only to existing columns
        applicable_mapping = {
            col: dtype
            for col, dtype in self.datatype_mapping.items()
            if col in self.df.columns
        }
        try:
            self.df = self.df.astype(applicable_mapping)
            logger.debug(f"Applied datatype mapping: {applicable_mapping}")
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error applying datatype mapping to DataFrame: {e}")
            raise

    def add_basic_info_to_df(self):
        """
        Add basic information columns to the DataFrame.
        """
        data = {
            "internalName": self.name_parser.raw_run_code,
            "rel_path": str(self.file_path.resolve().relative_to(Path.cwd().resolve())),
            "path": str(self.file_path.resolve()),
            "file": self.file_name,
        }

        # Add columns and convert to category in one step
        self.df = self.df.assign(**data).astype({key: "category" for key in data})

    def run_code_parts_to_df(self) -> None:
        """
        Extract and add R01, R02, etc., based on the run code.
        """
        for key, value in self.name_parser.run_code_parts.items():
            self.df[key] = pd.Categorical(value)

    def additional_attributes_to_df(self) -> None:
        """
        Extract and add TP, Duration, and AEP using the parser.
        """
        attributes = {
            "trim_runcode": self.name_parser.trim_runcode,
            "TP": self.name_parser.tp.numeric_value if self.name_parser.tp else None,
            "Duration_mins": (
                self.name_parser.duration.numeric_value
                if self.name_parser.duration
                else None
            ),
            "AEP_pct": (
                self.name_parser.aep.numeric_value if self.name_parser.aep else None
            ),
            "TP_text": self.name_parser.tp.text_repr if self.name_parser.tp else None,
            "Duration_text": (
                self.name_parser.duration.text_repr
                if self.name_parser.duration
                else None
            ),
            "AEP_1inX": (
                self.name_parser.aep.text_repr if self.name_parser.aep else None
            ),
            # AEP1inX not implemented yet
        }

        for key, value in attributes.items():
            self.df[key] = value

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

    def export_to_csv(self, output_path: Path) -> None:
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
        import pandas as pd

        if not self.processors:
            return pd.DataFrame()

        # Concatenate all DataFrames
        combined_df = pd.concat([p.df for p in self.processors], ignore_index=True)
        return combined_df
