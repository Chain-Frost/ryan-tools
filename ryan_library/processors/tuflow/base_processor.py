# ryan_library/processors/tuflow/base_processor.py

from dataclasses import dataclass, field
from pathlib import Path
from abc import ABC, abstractmethod
import pandas as pd
from loguru import logger
from pandas import DataFrame
from typing import Any
import importlib
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.classes.suffixes_and_dtypes import (
    DataTypeDefinition,
    ProcessingParts,
    data_types_config,
    suffixes_config,
)
from ryan_library.functions.dataframe_helpers import reorder_long_columns

# the processors are imported as required within the class (importlib)


@dataclass
class BaseProcessor(ABC):
    """
    Base class for processing different types of TUFLOW CSV and CCA files.
    """

    file_path: Path
    file_name: str = field(init=False)
    resolved_file_path: Path = field(init=False)
    name_parser: TuflowStringParser = field(init=False)
    data_type: str = field(init=False)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    processed: bool = field(default=False)

    # Attributes to hold configuration
    output_columns: dict[str, str] = field(init=False, default_factory=dict)
    dataformat: str = field(init=False, default="")
    skip_columns: list[int] = field(init=False, default_factory=list)
    columns_to_use: dict[str, str] = field(init=False, default_factory=dict)
    expected_in_header: list[str] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        self.file_name = self.file_path.name
        self.resolved_file_path = self.file_path.resolve()
        self.name_parser = TuflowStringParser(file_path=self.file_path)
        if self.name_parser.data_type is None:
            raise ValueError(
                "data_type was not set in TuflowStringParser for self.file_path.name"
            )
        self.data_type = self.name_parser.data_type
        logger.debug(f"{self.file_name}: Data type identified as '{self.data_type}'")
        self._load_configuration()

    @classmethod
    def from_file(cls, file_path: Path) -> "BaseProcessor":
        """
        Factory method to create the appropriate processor instance based on the file suffix.

        Args:
            file_path (Path): Path to the file to process.

        Returns:
            BaseProcessor: Instance of a subclass of BaseProcessor.

        Raises:
            ValueError: If data_type cannot be determined.
            KeyError: If processor class is not defined for the data_type.
            ImportError: If the processor module cannot be imported.
            AttributeError: If the processor class does not exist in the module.
        """
        file_name = file_path.name  # Preserve original capitalization
        logger.debug(f"Attempting to process file: {file_path}")

        # Determine data type based on suffix using suffixes_config
        data_type = cls.get_data_type_for_file(file_name)
        if not data_type:
            error_msg = f"No data type found for file: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Get processor class name based on data type
        processor_class_name = suffixes_config.get_processor_class_for_data_type(
            data_type
        )
        if not processor_class_name:
            error_msg = f"No processor class specified for data type '{data_type}'."
            logger.error(error_msg)
            raise KeyError(error_msg)

        # Dynamically import the processor class
        processor_cls: type[BaseProcessor] = cls.get_processor_class(
            class_name=processor_class_name
        )

        # Instantiate the processor class
        try:
            processor = processor_cls(file_path=file_path)
            return processor
        except Exception as e:
            logger.exception(
                f"Error instantiating processor '{processor_class_name}' for file {file_path}: {e}"
            )
            raise

    @staticmethod
    def get_processor_class(class_name: str) -> type["BaseProcessor"]:
        """
        Dynamically import and return a processor class by name.

        Args:
            class_name (str): Name of the processor class.

        Returns:
            Type[BaseProcessor]: The processor class.

        Raises:
            ImportError: If the module cannot be imported.
            AttributeError: If the class does not exist in the module.
        """
        module_path = f"ryan_library.processors.tuflow.{class_name}"
        try:
            module = importlib.import_module(module_path)
            processor_cls = getattr(module, class_name)
            return processor_cls
        except ImportError as ie:
            logger.error(f"Processor module '{module_path}' not found: {ie}")
            raise
        except AttributeError as ae:
            logger.error(
                f"Processor class '{class_name}' not found in '{module_path}': {ae}"
            )
            raise

    @staticmethod
    def get_data_type_for_file(file_name: str) -> str | None:
        """
        Determine the data type based on the file's suffix.

        Args:
            file_name (str): Name of the file.

        Returns:
            str | None: The corresponding data type if found, else None.
        """
        # I think this is redundant as we use tuflow string parser to do the same thing. remove after we get it working
        for suffix, data_type in suffixes_config.suffix_to_type.items():
            if file_name.endswith(suffix):
                logger.debug(
                    f"File '{file_name}' matches suffix '{suffix}' with data type '{data_type}'"
                )
                return data_type
        return None

    def _load_configuration(self) -> None:
        """
        Load configuration for expected headers and output columns from the config.
        """
        if not self.data_type:
            raise ValueError("data_type was not set in TuflowStringParser.")

        data_type_def: DataTypeDefinition | None = data_types_config.data_types.get(
            self.data_type
        )
        if data_type_def is None:
            raise KeyError(
                f"Data type '{self.data_type}' is not defined in the config."
            )

        # Load output_columns
        self.output_columns = data_type_def.output_columns
        logger.debug(f"{self.file_name}: Loaded output_columns: {self.output_columns}")

        # Load processingParts
        processing_parts: ProcessingParts = data_type_def.processing_parts
        self.dataformat = processing_parts.dataformat
        self.skip_columns = processing_parts.skip_columns
        logger.debug(
            f"{self.file_name}: Loaded processingParts - dataformat: {self.dataformat}, skip_columns: {self.skip_columns}"
        )

        # Depending on dataformat, load columns_to_use or expected_in_header
        if self.dataformat in ["Maximums", "ccA"]:
            self.columns_to_use = processing_parts.columns_to_use
            logger.debug(
                f"{self.file_name}: Loaded columns_to_use: {self.columns_to_use}"
            )
        elif self.dataformat == "Timeseries":
            self.expected_in_header = processing_parts.expected_in_header
            logger.debug(
                f"{self.file_name}: Loaded expected_in_header: {self.expected_in_header}"
            )
        else:
            logger.warning(f"{self.file_name}: Unknown dataformat '{self.dataformat}'.")

    def add_common_columns(self) -> None:
        """
        Add all common columns by delegating to specific methods.
        """
        self.add_basic_info_to_df()
        self.run_code_parts_to_df()
        self.additional_attributes_to_df()
        self.reorder_long_text_columns()

    def reorder_long_text_columns(self) -> None:
        # move large width column names to the right side
        self.df = reorder_long_columns(
            df=self.df,
        )

    def apply_output_transformations(self) -> None:
        """
        Apply output column transformations:
        - Checks if DataFrame is empty or if no output_columns are defined.
        - Applies data types as specified in output_columns.
        """
        if self.df.empty:
            logger.warning(
                f"{self.file_name}: DataFrame is empty, skipping datatype transformations."
            )
            return

        if not self.output_columns:
            logger.warning(
                f"{self.file_name}: No output_columns mapping for '{self.data_type}'."
            )
            return
        missing_columns: list[str] = [
            col for col in self.output_columns if col not in self.df.columns
        ]
        if missing_columns:
            logger.warning(
                f"{self.file_name}: Missing columns in DataFrame: {missing_columns}"
            )
        # Apply data types based on output_columns configuration
        try:
            self.df = self.df.astype(self.output_columns)
            logger.debug(
                f"{self.file_name}: Applied output_columns datatype mapping: {self.output_columns}"
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.error(
                f"{self.file_name}: Error applying output_columns datatypes: {e}"
            )
            raise

    def add_basic_info_to_df(self) -> None:
        """
        Add basic information columns to the DataFrame.
        """
        resolved_path = self.resolved_file_path
        cwd_resolved = Path.cwd().resolve()
        parent_path = resolved_path.parent
        data: dict[str, str] = {
            "internalName": self.name_parser.raw_run_code,
            "rel_path": str(resolved_path.relative_to(cwd_resolved)),
            "path": str(resolved_path),
            "directory_path": str(parent_path),
            "rel_directory": str(parent_path.relative_to(cwd_resolved)),
            "file": self.file_name,
        }
        logger.debug(f"{self.file_name}: Adding basic info columns: {data}")
        # Assign basic info columns as strings
        self.df = self.df.assign(**data).astype({key: "string" for key in data})
        # Convert basic info columns to ordered categorical
        basic_info_columns = list(data.keys())
        self.order_categorical_columns(self.df, basic_info_columns)
        logger.debug(
            f"{self.file_name}: Basic info columns added and converted to ordered categorical."
        )

    def run_code_parts_to_df(self) -> None:
        """
        Extract and add R01, R02, etc., based on the run code.
        """
        run_code_keys = list(self.name_parser.run_code_parts.keys())
        logger.debug(f"{self.file_name}: Run code keys: {run_code_keys}")
        for key, value in self.name_parser.run_code_parts.items():
            self.df[key] = value
        # Convert run code parts to ordered categorical
        self.order_categorical_columns(self.df, run_code_keys)
        logger.debug(
            f"{self.file_name}: Run code parts added and converted to ordered categorical."
        )

    def additional_attributes_to_df(self) -> None:
        """
        Extract and add TP, Duration, and AEP using the parser.
        """
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
        logger.debug(f"{self.file_name}: Additional attributes to add: {attributes}")

        # Assign attributes to DataFrame
        self.df = self.df.assign(**attributes)

        # Define the complete type mapping
        complete_type_mapping: dict[str, str] = {
            "trim_runcode": "string",
            "tp_text": "string",
            "duration_text": "string",
            "aep_text": "string",
            "tp_numeric": "Int32",
            "duration_numeric": "Int32",
            "aep_numeric": "float32",
        }

        # Create a dynamic dtype mapping based on present attributes
        dtype_mapping: dict[str, str] = {
            col: dtype
            for col, dtype in complete_type_mapping.items()
            if col in attributes
        }

        # Apply data types
        if dtype_mapping:
            try:
                self.df = self.df.astype(dtype=dtype_mapping)
                logger.debug(
                    f"{self.file_name}: Applied additional attributes datatype mapping: {dtype_mapping}"
                )
            except (KeyError, ValueError, TypeError) as e:
                logger.error(
                    f"{self.file_name}: Error applying additional attributes datatypes: {e}"
                )
                raise

        # Convert the added columns to ordered categorical
        category_columns = list(attributes.keys())
        existing_category_columns = [
            col for col in category_columns if col in self.df.columns
        ]
        self.order_categorical_columns(self.df, existing_category_columns)
        logger.debug(
            f"{self.file_name}: Additional attributes converted to ordered categorical."
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
        """
        Check if the CSV headers match the expected headers.

        Args:
            test_headers (list[str]): The headers from the CSV file.

        Returns:
            bool: True if headers match, False otherwise.
        """
        if self.columns_to_use:
            expected = list(self.columns_to_use.keys())
            if test_headers != expected:
                logger.error(
                    f"Error reading {self.file_name}, headers did not match expected format {expected}. Got {test_headers}"
                )
                return False
            logger.debug("Test headers matched expected columns_to_use.")
            return True
        elif self.expected_in_header:
            if test_headers != self.expected_in_header:
                logger.error(
                    f"Error reading {self.file_name}, headers did not match expected_in_header format {self.expected_in_header}. Got {test_headers}"
                )
                return False
            logger.debug("Test headers matched expected_in_header.")
            return True
        else:
            logger.warning(f"{self.file_name}: No headers to validate against.")
            return True

    def read_maximums_csv(self) -> int:
        """
        Reads CSV files with 'Maximums' or 'ccA' dataformat.

        Returns:
             int: status code.
        """

        usecols = list(self.columns_to_use.keys())
        dtype = {col: self.columns_to_use[col] for col in usecols}

        try:
            df: DataFrame = pd.read_csv(
                filepath_or_buffer=self.file_path,
                usecols=usecols,
                header=0,
                dtype=dtype,
                skipinitialspace=True,
            )
            logger.debug(
                f"CSV file '{self.file_name}' read successfully with {len(df)} rows."
            )
        except Exception as e:
            logger.exception(
                f"{self.file_name}: Failed to read CSV file '{self.file_path}': {e}"
            )
            return 3

        if df.empty:
            logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
            return 1

        # Validate headers
        if not self.check_headers_match(df.columns.tolist()):
            return 2

        self.df = df
        return 0

    def read_timeseries_csv(self) -> int:
        """
        Reads CSV files with 'Timeseries' dataformat.

        Returns:
            int: DataFrame and status code.
                Status Codes:
                    0 - Success
                    1 - Empty DataFrame
                    2 - Header mismatch
                    3 - Read error
        """
        usecols = self.expected_in_header

        try:
            df = pd.read_csv(
                filepath_or_buffer=self.file_path,
                usecols=usecols,
                header=0,
                skipinitialspace=True,
            )
            logger.debug(
                f"Timeseries CSV file '{self.file_name}' read successfully with {len(df)} rows."
            )
        except Exception as e:
            logger.error(
                f"{self.file_name}: Failed to read Timeseries CSV file '{self.file_path}': {e}"
            )
            return 3

        if df.empty:
            logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
            return 1

        # Validate headers
        if not self.check_headers_match(df.columns.tolist()):
            return 2

        self.df = df
        return 0

    def read_ccA_data(self) -> tuple[pd.DataFrame, int]:
        """
        Reads ccA files with 'ccA' data format.

        Returns:
            tuple[pd.DataFrame, int]: DataFrame and status code.
                Status Codes:
                    0 - Success
                    1 - Empty DataFrame
                    2 - Header mismatch
                    3 - Read error
        """
        # 'ccA' cannot be processed yet; raise NotImplementedError
        raise NotImplementedError(
            "Processing of ccA data format is not yet implemented."
        )

    @abstractmethod
    def process(self) -> pd.DataFrame:
        """
        Process the file and return a DataFrame.
        Must be implemented by subclasses.

        Returns:
            pd.DataFrame: Processed data.
        """
        pass

    def separate_metrics(
        self, metric_columns: dict[str, str], new_columns: dict[str, Any]
    ) -> DataFrame:
        """
        Separate metrics into distinct rows based on provided column mappings.

        Args:
            metric_columns (dict[str, str]): Mapping of original columns to new columns.
            new_columns (dict[str, Any]): Additional columns to add with default values.

        Returns:
            DataFrame: Reshaped DataFrame.
        """
        reshaped_dfs = []
        for original, new in metric_columns.items():
            temp_df = self.df[[original]].copy()
            temp_df.rename(columns={original: new}, inplace=True)
            for col, val in new_columns.items():
                temp_df[col] = val
            reshaped_dfs.append(temp_df)

        return pd.concat(reshaped_dfs, ignore_index=True)

    def order_categorical_columns(self, df: pd.DataFrame, columns: list[str]) -> None:
        """
        Convert specified columns to ordered categorical types with categories sorted alphabetically.

        Args:
            df (pd.DataFrame): The DataFrame containing the columns.
            columns (list[str]): List of column names to convert.
        """
        for col in columns:
            if df[col].dtype.name == "category" and not df[col].cat.ordered:
                sorted_categories = sorted(df[col].cat.categories)
                df[col] = df[col].cat.set_categories(sorted_categories, ordered=True)
                logger.debug(f"Column '{col}' ordered alphabetically.")
