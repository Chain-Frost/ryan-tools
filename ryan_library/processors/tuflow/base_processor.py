# ryan_library/processors/tuflow/base_processor.py

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, cast
import importlib
import pandas as pd
from loguru import logger
from ryan_library.classes.suffixes_and_dtypes import (
    Config,
    DataTypeDefinition,
    ProcessingParts,
    SuffixesConfig,
)
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.dataframe_helpers import reorder_long_columns


# Custom Exceptions
class ProcessorError(Exception):
    """Base exception for processor errors."""


class ConfigurationError(ProcessorError):
    """Exception raised for configuration-related errors."""


class ImportProcessorError(ProcessorError):
    """Exception raised when importing a processor class fails."""


class DataValidationError(ProcessorError):
    """Exception raised for data validation errors."""


# the processors are imported as required within the class (importlib)


@dataclass
class BaseProcessor(ABC):
    """Base class for processing different types of TUFLOW CSV and CCA files."""

    file_path: Path
    file_name: str = field(init=False)
    resolved_file_path: Path = field(init=False)
    name_parser: TuflowStringParser = field(init=False)
    data_type: str = field(init=False)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    raw_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    processed: bool = field(default=False)

    # Define _processor_cache as a ClassVar to make it a class variable
    _processor_cache: ClassVar[dict[str, type["BaseProcessor"]]] = {}

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
            raise ValueError("data_type was not set in TuflowStringParser for self.file_path.name")
        self.data_type = self.name_parser.data_type
        logger.debug(f"{self.file_name}: Data type identified as '{self.data_type}'")
        self._load_configuration()

    @classmethod
    def from_file(cls, file_path: Path) -> "BaseProcessor":
        """Factory method to create the appropriate processor instance based on the file suffix."""
        file_name: str = file_path.name
        logger.debug(f"Attempting to process file: {file_path}")

        # Use TuflowStringParser to determine data_type
        name_parser = TuflowStringParser(file_path=file_path)
        data_type: str | None = name_parser.data_type

        if not data_type:
            error_msg = f"No data type found for file: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Get processor class name based on data type
        processor_class_name: str | None = SuffixesConfig.get_instance().get_processor_class_for_data_type(
            data_type=data_type
        )
        if not processor_class_name:
            error_msg = f"No processor class specified for data type '{data_type}'."
            logger.error(error_msg)
            raise KeyError(error_msg)

        # Dynamically import the processor class
        processor_cls: type[BaseProcessor] = cls.get_processor_class(class_name=processor_class_name)

        # Instantiate the processor class
        try:
            processor = processor_cls(file_path=file_path)
            return processor
        except Exception as e:
            logger.exception(f"Error instantiating processor '{processor_class_name}' for file {file_path}: {e}")
            raise

    @staticmethod
    def get_processor_class(class_name: str) -> type["BaseProcessor"]:
        """Dynamically import and return a processor class by name with caching."""
        if class_name in BaseProcessor._processor_cache:
            logger.debug(f"Using cached processor class '{class_name}'.")
            return BaseProcessor._processor_cache[class_name]

        module_path: str = f"ryan_library.processors.tuflow.{class_name}"
        try:
            module = importlib.import_module(module_path)
            processor_cls = cast(type["BaseProcessor"], getattr(module, class_name))
            BaseProcessor._processor_cache[class_name] = processor_cls
            logger.debug(f"Imported processor class '{class_name}' from '{module_path}'.")
            return processor_cls
        except ImportError as ie:
            msg: str = f"Processor module '{module_path}' not found: {ie}"
            logger.exception(msg)
            raise ImportProcessorError(msg) from ie
        except AttributeError as ae:
            msg = f"Processor class '{class_name}' not found in '{module_path}': {ae}"
            logger.exception(msg)
            raise ImportProcessorError(msg) from ae

    def _load_configuration(self) -> None:
        """Load configuration for expected headers and output columns from the config."""
        if not self.data_type:
            raise ValueError("data_type was not set in TuflowStringParser.")

        data_type_def: DataTypeDefinition | None = Config.get_instance().data_types.get(self.data_type)
        if data_type_def is None:
            raise KeyError(f"Data type '{self.data_type}' is not defined in the config.")

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
            logger.debug(f"{self.file_name}: Loaded columns_to_use: {self.columns_to_use}")
        elif self.dataformat == "Timeseries":
            self.expected_in_header = processing_parts.expected_in_header
            logger.debug(f"{self.file_name}: Loaded expected_in_header: {self.expected_in_header}")
        else:
            logger.warning(f"{self.file_name}: Unknown dataformat '{self.dataformat}'.")

    @abstractmethod
    def process(self) -> None:
        """Process the file and modify the instance's DataFrame (`self.df`) in place.
        Must be implemented by subclasses.
        Returns None. The processed dataframe is a property of the class."""

        pass

    def reorder_long_text_columns(self) -> None:
        """Move large width column names to the right side."""
        self.df = reorder_long_columns(df=self.df)

    def add_common_columns(self) -> None:
        """Add all common columns by delegating to specific methods."""
        self.add_basic_info_to_df()
        self.run_code_parts_to_df()
        self.additional_attributes_to_df()
        self.reorder_long_text_columns()

    def add_basic_info_to_df(self) -> None:
        """Add basic information columns to the DataFrame."""
        resolved_path: Path = self.resolved_file_path
        cwd_resolved: Path = Path.cwd().resolve()
        parent_path: Path = resolved_path.parent
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
        # Convert columns to 'category' dtype before ordering
        basic_info_columns = list(data.keys())
        self.df[basic_info_columns] = self.df[basic_info_columns].astype(dtype="category")
        self.order_categorical_columns(df=self.df, columns=basic_info_columns)
        logger.debug(f"{self.file_name}: Basic info columns added and converted to ordered categorical.")

    def run_code_parts_to_df(self) -> None:
        """Extract and add R01, R02, etc., based on the run code."""
        run_code_keys = list(self.name_parser.run_code_parts.keys())
        logger.debug(f"{self.file_name}: Run code keys: {run_code_keys}")
        for key, value in self.name_parser.run_code_parts.items():
            self.df[key] = value
        # Convert run code parts to 'category' dtype before ordering
        self.df[run_code_keys] = self.df[run_code_keys].astype(dtype="category")
        self.order_categorical_columns(df=self.df, columns=run_code_keys)
        logger.debug(f"{self.file_name}: Run code parts added and converted to ordered categorical.")

    def additional_attributes_to_df(self) -> None:
        """Extract and add TP, Duration, and AEP using the parser."""
        # Build the attributes dictionary conditionally
        attributes: dict[str, str | Any] = {
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
            col: dtype for col, dtype in complete_type_mapping.items() if col in attributes
        }

        # Apply data types
        if dtype_mapping:
            try:
                self.df = self.df.astype(dtype=dtype_mapping)
                logger.debug(f"{self.file_name}: Applied additional attributes datatype mapping: {dtype_mapping}")
            except (KeyError, ValueError, TypeError) as e:
                logger.error(f"{self.file_name}: Error applying additional attributes datatypes: {e}")
                raise

        # Convert the added columns to 'category' dtype before ordering
        category_columns = list(attributes.keys())
        existing_category_columns: list[str] = [col for col in category_columns if col in self.df.columns]
        self.df[existing_category_columns] = self.df[existing_category_columns].astype(dtype="category")
        self.order_categorical_columns(df=self.df, columns=existing_category_columns)
        logger.debug(f"{self.file_name}: Additional attributes converted to ordered categorical.")

    def apply_output_transformations(self) -> None:
        """Apply output column transformations:
        - Checks if DataFrame is empty or if no output_columns are defined.
        - Applies data types as specified in output_columns."""
        if self.df.empty:
            logger.warning(f"{self.file_name}: DataFrame is empty, skipping datatype transformations.")
            return

        if not self.output_columns:
            logger.warning(f"{self.file_name}: No output_columns mapping for '{self.data_type}'.")
            return
        missing_columns: list[str] = [col for col in self.output_columns if col not in self.df.columns]
        if missing_columns:
            logger.warning(f"{self.file_name}: Missing columns in DataFrame: {missing_columns}")
        # Apply data types based on output_columns configuration
        try:
            self.df = self.df.astype(self.output_columns)
            logger.debug(f"{self.file_name}: Applied output_columns datatype mapping: {self.output_columns}")
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"{self.file_name}: Error applying output_columns datatypes: {e}")
            raise

    def validate_data(self) -> bool:
        """Validate the processed data.
        By default, just checks if DataFrame is non-empty, but can be overridden by subclasses.

        Returns:
            bool: True if data is valid, False otherwise."""
        if self.df.empty:
            logger.warning(f"{self.file_name}: DataFrame is empty for file: {self.file_path}")
            return False
        return True

    def check_headers_match(self, test_headers: list[str]) -> bool:
        """Check if the CSV headers match the expected headers.
        Args:
            test_headers (list[str]): The headers from the CSV file.
        Returns:
            bool: True if headers match, False otherwise."""
        if self.columns_to_use:
            expected = list(self.columns_to_use.keys())
            got_set = set(test_headers)
            expected_set = set(expected)
            # First—ensure nothing is missing or extra:
            if got_set != expected_set:
                missing: set[str] = expected_set - got_set
                extra: set[str] = got_set - expected_set
                logger.error(
                    f"Error reading {self.file_name}, header mismatch. "
                    f"Missing columns: {sorted(missing)}; Extra columns: {sorted(extra)}."
                )
                return False
            # If the sets match but the order is different:
            if test_headers != expected:
                logger.warning(
                    f"{self.file_name}: column order differs. "
                    f"Expected {expected}, got {test_headers}. Proceeding by reordering."
                )
            else:
                logger.debug("Test headers matched expected columns_to_use in order.")
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
        """Reads CSV files with 'Maximums' or 'ccA' dataformat.

        Returns:
             int: status code."""
        usecols = list(self.columns_to_use.keys())
        dtype: dict[str, str] = {col: self.columns_to_use[col] for col in usecols}

        try:
            df: pd.DataFrame = pd.read_csv(
                filepath_or_buffer=self.file_path,
                usecols=usecols,
                header=0,
                dtype=dtype,
                skipinitialspace=True,
            )
            logger.debug(f"CSV file '{self.file_name}' read successfully with {len(df)} rows.")
        except Exception as e:
            logger.exception(f"{self.file_name}: Failed to read CSV file '{self.file_path}': {e}")
            return 3

        if df.empty:
            logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
            return 1

        # Validate headers
        if not self.check_headers_match(df.columns.tolist()):
            return 2

        self.df = df
        return 0

    def read_and_process_timeseries_csv(self, data_type: str) -> int:
        """Reads and processes timeseries CSV files, including cleaning headers and reshaping data.

        Args:
            data_type (str): The data type identifier (e.g., 'H', 'Q').

        Returns:
            int: Status code.
                0 - Success
                1 - Empty DataFrame
                2 - Header mismatch
                3 - Read or processing error"""
        try:
            df_full: pd.DataFrame = self._read_csv(file_path=self.file_path)
            if df_full.empty:
                logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
                return 1

            df: pd.DataFrame = self._clean_headers(df=df_full, data_type=data_type)
            if df.empty:
                logger.error(f"{self.file_name}: DataFrame is empty after cleaning headers.")
                return 1

            df_melted: pd.DataFrame = self._reshape_timeseries_df(df=df, data_type=data_type)
            if df_melted.empty:
                logger.error(f"{self.file_name}: No data found after reshaping.")
                return 1

            self.df = df_melted
            self._apply_final_transformations(data_type=data_type)
            self.processed = True  # Mark as processed
            logger.info(f"{self.file_name}: Timeseries CSV processed successfully.")
            return 0
        except (ProcessorError, DataValidationError) as e:
            logger.error(f"{self.file_name}: Processing error: {e}")
            return 3
        except Exception as e:
            logger.exception(f"{self.file_name}: Unexpected error: {e}")
            return 3

    def _read_csv(self, file_path: Path) -> pd.DataFrame:
        try:
            df: pd.DataFrame = pd.read_csv(
                filepath_or_buffer=file_path,
                header=0,
                skipinitialspace=True,
                encoding="utf-8",
            )
            logger.debug(f"CSV file '{self.file_name}' read successfully with {len(df)} rows.")
            return df
        except Exception as e:
            logger.exception(f"{self.file_name}: Failed to read CSV file '{file_path}': {e}")
            raise ProcessorError(f"Failed to read CSV file '{file_path}': {e}")

    def _clean_headers(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        try:
            df = df.drop(labels=df.columns[0], axis=1)
            logger.debug(f"Dropped the first column from '{self.file_path}'.")

            if "Time (h)" in df.columns:
                df.rename(columns={"Time (h)": "Time"}, inplace=True)
                logger.debug("Renamed 'Time (h)' to 'Time'.")

            if "Time" not in df.columns:
                logger.error(f"{self.file_name}: 'Time' column is missing after cleaning headers.")
                raise DataValidationError("'Time' column is missing after cleaning headers.")

            cleaned_columns: list[str] = self._clean_column_names(columns=df.columns, data_type=data_type)
            df.columns = cleaned_columns
            logger.debug(f"Cleaned headers: {cleaned_columns}")
            return df
        except Exception as e:
            logger.exception(f"{self.file_name}: Failed to clean headers: {e}")
            raise ProcessorError(f"Failed to clean headers: {e}")

    def _clean_column_names(self, columns: pd.Index, data_type: str) -> list[str]:
        cleaned_columns: list[str] = []
        for col in columns:
            if col.startswith(f"{data_type} "):
                col_clean: str = col[len(data_type) + 1 :]
            else:
                col_clean = col

            if "[" in col_clean and "]" in col_clean:
                col_clean = col_clean.split("[")[0].strip()

            cleaned_columns.append(col_clean)
        return cleaned_columns

    def _reshape_timeseries_df(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Reshape the timeseries DataFrame based on the data type.

        Headers are validated against a dynamically built list via ``check_headers_match``.

        Args:
            df (pd.DataFrame): The cleaned DataFrame.
            data_type (str): The data type identifier.

        Returns:
            pd.DataFrame: The reshaped DataFrame.
        """
        category_type = "Chan ID" if "1d" in self.name_parser.suffixes else "Location"
        logger.debug(
            f"{'1d' in self.name_parser.suffixes and 'Chan ID' or 'Location'} suffix detected; using '{category_type}' as category type."
        )

        try:
            if data_type == "H":
                df_melted: pd.DataFrame = self._reshape_h_data(df=df, category_type=category_type)
            else:
                # For data types like 'Q', 'F', 'V' with single value per channel
                df_melted = df.melt(id_vars=["Time"], var_name=category_type, value_name=data_type)
                logger.debug(f"Reshaped DataFrame to long format with {len(df_melted)} rows.")
        except Exception as e:
            logger.exception(f"{self.file_name}: Failed to reshape DataFrame: {e}")
            raise ProcessorError(f"Failed to reshape DataFrame: {e}")

        if df_melted.empty:
            logger.error(f"{self.file_name}: No data found after reshaping.")
            raise DataValidationError("No data found after reshaping.")

        # Validate headers
        # Build the expected header list dynamically. It always starts with "Time" and the
        # category column, which switches between "Chan ID" for 1D results and "Location" for
        # 2D results. For "H" data types, both "H_US" and "H_DS" are expected; otherwise the
        # single data type column is used. ``check_headers_match`` validates against this list.
        expected_headers: list[str] = (
            ["Time", category_type, "H_US", "H_DS"] if data_type == "H" else ["Time", category_type, data_type]
        )
        self.expected_in_header = expected_headers
        if not self.check_headers_match(test_headers=df_melted.columns.tolist()):
            logger.error(f"{self.file_name}: Header mismatch after reshaping.")
            raise DataValidationError("Header mismatch after reshaping.")

        return df_melted

    def _reshape_h_data(self, df: pd.DataFrame, category_type: str) -> pd.DataFrame:
        """Special handling for 'H' data type which has 'H_US' and 'H_DS' per channel.
        # Assuming headers are like 'H_US', 'H_DS' for each channel
        # We need to reshape such that each channel has two entries per time: 'H_US' and 'H_DS'
        # Alternatively, we can create separate columns for 'H_US' and 'H_DS'
        # For simplicity, we'll assume each channel has both 'H_US' and 'H_DS' and reshape accordingly

        # First, verify that for each channel, both 'H_US' and 'H_DS' exist
        Args:
            df (pd.DataFrame): The cleaned DataFrame.
            category_type (str): The category type identifier.

        Returns:
            pd.DataFrame: The reshaped DataFrame."""
        # Extract channel identifiers by removing suffixes
        channels = set()
        for col in df.columns:
            if col.endswith("_US") or col.endswith("_DS"):
                channels.add(col.rsplit("_", 1)[0])

        records = []
        for _, row in df.iterrows():
            time = row["Time"]
            for chan in channels:
                h_us = row.get(f"{chan}_US", -9999.0)
                h_ds = row.get(f"{chan}_DS", -9999.0)
                records.append(
                    {
                        "Time": time,
                        category_type: chan,
                        "H_US": h_us,
                        "H_DS": h_ds,
                    }
                )

        df_melted = pd.DataFrame(data=records)
        logger.debug(f"Reshaped 'H' DataFrame to long format with {len(df_melted)} rows.")
        return df_melted

    def _apply_final_transformations(self, data_type: str) -> None:
        """Apply final transformations to the DataFrame after reshaping.

        Args:
            data_type (str): The data type identifier."""
        col_types: dict[str, str] = {
            "Time": "float64",
            data_type: "float64",
        }

        if data_type == "H":
            col_types.update({"H_US": "float64", "H_DS": "float64"})

        self.apply_dtype_mapping(dtype_mapping=col_types, context="final_transformations")

    def read_ccA_data(self) -> tuple[pd.DataFrame, int]:
        """Reads ccA files with 'ccA' data format.

        Returns:
            tuple[pd.DataFrame, int]: DataFrame and status code.
                Status Codes:
                    0 - Success
                    1 - Empty DataFrame
                    2 - Header mismatch
                    3 - Read error"""
        # 'ccA' cannot be processed yet; raise NotImplementedError
        raise NotImplementedError("Processing of ccA data format is not yet implemented.")

    def apply_dtype_mapping(self, dtype_mapping: dict[str, str], context: str = "") -> None:
        """Apply dtype mapping to the DataFrame.

        Args:
            dtype_mapping (dict[str, str]): Mapping of column names to data types.
            context (str): Contextual information for logging."""
        try:
            self.df = self.df.astype(dtype=dtype_mapping)
            logger.debug(f"{self.file_name}: Applied dtype mapping in {context}: {dtype_mapping}")
        except (KeyError, ValueError, TypeError) as e:
            msg: str = f"{self.file_name}: Error applying dtype mapping in {context}: {e}"
            logger.error(msg)
            raise ProcessorError(msg)

    def separate_metrics(self, metric_columns: dict[str, str], new_columns: dict[str, Any]) -> pd.DataFrame:
        """Separate metrics into distinct rows based on provided column mappings.

        Args:
            metric_columns (dict[str, str]): Mapping of original columns to new columns.
            new_columns (dict[str, Any]): Additional columns to add with default values.

        Returns:
            pd.DataFrame: Reshaped DataFrame."""
        reshaped_dfs: list[pd.DataFrame] = []
        for original, new in metric_columns.items():
            temp_df: pd.DataFrame = self.df[[original]].copy()
            temp_df.rename(columns={original: new}, inplace=True)
            for col, val in new_columns.items():
                temp_df[col] = val
            reshaped_dfs.append(temp_df)

        return pd.concat(reshaped_dfs, ignore_index=True)

    def order_categorical_columns(self, df: pd.DataFrame, columns: list[str]) -> None:
        """Convert specified columns to ordered categorical types with categories sorted alphabetically.

        Args:
            df (pd.DataFrame): The DataFrame containing the columns.
            columns (list[str]): List of column names to convert."""
        for col in columns:
            if df[col].dtype.name == "category" and not df[col].cat.ordered:
                sorted_categories: list[str] = sorted(df[col].cat.categories)
                df[col] = df[col].cat.set_categories(new_categories=sorted_categories, ordered=True)
                logger.debug(f"Column '{col}' ordered alphabetically.")
