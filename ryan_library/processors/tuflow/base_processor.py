# ryan_library/processors/tuflow/base_processor.py

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
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


# Standard status codes returned by processor helpers.
class ProcessorStatus(IntEnum):
    """Named status codes shared by processor helpers."""

    SUCCESS = 0
    EMPTY_DATAFRAME = 1
    HEADER_MISMATCH = 2
    FAILURE = 3


# the processors are imported as required within the class (importlib)


@dataclass
class BaseProcessor(ABC):
    """Base class for processing different types of TUFLOW CSV and CCA files.

    Concrete processors generally inherit from intermediate helpers such as
    :class:`MaxDataProcessor` or :class:`TimeSeriesProcessor` to ingest CSV data
    before applying dataset specific reshaping or validation steps.
    """

    file_path: Path
    file_name: str = field(init=False)
    resolved_file_path: Path = field(init=False)
    name_parser: TuflowStringParser = field(init=False)
    data_type: str = field(init=False)
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    raw_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    processed: bool = field(default=False)

    # Define _processor_cache as a ClassVar to make it a class variable
    _processor_cache: ClassVar[dict[tuple[str, str], type["BaseProcessor"]]] = {}

    # Attributes to hold configuration
    output_columns: dict[str, str] = field(init=False, default_factory=dict)
    dataformat: str = field(init=False, default="")
    processor_module: str | None = field(init=False, default=None)
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
        logger.debug(f"Attempting to process file: {file_path}")

        # Use TuflowStringParser to determine data_type
        name_parser = TuflowStringParser(file_path=file_path)
        data_type: str | None = name_parser.data_type

        if not data_type:
            error_msg = f"No data type found for file: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Get processor class name based on data type
        suffixes_config = SuffixesConfig.get_instance()
        data_type_def = suffixes_config.get_definition_for_data_type(data_type=data_type)

        processor_class_name: str | None = None
        processor_dataformat: str | None = None
        processor_module: str | None = None
        if data_type_def is not None:
            processor_class_name = data_type_def.processor
            processor_parts = data_type_def.processing_parts
            processor_dataformat = processor_parts.dataformat or None
            processor_module = processor_parts.processor_module

        if not processor_class_name:
            error_msg = f"No processor class specified for data type '{data_type}'."
            logger.error(error_msg)
            raise KeyError(error_msg)

        # Dynamically import the processor class
        processor_cls: type[BaseProcessor] = cls.get_processor_class(
            class_name=processor_class_name,
            processor_module=processor_module,
            dataformat=processor_dataformat,
        )

        # Instantiate the processor class
        try:
            processor = processor_cls(file_path=file_path)
            return processor
        except Exception as e:
            logger.exception(f"Error instantiating processor '{processor_class_name}' for file {file_path}: {e}")
            raise

    @staticmethod
    def get_processor_class(
        class_name: str, processor_module: str | None = None, dataformat: str | None = None
    ) -> type["BaseProcessor"]:
        """Dynamically import and return a processor class by name with caching."""

        cache_namespace = processor_module or dataformat or ""
        cache_key = (cache_namespace, class_name)
        if cache_key in BaseProcessor._processor_cache:
            logger.debug(
                "Using cached processor class '%s' for module hint '%s'.",
                class_name,
                cache_namespace or "<default>",
            )
            return BaseProcessor._processor_cache[cache_key]

        base_package = "ryan_library.processors.tuflow"
        candidate_modules: list[str] = []

        def extend_with_module(module_hint: str) -> None:
            module_hint = module_hint.strip()
            if not module_hint:
                return
            is_absolute = module_hint.startswith("ryan_library.")
            normalized = module_hint.lstrip(".") if not is_absolute else module_hint
            module_base = normalized if is_absolute else f"{base_package}.{normalized}"
            candidate_modules.append(f"{module_base}.{class_name}")
            candidate_modules.append(module_base)

        if processor_module:
            extend_with_module(processor_module)

        candidate_modules.append(f"{base_package}.{class_name}")
        candidate_modules.append(base_package)

        seen: set[str] = set()
        ordered_candidates: list[str] = []
        for module_path in candidate_modules:
            if module_path not in seen:
                seen.add(module_path)
                ordered_candidates.append(module_path)

        last_exception: Exception | None = None
        attempted_paths: list[str] = []

        for module_path in ordered_candidates:
            attempted_paths.append(module_path)
            try:
                module = importlib.import_module(module_path)
                processor_cls: type[BaseProcessor] = cast(
                    type["BaseProcessor"], getattr(module, class_name)
                )
                BaseProcessor._processor_cache[cache_key] = processor_cls
                logger.debug(f"Imported processor class '{class_name}' from '{module_path}'.")
                return processor_cls
            except ImportError as exc:
                logger.debug(f"Failed to import module '{module_path}': {exc}")
                last_exception = exc
            except AttributeError as exc:
                logger.debug(f"Module '{module_path}' does not define '{class_name}': {exc}")
                last_exception = exc

        attempted = ", ".join(attempted_paths)
        msg = f"Processor class '{class_name}' not found in modules: {attempted}."
        logger.exception(msg)
        if last_exception is not None:
            raise ImportProcessorError(msg) from last_exception
        raise ImportProcessorError(msg)

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
        self.processor_module = processing_parts.processor_module
        self.skip_columns = processing_parts.skip_columns
        logger.debug(
            f"{self.file_name}: Loaded processingParts - dataformat: {self.dataformat}, "
            f"processor_module: {self.processor_module}, skip_columns: {self.skip_columns}"
        )

        # TODO: tighten configuration validation by inspecting the JSON contents
        # before attempting to load data type specific sections.

        # Depending on dataformat, load columns_to_use or expected_in_header
        handled_formats: set[str] = {"Maximums", "ccA", "Timeseries", "POMM"}

        if self.dataformat in {"Maximums", "ccA", "POMM"}:
            self.columns_to_use = processing_parts.columns_to_use
            logger.debug(f"{self.file_name}: Loaded columns_to_use: {self.columns_to_use}")

        if self.dataformat in {"Timeseries", "POMM"}:
            self.expected_in_header = processing_parts.expected_in_header
            logger.debug(f"{self.file_name}: Loaded expected_in_header: {self.expected_in_header}")

        if self.dataformat not in handled_formats:
            logger.warning(f"{self.file_name}: Unknown dataformat '{self.dataformat}'.")

    @abstractmethod
    def process(self) -> None:
        """Process the file and modify the instance's DataFrame (``self.df``) in place.

        Subclasses are expected to orchestrate reading via an intermediate
        processor helper (e.g. ``MaxDataProcessor.read_maximums_csv``) and then
        apply any bespoke transformations.
        """

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
        dtype_mapping: dict[str, str] = {
            col: dtype for col, dtype in self.output_columns.items() if col in self.df.columns
        }
        if not dtype_mapping:
            logger.debug(f"{self.file_name}: No matching columns found for output datatype mapping.")
            return
        # Apply data types based on output_columns configuration
        try:
            self.df = self.df.astype(dtype_mapping)
            logger.debug(f"{self.file_name}: Applied output_columns datatype mapping: {dtype_mapping}")
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
                header_error: str = (
                    f"Error reading {self.file_name}, headers did not match expected_in_header format "
                    f"{self.expected_in_header}. Got {test_headers}"
                )
                logger.error(header_error)
                return False
            logger.debug("Test headers matched expected_in_header.")
            return True
        else:
            logger.warning(f"{self.file_name}: No headers to validate against.")
            return True

    def read_maximums_csv(self) -> ProcessorStatus:
        """Read a ``Maximums`` or ``ccA`` CSV into :attr:`self.df`.

        The helper uses the configuration on the processor instance to select
        the expected columns, loads the file and validates the header order
        before storing the DataFrame on the object.

        Returns:
            ProcessorStatus: ``ProcessorStatus.SUCCESS`` if the CSV was loaded
            successfully. ``ProcessorStatus.EMPTY_DATAFRAME`` signals a file
            without data, ``ProcessorStatus.HEADER_MISMATCH`` indicates the
            headers did not align with the configuration and
            ``ProcessorStatus.FAILURE`` captures read failures.
        """
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
            return ProcessorStatus.FAILURE

        if df.empty:
            logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
            return ProcessorStatus.EMPTY_DATAFRAME

        # Validate headers
        if not self.check_headers_match(df.columns.tolist()):
            return ProcessorStatus.HEADER_MISMATCH

        self.df = df
        return ProcessorStatus.SUCCESS

    def apply_dtype_mapping(self, dtype_mapping: dict[str, str], context: str = "") -> None:
        """Apply dtype mapping to the DataFrame.

        Args:
            dtype_mapping (dict[str, str]): Mapping of column names to data types.
            context (str): Contextual information for logging.

        Raises:
            ProcessorError: If :meth:`pandas.DataFrame.astype` fails for any column.
        """
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
