# ryan_library\classes\tuflow_string_classes.py
import re
from pathlib import Path
from loguru import logger
from dataclasses import dataclass, field
from ryan_library.classes.suffixes_and_dtypes import suffixes_config


@dataclass
class RunCodeComponent:
    """
    Represents a component of a run code (e.g., AEP, Duration, TP).
    """

    raw_value: str
    component_type: str
    numeric_value: float | int | None = field(init=False)
    text_repr: str = field(init=False)

    def __post_init__(self) -> None:
        self.component_type = self.component_type.lower()
        self.numeric_value = self._parse_numeric_value(self.raw_value)
        self.text_repr = self._generate_text_repr()

    def _parse_numeric_value(self, raw_value: str) -> float | int | None:
        """
        Parse the raw value into a numeric type.

        Returns:
            int or float: Parsed numeric value.
        """
        try:
            return float(raw_value) if "." in raw_value else int(raw_value)
        except ValueError:
            logger.error(
                f"Invalid numeric value for {self.component_type}: {raw_value}"
            )
            return None

    def _generate_text_repr(self) -> str:
        """
        Generate a textual representation based on the component type.

        Returns:
            str: Textual representation of the component.
        """
        mappings = {
            "tp": f"TP{self.raw_value}",
            "duration": f"{self.raw_value}m",
            "aep": f"{self.raw_value}p",
        }
        return mappings.get(self.component_type, self.raw_value)

    def __str__(self) -> str:
        return self.text_repr


class TuflowStringParser:
    """
    A class to parse Tuflow-specific strings and file paths.
    """

    # Precompile regex patterns for efficiency
    TP_PATTERN: re.Pattern[str] = re.compile(
        pattern=r"(?:[_+]|^)TP(\d{2})(?:[_+]|$)", flags=re.IGNORECASE
    )
    DURATION_PATTERN: re.Pattern[str] = re.compile(
        pattern=r"(?:[_+]|^)(\d{3,5})[mM](?:[_+]|$)", flags=re.IGNORECASE
    )
    AEP_PATTERN: re.Pattern[str] = re.compile(
        pattern=r"(?:[_+]|^)(\d{2}\.\d{1,2})p(?:[_+]|$)", flags=re.IGNORECASE
    )

    def __init__(self, file_path: Path | str):
        """
        Initialize the TuflowStringParser with the given file path.

        Args:
            file_path (Path | str): Path to the file to be processed.
        """
        self.file_path = Path(file_path)
        self.file_name: str = self.file_path.name
        self.suffixes: dict[str, str] = self.load_suffixes()
        self.data_type: str | None = self.determine_data_type()
        self.raw_run_code: str = self.extract_raw_run_code()
        self.clean_run_code: str = self.clean_runcode(run_code=self.raw_run_code)
        self.run_code_parts: dict[str, str] = self.extract_run_code_parts(
            clean_run_code=self.clean_run_code
        )
        self.tp: RunCodeComponent | None = self.parse_tp(string=self.clean_run_code)
        self.duration: RunCodeComponent | None = self.parse_duration(
            string=self.clean_run_code
        )
        self.aep: RunCodeComponent | None = self.parse_aep(string=self.clean_run_code)
        self.trim_run_code: str = self.trim_the_run_code()

    @staticmethod
    def clean_runcode(run_code: str) -> str:
        """
        Replace '+' with '_' to standardize delimiters.

        Args:
            run_code (str): The raw run code string.

        Returns:
            str: Cleaned run code string.
        """
        return run_code.replace("+", "_")

    # remake this function to use suffixes_and_dtypes.py
    @staticmethod
    def load_suffixes() -> dict[str, str]:
        """
        Load suffixes using the SuffixesConfig class.

        Returns:
            dict[str, str]: Suffix to type mapping.
        """
        try:
            suffixes = suffixes_config.suffix_to_type
            logger.debug(f"Loaded suffixes: {suffixes}")
            return suffixes
        except AttributeError as e:
            logger.error(f"Error accessing suffixes from SuffixesConfig: {e}")
            return {}

    def determine_data_type(self) -> str | None:
        """
        Determine the data type based on the file suffix.

        Returns:
            Optional[str]: Data type if a matching suffix is found, otherwise None.
        """
        for suffix, data_type in self.suffixes.items():
            if self.file_name.lower().endswith(suffix.lower()):
                logger.debug(
                    f"Determined data type '{data_type}' for suffix '{suffix}'"
                )
                return data_type
        logger.warning(f"No matching suffix found for file '{self.file_name}'")
        return None

    def extract_raw_run_code(self) -> str:
        """
        Extract the raw run code from the file name.

        Returns:
            str: Extracted run code.
        """
        for suffix in self.suffixes.keys():
            if self.file_name.lower().endswith(suffix.lower()):
                run_code = self.file_name[: -len(suffix)]
                logger.debug(
                    f"Extracted raw run code '{run_code}' from file name '{self.file_name}'"
                )
                return run_code
        logger.debug(
            f"No suffix matched; using entire file name '{self.file_name}' as run code"
        )
        return self.file_name

    @staticmethod
    def extract_run_code_parts(clean_run_code: str) -> dict[str, str]:
        """
        Extract additional RunCode parts from the filename and insert as new columns.

        Args:
            clean_run_code (str): The cleaned run code string.

        Returns:
            dict[str, str]: Dictionary of run code parts with keys like 'R01', 'R02', etc.
        """
        run_code_parts = clean_run_code.split("_")
        r_dict = {
            f"R{index:02}": part for index, part in enumerate(run_code_parts, start=1)
        }
        logger.debug(f"Extracted run code parts: {r_dict}")
        return r_dict

    def parse_tp(self, string: str) -> RunCodeComponent | None:
        """
        Parse the TP component from the run code.

        Args:
            string (str): The run code string.

        Returns:
            Optional[RunCodeComponent]: Parsed TP component or None if not found.
        """
        match = self.TP_PATTERN.search(string)
        if match:
            tp_value = match.group(1)
            logger.debug(f"Parsed TP value: {tp_value}")
            return RunCodeComponent(raw_value=tp_value, component_type="TP")
        logger.debug("No TP component found")
        return None

    def parse_duration(self, string: str) -> RunCodeComponent | None:
        """
        Parse the Duration component from the run code.

        Args:
            string (str): The run code string.

        Returns:
            Optional[RunCodeComponent]: Parsed Duration component or None if not found.
        """
        match = self.DURATION_PATTERN.search(string)
        if match:
            duration_value = match.group(1)
            logger.debug(f"Parsed Duration value: {duration_value}")
            return RunCodeComponent(raw_value=duration_value, component_type="Duration")
        logger.debug("No Duration component found")
        return None

    def parse_aep(self, string: str) -> RunCodeComponent | None:
        """
        Parse the AEP component from the run code.

        Args:
            string (str): The run code string.

        Returns:
            Optional[RunCodeComponent]: Parsed AEP component or None if not found.
        """
        match = self.AEP_PATTERN.search(string)
        if match:
            aep_value = match.group(1)
            logger.debug(f"Parsed AEP value: {aep_value}")
            return RunCodeComponent(raw_value=aep_value, component_type="AEP")
        logger.debug("No AEP component found")
        return None

    def trim_the_run_code(self) -> str:
        """
        Clean the run code by removing AEP, Duration, and TP components.

        Returns:
            str: Cleaned run code.
        """
        components_to_remove = {
            str(component)
            for component in [self.aep, self.duration, self.tp]
            if component
        }
        logger.debug(f"Components to remove: {components_to_remove}")
        trimmed_runcode = "_".join(
            part
            for part in self.clean_run_code.split("_")
            if part not in components_to_remove
        )
        logger.debug(f"Trimmed run code: {trimmed_runcode}")
        return trimmed_runcode
