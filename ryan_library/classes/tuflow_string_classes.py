# ryan_library\classes\tuflow_string_classes.py
import re
import json
from pathlib import Path
from typing import Optional
import logging
from dataclasses import dataclass, field

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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
            logging.error(
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
            "TP": f"TP{self.raw_value}",
            "Duration": f"{self.raw_value}m",
            "AEP": f"{self.raw_value}p",
        }
        return mappings.get(self.component_type, self.raw_value)

    def __str__(self) -> str:
        return self.text_repr


class TuflowStringParser:
    """
    A class to parse Tuflow-specific strings and file paths.
    """

    # Precompile regex patterns for efficiency
    TP_PATTERN = re.compile(r"(?:[_+]|^)TP(\d{2})(?:[_+]|$)", re.IGNORECASE)
    DURATION_PATTERN = re.compile(r"(?:[_+]|^)(\d{3,5})[mM](?:[_+]|$)", re.IGNORECASE)
    AEP_PATTERN = re.compile(r"(?:[_+]|^)(\d{2}\.\d{1,2})p(?:[_+]|$)", re.IGNORECASE)

    def __init__(self, file_path: Path | str):
        """
        Initialize the TuflowStringParser with the given file path.

        Args:
            file_path (Path | str): Path to the file to be processed.
        """
        self.file_path = Path(file_path)
        self.file_name: str = self.file_path.name
        self.suffixes: dict[str, str] = self.load_suffixes()
        self.data_type: Optional[str] = self.determine_data_type()
        self.raw_run_code: str = self.extract_raw_run_code()
        self.clean_run_code: str = self.clean_runcode(self.raw_run_code)
        self.run_code_parts: dict[str, str] = self.extract_run_code_parts(
            self.clean_run_code
        )
        self.tp: Optional[RunCodeComponent] = self.parse_tp(self.clean_run_code)
        self.duration: Optional[RunCodeComponent] = self.parse_duration(
            self.clean_run_code
        )
        self.aep: Optional[RunCodeComponent] = self.parse_aep(self.clean_run_code)
        self.trim_run_code: str = self.trim_runcode()

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
        Load suffixes from a JSON file.

        Returns:
            dict[str, str]: Suffix to type mapping.

        Raises:
            FileNotFoundError: If the suffixes.json file is not found.
            json.JSONDecodeError: If the JSON file is invalid.
        """
        suffixes_path = Path(__file__).parent / "suffixes.json"
        try:
            with suffixes_path.open("r", encoding="utf-8") as file:
                suffixes = json.load(file)["suffixes"]
                logging.debug(f"Loaded suffixes: {suffixes}")
                return suffixes
        except FileNotFoundError:
            logging.error(f"Suffixes file not found at {suffixes_path}")
            return {}
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from {suffixes_path}: {e}")
            return {}

    def determine_data_type(self) -> Optional[str]:
        """
        Determine the data type based on the file suffix.

        Returns:
            Optional[str]: Data type if a matching suffix is found, otherwise None.
        """
        for suffix, data_type in self.suffixes.items():
            if self.file_name.lower().endswith(suffix.lower()):
                logging.debug(
                    f"Determined data type '{data_type}' for suffix '{suffix}'"
                )
                return data_type
        logging.warning(f"No matching suffix found for file '{self.file_name}'")
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
                logging.debug(
                    f"Extracted raw run code '{run_code}' from file name '{self.file_name}'"
                )
                return run_code
        logging.debug(
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
        logging.debug(f"Extracted run code parts: {r_dict}")
        return r_dict

    def parse_tp(self, string: str) -> Optional[RunCodeComponent]:
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
            logging.debug(f"Parsed TP value: {tp_value}")
            return RunCodeComponent(raw_value=tp_value, component_type="TP")
        logging.debug("No TP component found")
        return None

    def parse_duration(self, string: str) -> Optional[RunCodeComponent]:
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
            logging.debug(f"Parsed Duration value: {duration_value}")
            return RunCodeComponent(raw_value=duration_value, component_type="Duration")
        logging.debug("No Duration component found")
        return None

    def parse_aep(self, string: str) -> Optional[RunCodeComponent]:
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
            logging.debug(f"Parsed AEP value: {aep_value}")
            return RunCodeComponent(raw_value=aep_value, component_type="AEP")
        logging.debug("No AEP component found")
        return None

    def trim_runcode(self) -> str:
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
        logging.debug(f"Components to remove: {components_to_remove}")
        trimmed_runcode = "_".join(
            part
            for part in self.clean_run_code.split("_")
            if part not in components_to_remove
        )
        logging.debug(f"Trimmed run code: {trimmed_runcode}")
        return trimmed_runcode
