# ryan_library\classes\tuflow_string_classes.py
import math
import re
from pathlib import Path
from loguru import logger
from dataclasses import dataclass, field
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig


@dataclass
class RunCodeComponent:
    """Represents a component of a run code (e.g., AEP, Duration, TP)."""

    raw_value: str
    component_type: str
    numeric_value: float | int | None = field(init=False)
    text_repr: str = field(init=False)
    original_text: str | None = field(default=None)

    def __post_init__(self) -> None:
        self.component_type = self.component_type.lower()
        self.numeric_value = self._parse_numeric_value(raw_value=self.raw_value)
        self.text_repr = self._generate_text_repr()

    def _parse_numeric_value(self, raw_value: str) -> float | int | None:
        """Parse the raw_value into a numeric type. If there is a decimal point,
        round to exactly the number of decimals that appeared in raw_value."""
        if self.component_type == "aep" and raw_value.upper() in {"PMP", "PMPF"}:
            return float("nan")

        try:
            if "." in raw_value:
                val = float(raw_value)
                if self.component_type == "aep":
                    # Count how many digits follow the decimal point in raw_value.
                    decimals = len(raw_value.split(".")[1])
                    return round(val, decimals)
                return val
            else:
                return int(raw_value)
        except ValueError:
            logger.error(f"Invalid numeric value for {self.component_type}: {raw_value}")
            return None

    def _generate_text_repr(self) -> str:
        """Generate a textual representation based on the component type.
        Returns:
            str: Textual representation of the component."""
        if self.component_type == "tp":
            return f"TP{self.raw_value}"
        if self.component_type == "duration":
            return f"{self.raw_value}m"
        if self.component_type == "aep":
            normalized_aep: str = self.raw_value.upper()
            if normalized_aep in {"PMP", "PMPF"}:
                return normalized_aep
            return f"{self.raw_value}p"
        return self.raw_value

    def __str__(self) -> str:
        return self.text_repr


class TuflowStringParser:
    """A class to parse Tuflow-specific strings and file paths."""

    # Precompile regex patterns for efficiency
    # ``TP_PATTERN`` finds patterns like ``TP01`` that are surrounded by ``_`` or ``+`` (or appear at the edges).
    # Keep the core ``TP##``/``TP#`` style bounded by delimiters so we do not
    # accidentally read other numbers embedded within filenames.
    # ``TP_PATTERN`` finds patterns like ``TP01`` that are surrounded by ``_`` or ``+`` (or appear at the edges).
    # Keep the core ``TP##``/``TP#`` style bounded by delimiters so we do not
    # accidentally read other numbers embedded within filenames.
    TP_PATTERN: re.Pattern[str] = re.compile(pattern=r"(?:[_+]|^)TP(\d{1,2})(?:[_+.]|$)", flags=re.IGNORECASE)
    # ``DURATION_PATTERN`` captures 3-5 digits followed by ``m`` (e.g. ``00360m`` or ``360m``).
    DURATION_PATTERN: re.Pattern[str] = re.compile(pattern=r"(?:[_+]|^)(\d{3,5})[mM](?:[_+.]|$)", flags=re.IGNORECASE)
    # ``AEP_PATTERN`` matches values like ``01.00p`` or ``5p`` and reads the numeric portion before ``p``.
    AEP_PATTERN: re.Pattern[str] = re.compile(
        pattern=r"(?:^|[_+])(?P<aep>(?P<numeric>\d+(?:\.\d{1,2})?)p|(?P<text>PMPF|PMP))(?=$|[_+.])",
        flags=re.IGNORECASE,
    )
    # ``GENERIC_TP_PATTERN`` handles free-form strings such as "tp 3".
    GENERIC_TP_PATTERN: re.Pattern[str] = re.compile(pattern=r"TP?\s*(\d{1,2})", flags=re.IGNORECASE)
    HUMAN_DURATION_PATTERN: re.Pattern[str] = re.compile(
        pattern=r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>hours?|hrs?|hr|h|minutes?|mins?|min|m)(?=$|[^A-Za-z0-9])",
        flags=re.IGNORECASE,
    )

    def __init__(self, file_path: Path | str) -> None:
        """Initialize the TuflowStringParser with the given file path.
        Args:
            file_path (Path | str): Path to the file to be processed."""
        self.file_path = Path(file_path)
        self.file_name: str = self.file_path.name
        self.suffixes: dict[str, str] = self.load_suffixes()
        self.data_type: str | None = self.determine_data_type()
        self.raw_run_code: str = self.extract_raw_run_code()
        self.clean_run_code: str = self.clean_runcode(run_code=self.raw_run_code)
        self.run_code_parts: dict[str, str] = self.extract_run_code_parts(clean_run_code=self.clean_run_code)
        self.tp: RunCodeComponent | None = self.parse_tp(string=self.clean_run_code)
        self.duration: RunCodeComponent | None = self.parse_duration(string=self.clean_run_code)
        self.aep: RunCodeComponent | None = self.parse_aep(string=self.clean_run_code)
        self.trim_run_code: str = self.trim_the_run_code()

    @staticmethod
    def _coerce_text(value: object) -> str | None:
        """Convert ``value`` into a cleaned string or ``None`` if it is effectively empty."""

        if value is None:
            return None
        try:
            if math.isnan(value):  # type: ignore[arg-type]
                return None
        except (TypeError, ValueError):
            pass
        text: str = str(value).strip()
        if not text:
            return None
        lowered: str = text.lower()
        if lowered in {"nan", "none", "<na>"}:
            return None
        return text

    @classmethod
    def normalize_tp_label(cls, value: object) -> str | None:
        """Return a canonical ``TP##`` label extracted from ``value`` when possible."""

        text: str | None = cls._coerce_text(value)
        if text is None:
            return None

        # Normalise ``+`` to ``_`` so ``TP##`` detection works for both
        # delimiter styles used in TUFLOW artefacts.
        normalized: str = text.replace("+", "_")
        match: re.Match[str] | None = cls.TP_PATTERN.search(normalized.upper())
        digits: str | None = match.group(1) if match else None
        if digits is None:
            generic_match: re.Match[str] | None = cls.GENERIC_TP_PATTERN.search(text)
            if generic_match:
                digits = generic_match.group(1)
        if digits is None:
            fallback: re.Match[str] | None = re.search(r"\b(\d{1,2})\b", text)
            digits = fallback.group(1) if fallback else None
        if digits is None:
            return None
        try:
            numeric = int(digits)
        except ValueError:
            logger.debug(f"Unable to parse TP digits from value {text!r}")
            return None
        return f"TP{numeric:02d}"

    @classmethod
    def normalize_duration_value(cls, value: object) -> float:
        """Return the numeric component of a duration string or ``nan`` when parsing fails."""

        text: str | None = cls._coerce_text(value)
        if text is None:
            return float("nan")

        # ``DURATION_PATTERN`` covers the common ``00120m`` style.
        normalized: str = text.replace("+", "_")
        match: re.Match[str] | None = cls.DURATION_PATTERN.search(normalized)
        if match:
            return float(match.group(1))

        for human_match in cls.HUMAN_DURATION_PATTERN.finditer(normalized):
            minutes: float | None = cls._minutes_from_human_match(human_match)
            if minutes is not None:
                return minutes

        fallback: re.Match[str] | None = re.search(r"\d{3,}", normalized)
        if fallback:
            return float(fallback.group(0))

        logger.debug(f"Unable to parse duration from value {text!r}")
        return float("nan")

    @staticmethod
    def _minutes_from_human_match(match: re.Match[str]) -> float | None:
        """Convert a ``HUMAN_DURATION_PATTERN`` match into minutes."""

        value_str: str = match.group("value")
        unit: str = match.group("unit").lower()
        digits_only: str = value_str.replace(".", "")
        if unit == "m" and len(digits_only) <= 2:
            # Short bare ``m`` tokens (e.g. "5m") represent grid resolutions
            # rather than durations, so ignore them and keep searching.
            return None

        try:
            magnitude = float(value_str)
        except ValueError:
            return None

        if unit in {"hours", "hour", "hrs", "hr", "h"}:
            return magnitude * 60.0
        if unit in {"minutes", "minute", "mins", "min", "m"}:
            return magnitude
        return None

    @staticmethod
    def clean_runcode(run_code: str) -> str:
        """Replace '+' with '_' to standardize delimiters.
        Args:
            run_code (str): The raw run code string.
        Returns:
            str: Cleaned run code string."""
        return run_code.replace("+", "_")

    # remake this function to use suffixes_and_dtypes.py
    @staticmethod
    def load_suffixes() -> dict[str, str]:
        """Load suffixes using the SuffixesConfig class.
        Returns:
            dict[str, str]: Suffix to type mapping."""
        try:
            suffixes: dict[str, str] = SuffixesConfig.get_instance().suffix_to_type
            logger.debug(f"Loaded suffixes: {suffixes}")
            return suffixes
        except AttributeError as e:
            logger.error(f"Error accessing suffixes from SuffixesConfig: {e}")
            return {}

    def determine_data_type(self) -> str | None:
        """Determine the data type based on the file suffix.

        Returns:
            Optional[str]: Data type if a matching suffix is found, otherwise None."""
        for suffix, data_type in self.suffixes.items():
            if self.file_name.lower().endswith(suffix.lower()):
                logger.debug(f"Determined data type '{data_type}' for suffix '{suffix}'")
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
                run_code: str = self.file_name[: -len(suffix)]
                logger.debug(f"Extracted raw run code '{run_code}' from file name '{self.file_name}'")
                return run_code
        logger.debug(f"No suffix matched; using entire file name '{self.file_name}' as run code")
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
        run_code_parts: list[str] = clean_run_code.split("_")
        r_dict: dict[str, str] = {f"R{index:02}": part for index, part in enumerate(run_code_parts, start=1)}
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
        match: re.Match[str] | None = self.TP_PATTERN.search(string)
        if match:
            tp_value: str = match.group(1)
            original_text: str = match.group(0).strip("_+")
            logger.debug(f"Parsed TP value: {tp_value}")
            return RunCodeComponent(raw_value=tp_value, component_type="TP", original_text=original_text)
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
        # Apply the same normalisation rules as ``normalize_duration_value`` so
        # both parsing paths stay in sync.
        normalized: str = string.replace("+", "_")
        match: re.Match[str] | None = self.DURATION_PATTERN.search(normalized)
        if match:
            duration_value = match.group(1)
            original_text: str = match.group(0).strip("_+")
            logger.debug(f"Parsed Duration value: {duration_value}")
            return RunCodeComponent(
                raw_value=duration_value, component_type="Duration", original_text=original_text
            )

        for human_match in self.HUMAN_DURATION_PATTERN.finditer(normalized):
            minutes: float | None = self._minutes_from_human_match(human_match)
            if minutes is None:
                continue
            minutes_str: str = str(int(minutes)) if minutes.is_integer() else str(minutes)
            original_text = human_match.group(0).strip("_+")
            logger.debug(f"Parsed Duration value from human-readable token: {minutes_str}")
            return RunCodeComponent(
                raw_value=minutes_str, component_type="Duration", original_text=original_text
            )
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
        match: re.Match[str] | None = self.AEP_PATTERN.search(string)
        if match:
            aep_value: str | None = match.group("numeric") or match.group("text")
            if aep_value is None:
                logger.error("AEP pattern matched but no numeric or text group was captured.")
                return None
            logger.debug(f"Parsed AEP value: {aep_value!r}")
            original_text: str = match.group(0).strip("_+")
            return RunCodeComponent(raw_value=aep_value, component_type="AEP", original_text=original_text)
        logger.debug("No AEP component found")
        return None

    def trim_the_run_code(self) -> str:
        """
        Clean the run code by removing AEP, Duration, and TP components.

        Returns:
            str: Cleaned run code.
        """
        removable_components: list[RunCodeComponent | None] = [self.aep, self.duration, self.tp]
        components_to_remove: set[str] = {str(component).lower() for component in removable_components if component}
        # Add original text to components to remove, including duration tokens.
        for component in removable_components:
            if component and component.original_text:
                components_to_remove.add(component.original_text.lower())

        logger.debug(f"Components to remove: {components_to_remove}")
        trimmed_runcode: str = "_".join(
            part for part in self.clean_run_code.split("_") if part.lower() not in components_to_remove
        )
        logger.debug(f"Trimmed run code: {trimmed_runcode}")
        return trimmed_runcode
