# ryan_library.functions.data_processing.py
import re  # Unlicensed regex
import logging
from typing import Any, Callable, Optional

# Configure the logger for the module
logger = logging.getLogger(__name__)


def safe_apply(func: Callable[[Any], Any], value: Any) -> Optional[Any]:
    """
    Safely applies a function to a given value, ignoring any exceptions that occur.

    This function attempts to execute `func(value)`. If `func` raises an exception,
    the exception is caught, an error is logged, and `None` is returned instead.

    Args:
        func (Callable[[Any], Any]): A function that takes a single argument and returns a value.
        value (Any): The value to be processed by `func`.

    Returns:
        Optional[Any]: The result of `func(value)` if successful; otherwise, `None`.

    Example:
        >>> def divide_by_two(x):
        ...     return x / 2
        >>> safe_apply(divide_by_two, 10)
        5.0
        >>> safe_apply(divide_by_two, 'a')  # This will raise an exception inside divide_by_two
        None
    """
    try:
        # Attempt to apply the function to the value
        result = func(value)
        logger.debug(
            f"Function {func.__name__} applied successfully on value: {value}. Result: {result}"
        )
        return result
    except Exception as e:
        # Catch all exceptions to prevent the application from crashing
        logger.debug(
            f"Error applying function '{func.__name__}' on value '{value}': {e}",
            exc_info=True,
        )
        return None


def check_string_TP(string: str) -> str:
    """
    Searches for a 'TP' pattern followed by exactly two digits in the provided string.
    Considers the context to ensure 'TP' is either at the start/end of the string
    or surrounded by underscores or pluses.

    Args:
        string (str): The string in which to search for the TP pattern.

    Returns:
        str: A string of exactly two digits following 'TP' if found.

    Raises:
        ValueError: If the TP pattern is not found in the string.
    """
    # Regex pattern to find 'TP' followed by exactly two digits, with context checks
    pattern = r"(?:[_+]|^)TP(\d{2})(?:[_+]|$)"
    match: re.Match[str] | None = re.search(
        pattern=pattern, string=string, flags=re.IGNORECASE
    )

    if match:
        return match.group(1)  # Return the two digits following 'TP'
    else:
        raise ValueError(f"TP pattern not found in the string: {string}")


def check_string_duration(string: str) -> str:
    """
    Searches for a duration pattern within a given string. The pattern is defined to capture
    a duration formatted as three to five digits followed by 'm' or 'M', which is either at the start/end of the string
    or surrounded by underscores or pluses.
    June 2024

    Args:
        string (str): The string to search for a duration pattern.
        e.g. TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv

    Returns:
        str: The matched duration substring without underscores or the character 'm'.

    Raises:
        ValueError: If no duration pattern is found in the string.
    """
    pattern = r"(?:[_+]|^)(\d{3,5}[mM])(?:[_+]|$)"
    match: re.Match[str] | None = re.search(
        pattern=pattern, string=string, flags=re.IGNORECASE
    )
    if match:
        return match.group(0).replace("_", "").replace("m", "")
    else:
        raise ValueError(f"Duration pattern not found in the string: {string}")


def check_string_aep(string: str) -> str:
    """
    Searches for an Annual Exceedance Probability (AEP) pattern within a given string. The pattern is defined to capture
    an AEP formatted as two digits followed by a decimal and one or two more digits, ending with 'p', which is either at the start/end of the string
    or surrounded by underscores or pluses.
    June 2024

    Args:
        string (str): The string to search for an AEP pattern.
        e.g. TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv

    Returns:
        str: The matched AEP substring without underscores or the character 'p'.

    Raises:
        ValueError: If no AEP pattern is found in the string.
    """
    pattern = r"(?:[_+]|^)(\d{2}\.\d{1,2}p)(?:[_+]|$)"
    match: re.Match[str] | None = re.search(
        pattern=pattern, string=string, flags=re.IGNORECASE
    )
    if match:
        return match.group(0).replace("_", "").replace("p", "")
    else:
        raise ValueError(f"AEP pattern not found in the string: {string}")
