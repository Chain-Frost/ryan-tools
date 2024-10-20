import re

# Unlicensed regex

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
    match: re.Match[str] | None = re.search(pattern=pattern, string=string, flags=re.IGNORECASE)
    
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
    match: re.Match[str] | None = re.search(pattern=pattern, string=string, flags=re.IGNORECASE)
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
    match: re.Match[str] | None = re.search(pattern=pattern, string=string, flags=re.IGNORECASE)
    if match:
        return match.group(0).replace("_", "").replace("p", "")
    else:
        raise ValueError(f"AEP pattern not found in the string: {string}")
