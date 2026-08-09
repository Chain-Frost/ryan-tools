# ryan_library/functions/string_helpers.py
"""String manipulation helpers for ryan_library."""


def split_strings(input_str: str | list[str]) -> list[str]:
    """Split input string(s) by whitespace into a flat list of strings.
    Args:
        input_str (str | list[str]): A string or list of strings to split.
    Returns:
        list[str]: A flat list of split strings."""
    if isinstance(input_str, str):
        input_list: list[str] = [input_str]
    else:  # input is already a list
        input_list = input_str

    # Split each string by whitespace and flatten the list
    split_list: list[str] = []
    for item in input_list:
        split_list.extend(item.split())

    return split_list


def split_strings_in_dict(params_dict: dict[str, list[str]]) -> dict[str, list[str]]:
    """Apply split_strings to each list of strings in the dictionary.
    Args:
        params_dict (dict[str, list[str]]): Dictionary with string lists to split.
    Returns:
        dict[str, list[str]]: Dictionary with split string lists."""
    for key, value in params_dict.items():
        # Use split_strings to handle both string and list of strings cases
        params_dict[key] = split_strings(input_str=value)
    return params_dict
