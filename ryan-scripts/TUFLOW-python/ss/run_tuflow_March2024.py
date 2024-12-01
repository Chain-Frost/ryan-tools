import datetime
import subprocess
import itertools
import pprint
import re
import os
import sys
import time
from typing import Union

# 12 March 2024 version


def get_parameters() -> dict[
    str,
    str | list[str] | dict[str, list[str]] | dict[str, int] | bool | float | None,
]:
    # TCF and TUFLOWEXE settings
    tcf: str = r".\runs\~s1~_17_~s2~_~e1~_~e2~_~s4~.tcf"
    tuflowexe: str = r"C:\TUFLOW\2023-03-AD\TUFLOW_iSP_w64.exe"
    batch_commands: str = " -b -pu0 "
    computational_priority: str = (
        "/NORMAL"  # /LOW /BELOWNORMAL /NORMAL /ABOVENORMAL /HIGH /REALTIME
    )
    # run_simulations: bool = False    #True False #if you only want the args list
    next_run_file: str | None = (
        None  # "run_HBI_36.py" # Fine to leave this as None if you don't want to
    )
    # priority_order = ['e3 s5']  # Define priority: e.g. ['e3', 's2'] e3 is highest, then s2, etc.

    parameters: dict[str, list[str]] = {
        # you don't need ' and spaces between items, they will be split automatically
        "e1": [" PMPMax "],  # 2aep AEP Event magnitude
        "e2": ["72hr 120hr"],  # StormDuration - event duration
        "e3": [" PMP "],  # TP - Temporal Pattern
        "e4": [""],  # Method - method/source eg flavell/index/rorb style
        # Scenario - Baseline scenarios - DEV or EXG generally
        "s1": ["  END3 "],  # EXG,  # DEV
        "s2": [" MMENDWQR Gatehouse "],  # Extent
        "s3": [""],  # model
        "s4": ["08m"],  # 30m 16m 08m GridSize - base Grid Size
        "s5": [" "],  # Quadtree - Q if Quadtree, C Classic
        "s6": [],  # Materials - For sensitivity, then default
        "s7": [],  # Soil - For sensitivity, then default
        "s8": [],  # not currently used
        "s9": [],  # not currently used
        "e5": [],  # not currently used
        "e6": [],  # not currently used
        "e7": [],  # not currently used
        "e8": [],  # not currently used
        "e9": [],  # not currently used
    }

    if "priority_order" in locals():  # make sure it exists
        priority_order = clean_priority_order(
            priority_order
        )  # allows for list, string, mixed input
    # the first item is the last to be iterated over.
    # so all the others would happen first before changing priority one
    # will be sorted in alphabetical order for items not listed

    """ Options:
    -b batch mode
    -c test input and copy model only
    -ca test input and copy model including all native GIS layers
    -cp "<path>" folder to use for -c or -ca option
    -e{1-9} "<event>" specify Event(s)
    -nmb no message boxes
    -nwk use network dongle only
    -oz "<OZ>" map output includes Output Zone <OZ>
    -pu<PU> utilise GPU Processing Unit (device) <PU> (repeat for more than one)
    -qcf query the creation of a folder
    -s{1-9} "<scenario>" specify Scenario(s)
    -t test input only
    -x execute simulation (default) """

    parameters = filter_parameters(parameters, tcf)
    parameters: dict[str, list[str]] = split_strings_in_dict(parameters)
    max_lengths: dict[str, int] = {
        k: max(len(v) for v in values) if values else 0
        for k, values in parameters.items()
    }

    # Check if batch is None, empty, or contains only spaces
    if not batch_commands.strip():
        batch_commands = "-x"
    # Split the batch string into a list of strings based on spaces
    batch: list[str] = batch_commands.split()
    # Insert the new value at the beginning of the list
    if computational_priority != "/NORMAL":
        batch.insert(0, computational_priority)

    # wait_time_after_run = 2.0  # Default wait time of 2 seconds

    local_vars = locals()
    config = {
        key: local_vars[key]
        for key in [
            "tuflowexe",
            "tcf",
            "batch",
            "parameters",
            "max_lengths",
            "next_run_file",
            "wait_time_after_run",
            "run_simulations",
            "priority_order",
        ]
        if key in local_vars
    }
    # Set defaults for optional variables if they are not set
    config.setdefault("next_run_file", None)
    config.setdefault("wait_time_after_run", 2.0)
    config.setdefault("run_simulations", True)

    required_vars = ["tuflowexe", "tcf", "batch"]
    missing_vars = [var for var in required_vars if var not in config]
    if missing_vars:
        raise ValueError(
            f"Missing required configuration(s): {', '.join(missing_vars)}"
        )

    return config


def run_at_end(script_path: str | None = None) -> None:
    # Use subprocess.run() to execute the script
    if script_path:
        try:
            process = subprocess.Popen(
                ["python", script_path], creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            stdout, stderr = process.communicate()

            if process.returncode == 0:
                print("Script executed successfully.")
            else:
                print(
                    f"An error occurred:\nSTDOUT: {stdout.decode()}\nSTDERR: {stderr.decode()}"
                )
        except Exception as e:
            print(f"An error occurred: {e}")
    else:
        print("Run next path is None. No further execution.")


def clean_priority_order(priority_order: Union[str, list[str]]) -> list[str]:
    return split_strings(priority_order)


def split_strings(input: Union[str, list[str]]) -> list[str]:
    # Normalize the input to a list
    if isinstance(input, str):
        input_list = [input]
    else:  # input is already a list
        input_list = input

    # Split each string by whitespace and flatten the list
    split_list = []
    for item in input_list:
        split_list.extend(item.split())

    return split_list


def split_strings_in_dict(
    params_dict: dict[str, Union[str, list[str]]]
) -> dict[str, list[str]]:
    for key, value in params_dict.items():
        # Use split_strings to handle both string and list of strings cases
        params_dict[key] = split_strings(value)
    return params_dict


# def split_strings_in_dict(params_dict: dict[str, Union[str, list[str]]]) -> dict[str, list[str]]:
#     for key, value in params_dict.items():
#         # Check if the value is a string and not just whitespace, then split
#         if isinstance(value, str) and value.strip():
#             params_dict[key] = value.split()
#         # If the value is a list, iterate through each item, split, and flatten the list
#         elif isinstance(value, list):
#             split_list = []
#             for item in value:
#                 if item.strip():  # Ensure item is not just whitespace
#                     split_list.extend(item.split())  # Split and add to the new list
#             params_dict[key] = split_list
#     return params_dict


def filter_parameters(
    parameters: dict[str, list[str]], tcf: str
) -> dict[str, list[str]]:
    # Exclude parameters with effectively empty values
    non_empty_parameters: dict[str, list[str]] = {
        k: v for k, v in parameters.items() if v and v[0].strip()
    }
    parameter_names: set[str] = set(non_empty_parameters.keys())
    tcf_parameters: set[str] = set(re.findall(r"~(\w{2})~", tcf))

    missing_parameters: set[str] = tcf_parameters - parameter_names
    extra_parameters: set[str] = parameter_names - tcf_parameters

    if missing_parameters:
        print("")
        print("Warning: TCF file has these extra flags:", ", ".join(missing_parameters))

    if extra_parameters:
        print("")
        print(
            "Warning: TCF filename is missing these flags:", ", ".join(extra_parameters)
        )

    return non_empty_parameters


def format_duration(duration: float) -> str:
    # Convert duration in seconds to a timedelta object
    timedelta_obj = datetime.timedelta(seconds=duration)

    # Format the timedelta object as HH:MM:SS or HH:MM:SS.mmmmmm
    formatted_duration = str(timedelta_obj)

    # Add leading zeros to the hours if necessary
    if timedelta_obj.seconds >= 3600:
        formatted_duration: str = formatted_duration.zfill(8)

    return formatted_duration


def run_command(args: list[str], index: int, total: int) -> None:
    print(f"Running simulation {index} of {total}")
    print(" ".join(args))

    start: datetime.datetime = datetime.datetime.now()
    print(f'Start time: {start.strftime("%Y-%m-%d %H:%M:%S")}')

    # Adjusted for Windows to open in a new console window
    creation_flags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0

    process = subprocess.Popen(args, creationflags=creation_flags)
    process.wait()  # Wait for the external process to finish

    # print(f"Return code: {process.returncode}")

    if process.returncode == 0:
        print("\033[92mSimulation completed successfully.\033[0m")  # Green
    else:
        print(
            f"\033[91mSimulation failed with return code {process.returncode}.\033[0m"
        )  # Red

    # if process.returncode == 0:
    #     print("Simulation completed successfully.")
    # else:
    #     print(f"Simulation failed with return code {process.returncode}.")

    end: datetime.datetime = datetime.datetime.now()
    print(f'End time:   {end.strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"Duration:   {format_duration((end-start).total_seconds())} ")
    print("")


def generate_arg(
    tuflowexe: str,
    batch: list[str],
    tcf: str,
    keys: list[str],
    combination: tuple[str, ...],
    max_lengths: dict[str, int],
) -> list[str]:
    # Generate the argument, padding each value to the maximum length
    # Start with the tuflow executable and extend with the batch
    args: list[str] = [tuflowexe]
    args.extend(batch)
    args += [
        item
        for sublist in (
            [f"-{key}", f"{value.ljust(max_lengths[key])}"]
            for key, value in zip(keys, combination)
        )
        for item in sublist
    ]
    args.append(tcf)
    return args


def generate_all_args(
    tuflowexe: str,
    batch: list[str],
    tcf: str,
    keys,
    combinations,
    max_lengths: dict[str, int],
) -> list[list[str]]:
    return [
        generate_arg(tuflowexe, batch, tcf, keys, combination, max_lengths)
        for combination in combinations
    ]


def export_args(args_list: list[list[str]], base_filename: str) -> None:
    # Extract the base name of the current script without the .py extension
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    # Create a unique filename by appending the script name to the base filename
    unique_filename = f"{script_name}_{base_filename}"

    # Proceed with exporting the arguments to the unique filename
    with open(unique_filename, "w") as file:
        for args in args_list:
            file.write(" ".join(args) + "\n")


def main() -> None:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    print(script_dir)
    print("")
    params: dict[
        str, str | list[str] | dict[str, list[str]] | dict[str, int] | None
    ] = get_parameters()

    tuflowexe: str = params["tuflowexe"]  # type: ignore
    batch: list[str] = params["batch"]  # type: ignore
    tcf: str = params["tcf"]  # type: ignore
    next_run_file: str = params["next_run_file"]  # type: ignore
    parameters: dict[str, list[str]] = params["parameters"]  # type: ignore
    max_lengths: dict[str, int] = params["max_lengths"]  # type: ignore
    wait_time_after_run: float = params["wait_time_after_run"]

    # Check if "priority_order" exists and is not None
    if params.get("priority_order") is not None:
        # Existing code to get keys and values from parameters
        keys, values = zip(*parameters.items())
        # Step 2: Sort keys based on custom order
        priority_order = params["priority_order"]
        sorted_keys = sorted(
            keys,
            key=lambda x: (
                priority_order.index(x) if x in priority_order else len(priority_order)
            ),
        )
        # Step 3: Use sorted_keys for combinations
        combinations = list(
            itertools.product(*(parameters[key] for key in sorted_keys))
        )
    else:
        # Fallback to original keys order if priority_order doesn't exist
        sorted_keys, values = zip(*sorted(parameters.items()))
        combinations = list(itertools.product(*values))

    # Generate all the command line arguments
    args_list: list[list[str]] = generate_all_args(
        tuflowexe, batch, tcf, sorted_keys, combinations, max_lengths
    )

    # Print all arguments before presenting the parameters
    print("All Generated Arguments:")
    for i, args in enumerate(args_list, 1):
        print(" ".join(args))
    print()

    # Export these command line arguments to a text file
    export_args(args_list=args_list, base_filename="args.txt")

    print()
    params.pop("max_lengths")
    pprint.pprint(params, sort_dicts=False)
    print()

    if params["run_simulations"]:
        print(f'run_simulation: {params["run_simulations"]}')
        # Run the commands one by one
        for i, args in enumerate(args_list, 1):
            run_command(args, i, len(args_list))
            if i < len(args_list):  # Wait only if it's not the last simulation
                time.sleep(wait_time_after_run)
        run_at_end(script_path=next_run_file)
    else:
        print(
            f'TUFLOW runs exported to text file only - run_simulations: {params["run_simulations"]}'
        )
        print()
    subprocess.call("pause", shell=True)  # wait for exit


if __name__ == "__main__":
    main()
