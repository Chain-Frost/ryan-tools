import datetime
import subprocess
import itertools
import re
import os
import sys
import time
import psutil
from dataclasses import dataclass, field
from pathlib import Path
import colorama
from typing import Optional, List

#2024-10-01 version 

def get_parameters() -> "Parameters":
    """
    Retrieves and initializes parameters for the TUFLOW simulation.

    Returns:
        Parameters: An instance containing core parameters and run variables.
    """
    # Instantiate core parameters
    core_params = CoreParameters(
        tcf=Path(r".\runs\tuflow_01_~s1~_~e1~_~e2~_~s4~.tcf"),
        tuflowexe=Path(r"C:\TUFLOW\2023-03-AF\TUFLOW_iSP_w64.exe"),
        batch_commands=r"-b ",
        computational_priority="LOW",  # Use Windows START command priority options
        # gpu_devices=["-pu0", "-pu1"],  # Uncomment to specify GPU devices
        # wait_time_after_run=2.0,       # Uncomment to specify wait time after run
        # Optional parameters can be set here if different from defaults
    )

    # Define run variables (raw strings) using only valid parameter keys
    run_variables_raw = {
        "e1": "01.00p pmp",
        "e2": "01440m",
        "s1": "EXG supers6",
        "s2": "bigModel",
        "s4": "16M",
    }

    # Process run variables to split strings into lists
    run_variables = {k: split_strings(v) for k, v in run_variables_raw.items()}

    # Create Parameters instance
    params = Parameters(
        core_params=core_params,
        run_variables=run_variables,
        # gpu_devices are now part of core_params
        # wait_time_after_run is now part of core_params
    )

    # Validate required parameters
    check_and_set_defaults(params)

    return params


def split_strings(input_value: str | List[str]) -> List[str]:
    """
    Splits input strings into a list of strings based on whitespace.

    Args:
        input_value (str | List[str]): The input string or list of strings to split.

    Returns:
        List[str]: A list of split strings.
    """
    if isinstance(input_value, str):
        input_list: List[str] = [input_value]
    else:
        input_list = input_value

    split_list: List[str] = []
    for item in input_list:
        split_list.extend(item.strip().split())

    return split_list


@dataclass
class CoreParameters:
    """
    Core parameters required for the TUFLOW simulation.
    """

    tcf: Path
    tuflowexe: Path
    batch_commands: str = "-x"
    computational_priority: str = "NORMAL"  # Use Windows START command priority options
    next_run_file: Optional[str] = None
    priority_order: Optional[str] = None
    run_simulations: bool = True
    gpu_devices: List[str] = field(default_factory=lambda: ["-pu0"])
    wait_time_after_run: float = 2.0


@dataclass
class Parameters:
    """
    Aggregated parameters for the simulation, including core parameters and run variables.
    """

    core_params: CoreParameters
    run_variables: dict[str, List[str]]


@dataclass
class Simulation:
    """
    Represents a single simulation with its associated parameters and state.
    """

    args_for_python: List[str]
    command_for_batch: str
    assigned_gpu: str
    index: int
    start_time: Optional[datetime.datetime] = field(default=None)
    process: Optional[subprocess.Popen] = field(default=None)
    end_time: Optional[datetime.datetime] = field(default=None)


def check_and_set_defaults(params: Parameters) -> None:
    """
    Validates required parameters in the Parameters instance.

    Args:
        params (Parameters): The Parameters instance to validate.

    Raises:
        ValueError: If required parameters are missing.
    """
    # Required parameters
    required_params = ["tcf", "tuflowexe"]

    for key in required_params:
        value = getattr(params.core_params, key)
        if not value:
            raise ValueError(
                f"Required parameter '{key}' is missing and must be set by the user."
            )


def validate_file_paths(core: CoreParameters) -> None:
    """
    Validates that the required file paths exist.

    Args:
        core (CoreParameters): The core parameters containing file paths.

    Raises:
        FileNotFoundError: If a required file does not exist.
    """
    if not core.tuflowexe.is_file():
        raise FileNotFoundError(f"TUFLOW executable not found at: {core.tuflowexe}")
    # Note: The TCF file may not exist yet if it's generated during simulation


def run_at_end(script_path: Optional[str] = None) -> None:
    """
    Executes a script at the end of the simulations if provided.

    Args:
        script_path (Optional[str]): The path to the script to execute.
    """
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
        print("No additional script to run at the end of simulations.")


def filter_parameters(
    parameters: dict[str, List[str]], tcf: Path
) -> dict[str, List[str]]:
    """
    Filters out empty parameters and checks for mismatches with TCF placeholders.

    Args:
        parameters (dict[str, List[str]]): The run variables.
        tcf (Path): The TCF file path.

    Returns:
        dict[str, List[str]]: Filtered run variables.
    """
    non_empty_parameters: dict[str, List[str]] = {
        k: v for k, v in parameters.items() if v and v[0].strip()
    }
    parameter_names: set[str] = set(non_empty_parameters.keys())

    # Extract placeholders from TCF filename (two-character placeholders only)
    tcf_placeholders = re.findall(r"~(\w{2})~", tcf.name)
    tcf_parameters: set[str] = set(tcf_placeholders)

    missing_parameters: set[str] = tcf_parameters - parameter_names
    extra_parameters: set[str] = parameter_names - tcf_parameters

    if missing_parameters:
        print(
            f"Warning: TCF filename is missing these flags: {', '.join(missing_parameters)}"
        )

    if extra_parameters:
        print(
            f"Warning: TCF filename has extra flags not defined in run variables: {', '.join(extra_parameters)}"
        )

    return non_empty_parameters


def format_duration(duration: float) -> str:
    """
    Formats a duration in seconds into a human-readable string.

    Args:
        duration (float): Duration in seconds.

    Returns:
        str: Formatted duration string.
    """
    timedelta_obj = datetime.timedelta(seconds=duration)
    formatted_duration = str(timedelta_obj)
    if timedelta_obj.seconds >= 3600:
        formatted_duration = formatted_duration.zfill(8)
    return formatted_duration


def get_psutil_priority(priority: str):
    """
    Returns the psutil priority constant for the given Windows START command priority string.

    Args:
        priority (str): The priority level as a string.

    Returns:
        int: The psutil priority constant.
    """
    priority_mapping = {
        "LOW": psutil.IDLE_PRIORITY_CLASS,
        "BELOWNORMAL": psutil.BELOW_NORMAL_PRIORITY_CLASS,
        "NORMAL": psutil.NORMAL_PRIORITY_CLASS,
        "ABOVENORMAL": psutil.ABOVE_NORMAL_PRIORITY_CLASS,
        "HIGH": psutil.HIGH_PRIORITY_CLASS,
        "REALTIME": psutil.REALTIME_PRIORITY_CLASS,
    }
    return priority_mapping.get(priority.upper(), psutil.NORMAL_PRIORITY_CLASS)


def generate_arg_for_python(
    tuflowexe: Path,
    batch: List[str],
    tcf: Path,
    keys: List[str],
    combination: tuple[str, ...],
    max_lengths: dict[str, int],
    assigned_gpu: str,
) -> List[str]:
    """
    Generates command arguments for a single simulation (for running within Python).

    Args:
        tuflowexe (Path): Path to the TUFLOW executable.
        batch (List[str]): Batch commands.
        tcf (Path): Path to the TCF file.
        keys (List[str]): List of parameter keys.
        combination (tuple[str, ...]): A combination of parameter values.
        max_lengths (dict[str, int]): Maximum lengths of parameter values for padding.
        assigned_gpu (str): The GPU assigned to this simulation.

    Returns:
        List[str]: A list of command arguments.
    """
    args: List[str] = [str(tuflowexe)]
    args.extend(batch)
    args.append(assigned_gpu)
    args += [
        item
        for sublist in (
            [f"-{key}", f"{value.ljust(max_lengths[key])}"]
            for key, value in zip(keys, combination)
        )
        for item in sublist
    ]
    args.append(str(tcf))
    return args


def generate_arg_for_batch(
    computational_priority: str,
    tuflowexe: Path,
    batch: List[str],
    tcf: Path,
    keys: List[str],
    combination: tuple[str, ...],
    max_lengths: dict[str, int],
    # assigned_gpu: str  # Removed assigned_gpu from batch commands
) -> str:
    """
    Generates command string for a single simulation (for batch file), including computational priority and /WAIT.

    Args:
        computational_priority (str): The computational priority (e.g., "NORMAL", "HIGH").
        tuflowexe (Path): Path to the TUFLOW executable.
        batch (List[str]): Batch commands.
        tcf (Path): Path to the TCF file.
        keys (List[str]): List of parameter keys.
        combination (tuple[str, ...]): A combination of parameter values.
        max_lengths (dict[str, int]): Maximum lengths of parameter values for padding.

    Returns:
        str: A command string ready to be executed in a batch file.
    """
    # Exclude the '/' and add it in the command string
    priority_option = f"/{computational_priority.upper()}"

    # Build the command components without the GPU assignment
    cmd_parts = [
        f'START {priority_option} /WAIT "" "{tuflowexe}"',
        *batch,
        # assigned_gpu  # Do not include GPU assignment in batch file
    ]
    # Pad the parameter keys and values
    for key, value in zip(keys, combination):
        padded_key = f"-{key}"
        padded_value = value.ljust(max_lengths[key])
        cmd_parts.extend([padded_key, padded_value])
    cmd_parts.append(f'"{tcf}"')

    # Join all parts into a single command string
    command_str = " ".join(cmd_parts)
    return command_str


def generate_all_args(
    computational_priority: str,
    tuflowexe: Path,
    batch: List[str],
    tcf: Path,
    keys: List[str],
    combinations: List[tuple[str, ...]],
    max_lengths: dict[str, int],
    gpu_devices: List[str],
) -> List[Simulation]:
    """
    Generates command arguments for all simulations (for Python and batch file), assigning GPUs.

    Args:
        computational_priority (str): The computational priority (e.g., "NORMAL", "HIGH").
        tuflowexe (Path): Path to the TUFLOW executable.
        batch (List[str]): Batch commands.
        tcf (Path): Path to the TCF file.
        keys (List[str]): List of parameter keys.
        combinations (List[tuple[str, ...]]): All combinations of parameter values.
        max_lengths (dict[str, int]): Maximum lengths of parameter values for padding.
        gpu_devices: List[str]: List of GPU devices to assign.

    Returns:
        List[Simulation]: A list of Simulation objects with all necessary info.
    """
    simulations: List[Simulation] = []
    total_simulations = len(combinations)
    for i, combination in enumerate(combinations):
        assigned_gpu = gpu_devices[i % len(gpu_devices)]
        args_for_python = generate_arg_for_python(
            tuflowexe, batch, tcf, keys, combination, max_lengths, assigned_gpu
        )
        command_for_batch = generate_arg_for_batch(
            computational_priority,
            tuflowexe,
            batch,
            tcf,
            keys,
            combination,
            max_lengths,
            # assigned_gpu  # Do not include GPU assignment in batch file
        )
        simulations.append(
            Simulation(
                args_for_python=args_for_python,
                command_for_batch=command_for_batch,
                assigned_gpu=assigned_gpu,
                index=i + 1,
            )
        )
    return simulations


def export_args(args_list: List[str], base_filename: str) -> None:
    """
    Exports the generated command strings to a batch file.

    Args:
        args_list (List[str]): List of command strings.
        base_filename (str): Base name for the output file.
    """
    script_name = Path(__file__).stem
    unique_filename = f"{script_name}_{base_filename}"

    with open(unique_filename, "w") as file:
        for command_str in args_list:
            file.write(command_str + "\n")


def main() -> None:
    """
    Main function to orchestrate the simulation setup and execution.
    """
    # Initialize colorama for colored text output
    colorama.init()

    # Print start time
    script_start_time = datetime.datetime.now()
    print(f"Script start time: {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir)
    print(f"Script directory: {script_dir}")

    # Print script file name
    script_name = Path(__file__).name
    print(f"Script file name: {script_name}\n")

    params = get_parameters()
    core = params.core_params

    # Validate file paths
    try:
        validate_file_paths(core)
    except FileNotFoundError as e:
        print(e)
        return

    computational_priority = core.computational_priority
    tuflowexe = core.tuflowexe
    batch = split_strings(core.batch_commands)
    tcf = core.tcf
    next_run_file = core.next_run_file
    wait_time_after_run = core.wait_time_after_run  # Now from core
    gpu_devices = core.gpu_devices  # Now from core

    # Filter and sort run variables according to the TCF file
    filtered_run_variables = filter_parameters(params.run_variables, tcf)

    if core.priority_order:
        priority_order = split_strings(core.priority_order)
        sorted_keys = sorted(
            filtered_run_variables.keys(),
            key=lambda x: (
                priority_order.index(x) if x in priority_order else len(priority_order)
            ),
        )
    else:
        sorted_keys = sorted(filtered_run_variables.keys())

    combinations = list(
        itertools.product(*(filtered_run_variables[key] for key in sorted_keys))
    )

    # Determine maximum lengths for padding
    max_lengths = {
        key: max(len(value) for value in values)
        for key, values in filtered_run_variables.items()
    }
    print(max_lengths)
    # Generate all the command arguments and command strings
    simulations = generate_all_args(
        computational_priority,
        tuflowexe,
        batch,
        tcf,
        sorted_keys,
        combinations,
        max_lengths,
        gpu_devices,
    )

    # Print parameter values
    print("Parameter values:")
    print(params)
    print()

    # Display all generated commands (for batch file)
    print("All Generated Commands (for batch file):")
    total_simulations = len(simulations)
    padding_width = len(str(total_simulations))  # Determine the number of digits needed

    for simulation in simulations:
        print(f" {simulation.index:0{padding_width}d}: {simulation.command_for_batch}")
    print()

    # Export these command strings to a batch file
    export_args(
        args_list=[sim.command_for_batch for sim in simulations],
        base_filename="commands.txt",
    )

    if core.run_simulations:
        print(f"Run simulations: {core.run_simulations}")
        total_simulations = len(simulations)
        running_processes: List[Simulation] = []
        simulation_queue = simulations.copy()
        completed_simulations = 0

        while simulation_queue or running_processes:
            # Start simulations up to the number of GPUs
            while len(running_processes) < len(gpu_devices) and simulation_queue:
                simulation = simulation_queue.pop(0)
                simulation.start_time = datetime.datetime.now()
                print(f"Running simulation {simulation.index} of {total_simulations}")
                print(" ".join(simulation.args_for_python))
                print(
                    f"Start time: {simulation.start_time.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                print(f"Assigned GPU: {simulation.assigned_gpu}")
                print("Simulation started.")

                # Start the process
                process = subprocess.Popen(
                    simulation.args_for_python,
                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                )
                # Set process priority
                psutil_priority = get_psutil_priority(computational_priority)
                process_psutil = psutil.Process(process.pid)
                process_psutil.nice(psutil_priority)

                simulation.process = process
                running_processes.append(simulation)

            # Check for completed processes
            for simulation in running_processes.copy():
                if (
                    simulation.process is not None
                    and simulation.process.poll() is not None
                ):
                    simulation.process.wait()
                    simulation.end_time = datetime.datetime.now()
                    if simulation.start_time and simulation.end_time:
                        duration = format_duration(
                            (
                                simulation.end_time - simulation.start_time
                            ).total_seconds()
                        )
                    else:
                        duration = "Unknown"
                    returncode = simulation.process.returncode
                    if returncode == 0:
                        # Print success message in green
                        print(
                            f"{colorama.Fore.GREEN}Simulation {simulation.index} completed successfully.{colorama.Style.RESET_ALL}"
                        )
                    else:
                        # Print error message in red
                        print(
                            f"{colorama.Fore.RED}Simulation {simulation.index} failed with return code {returncode}.{colorama.Style.RESET_ALL}"
                        )
                    print(
                        f"End time: {simulation.end_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    print(f"Duration: {duration}\n")
                    running_processes.remove(simulation)
                    completed_simulations += 1

            time.sleep(1)

        run_at_end(script_path=next_run_file)
    else:
        print(
            f"TUFLOW runs exported to batch file only - run_simulations: {core.run_simulations}\n"
        )

    # Wait for user input before exiting (Windows only; not tested on Linux)
    if sys.platform == "win32":
        os.system("pause")


if __name__ == "__main__":
    main()
