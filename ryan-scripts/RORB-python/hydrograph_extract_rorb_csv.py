import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import re
import sys
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# --- User Configuration ---
# Specify the hydrograph to process. Set to None to trigger dynamic selection.
selected_hydrograph = "Calculated hydrograph:  Outlet"  # Example: "Calculated hydrograph: Outlet"
# --- End of User Configuration ---


def extract_metadata_from_filename(file_name):
    """
    Extract Creek Name, AEP, Duration, and TP from the filename.
    Expected filename patterns:
      - PinarraCreek_01_ aep1_du12hourtp2.csv
      - Boolgeeda_03_ aep1_du12hourtp10.csv
    Returns a tuple: (creek_name, aep, duration, tp)
    """
    # Regex pattern to extract Creek Name, AEP, Duration, and TP with optional spaces.
    # Reader's guide:
    #   * ``^`` and ``$`` lock the pattern to the entire file name.
    #   * ``(?P<creek_name>[^_]+)`` collects everything up to the first underscore so ``PinarraCreek`` is captured.
    #   * ``_\d+_\s*`` skips the run number that follows the creek name. ``\s*`` allows the observed space before ``aep``.
    #   * ``aep(?P<aep>\d+)`` grabs the digits after ``aep`` (e.g. ``aep5``).
    #   * ``du(?P<duration>\d+)hour`` matches durations such as ``du12hour``.
    #   * ``tp(?P<tp>\d+)`` captures the temporal pattern (``tp10``).
    # Example: ``Boolgeeda_03_ aep1_du12hourtp10.csv`` matches with
    #   creek_name = ``Boolgeeda``
    #   aep = ``1``
    #   duration = ``12``
    #   tp = ``10``
    pattern = re.compile(
        r"^(?P<creek_name>[^_]+)_\d+_\s*aep(?P<aep>\d+)_du(?P<duration>\d+)hourtp(?P<tp>\d+)",
        re.IGNORECASE,
    )
    match = pattern.search(file_name)
    if match:
        creek_name = match.group("creek_name")
        aep = f"aep{match.group('aep')}"
        duration = f"du{match.group('duration')}hour"
        tp = f"tp{match.group('tp')}"
        return creek_name, aep, duration, tp
    else:
        return None, None, None, None


def import_data(script_directory, selected_combinations, selected_hydrograph):
    """
    Import and aggregate hydrograph data from CSV files.

    Parameters:
        script_directory (str): Path to the script directory.
        selected_combinations (list): List of tuples containing AEP and TP combinations.
        selected_hydrograph (str or None): Specific hydrograph to process.

    Returns:
        pd.DataFrame: Combined DataFrame containing all hydrograph data.
        dict: Mapping of AEP codes to percentage labels.
        str: Creek name extracted from filenames.
    """
    # Precompile regex patterns for efficiency. The pattern created for each combination looks like
    # ``aep10_du12hourtp1`` (with the actual AEP and TP plugged in). The ``\b`` word boundary ensures
    # we match the complete code while still allowing extra text afterwards, such as ``_20240101.csv``.
    # Example: ``PinarraCreek_01_ aep1_du12hourtp2.csv`` matches the pattern for ("aep1", "tp2").
    regex_patterns = {
        (aep, tp): re.compile(rf"{re.escape(aep)}_du12hour{re.escape(tp)}\b", re.IGNORECASE)
        for aep, tp in selected_combinations
    }

    # Initialize a list to store all hydrograph data
    combined_data = []

    # Initialize a set to collect unique AEPs for mapping
    unique_aep_set = set()

    # Initialize a set to collect unique creek names
    unique_creek_set = set()

    # Iterate through all files in the directory
    for file_name in os.listdir(script_directory):
        # Process only .csv files
        if not file_name.lower().endswith(".csv"):
            continue  # Skip non-CSV files

        # Check if the file matches any of the selected combinations using regex
        matched = False
        matched_aep = None
        matched_tp = None
        for (aep, tp), pattern in regex_patterns.items():
            if pattern.search(file_name):
                matched = True
                matched_aep = aep
                matched_tp = tp
                break  # Found a matching pattern

        if not matched:
            continue  # Skip files that do not match any pattern

        file_path = os.path.join(script_directory, file_name)
        print(f"Processing file: {file_name}")

        try:
            # Read the CSV file using pandas with comma delimiter and skip first two lines
            df = pd.read_csv(file_path, sep=",", skiprows=2)
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
            continue  # Skip to the next file

        # Strip whitespace from column names
        df.columns = [col.strip() for col in df.columns]

        # Identify hydrograph columns (excluding 'Inc' and 'Time (hrs)')
        hydrograph_columns = [col for col in df.columns if col not in ["Inc", "Time (hrs)"]]

        if len(hydrograph_columns) == 0:
            print(f"No hydrograph columns found in {file_name}. Skipping.")
            continue  # Skip files without hydrograph data

        elif len(hydrograph_columns) == 1:
            # Only one hydrograph present; proceed to process it
            selected_column = hydrograph_columns[0]

        else:
            # Multiple hydrographs detected
            print(f"Multiple hydrographs found in {file_name}:")
            for idx, col in enumerate(hydrograph_columns, start=1):
                print(f"  {idx}. {col}")

            if selected_hydrograph is None:
                print("\nError: Multiple hydrographs detected.")
                print(
                    "Please specify the desired hydrograph by setting the 'selected_hydrograph' variable in the script."
                )
                print("Example: selected_hydrograph = 'Calculated hydrograph: Outlet'\n")
                sys.exit(1)  # Exit the script with an error code

            elif selected_hydrograph not in hydrograph_columns:
                print(f"\nError: The specified hydrograph '{selected_hydrograph}' is not found in {file_name}.")
                print(
                    "Please update the 'selected_hydrograph' variable with a valid hydrograph name from the list above.\n"
                )
                sys.exit(1)

            else:
                selected_column = selected_hydrograph  # Use the user-specified hydrograph

        # Extract relevant data
        try:
            hydrograph_data = df[["Inc", "Time (hrs)", selected_column]].copy()
            hydrograph_data.rename(columns={selected_column: "Flow"}, inplace=True)
        except KeyError as e:
            print(f"Error processing columns in {file_name}: {e}")
            continue  # Skip to the next file

        # Convert data types
        try:
            hydrograph_data["Inc"] = hydrograph_data["Inc"].astype(int)
            hydrograph_data["Time (hrs)"] = hydrograph_data["Time (hrs)"].astype(float)
            hydrograph_data["Flow"] = hydrograph_data["Flow"].astype(float)
        except ValueError as e:
            print(f"Data type conversion error in {file_name}: {e}")
            continue  # Skip to the next file

        # Rename columns for consistency
        hydrograph_data.rename(columns={"Time (hrs)": "Time"}, inplace=True)

        # Extract metadata from filename
        creek_name, aep, duration, tp = extract_metadata_from_filename(file_name)
        if not all([creek_name, aep, duration, tp]):
            print(f"Warning: Unable to extract metadata from {file_name}. Setting as 'Unknown'.")
            creek_name = creek_name or "Unknown_Creek"
            aep = aep or "Unknown_AEP"
            duration = duration or "Unknown_Duration"
            tp = tp or "Unknown_TP"

        # Add metadata columns
        hydrograph_data["AEP"] = aep
        hydrograph_data["Duration"] = duration
        hydrograph_data["TP"] = tp
        hydrograph_data["Source Filename"] = file_name
        hydrograph_data["Creek Name"] = creek_name

        # Collect unique AEPs and Creek Names
        unique_aep_set.add(aep)
        unique_creek_set.add(creek_name)

        # Append to combined data
        combined_data.append(hydrograph_data)

    # Combine all data into a single DataFrame
    if combined_data:
        combined_df = pd.concat(combined_data, ignore_index=True)
        print("\nAll hydrograph data combined into a single DataFrame.")
    else:
        print("No matching hydrograph data found.")
        sys.exit(0)

    # Create AEP Mapping (e.g., "aep10" -> "10% AEP")
    aep_mapping = {}
    for aep in unique_aep_set:
        # ``r"aep(\d+)"`` isolates the digits following ``aep`` so ``aep20`` becomes ``20% AEP`` in the legend labels.
        match = re.match(r"aep(\d+)", aep, re.IGNORECASE)
        if match:
            percentage = match.group(1)
            aep_mapping[aep] = f"{percentage}% AEP"
        else:
            aep_mapping[aep] = aep  # Default to original if pattern doesn't match

    # Assuming all files belong to the same creek, extract it
    if len(unique_creek_set) == 1:
        creek_name_final = unique_creek_set.pop()
    else:
        creek_name_final = "Multiple_Creeks"

    return combined_df, aep_mapping, creek_name_final


def create_plot(combined_df, aep_mapping, creek_name, script_directory):
    """
    Create and save a hydrograph plot based on the combined DataFrame.

    Parameters:
        combined_df (pd.DataFrame): Combined hydrograph data.
        aep_mapping (dict): Mapping of AEP codes to percentage labels.
        creek_name (str): Name of the creek extracted from filenames.
        script_directory (str): Path to the script directory.
    """
    try:
        plt.figure(figsize=(12, 8))

        # Sort AEPs based on percentage for ordered legend
        sorted_aeps = sorted(
            aep_mapping.keys(),
            # ``r"\d+"`` pulls the first number from labels such as ``aep5`` so the legend appears in numerical order.
            key=lambda x: (int(re.search(r"\d+", x).group()) if re.search(r"\d+", x) else 0),
        )

        for aep in sorted_aeps:
            label = aep_mapping.get(aep, aep)
            aep_data = combined_df[combined_df["AEP"] == aep]
            plt.plot(aep_data["Time"], aep_data["Flow"], label=label)

            # Find the maximum flow point
            max_flow = aep_data["Flow"].max()
            max_flow_row = aep_data[aep_data["Flow"] == max_flow].iloc[0]
            max_flow_time = max_flow_row["Time"]

            # Format the max flow value
            if max_flow >= 1000:
                flow_label = f"{int(max_flow):,}"
            else:
                flow_label = f"{max_flow:.3g}"

            # Plot the max flow point
            plt.plot(max_flow_time, max_flow, "x", markersize=10, color="red")
            plt.text(
                max_flow_time,
                max_flow,
                f"{flow_label}",
                fontsize=10,  # Increased font size
                verticalalignment="bottom",
            )

        # Set axis labels and bounds
        plt.xlabel("Time (hours)", fontsize=14)
        plt.ylabel("Flow (m³/s)", fontsize=14)
        plt.title("Hydrograph Flow vs Time", fontsize=16)
        plt.legend(fontsize=12)  # Removed legend title
        plt.grid(True)
        plt.xlim([0, 75])  # Set x-axis bounds
        plt.ylim(bottom=0)  # Set y-axis to start from 0

        # Enhance x-axis ticks if necessary
        plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

        # Adjust layout for better spacing
        plt.tight_layout()

        # Save the plot to a PNG file
        current_time = datetime.now().strftime("%Y%m%d_%H%M")
        plot_file_name = f"hydrograph_plot_{creek_name}_{current_time}.png"
        plot_file_path = os.path.join(script_directory, plot_file_name)
        plt.savefig(plot_file_path, dpi=300)
        plt.close()

        print(f"Hydrograph plot saved to: {plot_file_path}")
    except Exception as e:
        print(f"Error creating or saving the plot: {e}")
        sys.exit(1)


def main():
    # Get the directory where the script is located
    script_directory = Path(__file__).absolute().parent

    # Define specific combinations of AEP and TP to process
    selected_combinations = [
        ("aep50", "tp5"),
        ("aep20", "tp3"),
        ("aep10", "tp1"),
        ("aep5", "tp1"),
        ("aep2", "tp1"),
        ("aep1", "tp3"),
    ]

    # Import and aggregate data
    combined_df, aep_mapping, creek_name = import_data(script_directory, selected_combinations, selected_hydrograph)

    # Save combined data to a single-sheet Excel file
    try:
        # Get current date and time
        current_time = datetime.now().strftime("%Y%m%d_%H%M")

        # Define the output file name
        output_file_name = f"combined_hydrograph_data_{creek_name}_{current_time}.xlsx"
        output_file_path = os.path.join(script_directory, output_file_name)

        # Save to Excel with a single sheet
        combined_df.to_excel(output_file_path, index=False, sheet_name="Hydrograph Data")
        print(f"Combined hydrograph data saved to: {output_file_path}")
    except Exception as e:
        print(f"Error saving combined Excel file: {e}")
        sys.exit(1)

    # Create and save the plot
    create_plot(combined_df, aep_mapping, creek_name, script_directory)


if __name__ == "__main__":
    main()
    input("Press Enter to exit...")
