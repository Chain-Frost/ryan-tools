import requests
import pandas as pd
from pathlib import Path
import os
import sys
import re
import json
from typing import Optional, Tuple, List, Dict
from bs4 import BeautifulSoup  # Optional: Only if you choose to use BeautifulSoup

# =======================
# Configuration Section
# =======================


def main():
    """
    Main function to process RFFE data for catchments.
    Modify the `input_folder` and `output_folder` variables below to change directories.
    """
    # Define script directory
    script_directory: Path = Path(__file__).resolve().parent
    print(f"📂 Script directory resolved to: {script_directory}")

    # Set input and output directories
    # By default, output_folder is the same as input_folder
    input_folder: Path = script_directory
    output_folder: Path = (
        input_folder  # Change this if you want a different output directory
    )

    # Example to override output_folder by uncommenting the following line:
    # output_folder = Path(r"/path/to/custom/output/directory")

    print(f"📥 Input folder set to: {input_folder}")
    print(f"📤 Output folder set to: {output_folder}")

    # Change current working directory to input_folder
    try:
        os.chdir(input_folder)
        print(f"🔄 Changed working directory to: {input_folder}")
    except Exception as e:
        print(f"❌ Failed to change directory: {e}")
        sys.exit(1)

    # Define the path to the input CSV file
    input_csv_path = input_folder / "input_catchments.csv"
    if not input_csv_path.exists():
        print(f"❌ Input CSV file not found: {input_csv_path}")
        sys.exit(1)

    print("📥 Reading input CSV file...")
    try:
        ctchdata = pd.read_csv(input_csv_path)
        print("✅ Input CSV loaded successfully.")
    except Exception as e:
        print(f"❌ Failed to read input CSV: {e}")
        sys.exit(1)

    total_catchments = len(ctchdata)
    print(f"📊 Total catchments to process: {total_catchments}")

    rffes_results = pd.DataFrame()
    all_catchments_results = pd.DataFrame()

    # Initialize list to track failed catchments
    failed_catchments: list[dict[str, any]] = []

    for idx, row in ctchdata.iterrows():
        catchment_number = idx + 1
        catchment_name = row.get("Catchment", f"Catchment_{catchment_number}")
        print(
            f"🔄 Processing catchment {catchment_number}/{total_catchments}: {catchment_name}"
        )
        rffe_results_df, all_catchments_df, error_info = process_catchment(
            lato=row.get("OutletY"),
            lono=row.get("OutletX"),
            latc=row.get("CentroidY"),
            lonc=row.get("CentroidX"),
            area=row.get("AreaKm2"),
            name=catchment_name,
            out_folder=output_folder,
        )
        if error_info:
            failed_catchments.append(
                {
                    "Catchment": catchment_name,
                    "OutletY": row.get("OutletY"),
                    "OutletX": row.get("OutletX"),
                    "CentroidY": row.get("CentroidY"),
                    "CentroidX": row.get("CentroidX"),
                    "AreaKm2": row.get("AreaKm2"),
                    "Error": error_info,
                }
            )
            print(f"⚠️ Catchment {catchment_name} failed with error: {error_info}")
        else:
            if not rffe_results_df.empty:
                rffes_results = pd.concat(
                    [rffes_results, rffe_results_df], ignore_index=True
                )
            else:
                print(f"⚠️ No 'results' data returned for catchment: {catchment_name}")

            if not all_catchments_df.empty:
                all_catchments_results = pd.concat(
                    [all_catchments_results, all_catchments_df], ignore_index=True
                )
            else:
                print(
                    f"⚠️ No 'allCatchmentResults' data returned for catchment: {catchment_name}"
                )

    # Define the output CSV paths
    output_results_csv_path = output_folder / "rffe_results.csv"
    output_all_catchments_csv_path = output_folder / "all_catchment_results.csv"
    failed_catchments_csv_path = output_folder / "failed_catchments.csv"

    print(f"💾 Saving aggregated 'results' data to: {output_results_csv_path}")
    try:
        rffes_results.to_csv(output_results_csv_path, index=False)
        print("✅ 'results' data saved successfully.")
    except Exception as e:
        print(f"❌ Failed to save 'results' CSV: {e}")
        sys.exit(1)

    print(
        f"💾 Saving aggregated 'allCatchmentResults' data to: {output_all_catchments_csv_path}"
    )
    try:
        all_catchments_results.to_csv(output_all_catchments_csv_path, index=False)
        print("✅ 'allCatchmentResults' data saved successfully.")
    except Exception as e:
        print(f"❌ Failed to save 'allCatchmentResults' CSV: {e}")
        sys.exit(1)

    # Save failed catchments if any
    if failed_catchments:
        print(f"💾 Saving failed catchments to: {failed_catchments_csv_path}")
        try:
            failed_df = pd.DataFrame(failed_catchments)
            failed_df.to_csv(failed_catchments_csv_path, index=False)
            print("✅ Failed catchments data saved successfully.")
        except Exception as e:
            print(f"❌ Failed to save failed catchments CSV: {e}")

        print("\n❌ Summary of Failed Catchments:")
        for failure in failed_catchments:
            print(f" - Catchment: {failure['Catchment']}")
            print(
                f"   Parameters: OutletY={failure['OutletY']}, OutletX={failure['OutletX']}, "
                f"CentroidY={failure['CentroidY']}, CentroidX={failure['CentroidX']}, "
                f"AreaKm2={failure['AreaKm2']}"
            )
            print(f"   Error: {failure['Error']}\n")
    else:
        print("✅ All catchments processed successfully without errors.")


def fetch_rffe(
    name: str, lono: float, lato: float, lonc: float, latc: float, area: float
) -> Optional[requests.Response]:
    """
    Fetches RFFE data from the specified URL with given parameters.

    Args:
        name (str): Name of the catchment.
        lono (float): Longitude of the outlet.
        lato (float): Latitude of the outlet.
        lonc (float): Longitude of the centroid.
        latc (float): Latitude of the centroid.
        area (float): Area of the catchment in Km².

    Returns:
        requests.Response object if successful, None otherwise.
    """
    print("🔄 Fetching RFFE data...")
    rffe_url = "http://rffe.arr-software.org"  # Update if necessary
    payload: dict[str, str] = {
        "catchment_name": name,  # Dynamic catchment name
        "lato": str(lato),
        "lono": str(lono),
        "latc": str(latc),
        "lonc": str(lonc),
        "area": str(area),
    }
    headers = {
        "Content-Type": "application/json",  # Adjust based on API requirements
        # "Authorization": "Bearer YOUR_API_KEY",  # Uncomment and set if needed
    }
    print(f"Payload: {payload}")
    try:
        response = requests.post(
            rffe_url,
            data=payload,
        )
        response.raise_for_status()  # Raise error for HTTP errors (4xx, 5xx)

        # Check if the response contains the expected script tags
        if "<script>" in response.text and "results =" in response.text:
            print("✅ RFFE data fetched successfully.")
            return response
        else:
            print(f"❗ Unexpected response format, {response}")
            print(f"Content-Type: {response.headers.get('Content-Type')}")
            print(f"Content: {response.text}")
            return None
    except requests.RequestException as e:
        print(f"❌ Error fetching RFFE data: {e}")
        return None


def clean_rffe(rffe_results: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and parses the RFFE response text into two pandas DataFrames:
    one for 'results' and another for 'allCatchmentResults'.
    Extracts the JSON data from embedded JavaScript using BeautifulSoup.

    Args:
        rffe_results (str): The raw HTML response from RFFE.

    Returns:
        Tuple containing:
            - DataFrame for 'results'
            - DataFrame for 'allCatchmentResults'
    """
    print("🧹 Cleaning RFFE data...")

    # Initialize empty DataFrames
    df_results = pd.DataFrame()
    df_all_catchments = pd.DataFrame()

    # Parse the HTML content
    soup = BeautifulSoup(rffe_results, "html.parser")

    # Find all <script> tags
    scripts = soup.find_all("script")

    # Initialize variables to store JSON strings
    results_str = None
    all_catchments_str = None

    # Iterate through script tags to find the ones containing our variables
    for script in scripts:
        if script.string:
            if "results =" in script.string:
                match = re.search(
                    r"results\s*=\s*(\[\{.*?\}\]);", script.string, re.DOTALL
                )
                if match:
                    results_str = match.group(1)
            if "allCatchmentResults =" in script.string:
                match = re.search(
                    r"allCatchmentResults\s*=\s*(\[\{.*?\}\]);",
                    script.string,
                    re.DOTALL,
                )
                if match:
                    all_catchments_str = match.group(1)

    # Process 'results'
    if results_str:
        try:
            # Replace single quotes with double quotes
            results_str_json = results_str.replace("'", '"')
            # Parse JSON
            results_json = json.loads(results_str_json)
            # Convert to DataFrame
            df_results = pd.DataFrame(results_json)
            print("✅ 'results' data cleaned and parsed successfully.")
        except json.JSONDecodeError as e:
            print(f"❌ JSON decoding failed for 'results': {e}")
        except Exception as e:
            print(f"❌ Unexpected error parsing 'results': {e}")
    else:
        print("❗ 'results' variable not found in the response.")

    # Process 'allCatchmentResults'
    if all_catchments_str:
        try:
            # Replace single quotes with double quotes
            all_catchments_str_json = all_catchments_str.replace("'", '"')
            # Parse JSON
            all_catchments_json = json.loads(all_catchments_str_json)
            # Convert to DataFrame
            df_all_catchments = pd.DataFrame(all_catchments_json)
            print("✅ 'allCatchmentResults' data cleaned and parsed successfully.")
        except json.JSONDecodeError as e:
            print(f"❌ JSON decoding failed for 'allCatchmentResults': {e}")
        except Exception as e:
            print(f"❌ Unexpected error parsing 'allCatchmentResults': {e}")
    else:
        print("❗ 'allCatchmentResults' variable not found in the response.")

    return df_results, df_all_catchments


def save_results_to_files(
    catchment: str, folder_path: Path, rffe_response: requests.Response
):
    """
    Saves the raw RFFE response text to a file named '{catchment}_rffe.txt' in the specified folder.

    Args:
        catchment (str): Name of the catchment.
        folder_path (Path): Path to the output folder.
        rffe_response (requests.Response): The HTTP response from RFFE.
    """
    file_path = folder_path / f"{catchment}_rffe.txt"
    try:
        with file_path.open("w", encoding="utf-8") as file:
            file.write(rffe_response.text)
        print(f"💾 Saved RFFE response to {file_path}")
    except IOError as e:
        print(f"❌ Error saving RFFE response to file: {e}")


def process_catchment(
    lato: float,
    lono: float,
    latc: float,
    lonc: float,
    area: float,
    name: str,
    out_folder: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, Optional[str]]:
    """
    Processes a single catchment: fetches data, saves raw response,
    cleans data, and returns two DataFrames (results and allCatchmentResults).

    Args:
        lato (float): Latitude of the outlet.
        lono (float): Longitude of the outlet.
        latc (float): Latitude of the centroid.
        lonc (float): Longitude of the centroid.
        area (float): Area of the catchment in Km².
        name (str): Name of the catchment.
        out_folder (Path): Output folder path.

    Returns:
        Tuple containing:
            - DataFrame for 'results'
            - DataFrame for 'allCatchmentResults'
            - Error message if any, else None
    """
    print(f"🔍 Processing catchment: {name}")
    try:
        rffe_response = fetch_rffe(name, lono, lato, lonc, latc, area)
        if rffe_response is None:
            return pd.DataFrame(), pd.DataFrame(), "Failed to fetch RFFE data."

        save_results_to_files(name, out_folder, rffe_response)
        rffe_results_df, all_catchments_df = clean_rffe(rffe_response.text)

        if not rffe_results_df.empty:
            rffe_results_df["Catchment"] = name
        else:
            print(f"⚠️ No 'results' data for catchment: {name}")

        if not all_catchments_df.empty:
            all_catchments_df["Catchment"] = name
        else:
            print(f"⚠️ No 'allCatchmentResults' data for catchment: {name}")

        return rffe_results_df, all_catchments_df, None

    except Exception as e:
        error_message = str(e)
        print(
            f"❌ An error occurred while processing catchment {name}: {error_message}"
        )
        return pd.DataFrame(), pd.DataFrame(), error_message


# =======================
# Main Guard
# =======================

if __name__ == "__main__":
    main()
