import requests
import os
from loguru import logger
from pathlib import Path
import pandas as pd

script_directory: Path = Path(__file__).absolute().parent
script_directory = Path(r"Q:\BGER\PER\RPRT\ryan-tools\tests\test_data")


# input_catchments.csv - column order not important I think
# Catchment,AreaKm2,OutletX,OutletY,CentroidY,CentroidX
# 10,338.7673404,120.4806,-23.386,-23.4811,120.5775
# 20,576.3559263,120.4358,-23.3459,-23.4877,120.5328

# Constants
RFFE_URL = "http://rffe.arr-software.org"
INPUT_CATCHMENTS_FILENAME = "input_catchments.csv"
OUTPUT_RFFE_FILENAME = "rffe.csv"


def fetch_rffe(lono: float, lato: float, lonc: float, latc: float, area: float) -> requests.Response:
    """
    Fetch RFFE data from the specified URL.

    Args:
        lono (float): Longitude of the outlet.
        lato (float): Latitude of the outlet.
        lonc (float): Longitude of the centroid.
        latc (float): Latitude of the centroid.
        area (float): Area in Km².

    Returns:
        requests.Response: The HTTP response from the RFFE server.
    """
    logger.info("Fetching RFFE data...")
    payload = {
        "catchment_name": "catchment1",
        "lato": lato,
        "lono": lono,
        "latc": latc,
        "lonc": lonc,
        "area": area,
    }
    try:
        response = requests.post(RFFE_URL, data=payload)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"Error fetching RFFE data: {e}")
        raise


def clean_rffe(rffe_results: str) -> pd.DataFrame:
    """
    Clean and parse RFFE results into a pandas DataFrame.

    Args:
        rffe_results (str): The raw text response from the RFFE server.

    Returns:
        pd.DataFrame: Cleaned DataFrame with RFFE results.
    """
    logger.debug("Cleaning RFFE results...")
    try:
        # Extract the JSON-like part from the response
        results_str = rffe_results.split("results=[{")[1].split("}]")[0]
        # Replace '},{' with '};{' to split records
        records = results_str.replace("},{", "};{").replace("{", "").replace("}", "").split(";")

        # Parse each record into a dictionary
        data = []
        for record in records:
            entry = {}
            for item in record.split(","):
                key, value = item.split(":")
                entry[key.strip()] = float(value.replace("'", ""))
            data.append(entry)

        df = pd.DataFrame(data)
        logger.debug("RFFE results cleaned successfully.")
        return df
    except (IndexError, ValueError) as e:
        logger.error(f"Error cleaning RFFE results: {e}")
        raise


def save_results_to_file(catchment: str, folder_path: str, rffe_response: requests.Response) -> None:
    """
    Save the RFFE response text to a file.

    Args:
        catchment (str): Name of the catchment.
        folder_path (str): Path to the folder where the file will be saved.
        rffe_response (requests.Response): The HTTP response from the RFFE server.
    """
    filename = f"{catchment}_rffe.txt"
    file_path = os.path.join(folder_path, filename)
    try:
        with open(file_path, "w") as file:
            file.write(rffe_response.text)
        logger.info(f"RFFE results saved to {file_path}")
    except IOError as e:
        logger.error(f"Error saving RFFE results to file: {e}")
        raise


def process_catchment(row: pd.Series, out_folder: str) -> pd.DataFrame:
    """
    Process a single catchment: fetch RFFE data, save it, and return the cleaned DataFrame.

    Args:
        row (pd.Series): A row from the catchments DataFrame.
        out_folder (str): Output folder path.

    Returns:
        pd.DataFrame: Cleaned RFFE data with catchment name.
    """
    name = row["Catchment"]
    area = row["AreaKm2"]
    lato = row["OutletY"]
    lono = row["OutletX"]
    latc = row["CentroidY"]
    lonc = row["CentroidX"]

    logger.info(f"Processing catchment '{name}'")

    rffe_response = fetch_rffe(lono, lato, lonc, latc, area)
    save_results_to_file(name, out_folder, rffe_response)

    rffe_df = clean_rffe(rffe_response.text)
    rffe_df["Catchment"] = name
    return rffe_df


def main(source_folder: str) -> None:
    """
    Main function to process all catchments and save the consolidated RFFE results.

    Args:
        source_folder (str): Path to the source folder containing the input CSV.
    """
    input_csv_path = os.path.join(source_folder, INPUT_CATCHMENTS_FILENAME)

    try:
        catchments_df = pd.read_csv(input_csv_path)
        logger.info(f"Loaded {len(catchments_df)} catchments from {input_csv_path}")
    except FileNotFoundError as e:
        logger.error(f"Input CSV file not found: {e}")
        return
    except pd.errors.ParserError as e:
        logger.error(f"Error parsing input CSV file: {e}")
        return

    rffe_results = []

    for idx, row in catchments_df.iterrows():
        logger.info(f"Working on {idx + 1} of {len(catchments_df)}: {row['Catchment']}")
        try:
            rffe_df = process_catchment(row, source_folder)
            rffe_results.append(rffe_df)
        except Exception as e:
            logger.error(f"Failed to process catchment '{row['Catchment']}': {e}")

    if rffe_results:
        consolidated_df = pd.concat(rffe_results, ignore_index=True)
        output_csv_path = os.path.join(source_folder, OUTPUT_RFFE_FILENAME)
        try:
            consolidated_df.to_csv(output_csv_path, index=False)
            logger.info(f"Consolidated RFFE results saved to {output_csv_path}")
        except IOError as e:
            logger.error(f"Error saving consolidated RFFE results: {e}")
    else:
        logger.warning("No RFFE results to save.")


if __name__ == "__main__":
    os.chdir(script_directory)
    main(script_directory)
