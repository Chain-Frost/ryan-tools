import os
import pandas as pd
import multiprocessing
from glob import iglob
from datetime import datetime
import logging
from ryan_functions.data_processing import (
    check_string_TP,
    check_string_duration,
    check_string_aep,
)

# TUFLOW changed the layout of POMM files at some point by switching rows, have updated this script for that 26/05/2021 Ryan Brook


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def calculate_pool_size(num_files) -> int:
    splits: int = max(num_files // 3, 1)
    available_cores = min(multiprocessing.cpu_count(), 20)
    calc_threads = min(available_cores - 1, splits)
    logging.info(f"Processing threads: {calc_threads}")
    return calc_threads


def processPomm(file) -> pd.DataFrame:
    logging.info(msg=f"Processing file: {file}")

    try:
        # Load the CSV data into a DataFrame
        df: pd.DataFrame = pd.read_csv(filepath_or_buffer=file, header=None)

        # Extract RunCode from the top left cell and remove the first column
        run_code: str = df.iat[0, 0]
        df_transposed = df.drop(columns=0).T  # Transpose after dropping the first column

        # Promote the first row to headers
        df_transposed.columns = pd.Index(df_transposed.iloc[0], dtype=str)

        df_transposed: pd.DataFrame = df_transposed.drop(index=df_transposed.index[0])  # type: ignore

        # Define new column names and their data types
        column_details = {
            "Type": ("Location", "string"),
            "Location": ("Time", "string"),
            "Max": ("Maximum (Extracted from Time Series)", "float"),
            "Tmax": ("Time of Maximum", "float"),
            "Min": ("Minimum (Extracted From Time Series)", "float"),
            "Tmin": ("Time of Minimum", "float"),
        }

        # Rename columns if they exist in the DataFrame
        for new_col, (old_col, dtype) in column_details.items():
            if old_col in df_transposed.columns:
                df_transposed.rename(columns={old_col: new_col}, inplace=True)
                df_transposed[new_col] = df_transposed[new_col].astype(dtype)  # type: ignore

        # Extract additional RunCode parts from the filename
        run_parts = run_code.replace("+", "_").split("_")
        for num, part in enumerate(run_parts):
            # Insert at the beginning, shifting position for each new column
            df_transposed.insert(num, f"r{num}", str(part))
            # Convert part to string to ensure correct dtype

        # Calculate AbsMax column
        df_transposed["AbsMax"] = (
            df_transposed[
                [
                    "Max",
                    "Min",
                ]
            ]
            .abs()
            .max(axis=1)
        )

        # Calculate new column with the same magnitude as AbsMax but with the sign of the datasource
        def signed_absmax(row):
            return row["Max"] if abs(row["Max"]) >= abs(row["Min"]) else row["Min"]

        df_transposed["SignedAbsMax"] = df_transposed.apply(signed_absmax, axis=1)

        # Calculate TP, Duration, and AEP using the provided functions with error handling applied to runcode
        def safe_apply(func, value):
            try:
                return func(value)
            except Exception:
                return None

        # Extract TP, Duration, and AEP from the RunCode.
        df_transposed["TP"] = safe_apply(check_string_TP, run_code)
        df_transposed["Duration"] = safe_apply(check_string_duration, run_code)
        df_transposed["AEP"] = safe_apply(check_string_aep, run_code)
        df_transposed["RunCode"] = run_code
        return df_transposed

    except Exception as e:
        print(f"Error processing file {file}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def aggregate_data(pomm_data):
    pomm_for_df = []
    for data in pomm_data:
        pomm_for_df.extend(data.to_dict("records"))
        # "records" style makes a dictionary
    # now make the dictionaries into a single dataframe
    return pd.DataFrame(pomm_for_df)


def main():
    startTime = datetime.now()
    setup_logging()
    pomm_files = [f for f in iglob("**/*POMM.csv", recursive=True) if os.path.isfile(f)]
    pool_size = calculate_pool_size(len(pomm_files))
    logging.info(f"Number of files found: {len(pomm_files)}")

    with multiprocessing.Pool(pool_size) as pool:
        pomm_data = pool.map(processPomm, pomm_files)

    df = aggregate_data(pomm_data)
    logging.info(f"POMM dataframe")
    print(df.head())
    datetime_string = datetime.now().strftime("%Y%m%d-%H%M")
    df.to_excel(f"{datetime_string}_POMM.xlsx")

    logging.info(f"Run time: {datetime.now() - startTime}")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    main()
