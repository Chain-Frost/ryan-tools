import logging
from pathlib import Path
import pandas as pd
import os
from glob import iglob
import csv
from datetime import datetime
import multiprocessing
import numpy as np
import pprint
import re

# Configure logging at the start of your script
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ryan Brook March 2024. adjusted for shorter 0360m and 01.0p, (00360m, 01.00p)
# assumes we can use POMM.csv, using TF2023


def make_file_list(searchString):
    textString = r"**/*" + searchString
    print(f"Recursively searching for {textString} files - can take a while")
    file_list = [f for f in iglob(textString, recursive=True) if os.path.isfile(f)]
    print(file_list)
    print("")
    return file_list


def calcNumWorkersCores(numFiles):
    splits = numFiles // 3
    if multiprocessing.cpu_count() <= 2:
        cores = min(2, splits)
    elif multiprocessing.cpu_count() <= 4:
        cores = min(multiprocessing.cpu_count(), splits)
    elif multiprocessing.cpu_count() <= 20:
        cores = min(multiprocessing.cpu_count() - 1, splits)
    else:
        cores = min(20, splits)
    return max(cores, 1)


""" def max_from_single(podf): #used for getting max from TF2016 _PO.csv
    # ['Type', 'Location', 'Value'] #time dropped in the call
    maxData = podf.groupby(['Type', 'Location'])[
        'Value'].agg(min='Min', max='Max')
    #minData = podf.groupby(['Type', 'Location'],as_index=False)['Value'].min()
    maxData['abs_min'] = abs(maxData['Min'])
    maxData['abs_max'] = maxData[["abs_min", "Max"]].max(axis=1)
    # print(maxData)
    maxData.drop(['abs_min'], axis=1, inplace=True)
    maxData.reset_index(inplace=True)
    return maxData """


def load_data_and_transpose(data, column_info):
    # Convert the data into a DataFrame
    df = pd.DataFrame(data)
    # Drop the first two columns - we only want the POMM table data
    df = df.drop([0, 1], axis=1)
    # Transpose the DataFrame
    df = df.transpose()
    # Extract column names from column_info
    columns = [col for col, _ in column_info.items()]
    # Set the column names and data types
    df.columns = columns
    for col, dtype in column_info.items():
        df[col] = df[col].astype(dtype)
    """  
    column_info  should be of the form: {
        'Type': 'string',
        'Location': 'string',
        'Max': 'float',
        'Tmax': 'float',
        'Min': 'float',
        'Tmin': 'float'
    } """
    # this was the style in 2024.
    # The row labels output by tuflow are wrong. Location is row below, and there is not time. it should be type.

    return df


def processPomm_extended(file, positionsDict):
    print(file)
    with open(file, "r") as pomm:
        reader = csv.reader(pomm)
        pommList = list(reader)

    pommData = load_data_and_transpose(pommList, positionsDict)
    # we use the name from in the file rather than the csv filename in case there was some dodginess
    pommData["internalName"] = pommList[0][0]
    # this is the other part of cell A1 that had the source TCF
    pommData["tcf_path"] = pommList[1][0]
    pommData["orig_po_path"] = pommList[2][0]
    # pommData['file'] = os.path.basename(file)
    # pommData['path'] = file

    pommData["RunCode"] = pommList[0][0]
    for num, runPart in enumerate(pommList[0][0].replace("+", "_").split("_")):
        pommData["r" + str(num)] = runPart
    # Calculate the absolute largest value for each row
    pommData["abs_max"] = pommData.apply(lambda row: max(abs(row["Max"]), abs(row["Min"])), axis=1)

    col_names = {
        "abs_max": "float64",
        "path": "string",
        "file": "string",
        "tcf_path": "string",
        "orig_po_path": "string",
        "internalName": "string",
    }
    for k in col_names:
        if k in pommData.columns:
            pommData = pommData.astype({k: col_names[k]})

    file_parts = process_PO_csv_name(file)
    for p in file_parts:
        pommData[p] = file_parts[p]

    return pommData


def check_string_TP(string):
    # Adjusted pattern to find 'TP' followed by exactly two digits, anywhere in the string
    pattern = r"TP(\d{2})"
    match = re.search(pattern, string)
    if match:
        return match.group(1)  # This should capture just the two digits following 'TP'
    else:
        return None


def check_string_duration(string):
    """The pattern r'(?:_|^)([^_]{5}m)(?:_|$)' is looking for a sequence of 5 characters that are not underscores, followed by the letter 'm'. This sequence must be preceded and followed by an underscore or the start/end of the string.
    If a match is found, the function returns the matched substring with underscores and 'm' removed. Otherwise, it returns None (ChatGPT3.5)
    """

    # TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv
    # pattern = r"(?:_|^)([^_]{5}m)(?:_|$)"
    # Adjusted pattern to allow for + as a separator in addition to _
    # pattern = r"(?:[_+]|^)([^_+]{5}m)(?:[_+]|$)"
    # adjusted to allow 360m, 0360m and 00360m style
    pattern = r"(?:[_+]|^)(\d{3,5}[mM])(?:[_+]|$)"
    match = re.search(pattern, string, re.IGNORECASE)
    if match:
        return match.group(0).replace("_", "").replace("m", "")
    else:
        return None


def check_string_aep(string):
    """The pattern r'(?:_|^)([^_]{5}p)(?:_|$)' is looking for a sequence of 5 characters that are not underscores, followed by the letter 'p'.
    This sequence must be preceded and followed by an underscore or the start/end of the string. (ChatGPT3.5)
    """

    # TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv
    # Adjusted pattern to allow for + as a separator in addition to _
    # adjusted to take both 01.0p and 01.00p
    pattern = r"(?:[_+]|^)(\d{2}\.\d{1,2}p)(?:[_+]|$)"
    # pattern = r"(?:[_+]|^)([^_+]{5}p)(?:[_+]|$)"
    # pattern = r"(?:_|^)([^_]{5}p)(?:_|$)"
    match = re.search(pattern, string, re.IGNORECASE)
    if match:
        return match.group(0).replace("_", "").replace("p", "")
    else:
        return None


def get_runcode_from_filename(PO_csv_filename):
    # Determine whether the file ends with _PO.csv or _POMM.csv
    basename = os.path.basename(PO_csv_filename)
    if basename.endswith("_PO.csv"):
        Runcode = basename[:-7]  # -7 to remove '_PO.csv'
    elif basename.endswith("_POMM.csv"):
        Runcode = basename[:-9]  # -9 to remove '_POMM.csv'
    else:
        logging.warning(f"Unrecognized file format: {PO_csv_filename}")
        return None  # Or handle the error as appropriate

    logging.info(f"{Runcode} : {PO_csv_filename}")
    return basename, Runcode


def process_PO_csv_name(PO_csv_filename):
    # now process the names of the PO csv file to get the params of the file
    basename = os.path.basename(PO_csv_filename)
    Runcode = os.path.basename(PO_csv_filename)[:-9].replace("+", "_")  # 9 POMM, 7 PO
    print(f"{Runcode} : {PO_csv_filename}")
    tidy_PO_csv_filename = Runcode

    # developed based on this style (TUFLOW ARR default)
    # TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv

    temp_dict = {"Runcode": Runcode}

    # paramsDF['IL']=IL
    # paramsDF['CL']=CL
    # paramsDF['ROC']=ROC

    temp_dict["TPat"] = check_string_TP(PO_csv_filename)
    temp_dict["Duration"] = check_string_duration(PO_csv_filename)
    temp_dict["AEP"] = check_string_aep(PO_csv_filename)
    temp_dict["file"] = basename
    dirs = os.path.dirname(PO_csv_filename)
    temp_dict["folder"] = dirs
    temp_dict["csv"] = PO_csv_filename
    replace_dict = {
        f"TP{temp_dict['TPat']}": "",
        f"{temp_dict['Duration']}m": "",
        f"{temp_dict['AEP']}p": "",
        "+": "_",
        "___": "_",
        "__": "_",
    }
    TrimRC = Runcode
    for k, v in replace_dict.items():
        TrimRC = TrimRC.replace(k, v)
    temp_dict["TrimRC"] = TrimRC

    # split the run code up by '_'
    # old style -RunHeader=['R'+str(r) for r in range(1,len(max(RunTable,key=len)))] # We don't know how many columns there will be so use this code
    # now use pandas
    # for elem in enumerate(Runcode.split('_'), start=1):
    #     temp_dict[f'R{elem[0]}'] = elem[1]

    # to get some bits out of the folder names - advanced work
    # paramsDF['method']=dirs.split('\\')[0]
    # paramsDF['model']=None #dirs.split('\\')[1]

    print(temp_dict)
    return temp_dict


# this function will extract min, max and median peak for peaks in the dataframe and also return corresponding event/scenario
def stats(thinned_df: pd.DataFrame, statcol: str, tpcol: str, durcol: str) -> tuple:
    max_stats_dict = {}
    bin_stats_list = []  # list of dicts
    tracking_median = -9999
    count_bin = 0
    for durgrp in thinned_df.groupby(by=durcol):
        ensemblestat = durgrp[1]
        r = len(ensemblestat.index)
        medianpos = int(r / 2)
        # The median position calculated as int(len(r) / 2) is based on 0 indexing, which is the standard indexing method in Python.
        # Therefore, in a range from 0 to 10 (range(0, 11)), the median position of 5 corresponds to the 6th element in the sequence, since indexing starts at 0.
        # round up if odd, +1 if even. so it assumes sorted in ascending order.
        # sort ensemblestat by statcol, ascending
        ensemblestat.sort_values(statcol, inplace=True, ascending=True, na_position="first")
        # make a dict for each durgrp which has the critical TP, low stat, high stat and count of storms in the bin
        stats_dict = {}
        # print('starting checking here')
        # print(r, medianpos)
        # print(ensemblestat[durcol])
        # pd.set_option('display.max_columns', None)
        # pd.set_option('display.max_rows', None)
        # print('')
        # print(ensemblestat)
        # exclude zeroes for mean
        # median includes zeroes
        # Mean including zeroes
        mean_including_zeroes = ensemblestat[statcol].mean()

        # Mean excluding zeroes
        mean_excluding_zeroes = ensemblestat[ensemblestat[statcol] != 0][statcol].mean()

        stats_dict = {
            "mean_including_zeroes": mean_including_zeroes,
            "mean_excluding_zeroes": mean_excluding_zeroes,
            "Duration": ensemblestat[durcol].iloc[medianpos],
            "Critical_TP": ensemblestat[tpcol].iloc[medianpos],
            "low": ensemblestat[statcol].iloc[0],
            "high": ensemblestat[statcol].iloc[-1],
            "count": r,
            "median": ensemblestat[statcol].iloc[medianpos],
        }
        if stats_dict["median"] > tracking_median:
            # update max_stats_dict if this median stat is higher than the previous highest
            # remember  q = dict will be a reference to the dict. so when you update the dict later, q will also see the update when you may not have expected it to
            # therefore we loop through and make a copy (there might be a more pythonic method but I was looking at this for too long before I figure it out)
            for k in stats_dict:
                max_stats_dict[k] = stats_dict[k]
            tracking_median = stats_dict["median"]
        # print('stats_dict', stats_dict)
        bin_stats_list.append(stats_dict)
        count_bin += r
    max_stats_dict["count_bin"] = count_bin
    # print('max_stats_dict')
    # pprint.pprint(max_stats_dict, width=300, compact=Falsex``)
    # print('bin_stats_list')
    # pprint.pprint(bin_stats_list, width=300, compact=False)
    return max_stats_dict, bin_stats_list


def setup_pomm_file_processing(pomm_file_strings):
    pomm_file_list = []
    for file_string in pomm_file_strings:
        pomm_file_list.extend(make_file_list(file_string))
    return pomm_file_list


def process_files_in_parallel(po_file_list, positionsDict):
    numFiles = calcNumWorkersCores(len(po_file_list))
    print(f"Processing {len(po_file_list)} files over {numFiles} threads\n")
    with multiprocessing.Pool(numFiles) as p:
        # Pass positionsDict as an additional argument to map
        resultsData = p.starmap(processPomm_extended, [(file, positionsDict) for file in po_file_list])
    return resultsData


def aggregate_and_fill_data(resultsData):
    """
    Aggregates data from a list of DataFrame objects, excludes None, concatenates them,
    and fills missing values with NaN.

    Args:
    - resultsData: List of DataFrame objects or None.

    Returns:
    - A single DataFrame with missing values filled with NaN.
    """
    # just_max = [q[1] for q in resultsData if q is not None] #this was when we got the TS data, we already have the max data
    df = pd.concat(resultsData)
    df_filled = df.fillna(value=np.nan)
    return df_filled


def process_and_aggregate(df):
    """
    Processes the dataframe to filter for 'Flow' types, groups by TrimRC, Location, Type, and AEP,
    applies statistical analysis on each group, and aggregates the results into a new dataframe.

    Args:
    - df: The original dataframe with columns ['Time', 'Type', 'Location', 'Value'].

    Returns:
    - Tuple of DataFrames: (meddf, all_bins_df) where:
      - meddf contains the median and other statistics for each group.
      - all_bins_df contains detailed bin statistics for each group.
    """
    # provide a new dataframe which is the subset of df where  df['Type']='Flow'
    # then groupby TrimRC, Location, AEP
    # then for each groupby, run the stats function on the groupby
    # then add the results to a new dataframe
    # [['Time', 'Type', 'Location', 'Value']]
    # just_flows = df.loc[df['Type']=='Flow']
    meddf = pd.DataFrame(
        columns=[
            "TrimRC",
            "Location",
            "Type",
            "AEP",
            "median",
            "Duration",
            "Critical_TP",
            "low",
            "high",
        ]
    )
    # abs_max instead of value due to the the max_from_single function
    all_bins_df = pd.DataFrame()

    for TrimRC, trimrc_df in df.groupby(by="TrimRC"):
        for location, loc_df in trimrc_df.groupby(by="Location"):
            for type, type_df in loc_df.groupby(by="Type"):
                for AEP, aep_df in type_df.groupby(by="AEP"):
                    new_row = {
                        "TrimRC": TrimRC,
                        "Location": location,
                        "Type": type,
                        "AEP": AEP,
                    }
                    max_stats_dict, bin_stats_list = stats(
                        thinned_df=aep_df,
                        statcol="abs_max",
                        tpcol="TPat",
                        durcol="Duration",
                    )

                    # Update bin stats with new_row keys
                    bin_stats_list = [{**new_row, **bin_stats_dict} for bin_stats_dict in bin_stats_list]

                    # Update new_row with stats from max_stats_dict
                    new_row.update(max_stats_dict)

                    # Append to meddf and all_bins_df
                    meddf = pd.concat([meddf, pd.DataFrame([new_row])], ignore_index=True)
                    all_bins_df = pd.concat([all_bins_df, pd.DataFrame(bin_stats_list)], ignore_index=True)

    return meddf, all_bins_df


def export_dataframes(dataframes, file_names, sheet_names) -> None:
    DateTimeString = datetime.now().strftime("%Y%m%d-%H%M")
    for dataframe, file_name, sheet_name in zip(dataframes, file_names, sheet_names):
        export_name = f"{DateTimeString}_{file_name}.xlsx"
        print(f"Exporting {export_name}")
        with pd.ExcelWriter(export_name) as writer:
            for df, sheet in zip(dataframe, sheet_name):
                df.to_excel(excel_writer=writer, sheet_name=sheet, merge_cells=False)
        print(dataframe)
        for actual_df in dataframe:
            print(actual_df.columns)


def load_files(file_patterns):
    pomm_file_list = setup_pomm_file_processing(file_patterns)
    pprint.pprint(pomm_file_list)
    print()

    positionsDict = {
        "Type": "string",
        "Location": "string",
        "Max": "float",
        "Tmax": "float",
        "Min": "float",
        "Tmin": "float",
    }

    resultsData = process_files_in_parallel(pomm_file_list, positionsDict)
    df = aggregate_and_fill_data(resultsData)
    print(df, df.columns, df.dtypes, sep="\n\n")
    return df


def main(process_medians=True, file_patterns=["_POMM.csv"]) -> None:
    df = load_files(file_patterns)
    df.to_parquet("data.parquet")  # if you want to save to parquet for faster loading next time

    # df = pd.read_parquet('data.parquet') # load it in

    print(df.head())
    print(df["Duration"].unique())

    meddf, all_bins_df = process_and_aggregate(df)

    dataframes = [
        [df],
        [meddf, all_bins_df],
    ]  # df is from POMM so it is already max values
    file_names = ["PO_max", "PO_max_median"]
    sheet_names = [["PO_max"], ["PO_medians", "PO_bins"]]
    export_dataframes(dataframes, file_names, sheet_names)

    print("Done")
    os.system("Pause")


if __name__ == "__main__":
    os.chdir(Path(__file__).absolute().parent)
    # originally developed here for TF2016
    # os.chdir(r'Q:\Model\RP21054.004 PILGANGOORA HYDROLOGY SUPPORT - PILOPE\Tuflow_Models\TF_v0_WholeSite_MGA94z50\results\tuflow-style\Pilg_30m_WholeSite_00180m')
    main()
