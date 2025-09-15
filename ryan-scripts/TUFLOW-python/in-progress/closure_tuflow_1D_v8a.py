import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import re
import logging
import csv


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def processTScsv(file):
    # print(' ')
    print(file)
    with open(file, "r") as csvfile:

        reader = csv.reader(csvfile)
        csvdata = list(reader)
        fileName = os.path.basename(file)

        # the data is in columns, we need to move the row A headings to be rows in the dataframe (make them an index)
        # we use the name from in the file rather than the csv filename in case there was some dodginess
        internalName = csvdata[0][0].split("[")[0][0:-1]
        # this is the other part of cell A1 that had the source TCF
        tcf_path = csvdata[0][0].split("[")[1][0:-1]

        # reformat the data into the format we want. there will be a column for each variable type, and a single channel column
        """ Mardie [tcf]	Time (h)	Q 3-4-1 [Mardie_NS_007_FW34_01p_Adv_08M_C]	Q 3-4-2 [Mardie_NS_007_FW34_01p_Adv_08M_C]	Q NS-09 [Mardie_NS_007_FW34_01p_Adv_08M_C]	Q NS-10 [Mardie_NS_007_FW34_01p_Adv_08M_C]
        1	0	0	0	0	0
        2	0.008333	0	0	0	0
        3	0.016667	0	0	0	0 """

        val_type = csvdata[0][2].split(" ")[0]
        chan_list = [c.split("[")[0].removeprefix(f"{val_type} ") for c in csvdata[0][2:]]
        # print(val_type, chan_list)
        # the channel might have a space in it so remove both sides with some extra string logic when looking on row A.
        # not going to bother grabbing the type or internal name each time - it should be identical unless someone has manually merged files.

        # check if our list is going to have multiple columns of data or not.
        # _1d_CF.csv - 1 column of the flow type
        # _1d_H.csv - 2 columns .1 and .2 for upstream and downstream for the node numbers. .1 should be US and .2 DS
        # _1d_L.csv - this has 3 columns for each pipe - entry, additional and exit loss coefficients
        # _1d_NF.csv - not sure what it is for, has 2 columns for .1 and .2 . Something to do with the pits/node?
        # _1d_Q.csv - flow, 1 column.
        # _1d_V.csv - tuflow velocity, 1 column.
        # not reading in NF as it is for nodes, not the channel. Could be extended to include it.
        # not reading in L as it is different and not used for this task. Could be extended to include it.Could be extended to include it.

        # res_type = {'F': 1, 'H': 2, 'L': 3, 'Q': 1, 'V': 1}  # CF is F in the cell

        # now we need to make a new list with each data point of the table.
        # the value goes under val_type as that's how we will have it in the final result
        # print(val_type)
        if val_type in ["F", "Q", "V"]:  # only one column style
            adj_data = [["Chan ID", "Time", val_type]]
            for line in csvdata[1:]:  # skip header
                # print(line)
                # skip the index col and time col. (num, data)
                for enum, val in enumerate(line[2:], start=0):
                    # we can use the enumerate index to read from the chan_list which will be in the same order
                    # ['Chan_ID', 'Time', val_type]
                    adj_data.append([chan_list[enum].strip(), line[1].strip(), val.strip()])
                    # print(adj_data[-1])
        elif val_type in ["H"]:  # two columns and has .1 .2
            adj_data = [["Chan ID", "Time", "H_US", "H_DS"]]
            for line in csvdata[1:]:  # skip header
                # skip the index col and time col. (num, data)
                # this groups the line into twos so we can use them at the same time
                for enum, val in enumerate(
                    zip(line[2::2], line[3::2]),
                    start=0,
                ):
                    # we can use the enumerate index to read from the chan_list which will be in the same order
                    # remember to allow for 2 group vs 1 group size
                    # ['Chan_ID', 'Time', 'H_US', 'H_DS']
                    # list slicing syntax Lst[ Initial : End : IndexJump ]
                    # 0,0 1,2 2,4 3,6 zip,chan_list n,2n
                    # drop the .1 .2
                    adj_data.append(
                        [
                            chan_list[2 * enum][:-3].strip(),
                            line[1].strip(),
                            val[0].strip(),
                            val[1].strip(),
                        ]
                    )

        # print(adj_data[0:10])
        dfData = pd.DataFrame(data=adj_data[1:], columns=adj_data[0])

        dfData["internalName"] = internalName
        # split the run code up by '_'
        for elem in enumerate(internalName.split("_"), start=1):
            dfData[f"R{elem[0]}"] = elem[1]
        dfData["path"] = file
        dfData["file"] = fileName
        # print(dfData)
        if "Time (h)" in dfData.columns:
            dfData["Time"] = dfData["Time (h)"]
        col_names = {
            "Chan ID": "string",
            "Time": "float64",
            "Time (h)": "float64",
            "H_US": "float64",
            "H_DS": "float64",
            "Q": "float64",
            "V": "float64",
            "F": "string",
            "path": "string",
            "file": "string",
            "internalName": "string",
        }
        for k in col_names:
            if k in dfData.columns:
                dfData = dfData.astype({k: col_names[k]})
        # tuflow not consistent with the column naming between files. imported a different way anyway
        # if ''Time (h)'' in df.columns:
        #   dfData.rename(columns={'Time (h)': 'Time'}, inplace=True)
        # dfData.rename(columns={'Time(h)': 'Time'}, inplace=True)
    return dfData, internalName


# #this was for PO lines to get flow. Extend it to deal with elevations as well.
# def read_csv(filepathname, separator, skip_row, header_row, skip_cols):
#     import csv

#     """ Runcode	Location	Flow	Flow
#     tacf_path	Time	trw	tr
#     this_csv_path	0	0	0
#         0.05	0.1261	0.0473 """
#     # have assumed this format - some olders versions do it swapped
#     # skip col 1.
#     # only use those columns which have Flow
#     # skiprows is 0-indexed

#     search_entry = "Flow"
#     with open(filepathname, "r") as file:
#         reader = csv.reader(file)
#         first_row = next(reader)
#         column_numbers = [
#             i for i, column in enumerate(first_row) if column == search_entry
#         ]
#         column_numbers.append(1)
#     returncsv = pd.read_csv(
#         filepathname,
#         sep=separator,
#         skiprows=skip_row,
#         header=header_row,
#         usecols=column_numbers,
#     )
#     #  this was Animesh command for RORB. instead I used usecols to select the columns which have Flow in them.
#     # Also added in the time column as the return function expects it first.
#     #  if skip_cols > 0:  # loop through the columns and delete them
#     #     # range extends from 0, which is the first column to skip_cols-1 (which is the nth column)
#     #     for d in range(skip_cols):
#     #         # drop it like it's cold, inplace=true basically modifies the returncsv itself without returning anything
#     #         returncsv.drop(returncsv.columns[0], axis=1, inplace=True)
#     #     return returncsv
#     # else:
#     #     return returncsv
#     return returncsv


def process_csv_name(csv_filename):
    ts_file_strings_1D = ["_1d_CF.csv", "_1d_H.csv", "_1d_Q.csv", "_1d_V.csv"]
    """
    Process the name of the CSV file to determine the file type.
    """
    basename = os.path.basename(csv_filename)

    for ts_string in ts_file_strings_1D:
        if basename.endswith(ts_string):
            Runcode = basename[: -len(ts_string)]
            print(f"{Runcode} : {csv_filename}")
            return "TS"
    if basename.endswith("_PO.csv"):
        Runcode = basename[: -len("_PO.csv")]
        print(f"{Runcode} : {csv_filename}")
        return "PO"

    raise ValueError("File name does not match any expected format.")


def read_csv(filepathname, separator, skip_row, header_row, skip_cols):
    """
    Read a CSV file and return the processed DataFrame.

    Args:
    filepathname (str): The path to the CSV file.
    separator (str): The separator used in the CSV file.
    skip_row (int): The number of rows to skip at the beginning of the file.
    header_row (int): The row number to use as the header.
    skip_cols (int): The number of columns to skip at the beginning of the file.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """
    # Determine the type of CSV file
    file_type = process_csv_name(filepathname)

    with open(filepathname, "r") as file:
        reader = csv.reader(file)
        first_row = next(reader)

        if file_type == "PO":
            search_entry = "Flow"
            column_numbers = [i for i, column in enumerate(first_row) if column == search_entry]
            column_numbers.append(1)  # Always skip the first column
        else:
            column_numbers = list(range(len(first_row)))
            column_numbers.remove(0)  # Always skip the first column

    # Read the CSV file into a DataFrame
    returncsv = pd.read_csv(
        filepathname,
        sep=separator,
        skiprows=skip_row if file_type == "PO" else 0,
        header=header_row,
        usecols=column_numbers,
    )

    # Extract value type from the CSV data
    val_type = first_row[2].split(" ")[
        0
    ]  # Assuming the value type is determined from the third column in the first row

    # Transform column names
    new_column_names = [c.split("[")[0].removeprefix(f"{val_type} ") for c in returncsv.columns]

    # Rename columns in the DataFrame
    returncsv.columns = new_column_names

    # print(returncsv.columns)

    if "Time (h)" in returncsv.columns:
        returncsv.rename(columns={"Time (h)": "Time"}, inplace=True)

    # Skip columns if required
    # if skip_cols > 0:
    #     returncsv.drop(returncsv.columns[:skip_cols], axis=1, inplace=True)

    return returncsv


# old stats
# def stats(freqdb, statcol, tpcol, durcol):
#     # this function will extract min, max and median peak for peaks in the dataframe and also return corresponding event/scenario and relevant _PO.csv file
#     # sourced from Animesh orginal style
#     median = -9999
#     for durgrp in freqdb.groupby(by=durcol):
#         ensemblestat = durgrp[1]
#         r = len(ensemblestat.index)
#         medianpos = int(r / 2)
#         # The median position calculated as int(len(r) / 2) is based on 0 indexing, which is the standard indexing method in Python.
#         # Therefore, in a range from 0 to 10 (range(0, 11)), the median position of 5 corresponds to the 6th element in the sequence, since indexing starts at 0.
#         # round up if odd, +1 if even. so it assumes sorted in ascending order.
#         # sort ensemblestat by statcol, ascending
#         ensemblestat.sort_values(statcol, inplace=True)
#         if ensemblestat[statcol].iloc[medianpos] > median:
#             median = ensemblestat[statcol].iloc[medianpos]
#             Tcrit = ensemblestat[durcol].iloc[medianpos]
#             Tpcrit = ensemblestat[tpcol].iloc[medianpos]
#             low = ensemblestat[statcol].iloc[0]
#             high = ensemblestat[statcol].iloc[-1]
#     return [median, Tcrit, Tpcrit, low, high]


def stats(freqdb, statcol, tpcol, durcol):
    """
    This function extracts the minimum, maximum, and median peak for peaks in the dataframe.
    It also returns the corresponding event/scenario and relevant values from the provided columns.
    based on Animesh and augmented by chatgpt

    Parameters:
    freqdb (DataFrame): The dataframe containing the data to analyze.
    statcol (str): The column name for the statistic to be analyzed.
    tpcol (str): The column name for the corresponding TP value.
    durcol (str): The column name for the duration.

    Returns:
    list: A list containing the median value, corresponding duration, TP value, minimum value, and maximum value.
    """

    # Initialize variables to store the results
    median = -9999
    Tcrit = Tpcrit = low = high = None

    # Group by the duration column
    for _, ensemblestat in freqdb.groupby(by=durcol):
        # Sort the group by the statistic column in ascending order
        ensemblestat_sorted = ensemblestat.sort_values(statcol)

        # Calculate the median position
        medianpos = len(ensemblestat_sorted) // 2
        current_median = ensemblestat_sorted[statcol].iloc[medianpos]

        # Update the median and corresponding values if the current median is higher
        if current_median > median:
            median = current_median
            Tcrit = ensemblestat_sorted[durcol].iloc[medianpos]
            Tpcrit = ensemblestat_sorted[tpcol].iloc[medianpos]
            low = ensemblestat_sorted[statcol].iloc[0]
            high = ensemblestat_sorted[statcol].iloc[-1]

    # Return the calculated statistics
    return [median, Tcrit, Tpcrit, low, high]


def calcNumWorkersCores(numFiles):
    import multiprocessing

    # def calculate_pool_size(num_files):
    splits = max(numFiles // 3, 1)
    available_cores = min(multiprocessing.cpu_count(), 20)
    calc_threads = min(available_cores - 1, splits)
    logging.info(f"Processing threads: {calc_threads}")
    return calc_threads


def check_string_TP(string):
    # this is from ChatGPT. Want to check if we have a standard TP naming pattern, then return the TP number
    r"""TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv
    In this updated code, the regular expression pattern (_TP\w{2}_)|^(_TP\w{2}$)|(^_TP\w{2}_) handles three cases:

    _TP\w{2}_: Matches if the string has a combination starting with "_TP" followed by any two word characters, and is surrounded by underscores on both sides.
    ^(_TP\w{2}$): Matches if the string starts with "_TP" followed by any two word characters, and ends at the string boundary.
    (^_TP\w{2}_): Matches if the string starts with "_TP" followed by any two word characters and is immediately followed by an underscore.
    You can use this updated check_string function to check if the string has a 4-character combination starting with "TP" and is prefixed, suffixed, or both by an underscore.
    """
    # This is from chatgpt. I feel like this might not return the right number when the duration, tp, AEP is at the start or end of a string
    # I have not tested these other cases.
    # TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv
    # pattern = r"(_TP\w{2}_)|^(_TP\w{2}$)|(^_TP\w{2}_)"
    # Updated pattern to include _TPXX+, +TPXX+, +TPXX_ 2024-02-29
    pattern = r"(_TP\w{2}_)|(_TP\w{2}\+)|(\+TP\w{2}\+)|(\+TP\w{2}_)"
    match = re.search(pattern, string)
    if match:
        r"""the regular expression pattern (?<=TP)\w{2} uses a positive lookbehind (?<=TP) to match any two word characters (\w{2}) that come after "TP" in the string"""
        pattern = r"(?<=TP)\w{2}"
        match = re.search(pattern, string)
        return match.group(0)
    else:
        return None


def check_string_duration(string):
    r"""The pattern r'(?:_|^)([^_]{5}m)(?:_|$)' is looking for a sequence of 5 characters that are not underscores, followed by the letter 'm'. This sequence must be preceded and followed by an underscore or the start/end of the string.
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
    r"""The pattern r'(?:_|^)([^_]{5}p)(?:_|$)' is looking for a sequence of 5 characters that are not underscores, followed by the letter 'p'.
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


def process_PO_csv_name(PO_csv_filename):
    return process_csv_name(PO_csv_filename)


def process_csv_name(PO_csv_filename):
    # expanded to deal with a few more types of csv.
    """in progress"""
    ts_file_strings = ["_1d_CF.csv", "_1d_H.csv", "_1d_Q.csv", "_1d_V.csv", "_PO.csv"]
    basename = os.path.basename(PO_csv_filename)
    for ts_string in ts_file_strings:
        if basename.endswith(ts_string):
            Runcode = basename[: -len(ts_string)]
            print(f"{Runcode} : {PO_csv_filename}")
            break
    else:
        raise ValueError("File name does not match any expected format.")

    print(f"{Runcode} : {PO_csv_filename}")

    # now process the names of the PO csv file to get the params of the file

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
        "___": "_",
        "__": "_",
    }
    TrimRC = Runcode
    for k, v in replace_dict.items():
        TrimRC = TrimRC.replace(k, v)
    temp_dict["TrimRC"] = TrimRC

    # old style -RunHeader=['R'+str(r) for r in range(1,len(max(RunTable,key=len)))] # We don't know how many columns there will be so use this code
    # now use pandas
    for elem in enumerate(Runcode.split("_"), start=1):
        temp_dict[f"R{elem[0]}"] = elem[1]

    # to get some bits out of the folder names - advanced work
    # paramsDF['method']=dirs.split('\\')[0]
    # paramsDF['model']=None #dirs.split('\\')[1]

    # print(temp_dict)
    return temp_dict


def process_hydrographCSV(params_dict):
    # this iterates over each hydrograph from the PO.csv file and determines durations exceeding for each check flow.
    # it then returns the duration_exceeding for each threshold flow. The hydrograph data is discarded.

    # [iter_num, aep, dur ,tp, out_path, csv_read, qcheck_list] as dict
    aep, dur, tp, csv_read, Runcode, TrimRC = (
        params_dict["AEP"],
        params_dict["Duration"],
        params_dict["TPat"],
        params_dict["csv"],
        params_dict["Runcode"],
        params_dict["TrimRC"],
    )
    qcheckList = params_dict["qc"]
    print(params_dict["iter_num"], aep, dur, tp, csv_read, Runcode, TrimRC)
    durexcdb = pd.DataFrame(
        columns=[
            "AEP",
            "Duration",
            "TP",
            "Location",
            "ThresholdFlow",
            "Duration_Exceeding",
            "Runcode",
        ]
    )
    hydrographs = read_csv(filepathname=csv_read, separator=",", skip_row=[0], header_row=0, skip_cols=1)
    # this has time as the first column then location+flows after
    # read_csv(filepathname, separator, skip_row, header_row, skip_cols)
    # this was for rorb - we just get the location names back now.
    #  hydrographs.columns = [
    #     m.replace('Calculated hydrograph:  ', '') for m in list(hydrographs.columns)]

    timestep = hydrographs["Time"][1] - hydrographs["Time"][0]
    for qch in qcheckList:
        location = hydrographs[hydrographs > qch].count()[1:].index.to_list()
        dur_exc = [(k + int(k > 0)) * timestep for k in hydrographs[hydrographs > qch].count()[1:]]
        dictionary = {
            "AEP": [aep] * len(location),
            "Duration": [dur] * len(location),
            "TP": [tp] * len(location),
            "Location": location,
            "ThresholdFlow": [qch] * len(location),
            "Duration_Exceeding": dur_exc,
            "Runcode": Runcode,
            "TrimRC": TrimRC,
        }
        dbdictionary = pd.DataFrame(data=dictionary)
        durexcdb = pd.concat([durexcdb, dbdictionary], ignore_index=True)
    return durexcdb


def plotOutFolder(outFol):
    if not os.path.exists(outFol):
        os.mkdir(outFol)
        print("Directory ", outFol, " Created ")
    else:
        print("Directory ", outFol, " already exists")
    return outFol + r"/"


def is_non_zero_file(fpath):
    """Check if a file is not zero size."""
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def process_files(file_subset, duration_skip):
    """Worker function to process a subset of files."""
    PO_details_list = []
    for PO_file in file_subset:
        skip = any(d in PO_file for d in duration_skip)
        if skip:
            print(f"File excluded due to filter: {PO_file}")
        elif is_non_zero_file(PO_file):
            PO_details = process_PO_csv_name(PO_csv_filename=PO_file)
            PO_details_list.append(PO_details)
        else:
            print(f"Zero byte file: {PO_file}")
    return PO_details_list


def search_csv_files(search_term="**/*_PO.csv"):
    from glob import iglob

    print(f"Recursively searching for {search_term} files - can take a while")
    file_list = [f for f in iglob(search_term, recursive=True) if os.path.isfile(f)]
    return file_list


def make_file_list(searchString):
    from glob import iglob

    textString = r"**/*" + searchString
    print(f"Recursively searching for {textString} files - can take a while")
    file_list = [f for f in iglob(textString, recursive=True) if os.path.isfile(f)]
    print(file_list)
    return file_list


def process_file_list(file_list):
    import multiprocessing

    # now we want to process the file_list filenames to get basic data about the csv files.
    # this will also provide extra fields for use later. Could even add extra parameters in the parsing function
    duration_skip = []  # ['5760', '7200', '8640', '10080']

    # Split file_list for multiprocessing
    num_processes = multiprocessing.cpu_count()  # Or set a specific number
    chunk_size = len(file_list) // num_processes
    files_chunks = [file_list[i : i + chunk_size] for i in range(0, len(file_list), chunk_size)]

    # Ensure the worker function can access the duration_skip list
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Map each file chunk to the process_files function
        result_lists = pool.starmap(process_files, [(chunk, duration_skip) for chunk in files_chunks])

    # Combine the results
    combined_results = []
    for sublist in result_lists:
        combined_results.extend(sublist)

    files_df = pd.DataFrame(combined_results)
    return files_df


def output_file_list(files_df):
    DateTimeString = datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    output = f"{DateTimeString}_PO_list.csv"
    print(f"Outputting file list: {output}")
    files_df.to_csv(output, index=False)


def create_params_list(files_df):
    # durexcdb=pd.DataFrame (columns=['AEP','Duration','TP','Location','ThresholdFlow','Duration_Exceeding', 'out_path'])
    # db=pd.read_excel (r"P:\P21182.01 NANUTARRA ROAD DD\200 CALC\200.2 Civil\Flooding\Hydrology\RORB Models\all ensemble results.xlsx",sheet_name='Samson_files')
    # files_df = files_df[0:5]

    # flows
    # qcheck = (
    #     list(range(2, 30, 1))
    #     + list(range(30, 100, 5))
    #     + list(range(100, 500, 20))
    #     + list(range(500, 2100, 50))
    # )

    # elevations
    import numpy as np

    qcheck = list(np.arange(60.0, 75.0, 0.01))

    reslen = files_df.shape[0]

    # make an iterable list that we can use with multiprocessing
    params_list = []  # list of dicts
    for i in range(reslen):
        # print (i, ' of', reslen)
        # csv is the individual storm hydrograph
        # [aep,dur,tp, out_path, csv_read] - old style - now make a dict
        params_dict = {
            "iter_num": i + 1,
            "AEP": files_df["AEP"][i],
            "Duration": files_df["Duration"][i],
            "TPat": files_df["TPat"][i],
            "Runcode": files_df["Runcode"][i],
            "csv": files_df["csv"][i],
            "qc": qcheck,
            "TrimRC": files_df["TrimRC"][i],
        }
        # [iter, aep, dur ,tp, out_path, csv_read, qcheck_list]
        params_list.append(params_dict)

    return params_list


def process_params_list(params_list):
    import multiprocessing

    numFiles = len(params_list)
    numThreads = calcNumWorkersCores(numFiles)
    # numThreads=1
    print(f"Processing {numFiles} with {numThreads} threads")
    # numFiles = 1

    # for num, dir in enumerate(dir_list, start=1):
    # processFolder(num, dir)

    # manual
    # DATA = []
    # for num, run_details in enumerate(params_list, start=1):
    #     DATA.append(process_hydrographCSV(run_details))
    with multiprocessing.Pool(numThreads) as p:
        DATA = p.map(process_hydrographCSV, params_list)

    # multiprocessing - old style with an added constant. We made it a single list now though. Duplicating the list in the iterable increases memory a bit but it should not be a problem.
    # with multiprocessing.Pool(numThreads) as p:
    #     hydroCSV = partial(process_hydrographCSV, qcheckList=qcheck) #qcheck is a constant list
    #     rorbDATA = p.starmap(hydroCSV, enumerate(rorb_csvs, start=1))

    """ # simplified multiproc
    # broken :(
    print(params_list)
    with multiprocessing.Pool(numFiles) as p:
        DATA = p.map(process_hydrographCSV, params_list) """

    return DATA


def merge_data(DATA):
    print("now to merge")
    # df = pd.DataFrame()
    df = pd.concat(DATA)

    # for q in rorbDATA:
    # # print(q.head())
    # df.append(q)

    DateTimeString = datetime.now().strftime("d%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    output = f"{DateTimeString}_durex"
    df.to_parquet(f"{output}.parquet.gzip")
    # df.to_csv(f"{output}.csv", index=False)

    return df


# old create_finaldb
# def create_finaldb(df):
#     finaldb = pd.DataFrame(
#         columns=[
#             "TrimRC",
#             "location",
#             "Threshold_Q",
#             "AEP",
#             "Duration_Exceeded",
#             "Critical_Storm",
#             "Critical_Tp",
#             "Low_Duration",
#             "High_Duration",
#         ]
#     )
#     for TrimRC in df.groupby(by="TrimRC"):
#         for locgrp in TrimRC[1].groupby(by="Location"):
#             for flowgrp in locgrp[1].groupby(by="ThresholdFlow"):
#                 for AEPgrp in flowgrp[1].groupby(by="AEP"):
#                     result = [TrimRC[0], locgrp[0], flowgrp[0], AEPgrp[0]] + stats(
#                         AEPgrp[1], "Duration_Exceeding", "TP", "Duration"
#                     )
#                     finaldb.loc[len(finaldb)] = result

#     DateTimeString = (
#         datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
#     )
#     save_as = f"{DateTimeString}_HvsTexc.csv"
#     finaldb.to_csv(save_as, index=False)
#     print(f"Outputting {save_as}")


def create_finaldb(df):
    print("Initializing the final dataframe...")
    # Initialize the final dataframe
    finaldb = pd.DataFrame(
        columns=[
            "TrimRC",
            "location",
            "Threshold_Q",
            "AEP",
            "Duration_Exceeded",
            "Critical_Storm",
            "Critical_Tp",
            "Low_Duration",
            "High_Duration",
        ]
    )

    print("Grouping the dataframe by TrimRC, Location, ThresholdFlow, and AEP...")
    # Group by necessary columns
    grouped = df.groupby(["TrimRC", "Location", "ThresholdFlow", "AEP"])

    print("Applying the stats function to each group...")

    # Define a function to be applied to each group
    def apply_stats(group):
        return stats(group, "Duration_Exceeding", "TP", "Duration")

    # Apply the stats function to each group and collect the results
    results = grouped.apply(apply_stats).reset_index()

    print("Concatenating the results...")
    # Concatenate the results
    finaldb = pd.concat([results.drop(columns=0), results[0].apply(pd.Series)], axis=1)
    finaldb.columns = [
        "TrimRC",
        "location",
        "Threshold_Q",
        "AEP",
        "Duration_Exceeded",
        "Critical_Storm",
        "Critical_Tp",
        "Low_Duration",
        "High_Duration",
    ]

    # Save the dataframe to a CSV file
    DateTimeString = datetime.now().strftime("%Y%m%d-%H%M")
    save_as = f"{DateTimeString}_HvsTexc.csv"
    print(f"Saving the final dataframe to {save_as}...")
    finaldb.to_csv(save_as, index=False)
    print(f"Outputting {save_as}")


def plot_data(df, working_dir):
    from matplotlib import pyplot as plt
    from scipy.interpolate import BSpline, make_interp_spline

    db = df
    pltfolder = working_dir + r"\plots"
    print("Now plotting")
    plotOutFolder(pltfolder)

    # update location
    # cull data to just that which we are plotting - max exceedance time for each location+aep+flow duration combination

    # rorb
    # max_df=db.groupby(['Location','AEP','ThresholdFlow','out_path'])['Duration_Exceeding'].max()
    # tuflow
    max_df = db.groupby(["Location", "AEP", "ThresholdFlow", "TrimRC"])["Duration_Exceeding"].max()
    plot_table = max_df.reset_index()
    # plot_table.loc[plot_table['Location'] == 'Chinnamon Creek']

    mod_skip = ["TF2023"]  # this is a list of models to skip - TF2023 test
    # for modelgrp in plot_table.groupby(by='out_path'):
    # grouping by truncated run code
    for modelgrp in plot_table.groupby(by="TrimRC"):
        skip = False
        for m in mod_skip:
            if m in modelgrp[0]:
                skip = True
        if not skip:  # skips models in mod_skip
            for locgrp in modelgrp[1].groupby(by="Location"):
                # outplot=pltfolder+r'/'+ (locgrp[1])['out_path'].iloc[1]+r'/'+locgrp[0]+'.png'
                # includes truncated run code as you could get clashes unexpectedly if you ran extra models and forgot
                outplot = f"{pltfolder}\\{modelgrp[0]}_{locgrp[0]}.png"
                print(outplot)
                fig = plt.figure()
                for AEPgrp in locgrp[1].groupby(by="AEP"):
                    AEPgrp[1].sort_values("ThresholdFlow", inplace=True)
                    y = AEPgrp[1]["ThresholdFlow"].to_list()
                    x = AEPgrp[1]["Duration_Exceeding"].to_list()
                    plt.plot(x, y, label=AEPgrp[0])
                plt.legend(loc=(0, -0.4), ncol=7)
                plt.ylabel("Threshold Flow")
                plt.xlabel("Duration Flow Exceeds Threshold (hr)")
                plt.yscale("log")
                plt.grid(visible=True, which="major", linestyle="-")
                plt.grid(visible=True, which="minor", linestyle="--")
                fig.savefig(outplot, bbox_inches="tight")
                plt.close("all")


def main():
    file_list = make_file_list("_1d_H.csv")  #'_1d_Q.csv'
    files_df = process_file_list(file_list)
    output_file_list(files_df)
    params_list = create_params_list(files_df)
    DATA = process_params_list(params_list)
    df = merge_data(DATA)
    create_finaldb(df)
    # plot_data(df, working_dir)
    os.system("PAUSE")


if __name__ == "__main__":
    script_dir = Path(__file__).absolute().parent
    os.chdir(script_dir)
    main()
