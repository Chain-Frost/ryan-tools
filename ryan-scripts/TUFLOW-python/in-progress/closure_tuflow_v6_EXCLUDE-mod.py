import os
from datetime import datetime
import pandas as pd
from ryan_library.functions.data_processing import (
    check_string_aep,
    check_string_duration,
    check_string_TP,
)
from ryan_library.functions.misc_functions import calculate_pool_size

# originally based on Animesh functions


def read_csv(filepathname, separator, skip_row, header_row, skip_cols):
    import csv

    """ Runcode	Location	Flow	Flow
    tacf_path	Time	trw	tr
    this_csv_path	0	0	0
        0.05	0.1261	0.0473 """
    # have assumed this format - some olders versions do it swapped
    # skip col 1.
    # only use those columns which have Flow
    # skiprows is 0-indexed

    search_entry = "Flow"
    with open(filepathname, "r") as file:
        reader = csv.reader(file)
        first_row = next(reader)
        column_numbers = [
            i for i, column in enumerate(first_row) if column == search_entry
        ]
        column_numbers.append(1)
    returncsv = pd.read_csv(
        filepathname,
        sep=separator,
        skiprows=skip_row,
        header=header_row,
        usecols=column_numbers,
    )
    #  this was Animesh command for RORB. instead I used usecols to select the columns which have Flow in them.
    # Also added in the time column as the return function expects it first.
    #  if skip_cols > 0:  # loop through the columns and delete them
    #     # range extends from 0, which is the first column to skip_cols-1 (which is the nth column)
    #     for d in range(skip_cols):
    #         # drop it like it's cold, inplace=true basically modifies the returncsv itself without returning anything
    #         returncsv.drop(returncsv.columns[0], axis=1, inplace=True)
    #     return returncsv
    # else:
    #     return returncsv
    return returncsv


def stats(
    freqdb, statcol, tpcol, durcol
):  # this function will extract min, max and median peak for peaks in the dataframe and also return corresponding event/scenario and relevant _PO.csv file
    median = -9999
    for durgrp in freqdb.groupby(by=durcol):
        ensemblestat = durgrp[1]
        r = len(ensemblestat.index)
        medianpos = int(
            r / 2
        )  # pretty sure the +1 from animesh was incorrect. fix later on

        ensemblestat.sort_values(statcol, inplace=True)
        if ensemblestat[statcol].iloc[medianpos] > median:
            median = ensemblestat[statcol].iloc[medianpos]
            Tcrit = ensemblestat[durcol].iloc[medianpos]
            Tpcrit = ensemblestat[tpcol].iloc[medianpos]
            low = ensemblestat[statcol].iloc[0]
            high = ensemblestat[statcol].iloc[-1]
    return [median, Tcrit, Tpcrit, low, high]


def calcNumWorkersCores(numFiles) -> int:
    return calculate_pool_size(numFiles)


def process_PO_csv_name(PO_csv_filename):
    # now process the names of the PO csv file to get the params of the file
    basename = os.path.basename(PO_csv_filename)
    Runcode = os.path.basename(PO_csv_filename)[:-7]
    print(f"{Runcode} : {PO_csv_filename}")

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

    print(temp_dict)
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
    hydrographs = read_csv(
        filepathname=csv_read, separator=",", skip_row=[0], header_row=0, skip_cols=1
    )
    # this has time as the first column then location+flows after
    # read_csv(filepathname, separator, skip_row, header_row, skip_cols)
    # this was for rorb - we just get the location names back now.
    #  hydrographs.columns = [
    #     m.replace('Calculated hydrograph:  ', '') for m in list(hydrographs.columns)]
    timestep = hydrographs["Time"][1] - hydrographs["Time"][0]
    for qch in qcheckList:
        location = hydrographs[hydrographs > qch].count()[1:].index.to_list()
        dur_exc = [
            (k + int(k > 0)) * timestep
            for k in hydrographs[hydrographs > qch].count()[1:]
        ]
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
        # dbdictionary = pd.DataFrame(data=dictionary)
        # durexcdb = pd.concat([durexcdb, dbdictionary], ignore_index=True)

        # Convert the dictionary to a DataFrame and drop all-NA columns
        dbdictionary = pd.DataFrame(data=dictionary).dropna(axis=1, how="all")

        # Drop all-NA columns from durexcdb and concatenate
        durexcdb = pd.concat(
            [durexcdb.dropna(axis=1, how="all"), dbdictionary], ignore_index=True
        )

    return durexcdb


def plotOutFolder(outFol):
    if not os.path.exists(outFol):
        os.mkdir(outFol)
        print("Directory ", outFol, " Created ")
    else:
        print("Directory ", outFol, " already exists")
    return outFol + r"/"


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


if __name__ == "__main__":
    import multiprocessing
    import re
    from glob import iglob

    working_dir = r"Q:\BGER\PER\RP20181.364 YANDI MGD5 DRAIN FS - RTIO\TUFLOW_YANDI_MGD5\results\v02\big_EXG"
    os.chdir(working_dir)
    print("Recursively searching for **/*_PO.csv files - can take a while")
    # example TF_Pilg_WholeSite_model_01.00p_00360m_TP01_RyanTPs_PO.csv
    file_list = [f for f in iglob("**/*_PO.csv", recursive=True) if os.path.isfile(f)]
    file_list

    # now we want to process the file_list filenames to get basic data about the csv files.
    # this will also provide extra fields for use later. Could even add extra parameters in the parsing function
    # note that the later
    files_df = pd.DataFrame
    PO_details_list = []
    duration_skip = []  # ["5760", "7200", "8640", "10080"]
    for PO_file in file_list:
        skip = False
        for d in duration_skip:
            if d in PO_file:
                skip = True
        if skip:  # check for skips
            print(f"File excluded due to filter: {PO_file}")
        elif is_non_zero_file(
            PO_file
        ):  # if you're still running TUFLOW there will be some 0 byte files - these kill the script (or if you killed some runs)
            PO_details_list.append(process_PO_csv_name(PO_csv_filename=PO_file))
        else:
            print(f"Zero byte file: {PO_file}")
        # print(PO_details_list)
    files_df = pd.DataFrame(PO_details_list)
    files_df
    DateTimeString = (
        datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    )
    output = f"{DateTimeString}_PO_list.csv"
    print(f"Outputting file list: {output}")
    files_df.to_csv(output, index=False)

    # durexcdb=pd.DataFrame (columns=['AEP','Duration','TP','Location','ThresholdFlow','Duration_Exceeding', 'out_path'])
    # db=pd.read_excel (r"P:\P21182.01 NANUTARRA ROAD DD\200 CALC\200.2 Civil\Flooding\Hydrology\RORB Models\all ensemble results.xlsx",sheet_name='Samson_files')
    # files_df = files_df[0:5]
    qcheck = list(range(2, 30, 1)) + list(range(30, 220, 1))
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

    numFiles = len(params_list)
    numThreads = calcNumWorkersCores(numFiles)
    print("")
    print(f"Processing {numFiles} with {numThreads} threads")
    # numFiles = 1

    # for num, dir in enumerate(dir_list, start=1):
    # processFolder(num, dir)

    # manual
    DATA = []
    for num, run_details in enumerate(params_list, start=1):
        DATA.append(process_hydrographCSV(run_details))

    # multiprocessing - old style with an added constant. We made it a single list now though. Duplicating the list in the iterable increases memory a bit but it should not be a problem.
    # with multiprocessing.Pool(numThreads) as p:
    #     hydroCSV = partial(process_hydrographCSV, qcheckList=qcheck) #qcheck is a constant list
    #     rorbDATA = p.starmap(hydroCSV, enumerate(rorb_csvs, start=1))

    """ # simplified multiproc
    # broken :(
    print(params_list)
    with multiprocessing.Pool(numFiles) as p:
        DATA = p.map(process_hydrographCSV, params_list) """

    print("now to merge")
    # df = pd.DataFrame()
    df = pd.concat(DATA)

    # for q in rorbDATA:
    # # print(q.head())
    # df.append(q)

    DateTimeString = (
        datetime.now().strftime("d%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    )
    output = f"{DateTimeString}_durex"
    print("")
    print("Outputting durex.csv")
    df.to_parquet(f"{output}.parquet.gzip")
    df.to_csv(f"{output}.csv", index=False)

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
    for TrimRC in df.groupby(by="TrimRC"):
        for locgrp in TrimRC[1].groupby(by="Location"):
            for flowgrp in locgrp[1].groupby(by="ThresholdFlow"):
                for AEPgrp in flowgrp[1].groupby(by="AEP"):
                    # print([Runcode[0], locgrp[0], flowgrp[0], AEPgrp[0]])
                    # print(AEPgrp[1].to_string())
                    result = [TrimRC[0], locgrp[0], flowgrp[0], AEPgrp[0]] + stats(
                        AEPgrp[1], "Duration_Exceeding", "TP", "Duration"
                    )
                    # insert at the end of the df
                    finaldb.loc[len(finaldb)] = result
    print("Outputting QvsTexc.csv")
    DateTimeString = (
        datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    )
    save_as = f"{DateTimeString}_QvsTexc.csv"
    finaldb.to_csv(save_as, index=False)
    print("")
    print("End")


if __name__ == "__main__":
    import pandas as pd
    from matplotlib import pyplot as plt
    from scipy.interpolate import BSpline, make_interp_spline

    # import numpy as np
    # run this after process_rorb_v4.py or closure_tuflow_v6.py

    db = df  # pd.read_parquet(r"Q:\Model\RP21054.004 PILGANGOORA HYDROLOGY SUPPORT - PILOPE\Tuflow_Models\TF_v0_WholeSite_MGA94z50\results\tuflow-style\Pilg_30m_WholeSite_01440m\d20230605-2350_durex.parquet.gzip") #update file
    pltfolder = working_dir + r"\plot_exceedance"
    print("")
    print("Now plotting")
    plotOutFolder(pltfolder)

    # update location
    # cull data to just that which we are plotting - max exceedance time for each location+aep+flow duration combination

    # rorb
    # max_df=db.groupby(['Location','AEP','ThresholdFlow','out_path'])['Duration_Exceeding'].max()
    # tuflow
    max_df = db.groupby(["Location", "AEP", "ThresholdFlow", "TrimRC"])[
        "Duration_Exceeding"
    ].max()
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
            for locgrp in (modelgrp[1]).groupby(by="Location"):
                # outplot=pltfolder+r'/'+ (locgrp[1])['out_path'].iloc[1]+r'/'+locgrp[0]+'.png'
                # includes truncated run code as you could get clashes unexpectedly if you ran extra models and forgot
                outplot = f"{pltfolder}\\{modelgrp[0]}_{locgrp[0]}.png"
                print(outplot)
                fig = plt.figure()
                for AEPgrp in locgrp[1].groupby(by="AEP"):
                    AEPgrp[1].sort_values("ThresholdFlow", inplace=True)
                    y = (AEPgrp[1])["ThresholdFlow"].to_list()
                    x = (AEPgrp[1])["Duration_Exceeding"].to_list()
                    plt.plot(x, y, label=AEPgrp[0])
                plt.legend(loc=(0, -0.4), ncol=7)
                plt.ylabel("Threshold Flow")
                plt.xlabel("Duration Flow Exceeds Threshold (hr)")
                plt.yscale("log")
                plt.grid(visible=True, which="major", linestyle="-")
                plt.grid(visible=True, which="minor", linestyle="--")
                fig.savefig(outplot, bbox_inches="tight")
                plt.close("all")

    os.system("PAUSE")
