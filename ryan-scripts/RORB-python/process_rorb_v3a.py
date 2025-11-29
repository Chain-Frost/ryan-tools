import pandas as pd
import os
from glob import iglob
import csv
from datetime import datetime
import multiprocessing
import sys
from functools import partial
import warnings


# Animesh functions
def read_csv(filepathname, separator, skip_row, header_row, skip_cols):
    returncsv = pd.read_csv(filepathname, sep=separator, skiprows=skip_row, header=header_row)
    if skip_cols > 0:  # loop through the columns and delete them
        for d in range(
            skip_cols
        ):  # range extends from 0, which is the first column to skip_cols-1 (which is the nth column)
            returncsv.drop(
                returncsv.columns[0], axis=1, inplace=True
            )  # drop it like it's cold, inplace=true basically modifies the returncsv itself without returning anything
        return returncsv
    else:
        return returncsv


def stats(
    freqdb, statcol, tpcol, durcol
):  # this function will extract min, max and median peak for peaks in the dataframe and also return corresponding event/scenario and relevant _PO.csv file
    median = -9999
    Tcrit = 0
    Tpcrit = 0
    low = 0
    high = 0
    for durgrp in freqdb.groupby(by=durcol):
        ensemblestat = durgrp[1]
        r = len(ensemblestat.index)
        medianpos = int(r / 2) + 1  # round up if odd, +1 if even
        ensemblestat.sort_values(statcol, inplace=True)
        if ensemblestat[statcol].iloc[medianpos] > median:
            median = ensemblestat[statcol].iloc[medianpos]
            Tcrit = ensemblestat[durcol].iloc[medianpos]
            Tpcrit = ensemblestat[tpcol].iloc[medianpos]
            low = ensemblestat[statcol].iloc[0]
            high = ensemblestat[statcol].iloc[-1]
    return [median, Tcrit, Tpcrit, low, high]


def calcNumWorkersCores(numFiles):
    splits = numFiles // 3
    if multiprocessing.cpu_count() <= 2:
        return min(2, splits)
    elif multiprocessing.cpu_count() <= 4:
        return min(multiprocessing.cpu_count(), splits)
    elif multiprocessing.cpu_count() <= 20:
        return min(multiprocessing.cpu_count() - 1, splits)
    else:
        return min(20, splits)


def parametersLine(line):
    pars = " ".join(line.split(":")[1].split())
    kc = pars.split("m")[0].split("=")[1].strip()
    m = pars.split("m")[1].split("=")[1].strip()
    return kc, m


def processSummary(rawSummary):
    summaryDict = {}
    for q in rawSummary[1:]:  # lines of the list of lists
        rnum = q.split(" ", 1)[0]
        padded = "Hyd" + rnum.zfill(4)
        summaryDict[padded] = q.split(" ", 1)[1].strip()
    print(summaryDict)
    return summaryDict


def make_csv_path(batchout, aepPart, durationPart, Tpat):
    # RORB_14141_batch.out
    # RORB_14141_ aep1_du12hourtp1.out
    # RORB_14141_ aep1_du12hourtp1.csv
    aep = aepPart.replace(".", "p")
    du = durationPart.replace(".", "_")
    first = batchout.replace("batch.out", "")
    second = f" {aep}_du{du}tp{Tpat}.csv"
    return first + second


def process_batchout(batchout_file):
    # now process the names in the batch.out files to get the csv files of the data
    basename = os.path.basename(batchout_file)

    paramsdict = {}
    tempDF = pd.DataFrame()
    datalist = []

    with open(batchout_file, "r") as f:
        print(batchout_file)
        foundResults = 0
        IL = 0
        CL = 0
        ROC = 1.0
        LP = 0
        for num, line in enumerate(f, start=0):
            # print(num, line)
            if foundResults == 20:
                pass
            elif foundResults == 0:
                if LP > 0:
                    lps = line.strip().split()
                    IL = float(lps[0])
                    # print(IL, 'IL', LP)
                    if LP == 1:  # CL
                        CL = float(lps[1])
                        # print(CL, 'CL', LP)
                    elif LP == 2:
                        ROC = float(lps[1])
                        # print(ROC, 'ROC', LP)
                    LP = 0
                elif "Parameters" in line:
                    kc = float(line.strip().split()[3])
                    m = float(line.strip().split()[-1])
                # print(line, kc, m)
                # Parameters:  kc =    50.00    m = 0.80

                # Loss parameters     Initial loss (mm)     Runoff coeff.
                # 0.00              0.51

                # Parameters:  kc =    50.00    m = 0.80

                # Loss parameters     Initial loss (mm)   Cont. loss (mm/h)
                # 66.00              6.60
                elif " Loss parameters " in line:
                    if "Cont" in line:
                        LP = 1  # CL
                    # print(line, LP)
                    else:
                        LP = 2  # ROC
                        # print(line, LP)
                elif " Peak  Description" in line:
                    rawSummary = [["rorbNum", "Description", "Location"]]
                    foundResults = 1
            elif foundResults == 1:
                if len(line) > 3:
                    rawSummary.append(line.strip())  # type: ignore
                else:
                    summaryDict = processSummary(rawSummary)  # type: ignore
                    foundResults = 10
            elif foundResults == 10:
                if "Run,    Representative hydrograph" in line:  # got to the end, stop recording
                    foundResults = 20
                elif " Run        Duration             AEP  " in line:
                    rorbRuns = line.strip().split()
                    rorbRuns.append("csv")
                    rorbRuns = [rorbRuns]
                    print(rorbRuns)

                    # ['Run', 'Duration', 'AEP', 'TPat', 'Rain(mm)', 'ARF', 'Peak0001']
                else:
                    rawLine = line.strip().split()
                    # ['126', '168', 'hour', '1%', '30', '421.00', '0.99', '25.8327']
                    # change to floats
                    [2, 3]  # not these
                    # newer rorb has this
                    # ['1', '12', '50', '1', '0.00', 'N', '49.10', '0.84', '0.00', '6.6138']
                    # so also 6 for tpat filtering
                    #  Run        Duration             AEP   TPat  %Filtered    TempPatFiltering   Rain(mm)     ARF  PbDepth  Peak0001
                    rawLine[3] = rawLine[3].strip("%")
                    durationPart = rawLine[1] + rawLine[2]
                    aepPart = "aep" + rawLine[3]
                    if rawLine[6] == "Y":
                        rawLine[6] = "1"
                    else:
                        rawLine[6] = "0"
                    if rawLine[2] == "hour":
                        pass
                    else:
                        rawLine[1] = float(rawLine[1]) / 60  # type: ignore

                    rawLine.pop(2)
                    print(rawLine)
                    processedLine = [float(el) for el in rawLine]  # convert to floats
                    processedLine[3] = int(processedLine[3])  # make the TPat and integer
                    processedLine.append(
                        make_csv_path(batchout_file, aepPart, durationPart, processedLine[3])
                    )  # the csv value. we don't know which column it has to be, so append
                    # print(processedLine)
                    rorbRuns.append(processedLine)  # type: ignore
        # print(rorbRuns)

    batchDF = pd.DataFrame(rorbRuns[1:], columns=rorbRuns[0])  # type: ignore

    batchDF["IL"] = IL
    batchDF["CL"] = CL
    batchDF["m"] = m
    batchDF["kc"] = kc
    batchDF["ROC"] = ROC
    batchDF["file"] = basename
    dirs = os.path.dirname(batchout_file)
    batchDF["folder"] = dirs
    batchDF["Path"] = batchout_file
    # batchDF["method"] = dirs.split("\\")[0]
    # batchDF["model"] = dirs.split("\\")[1]

    return batchDF


def process_hydrographCSV(printText, rline, qcheckList):

    # [aep,dur,tp, out_path, csv_read]
    # rline=db['AEP'][i],db['Duration'][i],db['TPat'][i],db['Path'][i],db['csv'][i]
    print(printText, rline)
    aep, dur, tp, out_path, csv_read = rline[0], rline[1], rline[2], rline[3], rline[4]
    durexcdb = pd.DataFrame(
        columns=[
            "AEP",
            "Duration",
            "TP",
            "Location",
            "ThresholdFlow",
            "Duration_Exceeding",
            "out_path",
        ]
    )
    hydrographs = read_csv(csv_read, ",", 2, 0, 1)
    hydrographs.columns = [m.replace("Calculated hydrograph:  ", "") for m in list(hydrographs.columns)]
    timestep = hydrographs["Time (hrs)"][1] - hydrographs["Time (hrs)"][0]
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
            "out_path": out_path,
        }
        dbdictionary = pd.DataFrame(data=dictionary)
        warnings.simplefilter(action="ignore", category=FutureWarning)
        durexcdb = pd.concat([durexcdb, dbdictionary], ignore_index=True)
    return durexcdb


if __name__ == "__main__":

    print("Recursively searching for **/*batch.out files - can take a while")
    file_list = [f for f in iglob("**/*batch.out", recursive=True) if os.path.isfile(f)]
    file_list

    files_df = None
    batch_list = []
    for batchout_file in file_list:
        batch_list.append(process_batchout(batchout_file))
        files_df = pd.concat(batch_list, ignore_index=True)
    files_df
    DateTimeString = datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    output = f"{DateTimeString}_batchouts.csv"
    files_df.to_csv(output, index=False)

    db = files_df

    # durexcdb=pd.DataFrame (columns=['AEP','Duration','TP','Location','ThresholdFlow','Duration_Exceeding', 'out_path'])
    # db=pd.read_excel (r"P:\P21182.01 NANUTARRA ROAD DD\200 CALC\200.2 Civil\Flooding\Hydrology\RORB Models\all ensemble results.xlsx",sheet_name='Samson_files')
    db = files_df
    qcheck = list(range(1, 10, 1)) + list(range(10, 100, 2)) + list(range(100, 2100, 10))
    reslen = db.shape[0]

    rorb_csvs = []
    for i in range(reslen):
        # print (i, ' of', reslen)
        # path is for the batch_out, csv is the individual storm hydrograph
        # [aep,dur,tp, out_path, csv_read]
        rline = (
            db["AEP"][i],
            db["Duration"][i],
            db["TPat"][i],
            db["Path"][i],
            db["csv"][i],
        )
        rorb_csvs.append(rline)

    numFiles = len(rorb_csvs)
    numThreads = calcNumWorkersCores(numFiles)
    print("")
    print(f"Processing {numFiles} with {numThreads} threads")
    # numFiles = 1

    # for num, dir in enumerate(dir_list, start=1):
    # processFolder(num, dir)

    with multiprocessing.Pool(numThreads) as p:
        hydroCSV = partial(process_hydrographCSV, qcheckList=qcheck)  # qcheck is a constant list
        rorbDATA = p.starmap(hydroCSV, enumerate(rorb_csvs, start=1))

    print("now to merge")
    # df = pd.DataFrame()
    df = pd.concat(rorbDATA)

    # for q in rorbDATA:
    # # print(q.head())
    # df.append(q)

    DateTimeString = datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    output = f"{DateTimeString}_durex"
    print("")
    print("Outputting durex.csv")
    df.to_parquet(f"{output}.parquet.gzip")
    df.to_csv(f"{output}.csv", index=False)

    finaldb = pd.DataFrame(
        columns=[
            "Path",
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
    for pathgrp in df.groupby(by="out_path"):
        for locgrp in pathgrp[1].groupby(by="Location"):
            for flowgrp in locgrp[1].groupby(by="ThresholdFlow"):
                for AEPgrp in flowgrp[1].groupby(by="AEP"):
                    result = [pathgrp[0], locgrp[0], flowgrp[0], AEPgrp[0]] + stats(
                        AEPgrp[1], "Duration_Exceeding", "TP", "Duration"
                    )
                    finaldb.loc[len(finaldb)] = result
    print("Outputting QvsTexc.csv")
    DateTimeString = datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    save_as = f"{DateTimeString}_QvsTexc_TuesdayMorning.csv"
    finaldb.to_csv(save_as, index=False)
    print("")
    print("End")
