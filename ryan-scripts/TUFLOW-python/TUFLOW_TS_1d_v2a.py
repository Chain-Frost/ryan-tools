from logging import raiseExceptions
import pandas as pd
import os
from glob import iglob
import csv
from datetime import datetime
import multiprocessing

# import sys
# from functools import partial
import numpy as np
import pprint
from pandas import DataFrame
import shapefile
from math import pi


def make_file_list(searchString):
    textString = r"**/*" + searchString
    print(f"Recursively searching for {textString} files - can take a while")
    file_list = [f for f in iglob(textString, recursive=True) if os.path.isfile(f)]
    print(file_list)
    print("")
    return file_list


def calcNumWorkersCores(numFiles) -> int:
    splits: int = max(numFiles // 3, 1)
    if multiprocessing.cpu_count() <= 2:
        return min(2, splits)
    elif multiprocessing.cpu_count() <= 4:
        return min(multiprocessing.cpu_count(), splits)
    elif multiprocessing.cpu_count() <= 20:
        return min(multiprocessing.cpu_count() - 1, splits)
    else:
        return min(20, splits)


def processCmx(maxData):
    cleanedData = []
    for row in maxData:
        cleanedData.append([row[0], row[2], row[1], None])
        cleanedData.append([row[0], row[4], None, row[3]])
    # print(cleanedData)
    # we want to generate ['Chan ID', 'Time', 'Q', 'V', 'US_h', 'DS_h'. .1 is U
    dfData = pd.DataFrame(data=cleanedData, columns=["Chan ID", "Time", "Q", "V"])
    return dfData


def processNmx(maxData):
    cleanedData = []
    for row in maxData:  # note that maxData excludes the first column
        if row[0][-2:] == ".1":  # upstream node
            cleanedData.append([row[0][:-2], row[2], row[1], None])
        elif row[0][-2:] == ".2":  # .2
            cleanedData.append([row[0][:-2], row[2], None, row[1]])
        else:
            raise ValueError(f"Unhandled node index: {row[0]}, {row[0][-2:]}")
            # just put pass here if you want to ignore this error and comment out the above line.
            # pass
    # print(cleanedData)
    dfData = pd.DataFrame(data=cleanedData, columns=["Chan ID", "Time", "US_h", "DS_h"])
    return dfData


def process1dChan(maxData):
    dfData = pd.DataFrame(data=maxData[1:], columns=maxData[0])
    # [Channel	US Node	DS Node	US Channel	DS Channel	Flags	Length	Form Loss	n or Cd	pSlope	US Invert	DS Invert	LBUS Obvert	RBUS Obvert	LBDS Obvert	RBDS Obvert	pBlockage]
    # now we want to drop columns, and then set datatypes
    # df.drop(['column_nameA', 'column_nameB'], axis=1, inplace=True)
    # where 1 is the axis number (0 for rows and 1 for columns.)
    # print(dfData.columns)
    dfData = dfData.astype(
        {
            "Channel": "string",
            "US Node": "string",
            "DS Node": "string",
            "US Channel": "string",
            "DS Channel": "string",
            "Flags": "string",
        }
    )
    dfData = dfData.astype(
        {
            "Length": "float64",
            "Form Loss": "float64",
            "n or Cd": "float64",
            "pSlope": "float64",
            "US Invert": "float64",
            "DS Invert": "float64",
            "LBUS Obvert": "float64",
            "RBUS Obvert": "float64",
            "LBDS Obvert": "float64",
            "RBDS Obvert": "float64",
            "pBlockage": "float64",
        }
    )
    dfData.drop(
        [
            "US Node",
            "DS Node",
            "US Channel",
            "DS Channel",
            "Form Loss",
            "RBUS Obvert",
            "RBDS Obvert",
        ],
        axis=1,
        inplace=True,
    )
    dfData["Height"] = dfData["LBUS Obvert"] - dfData["US Invert"]
    # to match the cmx and nmx naming
    dfData.rename(columns={"Channel": "Chan ID"}, inplace=True)
    return dfData


def process1dCSV(file):
    print(file)
    with open(file, "r") as csvfile:

        reader = csv.reader(csvfile)
        csvdata = list(reader)
        fileName = os.path.basename(file)

        # skip the title row, and skip the first column
        maxData = [line[1:] for line in csvdata[:]]
        # print(maxData)
        # we want to generate ['Chan ID', 'Time', 'Q', 'V', 'US_h', 'DS_h'. .1 is US for cmx nmx. 1dchan is a bit different

        if "_1d_Chan" in fileName:  # 1d_chan
            dfData = process1dChan(maxData)
            internalName = fileName[:-12]
        else:
            raise ValueError(
                f"Filename does not have expected style. This error is very unexpected. {fileName}"
            )

        # split the run code up by '_'
        dfData["internalName"] = internalName
        for elem in enumerate(internalName.split("_"), start=1):
            dfData[f"R{elem[0]}"] = elem[1]
        dfData["path"] = file
        dfData["file"] = fileName
    return dfData  # , internalName


def process1dcca(dbf_file):
    with open(dbf_file, "rb") as mydbf:
        sf = shapefile.Reader(dbf=mydbf)
        ccArecords = sf.records()
        # skip the first field which is always DeletionFlag
        fieldnames = [f[0] for f in sf.fields[1:]]
        # print(fieldnames)
        ccaData = pd.DataFrame(list(ccArecords), columns=fieldnames)
        ccaData.rename(columns={"Channel": "Chan ID"}, inplace=True)

        fileName = os.path.basename(dbf_file)
        internalName = fileName[:-13]
        ccaData["internalName"] = internalName
        for elem in enumerate(internalName.split("_"), start=1):
            ccaData[f"R{elem[0]}"] = elem[1]
        ccaData["path"] = dbf_file
        ccaData["file"] = fileName
        # print(ccaData)
    return ccaData


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
        chan_list = [
            c.split("[")[0].removeprefix(f"{val_type} ") for c in csvdata[0][2:]
        ]
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
                    adj_data.append(
                        [chan_list[enum].strip(), line[1].strip(), val.strip()]
                    )
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
        col_names = {
            "Chan ID": "string",
            "Time": "float64",
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


# df_results_list of the form [internalName, df_blobs]
def group_results_df(df_results_list):
    print(list(df_results_list[0]["internalName"].unique()))
    smalldf = pd.concat(df_results_list)
    smalldf.drop(["path", "file"], axis=1, inplace=True)
    # print('Setting index')
    # smalldf.set_index(['internalName', 'Chan ID', 'Time'], inplace=True)
    # gc = ['internalName', 'Chan ID', 'Time']
    # print(f'Grouping by {gc}')
    # group all by run code and channel - merges multiple rows. uses level instead of by as we have multiindex
    smalldf = smalldf.groupby(
        by=["internalName", "Chan ID", "Time"], axis=0, dropna=True
    ).max()
    # print(smalldf)
    return smalldf


if __name__ == "__main__":
    ts_file_strings = ["_1d_CF.csv", "_1d_H.csv", "_1d_Q.csv", "_1d_V.csv"]
    ts_file_list = []
    for file_string in ts_file_strings:
        ts_file_list.extend(make_file_list(file_string))
    chan_file_list = make_file_list("_1d_Chan.csv")
    ccafile_list = make_file_list("_1d_ccA_L.dbf")
    pprint.pprint(ts_file_list)

    # also need the 1d_chan file data to get pipe size
    # use the 1d_cca pipe area to get the number of pipes

    print("")
    pprint.pprint(ccafile_list)
    print("")
    pprint.pprint(chan_file_list)
    print("")
    numFiles = calcNumWorkersCores(len(ts_file_list))
    with multiprocessing.Pool(numFiles) as p:
        resultsData = p.map(processTScsv, ts_file_list)

        # to do. make a new helper function that gives internal name back so we can group the results by internal name, then merge them before we make a big bloc (groupby slow on the blob)
    # make a list of unique runcodes
    runCodes = [r[1] for r in resultsData]
    runCodes = set(runCodes)
    print(runCodes)
    print(" ")

    half_done_results = []
    for run in runCodes:
        # make a new list of the dataframes for each run code, then process by run codes in multiproc
        print(run)
        smalldf_list = [d[0] for d in resultsData if d[1] == run]
        # it doesn't matter what the runcode is as we are grouping by runcode
        half_done_results.append(smalldf_list)
    with multiprocessing.Pool(numFiles) as p:
        half_merge_resultsData = p.map(
            group_results_df, half_done_results
        )  # df, internalName

    print("")
    print("Merging small dfs")
    df = pd.concat(half_merge_resultsData)
    df = df.fillna(value=np.nan)
    # df.reset_index(inplace=True)

    print(df)
    print(df.columns)
    # print(df[['internalName', 'Chan ID', 'Time']])
    # rint(df[['internalName', 'Chan ID', 'Time']].nunique())

    numFiles = calcNumWorkersCores(len(chan_file_list))
    with multiprocessing.Pool(numFiles) as p:
        chanData = p.map(process1dCSV, chan_file_list)
    dfchan = pd.concat(chanData)
    dfchan = dfchan.fillna(value=np.nan)
    # print(dfchan.columns)
    # reduce number of columns as we don't want to be cluttered with all the data
    dfchan_trim = dfchan[
        [
            "internalName",
            "Chan ID",
            "Flags",
            "US Invert",
            "DS Invert",
            "Length",
            "pBlockage",
            "LBUS Obvert",
        ]
    ]
    # print(dfchan_trim)

    print("")
    numFiles = calcNumWorkersCores(len(ccafile_list))
    with multiprocessing.Pool(numFiles) as p:
        ccaResultsData = p.map(process1dcca, ccafile_list)
    if ccaResultsData:
        dfcca = pd.concat(ccaResultsData)
        dfcca = dfcca.fillna(value=np.nan)
        dfcca = dfcca.astype(
            {
                "pFull_Max": "float64",
                "pTime_Full": "float64",
                "Area_Max": "float64",
                "Area_Culv": "float64",
                "Dur_Full": "float64",
                "Dur_10pFul": "float64",
                "Sur_CD": "float64",
                "Dur_Sur": "float64",
                "pTime_Sur": "float64",
                "TFirst_Sur": "float64",
            }
        )
        dfcca = dfcca.astype(
            {
                "Chan ID": "string",
                "path": "string",
                "file": "string",
                "internalName": "string",
            }
        )
        dfcca_trim = dfcca[["internalName", "Chan ID", "Area_Culv"]]
        # print(dfcca.columns)
        # print(dfcca.dtypes)
    else:
        dfcca_trim = DataFrame()
    dfculv = pd.concat([dfchan_trim, dfcca_trim])
    # group all by run code and channel - merges multiple rows
    dfculv = dfculv.groupby(["internalName", "Chan ID"], axis=0, dropna=True).max()
    dfculv.reset_index(inplace=True)
    dfculv["Diameter"] = dfculv["LBUS Obvert"] - dfculv["US Invert"]
    # haven't checked if pblockage does anything to this
    if ccaResultsData:
        dfculv["Number"] = 4 / pi * dfculv["Area_Culv"] / dfculv["Diameter"].pow(2)
        dfculv["Number"] = dfculv["Number"].round(2)

    print("")
    print(dfculv)
    print(dfculv.dtypes)
    print("")

    DateTimeString = (
        datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    )
    export_name = f"{DateTimeString}_1d_data_TS.xlsx"
    print(f"Exporting {export_name}")
    df.to_excel(excel_writer=export_name, merge_cells=False, sheet_name="1d_TS_data")

    # not sure what this was doing
    # now make a flat table
    # need to join the tables
    # df.drop(["Time", "path", "file"], axis=1, inplace=True)
    # df = df.groupby(["internalName", "Chan ID"], axis=0, dropna=True).max()
    # df.reset_index(inplace=True)

    # export_name = f"{DateTimeString}_1d_data_TS_flat.xlsx"
    # print(f"Exporting {export_name}")
    # df.to_excel(export_name)

    print("Done")
