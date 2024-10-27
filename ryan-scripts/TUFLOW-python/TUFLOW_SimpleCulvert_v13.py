import pandas as pd
import os
from glob import iglob
import csv
from datetime import datetime
import multiprocessing
import numpy as np
import pprint
from pandas import DataFrame
import shapefile
import logging
import geopandas as gpd
from typing import Any
import fiona

# 2024-09-19 Implemented geopackage for 1d_CCA


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def make_file_list(searchString) -> list[str]:
    textString: str = r"**/*" + searchString
    print(f"Recursively searching for {textString} files - can take a while")
    file_list: list[str] = [
        f for f in iglob(pathname=textString, recursive=True) if os.path.isfile(path=f)
    ]
    print(file_list)
    return file_list


def calculate_pool_size(num_files: int) -> int:
    splits: int = max(num_files // 3, 1)
    available_cores: int = min(multiprocessing.cpu_count(), 20)
    calc_threads: int = min(available_cores - 1, splits)
    logging.info(msg=f"Processing threads: {calc_threads}")
    return calc_threads


def processCmx(maxData) -> DataFrame:
    cleanedData: list[Any] = []
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


def process1dChan(maxData) -> DataFrame:
    dfData: DataFrame = pd.DataFrame(data=maxData[1:], columns=maxData[0])
    # [Channel	US Node	DS Node	US Channel	DS Channel	Flags	Length	Form Loss	n or Cd	pSlope	US Invert	DS Invert	LBUS Obvert	RBUS Obvert	LBDS Obvert	RBDS Obvert	pBlockage]
    # now we want to drop columns, and then set datatypes
    # df.drop(['column_nameA', 'column_nameB'], axis=1, inplace=True)
    # where 1 is the axis number (0 for rows and 1 for columns.)
    # print(dfData.columns)
    dfData = dfData.astype(
        dtype={
            "Channel": "string",
            "US Node": "string",
            "DS Node": "string",
            "US Channel": "string",
            "DS Channel": "string",
            "Flags": "string",
        }
    )
    dfData = dfData.astype(
        dtype={
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
        labels=[
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


def processCSV(file) -> DataFrame:
    print(file)
    with open(file=file, mode="r") as csvfile:

        reader = csv.reader(csvfile)
        csvdata = list(reader)
        fileName: str = os.path.basename(file)

        # skip the title row, and skip the first column
        maxData: list[list[str]] = [line[1:] for line in csvdata[:]]
        # print(maxData)
        # we want to generate ['Chan ID', 'Time', 'Q', 'V', 'US_h', 'DS_h'. .1 is US for cmx nmx. 1dchan is a bit different
        if "_1d_Cmx" in fileName:  # two sets of data. Qmax first then Vmax.
            dfData: DataFrame = processCmx(maxData[1:])
            # the bit in cell A1 with the name but skip the path to the tcf
            internalName = csvdata[0][0].split("[")[0][0:-1]
            # Chan ID	Qmax	Time Qmax	Vmax	Time Vmax
        elif "_1d_Nmx" in fileName:  # Nmx
            dfData = processNmx(maxData[1:])
            # the bit in cell A1 with the name but skip the path to the tcf
            internalName = csvdata[0][0].split("[")[0][0:-1]
        elif "_1d_Chan" in fileName:  # 1d_chan
            dfData = process1dChan(maxData)
            internalName = fileName[:-12]
        else:
            raise ValueError(f"Could not determine type of {fileName}")

        # split the run code up by '_'
        dfData["internalName"] = internalName
        for elem in enumerate(
            iterable=internalName.replace("+", "_").split(sep="_"), start=1
        ):
            dfData[f"R{elem[0]}"] = elem[1]
        dfData["path"] = file
        dfData["file"] = fileName
    return dfData


def process1dcca_dbf(dbf_file) -> DataFrame:
    """Process 1D CCA data from a dbf file."""
    with open(dbf_file, "rb") as mydbf:
        sf = shapefile.Reader(dbf=mydbf)
        ccArecords = sf.records()
        # skip the first field which is always DeletionFlag
        fieldnames = [f[0] for f in sf.fields[1:]]
        ccaData = pd.DataFrame(list(ccArecords), columns=fieldnames)
        ccaData.rename(columns={"Channel": "Chan ID"}, inplace=True)

        fileName = os.path.basename(dbf_file)
        internalName = fileName[:-13]
        ccaData["internalName"] = internalName
        for idx, elem in enumerate(internalName.replace("+", "_").split("_"), start=1):
            ccaData[f"R{idx}"] = elem
        ccaData["path"] = dbf_file
        ccaData["file"] = fileName

    return ccaData


def process1dcca_geopackage(geopackage_file) -> DataFrame:
    # Get the list of layers in the geopackage

    if not os.path.exists(geopackage_file):
        raise FileNotFoundError(f"{geopackage_file} does not exist.")

    try:
        # Use fiona to list the layers in the geopackage file
        with fiona.Env():
            layers = fiona.listlayers(geopackage_file)
    except Exception as e:
        raise ValueError(f"Error reading geopackage layers: {e}")

    # Find the layer that ends with '1d_ccA_L'
    layer_name = next((layer for layer in layers if layer.endswith("1d_ccA_L")), None)

    if layer_name is None:
        raise ValueError("No layer found with '1d_ccA_L' in the geopackage.")

    # Load the layer into a GeoDataFrame
    gdf = gpd.read_file(geopackage_file, layer=layer_name)

    # Convert the GeoDataFrame to a regular DataFrame (if geometry isn't needed)
    ccaData = pd.DataFrame(gdf.drop(columns="geometry"))

    # Rename the "Channel" field to "Chan ID", assuming the column exists
    if "Channel" in ccaData.columns:
        ccaData.rename(columns={"Channel": "Chan ID"}, inplace=True)

    fileName = os.path.basename(geopackage_file)
    internalName = fileName[:-15]
    ccaData["internalName"] = internalName

    for idx, elem in enumerate(internalName.replace("+", "_").split("_"), start=1):
        ccaData[f"R{idx}"] = elem

    ccaData["path"] = geopackage_file
    ccaData["file"] = fileName

    return ccaData


def processTS(file):
    pass


def process_csv_files(file_list) -> DataFrame:
    """Process CSV files in parallel."""
    numFiles: int = calculate_pool_size(num_files=len(file_list))
    with multiprocessing.Pool(processes=numFiles) as pool:
        resultsData: list[DataFrame] = pool.map(func=processCSV, iterable=file_list)
    return pd.concat(resultsData)


def process_cca_files(ccafile_list, processor) -> DataFrame:
    """Process CCA files (DBF or GPKG) using the provided processor function."""
    if not ccafile_list:
        logging.info("CCA file list is empty, skipping processing.")
        return pd.DataFrame()

    numFiles = calculate_pool_size(len(ccafile_list))
    with multiprocessing.Pool(numFiles) as pool:
        ccaResultsData = pool.map(processor, ccafile_list)

    return pd.concat(ccaResultsData)


def export_to_excel(df, suffix) -> None:
    """Export DataFrame to Excel with a timestamp."""
    DateTimeString = datetime.now().strftime("%Y%m%d-%H%M")
    export_name = f"{DateTimeString}_1d_data_{suffix}.xlsx"
    logging.info(f"Exporting {export_name}")
    df.to_excel(export_name)


def main() -> None:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    setup_logging()

    # Find files
    cfile_list = make_file_list("_1d_Cmx.csv")
    nfile_list = make_file_list("_1d_Nmx.csv")
    chanfile_list = make_file_list("_1d_Chan.csv")
    ccafile_list = make_file_list("_1d_ccA_L.dbf")
    ccafile_gp_list = make_file_list("_Results1D.gpkg")

    csv_file_list = cfile_list + nfile_list + chanfile_list

    pprint.pprint(csv_file_list)
    pprint.pprint(ccafile_list)
    pprint.pprint(ccafile_gp_list)

    # Process CSV files
    logging.info("Processing CSV files (Cmx, Nmx, Chan)...")
    df: DataFrame = process_csv_files(file_list=csv_file_list)
    df.fillna(value=np.nan, inplace=True)
    df = df.astype(
        dtype={
            "Time": "float64",
            "Q": "float64",
            "V": "float64",
            "US_h": "float64",
            "DS_h": "float64",
            "Chan ID": "string",
            "path": "string",
            "file": "string",
            "internalName": "string",
        }
    )

    # Process DBF CCA files
    logging.info(msg="Processing DBF CCA files...")
    if ccafile_list:
        dfcca_dbf = process_cca_files(ccafile_list, process1dcca_dbf)
        df = pd.concat([df, dfcca_dbf], ignore_index=True)

    # Process GPKG CCA files
    logging.info("Processing GPKG CCA files...")
    if ccafile_gp_list:
        dfcca_gp = process_cca_files(ccafile_gp_list, process1dcca_geopackage)
        df = pd.concat([df, dfcca_gp], ignore_index=True)

    # Final processing
    df.reset_index(inplace=True)
    logging.info(df.dtypes)

    # Export data
    export_to_excel(df, "max")

    # Create flat table
    df.drop(["Time", "path", "file"], axis=1, inplace=True)
    df = df.groupby(["internalName", "Chan ID"]).max().reset_index()
    export_to_excel(df, "flat")

    logging.info("Done")


def main_old() -> None:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(script_dir)
    setup_logging()
    # '1d_V.csv'
    cfile_list = make_file_list("_1d_Cmx.csv")
    nfile_list = make_file_list("_1d_Nmx.csv")
    chanfile_list = make_file_list("_1d_Chan.csv")
    file_list = cfile_list + nfile_list + chanfile_list
    ccafile_list = make_file_list("_1d_ccA_L.dbf")
    ccafile_gp_list = make_file_list("_Results1D.gpkg")
    pprint.pprint(file_list)
    pprint.pprint(ccafile_list)
    pprint.pprint(ccafile_gp_list)

    print("  --now cmx, nmx, 1dchan")
    numFiles = calculate_pool_size(len(file_list))
    with multiprocessing.Pool(numFiles) as p:
        resultsData = p.map(processCSV, file_list)
    # resultsData

    df = pd.concat(resultsData)
    df = df.fillna(value=np.nan)
    df = df.astype(
        {
            "Time": "float64",
            "Q": "float64",
            "V": "float64",
            "US_h": "float64",
            "DS_h": "float64",
        }
    )
    df = df.astype(
        {
            "Chan ID": "string",
            "path": "string",
            "file": "string",
            "internalName": "string",
        }
    )

    print(" --now 1dcc1")
    if ccafile_list == []:
        print(
            "ccafile_list is empty - skipping - this might be using GPKG which I haven't implemented yet"
        )
    else:
        numFiles = calculate_pool_size(len(ccafile_list))
        with multiprocessing.Pool(numFiles) as p:
            ccaResultsData = p.map(process1dcca_dbf, ccafile_list)
        print(" --1d cca loaded")
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
        print(dfcca.columns)
        print(dfcca.dtypes)
        df = pd.concat([df, dfcca])
    df.reset_index(inplace=True)

    print("")
    print(df)
    print(df.dtypes)
    print("")

    DateTimeString = (
        datetime.now().strftime("%Y%m%d") + "-" + datetime.now().strftime("%H%M")
    )
    export_name = f"{DateTimeString}_1d_data_max.xlsx"
    print(f"Exporting {export_name}")
    df.to_excel(export_name)

    # now make a flat table
    df.drop(["Time", "path", "file"], axis=1, inplace=True)
    df = df.groupby(["internalName", "Chan ID"], axis=0, dropna=True).max()
    df.reset_index(inplace=True)

    export_name = f"{DateTimeString}_1d_data_flat.xlsx"
    print(f"Exporting {export_name}")
    df.to_excel(export_name)

    print("Done")


if __name__ == "__main__":
    main()
