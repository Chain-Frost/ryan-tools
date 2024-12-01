# ryan_library.functions/tuflow_to_df.py
import pandas as pd
import csv
from typing import Any
import os
from ryan_library.functions.file_utils import is_non_zero_file


def processCmx(maxData) -> pd.DataFrame:
    cleanedData: list[Any] = []
    for row in maxData:
        cleanedData.append([row[0], row[2], row[1], None])
        cleanedData.append([row[0], row[4], None, row[3]])
    # print(cleanedData)
    # we want to generate ['Chan ID', 'Time', 'Q', 'V', 'US_h', 'DS_h'. .1 is U
    dfData: pd.DataFrame = pd.DataFrame(
        data=cleanedData, columns=["Chan ID", "Time", "Q", "V"]
    )
    return dfData


def processNmx(maxData) -> pd.DataFrame:
    cleanedData: list[Any] = []
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
    dfData: pd.DataFrame = pd.DataFrame(
        data=cleanedData, columns=["Chan ID", "Time", "US_h", "DS_h"]
    )
    return dfData


def process1dChan(maxData) -> pd.DataFrame:
    dfData: pd.DataFrame = pd.DataFrame(data=maxData[1:], columns=maxData[0])
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


def processCSV(file) -> pd.DataFrame:
    print(file)
    with open(file=file, mode="r") as csvfile:

        reader = csv.reader(csvfile=csvfile)
        csvdata = list(reader)
        fileName: str = os.path.basename(file)

        # skip the title row, and skip the first column
        maxData = [line[1:] for line in csvdata[:]]
        # print(maxData)
        # we want to generate ['Chan ID', 'Time', 'Q', 'V', 'US_h', 'DS_h'. .1 is US for cmx nmx. 1dchan is a bit different
        if "_1d_Cmx" in fileName:  # two sets of data. Qmax first then Vmax.
            dfData: pd.DataFrame = processCmx(maxData=maxData[1:])
            # the bit in cell A1 with the name but skip the path to the tcf
            internalName: str = csvdata[0][0].split(sep="[")[0][0:-1]
            # Chan ID	Qmax	Time Qmax	Vmax	Time Vmax
        elif "_1d_Nmx" in fileName:  # Nmx
            dfData = processNmx(maxData=maxData[1:])
            # the bit in cell A1 with the name but skip the path to the tcf
            internalName = csvdata[0][0].split(sep="[")[0][0:-1]
        elif "_1d_Chan" in fileName:  # 1d_chan
            dfData = process1dChan(maxData=maxData)
            internalName = fileName[:-12]
        else:
            raise ValueError(f"{fileName} did not match filters")

        # split the run code up by '_'
        dfData["internalName"] = internalName
        for elem in enumerate(iterable=internalName.split(sep="_"), start=1):
            dfData[f"R{elem[0]}"] = elem[1]
        dfData["path"] = file
        dfData["file"] = fileName
    return dfData


# redundant function
def process1dcca(dbf_file) -> pd.DataFrame:
    import shapefile  # type:ignore

    with open(file=dbf_file, mode="rb") as mydbf:
        sf = shapefile.Reader(dbf=mydbf)
        ccArecords: list[Any] = sf.records()
        # skip the first field which is always DeletionFlag
        fieldnames: list[Any] = [f[0] for f in sf.fields[1:]]
        # print(fieldnames)
        ccaData = pd.DataFrame(list(ccArecords), columns=fieldnames)
        ccaData.rename(columns={"Channel": "Chan ID"}, inplace=True)

        fileName = os.path.basename(dbf_file)
        internalName = fileName[:-13]
        ccaData["internalName"] = internalName
        for elem in enumerate(
            internalName.split("_"), start=1
        ):  # need to deal with + sign too, make a function for it.
            # or have it calculated at some other point
            ccaData[f"R{elem[0]}"] = elem[1]
        ccaData["path"] = dbf_file
        ccaData["file"] = fileName
        # print(ccaData)
    return ccaData


def process1dcca(file_path):
    file_extension = os.path.splitext(file_path)[-1].lower()

    if file_extension == ".gpkg":
        return process1dcca_geopackage(file_path)
    elif file_extension == ".dbf":
        return process1dcca_dbf(file_path)
    else:
        print(f"Unsupported file type: {file_extension}")
        return None


def process1dcca_geopackage(geopackage_path):
    import geopandas as gpd
    import fiona

    print(geopackage_path)

    # Check file size
    file_size = os.path.getsize(geopackage_path)
    if file_size == 0:
        print("GeoPackage is empty.")
        return None

    gdf = gpd.read_file(geopackage_path)
    # list the layer names
    layers = fiona.listlayers(geopackage_path)

    # List all layers in the GeoPackage
    # layers = gpd.io.file.fiona.listlayers(geopackage_path)

    # Check if the GeoPackage is empty
    if not layers:
        print("GeoPackage is empty.")
        return None

    # Filter for the layer of interest, which ends with '1d_ccA_L'
    layer_of_interest = [layer for layer in layers if layer.endswith("1d_ccA_L")]

    if not layer_of_interest:
        print("No layer ending with '1d_ccA_L' found.")
        return None

    # Assuming there's only one such layer, or taking the first if there are multiple
    layer_name = layer_of_interest[0]

    # Read the specific layer into a GeoDataFrame
    gdf = gpd.read_file(geopackage_path, layer=layer_name)

    # Convert GeoDataFrame to DataFrame if necessary, dropping geometry column
    ccaData = pd.DataFrame(gdf.drop(columns="geometry"))

    ccaData.rename(columns={"Channel": "Chan ID"}, inplace=True)

    fileName = os.path.basename(geopackage_path)
    # Adjusted to exclude the '_Results1D.gpkg' part for internalName
    internalName = fileName.rsplit("_", 1)[
        0
    ]  # Splits at the last underscore and takes the first part
    ccaData["internalName"] = internalName

    for elem in enumerate(internalName.split("_"), start=1):
        ccaData[f"R{elem[0]}"] = elem[1]

    ccaData["path"] = geopackage_path
    ccaData["file"] = fileName

    return ccaData


def process1dcca_dbf(dbf_file):
    import shapefile

    print(dbf_file)
    if checkEmptyFile(dbf_file):
        return None  # exit if file is empty

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


def checkEmptyFile(file) -> bool:
    # Check if file is empty by checking its size
    if is_non_zero_file(file):
        return False
    else:
        print(f"Skipping empty file: {file}")
        return True
