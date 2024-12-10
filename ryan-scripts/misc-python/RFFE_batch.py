import requests
import pandas as pdd
import os

# input_catchments.csv
# Catchment,AreaKm2,OutletX,OutletY,CentroidY,CentroidX
# 10,338.7673404,120.4806,-23.386,-23.4811,120.5775
# 20,576.3559263,120.4358,-23.3459,-23.4877,120.5328

# Define the source folder path
# source_folder = r'P:\BGER\PER\RP19190.029 DAVIDSON CREEK TRANSPORT STUDY - HANROY\4 ENGINEERING\3 CIVIL\waterways\rffe' #edit this if needed
source_folder = os.getcwd()


def fetch_rffe(lono, lato, lonc, latc, area):
    print("fetching RFFE")
    rffe = "http://rffe.arr-software.org"
    args = {
        "catchment_name": "catchment1",
        "lato": str(lato),
        "lono": str(lono),
        "latc": str(latc),
        "lonc": str(lonc),
        "area": str(area),
    }
    rfferespond = requests.post(rffe, data=args)
    return rfferespond


def clean_rffe(rfferesults):
    lines = rfferesults.replace(" ", "")
    # reading xml (weird syntax hence string instead of parsing)
    dfrffe = pdd.DataFrame(columns=["aep", "upper_limit", "lower_limit", "flow"])
    print(lines)
    lines = lines.split("results=[{")[1].split("}]")[0]
    lines = (lines.replace("},{", "};{")).replace("{", "").replace("}", "").split(";")
    for line in lines:
        m = line.split(",")
        k = []
        for i in m:
            test = float((i.split(":")[1]).replace("'", ""))
            k.append(test)
        dfrffe.loc[len(dfrffe)] = k
    return dfrffe


def save_results_to_files(catchment, folderpath, rfferesponse):
    import os

    with open(os.path.join(folderpath, catchment + "_rffe.txt"), "w") as rffefile:
        rffefile.write(rfferesponse.text)


def main(lato, lono, latc, lonc, area, Name, outfolder):
    # lato, lono,latc,lonc,area,Name= -33.8783,150,-33.9607,150.752,88,'Catchment1'
    # outfolder= r'C:\Users\Animesh.Paudel\Desktop\Temp\Python training- web access'

    rfferesponse = fetch_rffe(lono, lato, lonc, latc, area)
    # SAVE TO FILES-----------
    save_results_to_files(Name, outfolder, rfferesponse)
    # process results to tables
    rffe = clean_rffe(rfferesponse.text)
    rffe["Catchment"] = Name
    return rffe


input_csv_path = os.path.join(
    source_folder, "input_catchments.csv"
)  # edit this if needed
ctchdata = pdd.read_csv(input_csv_path)
r = len(ctchdata)
rffes = pdd.DataFrame()
for i in range(r):
    print("Working ", str(i + 1), " of ", str(r))
    Name, area = str(ctchdata["Catchment"][i]), ctchdata["AreaKm2"][i]
    lato, lono, latc, lonc = (
        ctchdata["OutletY"][i],
        ctchdata["OutletX"][i],
        ctchdata["CentroidY"][i],
        ctchdata["CentroidX"][i],
    )
    outfolder = source_folder  # edit this if desired
    print(lato, lono, latc, lonc, area, Name, outfolder)
    rffe = main(lato, lono, latc, lonc, area, Name, outfolder)
    rffes = pdd.concat([rffes, rffe], ignore_index=True)

# Construct the path for the output CSV file using os.path.join
output_csv_path = os.path.join(source_folder, "rffe.csv")

# Save the DataFrame to a CSV file in the source folder
rffes.to_csv(output_csv_path, index=False)
