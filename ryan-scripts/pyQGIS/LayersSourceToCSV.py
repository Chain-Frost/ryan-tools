import csv

layer_paths = [[layer.name(), layer.source()] for layer in QgsProject.instance().mapLayers().values()]
csv_path = r"C:\temp\qgis_output.csv"

with open(csv_path, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    for paths in layer_paths:
        writer.writerow(paths)
