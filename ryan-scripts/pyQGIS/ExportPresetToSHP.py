suffix = "_export"
pathToFile = r"C:\temp\\"  # needs the extra \ to escape it
theme_Name = "layers"

layer_list = QgsProject.instance().mapThemeCollection().mapThemeVisibleLayers(theme_Name)
for layer in layer_list:
    newName = layer.name() + suffix + ".shp"
    QgsVectorFileWriter.writeAsVectorFormat(layer, pathToFile + newName, "UTF-8", layer.crs(), "ESRI Shapefile")
