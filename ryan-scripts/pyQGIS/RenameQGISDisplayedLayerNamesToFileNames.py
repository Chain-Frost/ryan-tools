import os

for layer in QgsProject.instance().mapLayers().values():
    basename = os.path.splitext(os.path.basename(layer.source()))[0]
    layer.setName(basename)
