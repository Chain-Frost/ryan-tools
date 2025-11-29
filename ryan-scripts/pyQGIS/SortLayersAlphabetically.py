from collections import OrderedDict

root = QgsProject.instance().layerTreeRoot()
LayerNamesEnumDict = lambda listCh: {listCh[q[0]].name() + str(q[0]): q[1] for q in enumerate(listCh)}

mLNED = LayerNamesEnumDict(root.children())
mLNEDkeys = OrderedDict(sorted(LayerNamesEnumDict(root.children()).items())).keys()

mLNEDsorted = [mLNED[k].clone() for k in mLNEDkeys]
root.insertChildNodes(0, mLNEDsorted)
for n in mLNED.values():
    root.removeChildNode(n)
