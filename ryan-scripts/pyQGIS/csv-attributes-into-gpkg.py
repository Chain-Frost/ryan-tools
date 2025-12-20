from qgis.core import QgsProject, QgsVectorLayer

SRC_NAME = "1d_nwk-GNH"  # CSV layer
DST_NAME = "1d_nwk_GNH_from_DXF_01_L"  # GPKG layer
KEY_FIELD = "ID"

src: QgsVectorLayer = QgsProject.instance().mapLayersByName(SRC_NAME)[0]
dst: QgsVectorLayer = QgsProject.instance().mapLayersByName(DST_NAME)[0]

src_fields = {f.name() for f in src.fields()}
dst_fields = {f.name() for f in dst.fields()}

if KEY_FIELD not in src_fields or KEY_FIELD not in dst_fields:
    raise RuntimeError("ID must exist in both layers.")

# Copy only fields that exist in both layers, excluding fid/FID and ID
common_fields = sorted((src_fields & dst_fields) - {"fid", "FID", KEY_FIELD})

# Build source lookup by ID
src_lookup: dict[object, dict[str, object]] = {}
for f in src.getFeatures():
    k = f[KEY_FIELD]
    if k is None:
        continue
    src_lookup[k] = {name: f[name] for name in common_fields}

# Update destination
dst.startEditing()
dst_fields_obj = dst.fields()

updated = 0
missing = 0

for f in dst.getFeatures():
    k = f[KEY_FIELD]
    data = src_lookup.get(k)
    if data is None:
        missing += 1
        continue

    fid = f.id()
    for name, val in data.items():
        idx = dst_fields_obj.indexFromName(name)
        if idx >= 0:
            dst.changeAttributeValue(fid, idx, val)
    updated += 1

if not dst.commitChanges():
    raise RuntimeError("Commit failed (layer may be read-only, locked, or has constraints).")

print(f"Updated features: {updated}")
print(f"Unmatched IDs:    {missing}")
print(f"Fields updated ({len(common_fields)}): {common_fields}")
