from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.tuflow.loss_combinations import (
    ObservedPair, UnresolvedLossCombination, effective_loss_signature,
    group_observed_pairs, observed_pairs_from_check_grids,
)
from ryan_library.orchestrators.tuflow.rainfall_input_audit import MaterialDefinition as Mat, SoilDefinition as Soil


def mat(i, il=0., cl=0., imp=0.):return Mat(i,il,cl,imp,'defined')
def soil(i, il=0., cl=0.):return Soil(i,'ILCL',(('IL',str(il)),('CL',str(cl))),'defined')


def test_only_observed_pairs_and_neutral_collapse():
    mats={1:mat(1),2:mat(2),3:mat(3,20.,3.),4:mat(4,20.,3.)}
    soils={1:soil(1),2:soil(2,10.,1.),3:Soil(3,'NONE',(),'defined')}
    groups=group_observed_pairs([
        ObservedPair(1,2,8),ObservedPair(2,2,9),ObservedPair(3,1,7),
        ObservedPair(4,3,11),ObservedPair(1,1,2),
    ], mats,soils)
    assert sum(g.cell_count for g in groups)==37
    assert len(groups)==3
    assert {g.category for g in groups}=={'no-loss','soil-only','material-only'}
    assert next(g for g in groups if g.category=='soil-only').material_ids==(1,2)
    assert next(g for g in groups if g.category=='material-only').material_ids==(3,4)


def test_impervious_matters_only_when_soil_active():
    wet=soil(2,12.,2.)
    dry=soil(1)
    assert effective_loss_signature(mat(1,imp=.5),wet)[0]=='coupled'
    assert effective_loss_signature(mat(1,imp=.5),dry)[0]=='no-loss'
    assert effective_loss_signature(mat(1,imp=0),wet)[0]=='soil-only'
    assert effective_loss_signature(mat(1,imp=1),wet)[1]!=effective_loss_signature(mat(1,imp=.5),wet)[1]


def test_unsupported_soil_fails_closed():
    with pytest.raises(UnresolvedLossCombination):
        effective_loss_signature(mat(1),Soil(1,'MYSTERY',(), 'defined'))
    with pytest.raises(UnresolvedLossCombination):
        group_observed_pairs([ObservedPair(1,99,1)],{1:mat(1)}, {})
    assert effective_loss_signature(mat(1),Soil(1,'GA',(('Suction','40'),('Hydraulic Conductivity','3')),'defined'))[0]=='soil-only'


def test_raster_pairs_only_observed(tmp_path):
    def write(name, values):
        p=tmp_path/name
        with rasterio.open(p, 'w', driver='GTiff', height=2,width=3,count=1, dtype='int16',crs='EPSG:28350', transform=from_origin(0,2,1,1),nodata=-9999) as ds:
            ds.write(np.asarray(values,dtype='int16'),1)
        return p
    m=write('mat.tif',[[1,1,2],[2,2,-9999]])
    s=write('soil.tif',[[3,3,4],[4,3,3]])
    a=write('active.tif',[[1,0,1],[1,1,1]])
    assert observed_pairs_from_check_grids(m,s,active_mask_grid=a)==(
        ObservedPair(1,3,1),ObservedPair(2,3,1),ObservedPair(2,4,2),
    )
