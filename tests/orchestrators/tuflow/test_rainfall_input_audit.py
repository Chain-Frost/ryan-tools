from pathlib import Path

import pandas as pd

from ryan_library.functions.tuflow.command_plan import CommandPlan, PlannedRun
from ryan_library.orchestrators.tuflow.rainfall_input_audit import audit_command_plan, factor_groups, _material_definition

class GisPath:
    def gis_attributes(self):
        return [{'Name': 'RF1', 'f1': 1.5, 'f2': 0.0}]

class GisInput:
    lhs='Read GIS RF'
    files=[GisPath()]

class BC:
    df=pd.DataFrame({'Source':['event.csv'], 'Column 2':['TP01'], 'Mult Col 2':[1.2], 'Add Col 2':[0.]}, index=['RF1'])
    def value(self,name):
        assert name=='RF1'
        return pd.DataFrame({'TP01':[10.,20.]})

class Materials:
    df=pd.DataFrame({'Rainfall Losses':['30,5','0,0'], 'Fraction Impervious':[0.,.5]}, index=[1,2])

class Soils:
    df=pd.DataFrame({'Method':['ILCL']}, index=[3])
    def value(self, id):
        return {'method':'ILCL','IL':20.,'CL':3.}

class Context:
    def get_inputs(self):return [GisInput()]
    def bc_dbase(self):return BC()
    def mat_file(self):return Materials()
    def soils_file(self):return Soils()

class Model:
    def __init__(self,path):self.path=path
    def context(self,flags):return Context()


def test_selected_run_inventory_and_factor_groups():
    runs = tuple(
        PlannedRun(
            index=n,
            source=Path("runs.txt"),
            line_start=n,
            line_end=n,
            tcf=Path("model.tcf"),
            working_directory=Path.cwd(),
            flag_pairs=(("e1", event),),
            arguments=("-e1", event),
            executable=None,
        )
        for n, event in ((1, "a"), (2, "b"))
    )
    plan = CommandPlan(
        source=Path("runs.txt"), runs=runs, directories=(), ignored_statements=0, syntax="commands"
    )
    audit=audit_command_plan(plan,model_factory=Model)
    assert len(audit.runs)==2
    assert audit.runs[0].rainfall[0].bc_adjusted_sum_mm==30.
    assert audit.runs[0].rainfall[0].spatially_adjusted_sum_mm is None
    assert audit.runs[0].materials[0].initial_loss_mm==30.
    assert audit.runs[0].soils[0].parameters[0]==('CL','3.0')
    assert audit.runs[0].spatial_combinations_verified is False
    assert len(factor_groups(audit))==1
    assert list(factor_groups(audit).values())==[(1,2)]
    assert any(f.code=='SPATIAL_COMBINATION_UNRESOLVED' for f in audit.runs[0].findings)


def test_unresolved_material_does_not_become_zero():
    material=_material_definition(1, {'Rainfall Losses':'', 'Fraction Impervious':0.})
    assert material.status=='unresolved' and material.initial_loss_mm is None
