set asc_to_asc="Q:\TUFLOW\asc_to_asc.2024-06-AE\asc_to_asc_w64.exe"

set Results="Q:\25\RP25010 TOWER H DR D - GENMIN\TUFLOW_TOWERH\results\v18\Stage1"


%asc_to_asc% -b -out TowerH_01.00p_Stage1_v18_d_Max.tif -Max %Results%\RORB\01.00p\grids\TowerH_v18_Stage1_01.00p_*_04M_RORB+*_SWA+Q_d_Max.tif
%asc_to_asc% -b -out TowerH_01.00p_Stage1_v18_h_Max.tif -Max %Results%\RORB\01.00p\grids\TowerH_v18_Stage1_01.00p_*_04M_RORB+*_SWA+Q_h_Max.tif
%asc_to_asc% -b -out TowerH_01.00p_Stage1_v18_V_Max.tif -Max %Results%\RORB\01.00p\grids\TowerH_v18_Stage1_01.00p_*_04M_RORB+*_SWA+Q_V_Max.tif
																			 
%asc_to_asc% -b -out TowerH_02.00p_Stage1_v18_d_Max.tif -Max %Results%\RORB\02.00p\grids\TowerH_v18_Stage1_02.00p_*_04M_RORB+*_SWA+Q_d_Max.tif
%asc_to_asc% -b -out TowerH_02.00p_Stage1_v18_h_Max.tif -Max %Results%\RORB\02.00p\grids\TowerH_v18_Stage1_02.00p_*_04M_RORB+*_SWA+Q_h_Max.tif
%asc_to_asc% -b -out TowerH_02.00p_Stage1_v18_V_Max.tif -Max %Results%\RORB\02.00p\grids\TowerH_v18_Stage1_02.00p_*_04M_RORB+*_SWA+Q_V_Max.tif
																			 
%asc_to_asc% -b -out TowerH_05.00p_Stage1_v18_d_Max.tif -Max %Results%\RORB\05.00p\grids\TowerH_v18_Stage1_05.00p_*_04M_RORB+*_SWA+Q_d_Max.tif
%asc_to_asc% -b -out TowerH_05.00p_Stage1_v18_h_Max.tif -Max %Results%\RORB\05.00p\grids\TowerH_v18_Stage1_05.00p_*_04M_RORB+*_SWA+Q_h_Max.tif
%asc_to_asc% -b -out TowerH_05.00p_Stage1_v18_V_Max.tif -Max %Results%\RORB\05.00p\grids\TowerH_v18_Stage1_05.00p_*_04M_RORB+*_SWA+Q_V_Max.tif
																			 
%asc_to_asc% -b -out TowerH_10.00p_Stage1_v18_d_Max.tif -Max %Results%\RORB\10.00p\grids\TowerH_v18_Stage1_10.00p_*_04M_RORB+*_SWA+Q_d_Max.tif
%asc_to_asc% -b -out TowerH_10.00p_Stage1_v18_h_Max.tif -Max %Results%\RORB\10.00p\grids\TowerH_v18_Stage1_10.00p_*_04M_RORB+*_SWA+Q_h_Max.tif
%asc_to_asc% -b -out TowerH_10.00p_Stage1_v18_V_Max.tif -Max %Results%\RORB\10.00p\grids\TowerH_v18_Stage1_10.00p_*_04M_RORB+*_SWA+Q_V_Max.tif


pause