@echo off
setlocal EnableDelayedExpansion

rem ——————————————————————————————
rem  Path to asc_to_asc executable (no embedded quotes)
rem ——————————————————————————————
set "ASC_TO_ASC=Q:\TUFLOW\asc_to_asc.2024-06-AE\asc_to_asc_w64.exe"

rem ——————————————————————————————
rem  Paths to your EXG and Stage1 result folders
rem————————————————————————————————
set "RESULTS_EXG=Q:\25\RP25010 TOWER H DR D - GENMIN\TUFLOW_TOWERH\results\v18\MAX\EXG"
set "RESULTS_STAGE1=Q:\25\RP25010 TOWER H DR D - GENMIN\TUFLOW_TOWERH\results\v18\MAX"

rem ——————————————————————————————
rem  Loop over AEPs and variables
rem————————————————————————————————
for %%A in (01.00p 02.00p 05.00p 10.00p) do (
    for %%V in (d h V) do (
        rem Build full paths
        set "IN1=%RESULTS_EXG%\TowerH_%%A_EXG_v18_%%V_Max.tif"
        set "IN2=%RESULTS_STAGE1%\TowerH_%%A_Stage1_v18_%%V_Max.tif"
        set "OUT=TowerH_%%A_Stage1-EXG_v18_%%V_diff_Max.tif"

        echo ===========================================================
        echo Generating diff for AEP %%A, variable %%V
        echo   EXG:     "!IN1!"
        echo   Stage1:  "!IN2!"
        echo   Output:  "!OUT!"
        echo ===========================================================

        "%ASC_TO_ASC%" -b -diff -out "!OUT!" "!IN2!" "!IN1!"
    )
)

pause
