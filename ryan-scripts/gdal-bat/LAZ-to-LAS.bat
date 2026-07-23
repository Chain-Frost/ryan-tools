@echo off
setlocal

set "PDAL=C:\Program Files\QGIS 4.0.1\bin\pdal.exe"
set "SOURCE=Q:\Library\GIS\AAM_LiDAR_ONLY_2021_2022\AAM_2021_2022_LiDAR_Final_Delivery\RFRP_Brockman_Oct2021\BR_FINAL_SUPPLY_2022_FEB\52_01_Classified_LAZ\01_Classified_LAZ"
set "OUTPUT=Q:\Library\GIS\AAM_LiDAR_ONLY_2021_2022\AAM_2021_2022_LiDAR_Final_Delivery\RFRP_Brockman_Oct2021\BR_FINAL_SUPPLY_2022_FEB\52_01_Classified_LAZ\02_Classified_LAS"

if not exist "%PDAL%" (
    echo ERROR: PDAL not found:
    echo %PDAL%
    exit /b 1
)

if not exist "%SOURCE%\" (
    echo ERROR: Source folder not found:
    echo %SOURCE%
    exit /b 1
)

if not exist "%OUTPUT%\" mkdir "%OUTPUT%"

set /a COUNT=0

for %%F in ("%SOURCE%\*.laz") do (
    echo Converting: %%~nxF
    "%PDAL%" translate "%%~fF" "%OUTPUT%\%%~nF.las"

    if errorlevel 1 (
        echo ERROR: Conversion failed for:
        echo %%~fF
        exit /b 1
    )

    set /a COUNT+=1
)

if %COUNT% EQU 0 (
    echo ERROR: No LAZ files found.
    exit /b 1
)

echo.
echo Conversion complete: %COUNT% file(s).
echo Output folder:
echo %OUTPUT%

endlocal