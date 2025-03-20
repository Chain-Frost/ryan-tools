@echo off
REM --------------------------------------------------
REM Set Python environment variable (adjust if needed)
REM --------------------------------------------------
set PYTHONHOME=C:\Program Files\QGIS 3.40.3\apps\Python312

REM --------------------------------------------------
REM Define paths for the QGIS python executable and gdal_calc.py
REM --------------------------------------------------
set "PYTHON_EXE=C:\Program Files\QGIS 3.40.3\bin\python.exe"
set "GDAL_CALC=C:\Program Files\QGIS 3.40.3\apps\Python312\Scripts\gdal_calc.py"

REM --------------------------------------------------
REM Define paths for the input NDVI file and the output file
REM --------------------------------------------------
set "NDVI=C:\Users\Brenda.Arciniegas\Downloads\NDVI\NDVI.tif"
set "OUT=C:\Users\Brenda.Arciniegas\Downloads\NDVI\NDVI_reclass_v5.tif"

REM --------------------------------------------------
REM Check if gdal_calc.py exists at the specified path
REM --------------------------------------------------
if not exist "%GDAL_CALC%" (
    echo gdal_calc.py not found at %GDAL_CALC%.
    echo Please verify the path and update the batch file.
    pause
    exit /b 1
)

REM --------------------------------------------------
REM Reclassify NDVI using the new rules:
REM   NDVI <  0.0  => -9999 (NoData, Water, Out)
REM   0.0 - 0.15   => 2  (Cleared areas, Bare n=0.04)
REM   0.15 - 0.35  => 3  (General Grass, Low n=0.05)
REM   0.35 - 0.5   => 4  (Moderate vegetation, n=0.06)
REM   >= 0.5      => 5  (Riparian vegetation)
REM --------------------------------------------------
"%PYTHON_EXE%" "%GDAL_CALC%" ^
  -A "%NDVI%" ^
  --outfile="%OUT%" ^
  --calc="(A<0)*(-9999) + (A>=0)*(A<0.15)*2 + (A>=0.15)*(A<0.35)*3 + (A>=0.35)*(A<0.5)*4 + (A>=0.5)*5" ^
  --NoDataValue=-9999 ^
  --type=Int16 ^
  --overwrite

echo Reclassification complete!
pause

