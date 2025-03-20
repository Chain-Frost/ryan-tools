@echo off
REM ------------------------------------------------------------
REM tif_to_xyz.bat
REM
REM Usage:
REM   tif_to_xyz.bat ["D:\temp\Worsley\Selected\250304_LIDAR_BDY_Merged.tif"] [resolution]
REM
REM If no input file is provided, the default is:
REM   D:\temp\Worsley\Selected\250304_LIDAR_BDY_Merged.tif
REM
REM This script:
REM   1) Determines the output file path by replacing .tif with .xyz.
REM   2) Optionally resamples the input if a resolution is given.
REM   3) Converts the (resampled) TIFF to an XYZ file.
REM      - It tries to use gdal2xyz.py with -skipnodata.
REM      - If gdal2xyz.py is not found, it falls back to gdal_translate (which does not skip NoData).
REM ------------------------------------------------------------

:: Use default input file if none is specified
if "%~1"=="" (
    set "INPUT_TIF=D:\temp\Worsley\Selected\250304_LIDAR_BDY_Merged.tif"
    echo No input file provided. Using default:
    echo %INPUT_TIF%
) else (
    set "INPUT_TIF=%~1"
)

:: Determine output file path by replacing .tif extension with .xyz
for %%F in ("%INPUT_TIF%") do (
    set "OUT_FOLDER=%%~dpF"
    set "BASE_NAME=%%~nF"
)
set "OUTPUT_XYZ=%OUT_FOLDER%%BASE_NAME%.xyz"

:: Check if user specified a resolution for resampling
if not "%~2"=="" (
    set "RESOLUTION=%~2"
    set "DO_RESAMPLE=1"
) else (
    set "DO_RESAMPLE=0"
)

echo.
echo ===========================================
echo  Converting:
echo    %INPUT_TIF%
echo  to:
echo    %OUTPUT_XYZ%
echo ===========================================
echo.

REM ------------------------------------------------------------
REM 1) Optionally resample the input (to reduce resolution).
REM ------------------------------------------------------------
if %DO_RESAMPLE%==1 (
    echo Resampling to %RESOLUTION%x%RESOLUTION%...
    gdalwarp -tr %RESOLUTION% %RESOLUTION% -r average -of GTiff ^
      "%INPUT_TIF%" temp_resampled.tif
    if errorlevel 1 (
        echo ERROR: gdalwarp failed.
        exit /b 1
    )
    set "WORKING_TIF=temp_resampled.tif"
) else (
    set "WORKING_TIF=%INPUT_TIF%"
)

REM ------------------------------------------------------------
REM 2) Convert the (resampled) TIF to XYZ.
REM    - Try using gdal2xyz.py (skips NoData)
REM    - If not found, fall back to gdal_translate (includes NoData)
REM --------
