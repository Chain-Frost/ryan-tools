@echo off
setlocal enabledelayedexpansion

REM ===================================================================
REM gdal_merge.bat - Generalized VRT + GeoTIFF builder
REM
REM Usage:
REM   gdal_merge.bat [input_folder] [file_pattern] [output_basename]
REM                  [output_vrt] [output_tif]
REM
REM Defaults:
REM   input_folder   -> current directory
REM   file_pattern   -> *.tif
REM   output_basename-> name of the input folder
REM   output_vrt     -> <input_folder>\<output_basename>.vrt
REM   output_tif     -> <input_folder>\<output_basename>.tif
REM
REM Example (process current folder, merge *.xyz files):
REM   gdal_merge.bat . "*.xyz" Woolpert_2024_DEM
REM ===================================================================

set "GDALBUILD_OPTIONS="
set "GDALTRANSLATE_OPTIONS=-of GTiff -co COMPRESS=DEFLATE -co TILED=YES"

if /I "%~1"=="-h"  goto :usage
if /I "%~1"=="--help" goto :usage
if /I "%~1"=="/?" goto :usage

call :setup_environment
if errorlevel 1 goto :end

set "TARGET_FOLDER=%~1"
if not defined TARGET_FOLDER (
    set "TARGET_FOLDER=%CD%"
) else (
    for %%F in ("%TARGET_FOLDER%") do set "TARGET_FOLDER=%%~fF"
)

if not exist "%TARGET_FOLDER%" (
    echo Error: Input folder "%TARGET_FOLDER%" does not exist.
    goto :end
)

pushd "%TARGET_FOLDER%" || (
    echo Error: Unable to change directory to "%TARGET_FOLDER%".
    goto :end
)

set "FILE_PATTERN=%~2"
if not defined FILE_PATTERN set "FILE_PATTERN=*.tif"

if "%~3"=="" (
    for %%F in ("%CD%") do set "OUTPUT_BASENAME=%%~nF"
) else (
    set "OUTPUT_BASENAME=%~3"
)
if not defined OUTPUT_BASENAME set "OUTPUT_BASENAME=merged"

if "%~4"=="" (
    set "OUTPUT_VRT=%CD%\%OUTPUT_BASENAME%.vrt"
) else (
    set "OUTPUT_VRT=%~4"
)

if "%~5"=="" (
    set "OUTPUT_TIF=%CD%\%OUTPUT_BASENAME%.tif"
) else (
    set "OUTPUT_TIF=%~5"
)

dir /b %FILE_PATTERN% >nul 2>&1
if errorlevel 1 (
    echo Error: No files matching "%FILE_PATTERN%" found in "%CD%".
    popd
    goto :end
)

echo.
echo Input folder      : %CD%
echo File pattern      : %FILE_PATTERN%
echo Output VRT        : %OUTPUT_VRT%
echo Output GeoTIFF    : %OUTPUT_TIF%
echo gdalbuildvrt opts : %GDALBUILD_OPTIONS%
echo gdal_translate opts: %GDALTRANSLATE_OPTIONS%
echo.

echo Building VRT...
echo gdalbuildvrt %GDALBUILD_OPTIONS% "%OUTPUT_VRT%" %FILE_PATTERN%
gdalbuildvrt %GDALBUILD_OPTIONS% "%OUTPUT_VRT%" %FILE_PATTERN%
if errorlevel 1 (
    echo Error: gdalbuildvrt failed.
    popd
    goto :end
)

echo Converting VRT to GeoTIFF...
echo gdal_translate %GDALTRANSLATE_OPTIONS% "%OUTPUT_VRT%" "%OUTPUT_TIF%"
gdal_translate %GDALTRANSLATE_OPTIONS% "%OUTPUT_VRT%" "%OUTPUT_TIF%"
if errorlevel 1 (
    echo Error: gdal_translate failed.
    popd
    goto :end
)

echo.
echo Combined mosaic created successfully.
echo VRT : %OUTPUT_VRT%
echo TIF : %OUTPUT_TIF%
echo.

popd
goto :end

:setup_environment
set "ENV_SETUP="

if exist "C:\OSGEO4W\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\OSGEO4W\bin\o4w_env.bat"
) else (
    for /f "delims=" %%D in ('dir /b /ad /o-n "C:\Program Files\QGIS*" 2^>nul') do (
        if exist "C:\Program Files\%%D\bin\o4w_env.bat" (
            set "ENV_SETUP=C:\Program Files\%%D\bin\o4w_env.bat"
            goto env_found
        )
    )
)

:env_found
if not defined ENV_SETUP (
    echo Error: Unable to locate OSGeo4W or QGIS o4w_env.bat.
    exit /b 1
)

call "%ENV_SETUP%"
if errorlevel 1 (
    echo Error: Failed to execute "%ENV_SETUP%".
    exit /b 1
)

where gdalbuildvrt >nul 2>&1
if errorlevel 1 (
    echo Error: gdalbuildvrt not found. Ensure GDAL is installed.
    exit /b 1
)

where gdal_translate >nul 2>&1
if errorlevel 1 (
    echo Error: gdal_translate not found. Ensure GDAL is installed.
    exit /b 1
)

echo Environment ready. Using "%ENV_SETUP%".
exit /b 0

:usage
echo.
echo Usage:
echo   gdal_merge.bat [input_folder] [file_pattern] [output_basename] ^
[output_vrt] [output_tif]
echo.
echo Examples:
echo   gdal_merge.bat
echo   gdal_merge.bat "D:\data\tiles" "*.xyz" Final_DEM
echo   gdal_merge.bat "D:\data\tiles" "*.tif" "MyMerge" "D:\out\merge.vrt" "D:\out\merge.tif"
echo.
goto :end

:end
endlocal
