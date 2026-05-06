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
REM   input_folder    -> current directory
REM   file_pattern    -> *.tif
REM   output_basename -> name of the input folder
REM   output_vrt      -> <input_folder>\<output_basename>.vrt
REM   output_tif      -> <input_folder>\<output_basename>.tif
REM
REM Example:
REM   gdal_merge.bat
REM   gdal_merge.bat "D:\data\tiles" "*.tif" Final_DEM
REM   gdal_merge.bat "D:\data\tiles" "*.tif" "MyMerge" "D:\out\merge.vrt" "D:\out\merge.tif"
REM ===================================================================

REM -------------------------------------------------------------------
REM GDAL options
REM -------------------------------------------------------------------

REM VRT options.
REM Leave blank unless you specifically want options such as:
REM   -resolution highest
REM   -resolution highest -tap
set "GDALBUILD_OPTIONS="

REM Balanced speed/size GeoTIFF output for a 64 GB RAM Windows machine.
REM Uses:
REM   - 8 GB GDAL cache
REM   - all CPU threads where GDAL/GeoTIFF supports it
REM   - tiled GeoTIFF
REM   - BigTIFF allowed
REM   - DEFLATE with low compression level for speed
set "GDALTRANSLATE_OPTIONS=--config GDAL_CACHEMAX 8192 --config GDAL_NUM_THREADS ALL_CPUS -of GTiff -co COMPRESS=DEFLATE -co ZLEVEL=1 -co TILED=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 -co BIGTIFF=YES -co NUM_THREADS=ALL_CPUS"

REM If maximum speed matters more than output size, use this instead:
REM set "GDALTRANSLATE_OPTIONS=--config GDAL_CACHEMAX 8192 --config GDAL_NUM_THREADS ALL_CPUS -of GTiff -co COMPRESS=NONE -co TILED=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 -co BIGTIFF=YES"

REM -------------------------------------------------------------------
REM Help
REM -------------------------------------------------------------------

if /I "%~1"=="-h" goto :usage
if /I "%~1"=="--help" goto :usage
if /I "%~1"=="/?" goto :usage

REM -------------------------------------------------------------------
REM Setup GDAL/QGIS environment
REM -------------------------------------------------------------------

call :setup_environment
if errorlevel 1 goto :end

REM -------------------------------------------------------------------
REM Resolve input folder
REM -------------------------------------------------------------------

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

REM -------------------------------------------------------------------
REM Resolve file pattern and output paths
REM -------------------------------------------------------------------

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

REM Convert output paths to absolute paths where possible.
for %%F in ("%OUTPUT_VRT%") do set "OUTPUT_VRT=%%~fF"
for %%F in ("%OUTPUT_TIF%") do set "OUTPUT_TIF=%%~fF"

REM -------------------------------------------------------------------
REM Validate matching files and create input file list
REM -------------------------------------------------------------------

set "INPUT_FILE_LIST=%TEMP%\gdalbuildvrt_input_%RANDOM%_%RANDOM%.txt"

dir /b /a:-d %FILE_PATTERN% > "%INPUT_FILE_LIST%" 2>nul

if errorlevel 1 (
    echo Error: No files matching "%FILE_PATTERN%" found in "%CD%".
    del "%INPUT_FILE_LIST%" >nul 2>&1
    popd
    goto :end
)

for %%A in ("%INPUT_FILE_LIST%") do (
    if %%~zA EQU 0 (
        echo Error: No files matching "%FILE_PATTERN%" found in "%CD%".
        del "%INPUT_FILE_LIST%" >nul 2>&1
        popd
        goto :end
    )
)

REM Count files.
set "FILE_COUNT=0"
for /f "usebackq delims=" %%F in ("%INPUT_FILE_LIST%") do (
    set /a FILE_COUNT+=1
)

REM -------------------------------------------------------------------
REM Report settings
REM -------------------------------------------------------------------

echo.
echo Input folder        : %CD%
echo File pattern        : %FILE_PATTERN%
echo Matching files      : %FILE_COUNT%
echo Input file list     : %INPUT_FILE_LIST%
echo Output VRT          : %OUTPUT_VRT%
echo Output GeoTIFF      : %OUTPUT_TIF%
echo gdalbuildvrt opts   : %GDALBUILD_OPTIONS%
echo gdal_translate opts : %GDALTRANSLATE_OPTIONS%
echo.

REM -------------------------------------------------------------------
REM Remove existing outputs if present
REM -------------------------------------------------------------------

if exist "%OUTPUT_VRT%" (
    echo Warning: Existing VRT will be overwritten:
    echo   %OUTPUT_VRT%
    del "%OUTPUT_VRT%" >nul 2>&1
    if exist "%OUTPUT_VRT%" (
        echo Error: Unable to delete existing VRT.
        del "%INPUT_FILE_LIST%" >nul 2>&1
        popd
        goto :end
    )
)

if exist "%OUTPUT_TIF%" (
    echo Warning: Existing GeoTIFF will be overwritten:
    echo   %OUTPUT_TIF%
    del "%OUTPUT_TIF%" >nul 2>&1
    if exist "%OUTPUT_TIF%" (
        echo Error: Unable to delete existing GeoTIFF.
        del "%INPUT_FILE_LIST%" >nul 2>&1
        popd
        goto :end
    )
)

REM -------------------------------------------------------------------
REM Build VRT using file list
REM -------------------------------------------------------------------

echo Building VRT...
echo gdalbuildvrt %GDALBUILD_OPTIONS% -input_file_list "%INPUT_FILE_LIST%" "%OUTPUT_VRT%"
echo.

gdalbuildvrt %GDALBUILD_OPTIONS% -input_file_list "%INPUT_FILE_LIST%" "%OUTPUT_VRT%"

if errorlevel 1 (
    echo.
    echo Error: gdalbuildvrt failed.
    del "%INPUT_FILE_LIST%" >nul 2>&1
    popd
    goto :end
)

REM -------------------------------------------------------------------
REM Convert VRT to GeoTIFF
REM -------------------------------------------------------------------

echo.
echo Converting VRT to GeoTIFF...
echo gdal_translate %GDALTRANSLATE_OPTIONS% "%OUTPUT_VRT%" "%OUTPUT_TIF%"
echo.

gdal_translate %GDALTRANSLATE_OPTIONS% "%OUTPUT_VRT%" "%OUTPUT_TIF%"

if errorlevel 1 (
    echo.
    echo Error: gdal_translate failed.
    del "%INPUT_FILE_LIST%" >nul 2>&1
    popd
    goto :end
)

REM -------------------------------------------------------------------
REM Clean up
REM -------------------------------------------------------------------

del "%INPUT_FILE_LIST%" >nul 2>&1

echo.
echo Combined mosaic created successfully.
echo VRT : %OUTPUT_VRT%
echo TIF : %OUTPUT_TIF%
echo.

popd
goto :end

REM ===================================================================
REM Environment setup
REM ===================================================================

:setup_environment
set "ENV_SETUP="

if exist "C:\OSGeo4W\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\OSGeo4W\bin\o4w_env.bat"
) else if exist "C:\OSGEO4W\bin\o4w_env.bat" (
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

REM ===================================================================
REM Usage
REM ===================================================================

:usage
echo.
echo Usage:
echo   gdal_merge.bat [input_folder] [file_pattern] [output_basename] [output_vrt] [output_tif]
echo.
echo Examples:
echo   gdal_merge.bat
echo   gdal_merge.bat "D:\data\tiles" "*.tif" Final_DEM
echo   gdal_merge.bat "D:\data\tiles" "*.xyz" Woolpert_2024_DEM
echo   gdal_merge.bat "D:\data\tiles" "*.tif" "MyMerge" "D:\out\merge.vrt" "D:\out\merge.tif"
echo.
goto :end

:end
endlocal