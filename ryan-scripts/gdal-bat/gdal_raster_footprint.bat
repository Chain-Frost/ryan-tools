@echo off
setlocal enabledelayedexpansion

REM ================================================================
REM gdal_raster_footprint.bat
REM
REM Creates valid-data raster footprints excluding NoData.
REM
REM Usage:
REM   gdal_raster_footprint.bat [input_root] [file_filter]
REM
REM Example:
REM   gdal_raster_footprint.bat "D:\Project\DEM" "*.tif"
REM ================================================================

set "_ext=.tif"
set "_vectorExt=.gpkg"
set "_vectorFormat=GPKG"
set "_layerName=raster_footprint"

REM ---------------------------------------------------------------
REM Resolve arguments
REM ---------------------------------------------------------------
if "%~1"=="" (
    set "ROOT=%CD%"
) else (
    set "ROOT=%~1"
)

if "%~2"=="" (
    set "MASK=*%_ext%"
) else (
    set "MASK=%~2"
)

REM ---------------------------------------------------------------
REM Load OSGeo4W / QGIS environment
REM ---------------------------------------------------------------
call :load_env || exit /b 1

where gdal_footprint >nul 2>&1 || (
    echo [ERROR] gdal_footprint not found.
    echo Your GDAL/QGIS version may be too old.
    echo Use a newer QGIS/GDAL install, or use the fallback mask/polygonize method.
    exit /b 1
)

echo --------------------------------------------------------------
echo Raster valid-data footprint
echo Root folder : "%ROOT%"
echo File filter : "%MASK%"
echo --------------------------------------------------------------
echo.

for /r "%ROOT%" %%F in (%MASK%) do (
    call :process_one "%%~fF"
)

echo.
echo Finished.
pause
exit /b 0


REM ================================================================
REM Process one raster
REM ================================================================
:process_one
set "_in=%~1"

for %%I in ("%_in%") do (
    set "_dir=%%~dpI"
    set "_base=%%~nI"
)

set "_outV=!_dir!!_base!_footprint%_vectorExt%"

if exist "!_outV!" (
    echo [SKIP] "!_outV!"
    exit /b 0
)

echo [RUN ] "%_in%"

gdal_footprint ^
    "%_in%" ^
    "!_outV!" ^
    -f "%_vectorFormat%" ^
    -lyr_name "%_layerName%"

if errorlevel 1 (
    echo [ERROR] gdal_footprint failed: "%_in%"
    exit /b 1
)

echo [DONE] "!_outV!"
exit /b 0


REM ================================================================
REM Load QGIS / OSGeo4W environment
REM ================================================================
:load_env
set "ENV="

if exist "C:\OSGeo4W\bin\o4w_env.bat" (
    set "ENV=C:\OSGeo4W\bin\o4w_env.bat"
) else (
    for /f "delims=" %%D in ('dir /b /ad /o-n "C:\Program Files\QGIS*" 2^>nul') do (
        if exist "C:\Program Files\%%D\bin\o4w_env.bat" (
            set "ENV=C:\Program Files\%%D\bin\o4w_env.bat"
            goto :env_found
        )
    )
)

:env_found
if defined ENV (
    call "%ENV%" || (
        echo [ERROR] Could not load GDAL/QGIS environment.
        exit /b 1
    )
) else (
    echo [WARN] Could not find o4w_env.bat. Continuing with current PATH.
)

exit /b 0