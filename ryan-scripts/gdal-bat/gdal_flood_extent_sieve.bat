@echo off
setlocal enabledelayedexpansion

REM ================================================================
REM gdal_flood_extent_sieve.bat
REM
REM Usage:
REM   gdal_flood_extent_sieve.bat [input_root] [file_filter]
REM
REM Example:
REM   gdal_flood_extent_sieve.bat "D:\Project\Results" "*.tif"
REM ================================================================

REM ---------------- User settings --------------------------------
set "_cutoffs=0.1"
set "_sievePixels=8"
set "_ext=.tif"

REM Output options for temporary binary rasters.
REM Byte is critical: the mask is only 0/1.
set "_createOpts=--type=Byte --co TILED=YES --co BLOCKXSIZE=512 --co BLOCKYSIZE=512 --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER"

REM Output vector settings.
set "_vectorExt=.gpkg"
set "_vectorFormat=GPKG"
set "_layerName=flood_extent"
set "_fieldName=DN"

REM Delete intermediate masks?
REM 0 = keep masks
REM 1 = delete masks after polygonizing
set "DELETE_INTERMEDIATE=0"

REM GDAL cache in MB.
set "GDAL_CACHEMAX=2048"

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

where gdal_calc >nul 2>&1 || (
    echo [ERROR] gdal_calc not found.
    exit /b 1
)

where gdal_sieve >nul 2>&1 || (
    echo [ERROR] gdal_sieve not found.
    exit /b 1
)

where gdal_polygonize >nul 2>&1 || (
    echo [ERROR] gdal_polygonize not found.
    exit /b 1
)

echo --------------------------------------------------------------
echo Flood extent processing
echo Root folder : "%ROOT%"
echo File filter : "%MASK%"
echo Cutoffs     : %_cutoffs%
echo Sieve pixels: %_sievePixels%
echo --------------------------------------------------------------
echo.

for /r "%ROOT%" %%F in (%MASK%) do (
    for %%C in (%_cutoffs%) do (
        call :process_one "%%~fF" "%%C"
    )
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
set "_cutoff=%~2"

call :formatVal "%_cutoff%"

for %%I in ("%_in%") do (
    set "_dir=%%~dpI"
    set "_base=%%~nI"
)

set "_mask=!_dir!!_base!_FE_!_fmt!m_mask.tif"
set "_sieved=!_dir!!_base!_FE_!_fmt!m_sieved.tif"
set "_outV=!_dir!!_base!_FE_!_fmt!m%_vectorExt%"

if exist "!_outV!" (
    echo [SKIP] "!_outV!"
    exit /b 0
)

echo [RUN ] "%_in%" cutoff %_cutoff%

REM ---------------------------------------------------------------
REM 1. Create binary flood mask.
REM    1 = flood
REM    0 = background / NoData
REM ---------------------------------------------------------------
gdal_calc ^
    -A "%_in%" ^
    --calc="where(A>=%_cutoff%,1,0)" ^
    --NoDataValue=0 ^
    --outfile="!_mask!" ^
    %_createOpts%

if errorlevel 1 (
    echo [ERROR] gdal_calc failed: "%_in%"
    exit /b 1
)

REM ---------------------------------------------------------------
REM 2. Sieve small regions.
REM    -st is the size threshold in pixels.
REM    -8 uses 8-connectedness.
REM ---------------------------------------------------------------
gdal_sieve ^
    -st %_sievePixels% ^
    -8 ^
    "!_mask!" ^
    "!_sieved!"

if errorlevel 1 (
    echo [ERROR] gdal_sieve failed: "!_mask!"
    exit /b 1
)

REM ---------------------------------------------------------------
REM 3. Polygonize only non-zero / valid flood cells.
REM ---------------------------------------------------------------
if exist "!_outV!" del /q "!_outV!" >nul 2>&1

gdal_polygonize ^
    "!_sieved!" ^
    -b 1 ^
    -mask "!_sieved!" ^
    -f "%_vectorFormat%" ^
    "!_outV!" ^
    "%_layerName%" ^
    "%_fieldName%"

if errorlevel 1 (
    echo [ERROR] gdal_polygonize failed: "!_sieved!"
    exit /b 1
)

if "%DELETE_INTERMEDIATE%"=="1" (
    del /q "!_mask!" >nul 2>&1
    del /q "!_sieved!" >nul 2>&1
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


REM ================================================================
REM Format cutoff for filenames
REM 0.1 -> 1
REM 1.0 -> 1
REM 0.05 -> 05
REM ================================================================
:formatVal
set "_fmt=%~1"
if "%_fmt:~-2%"==".0" set "_fmt=%_fmt:~0,-2%"
if "%_fmt:~0,1%"=="0" set "_fmt=%_fmt:~1%"
set "_fmt=%_fmt:.=%"
exit /b 0