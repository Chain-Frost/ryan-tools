@echo off
setlocal enabledelayedexpansion

REM ==================================================================
REM  gdal_flood_extent_fast.bat
REM
REM  Faster flood extent processing for large GeoTIFFs.
REM
REM  Usage:
REM     gdal_flood_extent_fast.bat [input_root] [file_filter]
REM
REM  Examples:
REM     gdal_flood_extent_fast.bat
REM     gdal_flood_extent_fast.bat "D:\Models\Results" "*.tif"
REM ==================================================================

REM ------------------------------------------------------------------
REM Worker mode
REM ------------------------------------------------------------------
if /i "%~1"=="--worker" (
    shift
    goto :worker
)

REM ------------------------------------------------------------------
REM User parameters
REM ------------------------------------------------------------------
set "_cutoffs=0.1"
set "_ext=.tif"

REM Number of concurrent raster jobs.
REM Start with 2 on a 64 GB Windows machine. Try 3 or 4 only after testing.
set "MAX_JOBS=2"

REM GDAL cache per process, in MB.
REM Do not set this too high when using parallel jobs.
set "GDAL_CACHEMAX=2048"

REM Fast temporary raster options.
REM This is a binary mask, so Byte + tiling is usually the right choice.
REM No DEFLATE here because the mask is only an intermediate polygonize input.
set "_createOpts=--type=Byte --co TILED=YES --co BLOCKXSIZE=512 --co BLOCKYSIZE=512 --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER"

REM Output vector format.
set "_vectorExt=.gpkg"
set "_vectorFormat=GPKG"
set "_layerName=flood_extent"
set "_fieldName=DN"

REM Set to 1 to delete the temporary mask after polygonizing.
REM Set to 0 to keep the mask.
set "DELETE_MASK_AFTER_POLYGONIZE=0"

REM ------------------------------------------------------------------
REM Resolve root folder and mask
REM ------------------------------------------------------------------
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

REM ------------------------------------------------------------------
REM Load OSGeo4W/QGIS environment
REM ------------------------------------------------------------------
call :load_env || exit /b 1

REM ------------------------------------------------------------------
REM Sanity-check tools
REM ------------------------------------------------------------------
where gdal_calc >nul 2>&1 || (
    echo [ERROR] gdal_calc executable not found in PATH
    exit /b 1
)

where gdal_polygonize >nul 2>&1 || (
    echo [ERROR] gdal_polygonize executable not found in PATH
    exit /b 1
)

echo --------------------------------------------------------------
echo Root folder : "%ROOT%"
echo File filter : "%MASK%"
echo Cut-offs    : %_cutoffs%
echo Max jobs    : %MAX_JOBS%
echo GDAL cache  : %GDAL_CACHEMAX% MB per process
echo Vector out  : %_vectorFormat% %_vectorExt%
echo --------------------------------------------------------------
echo.

REM ------------------------------------------------------------------
REM Job tracking folder
REM ------------------------------------------------------------------
set "_jobdir=%TEMP%\gdal_fe_jobs_%RANDOM%_%RANDOM%"
mkdir "%_jobdir%" || (
    echo [ERROR] Could not create job folder "%_jobdir%"
    exit /b 1
)

set "_self=%~f0"
set /a "_jobid=0"

REM ------------------------------------------------------------------
REM Launch jobs
REM ------------------------------------------------------------------
for /r "%ROOT%" %%F in (%MASK%) do (
    for %%C in (%_cutoffs%) do (
        call :waitForSlot

        set /a "_jobid+=1"
        set "_marker=%_jobdir%\job_!_jobid!.run"
        break > "!_marker!"

        echo [QUEUE] %%~fF  cutoff %%C

        start "gdal_fe_!_jobid!" /b cmd /s /c ^
            call ""%_self%"" --worker "%%~fF" "%%C" "!_marker!" "%DELETE_MASK_AFTER_POLYGONIZE%"
    )
)

call :waitAllDone

echo.
echo Finished all rasters.

rd "%_jobdir%" >nul 2>&1
pause
exit /b 0


REM ==================================================================
REM Load OSGeo/QGIS environment
REM ==================================================================
:load_env
set "ENV="

if exist "C:\OSGEO4W\bin\o4w_env.bat" (
    set "ENV=C:\OSGEO4W\bin\o4w_env.bat"
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
        echo [ERROR] Could not load GDAL/QGIS environment
        exit /b 1
    )
) else (
    echo [WARN] Could not find o4w_env.bat. Continuing with current PATH.
)

exit /b 0


REM ==================================================================
REM Wait until number of running jobs is below MAX_JOBS
REM ==================================================================
:waitForSlot
set "_running=0"

for /f %%N in ('dir /b "%_jobdir%\*.run" 2^>nul ^| find /c /v ""') do (
    set "_running=%%N"
)

if %_running% GEQ %MAX_JOBS% (
    timeout /t 2 /nobreak >nul
    goto :waitForSlot
)

exit /b 0


REM ==================================================================
REM Wait for all jobs to finish
REM ==================================================================
:waitAllDone
set "_running=0"

for /f %%N in ('dir /b "%_jobdir%\*.run" 2^>nul ^| find /c /v ""') do (
    set "_running=%%N"
)

if %_running% GTR 0 (
    echo [WAIT] %_running% job(s) still running...
    timeout /t 5 /nobreak >nul
    goto :waitAllDone
)

exit /b 0


REM ==================================================================
REM Worker process
REM Arguments:
REM   %1 = input raster
REM   %2 = cutoff
REM   %3 = marker file
REM   %4 = delete mask flag
REM ==================================================================
:worker
setlocal enabledelayedexpansion

set "_in=%~1"
set "_cutoff=%~2"
set "_marker=%~3"
set "_deleteMask=%~4"

call :load_env || goto :worker_fail

call :formatVal "%_cutoff%"

for %%I in ("%_in%") do (
    set "_dir=%%~dpI"
    set "_base=%%~nI"
)

set "_outR=!_dir!!_base!_FE_!_fmt!m.tif"
set "_outV=!_dir!!_base!_FE_!_fmt!m%_vectorExt%"

REM Skip if both outputs already exist.
if exist "!_outR!" if exist "!_outV!" (
    echo [SKIP] "!_outV!"
    goto :worker_done
)

echo [RUN ] "%_in%"  cutoff %_cutoff%

REM --------------------------------------------------------------
REM Create binary flood mask.
REM Output is Byte:
REM   1 = flood depth/value >= cutoff
REM   0 = NoData/background
REM --------------------------------------------------------------
gdal_calc ^
    -A "%_in%" ^
    --calc="where(A>=%_cutoff%,1,0)" ^
    --NoDataValue=0 ^
    --outfile="!_outR!" ^
    %_createOpts%

if errorlevel 1 (
    echo [ERROR] gdal_calc failed: "%_in%"
    goto :worker_fail
)

REM --------------------------------------------------------------
REM Polygonize only non-zero mask cells.
REM -mask points at the same binary mask so zero cells are ignored.
REM --------------------------------------------------------------
if exist "!_outV!" del /q "!_outV!" >nul 2>&1

gdal_polygonize ^
    "!_outR!" ^
    -b 1 ^
    -mask "!_outR!" ^
    -f "%_vectorFormat%" ^
    "!_outV!" ^
    "%_layerName%" ^
    "%_fieldName%"

if errorlevel 1 (
    echo [ERROR] gdal_polygonize failed: "!_outR!"
    goto :worker_fail
)

if "%_deleteMask%"=="1" (
    del /q "!_outR!" >nul 2>&1
)

echo [DONE] "!_outV!"
goto :worker_done


:worker_fail
echo [FAIL] "%_in%"

:worker_done
if exist "%_marker%" del /q "%_marker%" >nul 2>&1
endlocal
exit /b 0


REM ==================================================================
REM formatVal - normalise cutoff for filenames
REM Examples:
REM   0.1 -> 1
REM   1.0 -> 1
REM   0.05 -> 05
REM ==================================================================
:formatVal
set "_fmt=%~1"
if "%_fmt:~-2%"==".0" set "_fmt=%_fmt:~0,-2%"
if "%_fmt:~0,1%"=="0" set "_fmt=%_fmt:~1%"
set "_fmt=%_fmt:.=%"
exit /b 0