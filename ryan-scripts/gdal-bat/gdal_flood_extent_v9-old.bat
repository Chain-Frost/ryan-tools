@echo off
setlocal enabledelayedexpansion

REM ==================================================================
REM  gdal_flood_extent_v9.bat  –  Sequential single‑thread version
REM  Uses gdal_calc and gdal_polygonize executables (exe/bat wrappers)
REM  -----------------------------------------------------------------
REM  USAGE
REM     gdal_flood_extent_v9.bat  [<input_root>]  [file_filter]
REM ==================================================================

:: ---------------- User parameters ----------------------------------
set "_cutoffs=0.1"
set "_ext=.tif"
set "_createOpts=--co COMPRESS=DEFLATE --co PREDICTOR=2 --co NUM_THREADS=ALL_CPUS --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER"

:: -------------------------------------------------------------------
:: 1. Resolve root folder & mask
:: -------------------------------------------------------------------
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

:: -------------------------------------------------------------------
:: 2. Load OSGeo4W/QGIS env (adds gdal_calc.exe, etc. to PATH)
:: -------------------------------------------------------------------
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
    call "%ENV%" || (echo [ERROR] Could not load GDAL/QGIS env & exit /b 1)
)

:: -------------------------------------------------------------------
:: 3. Sanity‑check tools
:: -------------------------------------------------------------------
where gdal_calc        >nul 2>&1 || (echo [ERROR] gdal_calc executable not found in PATH & exit /b 1)
where gdal_polygonize  >nul 2>&1 || (echo [ERROR] gdal_polygonize executable not found in PATH & exit /b 1)

echo --------------------------------------------------------------
echo Root folder : "%ROOT%"
echo File filter : "%MASK%"
echo Cut‑offs    : %_cutoffs%
echo --------------------------------------------------------------
echo.

call :treeProcess "%ROOT%"

echo.
echo Finished all rasters.
pause
exit /b

::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: treeProcess – recurse sequentially
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:treeProcess
pushd "%~1" >nul || (echo [WARN] Cannot access "%~1" & exit /b)

for %%F in (%MASK%) do (
    for %%C in (%_cutoffs%) do (
        call :formatVal %%C

        set "_base=%%~nF"
        set "_outR=%%~dpF!_base!_FE_!_fmt!m%_ext%"
        set "_outS=%%~dpF!_base!_FE_!_fmt!m.shp"

        REM skip if up‑to‑date
        set "skip="
        if exist "!_outR!" (
            for /f "delims=" %%T in ('dir /b /o:-D "%%~F" "!_outR!"') do set "newest=%%T"
            if /i "!newest!"=="!_outR!" set "skip=1"
        )

        if defined skip (
            echo [SKIP] !_outR!
        ) else (
            echo [RUN ] %%~nxF  (cut-off %%C)
            set calc=--calc="where(A>=%%C,1,0)" --NoDataValue=0
            gdal_calc !calc! -A "%%~F" --outfile="!_outR!" %_createOpts%
            if errorlevel 1 echo [ERROR] gdal_calc failed on %%~F

            gdal_polygonize "!_outR!" "!_outS!"
            if errorlevel 1 echo [ERROR] gdal_polygonize failed on !_outR!
        )
    )
)

for /d %%D in (*) do (
    call :treeProcess "%%~fD"
)

popd
exit /b

::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:: formatVal – normalise cut‑off for filenames
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
:formatVal
set "_fmt=%~1"
if "%_fmt:~-2%"==".0" set "_fmt=%_fmt:~0,-2%"
if "%_fmt:~0,1%"=="0" set "_fmt=%_fmt:~1%"
set "_fmt=%_fmt:.=%"
exit /b
