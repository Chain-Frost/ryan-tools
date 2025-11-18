@echo off
setlocal enabledelayedexpansion

REM ===================================================================
REM gdal_edit_Set_nodata.bat
REM Sets a nodata value for every raster matching a pattern.
REM
REM Usage:
REM   gdal_edit_Set_nodata.bat [target_folder] [pattern] [nodata_value]
REM
REM Defaults:
REM   target_folder -> current directory
REM   pattern       -> *.tif
REM   nodata_value  -> -9999
REM ===================================================================

set "TARGET_FOLDER=%~1"
set "FILE_PATTERN=%~2"
set "NODATA_VALUE=%~3"

if not defined TARGET_FOLDER (
    set "TARGET_FOLDER=%CD%"
) else (
    for %%F in ("%TARGET_FOLDER%") do set "TARGET_FOLDER=%%~fF"
)

if not defined FILE_PATTERN set "FILE_PATTERN=*.tif"
if not defined NODATA_VALUE set "NODATA_VALUE=-9999"

call :setup_environment
if errorlevel 1 goto :end

if not exist "%TARGET_FOLDER%" (
    echo Error: Target folder "%TARGET_FOLDER%" does not exist.
    goto :end
)

pushd "%TARGET_FOLDER%" || (
    echo Error: Unable to change directory to "%TARGET_FOLDER%".
    goto :end
)

dir /b %FILE_PATTERN% >nul 2>&1
if errorlevel 1 (
    echo Error: No files matching "%FILE_PATTERN%" found in "%CD%".
    popd
    goto :end
)

echo.
echo Target folder : %CD%
echo Pattern       : %FILE_PATTERN%
echo Nodata value  : %NODATA_VALUE%
echo.

for %%f in (%FILE_PATTERN%) do (
    echo Processing "%%f" ...
    gdal_edit -a_nodata %NODATA_VALUE% "%%f"
    if errorlevel 1 (
        echo   Error setting nodata for "%%f".
        popd
        goto :end
    )
)

echo.
echo Completed setting nodata for files matching %FILE_PATTERN%.
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

where gdal_edit >nul 2>&1
if errorlevel 1 (
    echo Error: gdal_edit not found. Ensure GDAL is installed.
    exit /b 1
)

echo Environment ready. Using "%ENV_SETUP%".
exit /b 0

:end
endlocal
