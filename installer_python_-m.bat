@echo off
@REM installer_python_-m.bat
setlocal

@REM If you are missing pip, use this
@REM python -m ensurepip --upgrade

REM Prefer the Windows Python launcher because it resolves to the latest installed Python 3.x.
set "PYTHON_CMD=py -3"
set "GIS_WHEEL_INDEX=https://gisidx.github.io/gwi"

%PYTHON_CMD% --version >nul 2>&1
if errorlevel 1 (
    echo Python launcher not found. Falling back to python in PATH.
    set "PYTHON_CMD=python"
)

call %PYTHON_CMD% -c "import sys; raise SystemExit(0 if sys.version_info[0] == 3 else 1)" >nul 2>&1
if errorlevel 1 (
    echo Python 3 was not found. Install the latest Python 3 and try again.
    pause
    endlocal
    goto :EOF
)

REM Install compiled geospatial dependencies as wheels before installing ryan-functions.
REM Keep GDAL in the same resolver transaction as Rasterio/Fiona so their version constraints stay compatible.
REM If a new Python version does not have matching wheels yet, fail here instead of attempting a source build.
call %PYTHON_CMD% -m pip install --upgrade --extra-index-url "%GIS_WHEEL_INDEX%" --only-binary=:all: fiona rasterio gdal
if errorlevel 1 (
    echo Failed to install Fiona/Rasterio/GDAL binary wheels for %PYTHON_CMD%.
    echo The latest Python may be too new for the available geospatial wheels.
    pause
    endlocal
    goto :EOF
)

REM Define the working path
set "PACKAGE_DIR=%~dp0dist"
echo Using Python command: %PYTHON_CMD%
echo %PACKAGE_DIR%

REM Find the latest version of the package (wheel format)
for /f "delims=" %%i in ('dir /b /o-n "%PACKAGE_DIR%\ryan_functions-*.whl"') do (
    set "LATEST_PACKAGE=%%i"
    goto found
)

:found
if "%LATEST_PACKAGE%"=="" (
    echo No package found in the directory.
    pause
    endlocal
    goto :EOF
)

echo Installing or updating "%LATEST_PACKAGE%"

REM Install or update the package using pip
call %PYTHON_CMD% -m pip install --upgrade --prefer-binary --extra-index-url "%GIS_WHEEL_INDEX%" --only-binary=fiona --only-binary=rasterio --only-binary=gdal "%PACKAGE_DIR%\%LATEST_PACKAGE%"

REM Check if the installation was successful
if errorlevel 1 (
    echo Installation failed. Please check the path and try again.
) else (
    echo Installation completed successfully.
)

endlocal
pause
goto :EOF
