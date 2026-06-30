@echo off
setlocal

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

echo Using Python command: %PYTHON_CMD%

call %PYTHON_CMD% -m pip install --upgrade --extra-index-url "%GIS_WHEEL_INDEX%" --only-binary=:all: gdal
if errorlevel 1 (
    echo Failed to install a GDAL binary wheel for %PYTHON_CMD%.
    pause
    endlocal
    goto :EOF
)

endlocal
goto :EOF
