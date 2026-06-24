@echo off
setlocal

set "PYTHON_CMD=py -3"

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

call %PYTHON_CMD% -m pip install --upgrade gdal-installer
if errorlevel 1 (
    echo Failed to install gdal-installer.
    pause
    endlocal
    goto :EOF
)

call %PYTHON_CMD% -m gdal_installer.cli
if errorlevel 1 (
    echo Failed to install GDAL for %PYTHON_CMD%.
    pause
    endlocal
    goto :EOF
)

endlocal
goto :EOF
