@echo off
setlocal

set "PYTHON_CMD=py -3"
%PYTHON_CMD% --version >nul 2>&1
if errorlevel 1 set "PYTHON_CMD=python"

call %PYTHON_CMD% -c "import sys; raise SystemExit(0 if sys.version_info[0] == 3 else 1)" >nul 2>&1
if errorlevel 1 (
    echo Python 3 was not found. Install Python 3 and try again.
    endlocal
    exit /b 1
)

call %PYTHON_CMD% "%~dp0repo-scripts\install_latest_wheel.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" echo Installation failed.

Pause
endlocal & exit /b %EXIT_CODE%
