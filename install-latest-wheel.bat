@echo off
setlocal
REM End-user installer: current ryan_functions from PyPI, not dist\*.whl.
set "PYTHON_CMD=py -3.14"
py -3.14 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 14) else 1)" >nul 2>&1
if errorlevel 1 (
    set "PYTHON_CMD=python"
    python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 14) else 1)" >nul 2>&1
    if errorlevel 1 (
        echo Python 3.14 or newer was not found.
        echo Install Python 3.14 and try again.
        pause
        endlocal & exit /b 1
    )
)
call %PYTHON_CMD% -B "%~dp0repo-scripts\install_from_pypi.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" echo Installer failed. See messages above.
pause
endlocal & exit /b %EXIT_CODE%
