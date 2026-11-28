@echo off
REM Developer installer: use the latest locally built and verified wheel.
setlocal

set "PYTHON_CMD=py -3"
if defined RYAN_TOOLS_PYTHON set "PYTHON_CMD="%RYAN_TOOLS_PYTHON%""
%PYTHON_CMD% -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 else 1)" >nul 2>&1
if errorlevel 1 (
    if defined RYAN_TOOLS_PYTHON (
        echo RYAN_TOOLS_PYTHON must point to a working Python 3 interpreter.
        endlocal & exit /b 1
    )
    set "PYTHON_CMD=python"
    python -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 else 1)" >nul 2>&1
    if errorlevel 1 (
        echo Python 3 was not found. Install Python 3 and try again.
        endlocal & exit /b 1
    )
)

call %PYTHON_CMD% "%~dp0repo-scripts\install_latest_wheel.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" echo Installation failed.

pause
endlocal & exit /b %EXIT_CODE%
