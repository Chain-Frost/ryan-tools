@echo off
setlocal
REM End-user installer: bypass Requires-Python metadata; Python 3.14+ remains supported.
REM Set RYAN_TOOLS_PYTHON to an interpreter path to override automatic selection.
set "PYTHON_CMD=py -3"
if defined RYAN_TOOLS_PYTHON set "PYTHON_CMD="%RYAN_TOOLS_PYTHON%""
%PYTHON_CMD% -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 else 1)" >nul 2>&1
if errorlevel 1 (
    if defined RYAN_TOOLS_PYTHON (
        echo RYAN_TOOLS_PYTHON must point to a working Python 3 interpreter.
        pause
        endlocal & exit /b 1
    )
    set "PYTHON_CMD=python"
    python -c "import sys; raise SystemExit(0 if sys.version_info.major == 3 else 1)" >nul 2>&1
    if errorlevel 1 (
        echo Python 3 was not found.
        echo Install Python 3 and try again.
        pause
        endlocal & exit /b 1
    )
)
call %PYTHON_CMD% "%~dp0repo-scripts\install_from_pypi.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" echo Installer failed. See messages above.
pause
endlocal & exit /b %EXIT_CODE%
