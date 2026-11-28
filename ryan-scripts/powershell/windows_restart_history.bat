@echo off
setlocal EnableExtensions

set "SCRIPT_PATH=%~dp0windows_restart_history.ps1"

if not exist "%SCRIPT_PATH%" (
    echo ERROR: PowerShell script not found:
    echo %SCRIPT_PATH%
    echo.
    pause
    exit /b 1
)

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_PATH%" %*
set "SCRIPT_EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%SCRIPT_EXIT_CODE%"=="0" echo The restart history script failed with exit code %SCRIPT_EXIT_CODE%.
pause
exit /b %SCRIPT_EXIT_CODE%
