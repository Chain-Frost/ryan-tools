@echo off
REM Opt-in user-wide Python bytecode suppression. No administrator rights required.
if /I "%~1"=="--dry-run" (
    echo Would persist PYTHONDONTWRITEBYTECODE=1 for the current Windows user.
    echo Would also set it in the current cmd.exe session.
    exit /b 0
)
if not "%~1"=="" (
    echo Usage: disable-python-bytecode.bat [--dry-run]
    exit /b 2
)

setx PYTHONDONTWRITEBYTECODE 1 >nul
if errorlevel 1 (
    echo Failed to save PYTHONDONTWRITEBYTECODE for the current Windows user.
    exit /b 1
)
set "PYTHONDONTWRITEBYTECODE=1"
echo Python bytecode writes are disabled for the current Windows user.
echo Restart terminals, editors and Codex to inherit the saved setting.
echo Existing .pyc files are unchanged.
exit /b 0
