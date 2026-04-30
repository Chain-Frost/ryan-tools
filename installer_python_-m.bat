@echo off
@REM installer_python_-m.bat
setlocal

@REM If you are missing pip, use this
@REM python -m ensurepip --upgrade

REM Prefer the Windows Python launcher because it resolves to the latest installed Python 3.x.
set "PYTHON_CMD=py -3"
py -3 --version >nul 2>&1
if errorlevel 1 (
    echo Python launcher not found. Falling back to python in PATH.
    set "PYTHON_CMD=python"
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
call %PYTHON_CMD% -m pip install --upgrade "%PACKAGE_DIR%\%LATEST_PACKAGE%"

REM Check if the installation was successful
if %ERRORLEVEL% equ 0 (
    echo Installation completed successfully.
) else (
    echo Installation failed. Please check the path and try again.
)

endlocal
pause
goto :EOF
