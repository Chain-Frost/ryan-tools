@echo off
@REM installer_python_-m.bat
setlocal

@REM If you are missing pip, use this
@REM python -m ensurepip --upgrade

REM Define the path to the Python executable
set "PYTHON_EXEC=C:\Program Files\Python312\python.exe"

REM Check if the Python executable exists
if not exist "%PYTHON_EXEC%" (
    echo "%PYTHON_EXEC%" not found. Falling back to Python in PATH.
    set "PYTHON_EXEC=python"
)

REM Define the working path
set "PACKAGE_DIR=%~dp0dist"
echo Using Python executable: %PYTHON_EXEC%
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
"%PYTHON_EXEC%" -m pip install --upgrade "%PACKAGE_DIR%\%LATEST_PACKAGE%"

REM Check if the installation was successful
if %ERRORLEVEL% equ 0 (
    echo Installation completed successfully.
) else (
    echo Installation failed. Please check the path and try again.
)

endlocal
pause
goto :EOF
