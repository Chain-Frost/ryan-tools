@echo off
setlocal

REM Define the the working path
set "PACKAGE_DIR=%~dp0dist"
echo %PACKAGE_DIR%


REM Find the latest version of the package
for /f "delims=" %%i in ('dir /b /o-n "%PACKAGE_DIR%\ryan_functions-*.tar.gz"') do (
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
pip install --upgrade "%PACKAGE_DIR%\%LATEST_PACKAGE%"

REM Check if the installation was successful
if %ERRORLEVEL% equ 0 (
    echo Installation completed successfully.
) else (
    echo Installation failed. Please check the path and try again.
)

endlocal
pause
goto :EOF
