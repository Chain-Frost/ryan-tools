@echo off
setlocal

REM Define the directory where the package will be stored
set "PACKAGE_DIR=%~dp0dist"

REM Navigate to the directory containing the setup.py script
cd /d "%~dp0"

REM Clean previous builds
if exist "%PACKAGE_DIR%" rmdir /s /q "%PACKAGE_DIR%"

REM Create the source distribution
python setup.py sdist

REM Check if the build was successful
if %ERRORLEVEL% neq 0 (
    echo Build failed. Please check the setup.py for errors.
    endlocal
    goto :EOF
)

REM Move the generated tar.gz file to the network share or desired location
move "%PACKAGE_DIR%\*.tar.gz" "%PACKAGE_DIR%"

REM Check if the move was successful
if %ERRORLEVEL% neq 0 (
    echo Failed to move the package to the destination folder.
    endlocal
    goto :EOF
)

echo Package created and moved to %PACKAGE_DIR% successfully.
endlocal
goto :EOF
