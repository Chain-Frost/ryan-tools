@echo off
REM packager.bat
setlocal

REM Define the build and package directories
if defined TEMP (
    set "BUILD_DIR=%TEMP%\ryan-tools-build"
) else (
    set "BUILD_DIR=%~dp0build"
)
set "PACKAGE_DIR=%~dp0dist"

REM Navigate to the directory containing the setup.py script
cd /d "%~dp0"

REM Clean previous builds
if exist "%PACKAGE_DIR%" rmdir /s /q "%PACKAGE_DIR%"
if exist "%BUILD_DIR%" rmdir /s /q "%BUILD_DIR%"
mkdir "%BUILD_DIR%" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Failed to create build directory at %BUILD_DIR%.
    endlocal
    goto :EOF
)

REM Install the build tool if it's not already installed
python -m pip install --upgrade build >nul 2>&1

REM Create the wheel distribution in the specified build directory
python -m build --wheel --outdir "%BUILD_DIR%"

REM Check if the build was successful
if %ERRORLEVEL% neq 0 (
    echo Build failed. Please check the setup.py for errors.
    endlocal
    goto :EOF
)

REM Move the generated wheel file to the destination package directory
if not exist "%PACKAGE_DIR%" mkdir "%PACKAGE_DIR%"
move "%BUILD_DIR%\*.whl" "%PACKAGE_DIR%" >nul 2>&1

REM Check if the move was successful
if %ERRORLEVEL% neq 0 (
    echo Failed to move the package to the destination folder.
    endlocal
    goto :EOF
)

echo Package created and moved to %PACKAGE_DIR% successfully.
endlocal
goto :EOF
