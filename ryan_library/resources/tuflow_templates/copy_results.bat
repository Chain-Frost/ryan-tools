@echo off
setlocal

set "SOURCE=C:\temp\<Project>\results"
set "DESTINATION=W:\26\<project>\results"

if not exist "%SOURCE%\" (
    echo ERROR: Source folder does not exist: "%SOURCE%"
    pause
    exit /b 1
)

robocopy "%SOURCE%" "%DESTINATION%" /E /Z /R:3 /W:5 /MT:8
set "COPY_RESULT=%ERRORLEVEL%"

echo.
if %COPY_RESULT% GEQ 8 (
    echo Copy failed. Robocopy exit code: %COPY_RESULT%
) else (
    echo Copy completed. Robocopy exit code: %COPY_RESULT%
)
pause

if %COPY_RESULT% GEQ 8 exit /b %COPY_RESULT%
exit /b 0
