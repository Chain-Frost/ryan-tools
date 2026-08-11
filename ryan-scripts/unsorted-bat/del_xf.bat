@echo off
REM ============================================================================
REM Recursively finds and deletes all directories named "xf".
REM
REM - Uses `dir /b /s /a:d xf` to recursively find all folders named exactly "xf".
REM - Deletes each matching tree synchronously so completion and exit status are
REM   reliable.
REM ============================================================================

setlocal

echo Searching recursively for 'xf' directories (this may take some time)...
dir /b /s /a:d xf 2>nul
if errorlevel 1 (
  echo No 'xf' directories found.
  pause
  endlocal
  exit /b 0
)

set "FAILED=0"
for /f "usebackq tokens=*" %%i in (`dir /b /s /a:d xf`) do (
  echo Deleting: %%i
  rd /s /q "%%i"
  if errorlevel 1 set "FAILED=1"
)

if "%FAILED%"=="1" (
  echo Completed with one or more deletion failures.
  pause
  endlocal
  exit /b 1
)

echo Complete.
pause
endlocal
exit /b 0
