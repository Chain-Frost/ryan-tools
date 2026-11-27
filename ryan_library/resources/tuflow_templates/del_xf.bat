@echo off
REM ============================================================================
REM Recursively finds and deletes all directories named "xf".
REM
REM - Uses `dir /b /s /a:d xf` to recursively find all folders named exactly "xf".
REM - Deletes each matching tree synchronously so completion and exit status are
REM   reliable.
REM
REM Maintainer-confirmed behaviour (2026-11-15):
REM - Intended use is to double-click this file in the folder to clean recursively.
REM - When launched from cmd.exe, use the caller's current working directory.
REM - Delete every matching xf directory immediately, including its contents.
REM - Do not change directory or add confirmation, dry-run or safety checks.
REM - The existing pauses report completion or failure; they are not approval.
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
