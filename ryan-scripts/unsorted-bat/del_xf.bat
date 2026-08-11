@echo off
REM ============================================================================
REM Recursively finds and deletes all directories named "xf".
REM 
REM - Uses `dir /b /s /a:d xf` to recursively find all folders named exactly "xf".
REM - Launches a separate minimized background process (`start /min`) for each
REM   deletion (`rd /s /q`) to speed up bulk removals across network drives.
REM 
REM NOTE: Due to the background processes, the main console will say "Complete"
REM       while the actual deletions may still be running in the background.
REM ============================================================================

setlocal enabledelayedexpansion

echo Searching recursively for 'xf' directories (this may take some time)...
for /f "usebackq tokens=*" %%i in (`dir /b /s /a:d xf`) do (
  REM Delete the directories and any files or subdirectories asynchronously
  echo Found: %%i
  start /min rd /s /q "%%i" ^&^& exit
)

endlocal
echo Complete! (Separate minimized windows may still be running to finish the deletions)
pause
exit /b