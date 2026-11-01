@echo off
REM Build locally, then reinstall that wheel without installing or updating dependencies.
call "%~dp0build-local-wheel.bat"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%~dp0install-local-wheel.bat" --force-reinstall %*
exit /b %ERRORLEVEL%
