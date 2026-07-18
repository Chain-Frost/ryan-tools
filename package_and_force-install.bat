@echo off
call "%~dp0packager.bat"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%~dp0install-latest-wheel.bat" --force-reinstall
exit /b %ERRORLEVEL%
