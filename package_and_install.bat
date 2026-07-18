@echo off
call "%~dp0packager.bat"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%~dp0install-latest-wheel.bat"
exit /b %ERRORLEVEL%
