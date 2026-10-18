@echo off
call "%~dp0packager.bat"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%~dp0install-local-wheel.bat" %*
exit /b %ERRORLEVEL%
