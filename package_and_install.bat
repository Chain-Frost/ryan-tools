@echo off
REM Build locally, then install that wheel with its dependencies and the QGIS MCP bridge.
call "%~dp0packager.bat"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%~dp0install-local-wheel.bat" %*
exit /b %ERRORLEVEL%
