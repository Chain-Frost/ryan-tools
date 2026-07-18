@echo off
REM Compatibility wrapper. Prefer install-latest-wheel.bat for new usage.
call "%~dp0install-latest-wheel.bat" %*
exit /b %ERRORLEVEL%
