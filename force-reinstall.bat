@echo off
call "%~dp0install-latest-wheel.bat" --force-reinstall %*
exit /b %ERRORLEVEL%
