@echo off
call "%~dp0install-local-wheel.bat" --force-reinstall %*
exit /b %ERRORLEVEL%
