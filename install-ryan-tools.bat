@echo off
REM Recommended end-user installer: latest published ryan_functions from PyPI.
call "%~dp0install-latest-wheel.bat" %*
exit /b %ERRORLEVEL%
