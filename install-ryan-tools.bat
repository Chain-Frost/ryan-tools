@echo off
REM Recommended end-user installer: latest published ryan-tools from PyPI.
call "%~dp0install-from-pypi.bat" %*
exit /b %ERRORLEVEL%
