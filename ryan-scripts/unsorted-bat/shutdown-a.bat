@echo off
SETLOCAL ENABLEDELAYEDEXPANSION
:do_while_loop_start
echo !time!
shutdown -a
timeout /t 2
echo.
goto do_while_loop_start
