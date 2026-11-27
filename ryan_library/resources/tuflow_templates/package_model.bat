@echo off
setlocal

pushd "%~dp0" || exit /b 1

set "TUFLOW_EXE=<<TUFLOW_EXE>>"
set "MODEL_TCF=runs\__project___01_~s2~_~s1~_~e1~_~e2~_~e4~_~e3~_~e5~_~s4~.tcf"

if not exist "%TUFLOW_EXE%" (
    echo ERROR: TUFLOW executable not found: "%TUFLOW_EXE%"
    popd
    pause
    exit /b 1
)
if not exist "%MODEL_TCF%" (
    echo ERROR: Model TCF not found: "%MODEL_TCF%"
    popd
    pause
    exit /b 1
)
if not exist "pm.ini" (
    echo ERROR: Package-model INI not found: "pm.ini"
    popd
    pause
    exit /b 1
)

START /LOW /WAIT "Package Model" "%TUFLOW_EXE%" -pmAllini "pm.ini" -xf0 "%MODEL_TCF%"
set "RESULT=%ERRORLEVEL%"

popd
if not "%RESULT%"=="0" echo ERROR: TUFLOW packaging failed with exit code %RESULT%.
pause
exit /b %RESULT%
