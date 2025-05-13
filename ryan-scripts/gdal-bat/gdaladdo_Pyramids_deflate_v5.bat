@echo off
setlocal enabledelayedexpansion

REM ========================
REM Parameters and Configurations
REM These parameters can be adjusted as needed.
REM ========================
set "_items=tif"
REM Set the options for the gdaladdo command
set "_gdaladdoCommands=--config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config NUM_THREADS ALL_CPUS --config SPARSE_OK TRUE"
set /a "_MAXinstances=%NUMBER_OF_PROCESSORS%"
set /a _Count=0
set /a _notice=0

echo Max instances allowed: %_MAXinstances%
echo.

REM ========================
REM Environment Setup
REM Search for OSGeo4W or QGIS o4w_env.bat and execute it.
REM ========================
set "ENV_SETUP="

REM Priority: OSGeo4W
if exist "C:\OSGEO4W\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\OSGEO4W\bin\o4w_env.bat"
) else (
    REM Search for the latest QGIS directory in "C:\Program Files"
    for /f "delims=" %%D in ('dir /b /ad /o-n "C:\Program Files\QGIS*" 2^>nul') do (
        if exist "C:\Program Files\%%D\bin\o4w_env.bat" (
            set "ENV_SETUP=C:\Program Files\%%D\bin\o4w_env.bat"
            goto :env_found
        )
    )
)

:env_found
if defined ENV_SETUP (
    call "%ENV_SETUP%"
    if errorlevel 1 (
        echo Error: Failed to execute environment setup script "%ENV_SETUP%".
        goto :eof
    ) else (
        echo Environment initialized via "%ENV_SETUP%".
    )
) else (
    echo Error: No OSGeo4W or QGIS environment setup script found.
    goto :eof
)

REM ========================
REM GDAL Tool Verification
REM ========================
where gdaladdo >nul 2>&1
if errorlevel 1 (
    echo Error: gdaladdo not found. Ensure GDAL is properly installed.
    goto :eof
)

echo.
echo Starting Pyramid Generation...
echo.

REM ========================
REM Start Processing Files
REM Process files in the current directory and its subdirectories.
REM ========================
call :treeProcess

echo.
echo All directories processed. Remaining tasks may still be running.
echo.
pause
endlocal
goto :eof

:treeProcess
REM ========================
REM File Processing Loop
REM Process each TIF file in the directory.
REM ========================
echo Processing directory: "%CD%"

for %%i in (%_items%) do (
    REM Loop through all files matching the current item type in the directory.
    for %%f in (*.%%i) do (
        set "_input=%%~f"
        set "_proceed=YES"

        REM If side-car exists *and* is newer, skip
        if exist "!_input!.ovr" (
            REM Find the most recently modified file between input and output.
            for /f "delims=" %%j in ('dir /b /o-d "!_input!.ovr" "%%~f"') do set "NEWEST=%%j"
            if /i "!NEWEST!"=="!_input!.ovr" (
                set "_proceed=NO"
                echo SKIP - "!_input!.ovr" is current
            ) else (
                echo Overwriting - source newer than "!_input!.ovr"
            )
        )

        REM If processing is needed, start the gdaladdo process.
        if /i "!_proceed!"=="YES" (
            call :throttle
            set /a _Count+=1
            echo Scheduling pyramid for: "!_input!" ^(Task !_Count!^)


            REM Start the gdaladdo command in a new minimized, low-priority window.
            start /LOW /MIN "!_Count! %%i %%f" gdaladdo %_gdaladdoCommands% -ro "!_input!"
            @REM gdaladdo %_gdaladdoCommands% -ro "!_input!"
            set /a _notice=0
            echo.
        )
    )
)

REM Recurse subdirectories…
for /D %%d in (*) do (
    pushd "%%d" 2>nul && (
        call :treeProcess
        popd
    ) || echo Warning: cannot enter "%%d"
)
goto :eof


:throttle
REM Throttle concurrent gdaladdo.exe processes
for /f %%x in ('tasklist /fi "imagename eq gdaladdo.exe" /fo csv ^| find /c /v ""') do set _running=%%x

if !_running! geq %_MAXinstances% (
    if !_notice! equ 0 (
        echo Waiting for a free slot...
    )
    set /a _notice=1
    REM Wait for 5 seconds before checking again.
    timeout /t 5 /nobreak >nul
    goto :throttle
) else (
    set /a _notice=0
)
goto :eof
