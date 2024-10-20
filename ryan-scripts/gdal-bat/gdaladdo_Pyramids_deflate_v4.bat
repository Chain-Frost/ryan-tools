@echo off
setlocal enabledelayedexpansion

REM ========================
REM Parameters and Configurations
REM These parameters can be adjusted as needed.
REM ========================
set _items=tif
REM Set the options for the gdaladdo command
set _gdaladdoCommands=--config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config NUM_THREADS ALL_CPUS --config SPARSE_OK TRUE
set /a _MAXinstances=6
set /a count=0
set /a _notice=0

echo Max instances allowed: %_MAXinstances%
echo.

REM ========================
REM Environment Setup
REM Check if the OSGeo4W environment setup script exists and execute it.
REM ========================
IF EXIST "C:\OSGEO4W\bin\o4w_env.bat" (
    Call "C:\OSGEO4W\bin\o4w_env.bat"
) ELSE (
    echo Error: OSGeo4W environment setup script not found at C:\OSGEO4W\bin\o4w_env.bat.
    goto :eof
)

REM ========================
REM GDAL Tool Verification
REM Ensure that gdaladdo is available in the system PATH.
REM ========================
where gdaladdo
IF ERRORLEVEL 1 (
    echo Error: gdaladdo not found. Ensure GDAL is properly installed.
    goto :eof
)

echo.
echo Starting Pyramid Generation
echo.

REM ========================
REM Start Processing Files
REM Process files in the current directory and its subdirectories.
REM ========================
call :treeProcess

echo.
echo Processing completed. Tasks may still be running in separate windows.
echo.
Pause
endlocal
goto :eof

:treeProcess
REM ========================
REM File Processing Loop
REM Process each TIF file in the directory.
REM ========================
echo Processing in directory: !cd!

for %%i in (%_items%) do (
    
    REM Loop through all files matching the current item type in the directory.
    for %%f in (*%%i) do (
        set "_inputFile=%%f"
        set "_outputFile=!_inputFile!.ovr"
        set "_proceed=YES"

        REM Check if the output OVR file already exists.
        if exist "!_outputFile!" (
            REM Find the most recently modified file between input and output.
            FOR /F %%j IN ('DIR /B /O:D "!_outputFile!" "%%f"') DO SET NEWEST=%%j
            
            REM If the output file is the newest, skip processing.
            if /i "!NEWEST!"=="!_outputFile!" (
                set "_proceed=NO"
                echo SKIP - Output file "!_outputFile!" is up-to-date
            ) ELSE (
                echo Overwriting - Input file is newer than "!_outputFile!"
            )
        )

        REM If processing is needed, start the gdaladdo process.
        if /i "!_proceed!"=="YES" (
            call :do_while_loop_start
            
            set /a _CountPlus=!count! + 1
            echo Task !_CountPlus! started: "!_inputFile!" -> "!_outputFile!"
            
            REM Start the gdaladdo command in a new minimized, low-priority window.
            Start /LOW /MIN "!_CountPlus! %%i %%f" gdaladdo %_gdaladdoCommands% -ro "!_inputFile!"
            
            set /a _notice=0
            echo.
        )
    )
)

REM Recursively process subdirectories.
for /D %%d in (*) do (
    cd %%d
    call :treeProcess
    cd ..
)
goto :eof

:do_while_loop_start
REM ========================
REM Monitor Running Tasks
REM Ensure that the number of concurrent gdaladdo tasks does not exceed the maximum allowed.
REM ========================
for /f %%x in ('tasklist /fi "imagename eq gdaladdo.exe" /fo csv ^| find /c /v ""') do set count=%%x

if !count! geq %_MAXinstances% (
    if !_notice! equ 0 (
        echo Waiting for a free slot...
    )
    set /a _notice=1
    REM Wait for 5 seconds before checking again.
    timeout /t 5 /nobreak >nul
    goto do_while_loop_start
)
