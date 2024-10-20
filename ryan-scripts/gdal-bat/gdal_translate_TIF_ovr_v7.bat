@echo off
setlocal enabledelayedexpansion

REM ========================
REM Parameters and Configurations
REM These parameters can be adjusted as needed.
REM ========================
set _items=flt asc rst
set _commands=-stats -co COMPRESS=DEFLATE -co PREDICTOR=2 -co NUM_THREADS=ALL_CPUS -co SPARSE_OK=TRUE -co BIGTIFF=IF_SAFER
set _gdaladdoCommands=--config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config NUM_THREADS ALL_CPUS --config SPARSE_OK TRUE
set /a _MAXinstances=%NUMBER_OF_PROCESSORS%
set /a count=0
set /a _notice=0

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
REM Ensure that gdal_translate is available in the system PATH.
REM ========================
where gdal_translate
IF ERRORLEVEL 1 (
    echo Error: gdal_translate not found. Ensure GDAL is properly installed.
    goto :eof
)

echo.
echo Starting TIF and Pyramid Generation
echo Max instances allowed: %_MAXinstances%
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
REM Process each file that matches the item types (flt, asc, rst).
REM ========================
echo Processing in directory: !cd!

for %%i in (%_items%) do (
    
    REM Loop through all files matching the current item type in the directory.
    for %%f in (*%%i) do (
        set "_inputFile=%%f"
        set "_outputFile=!_inputFile:~0,-3!tif"
        set "_proceed=YES"

        REM Check if the output TIF file already exists.
        if exist "!_outputFile!" (
            REM Find the most recently modified file between the input and output.
            FOR /F %%j IN ('DIR /B /O:D "!_outputFile!" "%%f"') DO SET NEWEST=%%j
            
            REM If the output file is the newest, skip processing.
            if /i "!NEWEST!"=="!_outputFile!" (
                set "_proceed=NO"
                echo SKIP - Output file "!_outputFile!" is up-to-date
            ) ELSE (
                echo Overwriting - Input file is newer than "!_outputFile!"
            )
        )

        REM If processing is needed, create a temporary script and start the process.
        if /i "!_proceed!"=="YES" (
            set "_tempScript=temp_!_inputFile:~0,-3!cmd"
            echo gdal_translate %_commands% "%%f" "!_outputFile!" ^&^& gdaladdo %_gdaladdoCommands% -ro "!_outputFile!" ^&^& del "!_tempScript!" ^&^& exit >"!_tempScript!"
            
            REM Monitor the number of running gdal_translate tasks.
            call :do_while_loop_start
            
            set /a _CountPlus=!count! + 1
            echo Task !_CountPlus! started: "!_inputFile!" -> "!_outputFile!"
            
            REM Start the translation in a new minimized, low-priority window.
            Start /LOW /MIN "!_CountPlus! %%i %%f" "!_tempScript!"
            
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
REM Ensure that the number of concurrent gdal_translate tasks does not exceed the maximum allowed.
REM ========================
for /f %%x in ('tasklist /fi "imagename eq gdal_translate.exe" /fo csv ^| find /c /v ""') do set count=%%x

if !count! geq %_MAXinstances% (
    if !_notice! equ 0 (
        echo Waiting for a free slot...
    )
    set /a _notice=1
    REM Wait for 5 seconds before checking again.
    timeout /t 5 /nobreak >nul
    goto do_while_loop_start
)
