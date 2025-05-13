@echo off
setlocal enabledelayedexpansion

@REM to run from here, to a different location.
@REM start "" /D "P:\BGER\PER\RP20180.365 BLACKSMITH SCOPING STUDY - FMG\7 DOCUMENT CONTROL\2 RECEIVED DATA\1 CLIENT\20250509 - Data Pack #1\WIP alignments (3, 4 and 4A)" "Q:\BGER\PER\RPRT\ryan-tools\ryan-scripts\gdal-bat\gdal_translate_TIF_ovr_PATH_rename.bat"


REM ========================
REM Parameters and Configurations
REM ========================
set "_items=flt asc rst tif"
set "_commands=-stats -co COMPRESS=DEFLATE -co PREDICTOR=2 -co NUM_THREADS=ALL_CPUS -co SPARSE_OK=TRUE -co BIGTIFF=IF_SAFER"
set "_gdaladdoCommands=--config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config NUM_THREADS ALL_CPUS --config SPARSE_OK TRUE"
set /a "_MAXinstances=%NUMBER_OF_PROCESSORS%"
set /a count=0
set /a "_notice=0"

REM ========================
REM Environment Setup
REM ========================
set "ENV_SETUP="

REM Priority: Check for OSGeo4W environment setup script first
if exist "C:\OSGEO4W\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\OSGEO4W\bin\o4w_env.bat"
) else (
    REM Search for the latest QGIS directory in "C:\Program Files"
    for /f "delims=" %%D in ('dir /b /ad /o-n "C:\Program Files\QGIS*" 2^>nul') do (
        if exist "C:\Program Files\%%D\bin\o4w_env.bat" (
            set "ENV_SETUP=C:\Program Files\%%D\bin\o4w_env.bat"
            goto env_found
        )
    )
)

:env_found
if defined ENV_SETUP (
    call "%ENV_SETUP%"
    if errorlevel 1 (
        echo Error: Failed to execute environment setup script.
        goto :eof
    ) else (
        echo Environment setup script executed successfully.
    )
) else (
    echo Error: OSGeo4W or QGIS environment setup script not found in "C:\Program Files".
    goto :eof
)

REM ========================
REM GDAL Tool Verification
REM ========================
where gdal_translate >nul 2>&1
IF ERRORLEVEL 1 (
    echo Error: gdal_translate not found. Ensure GDAL is properly installed.
    goto :eof
)

where gdaladdo >nul 2>&1
IF ERRORLEVEL 1 (
    echo Error: gdaladdo not found. Ensure GDAL is properly installed.
    goto :eof
)

echo.
echo Starting TIF and Pyramid Generation
echo Max instances allowed: %_MAXinstances%
echo.

REM ========================
REM Start Processing Files
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
REM ========================
echo Processing in directory: "%CD%"

for %%i in (%_items%) do (
    for %%f in (*.%%i) do (
        echo   -- looking for *.%%i, got %%~f
        if exist "%%~f" (
            set "_inputFile=%%~f"
			REM adjust file name here
            set "_outputFile=%%~dpf%%~nf_compress.tif"
            set "_proceed=YES"

            REM Check if the output TIF file already exists.
            if exist "!_outputFile!" (
                REM Find the most recently modified file between the input and output.
                for /f "delims=" %%j in ('dir /b /o-d "!_outputFile!" "!_inputFile!"') do set "NEWEST=%%j"
                
                REM If the output file is the newest, skip processing.
                if /i "!NEWEST!"=="!_outputFile!" (
                    set "_proceed=NO"
                    echo SKIP - Output file "!_outputFile!" is up-to-date
                ) else (
                    echo Overwriting - Input file is newer than "!_outputFile!"
                )
            )

            REM If processing is needed, create a temporary script and start the process.
            if /i "!_proceed!"=="YES" (
                REM Extract the filename without extension for the temp script
                for %%g in ("%%~nf") do set "fileName=%%~ng"
                set "_tempScript=%~dp0temp_!fileName!.bat"

                echo gdal_translate %_commands% "%%~f" "!_outputFile!" ^&^& gdaladdo %_gdaladdoCommands% -ro "!_outputFile!" ^&^& del "!_tempScript!" ^&^& exit >"!_tempScript!"
                
                REM Monitor the number of running gdal_translate tasks.
                call :do_while_loop_start

                set /a count+=1
                echo Task !count! started: "%%~f" -> "!_outputFile!"

                REM Start the translation in a new minimized, low-priority window.
                start /LOW /MIN "!count! %%i %%f" "!_tempScript!"
                
                set /a "_notice=0"
                echo.
            )
        )
    )
)

REM Recursively process subdirectories.
for /d %%d in (*) do (
    pushd "%%d" || (
        echo Error: Failed to enter directory "%%d"
        echo.
        continue
    )
    call :treeProcess
    popd
)
goto :eof

:do_while_loop_start
REM ========================
REM Monitor Running Tasks
REM ========================
for /f %%x in ('tasklist /fi "imagename eq gdal_translate.exe" /fo csv ^| find /c /v ""') do set "currentCount=%%x"

if !currentCount! geq %_MAXinstances% (
    if !"_notice"! equ "0" (
        echo Waiting for a free slot...
        set /a "_notice=1"
    )
    REM Wait for 5 seconds before checking again.
    timeout /t 5 /nobreak >nul
    goto do_while_loop_start
) else (
    set /a "_notice=0"
)
goto :eof
