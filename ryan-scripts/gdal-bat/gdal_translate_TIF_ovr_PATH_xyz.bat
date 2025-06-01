@echo off
setlocal enabledelayedexpansion

REM Set the input folder and change working directory to it.
pushd "D:\temp\Worsley\Part1\Part1\TIFs\Selected"

REM Set the output folder – all TIFF files will be saved here.
set "outputFolder=D:\temp\Worsley\Part1\Part1\TIFs\Selected"
if not exist "%outputFolder%" mkdir "%outputFolder%"

REM ========================
REM Parameters and Configurations
REM ========================
set "_items=.xyz"
set "_commands=-stats -co COMPRESS=DEFLATE -co PREDICTOR=2 -co NUM_THREADS=ALL_CPUS -co SPARSE_OK=TRUE -co BIGTIFF=IF_SAFER"
set "_gdaladdoCommands=--config COMPRESS_OVERVIEW DEFLATE --config PREDICTOR_OVERVIEW 2 --config NUM_THREADS ALL_CPUS --config SPARSE_OK TRUE"
set /a "_MAXinstances=%NUMBER_OF_PROCESSORS%"
set /a count=0
set /a "_notice=0"

REM ========================
REM Environment Setup
REM ========================
set "ENV_SETUP="

REM Use non-interactive o4w_env.bat.
if exist "C:\Program Files\QGIS 3.42.0\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\Program Files\QGIS 3.42.0\bin\o4w_env.bat"
) else if exist "C:\OSGEO4W\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\OSGEO4W\bin\o4w_env.bat"
) else (
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
    echo Error: OSGeo4W or QGIS environment setup script not found.
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
pause
popd
endlocal
goto :eof

:treeProcess
echo Processing in directory: "%CD%"
for %%i in (%_items%) do (
    for %%f in ("*%%i") do (
        if exist "%%~f" (
            set "_inputFile=%%~f"
            REM Save output TIFF in the output folder using the same base filename.
            set "_outputFile=%outputFolder%\%%~nf.tif"
            set "_proceed=YES"
            if exist "!_outputFile!" (
                for /f "delims=" %%j in ('dir /b /o-d "!_outputFile!" "!_inputFile!"') do set "NEWEST=%%j"
                if /i "!NEWEST!"=="!_outputFile!" (
                    set "_proceed=NO"
                    echo SKIP - Output file "!_outputFile!" is up-to-date
                ) else (
                    echo Overwriting - Input file is newer than "!_outputFile!"
                )
            )
            if /i "!_proceed!"=="YES" (
                for %%g in ("%%~nf") do set "fileName=%%~ng"
                set "_tempScript=%~dp0temp_!fileName!.bat"
                echo gdal_translate %_commands% "%%~f" "!_outputFile!" ^&^& gdaladdo %_gdaladdoCommands% -ro "!_outputFile!" ^&^& del "!_tempScript!" ^&^& exit >"!_tempScript!"
                call :do_while_loop_start
                set /a count+=1
                echo Task !count! started: "%%~f" -> "!_outputFile!"
                start /LOW /MIN "!count! %%i %%f" "!_tempScript!"
                set /a "_notice=0"
                echo.
            )
        )
    )
)
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
for /f %%x in ('tasklist /fi "imagename eq gdal_translate.exe" /fo csv ^| find /c /v ""') do set "currentCount=%%x"
if !currentCount! geq %_MAXinstances% (
    if !"_notice!" equ "0" (
        echo Waiting for a free slot...
        set /a "_notice=1"
    )
    timeout /t 5 /nobreak >nul
    goto do_while_loop_start
) else (
    set /a "_notice=0"
)
goto :eof
