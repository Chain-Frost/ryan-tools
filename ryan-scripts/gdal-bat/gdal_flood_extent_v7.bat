@echo off
setlocal enabledelayedexpansion

REM ========================
REM Parameters and Configurations
REM These parameters can be adjusted as needed.
REM ========================
set _file_extension=.tif
set _cutoff=0.01 0.1 0.10 1.0 1 10 100 10.0 1000
set _commandsCalc=--calc="where(A>=%%%c,1,0)" --NoDataValue=0
set /a _instances=%NUMBER_OF_PROCESSORS%
set _CreateOpts=--co COMPRESS=DEFLATE --co PREDICTOR=2 --co NUM_THREADS=ALL_CPUS --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER
set _gdal_calc=python3 "C:\OSGeo4W\apps\Python312\Scripts\gdal_calc.py"
set _gdal_polygonize=python3 "C:\OSGeo4W\apps\Python312\Scripts\gdal_polygonize.py"
set _gdal_translate="C:\OSGeo4W\bin\gdal_translate.exe"

REM Specify filenames to process (leave empty to process all files with the extension)
set _filenames=

REM ========================
REM Environment Setup
REM Check if the OSGeo4W environment setup script exists and execute it.
REM ========================
IF EXIST "C:\OSGEO4W\bin\o4w_env.bat" (
    Call "C:\OSGEO4W\bin\o4w_env.bat"
)

REM ========================
REM Check for Required Components
REM Ensure that required tools are available in the system PATH.
REM ========================
where gdal_translate
IF ERRORLEVEL 1 (
    echo Error: gdal_translate not found. Ensure GDAL is properly installed.
    goto :eof
)

where python
IF ERRORLEVEL 1 (
    echo Error: Python not found. Ensure Python is properly installed.
    goto :eof
)

IF NOT EXIST "%_gdal_calc%" (
    echo Error: gdal_calc.py not found. Ensure it is correctly installed.
    goto :eof
)

IF NOT EXIST "%_gdal_polygonize%" (
    echo Error: gdal_polygonize.py not found. Ensure it is correctly installed.
    goto :eof
)

echo %_commandsCalc%
echo.

REM ========================
REM Start Processing Files
REM Process the specified files or all files with the specified extension.
REM ========================
if defined _filenames (
    REM Process specific filenames provided in _filenames
    for %%a in (%_filenames%) do (
        call :process_file "%%a"
    )
) else (
    REM Process all files with the specified extension
    for %%a in (*%_file_extension%) do (
        call :process_file "%%a"
    )
)

echo.
echo Complete
Pause
goto :eof

:process_file
REM ========================
REM Process a Single File
REM Process a file with the given cutoff values.
REM ========================
set _p=%~1
for %%c in (%_cutoff%) do (
    REM Standardize the cutoff value to two decimal places
    set _formattedValue=%%c
    if "!_formattedValue:~-2!"==".0" set _formattedValue=!_formattedValue:~0,-2!
    if "!_formattedValue:~0,1!"=="0" set _formattedValue=!_formattedValue:~1!
    set _formattedValue=!_formattedValue:.=!

    echo Processing cutoff %%c for file %_p%
    call :do_while_loop_start

    set /a _notice=0
    set _D=!_p:~0,-4!
    set _outname=!_D!_FE_!_formattedValue!m.tif
    set _shpname=!_D!_FE_!_formattedValue!m.shp
    set _temp=Ztemp_!_D:~0,-5!.cmd

    REM Create the temporary command file to execute GDAL operations
    echo echo. ^&^& echo   Calculating cutoff raster then converting to polygon. Closes when done. > "!_temp!"
    echo %_gdal_calc% %_commandsCalc% -A "!_p!" --outfile="!_outname!" %_CreateOpts% >> "!_temp!"
    echo %_gdal_polygonize% "!_outname!" "!_shpname!" >> "!_temp!"
    echo del "!_temp!" ^&^& exit >> "!_temp!"

    REM Start the task in a new minimized, low-priority window
    Start /LOW /MIN "!_CountPlus! %_p% %%c" "!_temp!"

    set /a _notice=0
    echo.
)
goto :eof

:do_while_loop_start
REM ========================
REM Monitor Running Python Tasks
REM Ensure that the number of concurrent Python tasks does not exceed the maximum allowed.
REM ========================
set /a count=0
for /f %%x in ('tasklist ^| find /c "python"') do set count=%%x

if %count% geq %_instances% (
    if !_notice! equ 0 (
        echo Waiting for a free slot...
    )
    set /a _notice=1
    timeout /t 15 /nobreak >nul
    goto do_while_loop_start
)
