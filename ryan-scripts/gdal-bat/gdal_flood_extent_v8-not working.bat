@echo off
setlocal enabledelayedexpansion

REM ========================
REM Parameters and Configurations
REM These parameters can be adjusted as needed.
REM ========================
set "_file_extension=.tif"
set "_cutoff=0.01 0.1 0.10 1.0 1 10 100 10.0 1000"
set "_commandsCalc=--calc=\"where(A>=%%%c,1,0)\" --NoDataValue=0"
set /a "_instances=%NUMBER_OF_PROCESSORS%"
set "_CreateOpts=--co COMPRESS=DEFLATE --co PREDICTOR=2 --co NUM_THREADS=ALL_CPUS --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER"

REM Specify filenames to process (leave empty to process all files with the extension)
set "_filenames="

REM ========================
REM Environment Setup
REM Enhanced environment detection and setup
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
REM Dynamic Path Resolution for GDAL Tools
REM ========================

REM Function to find the path of a given executable or script
:find_tool
REM %1 = Tool name
REM %2 = Variable name to set
where %1 >nul 2>&1
if not errorlevel 1 (
    for /f "delims=" %%a in ('where %1') do (
        set "%2=%%a"
        goto :found_%2%
    )
)
REM If not found in PATH, search common installation directories
REM Check OSGeo4W Python Scripts
set "possible_paths=C:\OSGeo4W\apps\Python*\Scripts;C:\Program Files\QGIS*\apps\Python*\Scripts"
for %%p in (%possible_paths%) do (
    for /f "delims=" %%d in ('dir /b /ad %%~p 2^>nul') do (
        if exist "%%~p\%%d\%1" (
            set "%2=%%~p\%%d\%1"
            goto :found_%2%
        )
    )
)
REM If still not found, set to empty
set "%2="
:found_%2%
goto :eof

REM Find gdal_translate.exe
call :find_tool gdal_translate.exe _gdal_translate
if not defined _gdal_translate (
    echo Error: gdal_translate.exe not found. Ensure GDAL is properly installed and in PATH.
    goto :eof
)

REM Find gdal_calc.py
call :find_tool gdal_calc.py _gdal_calc
if not defined _gdal_calc (
    echo Error: gdal_calc.py not found. Ensure it is correctly installed and in PATH.
    goto :eof
)

REM Find gdal_polygonize.py
call :find_tool gdal_polygonize.py _gdal_polygonize
if not defined _gdal_polygonize (
    echo Error: gdal_polygonize.py not found. Ensure it is correctly installed and in PATH.
    goto :eof
)

REM ========================
REM Check for Required Components
REM Ensure that required tools are available.
REM ========================
where python >nul 2>&1
IF ERRORLEVEL 1 (
    echo Error: Python not found. Ensure Python is properly installed and in PATH.
    goto :eof
)

echo.
echo Command Calculation Parameters:
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
set "_p=%~1"
for %%c in (%_cutoff%) do (
    REM Standardize the cutoff value to two decimal places
    set "_formattedValue=%%c"
    if "!_formattedValue:~-2!"==".0" set "_formattedValue=!_formattedValue:~0,-2!"
    if "!_formattedValue:~0,1!"=="0" set "_formattedValue=!_formattedValue:~1!"
    set "_formattedValue=!_formattedValue:.=!

    echo Processing cutoff %%c for file %_p%
    call :do_while_loop_start

    set "_D=!_p:~0,-4!"
    set "_outname=!_D!_FE_!_formattedValue!m.tif"
    set "_shpname=!_D!_FE_!_formattedValue!m.shp"
    set "_temp=%TEMP%\Ztemp_!_D:~0,-5!.cmd"

    REM Create the temporary command file to execute GDAL operations
    (
        echo echo.
        echo echo Calculating cutoff raster then converting to polygon. Closes when done.
        echo python "!_gdal_calc!" %_commandsCalc% -A "!_p!" --outfile="!_outname!" %_CreateOpts%
        echo python "!_gdal_polygonize!" "!_outname!" "!_shpname!"
        echo del "!_temp!" ^&^& exit
    ) > "!_temp!"

    REM Start the task in a new minimized, low-priority window
    Start /LOW /MIN "!_p! %%c" "!_temp!"

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

if !count! geq %_instances% (
    if !_notice! equ 0 (
        echo Waiting for a free slot...
    )
    set /a _notice=1
    timeout /t 15 /nobreak >nul
    goto do_while_loop_start
)
set /a _notice=0
goto :eof
