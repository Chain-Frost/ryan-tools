@echo off
setlocal enabledelayedexpansion

REM ========================
REM Parameters and Configurations
REM ========================
set "_items=flt asc rst"
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
      goto :env_found
    )
  )
)

:env_found
if defined ENV_SETUP (
  call "%ENV_SETUP%" || (
    echo Error: Failed to execute environment setup script.
    goto :eof
  )
  echo Environment setup script executed successfully.
) else (
  echo Error: OSGeo4W/QGIS environment setup script not found.
  goto :eof
)

REM ========================
REM GDAL Tool Verification
REM ========================
where gdal_translate >nul 2>&1 || (
  echo Error: gdal_translate not found.
  goto :eof
)
where gdaladdo >nul 2>&1 || (
  echo Error: gdaladdo not found.
  goto :eof
)

echo.
echo Starting TIF and Pyramid Generation
echo Max instances allowed: %_MAXinstances%
echo.

REM Kick off the recursive processing
call :treeProcess

echo.
echo Processing completed. Tasks may still be running in separate windows.
echo Press any key to exit.
pause >nul
endlocal
goto :eof


:treeProcess
REM ========================
REM File Processing Loop
REM ========================
echo Processing directory: "%CD%"

for %%i in (%_items%) do (
  for %%f in ("*.%%i") do (
    set "_inputFile=%%~f"
    set "_outputFile=%%~dpnf.tif"
    set "_proceed=YES"

    REM Skip if output is newer
    if exist "!_outputFile!" (
      REM extract just the filename.ext of the output for comparison
      for %%A in ("!_outputFile!") do set "_outputName=%%~nxa"
      REM list only the two files, newest first, and grab the first line
      for /f "delims=" %%j in ('dir /b /O:-D "!_outputFile!" "!_inputFile!"') do (
        set "NEWEST=%%j"
        goto :processNewest
      )
    )

    :processNewest
    if exist "!_outputFile!" (
      if /i "!NEWEST!"=="!_outputName!" (
        set "_proceed=NO"
        echo SKIP - "!_outputFile!" is up-to-date
      ) else (
        echo OVERWRITE - Input newer than "!_outputFile!"
      )
    )

    if /i "!_proceed!"=="YES" (
      REM Throttle parallel gdal_translate instances
      call :do_while_loop_start
      set /a count+=1
      echo Task !count! - Processing "!_inputFile!" -> "!_outputFile!"

      REM Launch both GDAL steps in a single cmd /C
      start "" /LOW /MIN cmd /C "gdal_translate %_commands% "%%~f" "%%~dpnf.tif" && gdaladdo %_gdaladdoCommands% -ro "%%~dpnf.tif""
      echo.
    )
  )
)

REM ========================
REM Recurse into subdirectories with error-checked pushd
REM ========================
for /d %%d in (*) do (
  pushd "%%d" >nul 2>&1 && (
    call :treeProcess
    popd
  ) || echo ERROR: Could not enter directory "%%d"
)
goto :eof


:do_while_loop_start
REM ================================================
REM Wait until number of gdal_translate.exe tasks < _MAXinstances
REM ================================================
for /f %%x in ('tasklist /fi "imagename eq gdal_translate.exe" /fo csv ^| find /c /v ""') do set "currentCount=%%x"
if !currentCount! geq %_MAXinstances% (
  if "!_notice!" equ "0" (
    echo Waiting for a free slot...
    set /a "_notice=1"
  )
  timeout /t 5 /nobreak >nul
  goto :do_while_loop_start
) else (
  set /a "_notice=0"
)
goto :eof
