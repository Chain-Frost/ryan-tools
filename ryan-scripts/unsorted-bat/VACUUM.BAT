@echo off
REM ============================================================================
REM Rebuilds and shrinks all GeoPackage (.gpkg) databases in the directory.
REM
REM What this does: When you delete features or layers from a GeoPackage (SQLite),
REM the file size doesn't automatically shrink. VACUUM rebuilds the database,
REM purging the empty space and reducing the file size on disk.
REM ============================================================================

setlocal

if not exist *.gpkg (
  echo No GeoPackages found.
  Pause
  endlocal
  exit /b 0
)

REM Use an existing ogrinfo first, then search standalone OSGeo4W and versioned
REM QGIS installations. Reverse name sorting prefers QGIS 4 over QGIS 3.
where ogrinfo >nul 2>&1
if not errorlevel 1 goto ogrinfo_ready

set "ENV_SETUP="
if exist "C:\OSGeo4W\bin\o4w_env.bat" set "ENV_SETUP=C:\OSGeo4W\bin\o4w_env.bat"
if not defined ENV_SETUP if exist "C:\OSGeo4W64\bin\o4w_env.bat" set "ENV_SETUP=C:\OSGeo4W64\bin\o4w_env.bat"

if not defined ENV_SETUP (
  for /f "delims=" %%D in ('dir /b /ad /o-n "%ProgramFiles%\QGIS*" 2^>nul') do (
    if exist "%ProgramFiles%\%%D\bin\o4w_env.bat" (
      set "ENV_SETUP=%ProgramFiles%\%%D\bin\o4w_env.bat"
      goto environment_found
    )
  )
)

if not defined ENV_SETUP (
  for /f "delims=" %%D in ('dir /b /ad /o-n "%ProgramFiles(x86)%\QGIS*" 2^>nul') do (
    if exist "%ProgramFiles(x86)%\%%D\bin\o4w_env.bat" (
      set "ENV_SETUP=%ProgramFiles(x86)%\%%D\bin\o4w_env.bat"
      goto environment_found
    )
  )
)

:environment_found
if not defined ENV_SETUP (
  echo Error: Unable to locate ogrinfo through PATH, OSGeo4W, or QGIS.
  endlocal
  exit /b 1
)

call "%ENV_SETUP%"
if errorlevel 1 (
  echo Error: Failed to initialise "%ENV_SETUP%".
  endlocal
  exit /b 1
)

where ogrinfo >nul 2>&1
if errorlevel 1 (
  echo Error: ogrinfo was not available after initialising "%ENV_SETUP%".
  endlocal
  exit /b 1
)

:ogrinfo_ready
set "FAILED=0"
for %%f in (*.gpkg) do (
  ogrinfo "%%f" -sql "VACUUM"
  if errorlevel 1 set "FAILED=1"
)
Pause
endlocal & exit /b %FAILED%
