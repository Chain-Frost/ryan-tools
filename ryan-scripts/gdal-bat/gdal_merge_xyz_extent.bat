@echo off
setlocal enabledelayedexpansion

REM ================================
REM Merge Higginsville XYZ tiles that overlap a shapefile extent.
REM
REM Tile overlap is checked against the rectangular extent of the shapefile.
REM The XYZ tile filenames are expected to follow:
REM   DEM-GRID_001_<xmin>_<ymin>_<tile_size>m.xyz
REM ================================

set "PAUSE_AT_END=YES"
set "LIST_ONLY=NO"

for %%A in (%*) do (
    if /I "%%~A"=="--list-only" (
        set "LIST_ONLY=YES"
        set "PAUSE_AT_END=NO"
    )
    if /I "%%~A"=="--no-pause" (
        set "PAUSE_AT_END=NO"
    )
)

REM ================================
REM Environment Setup
REM ================================
set "ENV_SETUP="

REM Check for a non-interactive GDAL environment setup script.
if exist "C:\Program Files\QGIS 4.0.1\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\Program Files\QGIS 4.0.1\bin\o4w_env.bat"
) else if exist "C:\Program Files\QGIS 3.44.3\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\Program Files\QGIS 3.44.3\bin\o4w_env.bat"
) else if exist "C:\OSGEO4W\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\OSGEO4W\bin\o4w_env.bat"
) else (
    echo Error: GDAL environment setup script not found.
    goto end
)

call "%ENV_SETUP%"
if errorlevel 1 (
    echo Error: Failed to execute environment setup script.
    goto end
)

where ogrinfo >nul 2>&1
if errorlevel 1 (
    echo Error: ogrinfo not found. Ensure GDAL is properly installed.
    goto end
)

where gdalbuildvrt >nul 2>&1
if errorlevel 1 (
    echo Error: gdalbuildvrt not found. Ensure GDAL is properly installed.
    goto end
)

where gdal_translate >nul 2>&1
if errorlevel 1 (
    echo Error: gdal_translate not found. Ensure GDAL is properly installed.
    goto end
)

REM ================================
REM Set Input/Output Paths
REM ================================
REM Run this batch from the Q drive gdal-bat script folder.
REM Folder containing the individual XYZ files.
set "inputFolder=C:\Temp\westgold\higginsville\DTM_1.0m"
set "inputPattern=*.xyz"

REM Polygon whose rectangular extent limits the merged tiles.
set "extentShapefile=P:\26\RP26132 HIGGINSVILLE SURFACE WATER - WESRES\4 ENGINEERING\9 GIS\shp\Regional_Survey_Request.shp"

REM The survey request shapefile is GDA2020 MGA Zone 51.
set "outputSRS=EPSG:7851"
set "nodataValue=-9999"

REM Output mosaic filenames.
set "outputVRT=%inputFolder%\Higginsville_DTM_1m_Regional_Survey_Request_EPSG7851.vrt"
set "outputTIF=%inputFolder%\Higginsville_DTM_1m_Regional_Survey_Request_EPSG7851.tif"

if not exist "%inputFolder%" (
    echo Error: Input folder not found:
    echo   %inputFolder%
    goto end
)

if not exist "%extentShapefile%" (
    echo Error: Extent shapefile not found:
    echo   %extentShapefile%
    goto end
)

REM ================================
REM Read rectangular extent from shapefile
REM ================================
for /f "tokens=2,3,4,5 delims=(),- " %%A in ('ogrinfo -so -al "%extentShapefile%" ^| findstr /B /C:"Extent:"') do (
    set "extentXMin=%%A"
    set "extentYMin=%%B"
    set "extentXMax=%%C"
    set "extentYMax=%%D"
)

if not defined extentXMin (
    echo Error: Failed to read shapefile extent:
    echo   %extentShapefile%
    goto end
)

for /f "tokens=1 delims=." %%A in ("%extentXMin%") do set /a "extentXMinInt=%%A"
for /f "tokens=1 delims=." %%A in ("%extentYMin%") do set /a "extentYMinInt=%%A"
for /f "tokens=1 delims=." %%A in ("%extentXMax%") do set /a "extentXMaxInt=%%A"
for /f "tokens=1 delims=." %%A in ("%extentYMax%") do set /a "extentYMaxInt=%%A"

REM ================================
REM Build list of overlapping XYZ tiles
REM ================================
set "inputFileList=%TEMP%\gdalbuildvrt_higginsville_extent_%RANDOM%_%RANDOM%.txt"
if exist "%inputFileList%" del "%inputFileList%" >nul 2>&1

set /a selectedCount=0
set /a skippedCount=0

pushd "%inputFolder%" || (
    echo Error: Unable to change directory to:
    echo   %inputFolder%
    goto end
)

for %%F in (%inputPattern%) do (
    if exist "%%~fF" (
        set "tileX="
        set "tileY="
        set "tileSize="

        for /f "tokens=3,4,5 delims=_" %%A in ("%%~nF") do (
            set "tileX=%%A"
            set "tileY=%%B"
            set "tileSize=%%C"
        )

        set "tileSize=!tileSize:m=!"
        set "tileSize=!tileSize:M=!"

        if defined tileX if defined tileY if defined tileSize (
            set /a "tileMaxX=tileX+tileSize"
            set /a "tileMaxY=tileY+tileSize"

            if !tileMaxX! GEQ !extentXMinInt! if !tileX! LEQ !extentXMaxInt! if !tileMaxY! GEQ !extentYMinInt! if !tileY! LEQ !extentYMaxInt! (
                >>"%inputFileList%" echo %%~fF
                set /a selectedCount+=1
            )
        ) else (
            echo Warning: Skipping filename that does not match expected tile pattern: %%~nxF
            set /a skippedCount+=1
        )
    )
)

if !selectedCount! EQU 0 (
    echo Error: No XYZ tiles overlap the shapefile extent.
    del "%inputFileList%" >nul 2>&1
    popd
    goto end
)

REM ================================
REM Report settings
REM ================================
echo.
echo Input folder        : %inputFolder%
echo Input pattern       : %inputPattern%
echo Extent shapefile    : %extentShapefile%
echo Extent xmin ymin    : %extentXMin% %extentYMin%
echo Extent xmax ymax    : %extentXMax% %extentYMax%
echo Output SRS          : %outputSRS%
echo Selected XYZ tiles  : !selectedCount!
echo Skipped filenames   : !skippedCount!
echo Input file list     : %inputFileList%
echo Output VRT          : %outputVRT%
echo Output GeoTIFF      : %outputTIF%
echo.

if /I "%LIST_ONLY%"=="YES" (
    echo Selected files:
    type "%inputFileList%"
    del "%inputFileList%" >nul 2>&1
    popd
    goto end
)

REM ================================
REM Remove existing outputs if present
REM ================================
if exist "%outputVRT%" (
    echo Warning: Existing VRT will be overwritten:
    echo   %outputVRT%
    del "%outputVRT%" >nul 2>&1
)

if exist "%outputTIF%" (
    echo Warning: Existing GeoTIFF will be overwritten:
    echo   %outputTIF%
    del "%outputTIF%" >nul 2>&1
)

REM ================================
REM Build VRT from selected XYZ files and crop to rectangular extent
REM ================================
echo Building VRT from selected XYZ files...
gdalbuildvrt -a_srs %outputSRS% -vrtnodata %nodataValue% -te %extentXMin% %extentYMin% %extentXMax% %extentYMax% -input_file_list "%inputFileList%" "%outputVRT%"
if errorlevel 1 (
    echo Error: gdalbuildvrt failed.
    del "%inputFileList%" >nul 2>&1
    popd
    goto end
)

echo Converting VRT to GeoTIFF...
gdal_translate -of GTiff -a_nodata %nodataValue% -co COMPRESS=DEFLATE -co TILED=YES -co BIGTIFF=IF_SAFER -co SPARSE_OK=YES -co NUM_THREADS=ALL_CPUS "%outputVRT%" "%outputTIF%"
if errorlevel 1 (
    echo Error: gdal_translate failed.
    del "%inputFileList%" >nul 2>&1
    popd
    goto end
)

del "%inputFileList%" >nul 2>&1

echo.
echo Filtered mosaic created successfully:
echo   %outputTIF%
echo.

popd

:end
if /I "%PAUSE_AT_END%"=="YES" pause
endlocal
