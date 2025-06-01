@echo off
setlocal

REM ================================
REM Environment Setup
REM ================================
set "ENV_SETUP="

REM Check for a non-interactive GDAL environment setup script.
if exist "C:\Program Files\QGIS 3.42.0\bin\o4w_env.bat" (
    set "ENV_SETUP=C:\Program Files\QGIS 3.42.0\bin\o4w_env.bat"
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

REM ================================
REM Set Input/Output Paths
REM ================================
REM Folder containing the individual TIFF files
set "inputFolder=D:\temp\Worsley\Selected"

REM Output mosaic filenames (adjust these paths as needed)
set "outputVRT=D:\temp\Worsley\Selected\250304_LIDAR_BDY_Merged.vrt"
set "outputTIF=D:\temp\Worsley\Selected\250304_LIDAR_BDY_Merged.tif"

REM ================================
REM Change to Input Folder
REM ================================
pushd "%inputFolder%"

echo Building VRT from TIFF files in %inputFolder%...
REM Create a VRT mosaic of all .tif files in the folder.
gdalbuildvrt "%outputVRT%" *.tif
if errorlevel 1 (
    echo Error: gdalbuildvrt failed.
    popd
    goto end
)

echo Converting VRT to GeoTIFF...
REM Convert the VRT to a final GeoTIFF with DEFLATE compression and tiling.
gdal_translate -of GTiff -co COMPRESS=DEFLATE -co TILED=YES "%outputVRT%" "%outputTIF%"
if errorlevel 1 (
    echo Error: gdal_translate failed.
    popd
    goto end
)

echo Combined mosaic created successfully: %outputTIF%

popd

:end
pause
endlocal
