@echo off
setlocal

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

REM ================================
REM Set Input/Output Paths
REM ================================
REM Folder containing the individual XYZ files
set "inputFolder=C:\Temp\westgold\higginsville\DTM_1.0m"

REM Output mosaic filenames (adjust these paths as needed)
set "outputVRT=C:\Temp\westgold\higginsville\DTM_1.0m\Higginsville_DTM_1m_EPSG7851.vrt"
set "outputTIF=C:\Temp\westgold\higginsville\DTM_1.0m\Higginsville_DTM_1m_EPSG7851.tif"

REM ================================
REM Change to Input Folder
REM ================================
pushd "%inputFolder%"

echo Building VRT from XYZ files in %inputFolder%...
REM Create a VRT mosaic of all .xyz files in the folder and assign GDA2020 MGA Zone 51.
gdalbuildvrt -a_srs EPSG:7851 -vrtnodata -9999 "%outputVRT%" *.xyz
if errorlevel 1 (
    echo Error: gdalbuildvrt failed.
    popd
    goto end
)

echo Converting VRT to GeoTIFF...
REM Convert the VRT to a final GeoTIFF with DEFLATE compression and tiling.
gdal_translate -of GTiff -a_nodata -9999 -co COMPRESS=DEFLATE -co TILED=YES -co BIGTIFF=IF_SAFER -co SPARSE_OK=YES -co NUM_THREADS=ALL_CPUS "%outputVRT%" "%outputTIF%"
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
