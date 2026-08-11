@echo off
REM ============================================================================
REM Rebuilds and shrinks all GeoPackage (.gpkg) databases in the directory.
REM 
REM What this does: When you delete features or layers from a GeoPackage (SQLite),
REM the file size doesn't automatically shrink. VACUUM rebuilds the database,
REM purging the empty space and reducing the file size on disk.
REM ============================================================================

Call "C:\OSGEO4W64\bin\o4w_env.bat"
for %%f in (*.gpkg) do (	
ogrinfo %%f -sql "VACUUM"
)
Pause