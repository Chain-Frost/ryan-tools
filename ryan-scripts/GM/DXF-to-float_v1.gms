GLOBAL_MAPPER_SCRIPT VERSION="1.00"

// Start looping through all DXF files in the specified directory
DIR_LOOP_START DIRECTORY="C:\path\241108" FILENAME_MASKS=*dxf

    // Import the current DXF file
    IMPORT FILENAME="%FNAME_W_DIR%" TYPE=DXF

    // Generate an elevation grid with 0.5m resolution using the TIN algorithm
    GENERATE_ELEV_GRID FILENAME="%FNAME_W_DIR%" ELEV_UNITS=METERS GRID_ALG=TIN SPATIAL_RES_METERS=0.5,0.5

    // Export the generated grid as a singleband GeoTIFF file with the same base name
    EXPORT_ELEVATION FILENAME="C:\path\241108\%FNAME_WO_EXT%.flt" TYPE=FLOATGRID SPATIAL_RES=0.5,0.5 FORCE_SQUARE_PIXELS=YES FILL_GAPS=YES ELEV_UNITS=METERS

    // Unload all loaded data to prepare for the next iteration
    UNLOAD_ALL

DIR_LOOP_END
