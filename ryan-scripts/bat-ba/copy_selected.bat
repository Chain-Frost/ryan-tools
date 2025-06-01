@echo off
REM ---------------------------------------------------------
REM Batch file to copy .tif files listed in Selected.csv
REM from a source directory to a target directory.
REM ---------------------------------------------------------

:: Set the path to your source and destination folders
set "SRC=D:\temp\Worsley\Part2\Part2\TIFs"
set "DST=D:\temp\Worsley\Selected"

:: Path to your CSV file (converted from XLSX)
set "LISTFILE=D:\temp\Worsley\Part2\Part2\TIFs\Selected_part2.csv"

:: Make sure the destination folder exists
if not exist "%DST%" (
    echo Creating folder: %DST%
    mkdir "%DST%"
)

:: Loop through each line in the CSV file
:: Skip=1 is used to skip the header row ("File_name")
for /f "usebackq skip=1 tokens=1 delims=," %%A in ("%LISTFILE%") do (
    echo Copying "%%A" ...
    copy "%SRC%\%%A" "%DST%\"
)

echo.
echo Done copying files!
pause
