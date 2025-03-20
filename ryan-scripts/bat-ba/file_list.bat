@echo off
setlocal

REM Set the folder to list files from.
set "folder=D:\temp\Worsley\Part2\Part2\TIFs"

REM Set the output CSV file path.
set "output=%folder%\file_list.csv"

REM Write header to the CSV file.
echo File Name,Format > "%output%"

REM Loop through files in the folder.
for %%f in ("%folder%\*") do (
    if exist "%%f" (
        REM Get just the file name with extension.
        for %%A in ("%%~nxf") do (
            REM Extract the extension.
            set "filename=%%~nA"
            set "extension=%%~xA"
            echo %%~nxf,%%~xA >> "%output%"
        )
    )
)

echo CSV file created at: %output%
pause
