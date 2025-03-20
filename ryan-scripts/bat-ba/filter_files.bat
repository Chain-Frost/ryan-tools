@echo off
set "input_csv=D:\temp\Worsley\Part1\Part1\TIFs\file_list_part1.csv"
set "output_csv=D:\temp\Worsley\Part1\Part1\TIFs\Selected_part1.csv"
set "ps1file=%temp%\filter_script.ps1"

> "%ps1file%" echo $csv = Import-Csv '%input_csv%'
>> "%ps1file%" echo $selected = $csv ^| Where-Object {
>> "%ps1file%" echo     $fn = $_.'File Name'
>> "%ps1file%" echo     if ($fn -match "^(\d+)_([\d]+)\.tif$") {
>> "%ps1file%" echo         $x = [int]$matches[1]
>> "%ps1file%" echo         $y = [int]$matches[2]
>> "%ps1file%" echo         # Check if the coordinates are inside the rectangle
>> "%ps1file%" echo         $x -ge 437000 -and $x -le 464000 -and $y -ge 6362000 -and $y -le 6446000
>> "%ps1file%" echo     } else { $false }
>> "%ps1file%" echo }
>> "%ps1file%" echo $selected ^| Export-Csv -Path '%output_csv%' -NoTypeInformation

powershell -ExecutionPolicy Bypass -File "%ps1file%"

echo.
echo Processing completed. The filtered CSV is located at: %output_csv%
pause
