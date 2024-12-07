@echo off
REM Navigate to the root directory of the repository
cd /d %~dp0..

REM Add the repository root to PYTHONPATH
set PYTHONPATH=%cd%

where pytest

REM Activate the virtual environment if you have one
REM Uncomment the following line if using a virtual environment
REM call venv\Scripts\activate.bat

REM Run pytest for a specific test file
@REM pytest -v  tests/classes/test_tuflow_string_classes.py
@REM pytest -v  tests/functions/test_file_utils.py
pytest -v  tests/functions/test_loguru_helpers.py

REM Deactivate the virtual environment if activated
REM deactivate

echo Tests completed.
pause
