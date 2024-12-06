@echo off
REM Navigate to the root directory of the repository
cd /d %~dp0..

REM Activate the virtual environment if you have one
REM Uncomment the following lines if using a virtual environment
REM call venv\Scripts\activate

REM Install pytest-cov if not already installed
python -m pip install --upgrade pip
python -m pip install pytest
python -m pip install pytest-cov

echo Run pytest with coverage
pytest --cov=ryan_library tests/

REM Deactivate the virtual environment if activated
REM deactivate
