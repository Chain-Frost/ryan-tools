@echo off
REM Navigate to the root directory of the repository
cd /d %~dp0..

echo Run pytest with coverage
set PYTHONPATH=.;vendor\run_hy8\src
python -m pytest -o "cache_dir=%TEMP%\pytest_cache" --cov --cov-report=term-missing --cov-report=html --cov-report=xml tests/
