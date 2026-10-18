@echo off
REM Navigate to the root directory of the repository
cd /d %~dp0..

echo Run pytest with coverage
REM run-hy8 and ryan-culverts are installed from PyPI, not vendor submodules.
set "PYTHONPATH=%CD%;%PYTHONPATH%"
python -m pytest -o "cache_dir=.pytest_cache" --basetemp=".pytest_cache\basetemp" --cov --cov-report=term-missing --cov-report=html --cov-report=xml tests/
