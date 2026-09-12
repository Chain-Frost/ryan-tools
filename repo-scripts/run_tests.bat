@echo off
REM Navigate to the root directory of the repository
cd /d %~dp0..

echo Run pytest with coverage
set "PYTHONPATH=.;vendor\run_hy8\src;vendor\ryan_culverts\src"
python -m pytest -o "cache_dir=.pytest_cache" --basetemp=".pytest_cache\basetemp" --cov --cov-report=term-missing --cov-report=html --cov-report=xml tests/
