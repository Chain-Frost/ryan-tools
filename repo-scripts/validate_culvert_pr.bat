@echo off
setlocal EnableExtensions

REM Validate PR #86 from a normal Windows checkout.
REM Run with --skip-build to skip the final wheel rebuild/verification step.

cd /d "%~dp0.."
if errorlevel 1 exit /b 1

set "SKIP_BUILD=0"
if /I "%~1"=="--skip-build" set "SKIP_BUILD=1"
if not "%~1"=="" if /I not "%~1"=="--skip-build" goto :usage

set "PYTHONPATH=%CD%;%CD%\vendor\run_hy8\src;%CD%\vendor\ryan_culverts\src;%PYTHONPATH%"

echo ============================================================
echo Culvert PR validation
echo Repository: %CD%
echo ============================================================

set "STEP=Python interpreter"
echo.
echo [CHECK] %STEP%
python --version
if errorlevel 1 goto :failed

set "STEP=Ruff version"
echo.
echo [CHECK] %STEP%
python -m ruff --version
if errorlevel 1 goto :missing_tools

set "STEP=Pyright version"
echo.
echo [CHECK] %STEP%
python -m pyright --version
if errorlevel 1 goto :missing_tools

set "STEP=Pytest version"
echo.
echo [CHECK] %STEP%
python -m pytest --version
if errorlevel 1 goto :missing_tools

if "%SKIP_BUILD%"=="0" (
    set "STEP=Build frontend version"
    echo.
    echo [CHECK] Build frontend version
    python -m build --version
    if errorlevel 1 goto :missing_tools
)

set "STEP=Ruff format check"
echo.
echo [CHECK] %STEP%
python -m ruff format --check ^
    ryan_library\classes\culvert ^
    ryan_library\functions\culvert ^
    ryan_library\orchestrators\culvert ^
    ryan-scripts\culvert.py ^
    tests\culvert ^
    tests\mcp\test_registry.py
if errorlevel 1 goto :failed

set "STEP=Ruff lint check"
echo.
echo [CHECK] %STEP%
python -m ruff check ^
    ryan_library\classes\culvert ^
    ryan_library\functions\culvert ^
    ryan_library\orchestrators\culvert ^
    ryan-scripts\culvert.py ^
    tests\culvert ^
    tests\mcp\test_registry.py
if errorlevel 1 goto :failed

set "STEP=Strict Pyright"
echo.
echo [CHECK] %STEP%
python -m pyright ^
    ryan_library\classes\culvert ^
    ryan_library\functions\culvert ^
    ryan_library\orchestrators\culvert ^
    ryan-scripts\culvert.py
if errorlevel 1 goto :failed

set "STEP=Culvert and MCP tests"
echo.
echo [CHECK] %STEP%
python -m pytest tests\culvert tests\mcp\test_registry.py -q
if errorlevel 1 goto :failed

set "STEP=Wrapper compilation"
echo.
echo [CHECK] %STEP%
python -m py_compile ryan-scripts\culvert.py
if errorlevel 1 goto :failed

set "STEP=Wrapper help smoke test"
echo.
echo [CHECK] %STEP%
python ryan-scripts\culvert.py --help >nul
if errorlevel 1 goto :failed

set "STEP=Documentation links and index"
echo.
echo [CHECK] %STEP%
python repo-scripts\check_documentation.py
if errorlevel 1 goto :failed

set "STEP=Loguru formatting policy"
echo.
echo [CHECK] %STEP%
python repo-scripts\check_loguru_formatting.py
if errorlevel 1 goto :failed

set "STEP=PR whitespace check"
echo.
echo [CHECK] %STEP%
git rev-parse --verify origin/main >nul 2>&1
if errorlevel 1 (
    echo ERROR: origin/main is not available locally.
    echo Run: git fetch origin main
    goto :failed
)
git diff --check origin/main...HEAD
if errorlevel 1 goto :failed

git diff --check
if errorlevel 1 goto :failed

if "%SKIP_BUILD%"=="0" (
    set "STEP=Package build and wheel verification"
    echo.
    echo [CHECK] Package build and wheel verification
    echo NOTE: this rebuild may replace the tracked wheel in dist with a freshly verified wheel of the same version.
    python repo-scripts\build_library.py --no-bump --skip-pip
    if errorlevel 1 goto :failed
) else (
    echo.
    echo [SKIP] Package build and wheel verification
)

echo.
echo ============================================================
echo ALL REQUESTED VALIDATION CHECKS PASSED
echo ============================================================
echo.
echo Working tree status after validation:
git status --short
exit /b 0

:missing_tools
echo.
echo ERROR: Validation tool required for "%STEP%" is unavailable.
echo Install the repository development dependencies, for example:
echo     python -m pip install -e ".[dev]"
echo Then run this batch file again.
exit /b 2

:failed
echo.
echo ============================================================
echo VALIDATION FAILED: %STEP%
echo ============================================================
echo Fix the reported issue and rerun this script.
exit /b 1

:usage
echo Usage: %~nx0 [--skip-build]
echo.
echo   no argument   Run the full PR #86 validation matrix, including wheel rebuild/verification.
echo   --skip-build  Run source, tests, docs and whitespace checks without rebuilding the wheel.
exit /b 2
