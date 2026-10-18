# Register Python-based QGIS MCP and ryan-tools MCP with Codex on Windows.
# Optional -Install updates the external servers. QGIS plugin installation is separate.
[CmdletBinding()]
param(
    [string]$Python = '',
    [string]$QgisSource = '',
    [ValidateSet('core', 'analysis', 'create', 'privileged')]
    [string]$Profile = 'create',
    [switch]$Install,
    [switch]$DryRun,
    [switch]$SkipImportChecks
)

$ErrorActionPreference = 'Stop'
$scriptFile = Join-Path $PSScriptRoot 'setup_codex_mcp.py'
$repoRoot = (Get-Item -LiteralPath (Join-Path $PSScriptRoot '..')).FullName

if (-not $Python) {
    $launcher = Get-Command 'py' -ErrorAction SilentlyContinue
    if ($launcher) {
        $Python = (& $launcher.Source -3 -c 'import sys; print(sys.executable)' 2>$null)
        if ($LASTEXITCODE -ne 0) { $Python = '' }
    }
    if (-not $Python) {
        $fallback = Get-Command 'python' -ErrorAction SilentlyContinue
        if ($fallback) {
            $Python = (& $fallback.Source -c 'import sys; print(sys.executable)' 2>$null)
            if ($LASTEXITCODE -ne 0) { $Python = '' }
        }
    }
}
if (-not $Python -or -not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Python interpreter not found. Install Python 3.13 or newer, or pass -Python."
}
& $Python -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 13) else 1)' *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Python 3.13 or newer is required for setup. Interpreter: $Python"
}

$argsList = @(
    '-B', $scriptFile,
    '--python', $Python,
    '--ryan-root', $repoRoot,
    '--profile', $Profile
)
if ($QgisSource) { $argsList += @('--qgis-source', $QgisSource) }
if ($Install) { $argsList += '--install' }
if ($DryRun) { $argsList += '--dry-run' }
if ($SkipImportChecks) { $argsList += '--skip-import-checks' }

& $Python @argsList
exit $LASTEXITCODE
