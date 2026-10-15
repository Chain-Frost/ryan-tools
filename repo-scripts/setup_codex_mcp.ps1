# Register Python-based QGIS MCP and ryan-tools MCP with Codex on Windows.
# Optional -Install updates the external servers. QGIS plugin installation is separate.
[CmdletBinding()]
param(
    [string]$Python = 'C:\Program Files\Python314\python.exe',
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

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Python 3.14 interpreter not found: $Python. Pass -Python to override."
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
