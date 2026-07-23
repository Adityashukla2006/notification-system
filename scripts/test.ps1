# Runs the Go test suite with .env loaded into the process environment.
#
# This exists because the store and queue tests SKIP when TEST_DATABASE_URL and
# TEST_REDIS_ADDR are unset, and nothing in the project reads .env on its own.
# A bare `go test ./...` therefore reports a confident green while roughly
# twenty tests against real Postgres and Redis never execute at all.
#
# Usage:  .\scripts\test.ps1            (whole suite)
#         .\scripts\test.ps1 ./internal/store/   (one package)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$envFile = Join-Path $root '.env'

if (-not (Test-Path $envFile)) {
    Write-Error ".env not found at $envFile. Copy .env.example to .env first."
}

# Parse KEY=VALUE lines, ignoring comments and blanks. Values are taken
# verbatim: no quote stripping, so a password containing '#' or '=' survives.
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq '' -or $line.StartsWith('#')) { return }
    $split = $line.IndexOf('=')
    if ($split -lt 1) { return }
    $key = $line.Substring(0, $split).Trim()
    $value = $line.Substring($split + 1)
    Set-Item -Path "env:$key" -Value $value
}

if (-not $env:TEST_DATABASE_URL) {
    Write-Warning 'TEST_DATABASE_URL is unset - store tests will SKIP, not run.'
}
if (-not $env:TEST_REDIS_ADDR) {
    Write-Warning 'TEST_REDIS_ADDR is unset - queue tests will SKIP, not run.'
}

# [string[]] matters: a single-element array assigned bare unwraps to a plain
# string, which then expands character by character into separate arguments.
[string[]]$packages = if ($args.Count -gt 0) { $args } else { './...' }

Push-Location (Join-Path $root 'api')
try {
    & go test -count=1 $packages
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
