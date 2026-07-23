# Loads .env into the current PowerShell session's environment.
#
# Nothing in the project reads .env on its own, so any command that needs
# configuration — the server, the worker, keygen, migrate — must have it loaded
# first. Dot-source this: . .\scripts\env.ps1
#
# Values are taken verbatim after the first '=', so a password containing '='
# or '#' survives intact. Keys are trimmed, which tolerates "KEY = value".

$envFile = Join-Path (Split-Path -Parent $PSScriptRoot) '.env'

if (-not (Test-Path $envFile)) {
    Write-Error ".env not found at $envFile. Copy .env.example to .env first."
    return
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq '' -or $line.StartsWith('#')) { return }
    $split = $line.IndexOf('=')
    if ($split -lt 1) { return }
    $key = $line.Substring(0, $split).Trim()
    $value = $line.Substring($split + 1)
    Set-Item -Path "env:$key" -Value $value
}
