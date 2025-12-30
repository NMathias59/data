param(
    [string]$Command = "dbt debug --profiles-dir ."
)

$envFile = Join-Path $PSScriptRoot "..\.env"
if (-Not (Test-Path $envFile)) {
    Write-Error ".env not found at $envFile. Copy .env.example -> .env and set your values first."
    exit 1
}

# Load .env into environment for this session
Get-Content $envFile | ForEach-Object {
    if ($_ -and $_ -notmatch '^\s*#') {
        $pair = $_ -split '=',2
        if ($pair.Length -eq 2) {
            $name = $pair[0].Trim()
            $value = $pair[1].Trim()
            Write-Host "Setting env: $name"
            Set-Item -Path Env:$name -Value $value
        }
    }
}

# Run provided command
Write-Host "Running: $Command"
Invoke-Expression $Command
