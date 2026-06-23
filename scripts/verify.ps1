# SDKWork rpc-framework verification entrypoint
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Running cargo test --workspace..."
cargo test --workspace
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if (Get-Command node -ErrorAction SilentlyContinue) {
    $repoRoot = Split-Path $PSScriptRoot -Parent
    $checkScript = Join-Path (Split-Path $repoRoot -Parent) "sdkwork-specs\tools\check-rpc-framework-standard.mjs"
    if (Test-Path $checkScript) {
        Write-Host "Running RPC framework standard check..."
        node $checkScript
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
}

Write-Host "Running clippy..."
cargo clippy --workspace -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

exit 0
