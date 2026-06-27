# SDKWork rpc-framework verification entrypoint
# Mirrors .github/workflows/verify.yml for local Windows iteration.
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

Set-Location (Split-Path $PSScriptRoot -Parent)

Write-Host "Running cargo fmt --check..."
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Running cargo test --workspace..."
cargo test --workspace
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Running cargo clippy..."
cargo clippy --workspace -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$repoRoot = Split-Path $PSScriptRoot -Parent
$specsRoot = Split-Path $repoRoot -Parent
$checkScript = Join-Path $specsRoot "sdkwork-specs\tools\check-rpc-framework-standard.mjs"
if ((Get-Command node -ErrorAction SilentlyContinue) -and (Test-Path $checkScript)) {
    Write-Host "Running RPC framework standard check..."
    node $checkScript
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

if (Get-Command cargo-audit -ErrorAction SilentlyContinue) {
    Write-Host "Running cargo audit..."
    cargo audit --deny warnings
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} else {
    Write-Host "cargo-audit not installed; skipping supply-chain audit. Install with: cargo install cargo-audit"
}

exit 0
