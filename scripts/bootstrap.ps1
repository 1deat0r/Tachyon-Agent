$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    throw "rustup is required. Install it from https://rustup.rs/"
}

rustup toolchain install 1.98.1 --profile default --component rustfmt --component clippy
cargo verify

Write-Host "Tachyon workspace verified. See docs/DEVELOPMENT_WORKFLOW.md for local development checks."
