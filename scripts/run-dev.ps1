$ErrorActionPreference = "Stop"

$repo = Split-Path $PSScriptRoot -Parent
Set-Location $repo

if (!(Test-Path ".env")) {
  Copy-Item .env.example .env
  Write-Host "Created .env from .env.example"
}

Get-Content .env | ForEach-Object {
  if ($_ -match "^\s*#") { return }
  if ($_ -match "^\s*$") { return }
  $parts = $_ -split "=", 2
  if ($parts.Length -eq 2) {
    [Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
  }
}

if (-not $env:MAILBOX_REQUIRE_MTLS) {
  $env:MAILBOX_REQUIRE_MTLS = "true"
}

$env:PATH = "$repo\.tools\go\bin;$env:PATH"

if ($env:MAILBOX_REQUIRE_MTLS -eq "true") {
  $certFile = if ($env:MAILBOX_TLS_CERT_FILE) { $env:MAILBOX_TLS_CERT_FILE } else { "certs/server.crt" }
  $keyFile = if ($env:MAILBOX_TLS_KEY_FILE) { $env:MAILBOX_TLS_KEY_FILE } else { "certs/server.key" }
  $caFile = if ($env:MAILBOX_TLS_CLIENT_CA_FILE) { $env:MAILBOX_TLS_CLIENT_CA_FILE } else { "certs/ca.crt" }

  if (!(Test-Path $certFile) -or !(Test-Path $keyFile) -or !(Test-Path $caFile)) {
    Write-Host "TLS assets missing, generating local certs..."
    & "$repo\scripts\gen-certs.ps1"
  }
}

& go run ./cmd/migrate
& go run ./cmd/agent-mailbox
