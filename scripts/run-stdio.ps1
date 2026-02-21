$ErrorActionPreference = "Stop"

$repo = Split-Path $PSScriptRoot -Parent
Set-Location $repo

if (!(Test-Path ".env")) {
  Copy-Item .env.example .env
}

Get-Content .env | ForEach-Object {
  if ($_ -match "^\s*#") { return }
  if ($_ -match "^\s*$") { return }
  $parts = $_ -split "=", 2
  if ($parts.Length -eq 2) {
    [Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
  }
}

$env:MAILBOX_REQUIRE_MTLS = "false"

if (-not $env:MAILBOX_TOKEN) {
  $env:MAILBOX_TOKEN = [Environment]::GetEnvironmentVariable("MAILBOX_TOKEN", "User")
}

if (-not $env:MAILBOX_TOKEN) {
  $secretFile = if ($env:MAILBOX_BOOTSTRAP_TOKENS_FILE) { $env:MAILBOX_BOOTSTRAP_TOKENS_FILE } else { "tokens.local.secrets.json" }
  if (Test-Path $secretFile) {
    $secretDoc = Get-Content $secretFile | ConvertFrom-Json
    if ($secretDoc.tokens -and $secretDoc.tokens.Count -gt 0) {
      $env:MAILBOX_TOKEN = $secretDoc.tokens[0].token
    }
  }
}

if (-not $env:MAILBOX_TOKEN) {
  throw "MAILBOX_TOKEN is required for stdio mode. Set it directly or run HTTP mode once to bootstrap tokens."
}

& "$repo\scripts\start-postgres.ps1" *> $null

$env:PATH = "$repo\.tools\go\bin;$env:PATH"

& go run ./cmd/agent-mailbox-stdio
exit $LASTEXITCODE
