$ErrorActionPreference = "Stop"

if (!(Test-Path ".env")) {
  throw ".env not found"
}

Get-Content .env | ForEach-Object {
  if ($_ -match "^\s*#") { return }
  if ($_ -match "^\s*$") { return }
  $parts = $_ -split "=", 2
  if ($parts.Length -eq 2) {
    [Environment]::SetEnvironmentVariable($parts[0], $parts[1], "Process")
  }
}

function Get-TokenContext {
  $secretFile = if ($env:MAILBOX_BOOTSTRAP_TOKENS_FILE) { $env:MAILBOX_BOOTSTRAP_TOKENS_FILE } else { "tokens.local.secrets.json" }
  if (Test-Path $secretFile) {
    $secretDoc = Get-Content $secretFile | ConvertFrom-Json
    if ($secretDoc.tokens -and $secretDoc.tokens.Count -gt 0) {
      return @{
        token = $secretDoc.tokens[0].token
        team  = $secretDoc.tokens[0].team_id
      }
    }
  }

  $tokenFile = if ($env:MAILBOX_TOKENS_JSON_FILE) { $env:MAILBOX_TOKENS_JSON_FILE } else { "tokens.local.json" }
  if (!(Test-Path $tokenFile)) {
    throw "No token source found. Start the server once to bootstrap tokens."
  }
  $tokenDoc = Get-Content $tokenFile | ConvertFrom-Json
  if (-not $tokenDoc.tokens -or $tokenDoc.tokens.Count -eq 0 -or -not $tokenDoc.tokens[0].token) {
    throw "Token file does not contain plaintext tokens. Use MAILBOX_BOOTSTRAP_TOKENS_FILE or set MAILBOX_TOKEN manually."
  }
  return @{
    token = $tokenDoc.tokens[0].token
    team  = $tokenDoc.tokens[0].team_id
  }
}

$ctx = Get-TokenContext
$token = $ctx.token
$team = $ctx.team

$addr = if ($env:MAILBOX_ADDR) { $env:MAILBOX_ADDR } else { ":8443" }
$base = if ($env:MAILBOX_BASE_PATH) { $env:MAILBOX_BASE_PATH } else { "/mcp" }
$requireMTLS = ($env:MAILBOX_REQUIRE_MTLS -eq "true")
$proto = if ($requireMTLS) { "https" } else { "http" }
$url = "${proto}://127.0.0.1$addr$base"

function Invoke-Mcp([string]$method, [hashtable]$params) {
  $paramsJSON = $params | ConvertTo-Json -Depth 20 -Compress
  $goArgs = @(
    "run", "./cmd/mcp-call",
    "--url", $url,
    "--token", $token,
    "--method", $method,
    "--params", $paramsJSON
  )
  if ($requireMTLS) {
    $ca = if ($env:MAILBOX_TLS_CLIENT_CA_FILE) { $env:MAILBOX_TLS_CLIENT_CA_FILE } else { "certs/ca.crt" }
    $clientCert = if ($env:MAILBOX_TLS_CLIENT_CERT_FILE) { $env:MAILBOX_TLS_CLIENT_CERT_FILE } else { "certs/client.crt" }
    $clientKey = if ($env:MAILBOX_TLS_CLIENT_KEY_FILE) { $env:MAILBOX_TLS_CLIENT_KEY_FILE } else { "certs/client.key" }
    $goArgs += @("--ca", $ca, "--cert", $clientCert, "--key", $clientKey)
  }

  $resp = & go @goArgs
  if ($LASTEXITCODE -ne 0) {
    throw "MCP call failed for method $method"
  }
  if ([string]::IsNullOrWhiteSpace($resp)) {
    return $null
  }
  return ($resp | ConvertFrom-Json)
}

$init = Invoke-Mcp "initialize" @{
  protocolVersion = "2025-11-25"
  capabilities = @{}
  clientInfo = @{
    name = "smoke"
    version = "1.0.0"
  }
}

Invoke-Mcp "notifications/initialized" @{} | Out-Null

$a = Invoke-Mcp "tools/call" @{
  name = "register_agent"
  arguments = @{
    team_id = $team
    agent_id = "codex:smoke-a"
    display_name = "smoke-a"
    tags = @("implementer")
    capabilities = @("tests")
    replace_existing_session = $true
  }
}

$b = Invoke-Mcp "tools/call" @{
  name = "register_agent"
  arguments = @{
    team_id = $team
    agent_id = "claude:smoke-b"
    display_name = "smoke-b"
    tags = @("reviewer")
    replace_existing_session = $true
  }
}

$sessionA = ($a.result.structuredContent).session_id
$sessionB = ($b.result.structuredContent).session_id

$msg = Invoke-Mcp "tools/call" @{
  name = "send_message"
  arguments = @{
    session_id = $sessionA
    to = @{
      type = "direct"
      agent_id = "claude:smoke-b"
    }
    priority = "HIGH"
    topic = "smoke"
    body = "hello from smoke"
    require_ack = $true
  }
}

$poll = Invoke-Mcp "tools/call" @{
  name = "poll_inbox"
  arguments = @{
    session_id = $sessionB
    max_messages = 10
    wait_ms = 100
  }
}

$firstMessageId = ($poll.result.structuredContent.messages | Select-Object -First 1).message_id
if (-not $firstMessageId) {
  throw "poll_inbox returned no messages"
}

$ack = Invoke-Mcp "tools/call" @{
  name = "ack_messages"
  arguments = @{
    session_id = $sessionB
    message_ids = @($firstMessageId)
    ack_kind = "ACK"
  }
}

Write-Host "MCP protocol smoke passed"
Write-Host "Message id: $firstMessageId"

if (-not $requireMTLS) {
  claude mcp remove agent_mailbox --scope user *> $null
  claude mcp add --transport http agent_mailbox $url --header "Authorization: Bearer $token"
  claude mcp list

  codex mcp remove agent_mailbox *> $null
  codex mcp add agent_mailbox --url $url --bearer-token-env-var MAILBOX_TOKEN
  $env:MAILBOX_TOKEN = $token
  codex mcp list

  Write-Host "Installed MCP server in Claude and Codex configs."
} else {
  Write-Host "mTLS mode enabled; skipping automatic claude/codex mcp add because client cert wiring is environment-specific."
}
