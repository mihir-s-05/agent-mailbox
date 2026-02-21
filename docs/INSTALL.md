# Install and Run

## Prerequisites

- Docker
- Go 1.22+
- PowerShell 7+ (for provided scripts)

## Windows setup

### 1. Start Postgres

```powershell
.\scripts\start-postgres.ps1
```

### 2. Create local env file

```powershell
Copy-Item .env.example .env
```

### 3. Run server (mTLS on by default)

```powershell
.\scripts\run-dev.ps1
```

`run-dev.ps1` will:
- load `.env`
- auto-generate dev certs if required TLS files are missing
- run migrations
- start the HTTP MCP server

On first startup, token bootstrap runs automatically if `MAILBOX_TOKENS_JSON_FILE` is missing:
- `tokens.local.json`: hashed token records (server input)
- `tokens.local.secrets.json`: plaintext bearer tokens for client setup

### 4. Run smoke test

With server running in another shell:

```powershell
.\scripts\smoke.ps1
```

## macOS/Linux setup

### 1. Start Postgres

```bash
CONTAINER=agent-mailbox-postgres
if ! docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker run -d --name "$CONTAINER" \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=agent_mailbox \
    -p 5432:5432 \
    postgres:16
else
  docker start "$CONTAINER"
fi
```

### 2. Create env and certs

```bash
cp .env.example .env
./scripts/gen-certs.ps1
```

### 3. Run server

```bash
set -a
source .env
set +a
go run ./cmd/agent-mailbox
```

## MCP setup

Use a token from `tokens.local.secrets.json`.

### Claude Code

```bash
claude mcp add --transport http agent_mailbox https://127.0.0.1:8443/mcp --header "Authorization: Bearer <token>"
```

### Codex

```bash
export MAILBOX_TOKEN=<token>
codex mcp add agent_mailbox --url https://127.0.0.1:8443/mcp --bearer-token-env-var MAILBOX_TOKEN
```

If your MCP client needs explicit client cert wiring for mTLS, configure it with:
- CA: `certs/ca.crt`
- Client cert: `certs/client.crt`
- Client key: `certs/client.key`
- Windows Schannel clients can use `certs/client.pfx` (no password).

## Token lifecycle

- `expires_at` is required for every token.
- `revoked: true` invalidates a token.
- token file reload is automatic (`MAILBOX_TOKEN_RELOAD_INTERVAL`), so changes apply without server restart.
- Empty scopes are rejected; scopes must be explicit.

### Legacy plaintext token migration

If you still have legacy token files using `"token"` instead of `"token_hash"`:

1. Temporarily set `MAILBOX_ALLOW_PLAINTEXT_TOKENS=true`.
2. Start the server and migrate to hashed entries.
3. Set `MAILBOX_ALLOW_PLAINTEXT_TOKENS=false`.

## Troubleshooting

- `AUTH_INVALID` with message `expired bearer token`: token TTL elapsed; rotate token.
- `AUTH_INVALID` with message `revoked bearer token`: token marked revoked.
- `TEAM_MISMATCH`: token team and request/session team differ.
- `SESSION_INVALID`: register again and use a fresh `session_id`.
