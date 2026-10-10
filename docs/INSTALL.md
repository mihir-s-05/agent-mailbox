# Install and run

## Requirements

- Go 1.25 or newer.
- PostgreSQL 16 or newer, installed locally or hosted.
- PowerShell 7 for the Windows scripts. Go commands work on other platforms.

Docker is optional. `scripts/start-postgres.ps1` is an explicit convenience for existing Docker users. The server and STDIO launcher do not download or start Docker.

## Configure PostgreSQL

Using PostgreSQL tools and a role permitted to create databases:

```powershell
createdb agent_mailbox
Copy-Item .env.example .env
```

Set `MAILBOX_DATABASE_URL` to your database connection string. A local example is `postgres://postgres:<password>@127.0.0.1:5432/agent_mailbox?sslmode=disable`; use your database provider's TLS settings for hosted databases.

Run from the repository root:

```powershell
.\scripts\run-dev.ps1
```

The script loads `.env`, creates missing local certificates in default mTLS mode, migrates, and starts HTTP. Startup also applies migrations with a database advisory lock, so concurrent processes cannot apply the same migration simultaneously. Back up an existing database before upgrade.

On macOS/Linux, export variables from `.env` and run `go run ./cmd/agent-mailbox`. Supply certificates for TLS or run `pwsh ./scripts/gen-certs.ps1` with PowerShell/OpenSSL installed.

## Transport choices

| Mode | Configuration | Endpoint |
|---|---|---|
| Local HTTP | mTLS false, TLS false | `http://127.0.0.1:8443/mcp` |
| HTTPS bearer auth | mTLS false, TLS true, certificate/key | `https://your-host/mcp` |
| Default mTLS | both true, certificate/key and client CA | HTTPS with client certificate |
| STDIO | token and database URL; launcher disables HTTP TLS flags | stdin/stdout |

The flags are `MAILBOX_REQUIRE_MTLS` and `MAILBOX_TLS_ENABLED`. The local listener defaults to `127.0.0.1:8443`. Remote access needs an explicit listener and reachable TLS endpoint with appropriate credentials. Browsers need trusted HTTPS certificates; mTLS also needs a client certificate configured in the browser. Plugin hosts must support the selected authentication setup.

`MAILBOX_BASE_PATH` defaults to `/mcp` and must not overlap browser/API/health routes. `/healthz` checks the HTTP process; `/readyz` checks PostgreSQL and event materialization health.

## Ownership and credentials

Startup creates missing token metadata/secrets files. Tokens have hashes, explicit scopes, required expiry and optional revocation. The registry reloads every 15 seconds by default. Invalid reloads preserve the last valid registry.

Keep `team_id` and `subject` stable when rotating credentials. `id` names a credential; `subject` owns mailboxes. Use explicit token records rather than changing positional IDs in legacy inline configuration.

Migration 003 assigns agent ownership from the active session, falling back to latest inactive history. It cannot prove who historically should have owned an already compromised agent. Review owners during upgrade. Missing or invalid history fails closed. A caller with `agents:takeover` and explicit replacement can recover an unowned agent; transfers invalidate old sessions.

`MAILBOX_EVENT_KEY_FILE` defaults to a 32-byte `events.key.local` file created on startup. It encrypts webhook secrets and authenticates replay cursors. Back it up with the database, preserve it across restarts and share it across replicas. Replacing it invalidates existing secrets/cursors. Restrict Windows ACLs on secret files in addition to application file modes.

For legacy plaintext token records, temporarily enable `MAILBOX_ALLOW_PLAINTEXT_TOKENS=true`, replace raw tokens with their SHA-256 hashes in metadata, then disable the flag. The application does not rewrite legacy files automatically.

## Clients and verification

Configure an MCP client with the selected URL and `Authorization: Bearer <token>`. mTLS clients also need CA/client certificate settings. STDIO is available when a client cannot supply certificates:

```powershell
$env:MAILBOX_TOKEN = '<token>'
.\scripts\run-stdio.ps1
```

PostgreSQL must already be available. Protocol responses use stdout; logs use stderr. With HTTP running, `scripts/smoke.ps1` exercises discovery, registration, send, poll and ack without modifying client configurations.

```powershell
go test ./...
go vet ./...
node --test internal/web/assets/webmcp.test.mjs
$env:MAILBOX_INTEGRATION_TESTS = '1'
go test -p 1 ./...
```

Integration tests download isolated PostgreSQL into the OS cache and remove temporary runtimes/databases afterward. Alternatively set `MAILBOX_TEST_DATABASE_URL`; tests create/drop only their own schemas. Docker is not involved.

## Troubleshooting

- `FORBIDDEN`: wrong owner or missing scope.
- `SESSION_INVALID`: replaced/deregistered session; register using the mailbox owner's credential.
- `TEAM_MISMATCH`: token and session/request teams differ.
- `IDEMPOTENCY_CONFLICT`: retry the original payload or use a new key for new content.
- `MAILBOX_FULL`: receive/acknowledge outstanding messages or wait for expiry.
- Stopped webhooks: inspect authenticated `/api/subscriptions?agent_id=...`, expiry, callback responses and credential status. A refresh can retry failed delivery.
- No native WebMCP: manual controls remain available; browser API support varies.

See [push/browser contracts](PUSH_AND_WEBMCP.md) and [messaging design](MESSAGING_DESIGN.md).
