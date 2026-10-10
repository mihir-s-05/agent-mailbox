# Agent Mailbox website and explorer

The main page helps visitors install the GitHub plugin for Codex or Claude Code, connect an existing mailbox service, and send a first message. It includes clipboard copying, safe local onboarding instructions, explicit Claude Channel opt-in, and optional platform-specific service commands in a collapsed section. A compact animated architecture diagram illustrates send → commit → arrival hint → fetch → acknowledgement. Visitors can pause, replay, choose a step, and compare MCP resource subscriptions, signed MCP Events webhooks, and ordinary polling. It is a local illustration with no API calls, credentials, or real messages. Automatic playback stops off screen and in hidden tabs; reduced-motion settings start it paused and suppress moving packets. The example requests explicit acknowledgement, and distinguishes receipt from completed work. The optional mailbox explorer lives at `/mailbox.html`. Both are dependency-free and embedded in the Go service. The visual reference is the ICL Self-play viewer: white page, serif headings, thin rules, compact controls, and a short footer.

## Local preview

From the repository root:

```powershell
go run ./cmd/viewer-preview
# Open http://127.0.0.1:4174/
```

This starts the installation website and optional synthetic explorer on a loopback server. It needs no PostgreSQL, frontend dependency install, or Docker to preview. It serves only embedded public assets, not the repository. Live explorer mode is disabled until a backend is attached. Port 4173 is left for the reference viewer.

The real Go service serves the same installation page at its root and explorer at `/mailbox.html`. To use the preview as a same-origin proxy to an existing backend:

```powershell
go run ./cmd/viewer-preview -backend http://127.0.0.1:8443
```

The backend must already be running. HTTP upstreams must be loopback; remote upstreams require HTTPS. There is no certificate verification bypass. This preview proxy uses normal system trust and does not provide a client certificate. If the backend requires mTLS, open the service directly with your configured browser certificate, or use the documented loopback HTTP backend behind a trusted TLS proxy.

Test a project-path mount with `-base-path /agent-mailbox/`. All asset, guide, stream, and API URLs resolve relative to that mount. The preview forwards only `/api/` and `/mcp` to its fixed backend; the browser cannot select arbitrary upstreams. Cross-origin unsafe requests are rejected.

```powershell
go run ./cmd/viewer-preview -export viewer/dist
```

This exports an explicit allowlist of installation/explorer assets and shared tool schemas. Live mode is disabled in the static export. Export refuses an output directory containing unexpected files or symlinks. Do not serve the checkout or a directory with local secrets. `dist/` is ignored.

## Installation guidance

Version 0.1 installation commands pin the published `codex/github-plugins` review branch. The root page installs the GitHub marketplace plugin; [PLUGINS.md](PLUGINS.md) documents local testing, configuration, and Claude Channels. A service can be self-hosted from source or supplied by a team. No release binaries, hosted shared service, or official-directory approval are claimed.

The local HTTP instructions explicitly disable TLS/mTLS only for the default loopback listener and require a configured PostgreSQL database. They invoke the actual `cmd/agent-mailbox` entry point. The page never asks for a database password or bearer token: commands contain placeholders, and credentials are set locally. It does not execute shell commands or change client configuration.

Plugin CLI commands and package formats were checked against the installed clients and official documentation. Isolated client homes verify marketplace installation without changing the user's settings. Claude's package declares prompted configuration with a sensitive token; Codex's terminal setup masks the token and protects its local file. Browser QA checks copyable installation/onboarding/channel instructions and optional service setup. It never runs shell commands on a visitor's computer or asks for credentials on the page. Native Claude Channel acceptance and model handling require the visitor's explicit interactive host opt-in.

## Demo and live data

| View | Demo | Connected mailbox |
| --- | --- | --- |
| Agents | Five fictional agents and their presence/status | Same-team directory; without `list:agents`, only owned agents |
| Inbox | Synthetic outstanding deliveries | Owner-scoped read of up to 20 outstanding messages |
| Observed | Fictional history including terminal states | In-memory cache of messages seen or sent by this browser, not a server archive |
| Conversation | Explicit `in_reply_to` links | Only known messages linked by reply ID; unavailable parents are labeled |
| Receive / ack / reply | Changes local fixture state only | Requires explicit activation of an owned agent session and appropriate token scopes |
| Delivery | Clearly labeled synthetic stream/subscription example | Authenticated browser stream status/revision, recent lifecycle metadata, subscription activity/expiry/counts/cursor |

Filtering searches only loaded data. Agent selection clears the live message cache; disconnect clears the cache and credentials. Topic equality does not group conversations. Broadcast receipts remain per recipient. Absence from the limited preview never implies acknowledgement, cancellation, or expiry without a confirming lifecycle event.

Connecting is read-only and never registers an agent automatically. First register the agent using MCP. The viewer derives team/subject from the credential; there is no arbitrary team selector or owner override. `list:agents` permits same-team directory metadata, not another owner's message access. Observer reads leave agent presence and its active session unchanged.

“Activate message actions” creates a viewer session. Replacing an existing connection requires a separate, unchecked-by-default checkbox and invalidates its old session. Return to observer explicitly deregisters the viewer's session. Disconnect attempts to release an activated session, then clears the token; a network failure can leave that server session active and require explicit replacement on reconnect. Action mode binds one mailbox; return to observer before choosing another agent.

The token lives only in a private client closure. It is absent from URLs, local/session storage, demo settings, rendered diagnostics, and tool arguments. Inputs are cleared after submission. Message bodies and attachments render as text. Use only a trusted HTTPS origin or loopback preview; the server still owns authorization and scope checks. Subscriptions expose no callback URLs or signing secrets.

## Receipt semantics

Pending means accepted but not fetched. Delivered means fetched by the recipient. Peeking or viewing does not mark delivery. Receive inbox invokes `poll_inbox` for up to 100 messages. Explicit acknowledgements require a delivered message. Messages without `require_ack` are automatically acknowledged when fetched.

Acknowledgement records receipt according to the mailbox protocol; it does not prove work completion. A reply can report a work result. Callback HTTP 2xx records arrival-event acceptance independently of inbox delivery and acknowledgement. Arrival events use at-least-once delivery; consumers deduplicate by event ID. The viewer reads subscription diagnostics; setup, refresh, and replay remain in the MCP events API.

## WebMCP

Ordinary controls always work. The viewer feature-detects the October 2026 draft's `document.modelContext.registerTool`; it does not polyfill or replace the browser API. Observer mode registers the shared `peek_inbox` and `list_agents` schemas against the stable read endpoints. Activated mode preserves the existing shared mailbox tool set. Tools bind the selected page identity, reject hidden session/team overrides, respect cancellation, and are removed when selection changes or disconnects.

The UI reports whether registration is available. Mock contract tests cover registration/execution/cancellation. Installed Chrome did not expose the native API during QA; native WebMCP execution by a browser agent or host is not verified.

## Concrete hosting arrangement

Use an existing or separately chosen Linux host for one native Go service plus PostgreSQL 16 or a compatible managed PostgreSQL database. Serve the embedded frontend and `/api/` from the same Go origin. Terminate public HTTPS at the site's reverse proxy, forwarding only the chosen project path to the loopback Go listener. No container is required. Do not expose the PostgreSQL port or the loopback application directly to the internet.

1. Build `go build -o agent-mailbox ./cmd/agent-mailbox` for the target Linux architecture. Install the binary and `migrations/` in `/opt/agent-mailbox`; startup migrations are relative to this working directory. The embedded viewer needs no separate frontend build.
2. Create a dedicated `agent-mailbox` service account and database role/database. For a database on the same machine, use a loopback DSN as in `deploy/mailbox.env.example`. For a remote managed database, use its trusted CA and `sslmode=verify-full`.
3. Install a reviewed token registry in `/etc/agent-mailbox/tokens.json`, with hashed tokens, explicit subjects/team/scopes, expiry/revocation, and file permissions allowing only the service account to read it. Disable auto generation as in the environment template. Create `/var/lib/agent-mailbox` owned by the service account; the persistent event key must survive restarts and be included in protected backups. Do not expose an administration endpoint or give public demo visitors live credentials.
4. Install `deploy/agent-mailbox.service`, the restricted environment file, and start the service. Check the loopback `/readyz`. The service template makes `/opt` read-only and permits writes only to `/var/lib/agent-mailbox`.
5. The portfolio's owning session can import `deploy/mailbox.caddy` into an existing HTTPS site block after choosing the public path. The example mounts `/agent-mailbox/` by stripping that prefix before forwarding to Go, preserving same-origin browser requests and stream flushing. This is a Caddy snippet, not a full replacement for the site's routing. [Caddy documents prefix stripping](https://caddyserver.com/docs/caddyfile/directives/handle_path) and [reverse-proxy streaming](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#streaming).
6. Maintain PostgreSQL and event-key backups, test restore, and monitor readiness and expired subscriptions. See [PostgreSQL backup guidance](https://www.postgresql.org/docs/16/backup.html). Deploy one service process initially; PostgreSQL remains the durable store.

If mihir-s.com's current frontend is hosted elsewhere, its owner must add a same-origin route/proxy to this chosen backend rather than upload the Go application as static assets. The public route/origin, host availability, TLS certificates, and operational credentials remain deployment decisions. No public subdomain, paid resource, or portfolio change has been made.

## Validation

```powershell
go test ./...
node --test internal/web/assets/*.test.mjs
$env:MAILBOX_INTEGRATION_TESTS = '1'
$env:MAILBOX_BROWSER_TESTS = '1'
$env:PLAYWRIGHT_MODULE = 'PATH_TO_EXISTING_PLAYWRIGHT_MODULE'
go test -timeout 3m -p 1 ./...
```

Integration tests use an isolated real PostgreSQL database; set `MAILBOX_TEST_DATABASE_URL` to reuse an available test database. The existing embedded PostgreSQL test harness can use its cached binaries. It never invokes Docker. Browser tests use the existing Playwright setup and installed Chrome, exercising the architecture animation and controls, all three update paths, off-screen pause, keyboard selection, reduced motion, absence of illustration API calls, installation platform/client selection, valid JSON configuration, clipboard copy, documentation navigation, the secondary explorer, authenticated observation without takeover, explicit replacement, sending, streamed arrival, receive/explicit or automatic ack, cancelled connection, disconnect, and desktop/phone layout. Routing tests cover a project prefix, bearer forwarding, stream flushing, cross-origin rejection, and the static-export allowlist.

The Caddy/systemd templates are prepared for review against their documented interfaces; this Windows QA does not run a Linux service or a production TLS proxy. No deployment has been performed.
