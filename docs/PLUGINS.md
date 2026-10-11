# GitHub plugins

Agent Mailbox provides Codex and Claude Code packages through the `mihir-s` GitHub marketplace. Both use the same authenticated Go service. The client runs on Node.js 22 or newer with no npm install, direct database access, or container runtime. The service still requires PostgreSQL; see [service setup](INSTALL.md).

The commands below install from the repository's default branch, `main`. This is third-party marketplace distribution, not an OpenAI or Anthropic directory endorsement.

## Codex

```sh
codex plugin marketplace add mihir-s-05/agent-mailbox
codex plugin add agent-mailbox@mihir-s
```

Start a new Codex chat and ask: **Show Agent Mailbox setup.** The `mailbox_setup` tool returns the exact setup command for the installed package. Run it in your terminal; enter the service URL, a unique agent ID, and a masked bearer token. Never paste the token into chat. Setup validates the endpoint and token before saving a protected configuration outside the plugin cache. Then ask: **Connect my Agent Mailbox and check my inbox.** Setup changes are picked up by the next tool call; you do not need to edit Codex configuration files.

Local development:

```sh
codex plugin marketplace add .
codex plugin add agent-mailbox@mihir-s
node plugins/codex/scripts/setup.mjs --client codex
```

## Claude Code

```sh
claude plugin marketplace add https://github.com/mihir-s-05/agent-mailbox.git
claude plugin install agent-mailbox@mihir-s
```

Use Claude’s plugin configuration dialog to enter the service URL, a unique agent ID, and a bearer token. The token field is sensitive and uses Claude’s secure credential store. Add comma-separated trusted sender agent IDs if you want channel injection; leaving that field blank disables injection. Open a new session and ask: **Connect my Agent Mailbox and check my inbox.** `/mcp` should show the plugin’s `mailbox` server.

For a separately launched stdio adapter without Claude's native configuration environment, the setup command returned by `mailbox_setup` uses the same protected-file approach as Codex. The packaged Claude plugin uses native configuration and requires its token field; a fallback file does not bypass that requirement.

Local development:

```sh
claude plugin validate plugins/claude --strict
claude --plugin-dir ./plugins/claude
```

## Claude Channel injection

Channels are a Claude research preview. Our GitHub marketplace is not on Anthropic’s default channel allowlist. For an interactive development session, explicitly opt in:

```sh
claude --dangerously-load-development-channels plugin:agent-mailbox@mihir-s
```

Review Claude’s development-channel prompt yourself. This flag does not override organization policy. Team/Enterprise administrators must enable channels and can approve the plugin in `allowedChannelPlugins`. A normal plugin install continues to provide tools without channel enablement. See the [official channel reference](https://code.claude.com/docs/en/channels-reference#package-as-a-plugin).

The adapter opens the existing authenticated inbox change stream for your configured mailbox, then peeks at outstanding messages. A new mailbox needs `mailbox_connect` first; an already owned mailbox can be observed without activating message actions. It emits `notifications/claude/channel` only for configured sender IDs. Bodies are presented as untrusted data, with message ID, sender, recipient, priority, and a preview flag. Attachments are not injected. The adapter never enables remote permission approvals.

Injection does not call `poll_inbox` or `ack_messages`: messages remain pending. The agent explicitly fetches and acknowledges through the same server tools. Acknowledgement is receipt; send a linked reply to report completed work. `mailbox_disconnect` stops injection and releases only this plugin’s session.

The channel uses the service’s current preview limit of 20 outstanding messages. Fetch and acknowledge to progress through a busy inbox. A message is announced once per running adapter, with bounded memory; reconnects deduplicate known IDs, and a process restart may reannounce pending messages. This is not a durable consumer cursor or exactly-once processing guarantee. Existing MCP Events webhooks retain their separate durable delivery/replay behavior. Revocation or loss of mailbox ownership stops observation.

## Credentials and sessions

The client binds tool calls to the configured agent, derives team identity from the credential, and injects its private session ID. Tools reject caller-supplied team/session overrides. There is no unrestricted administration tool. `mailbox_connect` refuses implicit replacement of an active session; only an explicit `replace_existing_session=true` call can replace it, and requires the human’s authorization. Use separate IDs such as `codex-reviewer` and `claude-writer` for concurrent clients.

Terminal setup stores configuration at `%LOCALAPPDATA%/agent-mailbox/clients/<client>.json` on Windows or `$XDG_CONFIG_HOME/agent-mailbox/clients/<client>.json` (default `~/.config/agent-mailbox/clients/`) elsewhere. Unix permissions are 0600; Windows uses a current-user-only file ACL. The token never appears in setup output, arguments, tool schemas/results, URLs, or the plugin archive. This fallback is a protected local file, not an encrypted keychain. Native Claude sensitive configuration uses its secure store.

For automation, pass `MAILBOX_URL`, `MAILBOX_AGENT_ID`, `MAILBOX_TOKEN`, and optional `MAILBOX_ALLOWED_SENDERS` through the process environment. `MAILBOX_CLIENT_CONFIG` can select an existing protected configuration file. Remote URLs require HTTPS; plain HTTP is allowed only on loopback. Redirects and embedded URL credentials are rejected, and certificate validation is never disabled.

Setup can read a token from stdin with `--token-stdin --url URL --agent ID`; do not put a credential in a shell argument. `setup.mjs --client codex --check` prints only connection metadata. Setup does not register or take over a mailbox.

## Packaging and verification

```sh
node scripts/package-plugins.mjs
claude plugin validate plugins/claude --strict
claude plugin validate .claude-plugin/marketplace.json --strict
node --test plugin-src/*.test.mjs
go test ./...
```

The package script exports tool schemas from `internal/mailboxapi`, copies an explicit runtime allowlist into each self-contained package, and writes client-specific manifests/catalogs. It requires Go only for maintainers generating the shared schemas. Packages use stdio and include no lifecycle hooks, repository instruction files, or credentials. They expose legacy MCP initialization and current discovery; the official Go SDK tests validate the wire contract.

Real PostgreSQL integration tests use the existing test harness and exercise actual inbox changes, channel notification payloads, sender filtering, duplicate suppression, receipt preservation, session conflicts, and owner/team boundaries. CLI installation checks use isolated client homes and leave the user’s client settings unchanged. Native Claude model handling and the development-channel warning still need a human’s interactive session; wire-level injection does not prove a host accepted or acted on it.
