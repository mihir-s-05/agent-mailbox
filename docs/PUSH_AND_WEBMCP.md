# Push integration and WebMCP

## Notice changes, then read the inbox

Polling remains supported. `peek_inbox` and resources do not consume messages; `poll_inbox` records delivery, and `ack_messages` records receipt.

MCP resource subscriptions provide live change hints. Subscribe to `mailbox://teams/<encoded-team>/agents/<encoded-agent>/inbox` or its `/urgent` variant. Legacy session URIs remain supported. Current MCP uses `subscriptions/listen`; the official SDK handles the stream and acknowledgement. Clients reread after a hint. Revision reconciliation runs every second to repair missed PostgreSQL LISTEN notifications. Each outgoing update rechecks the subscriber's credential and mailbox access.

Durable webhooks use [OpenAI's MCP Events integration](https://developers.openai.com/plugins/build/mcp-events). `server/discover` advertises `events: {}`. A supporting host must create/refresh a subscription and supply its callback; installation alone does not wake an agent. Actual ChatGPT activation requires a deployed server and host-created event task. Local tests verify the server protocol and receiver behavior.

## Event contract

`events/list` exposes `mailbox.message.created`. Filters are `agent_id`, optional integer `min_priority` (0–3), and optional `topic`. Callers need ownership and `poll:self`.

Example subscription parameters:

```json
{
  "name": "mailbox.message.created",
  "arguments": { "agent_id": "reviewer", "min_priority": 2 },
  "delivery": {
    "mode": "webhook",
    "url": "https://receiver.example/mcp-events",
    "secret": "whsec_<base64 signing key>"
  },
  "ttlMs": 3600000,
  "cursor": null
}
```

Use a public HTTPS callback and a signing key decoding to 24–64 bytes. Verification sends a signed challenge; activation requires a successful matching echo. Address validation is repeated at connection time and blocks private/local destinations and redirects. Secrets are encrypted in PostgreSQL.

A new subscription without a cursor starts at the current mailbox watermark. Refresh without a cursor preserves progress. Explicit replay cursors are authenticated and bound to a mailbox; they cannot advance over an undelivered matching event. Retention can shorten replay, reported by `truncated=true`.

Lifetimes default to one hour, with requested values clamped to one minute through 24 hours. Refresh before `refreshBefore`. IDs are deterministic for subject/team, callback, event and canonical filters. Key rotation signs with both keys for five minutes. Unsubscribe uses the same event, arguments and webhook URL; a signing secret is not needed.

Deliveries contain `eventId`, `name`, occurrence `timestamp`, `data` and a replay `cursor`. Data contains message ID, recipient, sender, priority, topic and expiry. Body and attachments remain in the inbox. Consumers fetch separately and deduplicate event IDs.

Workers lease callbacks in PostgreSQL and preserve subscription ordering. Four workers per process allow unrelated subscriptions to progress concurrently. Transient failures retry with exponential delay and jitter, up to eight attempts. Permanent failures stop the subscription; a refresh can retry failed deliveries. Webhook receipt is independent from inbox acknowledgement and completed work.

Authenticated `GET /api/subscriptions?agent_id=<agent>` reports expiry, cursor and pending/failed counts without exposing callbacks or secrets. Preserve `MAILBOX_EVENT_KEY_FILE` across restarts and share the key across replicas using one database.

## Browser tools

The website's explorer at `/mailbox.html` includes a credential-free synthetic demo and a live mode using authenticated `/api/` endpoints. It keeps the bearer token in memory, obtains shared server tool schemas, and registers `mailbox_*` actions using the [current WebMCP draft](https://webmachinelearning.github.io/webmcp/). It feature-detects `document.modelContext`; manual controls work when native support is absent. The root page focuses on installation. See [the website and explorer guide](VIEWER.md).

Connecting starts as an observer, using stable owner-scoped inbox/log reads and a same-team directory. It leaves sessions, presence, and delivery state unchanged. Observer WebMCP exposes peek and directory tools only. Explicitly activating message actions registers a session and exposes peek, poll, send, acknowledge, list agents, logs, cancellation and status. Replacing an active connection requires the unchecked-by-default replacement option. Hidden identity overrides are rejected. Message results carry untrusted-content annotations, cancellation aborts requests, and selection/disconnect/unload unregisters tools with an AbortSignal. Disconnect clears credentials and observed live data.

`GET /api/inbox/stream` refreshes the page through an authenticated fetch stream. Tokens remain in headers, never URLs. The stream closes on revocation or ownership loss. A closed page cannot receive WebMCP calls; background delivery uses durable webhooks.
