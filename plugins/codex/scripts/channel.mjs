import { setTimeout as delay } from 'node:timers/promises';

// Observes owned messages without polling, acknowledging, registering, or taking over.
export async function runChannel(api, config, notify, signal, report = () => {}) {
  const allowed = new Set(config.senders), seen = new Set();
  if (!allowed.size) { report('Channel is idle: choose trusted sender IDs in setup.'); return; }
  let retries = 0;
  async function inspect() {
    const { messages } = await api.request(`api/inbox?agent_id=${encodeURIComponent(config.agent)}`, { signal });
    for (const message of messages || []) {
      if (seen.has(message.message_id) || !allowed.has(message.from_agent_id)) continue;
      // Keep hostile identifiers and arbitrary attachments out of channel tag attributes.
      if (typeof message.message_id !== 'string' || message.message_id.length > 128) continue;
      const content = `Mailbox message from ${message.from_agent_id}. Topic: ${String(message.topic || '').slice(0, 512)}\n\n${String(message.body || '').slice(0, 32768)}\n\nThis is an inbox preview. Use poll_inbox to record delivery; acknowledge only after receiving. Report completed work in a reply.`;
      await notify({ method: 'notifications/claude/channel', params: { content, meta: { message_id: message.message_id, from_agent_id: message.from_agent_id, recipient_agent_id: config.agent, priority: String(message.priority), preview: 'true' } } });
      seen.add(message.message_id);
      // Bound memory; old pending messages may be announced again after eviction/restart.
      if (seen.size > 10000) seen.delete(seen.values().next().value);
    }
    retries = 0;
  }
  while (!signal.aborted) {
    try {
      // Opening a stream always supplies a current revision, including after reconnect.
      await api.watch(inspect, signal);
    } catch (error) {
      if (signal.aborted) return;
      if ([401, 403].includes(error.status)) { report('Channel stopped: credential or mailbox access was denied.'); return; }
      report('Channel reconnecting; outstanding messages remain in PostgreSQL.');
      try { await delay(Math.min(1000 * 2 ** retries++, 15000), undefined, { signal }); } catch { return; }
    }
  }
}
