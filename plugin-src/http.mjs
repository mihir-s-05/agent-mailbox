export class MailboxError extends Error {
  constructor(message, status = 0, code = '') { super(message); this.status = status; this.code = code; }
}

export function createAPI(config, fetch_ = fetch) {
  const lifetime = new AbortController();
  const url = route => new URL(route, config.url);
  const headers = { Authorization: `Bearer ${config.token}`, 'Content-Type': 'application/json' };
  async function request(route, options = {}) {
    const response = await fetch_(url(route), { ...options, headers: { ...headers, ...options.headers }, redirect: 'error', signal: AbortSignal.any([lifetime.signal, AbortSignal.timeout(30000), options.signal].filter(Boolean)) });
    const reader = response.body?.getReader(), chunks = [];
    let size = 0;
    if (!reader) throw new MailboxError('Mailbox returned an empty response.');
    try {
      while (true) {
        const { value, done } = await reader.read(); if (done) break;
        size += value.byteLength;
        // A valid 100-message poll can include bodies and bounded attachments.
        if (size > 64 * 1024 * 1024) throw new MailboxError('Mailbox response exceeded the size limit.');
        chunks.push(value);
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    const raw = Buffer.concat(chunks).toString('utf8');
    let data;
    try { data = JSON.parse(raw); } catch { throw new MailboxError('The endpoint did not return the mailbox API. Check the service URL.', response.status); }
    if (!response.ok || data.error) {
      const code = typeof data.error === 'object' ? data.error?.code : '';
      const message = response.status === 401 ? 'Mailbox credential expired or was revoked. Run setup again.' : response.status === 403 ? 'This credential cannot access the selected mailbox.' : code === 'SESSION_CONFLICT' ? 'This agent already has an active connection. Choose another ID, or explicitly replace it with mailbox_connect.' : code === 'SESSION_INVALID' ? 'This connection was replaced or ended. Call mailbox_connect; existing connections still require explicit replacement.' : 'Mailbox request was rejected. Check permissions, session, and arguments.';
      throw new MailboxError(message, response.status, code);
    }
    return data;
  }
  return {
    request,
    call(name, arguments_, signal) { return request('api/call', { method: 'POST', body: JSON.stringify({ name, arguments: arguments_ }), signal }); },
    async watch(onChange, signal) {
      const response = await fetch_(url(`api/inbox/stream?agent_id=${encodeURIComponent(config.agent)}`), { headers: { ...headers, Accept: 'text/event-stream' }, redirect: 'error', signal: AbortSignal.any([lifetime.signal, signal].filter(Boolean)) });
      if (!response.ok || !response.headers.get('content-type')?.includes('text/event-stream')) { await response.body?.cancel(); throw new MailboxError('Inbox stream rejected.', response.status); }
      const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
      let buffer = '';
      try {
        while (true) {
          const { value, done } = await reader.read(); if (done) break;
          buffer += value.replace(/\r\n/g, '\n');
          if (buffer.length > 65536) throw new MailboxError('Inbox stream exceeded the frame limit.');
          let end;
          while ((end = buffer.indexOf('\n\n')) >= 0) {
            const event = buffer.slice(0, end); buffer = buffer.slice(end + 2);
            if (event.split('\n').includes('event: inbox_changed')) await onChange();
          }
        }
      } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
      throw new MailboxError('Inbox stream ended.');
    },
    close() { lifetime.abort(); }
  };
}
