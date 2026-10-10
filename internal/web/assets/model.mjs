export const priorities = ['Low', 'Normal', 'High', 'Urgent'];
export const states = { PENDING: 'Pending', DELIVERED: 'Delivered', ACKED: 'Acknowledged', EXPIRED: 'Expired', CANCELLED: 'Cancelled', UNKNOWN: 'No longer in preview', ACCEPTED: 'Accepted' };
export const keyOf = m => `${m.message_id}:${m.recipient_agent_id || ''}`;
export const isOutstanding = m => ['PENDING', 'DELIVERED'].includes(m.delivery_state) && m.outstanding !== false;
export function relatedTo(m, agent) { return m.recipient_agent_id === agent || m.from_agent_id === agent; }
export function filterMessages(messages, { agent, view, query = '', priority = '', state = '' }) {
  return messages.filter(m => (view === 'inbox' ? m.recipient_agent_id === agent && isOutstanding(m) : relatedTo(m, agent)) && (!priority || m.priority === Number(priority)) && (!state || m.delivery_state === state) && `${m.topic} ${m.body} ${m.from_agent_id} ${m.message_id}`.toLowerCase().includes(query.toLowerCase()))
    .sort((a, b) => view === 'inbox' ? b.priority - a.priority || a.created_at.localeCompare(b.created_at) : b.created_at.localeCompare(a.created_at));
}
// Follow explicit reply links only. Equal topics do not imply a conversation.
export function conversation(messages, selected, agent) {
  if (!selected) return { messages: [], missingParent: null };
  const visible = messages.filter(m => relatedTo(m, agent));
  const byID = new Map(visible.map(m => [m.message_id, m]));
  const ancestors = new Set([selected.message_id]);
  let root = selected, missingParent = null;
  while (root.in_reply_to && !ancestors.has(root.in_reply_to)) {
    ancestors.add(root.in_reply_to);
    const parent = byID.get(root.in_reply_to);
    if (!parent) { missingParent = root.in_reply_to; break; }
    root = parent;
  }
  const included = new Set([root.message_id]);
  // Bounded by the actual message count, including malformed/cyclic reply data.
  for (let i = 0; i < visible.length; i++) {
    let changed = false;
    for (const m of visible) if (included.has(m.in_reply_to) && !included.has(m.message_id)) { included.add(m.message_id); changed = true; }
    if (!changed) break;
  }
  const unique = new Map(visible.filter(m => included.has(m.message_id)).map(m => [m.message_id, m]));
  return { messages: [...unique.values()].sort((a, b) => a.created_at.localeCompare(b.created_at)), missingParent };
}
export function reconcile(previous, current, events, agent) {
  const terminal = new Set(['ACKED', 'EXPIRED', 'CANCELLED']);
  const map = new Map(previous.map(m => [keyOf(m), { ...m, outstanding: false }]));
  for (const m of current) {
    const normalized = { ...m, recipient_agent_id: agent, outstanding: true };
    const known = map.get(keyOf(normalized));
    // A read started before a mutation can return after its terminal receipt.
    if (known && terminal.has(known.delivery_state)) { normalized.delivery_state = known.delivery_state; normalized.acked_at = known.acked_at; normalized.outstanding = false; }
    map.set(keyOf(normalized), normalized);
  }
  const sorted = [...events].sort((a, b) => a.event_at.localeCompare(b.event_at));
  for (const e of sorted) {
    for (const m of map.values()) if (e.message_id === m.message_id && (!e.recipient_agent_id || e.recipient_agent_id === m.recipient_agent_id)) {
      if (['DELIVERED', 'ACKED', 'EXPIRED', 'CANCELLED'].includes(e.event_type)) {
        // Automatic ack and delivery share a timestamp; query tie order varies.
        if (terminal.has(m.delivery_state)) continue;
        m.delivery_state = e.event_type;
        if (terminal.has(e.event_type)) m.outstanding = false;
        if (e.event_type === 'DELIVERED') m.delivered_at = e.event_at;
        if (e.event_type === 'ACKED') m.acked_at = e.event_at;
      }
    }
  }
  // Absence from a 20-message peek is not evidence of acknowledgement or expiry.
  for (const m of map.values()) if (!m.outstanding && ['PENDING', 'DELIVERED'].includes(m.delivery_state) && m.recipient_agent_id === agent) m.delivery_state = 'UNKNOWN';
  return [...map.values()];
}
export function formatTime(value, full = false) {
  if (!value || Number.isNaN(Date.parse(value))) return '—';
  return new Intl.DateTimeFormat(undefined, full ? { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' } : { hour: 'numeric', minute: '2-digit' }).format(new Date(value));
}
export function destination(m) { return m.to?.type === 'broadcast' ? m.to.scope === 'tag' ? `Tag: ${m.to.tag}` : 'Whole team' : m.to?.agent_id || m.recipient_agent_id || '—'; }
