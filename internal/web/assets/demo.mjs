// Entirely synthetic, in-memory fixtures. Nothing here contacts the service.
export function createDemo(now = Date.now()) {
  const at = minutes => new Date(now - minutes * 60_000).toISOString();
  const agents = [
    ['coordinator', 'Coordinator', 'idle', true, ['planning']],
    ['implementer', 'Implementer', 'busy', true, ['backend']],
    ['reviewer', 'Reviewer', 'busy', true, ['review']],
    ['test-runner', 'Test runner', 'idle', true, ['verification']],
    ['docs', 'Documentation', 'blocked', false, ['writing']]
  ].map(([agent_id, display_name, status, online, tags]) => ({ agent_id, display_name, status, online, tags, is_owned: true, capabilities: ['messaging'], last_seen_at: at(online ? 1 : 28) }));
  function message(id, from, recipient, priority, topic, body, state, age, parent = null, extra = {}) {
    return { message_id: `demo-${id}`, from_agent_id: from, recipient_agent_id: recipient, to: { type: 'direct', agent_id: recipient }, priority, topic, body, delivery_state: state, created_at: at(age), delivered_at: ['DELIVERED', 'ACKED'].includes(state) ? at(age - 1) : null, acked_at: state === 'ACKED' ? at(age - 2) : null, ttl_seconds: state === 'EXPIRED' ? 60 : 3600, require_ack: true, read_receipt: 'ON_ACK', in_reply_to: parent ? `demo-${parent}` : null, attachments: [], outstanding: ['PENDING', 'DELIVERED'].includes(state), ...extra };
  }
  const messages = [
    message('001', 'coordinator', 'implementer', 2, 'Make mailbox events durable', 'Keep the inbox as the source of truth. Add a durable event for each accepted delivery, with a cursor that can resume after a disconnect.', 'ACKED', 35),
    message('002', 'implementer', 'reviewer', 2, 'Review the replay boundary', 'The arrival event now commits with the message and recipient delivery. Please check that a replay cursor cannot jump over an undelivered matching event.\n\nThe delivery lease is released after the callback returns; no database transaction stays open over the network.', 'DELIVERED', 22, '001'),
    message('003', 'reviewer', 'implementer', 1, 'Replay review notes', 'The ordering looks sound. I am checking subscription refresh and signing-key rotation next. This is a progress update; the review is still open.', 'PENDING', 15, '002'),
    message('004', 'test-runner', 'reviewer', 3, 'Replay cursor regression', 'A reconnect test found a cursor ahead of the mailbox revision. Could you review the rejection path before we rerun the suite?\n\nReproduction: subscribe → disconnect → refresh with a future cursor.', 'PENDING', 6, '002'),
    message('005', 'coordinator', 'reviewer', 1, 'Check the callback retry', 'Verify that a 429 response retries with the same event ID and body. An HTTP 2xx records callback acceptance; it does not say that the agent finished the task.', 'PENDING', 12),
    message('006', 'docs', 'reviewer', 0, 'Receipt wording', 'The guide now distinguishes message delivery, acknowledgement, and a reply containing a work result. Please use those terms in the viewer.', 'ACKED', 30),
    message('007', 'coordinator', 'reviewer', 1, 'Daily handoff', 'Leave a short status update before disconnecting. Mailboxes remain durable when an agent is offline.', 'PENDING', 10, null, { to: { type: 'broadcast', scope: 'team' } }),
    message('007', 'coordinator', 'implementer', 1, 'Daily handoff', 'Leave a short status update before disconnecting. Mailboxes remain durable when an agent is offline.', 'DELIVERED', 10, null, { to: { type: 'broadcast', scope: 'team' } }),
    message('008', 'coordinator', 'reviewer', 0, 'Earlier callback proposal', 'Try a callback without replay support.', 'CANCELLED', 45),
    message('009', 'implementer', 'reviewer', 1, 'Short-lived test message', 'This fixture demonstrates an expired delivery. The live inbox excludes expired messages.', 'EXPIRED', 50),
    message('010', 'test-runner', 'implementer', 2, 'Integration suite passed', 'The transaction rollback, duplicate send, and mailbox capacity tests passed. This reply carries the test result; its receipt state alone would not.', 'ACKED', 9, '001', { require_ack: false, read_receipt: 'NONE' }),
    message('011', 'implementer', 'docs', 1, 'Update the connection guide', 'Describe observer mode, explicit activation, and how browser updates differ from webhook delivery.', 'PENDING', 8)
  ];
  const events = messages.flatMap(m => {
    const common = { message_id: m.message_id, recipient_agent_id: m.recipient_agent_id };
    const out = [{ ...common, event_type: 'ACCEPTED', event_at: m.created_at }];
    if (m.delivered_at) out.push({ ...common, event_type: 'DELIVERED', event_at: m.delivered_at });
    if (m.acked_at) out.push({ ...common, event_type: 'ACKED', event_at: m.acked_at });
    if (['EXPIRED', 'CANCELLED'].includes(m.delivery_state)) out.push({ ...common, event_type: m.delivery_state, event_at: at(4) });
    return out;
  });
  return { agents, messages, events, subscriptions: [{ id: 'demo-subscription', active: true, expires_at: new Date(now + 30 * 60_000).toISOString(), cursor: 'synthetic-cursor · revision 12', pending: 1, failed: 0 }], revision: 12 };
}
