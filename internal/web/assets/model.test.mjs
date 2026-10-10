import test from 'node:test';
import assert from 'node:assert/strict';
import { createDemo } from './demo.mjs';
import { filterMessages, conversation, reconcile } from './model.mjs';
test('reply links exclude equal topics and label unavailable ancestors', () => {
  const demo = createDemo(1_800_000_000_000), selected = demo.messages.find(m => m.message_id === 'demo-002');
  const thread = conversation(demo.messages, selected, 'reviewer');
  assert.equal(thread.missingParent, 'demo-001');
  assert.deepEqual(thread.messages.map(m => m.message_id), ['demo-002', 'demo-003', 'demo-004']);
  const unrelated = { ...selected, message_id: 'same-topic', in_reply_to: null };
  assert.equal(conversation([...demo.messages, unrelated], selected, 'reviewer').messages.some(m => m.message_id === unrelated.message_id), false);
});
test('automatic ack cannot regress when same-timestamp audit arrives in either order', () => {
  const m = createDemo().messages.find(m => m.message_id === 'demo-004');
  const common = { message_id: m.message_id, recipient_agent_id: 'reviewer', event_at: new Date().toISOString() };
  const delivered = { ...common, event_type: 'DELIVERED' }, acked = { ...common, event_type: 'ACKED' };
  for (const events of [[delivered, acked], [acked, delivered]]) assert.equal(reconcile([m], [], events, 'reviewer')[0].delivery_state, 'ACKED');
  const known = { ...m, delivery_state: 'ACKED', acked_at: common.event_at };
  assert.equal(reconcile([known], [m], [delivered], 'reviewer')[0].delivery_state, 'ACKED');
});
test('broadcast receipts remain per recipient; inbox filters outstanding data', () => {
  const demo = createDemo();
  demo.messages.find(m => m.message_id === 'demo-007' && m.recipient_agent_id === 'reviewer').delivery_state = 'ACKED';
  assert.equal(demo.messages.find(m => m.message_id === 'demo-007' && m.recipient_agent_id === 'implementer').delivery_state, 'DELIVERED');
  assert.deepEqual(filterMessages(demo.messages, { agent: 'reviewer', view: 'inbox', priority: '3', query: 'cursor' }).map(m => m.message_id), ['demo-004']);
});
test('peek absence cannot imply acknowledgement; only matching recipient audit confirms it', () => {
  const m = createDemo().messages.find(m => m.message_id === 'demo-004');
  assert.equal(reconcile([m], [], [], 'reviewer')[0].delivery_state, 'UNKNOWN');
  const event = { message_id: m.message_id, recipient_agent_id: 'reviewer', event_type: 'ACKED', event_at: new Date().toISOString() };
  assert.equal(reconcile([m], [], [event], 'reviewer')[0].delivery_state, 'ACKED');
  assert.equal(reconcile([m], [], [{ ...event, recipient_agent_id: 'implementer' }], 'reviewer')[0].delivery_state, 'UNKNOWN');
});
