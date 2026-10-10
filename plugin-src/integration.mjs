import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const endpoint = process.env.MAILBOX_PLUGIN_TEST_URL;
if (!endpoint) throw new Error('An isolated test service is required.');
const token = 'qa-plugin-token';
const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
const request = async (route, body) => {
  const response = await fetch(new URL(route, endpoint), { headers, ...(body ? { method: 'POST', body: JSON.stringify(body) } : {}) });
  assert.equal(response.status, 200); return response.json();
};
const apiCall = (name, arguments_) => request('api/call', { name, arguments: arguments_ });
const children = [];
function start(agent, credential = token) {
  const child = spawn(process.execPath, [path.join(root, 'plugins/claude/scripts/bridge.mjs'), '--client=claude', '--channel'], { env: { ...process.env, MAILBOX_URL: endpoint, MAILBOX_AGENT_ID: agent, MAILBOX_TOKEN: credential, MAILBOX_ALLOWED_SENDERS: 'writer' }, stdio: ['pipe', 'pipe', 'pipe'] });
  children.push(child);
  let next = 1, stderr = '';
  const pending = new Map(), notifications = [];
  child.stderr.on('data', value => { stderr += value; });
  createInterface({ input: child.stdout }).on('line', line => {
    const message = JSON.parse(line);
    if (message.id !== undefined) { const callback = pending.get(message.id); pending.delete(message.id); callback?.(message); }
    else notifications.push(message);
  });
  function rpc(method, params = {}) {
    const id = next++;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`RPC timed out: ${method}`)); }, 10000);
      pending.set(id, response => { clearTimeout(timeout); response.error ? reject(new Error(response.error.message)) : resolve(response.result); });
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  const call = (name, arguments_ = {}) => rpc('tools/call', { name, arguments: arguments_ });
  return { child, rpc, call, notifications, stderr: () => stderr, ready: () => child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n') };
}
const wait = async predicate => { const until = Date.now() + 10000; while (!predicate()) { if (Date.now() > until) throw new Error('Expected channel state did not arrive.'); await new Promise(resolve => setTimeout(resolve, 50)); } };
try {
  const writer = await apiCall('register_agent', { team_id: 'plugin-test', agent_id: 'writer' });
  const stranger = await apiCall('register_agent', { team_id: 'plugin-test', agent_id: 'stranger' });
  const plugin = start('claude-reviewer');
  const init = await plugin.rpc('initialize', { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'channel-contract-test', version: '1' } });
  assert.deepEqual(init.capabilities.experimental['claude/channel'], {});
  assert.equal('claude/channel/permission' in init.capabilities.experimental, false);
  plugin.ready();
  assert.equal((await plugin.call('mailbox_connect')).isError, undefined);
  const initialNotifications = plugin.notifications.length;
  const hidden = await apiCall('send_message', { session_id: stranger.session_id, to: { type: 'direct', agent_id: 'claude-reviewer' }, priority: 3, topic: 'Unlisted sender', body: 'Do not inject this.', require_ack: true });
  const sent = await apiCall('send_message', { session_id: writer.session_id, to: { type: 'direct', agent_id: 'claude-reviewer' }, priority: 2, topic: 'Review request', body: 'Synthetic channel test. Review this patch.', require_ack: true });
  await wait(() => plugin.notifications.some(notification => notification.params?.meta?.message_id === sent.message_id));
  const notice = plugin.notifications.find(notification => notification.params?.meta?.message_id === sent.message_id);
  assert.equal(notice.method, 'notifications/claude/channel');
  assert.equal(notice.params.meta.from_agent_id, 'writer');
  assert.equal(notice.params.meta.preview, 'true');
  assert.match(notice.params.content, /Synthetic channel test/);
  assert.equal(plugin.notifications.some(notification => notification.params?.meta?.message_id === hidden.message_id), false);
  const inbox = await request('api/inbox?agent_id=claude-reviewer');
  assert.equal(inbox.messages.find(message => message.message_id === sent.message_id).delivery_state, 'PENDING');
  const denied = start('claude-reviewer', 'qa-other-owner');
  await denied.rpc('initialize', { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'denied', version: '1' } }); denied.ready();
  assert.equal((await denied.call('peek_inbox')).isError, true);
  assert.equal((await denied.call('mailbox_connect')).isError, true);
  assert.equal(denied.notifications.length, 0);
  const conflict = start('claude-reviewer');
  await conflict.rpc('initialize', { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'conflict', version: '1' } });
  assert.equal((await conflict.call('mailbox_connect')).isError, true);
  const received = await plugin.call('poll_inbox', { max_messages: 100 });
  assert.equal(received.isError, undefined);
  await plugin.call('ack_messages', { message_ids: [sent.message_id, hidden.message_id] });
  assert.equal((await request('api/inbox?agent_id=claude-reviewer')).messages.length, 0);
  assert.equal(plugin.notifications.filter(notification => notification.params?.meta?.message_id === sent.message_id).length, 1);
  const hiddenIdentity = await plugin.call('send_message', { session_id: writer.session_id });
  assert.equal(hiddenIdentity.isError, true);
  const reply = await plugin.call('send_message', { to: { type: 'direct', agent_id: 'writer' }, priority: 1, topic: 'Review result', body: 'Synthetic result.', in_reply_to: sent.message_id });
  assert.equal(reply.isError, undefined);
  assert.equal((await request('api/inbox?agent_id=writer')).messages.some(message => message.in_reply_to === sent.message_id), true);
  await plugin.call('mailbox_disconnect');
  assert.equal(JSON.stringify(plugin.notifications).includes(token), false);
  assert.equal(plugin.stderr().includes(token), false);
  assert.equal(plugin.notifications.length >= initialNotifications, true);
  console.log(JSON.stringify({ passed: true, checks: ['legacy channel handshake', 'no permission relay', 'real authenticated streamed arrival', 'sender allowlist', 'pending until poll', 'no duplicate injection', 'owner denial', 'session conflict without replacement', 'poll/ack', 'linked reply', 'identity binding', 'credential redaction', 'disconnect'] }));
} finally {
  await Promise.all(children.map(child => new Promise(resolve => { if (child.exitCode !== null) return resolve(); child.once('exit', resolve); child.stdin.end(); setTimeout(() => child.kill(), 2500).unref(); })));
}
