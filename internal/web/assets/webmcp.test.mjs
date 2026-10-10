import test from 'node:test';
import assert from 'node:assert/strict';
import { installMailboxTools, createMailboxClient } from './webmcp.mjs';

const definitions = ['peek_inbox', 'send_message', 'register_agent'].map(name => ({ name, description: 'Mailbox action', readOnly: name === 'peek_inbox', inputSchema: { type: 'object', properties: { session_id: { type: 'string' }, team_id: { type: 'string' }, body: { type: 'string' } }, required: ['session_id', 'body'] } }));
test('WebMCP tools bind page identity, return objects, cancel and unregister', async () => {
  const tools = new Map();
  const modelContext = { async registerTool(tool, { signal }) { tools.set(tool.name, tool); signal.addEventListener('abort', () => tools.delete(tool.name)); } };
  let observed;
  const installed = await installMailboxTools(modelContext, definitions, async (name, args, signal) => { observed = { name, args, signal }; return { messages: [{ body: 'untrusted' }] }; });
  assert.equal(installed.supported, true); assert.equal(tools.size, 2);
  const tool = tools.get('mailbox_peek_inbox'); assert.equal(tool.inputSchema.properties.session_id, undefined); assert.deepEqual(tool.inputSchema.required, ['body']); assert.equal(tool.annotations.untrustedContentHint, true);
  const controller = new AbortController(); const result = await tool.execute({ body: 'message' }, { signal: controller.signal }); assert.equal(result.messages[0].body, 'untrusted'); controller.abort(); assert.equal(observed.signal.aborted, true);
  await assert.rejects(tool.execute({ session_id: 'borrowed' }), /Identity/);
  installed.close(); assert.equal(tools.size, 0); await assert.rejects(tool.execute({}), /disconnected/);
});
test('observer uses the project mount without creating a session; close cancels requests', async () => {
  const calls = [];
  const client = createMailboxClient('private-token', async (path, options) => {
    calls.push({ path, options });
    return { ok: true, async json() {
      if (path.endsWith('/api/session')) return { team_id: 'owned-team' };
      if (path.endsWith('/api/agents')) return { agents: [{ agent_id: 'owned', is_owned: true }] };
      return { messages: [] };
    } };
  }, '/agent-mailbox/');
  assert.equal((await client.observe()).team_id, 'owned-team');
  assert.equal((await client.agents())[0].is_owned, true);
  await client.inbox('owned');
  assert.equal(calls.some(call => call.options.method === 'POST'), false);
  assert.equal(calls[2].path, '/agent-mailbox/api/inbox?agent_id=owned');
  await assert.rejects(client.call('send_message', {}), /first/);
  client.close();
  assert.equal(calls.every(call => call.options.signal.aborted), true);
  await assert.rejects(client.inbox('owned'), /disconnected/);
});
test('unsupported browsers keep manual API; credentials are cleared on disconnect', async () => {
  assert.equal((await installMailboxTools(undefined, definitions, () => {})).supported, false);
  const calls = [];
  const client = createMailboxClient('private-token', async (path, options) => { calls.push({ path, options }); return { ok: true, async json() { if (path === '/api/session') return { team_id: 'team' }; if (JSON.parse(options.body).name === 'register_agent') return { session_id: 'owned-session' }; return { ok: true }; } }; });
  await client.connect('agent'); await client.call('send_message', { body: 'message', idempotency_key: 'retry' });
  assert.equal(JSON.parse(calls[1].options.body).arguments.replace_existing_session, false);
  const request = JSON.parse(calls.at(-1).options.body); assert.equal(request.arguments.session_id, 'owned-session'); assert.equal(request.arguments.idempotency_key, 'retry'); assert.equal(calls.at(-1).options.headers.Authorization, 'Bearer private-token');
  client.close(); await assert.rejects(client.call('peek_inbox', {}), /first/);
});
