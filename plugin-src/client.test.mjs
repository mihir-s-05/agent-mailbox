import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { normalizeConfig, loadConfig } from './config.mjs';
import { configure } from './setup.mjs';
import { PluginClient } from './client.mjs';
import { runChannel } from './channel.mjs';

const input = { url: 'http://127.0.0.1:8443/project/mcp', agent: 'reviewer', token: 'fake-test-credential', senders: 'writer, writer, coordinator' };
const definitions = JSON.parse(await readFile(new URL('../plugins/codex/scripts/tools.json', import.meta.url), 'utf8'));
const json = value => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });
test('configuration accepts a project MCP URL and rejects remote HTTP, URL secrets and wildcard senders', () => {
  const config = normalizeConfig(input);
  assert.equal(config.url, 'http://127.0.0.1:8443/project/');
  assert.deepEqual(config.senders, ['writer', 'coordinator']);
  for (const url of ['http://example.com/', 'https://user:secret@example.com/', 'https://example.com/?token=secret', 'file:///tmp/']) assert.throws(() => normalizeConfig({ ...input, url }));
  assert.throws(() => normalizeConfig({ ...input, senders: '*' }));
});
test('terminal onboarding validates before saving, protects the file and returns no token', async () => {
  const directory = await mkdtemp(path.join(tmpdir(), 'agent-mailbox-plugin-test-'));
  const env = { MAILBOX_CLIENT_CONFIG: path.join(directory, 'config.json') };
  try {
    const fetch_ = async url => json(url.pathname.endsWith('api/session') ? { team_id: 'team', subject: 'owner' } : { tools: definitions });
    const status = await configure('codex', input, { env, fetch_ });
    assert.equal(status.team_id, 'team');
    assert.equal(JSON.stringify(status).includes(input.token), false);
    assert.equal((await loadConfig('codex', env)).token, input.token);
    if (process.platform !== 'win32') assert.equal((await stat(env.MAILBOX_CLIENT_CONFIG)).mode & 0o777, 0o600);
    await assert.rejects(configure('codex', { ...input, token: 'rejected' }, { env, fetch_: async () => new Response('{}', { status: 401 }) }));
    assert.equal((await loadConfig('codex', env)).token, input.token);
  } finally {
    const resolved = path.resolve(directory);
    assert.ok(resolved.startsWith(path.resolve(tmpdir()) + path.sep) && path.basename(resolved).startsWith('agent-mailbox-plugin-test-'));
    await rm(resolved, { recursive: true, force: true });
  }
});
test('plugin binds identity, requires activation and refuses implicit session replacement', async () => {
  const requests = [];
  let conflict = true;
  const env = { MAILBOX_URL: input.url, MAILBOX_AGENT_ID: input.agent, MAILBOX_TOKEN: input.token };
  const client = new PluginClient({ client: 'codex', definitions, root: '/plugin/scripts', env, fetch_: async (url, options) => {
    assert.equal(options.headers.Authorization, `Bearer ${input.token}`);
    assert.equal(options.redirect, 'error');
    if (url.pathname.endsWith('api/session')) return json({ team_id: 'team', subject: 'owner' });
    const call = JSON.parse(options.body); requests.push(call);
    if (call.name === 'register_agent') {
      if (conflict && !call.arguments.replace_existing_session) return new Response(JSON.stringify({ error: { code: 'SESSION_CONFLICT' } }), { status: 400 });
      return json({ session_id: 'private-session' });
    }
    return json({ accepted: true });
  } });
  await client.reload();
  assert.equal(JSON.stringify(client.setup()).includes(input.token), false);
  assert.equal(JSON.stringify(client.tools()).includes('session_id'), false);
  await assert.rejects(client.call('send_message', {}), /mailbox_connect/);
  await assert.rejects(client.call('mailbox_connect', {}), /active connection/);
  assert.equal(requests.at(-1).arguments.replace_existing_session, false);
  conflict = false;
  const connected = await client.call('mailbox_connect', { replace_existing_session: true });
  assert.equal(JSON.stringify(connected).includes('private-session'), false);
  await assert.rejects(client.call('send_message', { session_id: 'foreign-session' }), /identity/);
  await client.call('send_message', { body: 'hello' });
  assert.equal(requests.at(-1).arguments.session_id, 'private-session');
  await client.release();
  assert.equal(requests.at(-1).name, 'deregister_agent');
});
test('channel gates sender IDs, suppresses duplicates and never receives or acknowledges', async () => {
  const lifetime = new AbortController(), notifications = [], routes = [];
  const messages = [{ message_id: 'one', from_agent_id: 'writer', topic: 'Review', body: 'Trusted sender task data.', priority: 2 }, { message_id: 'two', from_agent_id: 'stranger', topic: 'Bad', body: 'Do not inject this.', priority: 3 }];
  const api = { request: async route => { routes.push(route); return { messages }; }, watch: async inspect => { await inspect(); await inspect(); lifetime.abort(); } };
  await runChannel(api, normalizeConfig(input), notification => notifications.push(notification), lifetime.signal);
  assert.equal(notifications.length, 1);
  assert.equal(notifications[0].method, 'notifications/claude/channel');
  assert.equal(notifications[0].params.meta.message_id, 'one');
  assert.equal(notifications[0].params.meta.preview, 'true');
  assert.match(notifications[0].params.content, /Use poll_inbox/);
  assert.ok(routes.every(route => route.startsWith('api/inbox?')));
  const empty = new AbortController();
  await runChannel(api, { ...normalizeConfig(input), senders: [] }, () => assert.fail('No trusted senders'), empty.signal);
  assert.equal(notifications.length, 1);
});
