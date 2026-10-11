import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { once } from 'node:events';
import { PluginClient, CHANNEL_INSTRUCTIONS } from './client.mjs';

const root = path.dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const client = args.includes('--client=claude') ? 'claude' : 'codex';
const channel = client === 'claude' && args.includes('--channel');
const definitions = JSON.parse(await readFile(new URL('./tools.json', import.meta.url), 'utf8'));
const implementation = { name: 'agent-mailbox-plugin', version: '0.1.1' };
const versions = ['2026-07-28', '2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05'];
const capabilities = { tools: {}, ...(channel ? { experimental: { 'claude/channel': {} } } : {}) };
const instructions = 'Call mailbox_setup to see onboarding instructions, then mailbox_connect to activate your configured agent. Identity is bound by the plugin; credentials never belong in tool arguments. Replacement of an active session needs explicit human authorization. ' + (channel ? CHANNEL_INSTRUCTIONS : 'Peeking leaves messages pending; poll_inbox records delivery and ack_messages records receipt. Report work results in a reply.');
let output = Promise.resolve(), calls = Promise.resolve(), buffer = '', closed = false, modern = false;
const running = new Map();
function send(message) {
  output = output.then(async () => { if (closed) return; if (!process.stdout.write(JSON.stringify(message) + '\n')) await once(process.stdout, 'drain'); });
  return output;
}
const plugin = new PluginClient({ client, channel, definitions, root, notify: send, report: message => process.stderr.write(`Agent Mailbox: ${message}\n`) });
function result(id, value) {
  if (modern) value = { ...value, resultType: 'complete', _meta: { ...value._meta, 'io.modelcontextprotocol/serverInfo': implementation } };
  return send({ jsonrpc: '2.0', id, result: value });
}
const rpcError = (id, code, message) => send({ jsonrpc: '2.0', id, error: { code, message } });
async function handle(message) {
  const { id, method, params = {} } = message;
  if (message.jsonrpc !== '2.0' || typeof method !== 'string' || (id !== undefined && typeof id !== 'string' && typeof id !== 'number')) return rpcError(null, -32600, 'Invalid request');
  if (method === 'notifications/cancelled') { running.get(params.requestId)?.abort(); return; }
  if (method === 'notifications/initialized') { await plugin.reload().catch(() => {}); await plugin.startChannel(); return; }
  if (id === undefined) return;
  if (method === 'initialize') return result(id, { protocolVersion: versions.includes(params.protocolVersion) ? params.protocolVersion : '2025-11-25', capabilities, serverInfo: implementation, instructions });
  if (method === 'server/discover') { modern = true; await plugin.reload().catch(() => {}); await result(id, { supportedVersions: versions, capabilities, instructions, ttlMs: 0, cacheScope: 'private' }); await plugin.startChannel(); return; }
  if (method === 'ping') return result(id, {});
  if (method === 'tools/list') return result(id, { tools: plugin.tools(), ...(modern ? { ttlMs: 0, cacheScope: 'private' } : {}) });
  if (method !== 'tools/call') return rpcError(id, -32601, 'Method not found');
  const lifetime = new AbortController(); running.set(id, lifetime);
  calls = calls.then(async () => {
    try {
      lifetime.signal.throwIfAborted();
      const value = await plugin.call(params.name, params.arguments || {}, lifetime.signal);
      await result(id, { content: [{ type: 'text', text: JSON.stringify(value) }], structuredContent: value });
    } catch (error) { await result(id, { isError: true, content: [{ type: 'text', text: error.name === 'AbortError' ? 'Mailbox request cancelled.' : error.message }] }); }
    finally { running.delete(id); }
  });
}
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => {
  buffer += chunk;
  if (Buffer.byteLength(buffer) > 512 * 1024) { process.stderr.write('Agent Mailbox: request frame exceeded the size limit.\n'); void shutdown(); return; }
  let end;
  while ((end = buffer.indexOf('\n')) >= 0) {
    const line = buffer.slice(0, end).trim(); buffer = buffer.slice(end + 1);
    if (!line) continue;
    let message; try { message = JSON.parse(line); } catch { void rpcError(null, -32700, 'Parse error'); continue; }
    void handle(message).catch(() => rpcError(message?.id ?? null, -32603, 'Mailbox request failed.'));
  }
});
async function shutdown() {
  if (closed) return;
  closed = true;
  for (const lifetime of running.values()) lifetime.abort();
  plugin.channelLifetime?.abort();
  const timeout = setTimeout(() => process.exit(0), 2000);
  await plugin.release(); clearTimeout(timeout); process.exit(0);
}
process.stdin.on('end', shutdown);
process.on('SIGINT', shutdown); process.on('SIGTERM', shutdown);
process.stdout.on('error', shutdown);
